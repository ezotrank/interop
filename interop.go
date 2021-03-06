package interop

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"strconv"
	"time"

	"github.com/segmentio/kafka-go"
	"golang.org/x/sync/errgroup"
)

func NewInterop(brokers []string, cg string, rules ...Rule) (*Interop, error) {
	if druls := dupls(rules); len(druls) != 0 {
		return nil, fmt.Errorf("duplicate rules: %v", druls)
	}

	return &Interop{
		rules: rules,
		cg:    cg,
		writer: &kafka.Writer{
			Addr: kafka.TCP(brokers...),
		},
		reader: kafka.NewReader(kafka.ReaderConfig{
			Brokers:     brokers,
			GroupTopics: topics(rules...),
			GroupID:     cg,
		}),
	}, nil
}

func dupls(rules []Rule) []string {
	hm := make(map[string]int)
	druls := make([]string, 0)
	for _, r := range rules {
		if val := hm[r.Topic]; val == 1 {
			druls = append(druls, r.Topic)
		} else {
			hm[r.Topic]++
		}
	}
	return druls
}

func topics(rules ...Rule) []string {
	topics := make([]string, 0, len(rules))
	for _, rule := range rules {
		topics = append(topics, rule.Topic)
	}

	return topics
}

type ireader interface {
	FetchMessage(ctx context.Context) (kafka.Message, error)
	CommitMessages(ctx context.Context, messages ...kafka.Message) error
	Close() error
}

type iwriter interface {
	WriteMessages(ctx context.Context, messages ...kafka.Message) error
	Close() error
}

type Interop struct {
	rules  []Rule
	cg     string
	reader ireader
	writer iwriter
}

func (i *Interop) run(ctx context.Context, msg kafka.Message) error {
	// TODO(ezo): terrible
	var rule Rule
	var found bool
	for _, rule = range i.rules {
		if rule.Topic == msg.Topic {
			found = true
			break
		}
	}

	if !found {
		return fmt.Errorf("no found rule for topic: %s", msg.Topic)
	}

	// TODO(ezo): validate this in the rule builder.
	if rule.Attempts < 1 {
		return fmt.Errorf("number of attempts must be greater than 0")
	}

	attempts := getAttempts(msg.Headers)

EXEC:
	// Never change the message.
	err := rule.Handler(ctx, msg)
	if err == nil {
		return nil
	}
	attempts++

	switch {
	case rule.Ordered && attempts < rule.Attempts:
		time.Sleep(rule.RetryDelay)
		goto EXEC
	case attempts >= rule.Attempts && rule.DLQ == "":
		return err
	case attempts >= rule.Attempts && rule.DLQ != "":
		msg.Topic = rule.DLQ
		// Remove attempts header before sending to DLQ.
		msg.Headers = removeAttempts(msg.Headers)
	default:
		msg.Headers = setAttempts(msg.Headers, attempts)
	}

	if err := i.writer.WriteMessages(ctx, msg); err != nil {
		return fmt.Errorf("failed to write message: %w", err)
	}

	return nil
}

func (i *Interop) Start(ctx context.Context) error {
	errc := make(chan error, 1)

	go func() {
		for {
			msg, err := i.reader.FetchMessage(ctx)
			if errors.Is(err, io.EOF) {
				errc <- nil
			} else if err != nil {
				errc <- fmt.Errorf("failed fetch message: %w", err)
				return
			}

			if err = i.run(ctx, msg); err != nil {
				errc <- err
				return
			}

			if err := i.reader.CommitMessages(ctx, msg); err != nil {
				errc <- err
				return
			}
		}
	}()

	defer func() {
		if err := i.shutdown(); err != nil {
			log.Printf("WARN: failed to shutdown: %s", err)
		}
	}()

	select {
	case <-ctx.Done():
		return nil
	case err := <-errc:
		return err
	}
}

const AttemptsHeader = "x-attempts"

func getAttempts(headers []kafka.Header) int {
	for _, header := range headers {
		if header.Key == AttemptsHeader {
			if val, err := strconv.Atoi(string(header.Value)); err != nil {
				log.Printf("failed to parse attempts header: %s", err)
			} else {
				return val
			}
		}
	}

	return 0
}

func removeAttempts(headers []kafka.Header) []kafka.Header {
	if len(headers) < 1 {
		return headers
	}

	nhs := make([]kafka.Header, 0)
	for _, h := range headers {
		if h.Key != AttemptsHeader {
			nhs = append(nhs, h)
		}
	}

	return nhs
}

func setAttempts(headers []kafka.Header, num int) []kafka.Header {
	// To prevent change origin data
	nhs := append([]kafka.Header{}, headers...)
	for i, h := range nhs {
		if h.Key == AttemptsHeader {
			nhs[i].Value = []byte(strconv.Itoa(num))
			return nhs
		}
	}

	return append(nhs, kafka.Header{
		Key:   AttemptsHeader,
		Value: []byte(strconv.Itoa(num)),
	})
}

func (i *Interop) shutdown() error {
	g := errgroup.Group{}
	g.Go(func() error {
		return i.reader.Close()
	})
	g.Go(func() error {
		return i.writer.Close()
	})

	return g.Wait()
}
