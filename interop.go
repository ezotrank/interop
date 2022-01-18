package interop

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"strconv"

	"github.com/segmentio/kafka-go"
	"golang.org/x/sync/errgroup"
)

func NewInterop(brokers []string, flow Flow, cg string) (*Interop, error) {
	return &Interop{
		flow: flow,
		cg:   cg,
		writer: &kafka.Writer{
			Addr: kafka.TCP(brokers...),
		},
		reader: kafka.NewReader(kafka.ReaderConfig{
			Brokers:     brokers,
			GroupTopics: flow.listenTopics(),
			GroupID:     cg,
		}),
	}, nil
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
	flow   Flow
	cg     string
	reader ireader
	writer iwriter
}

func (i *Interop) run(ctx context.Context, msg kafka.Message) error {
	rule, ok := i.flow.Rules[msg.Topic]
	if !ok {
		return fmt.Errorf("no rule for topic: %s", msg.Topic)
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
