package interop

import (
	"context"

	"github.com/segmentio/kafka-go"
)

type Rule struct {
	Handler func(ctx context.Context, msg kafka.Message) error
	// DLQ is the name of the DLQ topic to which messages should be sent.
	// If dlq is empty, returns error on failure.
	DLQ string
	// Attempts is a number of attempts to process message.
	Attempts int
}

type Flow struct {
	Rules map[string]Rule
}

func (f *Flow) listenTopics() []string {
	topics := make([]string, 0, len(f.Rules))
	for topic := range f.Rules {
		topics = append(topics, topic)
	}

	return topics
}
