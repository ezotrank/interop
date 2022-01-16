package interop

import (
	"context"

	"github.com/segmentio/kafka-go"
)

type Rule struct {
	Handler  func(ctx context.Context, msg kafka.Message) error
	DLQ      string // if dlq is empty, returns error on failure.
	Attempts int    // retry attempts before sending to DLQ.
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
