package nsq_client

import "context"

type (
	// Nsq defines the interface for NSQ operations including publishing and consuming messages.
	Nsq interface {
		// Publish sends a message to the specified topic.
		Publish(ctx context.Context, topic string, body []byte) error
		// RegisterConsumer registers a handler function for a topic.
		RegisterConsumer(topic string, channel string, handlerFunc func(context.Context, string)) error
	}

	// Publish defines the publish-only subset of NSQ operations.
	Publish interface {
		Publish(ctx context.Context, topic string, body []byte) error
	}
)
