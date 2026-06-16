// Package nsq_client provides an NSQ message queue client for publishing and
// consuming messages. NSQ is a real-time distributed messaging platform
// designed for high-throughput, fault-tolerant message delivery.
package nsq_client

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/nsqio/go-nsq"
)

type (
	NSQConfig struct {
		NSQDHost           string
		NSQDTCPPort        string
		LookupdHttpPort    string
		MaxInFlight        int
		ConcurrentConsumer int
		ReadTimeout        int
		WriteTimeout       int
		HeartbeatInterval  int
		BackoffMultiplier  int
		MaxBackoffDuration int
	}

	// Nsq defines the interface for NSQ operations including publishing and consuming messages.
	Nsq interface {
		// Publish sends a message to the specified topic.
		Publish(ctx context.Context, topic string, body []byte) error
		// Consume reads a message from the specified topic.
		Consume(ctx context.Context, topic string) (string, error)
		// RegisterConsumer registers a handler function for a topic.
		RegisterConsumer(topic string, channel string, handlerFunc func(context.Context, string)) error
	}

	// nsqClient is the internal implementation of the Nsq interface.
	nsqClient struct {
		pub                *nsq.Producer
		config             *NSQConfig
		nsqConfig          *nsq.Config
		lookupd            string
		concurrentConsumer int
	}

	// Publish defines the publish-only subset of NSQ operations.
	Publish interface {
		Publish(ctx context.Context, topic string, body []byte) error
	}

	// Consume defines the consume-only subset of NSQ operations.
	Consume interface {
		Consume(ctx context.Context, topic string) (string, error)
	}
)

// NewNsqClient creates a new NSQ client with a producer for publishing messages.
// Returns an error if the producer cannot be created.
func NewNsqClient(cfg *NSQConfig) (*nsqClient, error) {
	nsqConfig := nsq.NewConfig()

	nsqConfig.MaxInFlight = cfg.MaxInFlight
	nsqConfig.ReadTimeout = time.Duration(cfg.ReadTimeout) * time.Second
	nsqConfig.WriteTimeout = time.Duration(cfg.WriteTimeout) * time.Second
	nsqConfig.HeartbeatInterval = time.Duration(cfg.HeartbeatInterval) * time.Second

	nsqConfig.BackoffMultiplier = time.Duration(cfg.BackoffMultiplier) * time.Second
	nsqConfig.MaxBackoffDuration = time.Duration(cfg.MaxBackoffDuration) * time.Second

	addr := fmt.Sprintf("%s:%s", cfg.NSQDHost, cfg.NSQDTCPPort)
	producer, err := nsq.NewProducer(addr, nsqConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create NSQ producer: %w", err)
	}

	lookupd := fmt.Sprintf("%s:%s", cfg.NSQDHost, cfg.LookupdHttpPort)

	return &nsqClient{
		pub:                producer,
		config:             cfg,
		nsqConfig:          nsqConfig,
		lookupd:            lookupd,
		concurrentConsumer: cfg.ConcurrentConsumer,
	}, nil
}

// RegisterConsumer creates a new consumer for the specified topic and registers
// a handler function to process incoming messages. Messages are processed with
// a 30-second timeout context. Failed messages are automatically requeued.
// The handler function receives the context (with the message body as a value)
// and the topic name.
func (n *nsqClient) RegisterConsumer(topic string, channel string, handlerFunc func(context.Context, string)) error {
	log.Println("Creating NSQ consumer for topic:", topic)

	consumer, err := nsq.NewConsumer(topic, channel, n.nsqConfig)
	if err != nil {
		return fmt.Errorf("failed to create NSQ consumer: %w", err)
	}

	consumer.AddConcurrentHandlers(nsq.HandlerFunc(func(msg *nsq.Message) error {
		body := string(msg.Body)
		ctx := context.WithValue(context.Background(), topic, body)
		ctx, cancel := context.WithTimeout(ctx, time.Second*30)
		defer cancel()

		if err := func() error {
			handlerFunc(ctx, body)
			return nil
		}(); err != nil {
			log.Println("Error in handlerFunc:", err)
			msg.Requeue(-1)
			return err
		}

		return nil
	}), n.concurrentConsumer)

	lookupAddr := fmt.Sprintf("%s:%s", n.config.NSQDHost, n.config.LookupdHttpPort)
	log.Println("Connecting to nsqlookupd at", lookupAddr)

	if err := consumer.ConnectToNSQLookupd(lookupAddr); err != nil {
		return fmt.Errorf("failed to connect to NSQ lookupd: %w", err)
	}

	log.Println("NSQ consumer registered and running... for topic ", topic)
	return nil
}
