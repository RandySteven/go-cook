package nsq_client

import (
	"context"
	"testing"

	"github.com/nsqio/go-nsq"
)

func TestNewNsqClientUnreachable(t *testing.T) {
	_, err := NewNsqClient(&NSQConfig{
		NSQDHost:          "127.0.0.1",
		NSQDTCPPort:       "1",
		LookupdHttpPort:   "1",
		MaxInFlight:       1,
		ReadTimeout:       1,
		WriteTimeout:      1,
		HeartbeatInterval: 1,
	})
	if err == nil {
		t.Fatal("expected error when nsqd is unreachable")
	}
}

func TestRegisterConsumerInvalidTopic(t *testing.T) {
	client := &nsqClient{
		nsqConfig: nsq.NewConfig(),
		config: &NSQConfig{
			NSQDHost:    "127.0.0.1",
			NSQDTCPPort: "4150",
		},
	}
	err := client.RegisterConsumer("", "channel", func(context.Context, string) {})
	if err == nil {
		t.Fatal("expected error for empty topic")
	}
}

func TestRegisterConsumerConnectFailure(t *testing.T) {
	cfg := nsq.NewConfig()
	client := &nsqClient{
		nsqConfig:          cfg,
		concurrentConsumer: 1,
		config: &NSQConfig{
			NSQDHost:    "127.0.0.1",
			NSQDTCPPort: "1",
		},
	}
	err := client.RegisterConsumer("orders", "workers", func(context.Context, string) {})
	if err == nil {
		t.Fatal("expected connect error")
	}
}

func TestPublishNilProducerPanics(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("expected panic")
		}
	}()
	client := &nsqClient{}
	_ = client.Publish(context.Background(), "topic", []byte("body"))
}
