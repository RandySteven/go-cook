package nsq_client

import (
	"context"
	"errors"
	"testing"

	"github.com/nsqio/go-nsq"
)

func TestNsqClientImplementsNsq(t *testing.T) {
	var _ Nsq = (*nsqClient)(nil)
}

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
	var client Nsq = &nsqClient{
		nsqConfig: nsq.NewConfig(),
		config: &NSQConfig{
			NSQDHost:    "127.0.0.1",
			NSQDTCPPort: "4150",
		},
	}
	err := client.RegisterConsumer("", "channel", func(context.Context, *nsq.Message) error { return nil })
	if err == nil {
		t.Fatal("expected error for empty topic")
	}
}

func TestRegisterConsumerConnectFailure(t *testing.T) {
	cfg := nsq.NewConfig()
	var client Nsq = &nsqClient{
		nsqConfig:          cfg,
		concurrentConsumer: 1,
		config: &NSQConfig{
			NSQDHost:    "127.0.0.1",
			NSQDTCPPort: "1",
		},
	}
	err := client.RegisterConsumer("orders", "workers", func(context.Context, *nsq.Message) error { return nil })
	if err == nil {
		t.Fatal("expected connect error")
	}
}

func TestHandleMessageReturnsHandlerError(t *testing.T) {
	msg := nsq.NewMessage(nsq.MessageID{'a'}, []byte(`{"id":1}`))
	want := errors.New("retry")

	err := handleMessage(func(ctx context.Context, got *nsq.Message) error {
		if _, ok := ctx.Deadline(); !ok {
			t.Fatal("handler context has no deadline")
		}
		if string(got.Body) != `{"id":1}` {
			t.Fatalf("body = %s", got.Body)
		}
		return want
	}, msg)
	if !errors.Is(err, want) {
		t.Fatalf("error = %v, want %v", err, want)
	}
}

func TestHandleMessageNilFinishes(t *testing.T) {
	err := handleMessage(func(context.Context, *nsq.Message) error {
		return nil
	}, nsq.NewMessage(nsq.MessageID{}, []byte("ok")))
	if err != nil {
		t.Fatalf("error = %v", err)
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
