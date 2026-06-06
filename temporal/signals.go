package temporal_client

import (
	"go.temporal.io/sdk/workflow"
)

type (
	SignalHandler[T any] struct {
		signalName string
		handler    func(ctx workflow.Context, result T) error
	}

	SignalConsumer struct {
		handlers map[string]func(ctx workflow.Context)
	}
)

func NewSignalConsumer() *SignalConsumer {
	return &SignalConsumer{
		handlers: make(map[string]func(ctx workflow.Context)),
	}
}

// On registers a signal handler for a given signal name.
// It is used to register a signal handler for a given signal name.
func On[T any](ctx workflow.Context,
	sc *SignalConsumer,
	signalName string, handler func(ctx workflow.Context, result T) error) {
	sc.handlers[signalName] = func(ctx workflow.Context) {
		runAsync(ctx, signalName, handler)
	}
}

// runAsync runs a signal handler asynchronously.
// It is used to run a signal handler asynchronously.
func runAsync[T any](ctx workflow.Context, signalName string, handler func(ctx workflow.Context, result T) error) {
	ch := workflow.GetSignalChannel(ctx, signalName)
	workflow.Go(ctx, func(ctx workflow.Context) {
		for {
			var result T
			ch.Receive(ctx, &result)
			if err := handler(ctx, result); err != nil {
				workflow.GetLogger(ctx).Error("error handling signal", "error", err)
			}
		}
	})
}
