package temporal_client

import (
	"errors"
	"testing"
	"time"

	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"
)

func TestNewSignalConsumer(t *testing.T) {
	sc := NewSignalConsumer()
	if sc == nil || sc.handlers == nil {
		t.Fatal("expected initialized SignalConsumer")
	}
}

func TestOnRegistersHandler(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()

	env.ExecuteWorkflow(func(ctx workflow.Context) error {
		sc := NewSignalConsumer()
		On[string](ctx, sc, "ready", func(ctx workflow.Context, result string) error {
			return nil
		})
		if _, ok := sc.handlers["ready"]; !ok {
			return errors.New("handler was not registered")
		}
		return nil
	})
	if err := env.GetWorkflowError(); err != nil {
		t.Fatal(err)
	}
}

func TestRunAsyncHandlesSignal(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()

	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow("ready", "payload")
	}, time.Millisecond)

	env.ExecuteWorkflow(func(ctx workflow.Context) error {
		got := ""
		runAsync(ctx, "ready", func(ctx workflow.Context, result string) error {
			got = result
			return nil
		})
		return workflow.Await(ctx, func() bool { return got == "payload" })
	})
	if err := env.GetWorkflowError(); err != nil {
		t.Fatal(err)
	}
}

func TestRunAsyncLogsHandlerError(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()

	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow("ready", "payload")
	}, time.Millisecond)

	env.ExecuteWorkflow(func(ctx workflow.Context) error {
		seen := false
		runAsync(ctx, "ready", func(ctx workflow.Context, result string) error {
			seen = true
			return errors.New("handler failed")
		})
		return workflow.Await(ctx, func() bool { return seen })
	})
	if err := env.GetWorkflowError(); err != nil {
		t.Fatal(err)
	}
}
