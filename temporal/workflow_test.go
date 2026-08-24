package temporal_client

import (
	"context"
	"errors"
	"testing"
	"time"

	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"
)

type navState struct {
	Activity string
}

func (n *navState) SetActivity(name string) { n.Activity = name }
func (n *navState) GetActivity() string     { return n.Activity }

func identityActivity(ctx context.Context, s *navState) (*navState, error) {
	return s, nil
}

func branchActivity(ctx context.Context, s *navState) (*navState, error) {
	s.Activity = "step2"
	return s, nil
}

func failActivity(ctx context.Context, s *navState) (*navState, error) {
	return nil, errors.New("activity failed")
}

func TestNewWorkflowExecution(t *testing.T) {
	mt := &mockTemporal{}
	got := NewWorkflowExecution(mt)
	we, ok := got.(*WorkflowExecutionData)
	if !ok || we == nil {
		t.Fatal("expected WorkflowExecutionData")
	}
	if we.activity == nil || we.signalConsumer == nil || we.temporalClient != mt {
		t.Fatal("expected initialized maps and client")
	}
}

func TestAddTransitionActivityWithOptions(t *testing.T) {
	mt := &mockTemporal{}
	we := NewWorkflowExecution(mt).(*WorkflowExecutionData)

	we.AddTransitionActivityWithOptions("step1", "sig", identityActivity, nil, "step2")
	if we.firstActivity != "step1" {
		t.Fatalf("firstActivity = %q, want step1", we.firstActivity)
	}
	if len(mt.activities) != 1 || mt.activities[0].Name != "step1" {
		t.Fatalf("registered activities = %+v", mt.activities)
	}
	if we.activity["step1"] == nil || we.activity["step2"] == nil {
		t.Fatal("expected step1 and placeholder step2")
	}

	we.AddTransitionActivityWithOptions("step2", "", identityActivity, nil)
	if we.firstActivity != "step1" {
		t.Fatal("first activity should not change")
	}
	if we.activity["step2"].ActivityFn == nil {
		t.Fatal("step2 should be overwritten with a real activity")
	}
}

func TestRegisterWorkflowDelegates(t *testing.T) {
	mt := &mockTemporal{}
	we := NewWorkflowExecution(mt).(*WorkflowExecutionData)
	we.RegisterWorkflow("OrderWorkflow", func(ctx workflow.Context) error { return nil })
	if len(mt.workflows) != 1 || mt.workflows[0].Name != "OrderWorkflow" {
		t.Fatalf("workflows = %+v", mt.workflows)
	}
}

func TestWorkflowExecutionDelegates(t *testing.T) {
	mt := &mockTemporal{startErr: errors.New("start"), getErr: errors.New("get"), signalErr: errors.New("sig")}
	we := NewWorkflowExecution(mt).(*WorkflowExecutionData)
	we.WorkflowID = "wf"

	if _, err := we.StartWorkflow(context.Background(), StartWorkflowOptions{}, "fn"); err == nil {
		t.Fatal("expected start error")
	}
	if err := we.GetWorkflowResult(context.Background(), "wf", "run", nil); err == nil {
		t.Fatal("expected get error")
	}
	if err := we.SignalWorkflow(context.Background(), "wf", "run", "n", nil); err == nil {
		t.Fatal("expected signal error")
	}
	if err := we.GetWorkflowExecutionData(nil, "run", nil); err == nil {
		t.Fatal("expected wrapped get error")
	}
	if err := we.GetExternalWorkflowResult(nil, "wf", "run", nil); err != nil {
		t.Fatalf("GetExternalWorkflowResult: %v", err)
	}
}

func TestGetNextActivity(t *testing.T) {
	we := &WorkflowExecutionData{
		activity: map[string]*ActivityExecutionInfo{
			"step2": {ActivityName: "step2"},
		},
	}
	curr := &ActivityExecutionInfo{NextActivities: []string{"step2"}}
	got := we.getNextActivity(curr, "step2")
	if got == nil || got.ActivityName != "step2" {
		t.Fatalf("got = %+v", got)
	}
	if we.getNextActivity(curr, "missing") != nil {
		t.Fatal("expected nil for unknown next activity")
	}
}

func TestExecuteSingleActivity(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()
	env.RegisterActivity(identityActivity)

	mt := &mockTemporal{}
	we := NewWorkflowExecution(mt).(*WorkflowExecutionData)
	opts := &workflow.ActivityOptions{StartToCloseTimeout: time.Second}
	we.AddTransitionActivityWithOptions("step1", "", identityActivity, opts)

	env.ExecuteWorkflow(func(ctx workflow.Context) error {
		return we.Execute(ctx, &navState{})
	})
	if err := env.GetWorkflowError(); err != nil {
		t.Fatal(err)
	}
	if we.StartedAt.IsZero() || we.CompletedAt.IsZero() {
		t.Fatal("expected started and completed timestamps")
	}
}

func TestExecuteBranchesToNextActivity(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()
	env.RegisterActivity(branchActivity)
	env.RegisterActivity(identityActivity)

	mt := &mockTemporal{}
	we := NewWorkflowExecution(mt).(*WorkflowExecutionData)
	we.AddTransitionActivityWithOptions("step1", "", branchActivity, &workflow.ActivityOptions{StartToCloseTimeout: time.Second}, "step2")
	we.AddTransitionActivityWithOptions("step2", "", identityActivity, &workflow.ActivityOptions{StartToCloseTimeout: time.Second})

	env.ExecuteWorkflow(func(ctx workflow.Context) error {
		return we.Execute(ctx, &navState{})
	})
	if err := env.GetWorkflowError(); err != nil {
		t.Fatal(err)
	}
}

func TestExecuteActivityFailure(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()
	env.RegisterActivity(failActivity)

	mt := &mockTemporal{}
	we := NewWorkflowExecution(mt).(*WorkflowExecutionData)
	we.AddTransitionActivityWithOptions("step1", "", failActivity, &workflow.ActivityOptions{StartToCloseTimeout: time.Second})

	env.ExecuteWorkflow(func(ctx workflow.Context) error {
		return we.Execute(ctx, &navState{})
	})
	if env.GetWorkflowError() == nil {
		t.Fatal("expected activity failure")
	}
}

func TestWaitForSignalTimeout(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()
	we := &WorkflowExecutionData{}

	env.ExecuteWorkflow(func(ctx workflow.Context) error {
		return we.WaitForSignal(ctx, "never", nil, time.Millisecond)
	})
	if env.GetWorkflowError() == nil {
		t.Fatal("expected timeout")
	}
}

func TestWaitForSignalSuccess(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()
	we := &WorkflowExecutionData{}

	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow("go", "payload")
	}, time.Millisecond)

	env.ExecuteWorkflow(func(ctx workflow.Context) error {
		var got string
		if err := we.WaitForSignal(ctx, "go", &got, time.Second); err != nil {
			return err
		}
		if got != "payload" {
			return errors.New("unexpected payload")
		}
		return nil
	})
	if err := env.GetWorkflowError(); err != nil {
		t.Fatal(err)
	}
}

func TestGetSignalResult(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()
	we := &WorkflowExecutionData{}

	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow("ping", "hi")
	}, time.Millisecond)

	env.ExecuteWorkflow(func(ctx workflow.Context) error {
		var got string
		return we.GetSignalResult(ctx, "ping", &got)
	})
	if err := env.GetWorkflowError(); err != nil {
		t.Fatal(err)
	}
}

func TestWaitForAnySignal(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()
	we := &WorkflowExecutionData{}

	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow("b", "val")
	}, time.Millisecond)

	env.ExecuteWorkflow(func(ctx workflow.Context) error {
		var a, b string
		name, err := we.WaitForAnySignal(ctx, map[string]interface{}{
			"a": &a,
			"b": &b,
		})
		if err != nil {
			return err
		}
		if name != "b" {
			return errors.New("expected signal b")
		}
		return nil
	})
	if err := env.GetWorkflowError(); err != nil {
		t.Fatal(err)
	}
}

func TestListenSignalAndGoroutine(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()
	we := &WorkflowExecutionData{}

	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow("tick", 1)
	}, time.Millisecond)

	env.ExecuteWorkflow(func(ctx workflow.Context) error {
		handled := false
		var n int
		if err := we.ListenSignal(ctx, "tick", &n, func(ctx workflow.Context) {
			handled = true
		}); err != nil {
			return err
		}
		done := false
		we.Goroutine(ctx, func(ctx workflow.Context) {
			done = true
		})
		if err := workflow.Await(ctx, func() bool { return handled && done }); err != nil {
			return err
		}
		return nil
	})
	if err := env.GetWorkflowError(); err != nil {
		t.Fatal(err)
	}
}

func TestWorkflowExecutionImplementsInterface(t *testing.T) {
	var _ WorkflowExecution = &WorkflowExecutionData{}
}
