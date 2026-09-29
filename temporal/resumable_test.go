package temporal_client

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"
)

type resumableState struct {
	Activity string `json:"activity"`
	Account  string `json:"account"`
	status   string
}

var (
	_ ExecutionWorkflow  = &resumableState{}
	_ StatusReporter     = &resumableState{}
	_ CorrectionApplier  = &resumableState{}
)

func (s *resumableState) SetActivity(name string) { s.Activity = name }
func (s *resumableState) GetActivity() string     { return s.Activity }
func (s *resumableState) JSONString() (string, error) {
	b, err := json.Marshal(s)
	return string(b), err
}
func (s *resumableState) SetStatus(status string) { s.status = status }
func (s *resumableState) GetStatus() string       { return s.status }
func (s *resumableState) ApplyCorrection(payload json.RawMessage) error {
	var account string
	if err := json.Unmarshal(payload, &account); err != nil {
		return err
	}
	if account == "" {
		return errors.New("corrected account must be non-empty")
	}
	s.Account = account
	return nil
}

func transferActivity(ctx context.Context, s *resumableState) (*resumableState, error) {
	if s.Account != "account-123" {
		return nil, NewNonRetryableError("AccountNotFoundError", "account not found")
	}
	return s, nil
}

func alwaysFailActivity(ctx context.Context, s *resumableState) (*resumableState, error) {
	return nil, NewNonRetryableError("InvalidInput", "still invalid")
}

func okActivity(ctx context.Context, s *resumableState) (*resumableState, error) {
	return s, nil
}

func resumableActivityOptions() *workflow.ActivityOptions {
	return &workflow.ActivityOptions{
		StartToCloseTimeout: time.Second,
		RetryPolicy:         &temporal.RetryPolicy{MaximumAttempts: 1},
	}
}

func TestAddResumableTransitionActivityWithOptions(t *testing.T) {
	mt := &mockTemporal{}
	we := NewWorkflowExecution(mt).(*WorkflowExecutionData)
	we.AddResumableTransitionActivityWithOptions("transfer", transferActivity, nil, ResumableOptions{})

	info := we.activity["transfer"]
	if info == nil || info.Resumable == nil {
		t.Fatal("expected resumable options on the activity")
	}
	if info.SignalName != DefaultCorrectionSignal {
		t.Fatalf("SignalName = %q, want %s", info.SignalName, DefaultCorrectionSignal)
	}
	if info.Resumable.MaxCorrectionAttempts != defaultMaxCorrectionAttempts {
		t.Fatalf("MaxCorrectionAttempts = %d", info.Resumable.MaxCorrectionAttempts)
	}
}

func TestResumableActivityParksThenRetriesWithCorrection(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()
	env.RegisterActivity(transferActivity)

	mt := &mockTemporal{}
	we := NewWorkflowExecution(mt).(*WorkflowExecutionData)
	we.AddResumableTransitionActivityWithOptions("transfer", transferActivity, resumableActivityOptions(), ResumableOptions{})

	env.RegisterDelayedCallback(func() {
		val, err := env.QueryWorkflow(QueryGetStatus)
		if err != nil {
			t.Errorf("QueryWorkflow: %v", err)
			return
		}
		var status string
		if err := val.Get(&status); err != nil {
			t.Errorf("decode status: %v", err)
			return
		}
		if status != StatusAwaitingCorrection {
			t.Errorf("status = %q, want %s", status, StatusAwaitingCorrection)
		}
		env.SignalWorkflow(DefaultCorrectionSignal, "account-123")
	}, 50*time.Millisecond)

	env.ExecuteWorkflow(func(ctx workflow.Context) error {
		return we.Execute(ctx, &resumableState{Account: "invalid"})
	})
	if err := env.GetWorkflowError(); err != nil {
		t.Fatal(err)
	}
	if we.Status != StatusCompleted {
		t.Fatalf("Status = %q, want %s", we.Status, StatusCompleted)
	}
}

func TestResumableActivityFailsAfterMaxCorrectionAttempts(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()
	env.RegisterActivity(alwaysFailActivity)

	mt := &mockTemporal{}
	we := NewWorkflowExecution(mt).(*WorkflowExecutionData)
	we.AddResumableTransitionActivityWithOptions("transfer", alwaysFailActivity, resumableActivityOptions(), ResumableOptions{
		MaxCorrectionAttempts: 2,
	})

	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(DefaultCorrectionSignal, "fix-1")
		env.SignalWorkflow(DefaultCorrectionSignal, "fix-2")
	}, time.Millisecond)

	env.ExecuteWorkflow(func(ctx workflow.Context) error {
		return we.Execute(ctx, &resumableState{Account: "invalid"})
	})
	if env.GetWorkflowError() == nil {
		t.Fatal("expected failure after correction attempts")
	}
	if we.Status != StatusFailed {
		t.Fatalf("Status = %q, want %s", we.Status, StatusFailed)
	}
}

func TestResumableActivityCorrectionTimeout(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()
	env.RegisterActivity(alwaysFailActivity)

	mt := &mockTemporal{}
	we := NewWorkflowExecution(mt).(*WorkflowExecutionData)
	we.AddResumableTransitionActivityWithOptions("transfer", alwaysFailActivity, resumableActivityOptions(), ResumableOptions{
		WaitTimeout: time.Millisecond,
	})

	env.ExecuteWorkflow(func(ctx workflow.Context) error {
		return we.Execute(ctx, &resumableState{Account: "invalid"})
	})
	if env.GetWorkflowError() == nil {
		t.Fatal("expected correction timeout")
	}
}

func TestResumableActivityWaitsForApproval(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()
	env.RegisterActivity(okActivity)

	mt := &mockTemporal{}
	we := NewWorkflowExecution(mt).(*WorkflowExecutionData)
	we.AddResumableTransitionActivityWithOptions("transfer", okActivity, resumableActivityOptions(), ResumableOptions{
		ApprovalSignal: DefaultApprovalSignal,
	})

	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(DefaultApprovalSignal, true)
	}, time.Millisecond)

	env.ExecuteWorkflow(func(ctx workflow.Context) error {
		return we.Execute(ctx, &resumableState{Account: "account-123"})
	})
	if err := env.GetWorkflowError(); err != nil {
		t.Fatal(err)
	}
	if we.Status != StatusCompleted {
		t.Fatalf("Status = %q, want %s", we.Status, StatusCompleted)
	}
}

func TestResumableActivityRejectedByApproval(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()
	env.RegisterActivity(okActivity)

	mt := &mockTemporal{}
	we := NewWorkflowExecution(mt).(*WorkflowExecutionData)
	we.AddResumableTransitionActivityWithOptions("transfer", okActivity, resumableActivityOptions(), ResumableOptions{
		ApprovalSignal: DefaultApprovalSignal,
	})

	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(DefaultApprovalSignal, false)
	}, time.Millisecond)

	env.ExecuteWorkflow(func(ctx workflow.Context) error {
		return we.Execute(ctx, &resumableState{Account: "account-123"})
	})
	if env.GetWorkflowError() == nil {
		t.Fatal("expected rejection")
	}
	if we.Status != StatusRejected {
		t.Fatalf("Status = %q, want %s", we.Status, StatusRejected)
	}
}

func TestNewNonRetryableError(t *testing.T) {
	err := NewNonRetryableError("AccountNotFoundError", "account not found")
	var appErr *temporal.ApplicationError
	if !errors.As(err, &appErr) {
		t.Fatal("expected ApplicationError")
	}
	if !appErr.NonRetryable() {
		t.Fatal("expected non-retryable error")
	}
	if appErr.Type() != "AccountNotFoundError" {
		t.Fatalf("type = %q", appErr.Type())
	}
}

func TestApplyCorrectionWithoutApplier(t *testing.T) {
	state := &executionWorkflow{Activity: "old"}
	if err := applyCorrection(state, json.RawMessage(`{"activity":"step2"}`)); err != nil {
		t.Fatal(err)
	}
	if state.Activity != "step2" {
		t.Fatalf("Activity = %q, want step2", state.Activity)
	}
}
