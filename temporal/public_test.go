package temporal_client

import (
	"context"
	"errors"
	"testing"
	"time"

	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"
)

func TestExecuteWorkflowReturnsState(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()
	env.RegisterActivity(identityActivity)

	mt := &mockTemporal{}
	we := NewWorkflowExecution(mt).(*WorkflowExecutionData)
	we.AddTransitionActivityWithOptions("step1", "", identityActivity, &workflow.ActivityOptions{StartToCloseTimeout: time.Second})
	state := &executionWorkflow{Activity: "stay"}

	var got ExecutionData
	env.ExecuteWorkflow(func(ctx workflow.Context) error {
		var err error
		got, err = ExecuteWorkflow(ctx, we, state)
		return err
	})
	if err := env.GetWorkflowError(); err != nil {
		t.Fatal(err)
	}
	if got == nil || got.GetActivity() != "stay" {
		t.Fatalf("got = %#v", got)
	}
}

func TestExecuteWorkflowReturnsActivityError(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()
	env.RegisterActivity(failActivity)

	mt := &mockTemporal{}
	we := NewWorkflowExecution(mt).(*WorkflowExecutionData)
	we.AddTransitionActivityWithOptions("step1", "", failActivity, &workflow.ActivityOptions{StartToCloseTimeout: time.Second})

	env.ExecuteWorkflow(func(ctx workflow.Context) error {
		_, err := ExecuteWorkflow(ctx, we, &executionWorkflow{})
		return err
	})
	if err := env.GetWorkflowError(); err == nil {
		t.Fatal("expected activity error")
	}
}

func TestExecuteWorkflowRejectsNilInputs(t *testing.T) {
	if _, err := ExecuteWorkflow(nil, nil, &executionWorkflow{}); err == nil {
		t.Fatal("expected nil workflow error")
	}
	mt := &mockTemporal{}
	we := NewWorkflowExecution(mt)
	if _, err := ExecuteWorkflow(nil, we, nil); err == nil {
		t.Fatal("expected nil execution data error")
	}
}

func TestWorkflowInitRegistersQueryAndWorkflowID(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()

	mt := &mockTemporal{}
	we := NewWorkflowExecution(mt).(*WorkflowExecutionData)
	state := &executionWorkflow{Activity: "step1"}

	var queried executionWorkflow
	var queryErr error
	env.RegisterDelayedCallback(func() {
		val, err := env.QueryWorkflow("state")
		if err != nil {
			queryErr = err
			env.SignalWorkflow("done", true)
			return
		}
		queryErr = val.Get(&queried)
		env.SignalWorkflow("done", true)
	}, time.Millisecond)

	env.ExecuteWorkflow(func(ctx workflow.Context) error {
		got, err := WorkflowInit(ctx, we, "state", state, func(data ExecutionData) error {
			if data.GetActivity() != "step1" {
				return errors.New("state is not ready")
			}
			return nil
		})
		if err != nil {
			return err
		}
		if got.GetActivity() != "step1" {
			return errors.New("workflow init returned unexpected state")
		}
		data, err := GetWorkflowData(ctx, we)
		if err != nil {
			return err
		}
		if data.WorkflowID == "" || data.ExecutionData.GetActivity() != "step1" {
			return errors.New("workflow data was not initialized")
		}
		var done bool
		workflow.GetSignalChannel(ctx, "done").Receive(ctx, &done)
		return nil
	})
	if err := env.GetWorkflowError(); err != nil {
		t.Fatal(err)
	}
	if queryErr != nil {
		t.Fatal(queryErr)
	}
	if queried.Activity != "step1" {
		t.Fatalf("queried activity = %q", queried.Activity)
	}
	if we.GetWorkflowID() == "" {
		t.Fatal("expected workflow id from workflow info")
	}
}

func TestWorkflowInitKeepsExistingWorkflowID(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()
	mt := &mockTemporal{}
	we := NewWorkflowExecution(mt).(*WorkflowExecutionData)
	we.SetWorkflowID("order-1")

	env.ExecuteWorkflow(func(ctx workflow.Context) error {
		_, err := WorkflowInit(ctx, we, "state", &executionWorkflow{}, nil)
		return err
	})
	if err := env.GetWorkflowError(); err != nil {
		t.Fatal(err)
	}
	if we.GetWorkflowID() != "order-1" {
		t.Fatalf("workflow id = %q", we.GetWorkflowID())
	}
}

func TestWorkflowInitQueryReadyCanReject(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()

	mt := &mockTemporal{}
	we := NewWorkflowExecution(mt).(*WorkflowExecutionData)
	state := &executionWorkflow{Activity: "step1"}

	var queryErr error
	env.RegisterDelayedCallback(func() {
		_, queryErr = env.QueryWorkflow("state")
		env.SignalWorkflow("done", true)
	}, time.Millisecond)

	env.ExecuteWorkflow(func(ctx workflow.Context) error {
		_, err := WorkflowInit(ctx, we, "state", state, func(data ExecutionData) error {
			if data.GetActivity() != "ready" {
				return errors.New("not ready")
			}
			return nil
		})
		if err != nil {
			return err
		}
		var done bool
		workflow.GetSignalChannel(ctx, "done").Receive(ctx, &done)
		return nil
	})
	if err := env.GetWorkflowError(); err != nil {
		t.Fatal(err)
	}
	if queryErr == nil {
		t.Fatal("expected ready callback to reject the query")
	}
}

func TestWorkflowInitRejectsInvalidInput(t *testing.T) {
	if _, err := WorkflowInit(nil, nil, "state", &executionWorkflow{}, nil); err == nil {
		t.Fatal("expected nil workflow error")
	}
	mt := &mockTemporal{}
	we := NewWorkflowExecution(mt)
	if _, err := WorkflowInit(nil, we, "", &executionWorkflow{}, nil); err == nil {
		t.Fatal("expected empty query type error")
	}
	if _, err := WorkflowInit(nil, we, "state", nil, nil); err == nil {
		t.Fatal("expected nil execution data error")
	}
}

func TestGetWorkflowDataReadsStoredState(t *testing.T) {
	mt := &mockTemporal{}
	we := NewWorkflowExecution(mt).(*WorkflowExecutionData)
	state := &executionWorkflow{Activity: "step1"}
	we.SetWorkflowID("wf-1")
	we.SetExecutionData(state)

	got, err := GetWorkflowData(nil, we)
	if err != nil {
		t.Fatal(err)
	}
	if got.WorkflowID != "wf-1" || got.ExecutionData.GetActivity() != "step1" {
		t.Fatalf("got = %+v", got)
	}

	if _, err := GetWorkflowData(nil, nil); err == nil {
		t.Fatal("expected missing workflow error")
	}
	var empty *WorkflowExecutionData
	if _, err := GetWorkflowData(nil, empty); err == nil {
		t.Fatal("expected nil workflow data error")
	}
}

func TestQueryWorkflowDataReturnsExecutionData(t *testing.T) {
	state := &executionWorkflow{Activity: "step1"}
	mt := &mockTemporal{queryVal: state}

	start := time.Now()
	got, err := QueryWorkflowData(context.Background(), mt, "state", "wf-1", "run-1", nil)
	if err != nil {
		t.Fatal(err)
	}
	if time.Since(start) > 500*time.Millisecond {
		t.Fatal("query waited before the first attempt")
	}
	if got.GetActivity() != "step1" {
		t.Fatalf("activity = %q", got.GetActivity())
	}
}

func TestQueryWorkflowDataDecodesJSONPayload(t *testing.T) {
	mt := &mockTemporal{queryVal: map[string]interface{}{"activity": "step1"}}
	dest := &executionWorkflow{}

	got, err := QueryWorkflowData(context.Background(), mt, "state", "wf-1", "run-1", dest)
	if err != nil {
		t.Fatal(err)
	}
	if got != dest || dest.Activity != "step1" {
		t.Fatalf("dest = %+v", dest)
	}

	if _, err := QueryWorkflowData(context.Background(), mt, "state", "wf-1", "run-1", nil); err == nil {
		t.Fatal("expected type error without a destination")
	}
}

func TestQueryWorkflowDataRetriesUntilDeadline(t *testing.T) {
	calls := 0
	mt := &mockTemporal{}
	mt.queryFn = func(ctx context.Context, workflowID string, runID string, queryType string) (interface{}, error) {
		calls++
		if calls == 1 {
			return nil, errors.New("query handler is not registered")
		}
		return &executionWorkflow{Activity: "step1"}, nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	got, err := QueryWorkflowData(ctx, mt, "state", "wf-1", "run-1", nil)
	if err != nil {
		t.Fatal(err)
	}
	if got.GetActivity() != "step1" || calls < 2 {
		t.Fatalf("got activity %q after %d calls", got.GetActivity(), calls)
	}
}

func TestQueryWorkflowDataStopsWhenContextEnds(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	mt := &mockTemporal{queryVal: &executionWorkflow{Activity: "step1"}}
	if _, err := QueryWorkflowData(ctx, mt, "state", "wf-1", "run-1", nil); !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v", err)
	}

	ctx, cancel = context.WithTimeout(context.Background(), 1500*time.Millisecond)
	defer cancel()
	mt.queryErr = errors.New("not ready")
	mt.queryVal = nil
	_, err := QueryWorkflowData(ctx, mt, "state", "wf-1", "run-1", nil)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v", err)
	}
}

func TestQueryWorkflowDataRejectsInvalidInput(t *testing.T) {
	mt := &mockTemporal{}
	if _, err := QueryWorkflowData(nil, mt, "state", "wf", "run", nil); err == nil {
		t.Fatal("expected nil context error")
	}
	if _, err := QueryWorkflowData(context.Background(), nil, "state", "wf", "run", nil); err == nil {
		t.Fatal("expected nil client error")
	}
	if _, err := QueryWorkflowData(context.Background(), mt, "", "wf", "run", nil); err == nil {
		t.Fatal("expected empty query type error")
	}
	if _, err := QueryWorkflowData(context.Background(), mt, "state", "", "run", nil); err == nil {
		t.Fatal("expected empty workflow id error")
	}
	mt.queryVal = nil
	if _, err := QueryWorkflowData(context.Background(), mt, "state", "wf", "run", nil); err == nil {
		t.Fatal("expected nil execution data error")
	}
}
