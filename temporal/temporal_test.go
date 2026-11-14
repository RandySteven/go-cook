package temporal_client

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/mocks"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"
)

type mockTemporal struct {
	workflows  []WorkflowDefinition
	activities []ActivityDefinition

	startRun  client.WorkflowRun
	startErr  error
	signalErr error
	queryVal  interface{}
	queryErr  error
	queryFn   func(ctx context.Context, workflowID string, runID string, queryType string) (interface{}, error)
	cancelErr error
	getErr    error
	startWErr error
}

func (m *mockTemporal) RegisterWorkflow(definition WorkflowDefinition) {
	m.workflows = append(m.workflows, definition)
}

func (m *mockTemporal) RegisterActivity(definition ActivityDefinition) {
	m.activities = append(m.activities, definition)
}

func (m *mockTemporal) GetWorkflowInfo(workflowCtx workflow.Context) (*workflow.Info, error) {
	return workflow.GetInfo(workflowCtx), nil
}

func (m *mockTemporal) StartWorkflow(ctx context.Context, opts StartWorkflowOptions, workflowFn interface{}, args ...interface{}) (client.WorkflowRun, error) {
	return m.startRun, m.startErr
}

func (m *mockTemporal) SignalWithStartWorkflow(ctx context.Context, opts StartWorkflowOptions, signalName string, signalArg interface{}, workflowFn interface{}, workflowArgs ...interface{}) (client.WorkflowRun, error) {
	return m.startRun, m.startErr
}

func (m *mockTemporal) SignalWorkflow(ctx context.Context, workflowID string, runID string, signalName string, arg interface{}) error {
	return m.signalErr
}

func (m *mockTemporal) QueryWorkflow(ctx context.Context, workflowID string, runID string, queryType string, args ...interface{}) (interface{}, error) {
	if m.queryFn != nil {
		return m.queryFn(ctx, workflowID, runID, queryType)
	}
	return m.queryVal, m.queryErr
}

func (m *mockTemporal) CancelWorkflow(ctx context.Context, workflowID string) error {
	return m.cancelErr
}

func (m *mockTemporal) GetWorkflowResult(ctx context.Context, workflowID string, runID string, result interface{}) error {
	return m.getErr
}

func (m *mockTemporal) Start() error {
	return m.startWErr
}

func (m *mockTemporal) Stop() {}

func (m *mockTemporal) UpdateWorkflow(ctx context.Context, queryName, workflowID, runID string, stage client.WorkflowUpdateStage, args interface{}) (client.WorkflowUpdateHandle, error) {
	return nil, nil
}

func TestTemporalClientImplementsInterface(t *testing.T) {
	var _ Temporal = &temporalClient{}
	var _ Temporal = &mockTemporal{}
}

func TestTemporalClientStopNilWorker(t *testing.T) {
	tc := &temporalClient{}
	tc.Stop()
}

func TestGetWorkflowInfo(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()
	tc := &temporalClient{}

	env.ExecuteWorkflow(func(ctx workflow.Context) error {
		info, err := tc.GetWorkflowInfo(ctx)
		if err != nil {
			return err
		}
		if info == nil {
			return errors.New("expected workflow info")
		}
		return nil
	})
	if !env.IsWorkflowCompleted() {
		t.Fatal("workflow did not complete")
	}
	if err := env.GetWorkflowError(); err != nil {
		t.Fatal(err)
	}
}

func TestStartWorkflowUsesDefaultAndOverrideTaskQueue(t *testing.T) {
	mockClient := &mocks.Client{}
	mockRun := &mocks.WorkflowRun{}

	mockClient.On("ExecuteWorkflow", mock.Anything, mock.MatchedBy(func(opts client.StartWorkflowOptions) bool {
		return opts.TaskQueue == "default" && opts.ID == "wf-1"
	}), mock.Anything).Return(mockRun, nil).Once()

	mockClient.On("ExecuteWorkflow", mock.Anything, mock.MatchedBy(func(opts client.StartWorkflowOptions) bool {
		return opts.TaskQueue == "orders" &&
			opts.RetryPolicy != nil &&
			opts.RetryPolicy.MaximumAttempts == 3
	}), mock.Anything).Return(mockRun, nil).Once()

	tc := &temporalClient{client: mockClient, taskQueue: "default"}
	ctx := context.Background()

	run, err := tc.StartWorkflow(ctx, StartWorkflowOptions{WorkflowID: "wf-1"}, "MyWorkflow")
	if err != nil || run != mockRun {
		t.Fatalf("default queue: run=%v err=%v", run, err)
	}

	run, err = tc.StartWorkflow(ctx, StartWorkflowOptions{
		TaskQueue: "orders",
		RetryPolicy: &RetryPolicy{
			InitialInterval:    time.Second,
			BackoffCoefficient: 2,
			MaximumInterval:    time.Minute,
			MaximumAttempts:    3,
		},
	}, "MyWorkflow")
	if err != nil || run != mockRun {
		t.Fatalf("override queue: run=%v err=%v", run, err)
	}
	mockClient.AssertExpectations(t)
}

func TestSignalCancelQueryAndResult(t *testing.T) {
	mockClient := &mocks.Client{}
	mockRun := &mocks.WorkflowRun{}
	mockValue := &mocks.Value{}

	mockClient.On("SignalWorkflow", mock.Anything, "wf", "run", "ping", 1).Return(nil)
	mockClient.On("CancelWorkflow", mock.Anything, "wf", "").Return(nil)
	mockClient.On("QueryWorkflow", mock.Anything, "wf", "", "status").Return(mockValue, nil)
	mockValue.On("Get", mock.Anything).Run(func(args mock.Arguments) {
		ptr := args.Get(0).(*interface{})
		*ptr = "ok"
	}).Return(nil)
	mockClient.On("GetWorkflow", mock.Anything, "wf", "run").Return(mockRun)
	mockRun.On("Get", mock.Anything, mock.Anything).Return(nil)

	tc := &temporalClient{client: mockClient}
	ctx := context.Background()

	if err := tc.SignalWorkflow(ctx, "wf", "run", "ping", 1); err != nil {
		t.Fatal(err)
	}
	if err := tc.CancelWorkflow(ctx, "wf"); err != nil {
		t.Fatal(err)
	}
	got, err := tc.QueryWorkflow(ctx, "wf", "", "status")
	if err != nil || got != "ok" {
		t.Fatalf("QueryWorkflow = (%v, %v)", got, err)
	}
	if err := tc.GetWorkflowResult(ctx, "wf", "run", nil); err != nil {
		t.Fatal(err)
	}
}

func TestSignalWithStartWorkflow(t *testing.T) {
	mockClient := &mocks.Client{}
	mockRun := &mocks.WorkflowRun{}
	mockClient.On("SignalWithStartWorkflow", mock.Anything, "wf-1", "approve", true, mock.MatchedBy(func(opts client.StartWorkflowOptions) bool {
		return opts.ID == "wf-1" && opts.TaskQueue == "orders"
	}), mock.Anything, "payload").Return(mockRun, nil)

	tc := &temporalClient{client: mockClient, taskQueue: "default"}
	run, err := tc.SignalWithStartWorkflow(context.Background(), StartWorkflowOptions{
		WorkflowID: "wf-1",
		TaskQueue:  "orders",
	}, "approve", true, "PaymentWorkflow", "payload")
	if err != nil || run != mockRun {
		t.Fatalf("run=%v err=%v", run, err)
	}
	if _, err := tc.SignalWithStartWorkflow(context.Background(), StartWorkflowOptions{}, "", nil, "PaymentWorkflow"); err == nil {
		t.Fatal("expected empty signal name error")
	}
	mockClient.AssertExpectations(t)
}

func TestAlreadyStarted(t *testing.T) {
	err := serviceerror.NewWorkflowExecutionAlreadyStarted("already started", "req-1", "run-9")
	runID, ok := AlreadyStarted(err)
	if !ok || runID != "run-9" {
		t.Fatalf("AlreadyStarted = (%q, %v)", runID, ok)
	}
	if _, ok := AlreadyStarted(errors.New("other")); ok {
		t.Fatal("expected unrelated error to be ignored")
	}
}

func TestQueryWorkflowErrors(t *testing.T) {
	mockClient := &mocks.Client{}
	mockClient.On("QueryWorkflow", mock.Anything, "wf", "", "status").Return(nil, errors.New("query failed"))
	tc := &temporalClient{client: mockClient}
	if _, err := tc.QueryWorkflow(context.Background(), "wf", "", "status"); err == nil {
		t.Fatal("expected query error")
	}

	mockValue := &mocks.Value{}
	mockClient2 := &mocks.Client{}
	mockClient2.On("QueryWorkflow", mock.Anything, "wf", "", "status").Return(mockValue, nil)
	mockValue.On("Get", mock.Anything).Return(errors.New("decode failed"))
	tc2 := &temporalClient{client: mockClient2}
	if _, err := tc2.QueryWorkflow(context.Background(), "wf", "", "status"); err == nil {
		t.Fatal("expected decode error")
	}
}
