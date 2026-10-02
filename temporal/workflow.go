package temporal_client

import (
	"context"
	"fmt"
	"log"
	"time"

	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/workflow"
)

type (
	SignalName   string
	ActivityName string
)

type (
	// ActivityFunction is any Temporal activity function. Prefer a concrete
	// input/output type (not ExecutionData) so the SDK can serialize payloads.
	ActivityFunction any

	ActivityExecutionInfo struct {
		ActivityName    string
		SignalEvent     string
		ActivityFn      interface{}
		ActivityOptions *workflow.ActivityOptions
		NextActivities  []string
		Resumable       *ResumableOptions
	}

	WorkflowExecutionData struct {
		ID         uint64
		WorkflowID string
		RunID      string
		Status     string

		activity      map[string]*ActivityExecutionInfo
		firstActivity string
		StartedAt     time.Time

		CompletedAt time.Time

		temporalClient Temporal
		signalConsumer *SignalConsumer
	}
)

// UpdateWorkflow implements [WorkflowExecution].
func (w *WorkflowExecutionData) UpdateWorkflow(ctx context.Context, queryName string, workflowID string, runID string, stage client.WorkflowUpdateStage, args interface{}) (client.WorkflowUpdateHandle, error) {
	handle, err := w.temporalClient.UpdateWorkflow(ctx, queryName, workflowID, runID, stage, args)
	if err != nil {
		return nil, err
	}
	return handle, nil
}

// SetUpdateHandlerWithOptions implements [WorkflowExecution].
func (w *WorkflowExecutionData) SetUpdateHandlerWithOptions(ctx workflow.Context, queryName string, fn interface{}, opt workflow.UpdateHandlerOptions) (err error) {
	if err := workflow.SetUpdateHandlerWithOptions(ctx, queryName, fn, opt); err != nil {
		return err
	}
	return nil
}

// SetQueryHandler implements [WorkflowExecution].
func (w *WorkflowExecutionData) SetQueryHandler(ctx workflow.Context, queryName string, fn interface{}) (err error) {
	return workflow.SetQueryHandler(ctx, queryName, fn)
}

// GetExternalWorkflowResult implements [WorkflowExecution].
func (w *WorkflowExecutionData) GetExternalWorkflowResult(ctx workflow.Context, workflowID string, runID string, result interface{}) error {
	return nil
}

// SignalExternalWorkflow implements [WorkflowExecution].
func (w *WorkflowExecutionData) SignalExternalWorkflow(ctx workflow.Context, workflowID string, runID string, signalName string, arg interface{}) error {
	sigFuture := workflow.SignalExternalWorkflow(ctx, workflowID, runID, signalName, arg)
	if err := sigFuture.Get(ctx, nil); err != nil {
		return fmt.Errorf("failed to signal external workflow: %w", err)
	}
	return nil
}

// Execute runs the sequential activity pipeline, threading state through each activity.
// If the state implements Navigable and an activity sets NextActivity, Execute branches
// to that activity (which must be registered via AddBranchActivity). After the branch
// chain completes, Execute returns — it does NOT resume the sequential pipeline.
//
// Steps with a non-empty signalEvent park until that Signal arrives, then run.
// On failure they park again on the same signalEvent, apply the payload, and retry.
func (w *WorkflowExecutionData) Execute(ctx workflow.Context, executionData ExecutionData) error {
	currActivity := w.activity[w.firstActivity]

	w.StartedAt = time.Now()
	statusAttr := w.statusSearchAttribute()
	err := w.setExecutionStatus(ctx, executionData, StatusPending, statusAttr)
	if err != nil {
		return err
	}
	err = w.registerStatusQuery(ctx)
	if err != nil {
		return err
	}
	for currActivity != nil {
		if err := w.runActivity(ctx, currActivity, executionData); err != nil {
			if w.Status != StatusFailed && w.Status != StatusRejected {
				w.setExecutionStatus(ctx, executionData, StatusFailed, statusAttr)
			}
			return err
		}

		if currActivity.NextActivities == nil {
			break
		}

		if executionData == nil {
			break
		}

		nextActivity := executionData.GetActivity()
		if nextActivity == "" {
			break
		}

		currActivity = w.getNextActivity(currActivity, nextActivity)
	}

	w.CompletedAt = time.Now()
	if w.Status != StatusFailed && w.Status != StatusRejected {
		w.setExecutionStatus(ctx, executionData, StatusCompleted, statusAttr)
	}

	return nil
}

func (w *WorkflowExecutionData) getNextActivity(currActivity *ActivityExecutionInfo, nextActivity string) *ActivityExecutionInfo {
	log.Println("next activity", nextActivity)
	for _, info := range currActivity.NextActivities {
		if info == nextActivity {
			return w.activity[info]
		}
	}
	return nil
}

// StartWorkflow starts a new workflow execution and returns the run ID.
func (w *WorkflowExecutionData) StartWorkflow(ctx context.Context, opts StartWorkflowOptions, workflowFn interface{}, args ...interface{}) (client.WorkflowRun, error) {
	return w.temporalClient.StartWorkflow(ctx, opts, workflowFn, args...)
}

// GetWorkflowResult gets the workflow result from the Temporal server.
func (w *WorkflowExecutionData) GetWorkflowResult(ctx context.Context, workflowID string, runID string, result interface{}) error {
	return w.temporalClient.GetWorkflowResult(ctx, workflowID, runID, result)
}

func (w *WorkflowExecutionData) SignalWorkflow(ctx context.Context, workflowID string, runID string, signalName string, arg interface{}) error {
	return w.temporalClient.SignalWorkflow(ctx, workflowID, runID, signalName, arg)
}

// runActivity executes a single activity. Non-empty SignalEvent waits for that Signal first.
func (w *WorkflowExecutionData) runActivity(ctx workflow.Context, currActivity *ActivityExecutionInfo, executionData ExecutionData) error {
	if currActivity.SignalEvent != "" {
		return w.runResumableActivity(ctx, currActivity, executionData)
	}

	activityCtx := ctx
	if currActivity.ActivityOptions != nil {
		activityCtx = workflow.WithActivityOptions(ctx, *currActivity.ActivityOptions)
	}

	future := workflow.ExecuteActivity(activityCtx, currActivity.ActivityFn, executionData)
	if err := future.Get(ctx, executionData); err != nil {
		return fmt.Errorf("activity %s failed: %w", currActivity.ActivityName, err)
	}

	return applyBranch(currActivity, executionData)
}

// RegisterWorkflow registers a workflow with the Temporal worker.
func (w *WorkflowExecutionData) RegisterWorkflow(name string, fn interface{}) {
	w.temporalClient.RegisterWorkflow(WorkflowDefinition{
		Name: name,
		Fn:   fn,
	})
}

// GetWorkflowExecutionData gets the workflow execution data.
// It is used to get the workflow execution data from the Temporal server.
func (w *WorkflowExecutionData) GetWorkflowExecutionData(wfCtx workflow.Context, runID string, result interface{}) error {
	err := w.temporalClient.GetWorkflowResult(context.Background(), w.WorkflowID, runID, result)
	if err != nil {
		return fmt.Errorf("failed to get workflow execution data: %w", err)
	}
	return nil
}

func (w *WorkflowExecutionData) AddTransitionActivityWithOptions(activityName string, signalEvent string, activityFn ActivityFunction, options *workflow.ActivityOptions, nextActivities ...string) {
	w.temporalClient.RegisterActivity(ActivityDefinition{
		Name: activityName,
		Fn:   activityFn,
	})

	//initiate first activity
	if len(w.activity) == 0 {
		w.firstActivity = activityName
	}

	info := &ActivityExecutionInfo{
		ActivityName:    activityName,
		SignalEvent:     signalEvent,
		ActivityFn:      activityFn,
		ActivityOptions: options,
		NextActivities:  nextActivities,
	}
	if signalEvent != "" {
		resumable := ResumableOptions{CorrectionSignal: signalEvent}.withDefaults()
		info.Resumable = &resumable
	}
	w.activity[activityName] = info

	for _, nextActivity := range nextActivities {
		if _, exists := w.activity[nextActivity]; !exists {
			w.activity[nextActivity] = &ActivityExecutionInfo{}
		}
	}
}

func (w *WorkflowExecutionData) StartChildWorkflow(ctx workflow.Context, workflowID string, signalName string, request interface{}, result interface{}) error {
	childCtx := workflow.WithChildOptions(ctx, workflow.ChildWorkflowOptions{
		WorkflowID: workflowID,
	})

	childWorkflowRun := workflow.ExecuteChildWorkflow(childCtx, signalName)
	var workflowExecution workflow.Execution
	if err := childWorkflowRun.GetChildWorkflowExecution().Get(ctx, &workflowExecution); err != nil {
		return fmt.Errorf("failed to get child workflow execution: %w", err)
	}

	sigFuture := workflow.SignalExternalWorkflow(ctx, workflowExecution.ID, workflowExecution.RunID, signalName, request)
	if err := sigFuture.Get(ctx, nil); err != nil {
		return fmt.Errorf("failed to signal child workflow: %w", err)
	}

	if err := childWorkflowRun.Get(childCtx, result); err != nil {
		return fmt.Errorf("failed to get child workflow result: %w", err)
	}

	return nil
}

func (w *WorkflowExecutionData) Goroutine(ctx workflow.Context, goroutineFn func(ctx workflow.Context)) {
	workflow.Go(ctx, goroutineFn)
}

// GetSignalResult gets the signal result from the Temporal server.
// It is used to get the signal result from the Temporal server.
func (w *WorkflowExecutionData) GetSignalResult(ctx workflow.Context, signalName string, result interface{}) error {
	resultSelector := workflow.NewSelector(ctx)

	resultChan := workflow.GetSignalChannel(ctx, signalName)

	resultSelector.AddReceive(resultChan, func(c workflow.ReceiveChannel, more bool) {
		c.Receive(ctx, result)
	})

	resultSelector.Select(ctx)

	return nil
}

func (w *WorkflowExecutionData) WaitForSignal(ctx workflow.Context, signalName string, result interface{}, timeout time.Duration) error {
	signalChan := workflow.GetSignalChannel(ctx, signalName)

	ok, err := workflow.AwaitWithTimeout(ctx, timeout, func() bool {
		return signalChan.ReceiveAsync(result)
	})
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("signal %s timed out", signalName)
	}

	return nil
}

func (w *WorkflowExecutionData) ListenSignal(ctx workflow.Context, signalName string, result interface{}, handler func(ctx workflow.Context)) error {
	workflow.Go(ctx, func(ctx workflow.Context) {
		signalChan := workflow.GetSignalChannel(ctx, signalName)
		for {
			signalChan.Receive(ctx, result)
			handler(ctx)
		}
	})
	return nil
}

func (w *WorkflowExecutionData) WaitForAnySignal(ctx workflow.Context, signals map[string]interface{}) (string, error) {
	selector := workflow.NewSelector(ctx)
	received := ""
	for signalName, result := range signals {
		signalChan := workflow.GetSignalChannel(ctx, signalName)
		selector.AddReceive(signalChan, func(c workflow.ReceiveChannel, more bool) {
			c.Receive(ctx, result)
			received = signalName
		})
	}
	selector.Select(ctx)
	return received, nil
}

// NewWorkflowExecution creates a new WorkflowExecution.
// It is used to create a new WorkflowExecution.
func NewWorkflowExecution(
	temporalClient Temporal,
) WorkflowExecution {
	return &WorkflowExecutionData{
		temporalClient: temporalClient,
		activity:       make(map[string]*ActivityExecutionInfo),
		signalConsumer: NewSignalConsumer(),
	}
}
