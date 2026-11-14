package temporal_client

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sort"
	"time"

	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
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

	// ActivityOptions is the per-step configuration passed to
	// AddTransitionActivityWithOptions. Timeout and retry fields are the
	// Temporal activity options. ResumableOption, when the signal name is
	// non-empty, sets how that signal gates or resumes the step.
	ActivityOptions struct {
		TaskQueue              string
		ScheduleToCloseTimeout time.Duration
		StartToCloseTimeout    time.Duration
		HeartbeatTimeout       time.Duration
		RetryPolicy            *temporal.RetryPolicy

		// ResumableOption is optional. The signal name is the signalEvent
		// argument, not a field here. Nil uses the resumable defaults.
		ResumableOption *ResumableOptions
	}

	ActivityExecutionInfo struct {
		ActivityName    string
		SignalEvent     string
		ActivityFn      interface{}
		ActivityOptions *workflow.ActivityOptions
		NextActivities  []string
		Resumable       *ResumableOptions
	}

	WorkflowExecutionData struct {
		ID            uint64
		WorkflowID    string
		RunID         string
		Status        string
		ExecutionData ExecutionData
		QueryType     string

		activity      map[string]*ActivityExecutionInfo
		firstActivity string
		StartedAt     time.Time

		CompletedAt time.Time

		temporalClient Temporal
		signalConsumer *SignalConsumer
	}

	// executionRun is workflow-local state for one execution. Query handlers
	// close over this value. The fields on WorkflowExecutionData are shared by
	// every run of that workflow type and are only a snapshot for callers that
	// inspect the struct after the workflow task returns.
	executionRun struct {
		status      string
		data        ExecutionData
		startedAt   time.Time
		completedAt time.Time
	}
)

// GetWorkflowID implements [WorkflowExecution].
func (w *WorkflowExecutionData) GetWorkflowID() string {
	return w.WorkflowID
}

// SetWorkflowID implements [WorkflowExecution].
func (w *WorkflowExecutionData) SetWorkflowID(workflowID string) {
	w.WorkflowID = workflowID
}

// GetExecutionData implements [WorkflowExecution].
func (w *WorkflowExecutionData) GetExecutionData() ExecutionData {
	return w.ExecutionData
}

// SetExecutionData implements [WorkflowExecution].
func (w *WorkflowExecutionData) SetExecutionData(executionData ExecutionData) {
	w.ExecutionData = executionData
}

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
// After an activity returns, Execute reads GetActivity. An empty name, or a step with
// no next activities, ends the pipeline. A name that is not in that step's
// nextActivities fails the workflow.
//
// Steps with a non-empty signalEvent park until that Signal arrives, then run.
// On failure they park again on the same signalEvent, apply the payload, and retry.
// ResumableOptions.RunBeforeSignal runs the activity first and uses the signal only
// after a failure.
func (w *WorkflowExecutionData) Execute(ctx workflow.Context, executionData ExecutionData) error {
	currActivity := w.activity[w.firstActivity]
	if currActivity == nil || currActivity.ActivityFn == nil {
		return errors.New("workflow has no start activity")
	}

	run := &executionRun{
		data:      executionData,
		startedAt: workflow.Now(ctx),
	}
	w.StartedAt = run.startedAt
	statusAttr := w.statusSearchAttribute()
	err := w.setExecutionStatus(ctx, run, StatusPending, statusAttr)
	if err != nil {
		return err
	}
	err = w.registerStatusQuery(ctx, run)
	if err != nil {
		return err
	}
	for currActivity != nil {
		if err := w.runActivity(ctx, run, currActivity, executionData); err != nil {
			if run.status != StatusFailed && run.status != StatusRejected {
				w.setExecutionStatus(ctx, run, StatusFailed, statusAttr)
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

		next, err := w.getNextActivity(currActivity, nextActivity)
		if err != nil {
			w.setExecutionStatus(ctx, run, StatusFailed, statusAttr)
			return err
		}
		currActivity = next
	}

	run.completedAt = workflow.Now(ctx)
	w.CompletedAt = run.completedAt
	if run.status != StatusFailed && run.status != StatusRejected {
		w.setExecutionStatus(ctx, run, StatusCompleted, statusAttr)
	}

	return nil
}

func (w *WorkflowExecutionData) getNextActivity(currActivity *ActivityExecutionInfo, nextActivity string) (*ActivityExecutionInfo, error) {
	for _, info := range currActivity.NextActivities {
		if info != nextActivity {
			continue
		}
		next := w.activity[info]
		if next == nil || next.ActivityFn == nil {
			return nil, fmt.Errorf("activity %s branches to unregistered activity %q", currActivity.ActivityName, nextActivity)
		}
		return next, nil
	}
	return nil, fmt.Errorf("activity %s cannot branch to %q", currActivity.ActivityName, nextActivity)
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

// runActivity executes a single activity. Non-empty SignalEvent waits for that Signal first,
// unless ResumableOptions.RunBeforeSignal is set.
func (w *WorkflowExecutionData) runActivity(ctx workflow.Context, run *executionRun, currActivity *ActivityExecutionInfo, executionData ExecutionData) error {
	if currActivity.SignalEvent != "" {
		return w.runResumableActivity(ctx, run, currActivity, executionData)
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
	if wfCtx != nil {
		return errors.New("GetWorkflowExecutionData cannot be called from workflow code; use GetWorkflowResult")
	}
	err := w.temporalClient.GetWorkflowResult(context.Background(), w.WorkflowID, runID, result)
	if err != nil {
		return fmt.Errorf("failed to get workflow execution data: %w", err)
	}
	return nil
}

func (w *WorkflowExecutionData) SignalWithStartWorkflow(ctx context.Context, opts StartWorkflowOptions, signalName string, signalArg interface{}, workflowFn interface{}, workflowArgs ...interface{}) (client.WorkflowRun, error) {
	return w.temporalClient.SignalWithStartWorkflow(ctx, opts, signalName, signalArg, workflowFn, workflowArgs...)
}

func (o *ActivityOptions) sdk() *workflow.ActivityOptions {
	if o == nil {
		return nil
	}
	return &workflow.ActivityOptions{
		TaskQueue:              o.TaskQueue,
		ScheduleToCloseTimeout: o.ScheduleToCloseTimeout,
		StartToCloseTimeout:    o.StartToCloseTimeout,
		HeartbeatTimeout:       o.HeartbeatTimeout,
		RetryPolicy:            o.RetryPolicy,
	}
}

func (w *WorkflowExecutionData) AddTransitionActivityWithOptions(activityName string, signalEvent string, activityFn ActivityFunction, options *ActivityOptions, nextActivities ...string) {
	if activityName == "" {
		log.Println("temporal: skipped activity with an empty name")
		return
	}
	w.warnActivityTaskQueue(activityName, options)
	w.temporalClient.RegisterActivity(ActivityDefinition{
		Name: activityName,
		Fn:   activityFn,
	})

	//initiate first activity
	if w.firstActivity == "" {
		w.firstActivity = activityName
	}

	next := make([]string, 0, len(nextActivities))
	for _, nextActivity := range nextActivities {
		if nextActivity == "" {
			log.Printf("temporal: activity %s ignored an empty next activity", activityName)
			continue
		}
		next = append(next, nextActivity)
	}
	if len(next) == 0 {
		next = nil
	}

	info := &ActivityExecutionInfo{
		ActivityName:    activityName,
		SignalEvent:     signalEvent,
		ActivityFn:      activityFn,
		ActivityOptions: options.sdk(),
		NextActivities:  next,
	}
	if signalEvent != "" {
		resumable := ResumableOptions{CorrectionSignal: signalEvent}
		if options != nil && options.ResumableOption != nil {
			resumable = *options.ResumableOption
			resumable.CorrectionSignal = signalEvent
		}
		resumable = resumable.withDefaults()
		info.Resumable = &resumable
	}
	w.activity[activityName] = info
}

func (w *WorkflowExecutionData) warnActivityTaskQueue(activityName string, options *ActivityOptions) {
	if options == nil || options.TaskQueue == "" || w.temporalClient == nil {
		return
	}
	source, ok := w.temporalClient.(interface{ TaskQueue() string })
	if !ok {
		return
	}
	workerQueue := source.TaskQueue()
	if workerQueue == "" || options.TaskQueue == workerQueue {
		return
	}
	log.Printf("temporal: activity %s task queue %q differs from worker queue %q and will not be polled by this worker", activityName, options.TaskQueue, workerQueue)
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
	names := make([]string, 0, len(signals))
	for signalName := range signals {
		names = append(names, signalName)
	}
	sort.Strings(names)
	for _, signalName := range names {
		result := signals[signalName]
		signalChan := workflow.GetSignalChannel(ctx, signalName)
		selector.AddReceive(signalChan, func(c workflow.ReceiveChannel, more bool) {
			c.Receive(ctx, result)
			received = signalName
		})
	}
	selector.Select(ctx)
	return received, nil
}

func (w *WorkflowExecutionData) GetWorkflowStatus(ctx context.Context, workflowID string, runID string) (string, error) {
	raw, err := w.temporalClient.QueryWorkflow(ctx, workflowID, runID, QueryGetStatus)
	if err != nil {
		return "", err
	}
	status, ok := raw.(string)
	if !ok {
		return "", fmt.Errorf("unexpected status query type %T", raw)
	}
	return status, nil
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
