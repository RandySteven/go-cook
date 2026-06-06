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
	// Navigable allows the Execute state machine to read which activity
	// should run next. Any state struct that implements this interface
	// enables branching in the pipeline.
	//
	// After an activity returns, Execute checks GetNextActivity():
	//   - "" (empty)  → continue to the next sequential activity
	//   - activity name → branch to that activity (must be registered via AddBranchActivity)
	//
	// After branching, execution stops (the branch path runs to completion,
	// then Execute returns). The workflow function can inspect the state to
	// decide what to do next.
	NavigatableActivity interface {
		SetActivity(activityName string)
		GetActivity() string
	}

	SignalActivity struct {
	}

	ActivityExecutionInfo struct {
		ActivityName    string
		SignalName      string
		ActivityFn      interface{}
		ActivityOptions *workflow.ActivityOptions
		NextActivities  []string
	}

	WorkflowExecutionData struct {
		ID         uint64
		WorkflowID string
		RunID      string

		activity      map[string]*ActivityExecutionInfo
		firstActivity string
		StartedAt     time.Time

		CompletedAt time.Time

		temporalClient Temporal
		signalConsumer *SignalConsumer
	}

	WorkflowExecution interface {
		// Execute runs the sequential activity pipeline, threading state through each activity.
		Execute(ctx workflow.Context, executionData interface{}) error

		// AddTransitionActivityWithOptions registers an activity with the Temporal worker and adds it
		// to the sequential execution pipeline. Activities run in the order they are added.
		// It is used to add an activity with options to the sequential execution pipeline.
		AddTransitionActivityWithOptions(activityName string, signalName string, activityFn interface{}, options *workflow.ActivityOptions, nextActivities ...string)

		// RegisterWorkflow registers a workflow with the Temporal worker.
		RegisterWorkflow(name string, fn interface{})

		// GetWorkflowExecutionData gets the workflow execution data.
		// It is used to get the workflow execution data from the Temporal server.
		GetWorkflowExecutionData(wfCtx workflow.Context, runID string, result interface{}) error

		// StartWorkflow starts a new workflow execution and returns the run ID.
		// It is used to start a new workflow execution and returns the run ID.
		StartWorkflow(ctx context.Context, opts StartWorkflowOptions, workflowFn interface{}, args ...interface{}) (client.WorkflowRun, error)

		// GetWorkflowResult gets the workflow result from the Temporal server.
		// It is used to get the workflow result from the Temporal server.
		GetWorkflowResult(ctx context.Context, workflowID string, runID string, result interface{}) error

		// StartChildWorkflow starts a new child workflow execution and returns the run ID.
		// It is used to start a new child workflow execution and returns the run ID.
		StartChildWorkflow(ctx workflow.Context, workflowID string, signalName string, request interface{}, result interface{}) error

		//SignalWorkflow signals a workflow.
		SignalWorkflow(ctx context.Context, workflowID string, runID string, signalName string, arg interface{}) error

		//Goroutine workflow run
		Goroutine(ctx workflow.Context, goroutineFn func(ctx workflow.Context))

		//GetSignalResult gets the signal result from the Temporal server.
		//It is used to get the signal result from the Temporal server.
		GetSignalResult(ctx workflow.Context, signalName string, result interface{}) error

		//SignalExternalWorkflow signals an external workflow.
		//It is used to signal an external workflow.
		SignalExternalWorkflow(ctx workflow.Context, workflowID string, runID string, signalName string, arg interface{}) error

		//GetExternalWorkflowResult gets the external workflow result from the Temporal server.
		//It is used to get the external workflow result from the Temporal server.
		GetExternalWorkflowResult(ctx workflow.Context, workflowID string, runID string, result interface{}) error

		//WaitForSignal waits for a signal and returns the result.
		//It is used to wait for a signal and returns the result.
		WaitForSignal(ctx workflow.Context, signalName string, result interface{}, timeout time.Duration) error

		//ListenSignal listens for a signal and calls the handler function when the signal is received.
		//It is used to listen for a signal and call the handler function when the signal is received.
		ListenSignal(ctx workflow.Context, signalName string, result interface{}, handler func(ctx workflow.Context)) error

		//WaitForAnySignal waits for any signal from the given map of signals and returns the signal name.
		//It is used to wait for any signal from the given map of signals and returns the signal name.
		WaitForAnySignal(ctx workflow.Context, signals map[string]interface{}) (string, error)
	}
)

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
func (w *WorkflowExecutionData) Execute(ctx workflow.Context, executionData interface{}) error {
	navigable, _ := executionData.(NavigatableActivity)
	currActivity := w.activity[w.firstActivity]

	w.StartedAt = time.Now()

	for currActivity != nil {
		if err := w.runActivity(ctx, currActivity, executionData, navigable); err != nil {
			return err
		}

		if currActivity.NextActivities == nil {
			break
		}

		nextActivity := navigable.GetActivity()
		if nextActivity == "" {
			break
		}

		currActivity = w.getNextActivity(currActivity, nextActivity)
	}

	w.CompletedAt = time.Now()

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

// runActivity executes a single activity and handles its signal if present.
func (w *WorkflowExecutionData) runActivity(ctx workflow.Context, info *ActivityExecutionInfo, executionData interface{}, navigable NavigatableActivity) error {
	activityCtx := ctx

	if info.ActivityOptions != nil {
		activityCtx = workflow.WithActivityOptions(ctx, *info.ActivityOptions)
	}

	future := workflow.ExecuteActivity(activityCtx, info.ActivityFn, executionData)
	if err := future.Get(ctx, executionData); err != nil {
		return fmt.Errorf("activity %s failed: %w", info.ActivityName, err)
	}

	if navigable.GetActivity() != "" {
		for _, nextActivity := range info.NextActivities {
			if navigable.GetActivity() == nextActivity {
				navigable.SetActivity(nextActivity)
				return nil
			}
		}
	}

	if info.SignalName != "" {
		if err := w.StartChildWorkflow(ctx, w.WorkflowID, info.SignalName, executionData, executionData); err != nil {
			return fmt.Errorf("child workflow for activity %s failed: %w", info.ActivityName, err)
		}
	}
	return nil
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

func (w *WorkflowExecutionData) AddTransitionActivityWithOptions(activityName string, signalName string, activityFn interface{}, options *workflow.ActivityOptions, nextActivities ...string) {
	w.temporalClient.RegisterActivity(ActivityDefinition{
		Name: activityName,
		Fn:   activityFn,
	})

	//initiate first activity
	if len(w.activity) == 0 {
		w.firstActivity = activityName
	}

	w.activity[activityName] = &ActivityExecutionInfo{
		ActivityName:    activityName,
		SignalName:      signalName,
		ActivityFn:      activityFn,
		ActivityOptions: options,
		NextActivities:  nextActivities,
	}

	for _, nextActivity := range nextActivities {
		w.activity[nextActivity] = &ActivityExecutionInfo{}
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

	timedOut, _ := workflow.AwaitWithTimeout(ctx, timeout, func() bool {
		return signalChan.ReceiveAsync(result)
	})

	if timedOut {
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
