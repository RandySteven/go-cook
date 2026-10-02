package temporal_client

import (
	"context"
	"time"

	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/workflow"
)

type (
	// ExecutionData is the mutable state threaded through the activity pipeline.
	// Implementations must be Temporal-serializable (concrete structs).
	ExecutionData interface {
		// SetActivity records which activity should run next for branching.
		SetActivity(activityName string)

		// GetActivity returns the next activity name, or empty to stop branching.
		GetActivity() string

		// Marshal serializes the execution state to bytes (typically JSON).
		Marshal() ([]byte, error)

		// Unmarshal applies bytes onto the execution state. Used for full state
		// replacement and for correction Signal payloads after a failure.
		Unmarshal(data []byte) error

		// Clone returns an independent copy of the execution state.
		Clone() ExecutionData
	}

	// WorkflowExecution drives a sequential (optionally branching) activity
	// pipeline and exposes Temporal client/workflow helpers.
	WorkflowExecution interface {
		// Execute runs the sequential activity pipeline, threading state through
		// each activity. Steps with a non-empty signalEvent park until that
		// Signal arrives, then run; on failure they park again, apply the
		// payload via Unmarshal, and retry.
		Execute(ctx workflow.Context, executionData ExecutionData) error

		// AddTransitionActivityWithOptions registers an activity with the
		// Temporal worker and appends it to the sequential pipeline. Activities
		// run in registration order. Non-empty signalEvent is the Signal that
		// must be sent before the activity runs; after failure, Execute waits on
		// the same signalEvent, applies the payload, and retries.
		// nextActivities lists allowed branch targets for this step.
		AddTransitionActivityWithOptions(activityName string, signalEvent string, activityFn ActivityFunction, options *workflow.ActivityOptions, nextActivities ...string)

		// RegisterWorkflow registers a workflow function with the Temporal worker.
		RegisterWorkflow(name string, fn interface{})

		// GetWorkflowExecutionData fetches the result of a workflow run identified
		// by w.WorkflowID and runID into result.
		GetWorkflowExecutionData(wfCtx workflow.Context, runID string, result interface{}) error

		// StartWorkflow starts a new workflow execution and returns its run handle.
		StartWorkflow(ctx context.Context, opts StartWorkflowOptions, workflowFn interface{}, args ...interface{}) (client.WorkflowRun, error)

		// GetWorkflowResult blocks until the workflow completes and decodes the
		// result into result.
		GetWorkflowResult(ctx context.Context, workflowID string, runID string, result interface{}) error

		// StartChildWorkflow starts a child workflow by name (signalEvent is used
		// as the child workflow function name), signals it with request, then
		// waits for the child result into result.
		StartChildWorkflow(ctx workflow.Context, workflowID string, signalEvent string, request interface{}, result interface{}) error

		// SignalWorkflow sends a Signal to a running workflow.
		SignalWorkflow(ctx context.Context, workflowID string, runID string, signalEvent string, arg interface{}) error

		// Goroutine starts a workflow-safe goroutine (workflow.Go).
		Goroutine(ctx workflow.Context, goroutineFn func(ctx workflow.Context))

		// GetSignalResult parks until signalEvent is received and writes the
		// payload into result.
		GetSignalResult(ctx workflow.Context, signalEvent string, result interface{}) error

		// SignalExternalWorkflow signals another running workflow and waits until
		// Temporal accepts the Signal.
		SignalExternalWorkflow(ctx workflow.Context, workflowID string, runID string, signalEvent string, arg interface{}) error

		// GetExternalWorkflowResult retrieves the result of an external workflow
		// execution. Currently a no-op placeholder.
		GetExternalWorkflowResult(ctx workflow.Context, workflowID string, runID string, result interface{}) error

		// WaitForSignal parks until signalEvent arrives (writing into result) or
		// timeout elapses.
		WaitForSignal(ctx workflow.Context, signalEvent string, result interface{}, timeout time.Duration) error

		// ListenSignal starts a workflow goroutine that loops on signalEvent and
		// invokes handler after each receive into result.
		ListenSignal(ctx workflow.Context, signalEvent string, result interface{}, handler func(ctx workflow.Context)) error

		// WaitForAnySignal parks until the first of the given Signals arrives,
		// writes into that entry's value, and returns the Signal name.
		WaitForAnySignal(ctx workflow.Context, signals map[string]interface{}) (string, error)

		// SetQueryHandler registers a Query handler on the current workflow.
		SetQueryHandler(ctx workflow.Context, queryName string, fn interface{}) (err error)

		// SetUpdateHandlerWithOptions registers an Update handler on the current
		// workflow with the given options.
		SetUpdateHandlerWithOptions(ctx workflow.Context, queryName string, fn interface{}, opt workflow.UpdateHandlerOptions) (err error)

		// UpdateWorkflow sends a Workflow Update to a running workflow and returns
		// a handle for the update lifecycle.
		UpdateWorkflow(ctx context.Context, queryName string, workflowID string, runID string, stage client.WorkflowUpdateStage, args interface{}) (client.WorkflowUpdateHandle, error)
	}
)
