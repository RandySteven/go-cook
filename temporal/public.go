package temporal_client

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"go.temporal.io/sdk/workflow"
)

// ExecuteWorkflow runs the activity pipeline and returns the same execution state.
func ExecuteWorkflow(ctx workflow.Context, wf WorkflowExecution, executionData ExecutionData) (ExecutionData, error) {
	if wf == nil {
		return nil, errors.New("workflow execution is nil")
	}
	if executionData == nil {
		return nil, errors.New("execution data is nil")
	}
	if err := wf.Execute(ctx, executionData); err != nil {
		return nil, err
	}
	return executionData, nil
}

// WorkflowInit stores execution state, records the workflow ID, and registers a
// query that returns this run's execution data.
//
// ready, when set, runs inside the query handler. It must be pure: no network,
// cache, or logging. Return an error to tell a polling client the state is not
// ready yet. Prefer WaitForExecutionData when the readiness check can live on
// the client.
func WorkflowInit(
	ctx workflow.Context,
	wf WorkflowExecution,
	queryType string,
	executionData ExecutionData,
	ready func(ExecutionData) error) (ExecutionData, error) {
	if wf == nil {
		return nil, errors.New("workflow execution is nil")
	}
	if queryType == "" {
		return nil, errors.New("query type is empty")
	}
	if executionData == nil {
		return nil, errors.New("execution data is nil")
	}

	wf.SetExecutionData(executionData)
	if wf.GetWorkflowID() == "" {
		info := workflow.GetInfo(ctx)
		wf.SetWorkflowID(info.WorkflowExecution.ID)
	}

	// Capture the pointer for this run. Another execution may overwrite the
	// shared WorkflowExecution fields while this query is being served.
	state := executionData
	if err := workflow.SetQueryHandler(ctx, queryType, func() (ExecutionData, error) {
		if ready != nil {
			if err := ready(state); err != nil {
				return nil, err
			}
		}
		return state, nil
	}); err != nil {
		return nil, err
	}
	return state, nil
}

// WaitForExecutionData polls a workflow query until ready returns true or ctx ends.
// With no deadline, it queries once. ready runs on the client, not inside the workflow.
func WaitForExecutionData(ctx context.Context, temporalClient Temporal, queryType string, workflowID string, runID string, dest ExecutionData, ready func(ExecutionData) bool) (ExecutionData, error) {
	if ctx == nil {
		return nil, errors.New("context is nil")
	}
	if temporalClient == nil {
		return nil, errors.New("temporal client is nil")
	}
	if queryType == "" {
		return nil, errors.New("query type is empty")
	}
	if workflowID == "" {
		return nil, errors.New("workflow id is empty")
	}
	if ready == nil {
		return nil, errors.New("ready function is nil")
	}

	_, bounded := ctx.Deadline()
	var lastErr error
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}

		raw, err := temporalClient.QueryWorkflow(ctx, workflowID, runID, queryType)
		if err == nil && raw != nil {
			data, decErr := decodeExecutionData(raw, dest)
			if decErr != nil {
				return nil, decErr
			}
			if ready(data) {
				return data, nil
			}
			lastErr = errors.New("execution data is not ready")
		} else if err != nil {
			lastErr = err
		} else {
			lastErr = errors.New("execution data is nil")
		}
		if !bounded {
			return nil, lastErr
		}

		timer := time.NewTimer(time.Second)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, ctx.Err()
		case <-timer.C:
		}
	}
}

// LongRunningActivityOptions sets a start-to-close timeout and a heartbeat timeout
// so a worker crash is noticed before the activity's full deadline.
func LongRunningActivityOptions(startToClose, heartbeat time.Duration) *ActivityOptions {
	return &ActivityOptions{
		StartToCloseTimeout: startToClose,
		HeartbeatTimeout:    heartbeat,
	}
}

// GetWorkflowData returns the workflow execution held by wf.
// An empty workflow ID is filled from the workflow context.
func GetWorkflowData(ctx workflow.Context, wf WorkflowExecution) (WorkflowExecutionData, error) {
	data, ok := wf.(*WorkflowExecutionData)
	if !ok || data == nil {
		return WorkflowExecutionData{}, errors.New("workflow execution is not available")
	}
	if data.GetWorkflowID() == "" {
		if ctx == nil {
			return WorkflowExecutionData{}, errors.New("workflow context is nil")
		}
		info := workflow.GetInfo(ctx)
		data.SetWorkflowID(info.WorkflowExecution.ID)
	}
	return *data, nil
}

// QueryWorkflowData reads execution state through a workflow query.
// dest receives JSON query payloads from the Temporal client. A nil dest is
// valid when the client already returns an ExecutionData value.
// A context with a deadline retries once per second until the query succeeds.
func QueryWorkflowData(ctx context.Context, temporalClient Temporal, queryType string, workflowID string, runID string, dest ExecutionData) (ExecutionData, error) {
	if ctx == nil {
		return nil, errors.New("context is nil")
	}
	if temporalClient == nil {
		return nil, errors.New("temporal client is nil")
	}
	if queryType == "" {
		return nil, errors.New("query type is empty")
	}
	if workflowID == "" {
		return nil, errors.New("workflow id is empty")
	}

	_, bounded := ctx.Deadline()
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}

		raw, err := temporalClient.QueryWorkflow(ctx, workflowID, runID, queryType)
		if err == nil && raw != nil {
			return decodeExecutionData(raw, dest)
		}
		if err == nil {
			err = errors.New("execution data is nil")
		}
		if !bounded {
			return nil, err
		}

		timer := time.NewTimer(time.Second)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, ctx.Err()
		case <-timer.C:
		}
	}
}

func decodeExecutionData(raw interface{}, dest ExecutionData) (ExecutionData, error) {
	if data, ok := raw.(ExecutionData); ok {
		if dest == nil || dest == data {
			return data, nil
		}
		encoded, err := data.Marshal()
		if err != nil {
			return nil, err
		}
		if err := dest.Unmarshal(encoded); err != nil {
			return nil, err
		}
		return dest, nil
	}
	if dest == nil {
		return nil, fmt.Errorf("execution data is %T, not ExecutionData", raw)
	}

	encoded, err := json.Marshal(raw)
	if err != nil {
		return nil, err
	}
	if err := dest.Unmarshal(encoded); err != nil {
		return nil, err
	}
	return dest, nil
}
