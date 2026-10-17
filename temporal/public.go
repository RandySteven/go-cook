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
// query that returns the current execution data.
func WorkflowInit(ctx workflow.Context, wf WorkflowExecution, queryType string, executionData ExecutionData) (ExecutionData, error) {
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

	if err := workflow.SetQueryHandler(ctx, queryType, func() (ExecutionData, error) {
		return wf.GetExecutionData(), nil
	}); err != nil {
		return nil, err
	}
	return wf.GetExecutionData(), nil
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
