package temporal_client

import (
	"context"
	"errors"
	"time"

	"go.temporal.io/sdk/workflow"
)

/*
* ExecuteWorkflow is a function that executes a workflow.
* It executes the workflow and returns the execution data.
*/
func ExecuteWorkflow(ctx workflow.Context, workflow WorkflowExecution, executionData ExecutionData) (ExecutionData, error) {
	if err := workflow.Execute(ctx, executionData); err != nil {
		return nil, err
	}
	return executionData, nil
}

/*
* WorkflowInit is a function that initializes a workflow.
* It initializes the workflow and returns the execution data.
*/
func WorkflowInit(ctx workflow.Context, workflowExecution WorkflowExecution, queryType string, executionData ExecutionData) (ExecutionData, error) {
	workflowExecution.SetExecutionData(executionData)

	info := workflow.GetInfo(ctx)
	if workflowExecution.GetWorkflowID() == "" {
		workflowExecution.SetWorkflowID(info.WorkflowExecution.ID)
	}

	if err := workflow.SetQueryHandler(ctx, queryType, func() (ExecutionData, error) {
		return workflowExecution.GetExecutionData(), nil
	}); err != nil {
		return nil, err
	}
	return workflowExecution.GetExecutionData(), nil
}

func GetWorkflowData(ctx workflow.Context, workflow WorkflowExecution) (workflowExecutionData WorkflowExecutionData, err error) {
	return workflowExecutionData, nil
}

/*
* QueryWorkflowData is a function that queries a workflow.
* It queries the workflow and returns the execution data.
*/
func QueryWorkflowData(ctx context.Context, temporalClient Temporal, queryType string, workflowID string, runId string) (ExecutionData, error) {
	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(1 * time.Second):
			executionData, err := temporalClient.QueryWorkflow(ctx, workflowID, runId, queryType)
			if err != nil {
				return nil, err
			}
			if executionData == nil {
				return nil, errors.New("execution data is nil")
			}
			if executionData, ok := executionData.(ExecutionData); ok {
				return executionData, nil
			}
			return nil, errors.New("execution data is not of type ExecutionData")
		}
	}
}
