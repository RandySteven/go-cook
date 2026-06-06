package temporal_client

import (
	"context"
	"fmt"
	"log"
	"time"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
)

type (
	TemporalConfig struct {
		Host          string
		Port          string
		Namespace     string
		TaskQueue     string
		WorkerOptions struct {
			MaxConcurrentActivityExecutionSize      int
			WorkerActivitiesPerSecond               float64
			MaxConcurrentLocalActivityExecutionSize int
			WorkerLocalActivitiesPerSecond          float64
		}
	}

	temporalClient struct {
		worker                worker.Worker
		client                client.Client
		taskQueue             string
		workflowExecutionData *WorkflowExecutionData
	}

	// StartWorkflowOptions configures how a workflow execution is started.
	StartWorkflowOptions struct {
		// WorkflowID is the business-level identifier for the workflow execution.
		// If not set, Temporal will generate a random UUID.
		WorkflowID string

		// TaskQueue overrides the default task queue from the client config.
		// If empty, the default task queue configured in Config will be used.
		TaskQueue string

		// WorkflowExecutionTimeout is the timeout for the entire workflow execution
		// including all retries and continue-as-new.
		WorkflowExecutionTimeout time.Duration

		// WorkflowRunTimeout is the timeout for a single workflow run.
		WorkflowRunTimeout time.Duration

		// WorkflowTaskTimeout is the timeout for a single workflow task (decision task).
		// Default is 10 seconds.
		WorkflowTaskTimeout time.Duration

		// RetryPolicy specifies the retry behavior for the workflow execution.
		RetryPolicy *RetryPolicy
	}

	// RetryPolicy defines the retry behavior for workflow or activity execution.
	RetryPolicy struct {
		// InitialInterval is the backoff interval for the first retry.
		InitialInterval time.Duration

		// BackoffCoefficient is the coefficient used to calculate the next retry backoff interval.
		// Default is 2.0.
		BackoffCoefficient float64

		// MaximumInterval is the maximum backoff interval between retries.
		MaximumInterval time.Duration

		// MaximumAttempts is the maximum number of attempts. 0 means unlimited.
		MaximumAttempts int32
	}

	WorkflowDefinition struct {
		Name string
		Fn   interface{}
	}

	ActivityDefinition struct {
		Name string
		Fn   interface{}
	}

	Temporal interface {
		// RegisterWorkflow registers a workflow definition with the engine.
		RegisterWorkflow(definition WorkflowDefinition)

		// RegisterActivity registers an activity definition with the engine.
		RegisterActivity(definition ActivityDefinition)

		// GetWorkflow returns a workflow execution.
		GetWorkflowInfo(workflowCtx workflow.Context) (*workflow.Info, error)

		// StartWorkflow starts a new workflow execution and returns the run ID.
		StartWorkflow(ctx context.Context, opts StartWorkflowOptions, workflowFn interface{}, args ...interface{}) (client.WorkflowRun, error)

		// SignalWorkflow sends a signal to a running workflow.
		SignalWorkflow(ctx context.Context, workflowID string, runID string, signalName string, arg interface{}) error

		// QueryWorkflow queries a running workflow for its current state.
		QueryWorkflow(ctx context.Context, workflowID string, queryType string, args ...interface{}) (interface{}, error)

		// CancelWorkflow requests cancellation of a running workflow.
		CancelWorkflow(ctx context.Context, workflowID string) error

		// GetWorkflowResult blocks until the workflow completes and returns the result.
		GetWorkflowResult(ctx context.Context, workflowID string, runID string, result interface{}) error

		// Start starts the internal worker that polls for tasks.
		Start() error

		// Stop gracefully shuts down the worker.
		Stop()
	}
)

func (t *temporalClient) RegisterWorkflow(definition WorkflowDefinition) {
	t.worker.RegisterWorkflowWithOptions(definition.Fn, workflow.RegisterOptions{
		Name: definition.Name,
	})
}

func (t *temporalClient) RegisterActivity(definition ActivityDefinition) {
	t.worker.RegisterActivityWithOptions(definition.Fn, activity.RegisterOptions{
		Name: definition.Name,
	})
}

func (t *temporalClient) StartWorkflow(ctx context.Context, opts StartWorkflowOptions, workflowFn interface{}, args ...interface{}) (client.WorkflowRun, error) {
	taskQueue := t.taskQueue
	if opts.TaskQueue != "" {
		taskQueue = opts.TaskQueue
	}

	startOpts := client.StartWorkflowOptions{
		ID:                       opts.WorkflowID,
		TaskQueue:                taskQueue,
		WorkflowExecutionTimeout: opts.WorkflowExecutionTimeout,
		WorkflowRunTimeout:       opts.WorkflowRunTimeout,
		WorkflowTaskTimeout:      opts.WorkflowTaskTimeout,
	}

	if opts.RetryPolicy != nil {
		startOpts.RetryPolicy = &temporal.RetryPolicy{
			InitialInterval:    opts.RetryPolicy.InitialInterval,
			BackoffCoefficient: opts.RetryPolicy.BackoffCoefficient,
			MaximumInterval:    opts.RetryPolicy.MaximumInterval,
			MaximumAttempts:    opts.RetryPolicy.MaximumAttempts,
		}
	}

	return t.client.ExecuteWorkflow(ctx, startOpts, workflowFn, args...)
}

func (t *temporalClient) SignalWorkflow(ctx context.Context, workflowID string, runID string, signalName string, arg interface{}) error {
	return t.client.SignalWorkflow(ctx, workflowID, runID, signalName, arg)
}

func (t *temporalClient) QueryWorkflow(ctx context.Context, workflowID string, queryType string, args ...interface{}) (interface{}, error) {
	resp, err := t.client.QueryWorkflow(ctx, workflowID, "", queryType, args...)
	if err != nil {
		return nil, err
	}

	var result interface{}
	if err := resp.Get(&result); err != nil {
		return nil, err
	}

	return result, nil
}

func (t *temporalClient) CancelWorkflow(ctx context.Context, workflowID string) error {
	return t.client.CancelWorkflow(ctx, workflowID, "")
}

func (t *temporalClient) GetWorkflowResult(ctx context.Context, workflowID string, runID string, result interface{}) error {
	run := t.client.GetWorkflow(ctx, workflowID, runID)
	return run.Get(ctx, result)
}

func (t *temporalClient) Start() error {
	return t.worker.Start()
}

func (t *temporalClient) Stop() {
	if t.worker != nil {
		t.worker.Stop()
	}
}

func (t *temporalClient) GetWorkflowInfo(workflowCtx workflow.Context) (*workflow.Info, error) {
	info := workflow.GetInfo(workflowCtx)
	return info, nil
}

func NewTemporalClient(config *TemporalConfig) (Temporal, error) {
	opts := client.Options{
		HostPort:  fmt.Sprintf("%s:%s", config.Host, config.Port),
		Namespace: config.Namespace,
		ConnectionOptions: client.ConnectionOptions{
			GetSystemInfoTimeout: 15 * time.Second, // give server more time to respond (SDK default is 5s)
		},
	}
	client, err := client.NewClient(opts)
	if err != nil {
		log.Println(`error while creating temporal client: `, err)
		log.Println(`hint: with "temporal server start-dev", add -n cafe_connect so the namespace exists (e.g. temporal server start-dev --ui-port 8080 -n cafe_connect)`)
		return nil, err
	}

	workerOptions := worker.Options{
		MaxConcurrentActivityExecutionSize:      config.WorkerOptions.MaxConcurrentActivityExecutionSize,
		WorkerActivitiesPerSecond:               config.WorkerOptions.WorkerActivitiesPerSecond,
		MaxConcurrentLocalActivityExecutionSize: config.WorkerOptions.MaxConcurrentLocalActivityExecutionSize,
		WorkerLocalActivitiesPerSecond:          config.WorkerOptions.WorkerLocalActivitiesPerSecond,
	}

	taskQueue := config.TaskQueue
	if taskQueue == "" {
		taskQueue = "default"
	}

	return &temporalClient{
		client:    client,
		worker:    worker.New(client, taskQueue, workerOptions),
		taskQueue: taskQueue,
	}, nil
}
