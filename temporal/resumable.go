package temporal_client

import (
	"encoding/json"
	"fmt"
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

const (
	DefaultCorrectionSignal      = "retryWithCorrection"
	DefaultApprovalSignal        = "approve"
	DefaultStatusSearchAttribute = "WorkflowStatus"
	QueryGetStatus               = "getStatus"

	StatusPending            = "PENDING"
	StatusExecuting          = "EXECUTING"
	StatusAwaitingCorrection = "AWAITING_CORRECTION"
	StatusAwaitingApproval   = "AWAITING_APPROVAL"
	StatusFailed             = "FAILED"
	StatusCompleted          = "COMPLETED"
	StatusRejected           = "REJECTED"

	defaultMaxCorrectionAttempts = 5
	defaultActivityRetryAttempts = 3
	defaultStartToCloseTimeout   = 30 * time.Second
)

// ResumableOptions configures the Resumable Activity pattern for a single
// state-machine step. After Temporal exhausts the activity RetryPolicy, Execute
// parks the workflow (no polling) until a correction Signal arrives, then
// re-runs the same activity with the updated execution data.
//
// See https://docs.temporal.io/design-patterns/resumable-activity
type ResumableOptions struct {
	// CorrectionSignal is the Signal name operators use to inject corrected
	// input. Defaults to retryWithCorrection.
	CorrectionSignal string

	// ApprovalSignal, when set, parks again after the activity succeeds until
	// an operator sends a boolean approval (true to continue, false to reject).
	ApprovalSignal string

	// MaxCorrectionAttempts is the number of park-and-retry cycles allowed
	// before the workflow fails. Defaults to 5.
	MaxCorrectionAttempts int

	// ActivityRetryAttempts is applied when the activity has no RetryPolicy.
	// Defaults to 3 so transient errors retry before parking.
	ActivityRetryAttempts int32

	// StatusSearchAttribute, when set, is upserted on each status transition so
	// operators can filter parked workflows in the Temporal UI. Leave empty
	// unless this Keyword attribute is already registered on the namespace
	// (unregistered names fail the workflow with BadSearchAttributes).
	// Example: DefaultStatusSearchAttribute ("WorkflowStatus").
	StatusSearchAttribute string

	// WaitTimeout bounds how long to wait for a correction or approval Signal.
	// Zero waits indefinitely (durable, no polling).
	WaitTimeout time.Duration
}

// StatusReporter lets execution state expose the parked/running status via
// the getStatus Query. Optional — Execute still tracks status internally.
type StatusReporter interface {
	SetStatus(status string)
	GetStatus() string
}

// CorrectionApplier applies a correction Signal payload onto execution state.
// Implement this when the Signal is a partial fix (for example a new account
// number) rather than a full replacement of the activity input.
type CorrectionApplier interface {
	ApplyCorrection(payload json.RawMessage) error
}

// NewNonRetryableError marks a permanent input failure so Temporal does not
// exhaust retries before Execute parks for a correction.
func NewNonRetryableError(errType, message string) error {
	return temporal.NewNonRetryableApplicationError(message, errType, nil)
}

func (o ResumableOptions) withDefaults() ResumableOptions {
	if o.CorrectionSignal == "" {
		o.CorrectionSignal = DefaultCorrectionSignal
	}
	if o.MaxCorrectionAttempts <= 0 {
		o.MaxCorrectionAttempts = defaultMaxCorrectionAttempts
	}
	if o.ActivityRetryAttempts <= 0 {
		o.ActivityRetryAttempts = defaultActivityRetryAttempts
	}
	return o
}

func (w *WorkflowExecutionData) statusSearchAttribute() string {
	for _, info := range w.activity {
		if info != nil && info.Resumable != nil && info.Resumable.StatusSearchAttribute != "" {
			return info.Resumable.StatusSearchAttribute
		}
	}
	return ""
}

func (w *WorkflowExecutionData) registerStatusQuery(ctx workflow.Context, executionData interface{}) {
	_ = workflow.SetQueryHandler(ctx, QueryGetStatus, func() (string, error) {
		if reporter, ok := executionData.(StatusReporter); ok {
			return reporter.GetStatus(), nil
		}
		return w.Status, nil
	})
}

func (w *WorkflowExecutionData) setExecutionStatus(ctx workflow.Context, executionData interface{}, status, searchAttr string) {
	w.Status = status
	if reporter, ok := executionData.(StatusReporter); ok {
		reporter.SetStatus(status)
	}
	if searchAttr == "" {
		return
	}
	_ = workflow.UpsertSearchAttributes(ctx, map[string]interface{}{searchAttr: status})
}

func resumableActivityContext(ctx workflow.Context, info *ActivityExecutionInfo) workflow.Context {
	opts := workflow.ActivityOptions{}
	if info.ActivityOptions != nil {
		opts = *info.ActivityOptions
	}
	if opts.StartToCloseTimeout == 0 {
		opts.StartToCloseTimeout = defaultStartToCloseTimeout
	}
	if opts.RetryPolicy == nil && info.Resumable != nil {
		opts.RetryPolicy = &temporal.RetryPolicy{
			MaximumAttempts: info.Resumable.ActivityRetryAttempts,
		}
	}
	return workflow.WithActivityOptions(ctx, opts)
}

func (w *WorkflowExecutionData) runResumableActivity(ctx workflow.Context, info *ActivityExecutionInfo, executionData interface{}, navigable ExecutionWorkflow) error {
	opts := info.Resumable.withDefaults()
	info.Resumable = &opts
	activityCtx := resumableActivityContext(ctx, info)
	correctionCh := workflow.GetSignalChannel(ctx, opts.CorrectionSignal)

	correctionCount := 0
	for {
		w.setExecutionStatus(ctx, executionData, StatusExecuting, opts.StatusSearchAttribute)

		future := workflow.ExecuteActivity(activityCtx, info.ActivityFn, executionData)
		err := future.Get(ctx, executionData)
		if err == nil {
			break
		}

		correctionCount++
		if correctionCount > opts.MaxCorrectionAttempts {
			w.setExecutionStatus(ctx, executionData, StatusFailed, opts.StatusSearchAttribute)
			return fmt.Errorf("activity %s failed after %d correction attempts: %w", info.ActivityName, opts.MaxCorrectionAttempts, err)
		}

		w.setExecutionStatus(ctx, executionData, StatusAwaitingCorrection, opts.StatusSearchAttribute)
		workflow.GetLogger(ctx).Warn("activity failed — waiting for correction",
			"activity", info.ActivityName,
			"error", err,
		)

		payload, waitErr := waitForSignalPayload(ctx, correctionCh, opts.WaitTimeout)
		if waitErr != nil {
			w.setExecutionStatus(ctx, executionData, StatusFailed, opts.StatusSearchAttribute)
			return fmt.Errorf("activity %s: %w", info.ActivityName, waitErr)
		}
		if err := applyCorrection(executionData, payload); err != nil {
			return fmt.Errorf("activity %s: apply correction: %w", info.ActivityName, err)
		}
	}

	if opts.ApprovalSignal != "" {
		if err := w.waitForApproval(ctx, executionData, info.ActivityName, opts); err != nil {
			return err
		}
	}

	return applyBranch(info, navigable)
}

func (w *WorkflowExecutionData) waitForApproval(ctx workflow.Context, executionData interface{}, activityName string, opts ResumableOptions) error {
	w.setExecutionStatus(ctx, executionData, StatusAwaitingApproval, opts.StatusSearchAttribute)
	approvalCh := workflow.GetSignalChannel(ctx, opts.ApprovalSignal)

	var approved bool
	if opts.WaitTimeout > 0 {
		ok, err := workflow.AwaitWithTimeout(ctx, opts.WaitTimeout, func() bool {
			return approvalCh.ReceiveAsync(&approved)
		})
		if err != nil {
			return err
		}
		if !ok {
			w.setExecutionStatus(ctx, executionData, StatusFailed, opts.StatusSearchAttribute)
			return fmt.Errorf("activity %s: approval signal %q timed out", activityName, opts.ApprovalSignal)
		}
	} else if err := workflow.Await(ctx, func() bool {
		return approvalCh.ReceiveAsync(&approved)
	}); err != nil {
		return err
	}

	if !approved {
		w.setExecutionStatus(ctx, executionData, StatusRejected, opts.StatusSearchAttribute)
		return fmt.Errorf("activity %s rejected by approval signal", activityName)
	}
	return nil
}

func waitForSignalPayload(ctx workflow.Context, ch workflow.ReceiveChannel, timeout time.Duration) (json.RawMessage, error) {
	var payload interface{}
	if timeout > 0 {
		ok, err := workflow.AwaitWithTimeout(ctx, timeout, func() bool {
			return ch.ReceiveAsync(&payload)
		})
		if err != nil {
			return nil, err
		}
		if !ok {
			return nil, fmt.Errorf("correction signal timed out")
		}
	} else if err := workflow.Await(ctx, func() bool {
		return ch.ReceiveAsync(&payload)
	}); err != nil {
		return nil, err
	}
	return encodeCorrection(payload)
}

func encodeCorrection(payload interface{}) (json.RawMessage, error) {
	switch v := payload.(type) {
	case json.RawMessage:
		return v, nil
	case []byte:
		return json.RawMessage(v), nil
	default:
		return json.Marshal(v)
	}
}

func applyCorrection(executionData interface{}, payload json.RawMessage) error {
	if applier, ok := executionData.(CorrectionApplier); ok {
		return applier.ApplyCorrection(payload)
	}
	if executionData == nil {
		return fmt.Errorf("execution data is nil")
	}
	return json.Unmarshal(payload, executionData)
}

func applyBranch(info *ActivityExecutionInfo, navigable ExecutionWorkflow) error {
	if navigable == nil || navigable.GetActivity() == "" {
		return nil
	}
	for _, nextActivity := range info.NextActivities {
		if navigable.GetActivity() == nextActivity {
			navigable.SetActivity(nextActivity)
			return nil
		}
	}
	return nil
}
