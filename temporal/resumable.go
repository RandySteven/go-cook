package temporal_client

import (
	"encoding/json"
	"fmt"
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
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

func (w *WorkflowExecutionData) registerStatusQuery(ctx workflow.Context) error {
	err := workflow.SetQueryHandler(ctx, QueryGetStatus, func() (string, error) {
		return w.Status, nil
	})
	if err != nil {
		return err
	}
	return nil
}

func (w *WorkflowExecutionData) setExecutionStatus(ctx workflow.Context, status, searchAttr string) error {
	w.Status = status
	if searchAttr == "" {
		return nil
	}
	err := workflow.UpsertSearchAttributes(ctx, map[string]interface{}{searchAttr: status})
	if err != nil {
		workflow.GetLogger(ctx).Error("failed to upsert search attributes", "error", err)
		return err
	}
	return nil
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

func (w *WorkflowExecutionData) runResumableActivity(ctx workflow.Context, info *ActivityExecutionInfo, executionData ExecutionData) error {
	var opts ResumableOptions
	if info.Resumable != nil {
		opts = *info.Resumable
	}
	if opts.CorrectionSignal == "" {
		opts.CorrectionSignal = info.SignalEvent
	}
	opts = opts.withDefaults()
	info.Resumable = &opts
	activityCtx := resumableActivityContext(ctx, info)
	signalCh := workflow.GetSignalChannel(ctx, info.SignalEvent)

	correctionCount := 0
	for {
		w.setExecutionStatus(ctx, StatusAwaitingCorrection, opts.StatusSearchAttribute)
		payload, waitErr := waitForSignalPayload(ctx, signalCh, opts.WaitTimeout)
		if waitErr != nil {
			w.setExecutionStatus(ctx, StatusFailed, opts.StatusSearchAttribute)
			return fmt.Errorf("activity %s: %w", info.ActivityName, waitErr)
		}
		if len(payload) > 0 && string(payload) != "null" {
			if err := applyCorrection(executionData, payload); err != nil {
				return fmt.Errorf("activity %s: apply correction: %w", info.ActivityName, err)
			}
		}

		w.setExecutionStatus(ctx, StatusExecuting, opts.StatusSearchAttribute)

		future := workflow.ExecuteActivity(activityCtx, info.ActivityFn, executionData)
		err := future.Get(ctx, executionData)
		if err == nil {
			break
		}

		correctionCount++
		if correctionCount > opts.MaxCorrectionAttempts {
			w.setExecutionStatus(ctx, StatusFailed, opts.StatusSearchAttribute)
			return fmt.Errorf("activity %s failed after %d correction attempts: %w", info.ActivityName, opts.MaxCorrectionAttempts, err)
		}

		workflow.GetLogger(ctx).Warn("activity failed — waiting for signalEvent",
			"activity", info.ActivityName,
			"signalEvent", info.SignalEvent,
			"error", err,
		)
	}

	if opts.ApprovalSignal != "" {
		if err := w.waitForApproval(ctx, info.ActivityName, opts); err != nil {
			return err
		}
	}

	return applyBranch(info, executionData)
}

func (w *WorkflowExecutionData) waitForApproval(ctx workflow.Context, activityName string, opts ResumableOptions) error {
	w.setExecutionStatus(ctx, StatusAwaitingApproval, opts.StatusSearchAttribute)
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
			w.setExecutionStatus(ctx, StatusFailed, opts.StatusSearchAttribute)
			return fmt.Errorf("activity %s: approval signal %q timed out", activityName, opts.ApprovalSignal)
		}
	} else if err := workflow.Await(ctx, func() bool {
		return approvalCh.ReceiveAsync(&approved)
	}); err != nil {
		return err
	}

	if !approved {
		w.setExecutionStatus(ctx, StatusRejected, opts.StatusSearchAttribute)
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

func applyCorrection(executionData ExecutionData, payload json.RawMessage) error {
	if executionData == nil {
		return fmt.Errorf("execution data is nil")
	}
	// Delegate to ExecutionData.Unmarshal so callers can accept full state
	// replacements or partial correction payloads (e.g. a corrected account).
	return executionData.Unmarshal(payload)
}

func applyBranch(info *ActivityExecutionInfo, navigable ExecutionData) error {
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
