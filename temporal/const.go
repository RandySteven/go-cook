package temporal_client

import "time"

const (
	//Temporal signals
	DefaultCorrectionSignal      = "retryWithCorrection"
	DefaultApprovalSignal        = "approve"
	DefaultStatusSearchAttribute = "WorkflowStatus"
	QueryGetStatus               = "getStatus"

	//Temporal statuses
	StatusPending            = "PENDING"
	StatusExecuting          = "EXECUTING"
	StatusAwaitingCorrection = "AWAITING_CORRECTION"
	StatusAwaitingApproval   = "AWAITING_APPROVAL"
	StatusFailed             = "FAILED"
	StatusCompleted          = "COMPLETED"
	StatusRejected           = "REJECTED"

	//Default setup for resumable activities
	defaultMaxCorrectionAttempts = 5
	defaultActivityRetryAttempts = 3
	defaultStartToCloseTimeout   = 30 * time.Second
)
