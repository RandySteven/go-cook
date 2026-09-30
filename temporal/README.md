# temporal

Temporal client, worker, and activity state machine for go-cook services. Import path `github.com/RandySteven/go-cook/temporal`; package name `temporal_client`.

The package wraps `go.temporal.io/sdk` so application workflows can:

- connect a client and worker
- register workflows and activities
- run a named activity pipeline (`Execute`) with optional branching
- park a failed step for human correction ([Resumable Activity](https://docs.temporal.io/design-patterns/resumable-activity))
- wait on signals, queries, and updates

## Installation

```bash
go get github.com/RandySteven/go-cook/temporal
```

Requires Go 1.26.1+ and a Temporal server. For local development:

```bash
temporal server start-dev --ui-port 8080 -n <your-namespace>
```

The namespace in `TemporalConfig` must already exist. If you use `temporal server start-dev`, pass `-n` with the same value.

## Layout

| File | Responsibility |
|------|----------------|
| `temporal.go` | Client, worker, start/signal/query/cancel/update |
| `workflow.go` | Activity pipeline (`Execute`), transitions, signals inside a workflow |
| `resumable.go` | Park-on-failure, correction signal, optional approval |
| `signals.go` | Typed async signal consumer (`On`) |
| `nexus/` | Nexus stub (not implemented) |

---

## Client setup

```go
import temporal_client "github.com/RandySteven/go-cook/temporal"

cfg := &temporal_client.TemporalConfig{
    Host:      "localhost",
    Port:      "7233",
    Namespace: "default",
    TaskQueue: "orders",
}
cfg.WorkerOptions.MaxConcurrentActivityExecutionSize = 10

tc, err := temporal_client.NewTemporalClient(cfg)
if err != nil {
    log.Fatal(err)
}

exec := temporal_client.NewWorkflowExecution(tc)
exec.RegisterWorkflow("OrderWorkflow", OrderWorkflow)
exec.AddTransitionActivityWithOptions(
    "validate", "", ValidateActivity,
    &workflow.ActivityOptions{StartToCloseTimeout: 30 * time.Second},
    "charge",
)
exec.AddTransitionActivityWithOptions(
    "charge", "", ChargeActivity,
    &workflow.ActivityOptions{StartToCloseTimeout: 30 * time.Second},
)

if err := tc.Start(); err != nil {
    log.Fatal(err)
}
defer tc.Stop()
```

If `TaskQueue` is empty, the worker uses `"default"`.

### `TemporalConfig`

| Field | Meaning |
|-------|---------|
| `Host`, `Port` | Temporal frontend address (`Host:Port`) |
| `Namespace` | Temporal namespace |
| `TaskQueue` | Worker poll queue; also the default for `StartWorkflow` |
| `WorkerOptions` | Concurrency and rate limits for activities and local activities |

`NewTemporalClient` uses a 15s `GetSystemInfoTimeout` so a slow local server can still connect.

---

## Workflows this package supports

These are the execution patterns you compose in a workflow function. They are not separate Temporal workflow types; you register one workflow function and call the helpers below.

### 1. Sequential activity pipeline

Register steps with `AddTransitionActivityWithOptions`. Inside the workflow, call `Execute`. Each activity receives the same `executionData` pointer; the result of `future.Get` is written back into that pointer.

The first registered activity is the start node. After a step succeeds, `Execute` reads `GetActivity()` on `executionData` (if it implements `ExecutionWorkflow`) and jumps to a name listed in `nextActivities`. Empty `GetActivity()` or `nextActivities == nil` stops the pipeline.

```go
func OrderWorkflow(ctx workflow.Context, state *OrderState) error {
    return exec.Execute(ctx, state)
}
```

Use concrete struct types on activity signatures so Temporal can serialize input/output. Do not use interface types as activity parameters.

```go
func ValidateActivity(ctx context.Context, state *OrderState) (*OrderState, error) {
    return state, nil
}
```

### 2. Branching pipeline

`executionData` implements `ExecutionWorkflow`. After an activity returns, it sets the next step name:

```go
func ChargeActivity(ctx context.Context, state *OrderState) (*OrderState, error) {
    if state.NeedsReview {
        state.SetActivity("review")
    } else {
        state.SetActivity("fulfill")
    }
    return state, nil
}

exec.AddTransitionActivityWithOptions("charge", "", ChargeActivity, opts, "review", "fulfill")
exec.AddTransitionActivityWithOptions("review", "", ReviewActivity, opts)
exec.AddTransitionActivityWithOptions("fulfill", "", FulfillActivity, opts)
```

`Execute` only follows a name that appears in that step’s `nextActivities`. After the branch runs, it does not return to a skipped sequential path.

### 3. Fail-fast activity

Default `AddTransitionActivityWithOptions` steps. If the activity fails after Temporal retries, `Execute` returns the error and sets status `FAILED`.

### 4. Resumable activity (pause on failure)

Register with `AddResumableTransitionActivityWithOptions`. After retries are exhausted, the workflow parks (`AWAITING_CORRECTION`) until an operator sends a correction signal, then re-runs **the same step** and continues the state machine.

```go
exec.AddResumableTransitionActivityWithOptions(
    "transfer",
    TransferActivity,
    &workflow.ActivityOptions{
        StartToCloseTimeout: 30 * time.Second,
        RetryPolicy:         &temporal.RetryPolicy{MaximumAttempts: 3},
    },
    temporal_client.ResumableOptions{
        ApprovalSignal: temporal_client.DefaultApprovalSignal, // optional
    },
    "notify",
)
```

Permanent bad input should skip retries:

```go
return nil, temporal_client.NewNonRetryableError("AccountNotFoundError", "account not found")
```

Partial corrections implement `CorrectionApplier` on state. Otherwise the signal payload is JSON-unmarshaled onto `executionData`.

```go
func (s *OrderState) ApplyCorrection(payload json.RawMessage) error {
    var account string
    if err := json.Unmarshal(payload, &account); err != nil {
        return err
    }
    s.ToAccount = account
    return nil
}
```

Operator:

```bash
temporal workflow signal --workflow-id order-1 --name retryWithCorrection --input '"account-123"'
temporal workflow signal --workflow-id order-1 --name approve --input 'true'
temporal workflow query --workflow-id order-1 --type getStatus
```

Or from Go:

```go
_ = exec.SignalWorkflow(ctx, workflowID, runID, temporal_client.DefaultCorrectionSignal, "account-123")
status, _ := tc.QueryWorkflow(ctx, workflowID, runID, temporal_client.QueryGetStatus)
```

Do **not** set `StatusSearchAttribute` unless that Keyword attribute is registered on the namespace. Unregistered names fail the workflow with `BadSearchAttributes`.

```bash
temporal operator search-attribute create --namespace default --name WorkflowStatus --type Keyword
```

Then:

```go
temporal_client.ResumableOptions{
    StatusSearchAttribute: temporal_client.DefaultStatusSearchAttribute,
}
```

### 5. Human approval after a resumable step

If `ApprovalSignal` is set, success of that step parks again (`AWAITING_APPROVAL`). `true` continues the pipeline; `false` sets `REJECTED` and fails `Execute`.

### 6. Signal wait (single)

`WaitForSignal` parks until one named signal arrives, or until `timeout`.

### 7. Signal wait (any of several)

`WaitForAnySignal` parks until the first of several signals arrives and returns that signal name.

### 8. Background signal listener

`ListenSignal` starts a workflow goroutine that receives the signal in a loop and calls `handler`.

### 9. Typed async handlers (`On`)

`On[T]` registers a handler on a `SignalConsumer`. Call the stored handler (or `runAsync` via `On`) from the workflow so it loops on that channel.

### 10. Child workflow + signal

`StartChildWorkflow` starts a child by name (`signalName` is used as the child workflow function name), signals it, then waits for the child result.

### 11. External workflow signal

`SignalExternalWorkflow` sends a signal to another running workflow and waits until Temporal accepts it.

### 12. Query and update

`SetQueryHandler` / `SetUpdateHandlerWithOptions` register handlers on the current workflow. `QueryWorkflow` / `UpdateWorkflow` are client-side calls. `Execute` always registers `getStatus`.

---

## Status values

`Execute` tracks status on `WorkflowExecutionData.Status` and, if implemented, `StatusReporter`.

| Constant | When |
|----------|------|
| `PENDING` | Pipeline started |
| `EXECUTING` | Resumable activity running |
| `AWAITING_CORRECTION` | Parked for `retryWithCorrection` |
| `AWAITING_APPROVAL` | Parked for `approve` |
| `FAILED` | Activity or wait failed |
| `REJECTED` | Approval signal was `false` |
| `COMPLETED` | Pipeline finished without failure/rejection |

---

## Function reference

### Client (`Temporal`)

Created by `NewTemporalClient`.

| Function | Where | What it does |
|----------|--------|----------------|
| `NewTemporalClient` | `temporal.go` | Builds SDK client + worker. Fails if the server/namespace is unreachable. |
| `RegisterWorkflow` | `temporal.go` | Registers `Fn` on the worker under `Name`. |
| `RegisterActivity` | `temporal.go` | Registers `Fn` on the worker under `Name`. |
| `StartWorkflow` | `temporal.go` | Starts an execution. Uses config task queue unless `opts.TaskQueue` is set. Copies `RetryPolicy` onto start options. |
| `SignalWorkflow` | `temporal.go` | Sends a signal to `workflowID` / `runID`. Empty `runID` targets the current run. |
| `QueryWorkflow` | `temporal.go` | Runs a query and decodes the result into `interface{}`. |
| `CancelWorkflow` | `temporal.go` | Requests cancel for `workflowID` (empty run ID). |
| `GetWorkflowResult` | `temporal.go` | Blocks until completion and decodes into `result`. |
| `UpdateWorkflow` | `temporal.go` | Sends a workflow update and returns a handle. |
| `GetWorkflowInfo` | `temporal.go` | `workflow.GetInfo` — call only from workflow code. |
| `Start` | `temporal.go` | Starts the worker poller. |
| `Stop` | `temporal.go` | Stops the worker if non-nil. |

#### `StartWorkflowOptions`

| Field | Meaning |
|-------|---------|
| `WorkflowID` | Business ID; Temporal generates a UUID if empty |
| `TaskQueue` | Override worker default |
| `WorkflowExecutionTimeout` | Entire execution including retries / continue-as-new |
| `WorkflowRunTimeout` | Single run |
| `WorkflowTaskTimeout` | Single workflow task (SDK default 10s) |
| `RetryPolicy` | Workflow-level retry |

#### `RetryPolicy`

| Field | Meaning |
|-------|---------|
| `InitialInterval` | First backoff |
| `BackoffCoefficient` | Multiplier (SDK default 2.0) |
| `MaximumInterval` | Cap between retries |
| `MaximumAttempts` | `0` means unlimited |

### Pipeline (`WorkflowExecution`)

Created by `NewWorkflowExecution(temporalClient)`.

| Function | Where | What it does |
|----------|--------|----------------|
| `NewWorkflowExecution` | `workflow.go` | Allocates the activity map and a `SignalConsumer`. |
| `Execute` | `workflow.go` | Runs the pipeline from `firstActivity`. Registers `getStatus`. Sets `PENDING` then each step; `COMPLETED` on success. Branching uses `ExecutionWorkflow.GetActivity()`. |
| `AddTransitionActivityWithOptions` | `workflow.go` | Registers the activity on the worker and adds a fail-fast graph node. First call sets the start node. Unknown `nextActivities` get placeholder entries until registered. |
| `AddResumableTransitionActivityWithOptions` | `workflow.go` | Same as above, plus `ResumableOptions` (correction signal stored as `SignalName`). |
| `RegisterWorkflow` | `workflow.go` | Delegates to the Temporal client. |
| `StartWorkflow` | `workflow.go` | Delegates to the client. |
| `GetWorkflowResult` | `workflow.go` | Delegates to the client. |
| `GetWorkflowExecutionData` | `workflow.go` | `GetWorkflowResult` using `w.WorkflowID` and `runID`. |
| `SignalWorkflow` | `workflow.go` | Delegates to the client. |
| `StartChildWorkflow` | `workflow.go` | Child workflow named `signalName`, then signal + wait for result. |
| `SignalExternalWorkflow` | `workflow.go` | `workflow.SignalExternalWorkflow` and wait. |
| `GetExternalWorkflowResult` | `workflow.go` | **Not implemented** (always `nil`). |
| `Goroutine` | `workflow.go` | `workflow.Go`. |
| `GetSignalResult` | `workflow.go` | Selector receive on one signal channel (no timeout). |
| `WaitForSignal` | `workflow.go` | Await signal with timeout. `AwaitWithTimeout` `ok == false` means timeout. |
| `ListenSignal` | `workflow.go` | Background loop: receive then `handler`. |
| `WaitForAnySignal` | `workflow.go` | Selector over a map of `signalName → dest pointer`. |
| `SetQueryHandler` | `workflow.go` | `workflow.SetQueryHandler`. |
| `SetUpdateHandlerWithOptions` | `workflow.go` | `workflow.SetUpdateHandlerWithOptions`. |
| `UpdateWorkflow` | `workflow.go` | Delegates to the client. |

Internal (not on the interface): `runActivity`, `runResumableActivity`, `getNextActivity`, `waitForApproval`, `setExecutionStatus`, `registerStatusQuery`, `statusSearchAttribute`, `applyCorrection`, `applyBranch`.

### `ExecutionWorkflow`

Implement on pipeline state for branching:

| Method | Role |
|--------|------|
| `SetActivity(name)` | Store the next step name |
| `GetActivity()` | Name `Execute` should jump to; `""` stops |
| `JSONString()` | Serialize state for logs or debugging |

### Resumable helpers

| Function / type | Where | What it does |
|-----------------|--------|----------------|
| `ResumableOptions` | `resumable.go` | Per-step park/retry config. Empty `CorrectionSignal` → `retryWithCorrection`. Empty `MaxCorrectionAttempts` → 5. Empty `ActivityRetryAttempts` → 3. Empty `StatusSearchAttribute` → **no upsert**. `WaitTimeout` 0 → wait forever. |
| `StatusReporter` | `resumable.go` | Optional `SetStatus` / `GetStatus` on state; used by `getStatus`. |
| `CorrectionApplier` | `resumable.go` | Optional `ApplyCorrection(json.RawMessage)` for partial fixes. |
| `NewNonRetryableError` | `resumable.go` | `temporal.NewNonRetryableApplicationError` for bad input. |

`runResumableActivity` loop:

1. Status `EXECUTING`, run activity.
2. Success → optional approval wait → branch.
3. Failure → increment correction count; if over max, `FAILED`.
4. Else `AWAITING_CORRECTION`, `workflow.Await` on the correction channel, apply payload, retry the same activity.

### Signals (`signals.go`)

| Function | What it does |
|----------|----------------|
| `NewSignalConsumer` | Map of signal name → start-fn. |
| `On[T]` | Stores a function that starts `runAsync` for `signalName`. |
| `runAsync[T]` | Workflow goroutine: receive `T` forever and call `handler`. Logs handler errors. |

`On` only registers; you must invoke `sc.handlers[name](ctx)` (or equivalent) so the goroutine actually starts.

### Nexus (`nexus/`)

`NewNexus`, `RegisterWorkflow`, and `ExecuteNexus` are stubs. Do not use them in production yet.

---

## Activity options

Pass `*workflow.ActivityOptions` on each transition. Typical fields:

- `StartToCloseTimeout` — required in practice; resumable steps default to 30s if unset
- `RetryPolicy` — bounded `MaximumAttempts` on resumable steps so parking can happen; Temporal’s default retries can run for a long time

---

## End-to-end example

```go
type OrderState struct {
    Activity  string `json:"activity"`
    Account   string `json:"account"`
    Amount    float64
    status    string
}

func (s *OrderState) SetActivity(name string) { s.Activity = name }
func (s *OrderState) GetActivity() string     { return s.Activity }
func (s *OrderState) JSONString() (string, error) {
    b, err := json.Marshal(s)
    return string(b), err
}
func (s *OrderState) SetStatus(st string) { s.status = st }
func (s *OrderState) GetStatus() string   { return s.status }
func (s *OrderState) ApplyCorrection(payload json.RawMessage) error {
    var account string
    if err := json.Unmarshal(payload, &account); err != nil {
        return err
    }
    s.Account = account
    return nil
}

func TransferActivity(ctx context.Context, s *OrderState) (*OrderState, error) {
    if s.Account == "" {
        return nil, temporal_client.NewNonRetryableError("InvalidAccount", "account required")
    }
    return s, nil
}

func OrderWorkflow(ctx workflow.Context, state *OrderState) error {
    return exec.Execute(ctx, state)
}
```

Register `OrderWorkflow` and `TransferActivity` as shown in [Client setup](#client-setup), start a worker, then:

```go
run, err := tc.StartWorkflow(ctx, temporal_client.StartWorkflowOptions{
    WorkflowID: "order-1",
}, "OrderWorkflow", &OrderState{Amount: 50})
```
