# go-cook

Shared Go infrastructure library for building backend services. **go-cook** centralizes common integrations—database access, caching, messaging, authentication, workflow orchestration, and scheduled jobs—so application services can import only what they need without reimplementing boilerplate.

Each package is an independent Go module under a single repository. Import the sub-modules you use; you do not need to depend on the root module.

## Requirements

- Go 1.26.1+

## Repository layout

```
go-cook/
├── db/          # PostgreSQL / MySQL client, generic repository helpers, migrations
├── redis/       # Redis client, JSON cache, rate limiting, geo operations
├── nsq/         # NSQ publisher and consumer
├── security/    # JWT access and refresh token generation
├── temporal/    # Temporal workflow client, activity pipelines, signals
└── cronjob/     # Cron-based job scheduler
```

| Package | Module path | Import alias (package name) |
|---------|-------------|-------------------------------|
| Database | `github.com/RandySteven/go-cook/db` | `db_client` |
| Redis | `github.com/RandySteven/go-cook/redis` | `redis_client` |
| NSQ | `github.com/RandySteven/go-cook/nsq` | `nsq_client` |
| Security | `github.com/RandySteven/go-cook/security` | `security` |
| Temporal | `github.com/RandySteven/go-cook/temporal` | `temporal_client` |
| Cronjob | `github.com/RandySteven/go-cook/cronjob` | `cronjob_client` |

## Installation

Add only the modules your service needs:

```bash
go get github.com/RandySteven/go-cook/db
go get github.com/RandySteven/go-cook/redis
go get github.com/RandySteven/go-cook/nsq
go get github.com/RandySteven/go-cook/security
go get github.com/RandySteven/go-cook/temporal
go get github.com/RandySteven/go-cook/cronjob
```

---

## Packages

### `db` — Database client and repository

**Responsibility:** PostgreSQL and MySQL connection management with pooling, health checks, generic CRUD helpers, and SQL migration execution.

**Key types**

- `DBConfig` — connection credentials and driver selection (`postgresql` or `mysql`)
- `DBClient` — interface for `Close`, `Ping`, and access to the underlying `*sql.DB`
- `Trigger` — abstraction over `PrepareContext`, `ExecContext`, `QueryContext`, `QueryRowContext` (used by repository helpers and transactions)
- `MigrationWorker` — registers and runs DDL/DML migration scripts

**Connection defaults**

| Setting | Value |
|---------|-------|
| Max idle connections | 10 |
| Max open connections | 8 |
| Connection max lifetime | 10 minutes |
| Connection max idle time | 8 minutes |

**Repository helpers** (generic, reflection-based):

| Function | Purpose |
|----------|---------|
| `Save[T]` | INSERT with prepared statement; returns last insert ID |
| `FindAll[T]` | SELECT all rows into a slice of structs |
| `FindByID[T]` | SELECT single row by ID |
| `Update[T]` | UPDATE with validation |
| `Delete[T]` | DELETE by table name and ID |
| `QueryValidation` | Ensures a query contains the expected SQL keyword |

**Example**

```go
import db_client "github.com/RandySteven/go-cook/db"

client, err := db_client.NewMYSQLClient(&db_client.DBConfig{
    Db:     "postgresql",
    DbUser: "user",
    DbPass: "pass",
    DbHost: "localhost:5432",
    DbName: "mydb",
})
if err != nil {
    log.Fatal(err)
}
defer client.Close()

id, err := db_client.Save[User](ctx, client.Client(), insertQuery, user.Name, user.Email)
```

---

### `redis` — Cache and rate limiting

**Responsibility:** Redis connection pooling, JSON-serialized key/value storage, distributed rate limiting, and geo-spatial operations.

**Key types**

- `RedisConfig` — host, port, password, pool sizing, timeouts, and retry settings
- `Redis` — interface covering cache operations, pub/sub, and geo commands

**Environment variables**

| Variable | Purpose |
|----------|---------|
| `REDIS_EXPIRATION` | Default cache TTL in seconds |
| `RATE_LIMITER` | Requests allowed per minute per client IP |

**Example**

```go
import redis_client "github.com/RandySteven/go-cook/redis"

rdb, err := redis_client.NewRedisClient(&redis_client.RedisConfig{
    Host:     "localhost",
    Port:     "6379",
    PoolSize: 50,
})
if err != nil {
    log.Fatal(err)
}

ok, err := rdb.Set(ctx, "user:1", user, time.Hour)
```

Rate limiting expects the caller to attach the client IP to the context:

```go
ctx := context.WithValue(ctx, "X-Client-IP", clientIP)
if err := redis_client.RateLimiter(ctx); err != nil {
    // rate limit exceeded
}
```

---

### `nsq` — Message queue

**Responsibility:** Publish messages to NSQ topics and register consumers with handler functions.

**Key types**

- `NSQConfig` — NSQD host, TCP port, and lookupd HTTP port
- `Nsq` — interface for `Publish`, `Consume`, and `RegisterConsumer`

**Example**

```go
import nsq_client "github.com/RandySteven/go-cook/nsq"

nsq, err := nsq_client.NewNsqClient(&nsq_client.NSQConfig{
    NSQDHost:        "localhost",
    NSQDTCPPort:     "4150",
    LookupdHttpPort: "4161",
})

// Publish
err = nsq.Publish(ctx, "orders", []byte(`{"id": 1}`))

// Consume
err = nsq.RegisterConsumer("orders", func(ctx context.Context, topic string) {
    body, _ := nsq.Consume(ctx, topic)
    // process body
})
```

Consumers connect to `nsqlookupd`, use a fixed channel name (`channel`), and requeue messages on handler failure. Each message is processed with a 30-second timeout context.

---

### `security` — JWT authentication

**Responsibility:** Generate HS256-signed access and refresh token pairs for user authentication.

**Key types**

- `JWTAccessClaim` — user ID, username, role IDs, verification status (1-hour expiry)
- `JWTRefreshClaim` — user ID and email (10-hour expiry)

**Environment variables**

| Variable | Purpose |
|----------|---------|
| `JWT_KEY` | Secret key used to sign tokens |

**Example**

```go
import "github.com/RandySteven/go-cook/security"

accessToken, refreshToken := security.GenerateTokens(userID, email)
```

---

### `temporal` — Workflow orchestration

**Responsibility:** Temporal client and worker setup, workflow lifecycle management, sequential activity pipelines with branching, and signal handling.

**Key types**

- `TemporalConfig` — server host/port, namespace, task queue, and worker concurrency options
- `Temporal` — register workflows/activities, start/signal/query/cancel workflows, start/stop worker
- `WorkflowExecution` / `WorkflowExecutionData` — build activity pipelines with transitions, child workflows, and signal coordination
- `NavigatableActivity` — state interface for branching between activities at runtime
- `SignalConsumer` — register typed async signal handlers inside workflows

**Example — client setup**

```go
import temporal_client "github.com/RandySteven/go-cook/temporal"

tc, err := temporal_client.NewTemporalClient(&temporal_client.TemporalConfig{
    Host:      "localhost",
    Port:      "7233",
    Namespace: "default",
    TaskQueue: "my-queue",
})

tc.RegisterWorkflow(temporal_client.WorkflowDefinition{
    Name: "OrderWorkflow",
    Fn:   OrderWorkflowFn,
})
tc.RegisterActivity(temporal_client.ActivityDefinition{
    Name: "ProcessPayment",
    Fn:   ProcessPaymentActivity,
})

if err := tc.Start(); err != nil {
    log.Fatal(err)
}
defer tc.Stop()
```

**Example — activity pipeline**

```go
exec := temporal_client.NewWorkflowExecution(tc)

exec.AddTransitionActivityWithOptions(
    "validate", "", ValidateActivity, nil,
    "process", "cancel",
)
exec.AddTransitionActivityWithOptions(
    "process", "payment-done", ProcessActivity, nil,
)

// Inside a workflow function:
err := exec.Execute(ctx, &orderState)
```

For local development, start Temporal with the expected namespace:

```bash
temporal server start-dev --ui-port 8080 -n <your-namespace>
```

---

### `cronjob` — Scheduled jobs

**Responsibility:** Cron scheduler with second-level precision and timezone support.

**Key types**

- `JobConfig` — timezone via `LoadLocation` (e.g. `"Asia/Jakarta"`)
- `Scheduler` — `Run` starts the scheduler; `Stop` performs a graceful shutdown

**Example**

```go
import cronjob_client "github.com/RandySteven/go-cook/cronjob"

sched, err := cronjob_client.NewScheduler(&cronjob_client.JobConfig{
    LoadLocation: "UTC",
})
if err != nil {
    log.Fatal(err)
}

// Add jobs via the underlying cron instance after extending the scheduler,
// or register entries before calling Run.
sched.Run(ctx)
defer sched.Stop(ctx)
```

---

## Design principles

- **Modular imports** — Each integration is a separate Go module. Services depend only on the packages they use, keeping dependency graphs lean.
- **Interface-first** — Core types (`DBClient`, `Redis`, `Nsq`, `Temporal`, `Scheduler`) are defined as interfaces, making it straightforward to mock in tests.
- **Configuration via structs** — Connection and client settings are passed through typed config structs rather than global state (except environment-backed secrets like `JWT_KEY`).
- **Generic repository helpers** — The `db` package uses Go generics and reflection to reduce repetitive SQL boilerplate across services.

## Environment variables summary

| Variable | Package | Required |
|----------|---------|----------|
| `JWT_KEY` | security | Yes (for token signing) |
| `REDIS_EXPIRATION` | redis | No (defaults to 0) |
| `RATE_LIMITER` | redis | No (defaults to 0) |

## License

See repository license file for details.
