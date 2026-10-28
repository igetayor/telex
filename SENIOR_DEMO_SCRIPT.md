# Senior Engineering Walkthrough: Telex Event Streaming Worker

> **Target Audience:** Principal Systems Engineers, Messaging Infrastructure Leads, Enterprise Event-Driven Architects.  
> **Key Architecture Highlights:** Bounded Goroutine Worker Pools, AMQP 0-9-1 Protocol / RabbitMQ, Exponential Backoff with Decorrelated Full Jitter, Dead-Letter Queue (DLQ) Quarantine, Prometheus `/metrics`.

---

## 1. Executive Summary & Problem Space

In asynchronous message-driven microservices, traffic surges often overwhelm downstream databases and external APIs if message consumption is unbound. Naively spawning a goroutine per message leads to runaway memory usage, thread exhaustion, and cascading database connection pool crashes. Furthermore, unhandled transient errors cause messages to be dropped or get stuck in poison-pill retry loops.

**Telex** implements a hardened stream consumption architecture in Go:
- **Bounded Worker Pool:** Fixed number of worker goroutines consuming from a pre-allocated buffered channel.
- **Prefetch Tuning:** RabbitMQ prefetch QoS matches worker concurrency, enforcing hardware backpressure.
- **Full Jitter Exponential Backoff:** Prevents thundering herds when retrying downstream operations.
- **Dead-Letter Queue (DLQ):** Poison-pill payloads exceeding retry limits are quarantined into an auditable DLQ with error context.

---

## 2. File & Function Code Architecture

### `internal/worker/pool.go` — Bounded Ingestion Pipeline
- `type Pool struct`: Coordinates worker goroutines, the buffered task channel, and graceful drain synchronization (`sync.WaitGroup`).
- `NewPool(workers int, queueCapacity int) *Pool`: Allocates the worker pool.
- `(p *Pool) Start(ctx context.Context)`: Spawns $N$ continuous consumer goroutines.
- `(p *Pool) Submit(event Event) bool`: Enqueues an incoming event with non-blocking backpressure handling.

### `internal/retry/backoff.go` — Resilience & Jitter
- `CalculateBackoff(attempt int, base time.Duration, max time.Duration) time.Duration`: Computes exponential backoff with full decorrelated jitter to avoid synchronized retry stampedes.

### `internal/dlq/store.go` — Dead-Letter Quarantine
- `type DLQStore struct`: In-memory and disk-backed store capturing exhausted tasks, stack traces, and failure reasons for administrative replay.

### `internal/api/router.go` & `web/` — HTTP Gateway (Port 8082)
- Exposes Prometheus `/metrics`, `/healthz`, `/api/v1/dlq` for replaying quarantined messages, and serves the dark-mode dashboard at `GET /`.

---

## 3. How to Run the Live Demo

### Starting the Server (Port 8082)
```powershell
.\telex.exe
# or: go run ./cmd/server
```
Open your browser to: **http://127.0.0.1:8082**

### Demo Walkthrough Script
1. **Show the Metrics:** Point out the active worker pool utilization gauge, ingested message throughput, and DLQ counter.
2. **Dispatch Healthy Event Stream:** Use the built-in chaos/injection panel to dispatch 100 successful events. Show that the fixed pool of 8 workers processes them with zero memory growth.
3. **Inject Downstream Chaos (503 / Flakiness):** Trigger simulated upstream failures. Watch workers initiate exponential jittered retries.
4. **Exhausted Retries to DLQ:** Show a poison-pill payload fail 3 times and move into the Dead-Letter Queue.
5. **Inspect & Replay:** Open the DLQ viewer, inspect the JSON payload and error trace, and click **Replay** to re-inject it into the stream once downstream health is restored.
