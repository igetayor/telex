# ⚡ Telex | Resilient Event Streaming Worker Service

[![Go Version](https://img.shields.io/badge/Go-1.24%2B-00ADD8?style=flat&logo=go)](https://go.dev)
[![Architecture](https://img.shields.io/badge/Concurrency-Bounded%20Worker%20Pools-6366F1?style=flat)]()
[![Broker](https://img.shields.io/badge/Message%20Broker-RabbitMQ%20%2F%20AMQP-FF6600?style=flat&logo=rabbitmq)](https://rabbitmq.com)
[![Observability](https://img.shields.io/badge/Metrics-Prometheus%20Scrape-E6522C?style=flat&logo=prometheus)](https://prometheus.io)
[![Resilience](https://img.shields.io/badge/Resilience-Exponential%20Backoff%20%26%20DLQ-10B981?style=flat)]()

> A robust, high-performance event streaming worker service in Go. Features bounded concurrency worker pools, exponential backoff retries with decorrelated full jitter, dead-letter queue (DLQ) routing, graceful signal draining (`SIGTERM`), Prometheus `/metrics` instrumentation, and an embedded modern dark-mode dashboard with interactive chaos failure injection and DLQ replay.

---

## 🏛️ Clean Architecture Overview

Telex follows the **Pipeline & Worker Pool Concurrency Pattern**, completely decoupling message consumption from worker task execution via bounded channels.

```
┌───────────────────────────────────────────────────────────────────────────────────┐
│                      TELEX RESILIENT EVENT STREAMING PIPELINE                     │
└───────────────────────────────────────────────────────────────────────────────────┘

 [ RabbitMQ (AMQP 0-9-1) / Kafka Broker ]
                 │
                 │ (QoS Prefetch: 16)
                 ▼
     ┌───────────────────────┐
     │ Stream Message Consumer
     └───────────┬───────────┘
                 │
                 ▼
     ┌─────────────────────────────────────────────────┐
     │ Bounded Ingestion Queue (Capacity: 1,000 events)│
     └───────────┬────────────────────┬────────────────┘
                 │                    │
                 ▼                    ▼
          ┌─────────────┐      ┌─────────────┐
          │  Worker 1   │      │  Worker N   │  (Fixed Goroutine Pool)
          └──────┬──────┘      └──────┬──────┘
                 │                    │
                 ▼                    ▼
         [ Payload Processing & Task Execution ]
                 │
                 ├───▶ Success (HTTP 200 Ack, Latency Histogram)
                 │
                 ├───▶ Transient Error ──▶ Exponential Backoff (Full Jitter) ──▶ Re-enqueue
                 │
                 └───▶ Fatal Error / Max Retries Exceeded ──▶ Dead-Letter Queue (DLQ)
```

---

## ⚙️ Core Technical Highlights

### 1. Bounded Concurrency Worker Pools
* **Fixed Goroutine Allocation:** Spawns a pre-allocated pool of worker goroutines (`NUM_WORKERS=8`), preventing unbounded thread allocation or memory exhaustion during traffic spikes.
* **Bounded Ingestion Channel:** Incoming events enter a buffered channel (`QUEUE_CAPACITY=1000`). If downstream workers lag, backpressure triggers rather than crashing the runtime.
* **Prefetch Control:** RabbitMQ channel is tuned with `ch.Qos(prefetchCount, 0, false)` to match worker concurrency.

### 2. Exponential Backoff with Decorrelated Full Jitter
Transient failures (downstream network glitches, rate-limit throttles) are automatically retried with exponential backoff:
$$\text{delay} = \min\left(\text{max\_backoff},\, \text{base\_backoff} \times \text{factor}^{(\text{attempt} - 1)}\right)$$
$$\text{jittered\_delay} = \frac{\text{delay}}{2} + \text{rand}\left(0, \frac{\text{delay}}{2}\right)$$
Full Jitter prevents "thundering-herd" retry spikes from overwhelming downstream recovery services.

### 3. Dead-Letter Queue (DLQ) Routing & Replay
* **Unrecoverable Failures:** Non-retryable syntax errors, validation failures, and poison pills bypass retry cycles and are sent immediately to the DLQ.
* **Retries Exhausted:** Events that fail repeatedly up to `MAX_RETRIES` are enriched with rejection cause, timestamp, and failure stack trace, then routed to the DLQ.
* **One-Click Replay:** The DLQ API (`POST /api/v1/dlq/replay`) and web dashboard allow operators to inspect failed payloads and re-enqueue them into the worker pool.

### 4. Graceful Shutdown (`SIGTERM` / `SIGINT`)
* Traps OS signals via `signal.NotifyContext`.
* Closes consumer connections to stop pulling new messages.
* Drains in-flight worker tasks with a configurable grace period (`GRACEFUL_TIMEOUT=10s`).
* Flushes Prometheus metrics and exits with exit code `0`.

---

## 📊 Prometheus `/metrics` Instrumentation

Exposed at `http://localhost:8082/metrics` for Prometheus scraping:

| Metric Name | Type | Labels | Description |
| :--- | :--- | :--- | :--- |
| `telex_worker_messages_consumed_total` | Counter | `topic, type` | Total stream messages ingested from queue |
| `telex_worker_message_processing_duration_seconds` | Histogram | `topic, status` | Latency distribution of event payload execution |
| `telex_worker_message_errors_total` | Counter | `topic, error_type` | Total worker failures by error category |
| `telex_worker_message_retries_total` | Counter | `topic, attempt` | Total retries scheduled with exponential backoff |
| `telex_worker_dlq_messages_total` | Counter | `topic, reason` | Messages routed to Dead-Letter Queue |
| `telex_worker_workers_active` | Gauge | - | Current active worker goroutines |
| `telex_worker_queue_depth` | Gauge | - | In-flight events waiting in bounded queue |
| `telex_worker_dlq_depth` | Gauge | - | Total dead-letter messages currently retained |

---

## 🎨 Dual-Purpose Frontend & Interactive Documentation

Telex embeds a single-page management dashboard at `http://localhost:8082/`:

1. **Live Concurrency & Queue Saturation:**
   - Real-time gauges for Active Workers (`X / 8 Active`), Bounded Queue Depth, Success / Retry / DLQ counters, and throughput (events/sec).
2. **Real-Time Event Stream:**
   - Live stream feed showing events transitioning from `PENDING` -> `PROCESSING` -> `SUCCEEDED` / `RETRYING` / `DEAD LETTER`.
3. **Interactive Chaos & Fault Injector:**
   - **Healthy Event:** Processes cleanly on Attempt 1 within 25ms.
   - **Transient Error:** Fails twice, triggers exponential backoff, recovers on Attempt 3.
   - **Fatal Poison Pill:** Bypasses retry loop and routes directly to DLQ.
   - **Timeout Failure:** Exhausts max retries then routes to DLQ.
4. **Dead-Letter Queue (DLQ) Inspector & Replay Console:**
   - Inspect dead-lettered payloads, error traces, and trigger replays with 1 click.
5. **Architecture & Prometheus Reference:**
   - Full pipeline diagram, retry formula, metric definitions, and sample scrape configs.

---

## 🌐 REST Management Endpoints

| Method | Path | Description |
| :--- | :--- | :--- |
| `GET` | `/metrics` | Prometheus metrics scrape endpoint |
| `GET` | `/healthz` | Health check probe (uptime, workers, broker connectivity) |
| `GET` | `/api/v1/stats` | Real-time worker pool and queue depth telemetry |
| `GET` | `/api/v1/events/recent` | Recent processed events list for streaming UI |
| `POST` | `/api/v1/events/publish` | Enqueue a custom event with optional simulated error flag |
| `GET` | `/api/v1/dlq` | List all dead-lettered messages |
| `POST` | `/api/v1/dlq/replay` | Re-queue a dead-letter message back into worker pool |
| `DELETE` | `/api/v1/dlq` | Flush all DLQ messages |

---

## 🚀 Quickstart Guide

### 1. Run Locally with Go
```bash
# Navigate to directory
cd c:\Users\HP\Documents\Exquisapps\ID's\igeTayor\telex

# Run test suite
go test -v ./...

# Start worker (runs with built-in resilient stream generator if RabbitMQ is not running)
go run ./cmd/worker
```
Visit **http://localhost:8082** in your browser to view the live dashboard and documentation!

### 2. Full Stack with Docker Compose
Spin up RabbitMQ (with Web UI), Prometheus, and Telex worker:
```bash
docker compose up --build
```
* **Telex Worker UI & Docs:** http://localhost:8082
* **Prometheus Dashboard:** http://localhost:9090
* **RabbitMQ Management Console:** http://localhost:15672 (guest / guest)

---

## 🧪 Testing & Verification

Comprehensive unit tests are provided in `internal/worker/pool_test.go`, `internal/retry/backoff_test.go`, and `internal/dlq/dlq_test.go`:
```bash
# Run all tests
go test -v ./...

# Run with race detector
go test -race -v ./...
```

**Test Coverage Highlights:**
* `TestWorkerPool_SuccessfulExecution`: Verifies bounded concurrent processing and metric counters.
* `TestWorkerPool_TransientRetryRecovery`: Verifies transient errors trigger backoff delays and recover successfully.
* `TestWorkerPool_FatalErrorDLQRouting`: Verifies fatal poison pills bypass retries and go directly to DLQ.
* `TestWorkerPool_GracefulShutdown`: Verifies in-flight drain and clean termination.
* `TestBackoffPolicy_CalculateDelay`: Asserts exponential progression, max caps, and jitter bounds.
* `TestDLQManager_RoutingAndReplay`: Verifies DLQ capacity bounding, message retrieval, and replay re-enqueueing.

---

## 📁 Repository Structure

```
telex/
├── cmd/
│   └── worker/
│       └── main.go          # Worker entrypoint
├── internal/
│   ├── app/
│   │   └── app.go           # Core runner, signal handling, and shutdown orchestration
│   ├── api/
│   │   └── routes.go        # HTTP endpoints, /metrics, /healthz, DLQ management
│   ├── config/
│   │   └── config.go        # Concurrency, queue capacity, and retry configuration
│   ├── dlq/
│   │   ├── dlq.go           # Dead-Letter Queue manager and replay logic
│   │   └── dlq_test.go      # DLQ routing and replay unit tests
│   ├── logger/
│   │   └── logger.go        # Structured slog JSON logger with context trace IDs
│   ├── metrics/
│   │   └── metrics.go       # Prometheus metrics collectors (histograms, counters, gauges)
│   ├── models/
│   │   └── event.go         # Event, DLQMessage, ProcessingResult data models
│   ├── queue/
│   │   └── consumer.go      # RabbitMQ AMQP consumer and simulated stream generator
│   ├── retry/
│   │   ├── backoff.go       # Exponential backoff calculator with full jitter
│   │   └── backoff_test.go  # Backoff calculation and retry eligibility tests
│   └── worker/
│       ├── pool.go          # Bounded concurrent worker pool & task dispatcher
│       └── pool_test.go     # Worker pool execution, recovery, and DLQ tests
├── web/
│   ├── embed.go             # Go embed.FS declaration
│   ├── index.html           # Live worker dashboard, DLQ console, and docs
│   ├── style.css            # Dark glassmorphic design system
│   └── app.js               # Client console, chaos injector, and metrics polling
├── Dockerfile               # Multi-stage production container with non-root user
├── docker-compose.yml       # RabbitMQ, Prometheus, and Telex worker orchestration
├── prometheus.yml           # Prometheus scrape configuration
├── go.mod                   # Go module definitions
├── go.sum                   # Checksums
├── main.go                  # Root runner forwarder
└── README.md                # Clean architecture documentation
```

---

Built with pride by the **Exquisapps** team.
