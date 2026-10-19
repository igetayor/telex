package worker_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/igetayor/telex/internal/config"
	"github.com/igetayor/telex/internal/dlq"
	"github.com/igetayor/telex/internal/models"
	"github.com/igetayor/telex/internal/queue"
	"github.com/igetayor/telex/internal/worker"
)

func createTestConfig() *config.Config {
	return &config.Config{
		Port:            ":8082",
		ServiceName:     "telex-test-worker",
		NumWorkers:      4,
		QueueCapacity:   100,
		MaxRetries:      3,
		BaseBackoff:     20 * time.Millisecond,
		MaxBackoff:      100 * time.Millisecond,
		BackoffFactor:   2.0,
		TaskTimeout:     2 * time.Second,
		GracefulTimeout: 3 * time.Second,
		LogLevel:        "ERROR",
		SimIntervalMS:   5 * time.Second, // Long interval during unit tests
	}
}

// TestWorkerPool_SuccessfulExecution tests normal message processing through the worker pool.
func TestWorkerPool_SuccessfulExecution(t *testing.T) {
	cfg := createTestConfig()
	dlqMgr := dlq.NewManager(100)
	simConsumer := queue.NewSimulatedStreamConsumer(cfg)

	pool := worker.NewPool(cfg, simConsumer, dlqMgr)
	if err := pool.Start(); err != nil {
		t.Fatalf("Failed to start worker pool: %v", err)
	}
	defer func() {
		_ = pool.Stop(1 * time.Second)
	}()

	// Enqueue 5 healthy events
	for i := 0; i < 5; i++ {
		ev := models.Event{
			ID:         uuid.New().String(),
			Topic:      "orders",
			Type:       "order.created",
			TraceID:    uuid.New().String(),
			Payload:    json.RawMessage(`{"item":"book"}`),
			MaxRetries: 3,
			CreatedAt:  time.Now(),
		}
		pool.Enqueue(ev)
	}

	// Wait for processing
	time.Sleep(250 * time.Millisecond)

	stats := pool.GetStats()
	if stats.MessagesSuccess < 5 {
		t.Fatalf("Expected at least 5 successful messages, got %d", stats.MessagesSuccess)
	}
	if stats.MessagesDLQ != 0 {
		t.Fatalf("Expected 0 DLQ messages, got %d", stats.MessagesDLQ)
	}
}

// TestWorkerPool_TransientRetryRecovery tests transient errors that trigger backoff and recover.
func TestWorkerPool_TransientRetryRecovery(t *testing.T) {
	cfg := createTestConfig()
	dlqMgr := dlq.NewManager(100)
	simConsumer := queue.NewSimulatedStreamConsumer(cfg)

	pool := worker.NewPool(cfg, simConsumer, dlqMgr)
	_ = pool.Start()
	defer func() {
		_ = pool.Stop(1 * time.Second)
	}()

	// Enqueue transient failure event (designed to fail on Attempt 0 and 1, succeed on Attempt 2)
	ev := models.Event{
		ID:            uuid.New().String(),
		Topic:         "payments",
		Type:          "payment.charge",
		TraceID:       uuid.New().String(),
		Payload:       json.RawMessage(`{"charge_id":"ch_123"}`),
		SimulateError: models.SimulateTransientError,
		MaxRetries:    3,
		CreatedAt:     time.Now(),
	}

	pool.Enqueue(ev)

	// Wait for exponential backoff retries to complete
	time.Sleep(300 * time.Millisecond)

	stats := pool.GetStats()
	if stats.MessagesRetried == 0 {
		t.Fatalf("Expected message to be retried with exponential backoff")
	}
	if stats.MessagesSuccess == 0 {
		t.Fatalf("Expected event to eventually recover and succeed")
	}
	if stats.MessagesDLQ != 0 {
		t.Fatalf("Expected event NOT to go to DLQ because it recovered, got %d DLQ", stats.MessagesDLQ)
	}
}

// TestWorkerPool_FatalErrorDLQRouting tests poison pills routed directly to DLQ.
func TestWorkerPool_FatalErrorDLQRouting(t *testing.T) {
	cfg := createTestConfig()
	dlqMgr := dlq.NewManager(100)
	simConsumer := queue.NewSimulatedStreamConsumer(cfg)

	pool := worker.NewPool(cfg, simConsumer, dlqMgr)
	_ = pool.Start()
	defer func() {
		_ = pool.Stop(1 * time.Second)
	}()

	eventID := uuid.New().String()
	ev := models.Event{
		ID:            eventID,
		Topic:         "security",
		Type:          "auth.token.inspect",
		TraceID:       uuid.New().String(),
		Payload:       json.RawMessage(`{"poison":true}`),
		SimulateError: models.SimulateFatalError,
		MaxRetries:    3,
		CreatedAt:     time.Now(),
	}

	pool.Enqueue(ev)

	time.Sleep(100 * time.Millisecond)

	stats := pool.GetStats()
	if stats.MessagesDLQ != 1 {
		t.Fatalf("Expected 1 DLQ message, got %d", stats.MessagesDLQ)
	}

	dlqMsg, found := dlqMgr.GetMessage(eventID)
	if !found {
		t.Fatalf("Expected event %s to exist in DLQ manager", eventID)
	}
	if dlqMsg.Reason != "unrecoverable_poison_pill" {
		t.Fatalf("Expected reason 'unrecoverable_poison_pill', got '%s'", dlqMsg.Reason)
	}
}

// TestWorkerPool_GracefulShutdown verifies clean worker drain during SIGTERM.
func TestWorkerPool_GracefulShutdown(t *testing.T) {
	cfg := createTestConfig()
	dlqMgr := dlq.NewManager(100)
	simConsumer := queue.NewSimulatedStreamConsumer(cfg)

	pool := worker.NewPool(cfg, simConsumer, dlqMgr)
	_ = pool.Start()

	// Enqueue tasks
	for i := 0; i < 10; i++ {
		pool.Enqueue(models.Event{
			ID:        uuid.New().String(),
			Topic:     "test",
			Type:      "drain.test",
			CreatedAt: time.Now(),
		})
	}

	// Trigger graceful stop
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	err := pool.Stop(1 * time.Second)
	if err != nil {
		t.Fatalf("Graceful stop failed: %v", err)
	}

	_ = ctx
}
