package dlq_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/igetayor/telex/internal/dlq"
	"github.com/igetayor/telex/internal/models"
)

func TestDLQManager_RoutingAndReplay(t *testing.T) {
	mgr := dlq.NewManager(10)
	ctx := context.Background()

	ev := models.Event{
		ID:        "test-event-101",
		Topic:     "test.topic",
		Type:      "order.created",
		Attempt:   3,
		CreatedAt: time.Now(),
		Status:    models.StatusProcessing,
	}

	dlqMsg := mgr.RouteToDLQ(ctx, ev, "max_retries_exceeded", errors.New("database locked"))
	if dlqMsg.Reason != "max_retries_exceeded" {
		t.Fatalf("Expected reason 'max_retries_exceeded', got '%s'", dlqMsg.Reason)
	}
	if mgr.Count() != 1 {
		t.Fatalf("Expected 1 message in DLQ, got %d", mgr.Count())
	}

	// Verify retrieval
	retrieved, found := mgr.GetMessage("test-event-101")
	if !found || retrieved.Event.ID != "test-event-101" {
		t.Fatalf("Failed to retrieve DLQ message")
	}

	// Replay
	replayed, err := mgr.Replay("test-event-101")
	if err != nil {
		t.Fatalf("Replay failed: %v", err)
	}
	if replayed.Attempt != 0 {
		t.Fatalf("Expected replayed attempt to be reset to 0, got %d", replayed.Attempt)
	}
	if replayed.Status != models.StatusPending {
		t.Fatalf("Expected replayed status to be 'pending', got '%s'", replayed.Status)
	}

	// Clear
	mgr.Clear()
	if mgr.Count() != 0 {
		t.Fatalf("Expected DLQ to be empty after clear, got %d", mgr.Count())
	}
}
