package dlq

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/igetayor/telex/internal/logger"
	"github.com/igetayor/telex/internal/metrics"
	"github.com/igetayor/telex/internal/models"
)

// Manager orchestrates Dead-Letter Queue routing, persistence, and replay.
type Manager struct {
	messages map[string]models.DLQMessage
	order    []string
	mu       sync.RWMutex
	maxCap   int
}

// NewManager creates a new thread-safe DLQ Manager.
func NewManager(maxCapacity int) *Manager {
	if maxCapacity <= 0 {
		maxCapacity = 500
	}
	return &Manager{
		messages: make(map[string]models.DLQMessage),
		order:    make([]string, 0, maxCapacity),
		maxCap:   maxCapacity,
	}
}

// RouteToDLQ logs, captures, and routes a failed event to the Dead-Letter Queue.
func (m *Manager) RouteToDLQ(ctx context.Context, event models.Event, reason string, err error) models.DLQMessage {
	event.Status = models.StatusDeadLetter

	errDetails := ""
	if err != nil {
		errDetails = err.Error()
	}

	dlqMsg := models.DLQMessage{
		Event:            event,
		Reason:           reason,
		FailureTimestamp: time.Now(),
		FailedAtAttempt:  event.Attempt,
		ErrorDetails:     errDetails,
		Replayed:         false,
	}

	m.mu.Lock()
	// Maintain bounded FIFO capacity
	if len(m.order) >= m.maxCap {
		oldestID := m.order[0]
		m.order = m.order[1:]
		delete(m.messages, oldestID)
	}
	m.messages[event.ID] = dlqMsg
	m.order = append(m.order, event.ID)
	count := len(m.messages)
	m.mu.Unlock()

	// Metrics instrumentation
	metrics.Get().DLQMessagesTotal.WithLabelValues(event.Topic, reason).Inc()
	metrics.Get().DLQDepthGauge.Set(float64(count))

	logger.FromContext(ctx).Error("Event routed to Dead-Letter Queue (DLQ)",
		slog.String("event_id", event.ID),
		slog.String("topic", event.Topic),
		slog.Int("failed_at_attempt", event.Attempt),
		slog.String("reason", reason),
		slog.String("error_details", errDetails))

	return dlqMsg
}

// GetMessages returns a snapshot list of DLQ messages up to limit.
func (m *Manager) GetMessages(limit int) []models.DLQMessage {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if limit <= 0 || limit > len(m.order) {
		limit = len(m.order)
	}

	result := make([]models.DLQMessage, 0, limit)
	// Iterate in reverse for most recent first
	for i := len(m.order) - 1; i >= 0 && len(result) < limit; i-- {
		id := m.order[i]
		if msg, ok := m.messages[id]; ok {
			result = append(result, msg)
		}
	}
	return result
}

// GetMessage retrieves a specific DLQ message by Event ID.
func (m *Manager) GetMessage(eventID string) (*models.DLQMessage, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	msg, ok := m.messages[eventID]
	if !ok {
		return nil, false
	}
	return &msg, true
}

// Replay marks a DLQ message as replayed and prepares an event for reprocessing.
func (m *Manager) Replay(eventID string) (*models.Event, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	msg, exists := m.messages[eventID]
	if !exists {
		return nil, fmt.Errorf("DLQ message with ID %s not found", eventID)
	}

	msg.Replayed = true
	msg.ReplayedAt = time.Now()
	m.messages[eventID] = msg

	// Clone event and reset attempt counter for clean retry cycle
	replayedEvent := msg.Event
	replayedEvent.Attempt = 0
	replayedEvent.Status = models.StatusPending
	replayedEvent.ErrorChain = append(replayedEvent.ErrorChain, fmt.Sprintf("Replayed from DLQ at %s", time.Now().Format(time.RFC3339)))

	return &replayedEvent, nil
}

// Count returns the number of dead-lettered events currently retained.
func (m *Manager) Count() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.messages)
}

// Clear flushes the DLQ buffer.
func (m *Manager) Clear() {
	m.mu.Lock()
	m.messages = make(map[string]models.DLQMessage)
	m.order = make([]string, 0, m.maxCap)
	m.mu.Unlock()

	metrics.Get().DLQDepthGauge.Set(0)
}
