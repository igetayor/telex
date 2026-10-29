// Package services provides business logic and domain orchestration for Telex events and DLQ.
package services

import (
	"context"
	"encoding/json"
	"time"

	"github.com/google/uuid"
	"github.com/igetayor/telex/internal/dlq"
	"github.com/igetayor/telex/internal/models"
	"github.com/igetayor/telex/internal/queue"
	"github.com/igetayor/telex/internal/worker"
)

// EventService orchestrates event publishing, worker pool telemetry, and DLQ replay.
type EventService struct {
	pool     *worker.Pool
	dlqMgr   *dlq.Manager
	consumer queue.Consumer
}

// NewEventService creates a new EventService instance.
func NewEventService(pool *worker.Pool, dlqMgr *dlq.Manager, consumer queue.Consumer) *EventService {
	return &EventService{
		pool:     pool,
		dlqMgr:   dlqMgr,
		consumer: consumer,
	}
}

// GetStats returns live worker pool and broker metrics.
func (s *EventService) GetStats() models.WorkerPoolStats {
	return s.pool.GetStats()
}

// GetRecentEvents fetches recent events up to limit.
func (s *EventService) GetRecentEvents(limit int) []models.Event {
	return s.pool.GetRecentEvents(limit)
}

// PublishEvent produces an event to the worker pool queue.
func (s *EventService) PublishEvent(ctx context.Context, topic, evtType string, payload json.RawMessage, simulateError string) (*models.Event, error) {
	if topic == "" {
		topic = "default"
	}
	if evtType == "" {
		evtType = "generic.event"
	}

	evt := models.Event{
		ID:            uuid.New().String(),
		Topic:         topic,
		Type:          evtType,
		Payload:       payload,
		SimulateError: simulateError,
		Status:        models.StatusPending,
		CreatedAt:     time.Now().UTC(),
	}

	s.pool.Enqueue(evt)
	return &evt, nil
}

// GetDLQMessages retrieves messages from the Dead-Letter Queue.
func (s *EventService) GetDLQMessages(limit int) []models.DLQMessage {
	return s.dlqMgr.GetMessages(limit)
}

// GetDLQStats returns summary counts for DLQ state.
func (s *EventService) GetDLQStats() map[string]any {
	count := s.dlqMgr.Count()
	return map[string]any{
		"depth": count,
	}
}

// ReplayDLQ replays dead-lettered messages back into the worker pool.
func (s *EventService) ReplayDLQ(ctx context.Context, messageIDs []string) (int, error) {
	replayedCount := 0
	for _, id := range messageIDs {
		replayedEvt, err := s.dlqMgr.Replay(id)
		if err != nil {
			continue
		}
		s.pool.Enqueue(*replayedEvt)
		replayedCount++
	}
	return replayedCount, nil
}
