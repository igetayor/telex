// Package controllers contains HTTP adapters for Telex endpoints.
package controllers

import (
	"encoding/json"
	"net/http"
	"strconv"

	"github.com/igetayor/telex/internal/api/services"
)

// EventController handles publishing and querying event streams.
type EventController struct {
	service *services.EventService
}

// NewEventController creates an EventController instance.
func NewEventController(s *services.EventService) *EventController {
	return &EventController{service: s}
}

// HandleRecentEvents returns recent events from the pipeline.
func (c *EventController) HandleRecentEvents(w http.ResponseWriter, r *http.Request) {
	limitStr := r.URL.Query().Get("limit")
	limit := 50
	if l, err := strconv.Atoi(limitStr); err == nil && l > 0 {
		limit = l
	}

	events := c.service.GetRecentEvents(limit)
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"events": events,
		"count":  len(events),
	})
}

// HandlePublishEvent accepts an event payload and queues it for async processing.
func (c *EventController) HandlePublishEvent(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req struct {
		Topic         string          `json:"topic"`
		Type          string          `json:"type"`
		Payload       json.RawMessage `json:"payload"`
		SimulateError string          `json:"simulate_error"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid JSON payload: "+err.Error(), http.StatusBadRequest)
		return
	}

	evt, err := c.service.PublishEvent(r.Context(), req.Topic, req.Type, req.Payload, req.SimulateError)
	if err != nil {
		http.Error(w, "Failed to enqueue event: "+err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"status":   "queued",
		"event_id": evt.ID,
		"topic":    evt.Topic,
		"type":     evt.Type,
	})
}
