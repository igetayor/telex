package controllers

import (
	"encoding/json"
	"net/http"
	"strconv"

	"github.com/igetayor/telex/internal/api/services"
)

// DLQController handles Dead-Letter Queue management and message replays.
type DLQController struct {
	service *services.EventService
}

// NewDLQController creates a DLQController instance.
func NewDLQController(s *services.EventService) *DLQController {
	return &DLQController{service: s}
}

// HandleDLQ returns dead-lettered messages and high-level DLQ stats.
func (c *DLQController) HandleDLQ(w http.ResponseWriter, r *http.Request) {
	limitStr := r.URL.Query().Get("limit")
	limit := 50
	if l, err := strconv.Atoi(limitStr); err == nil && l > 0 {
		limit = l
	}

	messages := c.service.GetDLQMessages(limit)
	stats := c.service.GetDLQStats()

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"messages": messages,
		"stats":    stats,
		"count":    len(messages),
	})
}

// HandleDLQReplay triggers replay of dead-lettered messages back into the worker pool.
func (c *DLQController) HandleDLQReplay(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req struct {
		MessageIDs []string `json:"message_ids"`
	}

	_ = json.NewDecoder(r.Body).Decode(&req)

	replayed, err := c.service.ReplayDLQ(r.Context(), req.MessageIDs)
	if err != nil {
		http.Error(w, "Failed to replay messages: "+err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"status":   "replayed",
		"replayed": replayed,
	})
}
