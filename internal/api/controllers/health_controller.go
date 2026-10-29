package controllers

import (
	"encoding/json"
	"net/http"
	"time"

	"github.com/igetayor/telex/internal/api/services"
	"github.com/igetayor/telex/internal/config"
)

// HealthController handles telemetry, stats, and readiness probes.
type HealthController struct {
	cfg     *config.Config
	service *services.EventService
}

// NewHealthController creates a HealthController instance.
func NewHealthController(cfg *config.Config, s *services.EventService) *HealthController {
	return &HealthController{
		cfg:     cfg,
		service: s,
	}
}

// HandleHealthz reports system health, active workers, and uptime.
func (c *HealthController) HandleHealthz(w http.ResponseWriter, r *http.Request) {
	stats := c.service.GetStats()
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"status":           "healthy",
		"service":          c.cfg.ServiceName,
		"broker_connected": stats.BrokerConnected,
		"broker_type":      stats.BrokerType,
		"active_workers":   stats.ActiveWorkers,
		"uptime_seconds":   stats.UptimeSeconds,
		"timestamp":        time.Now().Unix(),
	})
}

// HandleStats returns worker pool statistics.
func (c *HealthController) HandleStats(w http.ResponseWriter, r *http.Request) {
	stats := c.service.GetStats()
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(stats)
}
