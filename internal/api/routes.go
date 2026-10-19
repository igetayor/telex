package api

import (
	"encoding/json"
	"io/fs"
	"net/http"
	"strconv"
	"time"

	"github.com/google/uuid"
	"github.com/igetayor/telex/internal/config"
	"github.com/igetayor/telex/internal/dlq"
	"github.com/igetayor/telex/internal/logger"
	"github.com/igetayor/telex/internal/models"
	"github.com/igetayor/telex/internal/queue"
	"github.com/igetayor/telex/internal/worker"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Server coordinates HTTP API routing and Prometheus metrics exposition.
type Server struct {
	cfg      *config.Config
	pool     *worker.Pool
	dlqMgr   *dlq.Manager
	consumer queue.Consumer
	mux      *http.ServeMux
	webFS    fs.FS
}

// NewServer builds an API server instance.
func NewServer(cfg *config.Config, pool *worker.Pool, dlqMgr *dlq.Manager, consumer queue.Consumer, webFS fs.FS) *Server {
	s := &Server{
		cfg:      cfg,
		pool:     pool,
		dlqMgr:   dlqMgr,
		consumer: consumer,
		mux:      http.NewServeMux(),
		webFS:    webFS,
	}

	s.registerRoutes()
	return s
}

// Handler returns the HTTP handler with correlation ID middleware.
func (s *Server) Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		traceID := r.Header.Get("X-Request-ID")
		if traceID == "" {
			traceID = uuid.New().String()
		}
		w.Header().Set("X-Request-ID", traceID)
		w.Header().Set("X-Service", s.cfg.ServiceName)

		ctx := logger.WithTraceID(r.Context(), traceID)
		r = r.WithContext(ctx)

		s.mux.ServeHTTP(w, r)
	})
}

func (s *Server) registerRoutes() {
	// Standard Prometheus Scrape Endpoint
	s.mux.Handle("/metrics", promhttp.Handler())

	// Health and Telemetry Endpoints
	s.mux.HandleFunc("/healthz", s.handleHealthz)
	s.mux.HandleFunc("/api/v1/stats", s.handleStats)
	s.mux.HandleFunc("/api/v1/events/recent", s.handleRecentEvents)
	s.mux.HandleFunc("/api/v1/events/publish", s.handlePublishEvent)

	// Dead-Letter Queue (DLQ) Management Endpoints
	s.mux.HandleFunc("/api/v1/dlq", s.handleDLQ)
	s.mux.HandleFunc("/api/v1/dlq/replay", s.handleDLQReplay)

	// Web Dashboard & Interactive Documentation
	if s.webFS != nil {
		fileServer := http.FileServer(http.FS(s.webFS))
		s.mux.Handle("/", fileServer)
	}
}

func (s *Server) handleHealthz(w http.ResponseWriter, r *http.Request) {
	stats := s.pool.GetStats()
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"status":           "healthy",
		"service":          s.cfg.ServiceName,
		"broker_connected": stats.BrokerConnected,
		"broker_type":      stats.BrokerType,
		"active_workers":   stats.ActiveWorkers,
		"uptime_seconds":   stats.UptimeSeconds,
		"timestamp":        time.Now().Unix(),
	})
}

func (s *Server) handleStats(w http.ResponseWriter, r *http.Request) {
	stats := s.pool.GetStats()
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(stats)
}

func (s *Server) handleRecentEvents(w http.ResponseWriter, r *http.Request) {
	limitStr := r.URL.Query().Get("limit")
	limit := 50
	if l, err := strconv.Atoi(limitStr); err == nil && l > 0 {
		limit = l
	}

	events := s.pool.GetRecentEvents(limit)
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"events": events,
		"count":  len(events),
	})
}

func (s *Server) handlePublishEvent(w http.ResponseWriter, r *http.Request) {
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
		http.Error(w, "Invalid JSON payload", http.StatusBadRequest)
		return
	}

	if req.Topic == "" {
		req.Topic = "telex.events.stream"
	}
	if req.Type == "" {
		req.Type = "custom.event"
	}
	if len(req.Payload) == 0 {
		req.Payload = json.RawMessage(`{"message":"Manual test payload"}`)
	}

	traceID := logger.TraceIDFromContext(r.Context())
	event := models.Event{
		ID:            uuid.New().String(),
		Topic:         req.Topic,
		Type:          req.Type,
		TraceID:       traceID,
		Payload:       req.Payload,
		Attempt:       0,
		MaxRetries:    s.cfg.MaxRetries,
		CreatedAt:     time.Now(),
		SimulateError: req.SimulateError,
		Status:        models.StatusPending,
	}

	enqueued := s.pool.Enqueue(event)

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"event_id": event.ID,
		"trace_id": traceID,
		"enqueued": enqueued,
		"status":   "accepted",
	})
}

func (s *Server) handleDLQ(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		limitStr := r.URL.Query().Get("limit")
		limit := 50
		if l, err := strconv.Atoi(limitStr); err == nil && l > 0 {
			limit = l
		}
		messages := s.dlqMgr.GetMessages(limit)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"messages": messages,
			"count":    len(messages),
		})

	case http.MethodDelete:
		s.dlqMgr.Clear()
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]string{
			"status":  "cleared",
			"message": "Dead-Letter Queue has been flushed",
		})

	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

func (s *Server) handleDLQReplay(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req struct {
		EventID string `json:"event_id"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.EventID == "" {
		http.Error(w, "event_id is required", http.StatusBadRequest)
		return
	}

	replayedEvent, err := s.dlqMgr.Replay(req.EventID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}

	// Re-enqueue into worker pool
	s.pool.Enqueue(*replayedEvent)

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"status":   "replayed",
		"event_id": replayedEvent.ID,
		"message":  "Event re-queued into worker processing pool",
	})
}
