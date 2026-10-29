package api

import (
	"io/fs"
	"net/http"

	"github.com/google/uuid"
	"github.com/igetayor/telex/internal/api/controllers"
	"github.com/igetayor/telex/internal/api/services"
	"github.com/igetayor/telex/internal/config"
	"github.com/igetayor/telex/internal/dlq"
	"github.com/igetayor/telex/internal/logger"
	"github.com/igetayor/telex/internal/queue"
	"github.com/igetayor/telex/internal/worker"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Server coordinates HTTP API routing, Prometheus metrics, and Controller-Service architecture.
type Server struct {
	cfg        *config.Config
	mux        *http.ServeMux
	webFS      fs.FS
	eventCtrl  *controllers.EventController
	dlqCtrl    *controllers.DLQController
	healthCtrl *controllers.HealthController
}

// NewServer builds an API server instance.
func NewServer(cfg *config.Config, pool *worker.Pool, dlqMgr *dlq.Manager, consumer queue.Consumer, webFS fs.FS) *Server {
	eventSvc := services.NewEventService(pool, dlqMgr, consumer)

	s := &Server{
		cfg:        cfg,
		mux:        http.NewServeMux(),
		webFS:      webFS,
		eventCtrl:  controllers.NewEventController(eventSvc),
		dlqCtrl:    controllers.NewDLQController(eventSvc),
		healthCtrl: controllers.NewHealthController(cfg, eventSvc),
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
	s.mux.HandleFunc("/healthz", s.healthCtrl.HandleHealthz)
	s.mux.HandleFunc("/api/v1/stats", s.healthCtrl.HandleStats)

	// Event Ingestion & Streams
	s.mux.HandleFunc("/api/v1/events/recent", s.eventCtrl.HandleRecentEvents)
	s.mux.HandleFunc("/api/v1/events/publish", s.eventCtrl.HandlePublishEvent)

	// Dead-Letter Queue (DLQ) Management Endpoints
	s.mux.HandleFunc("/api/v1/dlq", s.dlqCtrl.HandleDLQ)
	s.mux.HandleFunc("/api/v1/dlq/replay", s.dlqCtrl.HandleDLQReplay)

	// Web Dashboard & Interactive Documentation from web/
	if s.webFS != nil {
		fileServer := http.FileServer(http.FS(s.webFS))
		s.mux.Handle("/", fileServer)
	}
}
