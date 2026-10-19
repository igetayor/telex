package app

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/igetayor/telex/internal/api"
	"github.com/igetayor/telex/internal/config"
	"github.com/igetayor/telex/internal/dlq"
	"github.com/igetayor/telex/internal/logger"
	"github.com/igetayor/telex/internal/metrics"
	"github.com/igetayor/telex/internal/queue"
	"github.com/igetayor/telex/internal/worker"
	"github.com/igetayor/telex/web"
)

// Run initializes and executes the complete Telex worker service with graceful shutdown.
func Run() {
	// 1. Load Configuration
	cfg := config.LoadFromEnv()

	// 2. Initialize Structured JSON Logger
	log := logger.InitLogger(cfg.LogLevel, cfg.ServiceName)
	log.Info("Initializing Telex Resilient Event Streaming Worker Service",
		slog.String("service", cfg.ServiceName),
		slog.String("port", cfg.Port),
		slog.Int("num_workers", cfg.NumWorkers),
		slog.Int("queue_capacity", cfg.QueueCapacity),
		slog.Int("max_retries", cfg.MaxRetries))

	// 3. Initialize Prometheus Metrics
	_ = metrics.Init()

	// 4. Root Context with Graceful Signal Handling (SIGINT, SIGTERM)
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	// 5. Initialize Dead-Letter Queue Manager
	dlqManager := dlq.NewManager(1000)

	// 6. Connect Consumer: Attempt RabbitMQ, Fallback to In-Memory Stream Generator
	var consumer queue.Consumer
	rmq := queue.NewRabbitMQConsumer(cfg)
	if rmq.IsConnected() {
		consumer = rmq
		log.Info("Connected to RabbitMQ cluster", slog.String("url", cfg.AMQPURL))
	} else {
		log.Info("RabbitMQ broker not found; starting built-in resilient simulated stream generator")
		consumer = queue.NewSimulatedStreamConsumer(cfg)
	}

	// 7. Initialize and Start Bounded Worker Pool
	workerPool := worker.NewPool(cfg, consumer, dlqManager)
	if err := workerPool.Start(); err != nil {
		log.Error("Fatal error starting worker pool", slog.String("error", err.Error()))
		os.Exit(1)
	}

	// 8. Initialize HTTP & Prometheus Router
	apiServer := api.NewServer(cfg, workerPool, dlqManager, consumer, web.FS)

	httpServer := &http.Server{
		Addr:              cfg.Port,
		Handler:           apiServer.Handler(),
		ReadHeaderTimeout: 5 * time.Second,
		IdleTimeout:       120 * time.Second,
	}

	// 9. Launch HTTP Server in Dedicated Goroutine
	serverErr := make(chan error, 1)
	go func() {
		log.Info("HTTP and Prometheus server listening",
			slog.String("addr", cfg.Port),
			slog.String("metrics_url", "http://localhost"+cfg.Port+"/metrics"),
			slog.String("ui_url", "http://localhost"+cfg.Port+"/"))
		if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			serverErr <- err
		}
	}()

	// 10. Await Graceful Termination Signal
	select {
	case <-ctx.Done():
		log.Info("SIGTERM / SIGINT shutdown signal captured; draining worker pools...")
	case err := <-serverErr:
		log.Error("Fatal HTTP listener error", slog.String("error", err.Error()))
		os.Exit(1)
	}

	// 11. Graceful Worker Drain and HTTP Shutdown
	drainTimeout := cfg.GracefulTimeout
	if err := workerPool.Stop(drainTimeout); err != nil {
		log.Warn("Worker drain warning", slog.String("error", err.Error()))
	}

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()

	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		log.Error("HTTP shutdown error", slog.String("error", err.Error()))
	} else {
		log.Info("Telex worker service exited cleanly")
	}
}
