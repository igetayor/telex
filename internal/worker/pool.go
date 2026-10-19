package worker

import (
	"context"
	"fmt"
	"log/slog"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/igetayor/telex/internal/config"
	"github.com/igetayor/telex/internal/dlq"
	"github.com/igetayor/telex/internal/logger"
	"github.com/igetayor/telex/internal/metrics"
	"github.com/igetayor/telex/internal/models"
	"github.com/igetayor/telex/internal/queue"
	"github.com/igetayor/telex/internal/retry"
)

// Pool orchestrates bounded worker goroutines, payload processing, retries, and DLQ routing.
type Pool struct {
	cfg              *config.Config
	consumer         queue.Consumer
	dlqMgr           *dlq.Manager
	backoff          *retry.BackoffPolicy
	taskQueue        chan models.Event
	activeWorkers    atomic.Int64
	messagesConsumed atomic.Int64
	messagesSuccess  atomic.Int64
	messagesRetried  atomic.Int64
	messagesDLQ      atomic.Int64
	startTime        time.Time
	wg               sync.WaitGroup
	ctx              context.Context
	cancel           context.CancelFunc

	recentEvents   []models.Event
	recentEventsMu sync.RWMutex
	maxRecent      int
}

// NewPool builds a new bounded concurrent worker pool.
func NewPool(cfg *config.Config, consumer queue.Consumer, dlqMgr *dlq.Manager) *Pool {
	ctx, cancel := context.WithCancel(context.Background())
	return &Pool{
		cfg:          cfg,
		consumer:     consumer,
		dlqMgr:       dlqMgr,
		backoff:      retry.NewBackoffPolicy(cfg),
		taskQueue:    make(chan models.Event, cfg.QueueCapacity),
		startTime:    time.Now(),
		ctx:          ctx,
		cancel:       cancel,
		recentEvents: make([]models.Event, 0, 100),
		maxRecent:    100,
	}
}

// Start spawns the configured worker pool and begins consuming messages.
func (p *Pool) Start() error {
	logger.Get().Info("Starting Telex Bounded Worker Pool",
		slog.Int("num_workers", p.cfg.NumWorkers),
		slog.Int("queue_capacity", p.cfg.QueueCapacity),
		slog.Int("max_retries", p.cfg.MaxRetries),
		slog.String("broker_type", p.consumer.BrokerType()))

	// Spawn fixed pool of worker goroutines
	for i := 1; i <= p.cfg.NumWorkers; i++ {
		p.wg.Add(1)
		go p.worker(i)
	}

	// Connect consumer and dispatch to bounded task queue
	return p.consumer.Start(p.ctx, func(event models.Event) {
		p.Enqueue(event)
	})
}

// Enqueue adds an event to the bounded worker channel with backpressure tracking.
func (p *Pool) Enqueue(event models.Event) bool {
	p.messagesConsumed.Add(1)
	metrics.Get().MessagesConsumedTotal.WithLabelValues(event.Topic, event.Type).Inc()

	select {
	case p.taskQueue <- event:
		metrics.Get().QueueDepthGauge.Set(float64(len(p.taskQueue)))
		return true
	default:
		// Queue saturated: backpressure defense
		logger.Get().Warn("Worker task queue saturated; routing directly to DLQ under backpressure",
			slog.String("event_id", event.ID),
			slog.String("topic", event.Topic))
		p.dlqMgr.RouteToDLQ(p.ctx, event, "backpressure_queue_overflow", fmt.Errorf("queue capacity %d exceeded", p.cfg.QueueCapacity))
		p.messagesDLQ.Add(1)
		return false
	}
}

// worker is the concurrent execution loop for each pool worker.
func (p *Pool) worker(workerID int) {
	defer p.wg.Done()

	for {
		select {
		case <-p.ctx.Done():
			return
		case event, ok := <-p.taskQueue:
			if !ok {
				return
			}
			p.processEvent(workerID, event)
		}
	}
}

func (p *Pool) processEvent(workerID int, event models.Event) {
	metrics.Get().QueueDepthGauge.Set(float64(len(p.taskQueue)))
	p.activeWorkers.Add(1)
	metrics.Get().ActiveWorkersGauge.Set(float64(p.activeWorkers.Load()))
	defer func() {
		p.activeWorkers.Add(-1)
		metrics.Get().ActiveWorkersGauge.Set(float64(p.activeWorkers.Load()))
	}()

	ctx := logger.WithTraceID(p.ctx, event.TraceID)
	ctx = logger.WithWorkerID(ctx, workerID)
	log := logger.FromContext(ctx)

	event.Status = models.StatusProcessing
	event.LastAttempt = time.Now()
	start := time.Now()

	// Execute payload processing
	err := p.executeTask(ctx, &event)
	duration := time.Since(start)

	if err == nil {
		// Successful processing
		event.Status = models.StatusSucceeded
		p.messagesSuccess.Add(1)
		metrics.Get().MessageProcessingDuration.WithLabelValues(event.Topic, "success").Observe(duration.Seconds())
		log.Info("Event processed successfully",
			slog.String("event_id", event.ID),
			slog.String("type", event.Type),
			slog.Int("attempt", event.Attempt),
			slog.Duration("latency", duration))
		p.recordRecent(event)
		return
	}

	// Failure Handling: Retries or DLQ Routing
	metrics.Get().MessageProcessingDuration.WithLabelValues(event.Topic, "error").Observe(duration.Seconds())
	metrics.Get().MessageErrorsTotal.WithLabelValues(event.Topic, err.Error()).Inc()
	event.ErrorChain = append(event.ErrorChain, fmt.Sprintf("Attempt %d: %s", event.Attempt+1, err.Error()))

	if p.backoff.CanRetry(&event, err) {
		event.Attempt++
		delay := p.backoff.CalculateDelay(event.Attempt)
		event.Status = models.StatusRetrying
		event.NextRetryAt = time.Now().Add(delay)

		p.messagesRetried.Add(1)
		metrics.Get().MessageRetriesTotal.WithLabelValues(event.Topic, fmt.Sprintf("%d", event.Attempt)).Inc()

		log.Warn("Event failed; scheduling retry with exponential backoff",
			slog.String("event_id", event.ID),
			slog.Int("next_attempt", event.Attempt+1),
			slog.Duration("backoff_delay", delay),
			slog.String("error", err.Error()))

		p.recordRecent(event)

		// Schedule non-blocking retry re-enqueue
		time.AfterFunc(delay, func() {
			select {
			case <-p.ctx.Done():
				return
			case p.taskQueue <- event:
				metrics.Get().QueueDepthGauge.Set(float64(len(p.taskQueue)))
			}
		})
	} else {
		// Max retries exceeded or fatal error -> Route to Dead-Letter Queue
		reason := "max_retries_exceeded"
		if event.Attempt == 0 {
			reason = "unrecoverable_poison_pill"
		}
		p.messagesDLQ.Add(1)
		p.dlqMgr.RouteToDLQ(ctx, event, reason, err)
		event.Status = models.StatusDeadLetter
		p.recordRecent(event)
	}
}

// executeTask simulates realistic payload processing with fault injection checks.
func (p *Pool) executeTask(ctx context.Context, event *models.Event) error {
	// Simulate work duration (15ms - 40ms)
	workTime := time.Duration(15+rand.Intn(25)) * time.Millisecond
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(workTime):
	}

	// Check simulated fault triggers
	switch event.SimulateError {
	case models.SimulateFatalError:
		return retry.ErrPoisonPillPayload
	case models.SimulateTransientError:
		// Fails on initial attempts, recovers on attempt >= 2 to demonstrate retry recovery
		if event.Attempt < 2 {
			return retry.ErrDownstreamTimeout
		}
		return nil
	case models.SimulateTimeoutError:
		return retry.ErrRateLimitExceeded
	}

	return nil
}

func (p *Pool) recordRecent(ev models.Event) {
	p.recentEventsMu.Lock()
	defer p.recentEventsMu.Unlock()

	if len(p.recentEvents) >= p.maxRecent {
		p.recentEvents = p.recentEvents[1:]
	}
	p.recentEvents = append(p.recentEvents, ev)
}

// GetRecentEvents returns a copy of recent processed events for the UI.
func (p *Pool) GetRecentEvents(limit int) []models.Event {
	p.recentEventsMu.RLock()
	defer p.recentEventsMu.RUnlock()

	if limit <= 0 || limit > len(p.recentEvents) {
		limit = len(p.recentEvents)
	}

	result := make([]models.Event, 0, limit)
	for i := len(p.recentEvents) - 1; i >= 0 && len(result) < limit; i-- {
		result = append(result, p.recentEvents[i])
	}
	return result
}

// GetStats compiles live operational statistics for Prometheus & HTTP dashboards.
func (p *Pool) GetStats() models.WorkerPoolStats {
	uptime := int64(time.Since(p.startTime).Seconds())
	throughput := 0.0
	if uptime > 0 {
		throughput = float64(p.messagesSuccess.Load()) / float64(uptime)
	}

	return models.WorkerPoolStats{
		ActiveWorkers:    int(p.activeWorkers.Load()),
		TotalWorkers:     p.cfg.NumWorkers,
		QueueDepth:       len(p.taskQueue),
		QueueCapacity:    p.cfg.QueueCapacity,
		MessagesConsumed: p.messagesConsumed.Load(),
		MessagesSuccess:  p.messagesSuccess.Load(),
		MessagesRetried:  p.messagesRetried.Load(),
		MessagesDLQ:      p.messagesDLQ.Load(),
		UptimeSeconds:    uptime,
		ThroughputPerSec: throughput,
		BrokerConnected:  p.consumer.IsConnected(),
		BrokerType:       p.consumer.BrokerType(),
	}
}

// Stop executes graceful shutdown, draining in-flight messages up to timeout.
func (p *Pool) Stop(timeout time.Duration) error {
	logger.Get().Info("Executing graceful shutdown: draining worker pools...")
	_ = p.consumer.Stop()

	// Signal workers
	p.cancel()

	done := make(chan struct{})
	go func() {
		p.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		logger.Get().Info("All worker goroutines drained cleanly")
		return nil
	case <-time.After(timeout):
		return fmt.Errorf("graceful drain timed out after %v", timeout)
	}
}
