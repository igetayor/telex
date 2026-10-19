package queue

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/igetayor/telex/internal/config"
	"github.com/igetayor/telex/internal/logger"
	"github.com/igetayor/telex/internal/models"
	amqp "github.com/rabbitmq/amqp091-go"
)

// Consumer is the generic streaming interface for event consumers.
type Consumer interface {
	Start(ctx context.Context, handler func(event models.Event)) error
	Stop() error
	IsConnected() bool
	BrokerType() string
	Publish(ctx context.Context, event models.Event) error
}

// RabbitMQConsumer implements event consumption using RabbitMQ AMQP 0-9-1.
type RabbitMQConsumer struct {
	cfg       *config.Config
	conn      *amqp.Connection
	ch        *amqp.Channel
	connected atomic.Bool
	closed    atomic.Bool
	mu        sync.RWMutex
}

// NewRabbitMQConsumer constructs a RabbitMQ consumer with DLX topology configuration.
func NewRabbitMQConsumer(cfg *config.Config) *RabbitMQConsumer {
	rc := &RabbitMQConsumer{
		cfg: cfg,
	}

	conn, err := amqp.Dial(cfg.AMQPURL)
	if err != nil {
		logger.Get().Warn("RabbitMQ broker not reachable at startup; worker running in fallback mode",
			slog.String("amqp_url", cfg.AMQPURL),
			slog.String("error", err.Error()))
		rc.connected.Store(false)
		return rc
	}

	ch, err := conn.Channel()
	if err != nil {
		_ = conn.Close()
		rc.connected.Store(false)
		return rc
	}

	rc.conn = conn
	rc.ch = ch
	rc.connected.Store(true)

	// Setup exchanges and queues
	if err := rc.setupTopology(); err != nil {
		logger.Get().Error("Failed to configure RabbitMQ topology", slog.String("error", err.Error()))
	}

	return rc
}

func (r *RabbitMQConsumer) setupTopology() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	// 1. Declare DLX Exchange and Queue
	err := r.ch.ExchangeDeclare(
		r.cfg.DLQExchange, "direct", true, false, false, false, nil,
	)
	if err != nil {
		return err
	}

	_, err = r.ch.QueueDeclare(
		r.cfg.DLQQueueName, true, false, false, false, nil,
	)
	if err != nil {
		return err
	}

	err = r.ch.QueueBind(
		r.cfg.DLQQueueName, r.cfg.DLQQueueName, r.cfg.DLQExchange, false, nil,
	)
	if err != nil {
		return err
	}

	// 2. Declare Main Incoming Queue with DLX arguments
	args := amqp.Table{
		"x-dead-letter-exchange":    r.cfg.DLQExchange,
		"x-dead-letter-routing-key": r.cfg.DLQQueueName,
	}
	_, err = r.ch.QueueDeclare(
		r.cfg.QueueName, true, false, false, false, args,
	)
	if err != nil {
		return err
	}

	// 3. Set QoS Prefetch equal to bounded worker pool size
	return r.ch.Qos(r.cfg.NumWorkers*2, 0, false)
}

// Start begins pulling messages from the RabbitMQ queue.
func (r *RabbitMQConsumer) Start(ctx context.Context, handler func(event models.Event)) error {
	if !r.connected.Load() {
		return fmt.Errorf("rabbitmq not connected")
	}

	deliveries, err := r.ch.Consume(
		r.cfg.QueueName,
		"",    // consumer tag
		false, // auto-ack (manual ack for resilience)
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return err
	}

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case d, ok := <-deliveries:
				if !ok {
					return
				}
				var event models.Event
				if err := json.Unmarshal(d.Body, &event); err != nil {
					_ = d.Nack(false, false) // Send malformed messages straight to DLQ
					continue
				}
				_ = d.Ack(false)
				handler(event)
			}
		}
	}()

	return nil
}

// Publish enqueues an event into the RabbitMQ queue.
func (r *RabbitMQConsumer) Publish(ctx context.Context, event models.Event) error {
	if !r.connected.Load() {
		return fmt.Errorf("rabbitmq not connected")
	}

	body, err := json.Marshal(event)
	if err != nil {
		return err
	}

	return r.ch.PublishWithContext(ctx,
		"",              // exchange
		r.cfg.QueueName, // routing key
		false,
		false,
		amqp.Publishing{
			DeliveryMode: amqp.Persistent,
			ContentType:  "application/json",
			Body:         body,
			MessageId:    event.ID,
			Timestamp:    event.CreatedAt,
		},
	)
}

func (r *RabbitMQConsumer) IsConnected() bool {
	return r.connected.Load()
}

func (r *RabbitMQConsumer) BrokerType() string {
	return "RabbitMQ"
}

func (r *RabbitMQConsumer) Stop() error {
	r.closed.Store(true)
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.ch != nil {
		_ = r.ch.Close()
	}
	if r.conn != nil {
		return r.conn.Close()
	}
	return nil
}

// SimulatedStreamConsumer provides a resilient in-memory event generator with configurable fault injection.
type SimulatedStreamConsumer struct {
	cfg       *config.Config
	inboundCh chan models.Event
	running   atomic.Bool
	rng       *rand.Rand
}

// NewSimulatedStreamConsumer initializes the in-memory stream generator.
func NewSimulatedStreamConsumer(cfg *config.Config) *SimulatedStreamConsumer {
	return &SimulatedStreamConsumer{
		cfg:       cfg,
		inboundCh: make(chan models.Event, cfg.QueueCapacity),
		rng:       rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

// Start spawns the periodic event generator goroutine.
func (s *SimulatedStreamConsumer) Start(ctx context.Context, handler func(event models.Event)) error {
	s.running.Store(true)

	// Inbound dispatcher
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case ev := <-s.inboundCh:
				handler(ev)
			}
		}
	}()

	// Event generator ticker
	go func() {
		interval := s.cfg.SimIntervalMS
		if interval <= 0 {
			interval = 500 * time.Millisecond
		}
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		eventTypes := []string{
			"payment.authorized",
			"order.created",
			"inventory.reserved",
			"invoice.generated",
			"user.registered",
		}

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if !s.running.Load() {
					return
				}

				eventType := eventTypes[s.rng.Intn(len(eventTypes))]
				simulateErr := ""

				// Controlled fault injection based on configured rate
				if s.rng.Float64() < s.cfg.PoisonPillRate {
					faultType := s.rng.Intn(3)
					switch faultType {
					case 0:
						simulateErr = models.SimulateTransientError
					case 1:
						simulateErr = models.SimulateFatalError
					case 2:
						simulateErr = models.SimulateTimeoutError
					}
				}

				payload, _ := json.Marshal(map[string]any{
					"amount":    float64(s.rng.Intn(50000)) / 100.0,
					"currency":  "USD",
					"region":    "us-east-1",
					"client_ip": "192.168.1.10",
				})

				event := models.Event{
					ID:            uuid.New().String(),
					Topic:         "telex.events.stream",
					Type:          eventType,
					TraceID:       uuid.New().String(),
					Payload:       payload,
					Attempt:       0,
					MaxRetries:    s.cfg.MaxRetries,
					CreatedAt:     time.Now(),
					SimulateError: simulateErr,
					Status:        models.StatusPending,
				}

				select {
				case s.inboundCh <- event:
				default:
					logger.Get().Warn("Simulated stream channel saturated; dropping event")
				}
			}
		}
	}()

	return nil
}

// Publish injects an event directly into the stream channel.
func (s *SimulatedStreamConsumer) Publish(ctx context.Context, event models.Event) error {
	if event.ID == "" {
		event.ID = uuid.New().String()
	}
	if event.TraceID == "" {
		event.TraceID = uuid.New().String()
	}
	if event.CreatedAt.IsZero() {
		event.CreatedAt = time.Now()
	}

	select {
	case s.inboundCh <- event:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	default:
		return fmt.Errorf("inbound stream channel full")
	}
}

func (s *SimulatedStreamConsumer) IsConnected() bool {
	return true
}

func (s *SimulatedStreamConsumer) BrokerType() string {
	return "Simulated-Engine"
}

func (s *SimulatedStreamConsumer) Stop() error {
	s.running.Store(false)
	return nil
}
