package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Metrics holds Prometheus collectors for the event streaming worker.
type Metrics struct {
	MessagesConsumedTotal     *prometheus.CounterVec
	MessageProcessingDuration *prometheus.HistogramVec
	MessageErrorsTotal        *prometheus.CounterVec
	MessageRetriesTotal       *prometheus.CounterVec
	DLQMessagesTotal          *prometheus.CounterVec
	ActiveWorkersGauge        prometheus.Gauge
	QueueDepthGauge           prometheus.Gauge
	DLQDepthGauge             prometheus.Gauge
}

var globalMetrics *Metrics

// Init initializes and registers Prometheus metric collectors.
func Init() *Metrics {
	if globalMetrics != nil {
		return globalMetrics
	}

	m := &Metrics{
		MessagesConsumedTotal: promauto.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "telex",
				Subsystem: "worker",
				Name:      "messages_consumed_total",
				Help:      "Total number of stream messages ingested by the consumer",
			},
			[]string{"topic", "type"},
		),

		MessageProcessingDuration: promauto.NewHistogramVec(
			prometheus.HistogramOpts{
				Namespace: "telex",
				Subsystem: "worker",
				Name:      "message_processing_duration_seconds",
				Help:      "Execution latency of event payload processing in seconds",
				Buckets:   []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0},
			},
			[]string{"topic", "status"},
		),

		MessageErrorsTotal: promauto.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "telex",
				Subsystem: "worker",
				Name:      "message_errors_total",
				Help:      "Total number of worker processing failures classified by error type",
			},
			[]string{"topic", "error_type"},
		),

		MessageRetriesTotal: promauto.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "telex",
				Subsystem: "worker",
				Name:      "message_retries_total",
				Help:      "Total number of message retry attempts scheduled with exponential backoff",
			},
			[]string{"topic", "attempt"},
		),

		DLQMessagesTotal: promauto.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "telex",
				Subsystem: "worker",
				Name:      "dlq_messages_total",
				Help:      "Total messages routed to the Dead-Letter Queue due to exhausted retries or fatal errors",
			},
			[]string{"topic", "reason"},
		),

		ActiveWorkersGauge: promauto.NewGauge(
			prometheus.GaugeOpts{
				Namespace: "telex",
				Subsystem: "worker",
				Name:      "workers_active",
				Help:      "Current number of worker goroutines actively processing message payloads",
			},
		),

		QueueDepthGauge: promauto.NewGauge(
			prometheus.GaugeOpts{
				Namespace: "telex",
				Subsystem: "worker",
				Name:      "queue_depth",
				Help:      "Current number of pending messages waiting in the bounded ingestion channel",
			},
		),

		DLQDepthGauge: promauto.NewGauge(
			prometheus.GaugeOpts{
				Namespace: "telex",
				Subsystem: "worker",
				Name:      "dlq_depth",
				Help:      "Total count of dead-letter messages stored in DLQ buffer",
			},
		),
	}

	globalMetrics = m
	return m
}

// Get returns the global metrics instance.
func Get() *Metrics {
	if globalMetrics == nil {
		return Init()
	}
	return globalMetrics
}
