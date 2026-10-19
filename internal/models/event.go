package models

import (
	"encoding/json"
	"time"
)

// Event status constants
const (
	StatusPending    = "pending"
	StatusProcessing = "processing"
	StatusSucceeded  = "succeeded"
	StatusRetrying   = "retrying"
	StatusDeadLetter = "dead_letter"
)

// Simulation error triggers
const (
	SimulateTransientError = "transient"
	SimulateFatalError     = "fatal"
	SimulateTimeoutError   = "timeout"
)

// Event represents a streaming message payload processed by Telex workers.
type Event struct {
	ID             string          `json:"id"`
	Topic          string          `json:"topic"`
	Type           string          `json:"type"`
	TraceID        string          `json:"trace_id"`
	Payload        json.RawMessage `json:"payload"`
	Attempt        int             `json:"attempt"`
	MaxRetries     int             `json:"max_retries"`
	CreatedAt      time.Time       `json:"created_at"`
	LastAttempt    time.Time       `json:"last_attempt,omitempty"`
	NextRetryAt    time.Time       `json:"next_retry_at,omitempty"`
	ErrorChain     []string        `json:"error_chain,omitempty"`
	SimulateError  string          `json:"simulate_error,omitempty"` // For stress & chaos testing
	Status         string          `json:"status"`
}

// DLQMessage wraps an unrecoverable event routed to the Dead-Letter Queue.
type DLQMessage struct {
	Event            Event     `json:"event"`
	Reason           string    `json:"reason"`
	FailureTimestamp time.Time `json:"failure_timestamp"`
	FailedAtAttempt  int       `json:"failed_at_attempt"`
	ErrorDetails     string    `json:"error_details"`
	Replayed         bool      `json:"replayed"`
	ReplayedAt       time.Time `json:"replayed_at,omitempty"`
}

// ProcessingResult encapsulates worker execution outcome.
type ProcessingResult struct {
	Success   bool
	Retryable bool
	Err       error
	Duration  time.Duration
}

// WorkerPoolStats reports operational telemetry for the dashboard and API.
type WorkerPoolStats struct {
	ActiveWorkers    int     `json:"active_workers"`
	TotalWorkers     int     `json:"total_workers"`
	QueueDepth       int     `json:"queue_depth"`
	QueueCapacity    int     `json:"queue_capacity"`
	MessagesConsumed int64   `json:"messages_consumed"`
	MessagesSuccess  int64   `json:"messages_success"`
	MessagesRetried  int64   `json:"messages_retried"`
	MessagesDLQ      int64   `json:"messages_dlq"`
	UptimeSeconds    int64   `json:"uptime_seconds"`
	ThroughputPerSec float64 `json:"throughput_per_sec"`
	BrokerConnected  bool    `json:"broker_connected"`
	BrokerType       string  `json:"broker_type"`
}
