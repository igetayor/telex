package logger

import (
	"context"
	"log/slog"
	"os"
	"strings"

	"github.com/google/uuid"
)

type contextKey string

const (
	TraceIDKey   contextKey = "trace_id"
	EventIDKey   contextKey = "event_id"
	WorkerIDKey  contextKey = "worker_id"
	EventTypeKey contextKey = "event_type"
)

var defaultLogger *slog.Logger

// InitLogger configures the global structured JSON logger.
func InitLogger(levelStr string, serviceName string) *slog.Logger {
	var level slog.Level
	switch strings.ToUpper(levelStr) {
	case "DEBUG":
		level = slog.LevelDebug
	case "WARN", "WARNING":
		level = slog.LevelWarn
	case "ERROR":
		level = slog.LevelError
	default:
		level = slog.LevelInfo
	}

	opts := &slog.HandlerOptions{
		Level:     level,
		AddSource: true,
	}

	jsonHandler := slog.NewJSONHandler(os.Stdout, opts)
	baseLogger := slog.New(jsonHandler).With(slog.String("service", serviceName))
	slog.SetDefault(baseLogger)
	defaultLogger = baseLogger
	return baseLogger
}

// Get returns the default logger instance.
func Get() *slog.Logger {
	if defaultLogger == nil {
		return slog.Default()
	}
	return defaultLogger
}

// WithTraceID injects trace_id into context.
func WithTraceID(ctx context.Context, traceID string) context.Context {
	if traceID == "" {
		traceID = uuid.New().String()
	}
	return context.WithValue(ctx, TraceIDKey, traceID)
}

// WithWorkerID injects worker_id into context.
func WithWorkerID(ctx context.Context, workerID int) context.Context {
	return context.WithValue(ctx, WorkerIDKey, workerID)
}

// TraceIDFromContext retrieves the trace ID from context.
func TraceIDFromContext(ctx context.Context) string {
	if ctx == nil {
		return ""
	}
	if val, ok := ctx.Value(TraceIDKey).(string); ok {
		return val
	}
	return ""
}

// FromContext decorates logger with contextual attributes (trace_id, event_id, worker_id).
func FromContext(ctx context.Context) *slog.Logger {
	l := Get()
	if ctx == nil {
		return l
	}

	if traceID, ok := ctx.Value(TraceIDKey).(string); ok && traceID != "" {
		l = l.With(slog.String("trace_id", traceID))
	}
	if eventID, ok := ctx.Value(EventIDKey).(string); ok && eventID != "" {
		l = l.With(slog.String("event_id", eventID))
	}
	if workerID, ok := ctx.Value(WorkerIDKey).(int); ok {
		l = l.With(slog.Int("worker_id", workerID))
	}
	if eventType, ok := ctx.Value(EventTypeKey).(string); ok && eventType != "" {
		l = l.With(slog.String("event_type", eventType))
	}
	return l
}
