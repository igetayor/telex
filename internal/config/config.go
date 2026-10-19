package config

import (
	"os"
	"strconv"
	"time"
)

// Config represents runtime configuration parameters for the Telex worker service.
type Config struct {
	Port               string        `json:"port"`
	ServiceName        string        `json:"service_name"`
	AMQPURL            string        `json:"amqp_url"`
	QueueName          string        `json:"queue_name"`
	DLQExchange        string        `json:"dlq_exchange"`
	DLQQueueName       string        `json:"dlq_queue_name"`
	NumWorkers         int           `json:"num_workers"`
	QueueCapacity      int           `json:"queue_capacity"`
	MaxRetries         int           `json:"max_retries"`
	BaseBackoff        time.Duration `json:"base_backoff"`
	MaxBackoff         time.Duration `json:"max_backoff"`
	BackoffFactor      float64       `json:"backoff_factor"`
	TaskTimeout        time.Duration `json:"task_timeout"`
	GracefulTimeout    time.Duration `json:"graceful_timeout"`
	LogLevel           string        `json:"log_level"`
	SimulationMode     bool          `json:"simulation_mode"`
	SimIntervalMS      time.Duration `json:"sim_interval_ms"`
	PoisonPillRate     float64       `json:"poison_pill_rate"`
}

// LoadFromEnv loads configuration with sensible defaults.
func LoadFromEnv() *Config {
	port := getEnv("PORT", "8082")
	if port[0] != ':' && port != "" {
		port = ":" + port
	}

	numWorkers := getEnvInt("NUM_WORKERS", 8)
	queueCap := getEnvInt("QUEUE_CAPACITY", 1000)
	maxRetries := getEnvInt("MAX_RETRIES", 3)
	baseBackoffMS := getEnvInt("BASE_BACKOFF_MS", 500)
	maxBackoffMS := getEnvInt("MAX_BACKOFF_MS", 8000)
	backoffFactor := getEnvFloat("BACKOFF_FACTOR", 2.0)
	taskTimeoutSec := getEnvInt("TASK_TIMEOUT_SEC", 5)
	simIntervalMS := getEnvInt("SIM_INTERVAL_MS", 800)
	poisonRate := getEnvFloat("POISON_PILL_RATE", 0.15) // 15% generated events are faulty

	return &Config{
		Port:            port,
		ServiceName:     getEnv("SERVICE_NAME", "telex-worker-node"),
		AMQPURL:         getEnv("AMQP_URL", "amqp://guest:guest@localhost:5672/"),
		QueueName:       getEnv("QUEUE_NAME", "telex.events.incoming"),
		DLQExchange:     getEnv("DLQ_EXCHANGE", "telex.dlx"),
		DLQQueueName:    getEnv("DLQ_QUEUE_NAME", "telex.events.dlq"),
		NumWorkers:      numWorkers,
		QueueCapacity:   queueCap,
		MaxRetries:      maxRetries,
		BaseBackoff:     time.Duration(baseBackoffMS) * time.Millisecond,
		MaxBackoff:      time.Duration(maxBackoffMS) * time.Millisecond,
		BackoffFactor:   backoffFactor,
		TaskTimeout:     time.Duration(taskTimeoutSec) * time.Second,
		GracefulTimeout: 10 * time.Second,
		LogLevel:        getEnv("LOG_LEVEL", "INFO"),
		SimulationMode:  getEnvBool("SIMULATION_MODE", true),
		SimIntervalMS:   time.Duration(simIntervalMS) * time.Millisecond,
		PoisonPillRate:  poisonRate,
	}
}

func getEnv(key, defaultVal string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return defaultVal
}

func getEnvInt(key string, defaultVal int) int {
	if val := os.Getenv(key); val != "" {
		if i, err := strconv.Atoi(val); err == nil {
			return i
		}
	}
	return defaultVal
}

func getEnvFloat(key string, defaultVal float64) float64 {
	if val := os.Getenv(key); val != "" {
		if f, err := strconv.ParseFloat(val, 64); err == nil {
			return f
		}
	}
	return defaultVal
}

func getEnvBool(key string, defaultVal bool) bool {
	if val := os.Getenv(key); val != "" {
		if b, err := strconv.ParseBool(val); err == nil {
			return b
		}
	}
	return defaultVal
}
