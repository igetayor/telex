package retry

import (
	"errors"
	"math"
	"math/rand"
	"time"

	"github.com/igetayor/telex/internal/config"
	"github.com/igetayor/telex/internal/models"
)

// Standard Sentinel Errors
var (
	ErrNonRetryableValidation = errors.New("unrecoverable validation failure")
	ErrPoisonPillPayload      = errors.New("poison-pill payload detected")
	ErrDownstreamTimeout      = errors.New("downstream service timeout")
	ErrRateLimitExceeded      = errors.New("rate limit exceeded; backoff required")
)

// BackoffPolicy calculates exponential backoff durations with decorrelated full jitter.
type BackoffPolicy struct {
	baseBackoff   time.Duration
	maxBackoff    time.Duration
	backoffFactor float64
	maxRetries    int
	rng           *rand.Rand
}

// NewBackoffPolicy constructs a backoff policy from configuration.
func NewBackoffPolicy(cfg *config.Config) *BackoffPolicy {
	return &BackoffPolicy{
		baseBackoff:   cfg.BaseBackoff,
		maxBackoff:    cfg.MaxBackoff,
		backoffFactor: cfg.BackoffFactor,
		maxRetries:    cfg.MaxRetries,
		rng:           rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

// CalculateDelay computes exponential backoff with full jitter for a given attempt.
// Formula: delay = min(maxBackoff, baseBackoff * factor^(attempt-1)) + jitter
func (b *BackoffPolicy) CalculateDelay(attempt int) time.Duration {
	if attempt <= 0 {
		attempt = 1
	}

	// Calculate deterministic exponential factor
	multiplier := math.Pow(b.backoffFactor, float64(attempt-1))
	rawBackoff := float64(b.baseBackoff) * multiplier

	if rawBackoff > float64(b.maxBackoff) {
		rawBackoff = float64(b.maxBackoff)
	}

	// Decorrelated Full Jitter: randomize delay in range [rawBackoff/2, rawBackoff]
	half := rawBackoff / 2.0
	jitter := float64(b.rng.Int63n(int64(half + 1)))
	finalDuration := time.Duration(half + jitter)

	return finalDuration
}

// CanRetry determines whether the event is eligible for further retry attempts.
func (b *BackoffPolicy) CanRetry(event *models.Event, err error) bool {
	if event == nil {
		return false
	}

	// Check fatal unrecoverable errors
	if errors.Is(err, ErrNonRetryableValidation) || errors.Is(err, ErrPoisonPillPayload) {
		return false
	}

	maxAllowed := b.maxRetries
	if event.MaxRetries > 0 {
		maxAllowed = event.MaxRetries
	}

	return event.Attempt < maxAllowed
}
