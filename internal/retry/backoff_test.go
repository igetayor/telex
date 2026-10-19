package retry_test

import (
	"errors"
	"testing"
	"time"

	"github.com/igetayor/telex/internal/config"
	"github.com/igetayor/telex/internal/models"
	"github.com/igetayor/telex/internal/retry"
)

func TestBackoffPolicy_CalculateDelay(t *testing.T) {
	cfg := &config.Config{
		BaseBackoff:   100 * time.Millisecond,
		MaxBackoff:    2 * time.Second,
		BackoffFactor: 2.0,
		MaxRetries:    4,
	}
	policy := retry.NewBackoffPolicy(cfg)

	// Attempt 1: delay should be within [50ms, 100ms]
	d1 := policy.CalculateDelay(1)
	if d1 < 50*time.Millisecond || d1 > 100*time.Millisecond {
		t.Fatalf("Expected attempt 1 delay in [50ms, 100ms], got %v", d1)
	}

	// Attempt 2: raw = 200ms -> [100ms, 200ms]
	d2 := policy.CalculateDelay(2)
	if d2 < 100*time.Millisecond || d2 > 200*time.Millisecond {
		t.Fatalf("Expected attempt 2 delay in [100ms, 200ms], got %v", d2)
	}

	// Max Cap: Attempt 10 should be capped at MaxBackoff (2s)
	d10 := policy.CalculateDelay(10)
	if d10 > 2*time.Second {
		t.Fatalf("Expected attempt 10 to not exceed max backoff 2s, got %v", d10)
	}
}

func TestBackoffPolicy_CanRetry(t *testing.T) {
	cfg := &config.Config{
		MaxRetries: 3,
	}
	policy := retry.NewBackoffPolicy(cfg)

	ev := &models.Event{
		Attempt:    1,
		MaxRetries: 3,
	}

	// Retryable transient error on attempt 1
	if !policy.CanRetry(ev, errors.New("temporary network timeout")) {
		t.Fatalf("Expected event to be retryable on attempt 1")
	}

	// Fatal poison pill should never be retried
	if policy.CanRetry(ev, retry.ErrPoisonPillPayload) {
		t.Fatalf("Expected poison pill error to NOT be retryable")
	}

	// Non-retryable validation error
	if policy.CanRetry(ev, retry.ErrNonRetryableValidation) {
		t.Fatalf("Expected validation error to NOT be retryable")
	}

	// Exceeded max retries
	ev.Attempt = 3
	if policy.CanRetry(ev, errors.New("temporary error")) {
		t.Fatalf("Expected event with attempt 3 not to exceed max retries 3")
	}
}
