package services

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"sync"
	"time"

	"github.com/sony/gobreaker"

	"github.com/BrianObonyo/order-processing-system/internal/domain/payment"
)

// SimulatedGateway stands in for a real payment provider. Like a real one it is
// idempotent: the outcome for a given key is remembered and replayed.
type SimulatedGateway struct {
	declineRate   float64
	transientRate float64
	latency       time.Duration

	mu      sync.Mutex
	results map[string]error
}

func NewSimulatedGateway(declineRate, transientRate float64, latency time.Duration) *SimulatedGateway {
	return &SimulatedGateway{
		declineRate: declineRate, transientRate: transientRate, latency: latency,
		results: make(map[string]error),
	}
}

func (g *SimulatedGateway) Charge(ctx context.Context, key string, _ int64) error {
	select {
	case <-time.After(g.latency):
	case <-ctx.Done():
		return ctx.Err()
	}

	g.mu.Lock()
	defer g.mu.Unlock()
	if res, ok := g.results[key]; ok {
		return res
	}
	roll := rand.Float64()
	switch {
	case roll < g.declineRate:
		g.results[key] = payment.ErrDeclined
		return payment.ErrDeclined
	case roll < g.declineRate+g.transientRate:
		return payment.ErrTransient // not remembered: a retry may succeed
	default:
		g.results[key] = nil
		return nil
	}
}

// ResilientGateway adds retries with exponential backoff and a circuit breaker.
// Declines are business outcomes, not failures: they are never retried and never
// trip the breaker.
type ResilientGateway struct {
	inner       payment.Gateway
	breaker     *gobreaker.CircuitBreaker
	maxAttempts int
	baseDelay   time.Duration
}

func NewResilientGateway(inner payment.Gateway, maxAttempts int, baseDelay time.Duration) *ResilientGateway {
	return &ResilientGateway{
		inner:       inner,
		maxAttempts: maxAttempts,
		baseDelay:   baseDelay,
		breaker: gobreaker.NewCircuitBreaker(gobreaker.Settings{
			Name:        "payment-gateway",
			MaxRequests: 3,
			Interval:    30 * time.Second,
			Timeout:     15 * time.Second,
			ReadyToTrip: func(c gobreaker.Counts) bool {
				return c.Requests >= 10 && float64(c.TotalFailures)/float64(c.Requests) >= 0.5
			},
			OnStateChange: func(name string, from, to gobreaker.State) {
				slog.Warn("circuit breaker state change", "name", name, "from", from.String(), "to", to.String())
			},
		}),
	}
}

func (g *ResilientGateway) Charge(ctx context.Context, key string, amountCents int64) error {
	var last error
	delay := g.baseDelay
	for attempt := 1; attempt <= g.maxAttempts; attempt++ {
		res, err := g.breaker.Execute(func() (any, error) {
			err := g.inner.Charge(ctx, key, amountCents)
			if errors.Is(err, payment.ErrDeclined) {
				return err, nil // a decline is a successful round trip
			}
			return nil, err
		})
		if err == nil {
			if declined, ok := res.(error); ok {
				return declined
			}
			return nil
		}
		last = err
		if attempt == g.maxAttempts {
			break
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(delay):
		}
		delay *= 2
	}
	return fmt.Errorf("charge failed after %d attempts: %w", g.maxAttempts, errors.Join(payment.ErrTransient, last))
}
