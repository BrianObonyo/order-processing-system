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
	results map[string]payment.ChargeStatus
	refunds map[string]bool
}

func NewSimulatedGateway(declineRate, transientRate float64, latency time.Duration) *SimulatedGateway {
	return &SimulatedGateway{
		declineRate: declineRate, transientRate: transientRate, latency: latency,
		results: make(map[string]payment.ChargeStatus),
		refunds: make(map[string]bool),
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
	if status, ok := g.results[key]; ok {
		switch status {
		case payment.ChargeStatusSucceeded:
			return nil
		case payment.ChargeStatusDeclined:
			return payment.ErrDeclined
		default:
			return payment.ErrTransient
		}
	}
	roll := rand.Float64()
	switch {
	case roll < g.declineRate:
		g.results[key] = payment.ChargeStatusDeclined
		return payment.ErrDeclined
	case roll < g.declineRate+g.transientRate:
		return payment.ErrTransient // not remembered: a retry may succeed
	default:
		g.results[key] = payment.ChargeStatusSucceeded
		return nil
	}
}

func (g *SimulatedGateway) Refund(ctx context.Context, paymentID string, _ int64) error {
	if err := g.wait(ctx); err != nil {
		return err
	}

	g.mu.Lock()
	defer g.mu.Unlock()
	if g.refunds[paymentID] {
		return nil
	}
	if g.results[paymentID] != payment.ChargeStatusSucceeded {
		return errors.New("cannot refund a charge that did not succeed")
	}
	if rand.Float64() < g.transientRate {
		return payment.ErrTransient
	}
	g.refunds[paymentID] = true
	return nil
}

func (g *SimulatedGateway) GetCharge(ctx context.Context, key string) (payment.ChargeStatus, error) {
	if err := g.wait(ctx); err != nil {
		return "", err
	}

	g.mu.Lock()
	defer g.mu.Unlock()
	if status, ok := g.results[key]; ok {
		return status, nil
	}
	return payment.ChargeStatusNotFound, nil
}

func (g *SimulatedGateway) wait(ctx context.Context) error {
	select {
	case <-time.After(g.latency):
		return nil
	case <-ctx.Done():
		return ctx.Err()
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

func (g *ResilientGateway) Refund(ctx context.Context, paymentID string, amountCents int64) error {
	delay := g.baseDelay
	for attempt := 1; attempt <= g.maxAttempts; attempt++ {
		_, err := g.breaker.Execute(func() (any, error) {
			return nil, g.inner.Refund(ctx, paymentID, amountCents)
		})
		if err == nil {
			return nil
		}
		if !errors.Is(err, payment.ErrTransient) || attempt == g.maxAttempts {
			return fmt.Errorf("refund failed after %d attempts: %w", attempt, err)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(delay):
		}
		delay *= 2
	}
	return fmt.Errorf("refund failed after %d attempts", g.maxAttempts)
}

func (g *ResilientGateway) GetCharge(ctx context.Context, key string) (payment.ChargeStatus, error) {
	result, err := g.breaker.Execute(func() (any, error) {
		return g.inner.GetCharge(ctx, key)
	})
	if err != nil {
		return "", err
	}
	status, ok := result.(payment.ChargeStatus)
	if !ok {
		return "", errors.New("payment gateway returned an invalid charge status")
	}
	return status, nil
}
