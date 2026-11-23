package services

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/BrianObonyo/order-processing-system/internal/domain/payment"
)

type GatewaySettings struct {
	DeclineRate      float64
	TransientRate    float64
	Latency          time.Duration
	ProviderBaseURL  string
	ProviderClientID string
	ProviderSecret   string
}

type GatewayFactory func(GatewaySettings) (payment.Gateway, error)

type GatewayRegistry struct {
	factories map[string]GatewayFactory
}

func NewGatewayRegistry() *GatewayRegistry {
	return &GatewayRegistry{factories: map[string]GatewayFactory{
		payment.ProviderSimulated: func(settings GatewaySettings) (payment.Gateway, error) {
			return NewSimulatedGateway(settings.DeclineRate, settings.TransientRate, settings.Latency), nil
		},
		payment.ProviderPayPal: func(settings GatewaySettings) (payment.Gateway, error) {
			return NewPayPalGateway(settings.ProviderBaseURL, settings.ProviderClientID, settings.ProviderSecret)
		},
	}}
}

func (r *GatewayRegistry) Register(name string, factory GatewayFactory) error {
	if r == nil {
		return errors.New("payment gateway registry is nil")
	}
	name = normalizeGatewayProvider(name)
	if name == "" {
		return errors.New("payment gateway provider name is required")
	}
	if factory == nil {
		return fmt.Errorf("payment gateway provider %q has no factory", name)
	}
	if _, exists := r.factories[name]; exists {
		return fmt.Errorf("payment gateway provider %q is already registered", name)
	}
	if r.factories == nil {
		r.factories = make(map[string]GatewayFactory)
	}
	r.factories[name] = factory
	return nil
}

func (r *GatewayRegistry) Build(name string, settings GatewaySettings) (payment.Gateway, error) {
	name = normalizeGatewayProvider(name)
	if r == nil {
		return nil, errors.New("payment gateway registry is nil")
	}
	factory, exists := r.factories[name]
	if !exists {
		return nil, fmt.Errorf("payment gateway provider %q is not registered", name)
	}
	gateway, err := factory(settings)
	if err != nil {
		return nil, fmt.Errorf("create payment gateway provider %q: %w", name, err)
	}
	if gateway == nil {
		return nil, fmt.Errorf("payment gateway provider %q returned no gateway", name)
	}
	return gateway, nil
}

func normalizeGatewayProvider(name string) string {
	return strings.ToLower(strings.TrimSpace(name))
}
