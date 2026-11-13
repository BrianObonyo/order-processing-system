// Package metrics declares the Prometheus collectors used across modules.
package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	OrdersCreated = promauto.NewCounter(prometheus.CounterOpts{
		Name: "orders_created_total", Help: "Orders accepted and stock reserved.",
	})
	OrdersRejected = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "orders_rejected_total", Help: "Orders rejected before creation.",
	}, []string{"reason"})
	IdempotentReplays = promauto.NewCounter(prometheus.CounterOpts{
		Name: "idempotent_replays_total", Help: "Requests answered from an earlier identical request.",
	})
	PaymentsProcessed = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "payments_processed_total", Help: "Payment outcomes.",
	}, []string{"result"})
	OutboxPublished = promauto.NewCounter(prometheus.CounterOpts{
		Name: "outbox_events_published_total", Help: "Outbox events relayed to Kafka.",
	})
	RecoveryActions = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "recovery_actions_total", Help: "Recovery worker actions.",
	}, []string{"action"})
	HTTPDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name: "http_request_duration_seconds", Help: "HTTP request latency.",
		Buckets: prometheus.DefBuckets,
	}, []string{"method", "route", "status"})
)
