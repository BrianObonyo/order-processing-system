package config

import (
	"strings"
	"testing"
)

func TestLoadDefaults(t *testing.T) {
	setDefaultConfigEnv(t)
	t.Setenv("API_KEYS", "key-1234:user")

	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.PaymentMaxRecovery != 3 || len(cfg.KafkaBrokers) != 1 || cfg.KafkaBrokers[0] != "localhost:29092" {
		t.Fatalf("unexpected defaults: %+v", cfg)
	}
	if cfg.PaymentConsumerWorkers != 3 || cfg.PaymentGatewayAttempts != 1 ||
		cfg.KafkaPartitions != 3 || cfg.KafkaDLQTopic != "order-events.dead-letter" {
		t.Fatalf("unexpected event-processing defaults: %+v", cfg)
	}
}

func TestLoadRejectsMalformedConfiguration(t *testing.T) {
	tests := []struct {
		name  string
		key   string
		value string
	}{
		{name: "invalid boolean", key: "RUN_WORKERS", value: "sometimes"},
		{name: "invalid duration", key: "RECOVERY_INTERVAL", value: "soon"},
		{name: "invalid integer", key: "PAYMENT_MAX_RECOVERY", value: "many"},
		{name: "invalid rate", key: "PAYMENT_FAILURE_RATE", value: "1.5"},
		{name: "negative interval", key: "OUTBOX_POLL_INTERVAL", value: "-1s"},
		{name: "empty broker", key: "KAFKA_BROKERS", value: "localhost:9092, "},
		{name: "zero consumer workers", key: "PAYMENT_CONSUMER_WORKERS", value: "0"},
		{name: "zero gateway attempts", key: "PAYMENT_GATEWAY_MAX_ATTEMPTS", value: "0"},
		{name: "zero Kafka partitions", key: "KAFKA_PARTITIONS", value: "0"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			setDefaultConfigEnv(t)
			t.Setenv("API_KEYS", "key-1234:user")
			t.Setenv(tt.key, tt.value)

			_, err := Load()
			if err == nil || !strings.Contains(err.Error(), tt.key) {
				t.Fatalf("Load() error = %v, want an error mentioning %s", err, tt.key)
			}
		})
	}
}

func TestLoadRejectsOverlappingPaymentRates(t *testing.T) {
	setDefaultConfigEnv(t)
	t.Setenv("API_KEYS", "key-1234:user")
	t.Setenv("PAYMENT_FAILURE_RATE", "0.7")
	t.Setenv("PAYMENT_TRANSIENT_RATE", "0.4")

	if _, err := Load(); err == nil || !strings.Contains(err.Error(), "must sum to at most 1") {
		t.Fatalf("Load() error = %v, want overlapping payment rates error", err)
	}
}

func setDefaultConfigEnv(t *testing.T) {
	t.Helper()
	for _, key := range []string{
		"RUN_WORKERS",
		"OUTBOX_POLL_INTERVAL",
		"PAYMENT_FAILURE_RATE",
		"PAYMENT_TRANSIENT_RATE",
		"PAYMENT_LATENCY",
		"RECOVERY_INTERVAL",
		"ORDER_PAYMENT_TIMEOUT",
		"PAYMENT_RETRY_AFTER",
		"PAYMENT_MAX_RECOVERY",
		"PAYMENT_CONSUMER_WORKERS",
		"PAYMENT_GATEWAY_MAX_ATTEMPTS",
		"KAFKA_PARTITIONS",
		"KAFKA_BROKERS",
		"KAFKA_TOPIC",
		"KAFKA_DLQ_TOPIC",
	} {
		t.Setenv(key, "")
	}
}
