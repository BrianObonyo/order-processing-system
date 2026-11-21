package config

import (
	"strings"
	"testing"
	"time"

	"github.com/BrianObonyo/order-processing-system/internal/domain/payment"
)

func TestLoadDefaults(t *testing.T) {
	setDefaultConfigEnv(t)
	t.Setenv("API_KEYS", "key-1234:default:user:user")

	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.PaymentMaxRecovery != 3 || len(cfg.KafkaBrokers) != 1 || cfg.KafkaBrokers[0] != "localhost:29092" {
		t.Fatalf("unexpected defaults: %+v", cfg)
	}
	if cfg.PaymentConsumerWorkers != 3 || cfg.PaymentGatewayAttempts != 1 ||
		cfg.PaymentGatewayProvider != payment.ProviderSimulated ||
		cfg.KafkaPartitions != 3 || cfg.KafkaDLQTopic != "order-events.dead-letter" {
		t.Fatalf("unexpected event-processing defaults: %+v", cfg)
	}
	if cfg.HTTPRateLimit != 200 || cfg.HTTPRateBurst != 400 ||
		cfg.HTTPMaxBodyBytes != 1<<20 || cfg.OutboxRetention != 14*24*time.Hour ||
		cfg.IdempotencyRetention != 30*24*time.Hour || cfg.RetentionInterval != time.Hour ||
		cfg.RetentionBatchSize != 5000 {
		t.Fatalf("unexpected HTTP or retention defaults: %+v", cfg)
	}
}

func TestLoadRequiresProductionSecretsAndEndpoints(t *testing.T) {
	setDefaultConfigEnv(t)
	t.Setenv("APP_ENV", "production")
	t.Setenv("DATABASE_URL", "")
	t.Setenv("REDIS_ADDR", "")
	t.Setenv("KAFKA_BROKERS", "")
	t.Setenv("API_KEYS", "")

	_, err := Load()
	if err == nil || !strings.Contains(err.Error(), "DATABASE_URL") {
		t.Fatalf("Load() error = %v, want DATABASE_URL required", err)
	}
}

func TestLoadAcceptsStrongProductionConfiguration(t *testing.T) {
	setProductionConfigEnv(t)

	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.AppEnv != "production" || cfg.MetricsToken == "" {
		t.Fatalf("unexpected production config: %+v", cfg)
	}
}

func TestLoadRejectsWeakProductionCredentials(t *testing.T) {
	setProductionConfigEnv(t)
	t.Setenv("API_KEYS", "dev-key-1:default:user-1:user")

	_, err := Load()
	if err == nil || !strings.Contains(err.Error(), "32 characters") {
		t.Fatalf("Load() error = %v, want weak production API key rejection", err)
	}
}

func TestLoadRequiresProductionGatewayProvider(t *testing.T) {
	setProductionConfigEnv(t)
	t.Setenv("PAYMENT_GATEWAY_PROVIDER", "")

	_, err := Load()
	if err == nil || !strings.Contains(err.Error(), "PAYMENT_GATEWAY_PROVIDER") {
		t.Fatalf("Load() error=%v, want production gateway provider required", err)
	}
}

func TestLoadRejectsSimulatedGatewayInProduction(t *testing.T) {
	setProductionConfigEnv(t)
	t.Setenv("PAYMENT_GATEWAY_PROVIDER", payment.ProviderSimulated)

	_, err := Load()
	if err == nil || !strings.Contains(err.Error(), "not allowed in production") {
		t.Fatalf("Load() error=%v, want simulated gateway rejected in production", err)
	}
}

func TestLoadRejectsDevelopmentDatabaseCredentialsInProduction(t *testing.T) {
	setProductionConfigEnv(t)
	t.Setenv("DATABASE_URL", "postgres://orders:orders@postgres:5432/orders?sslmode=disable")

	_, err := Load()
	if err == nil || !strings.Contains(err.Error(), "development database credentials") {
		t.Fatalf("Load() error = %v, want development database credential rejection", err)
	}
}

func TestLoadDoesNotExposeAPIKeysInConfigurationErrors(t *testing.T) {
	const secret = "sensitive-development-key"
	tests := []struct {
		name  string
		value string
	}{
		{name: "malformed entry", value: secret + ":tenant:user:"},
		{name: "duplicate entry", value: secret + ":tenant:user-1:user," + secret + ":tenant:user-2:admin"},
		{name: "invalid role", value: secret + ":tenant:user-1:owner"},
		{name: "empty tenant", value: secret + "::user-1:user"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			setDefaultConfigEnv(t)
			t.Setenv("API_KEYS", tt.value)

			_, err := Load()
			if err == nil || strings.Contains(err.Error(), secret) {
				t.Fatalf("Load() error = %v, want a sanitized configuration error", err)
			}
		})
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
		{name: "zero HTTP rate", key: "HTTP_RATE_LIMIT", value: "0"},
		{name: "zero HTTP burst", key: "HTTP_RATE_BURST", value: "0"},
		{name: "zero body limit", key: "HTTP_MAX_BODY_BYTES", value: "0"},
		{name: "zero outbox retention", key: "OUTBOX_RETENTION", value: "0s"},
		{name: "zero idempotency retention", key: "IDEMPOTENCY_RETENTION", value: "0s"},
		{name: "zero retention interval", key: "RETENTION_INTERVAL", value: "0s"},
		{name: "zero retention batch", key: "RETENTION_BATCH_SIZE", value: "0"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			setDefaultConfigEnv(t)
			t.Setenv("API_KEYS", "key-1234:default:user:user")
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
	t.Setenv("API_KEYS", "key-1234:default:user:user")
	t.Setenv("PAYMENT_FAILURE_RATE", "0.7")
	t.Setenv("PAYMENT_TRANSIENT_RATE", "0.4")

	if _, err := Load(); err == nil || !strings.Contains(err.Error(), "must sum to at most 1") {
		t.Fatalf("Load() error = %v, want overlapping payment rates error", err)
	}
}

func setDefaultConfigEnv(t *testing.T) {
	t.Helper()
	for _, key := range []string{
		"APP_ENV",
		"DATABASE_URL",
		"REDIS_ADDR",
		"API_KEYS",
		"METRICS_TOKEN",
		"RUN_WORKERS",
		"OUTBOX_POLL_INTERVAL",
		"HTTP_RATE_LIMIT",
		"HTTP_RATE_BURST",
		"HTTP_MAX_BODY_BYTES",
		"OUTBOX_RETENTION",
		"IDEMPOTENCY_RETENTION",
		"RETENTION_INTERVAL",
		"RETENTION_BATCH_SIZE",
		"PAYMENT_FAILURE_RATE",
		"PAYMENT_TRANSIENT_RATE",
		"PAYMENT_LATENCY",
		"RECOVERY_INTERVAL",
		"ORDER_PAYMENT_TIMEOUT",
		"PAYMENT_RETRY_AFTER",
		"PAYMENT_MAX_RECOVERY",
		"PAYMENT_CONSUMER_WORKERS",
		"PAYMENT_GATEWAY_MAX_ATTEMPTS",
		"PAYMENT_GATEWAY_PROVIDER",
		"KAFKA_PARTITIONS",
		"KAFKA_BROKERS",
		"KAFKA_TOPIC",
		"KAFKA_DLQ_TOPIC",
	} {
		t.Setenv(key, "")
	}
	t.Setenv("APP_ENV", "development")
	t.Setenv("DATABASE_URL", "postgres://orders:orders@localhost:5432/orders?sslmode=disable")
	t.Setenv("API_KEYS", "dev-key-1:default:user-1:user,dev-key-2:default:user-2:user")
}

func setProductionConfigEnv(t *testing.T) {
	t.Helper()
	setDefaultConfigEnv(t)
	t.Setenv("APP_ENV", "production")
	t.Setenv("DATABASE_URL", "postgres://orders:strong-production-secret@postgres:5432/orders?sslmode=require")
	t.Setenv("REDIS_ADDR", "redis:6379")
	t.Setenv("KAFKA_BROKERS", "kafka:9092")
	t.Setenv("API_KEYS", strings.Repeat("a", 32)+":default:service-user:admin")
	t.Setenv("METRICS_TOKEN", strings.Repeat("m", 32))
	t.Setenv("PAYMENT_GATEWAY_PROVIDER", "provider-test")
}
