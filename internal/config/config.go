package config

import (
	"fmt"
	"math"
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	HTTPAddr    string
	DatabaseURL string
	RedisAddr   string

	KafkaBrokers    []string
	KafkaTopic      string
	KafkaDLQTopic   string
	KafkaGroupID    string
	KafkaPartitions int

	// APIKeys maps API key -> user ID. Dev-grade authentication; swap the
	// middleware for JWT/OIDC in a real deployment.
	APIKeys map[string]string

	RunWorkers bool

	PaymentConsumerWorkers int
	PaymentGatewayAttempts int

	OutboxPollInterval time.Duration

	PaymentFailureRate   float64 // permanent declines
	PaymentTransientRate float64 // temporary gateway errors
	PaymentLatency       time.Duration

	RecoveryInterval    time.Duration
	OrderPaymentTimeout time.Duration // pending orders older than this are cancelled
	PaymentRetryAfter   time.Duration // pending orders idle this long get their event re-emitted
	PaymentMaxRecovery  int
}

func Load() (*Config, error) {
	runWorkers, err := envBool("RUN_WORKERS", true)
	if err != nil {
		return nil, err
	}
	outboxPollInterval, err := envDuration("OUTBOX_POLL_INTERVAL", 300*time.Millisecond)
	if err != nil {
		return nil, err
	}
	paymentFailureRate, err := envFloat("PAYMENT_FAILURE_RATE", 0.1)
	if err != nil {
		return nil, err
	}
	paymentTransientRate, err := envFloat("PAYMENT_TRANSIENT_RATE", 0.1)
	if err != nil {
		return nil, err
	}
	paymentLatency, err := envDuration("PAYMENT_LATENCY", 100*time.Millisecond)
	if err != nil {
		return nil, err
	}
	recoveryInterval, err := envDuration("RECOVERY_INTERVAL", 15*time.Second)
	if err != nil {
		return nil, err
	}
	orderPaymentTimeout, err := envDuration("ORDER_PAYMENT_TIMEOUT", 2*time.Minute)
	if err != nil {
		return nil, err
	}
	paymentRetryAfter, err := envDuration("PAYMENT_RETRY_AFTER", 30*time.Second)
	if err != nil {
		return nil, err
	}
	paymentMaxRecovery, err := envInt("PAYMENT_MAX_RECOVERY", 3)
	if err != nil {
		return nil, err
	}
	paymentConsumerWorkers, err := envInt("PAYMENT_CONSUMER_WORKERS", 3)
	if err != nil {
		return nil, err
	}
	paymentGatewayAttempts, err := envInt("PAYMENT_GATEWAY_MAX_ATTEMPTS", 1)
	if err != nil {
		return nil, err
	}
	kafkaPartitions, err := envInt("KAFKA_PARTITIONS", 3)
	if err != nil {
		return nil, err
	}

	kafkaTopic := strings.TrimSpace(env("KAFKA_TOPIC", "order-events"))
	c := &Config{
		HTTPAddr:               env("HTTP_ADDR", ":8080"),
		DatabaseURL:            env("DATABASE_URL", "postgres://orders:orders@localhost:5432/orders?sslmode=disable"),
		RedisAddr:              env("REDIS_ADDR", "localhost:6379"),
		KafkaBrokers:           strings.Split(env("KAFKA_BROKERS", "localhost:29092"), ","),
		KafkaTopic:             kafkaTopic,
		KafkaDLQTopic:          strings.TrimSpace(env("KAFKA_DLQ_TOPIC", kafkaTopic+".dead-letter")),
		KafkaGroupID:           env("KAFKA_GROUP_ID", "order-processing"),
		KafkaPartitions:        kafkaPartitions,
		RunWorkers:             runWorkers,
		PaymentConsumerWorkers: paymentConsumerWorkers,
		PaymentGatewayAttempts: paymentGatewayAttempts,
		OutboxPollInterval:     outboxPollInterval,
		PaymentFailureRate:     paymentFailureRate,
		PaymentTransientRate:   paymentTransientRate,
		PaymentLatency:         paymentLatency,
		RecoveryInterval:       recoveryInterval,
		OrderPaymentTimeout:    orderPaymentTimeout,
		PaymentRetryAfter:      paymentRetryAfter,
		PaymentMaxRecovery:     paymentMaxRecovery,
		APIKeys:                map[string]string{},
	}

	for i, broker := range c.KafkaBrokers {
		c.KafkaBrokers[i] = strings.TrimSpace(broker)
		if c.KafkaBrokers[i] == "" {
			return nil, fmt.Errorf("KAFKA_BROKERS must not contain empty entries")
		}
	}

	for _, pair := range strings.Split(env("API_KEYS", "dev-key-1:user-1,dev-key-2:user-2"), ",") {
		pair = strings.TrimSpace(pair)
		if pair == "" {
			continue
		}
		key, user, ok := strings.Cut(pair, ":")
		key, user = strings.TrimSpace(key), strings.TrimSpace(user)
		if !ok || key == "" || user == "" {
			return nil, fmt.Errorf("API_KEYS entry %q must look like key:user", pair)
		}
		if _, exists := c.APIKeys[key]; exists {
			return nil, fmt.Errorf("API_KEYS contains duplicate key %q", key)
		}
		c.APIKeys[key] = user
	}
	if len(c.APIKeys) == 0 {
		return nil, fmt.Errorf("API_KEYS must contain at least one key:user pair")
	}
	if err := c.validate(); err != nil {
		return nil, err
	}
	return c, nil
}

func (c *Config) validate() error {
	if !validRate(c.PaymentFailureRate) {
		return fmt.Errorf("PAYMENT_FAILURE_RATE must be between 0 and 1")
	}
	if !validRate(c.PaymentTransientRate) {
		return fmt.Errorf("PAYMENT_TRANSIENT_RATE must be between 0 and 1")
	}
	if c.PaymentFailureRate+c.PaymentTransientRate > 1 {
		return fmt.Errorf("PAYMENT_FAILURE_RATE and PAYMENT_TRANSIENT_RATE must sum to at most 1")
	}
	if c.PaymentLatency < 0 {
		return fmt.Errorf("PAYMENT_LATENCY must not be negative")
	}
	if c.OutboxPollInterval <= 0 {
		return fmt.Errorf("OUTBOX_POLL_INTERVAL must be positive")
	}
	if c.RecoveryInterval <= 0 {
		return fmt.Errorf("RECOVERY_INTERVAL must be positive")
	}
	if c.OrderPaymentTimeout <= 0 {
		return fmt.Errorf("ORDER_PAYMENT_TIMEOUT must be positive")
	}
	if c.PaymentRetryAfter <= 0 {
		return fmt.Errorf("PAYMENT_RETRY_AFTER must be positive")
	}
	if c.PaymentMaxRecovery < 1 {
		return fmt.Errorf("PAYMENT_MAX_RECOVERY must be at least 1")
	}
	if c.PaymentConsumerWorkers < 1 {
		return fmt.Errorf("PAYMENT_CONSUMER_WORKERS must be at least 1")
	}
	if c.PaymentGatewayAttempts < 1 {
		return fmt.Errorf("PAYMENT_GATEWAY_MAX_ATTEMPTS must be at least 1")
	}
	if c.KafkaPartitions < 1 {
		return fmt.Errorf("KAFKA_PARTITIONS must be at least 1")
	}
	if strings.TrimSpace(c.KafkaTopic) == "" {
		return fmt.Errorf("KAFKA_TOPIC must not be empty")
	}
	if strings.TrimSpace(c.KafkaDLQTopic) == "" {
		return fmt.Errorf("KAFKA_DLQ_TOPIC must not be empty")
	}
	if c.KafkaTopic == c.KafkaDLQTopic {
		return fmt.Errorf("KAFKA_DLQ_TOPIC must differ from KAFKA_TOPIC")
	}
	return nil
}

func validRate(rate float64) bool {
	return !math.IsNaN(rate) && !math.IsInf(rate, 0) && rate >= 0 && rate <= 1
}

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func envBool(key string, def bool) (bool, error) {
	v := os.Getenv(key)
	if v == "" {
		return def, nil
	}
	value, err := strconv.ParseBool(v)
	if err != nil {
		return false, fmt.Errorf("%s: %w", key, err)
	}
	return value, nil
}

func envInt(key string, def int) (int, error) {
	v := os.Getenv(key)
	if v == "" {
		return def, nil
	}
	value, err := strconv.Atoi(v)
	if err != nil {
		return 0, fmt.Errorf("%s: %w", key, err)
	}
	return value, nil
}

func envFloat(key string, def float64) (float64, error) {
	v := os.Getenv(key)
	if v == "" {
		return def, nil
	}
	value, err := strconv.ParseFloat(v, 64)
	if err != nil {
		return 0, fmt.Errorf("%s: %w", key, err)
	}
	return value, nil
}

func envDuration(key string, def time.Duration) (time.Duration, error) {
	v := os.Getenv(key)
	if v == "" {
		return def, nil
	}
	value, err := time.ParseDuration(v)
	if err != nil {
		return 0, fmt.Errorf("%s: %w", key, err)
	}
	return value, nil
}
