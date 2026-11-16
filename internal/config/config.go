package config

import (
	"fmt"
	"math"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	AppEnv      string
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
	APIKeys      map[string]string
	MetricsToken string

	RunWorkers bool

	PaymentConsumerWorkers int
	PaymentGatewayAttempts int
	HTTPRateLimit          float64
	HTTPRateBurst          int
	HTTPMaxBodyBytes       int64

	OutboxRetention      time.Duration
	IdempotencyRetention time.Duration
	RetentionInterval    time.Duration
	RetentionBatchSize   int

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
	appEnv := strings.ToLower(strings.TrimSpace(os.Getenv("APP_ENV")))
	if appEnv == "" {
		appEnv = "production"
	}
	if appEnv != "development" && appEnv != "production" {
		return nil, fmt.Errorf("APP_ENV must be development or production")
	}
	databaseURL, err := envRequired("DATABASE_URL")
	if err != nil {
		return nil, err
	}
	redisAddr, err := envOrDevelopmentDefault("REDIS_ADDR", "localhost:6379", appEnv)
	if err != nil {
		return nil, err
	}
	kafkaBrokers, err := envOrDevelopmentDefault("KAFKA_BROKERS", "localhost:29092", appEnv)
	if err != nil {
		return nil, err
	}
	apiKeys, err := envRequired("API_KEYS")
	if err != nil {
		return nil, err
	}

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
	httpRateLimit, err := envFloat("HTTP_RATE_LIMIT", 200)
	if err != nil {
		return nil, err
	}
	httpRateBurst, err := envInt("HTTP_RATE_BURST", 400)
	if err != nil {
		return nil, err
	}
	httpMaxBodyBytes, err := envInt("HTTP_MAX_BODY_BYTES", 1<<20)
	if err != nil {
		return nil, err
	}
	outboxRetention, err := envDuration("OUTBOX_RETENTION", 14*24*time.Hour)
	if err != nil {
		return nil, err
	}
	idempotencyRetention, err := envDuration("IDEMPOTENCY_RETENTION", 30*24*time.Hour)
	if err != nil {
		return nil, err
	}
	retentionInterval, err := envDuration("RETENTION_INTERVAL", time.Hour)
	if err != nil {
		return nil, err
	}
	retentionBatchSize, err := envInt("RETENTION_BATCH_SIZE", 5000)
	if err != nil {
		return nil, err
	}

	kafkaTopic := strings.TrimSpace(env("KAFKA_TOPIC", "order-events"))
	c := &Config{
		AppEnv:                 appEnv,
		HTTPAddr:               env("HTTP_ADDR", ":8080"),
		DatabaseURL:            databaseURL,
		RedisAddr:              redisAddr,
		KafkaBrokers:           strings.Split(kafkaBrokers, ","),
		KafkaTopic:             kafkaTopic,
		KafkaDLQTopic:          strings.TrimSpace(env("KAFKA_DLQ_TOPIC", kafkaTopic+".dead-letter")),
		KafkaGroupID:           env("KAFKA_GROUP_ID", "order-processing"),
		KafkaPartitions:        kafkaPartitions,
		MetricsToken:           strings.TrimSpace(os.Getenv("METRICS_TOKEN")),
		RunWorkers:             runWorkers,
		PaymentConsumerWorkers: paymentConsumerWorkers,
		PaymentGatewayAttempts: paymentGatewayAttempts,
		HTTPRateLimit:          httpRateLimit,
		HTTPRateBurst:          httpRateBurst,
		HTTPMaxBodyBytes:       int64(httpMaxBodyBytes),
		OutboxRetention:        outboxRetention,
		IdempotencyRetention:   idempotencyRetention,
		RetentionInterval:      retentionInterval,
		RetentionBatchSize:     retentionBatchSize,
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

	for _, pair := range strings.Split(apiKeys, ",") {
		pair = strings.TrimSpace(pair)
		if pair == "" {
			continue
		}
		key, user, ok := strings.Cut(pair, ":")
		key, user = strings.TrimSpace(key), strings.TrimSpace(user)
		if !ok || key == "" || user == "" {
			return nil, fmt.Errorf("API_KEYS entries must look like key:user")
		}
		if _, exists := c.APIKeys[key]; exists {
			return nil, fmt.Errorf("API_KEYS contains duplicate entries")
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
	if c.HTTPRateLimit <= 0 || math.IsNaN(c.HTTPRateLimit) || math.IsInf(c.HTTPRateLimit, 0) {
		return fmt.Errorf("HTTP_RATE_LIMIT must be positive")
	}
	if c.HTTPRateBurst < 1 {
		return fmt.Errorf("HTTP_RATE_BURST must be at least 1")
	}
	if c.HTTPMaxBodyBytes < 1 {
		return fmt.Errorf("HTTP_MAX_BODY_BYTES must be at least 1")
	}
	if c.OutboxRetention <= 0 {
		return fmt.Errorf("OUTBOX_RETENTION must be positive")
	}
	if c.IdempotencyRetention <= 0 {
		return fmt.Errorf("IDEMPOTENCY_RETENTION must be positive")
	}
	if c.RetentionInterval <= 0 {
		return fmt.Errorf("RETENTION_INTERVAL must be positive")
	}
	if c.RetentionBatchSize < 1 {
		return fmt.Errorf("RETENTION_BATCH_SIZE must be at least 1")
	}
	if c.AppEnv == "production" {
		databaseURL, err := url.Parse(c.DatabaseURL)
		if err != nil || (databaseURL.Scheme != "postgres" && databaseURL.Scheme != "postgresql") || databaseURL.Host == "" {
			return fmt.Errorf("DATABASE_URL is invalid")
		}
		if databaseURL.User != nil {
			password, hasPassword := databaseURL.User.Password()
			if databaseURL.User.Username() == "orders" && hasPassword && password == "orders" {
				return fmt.Errorf("DATABASE_URL must not use the development database credentials in production")
			}
		}
		for key := range c.APIKeys {
			if len(key) < 32 {
				return fmt.Errorf("production API_KEYS entries must be at least 32 characters")
			}
		}
		if len(c.MetricsToken) < 32 {
			return fmt.Errorf("production METRICS_TOKEN must be at least 32 characters")
		}
	}
	return nil
}

func envRequired(key string) (string, error) {
	value := strings.TrimSpace(os.Getenv(key))
	if value != "" {
		return value, nil
	}
	return "", fmt.Errorf("%s is required", key)
}

func envOrDevelopmentDefault(key, developmentDefault, appEnv string) (string, error) {
	value := strings.TrimSpace(os.Getenv(key))
	if value != "" {
		return value, nil
	}
	if appEnv == "development" {
		return developmentDefault, nil
	}
	return "", fmt.Errorf("%s is required when APP_ENV=production", key)
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
