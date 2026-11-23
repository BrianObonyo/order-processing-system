package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/BrianObonyo/order-processing-system/internal/config"
	"github.com/BrianObonyo/order-processing-system/internal/handlers"
	kafkainfra "github.com/BrianObonyo/order-processing-system/internal/infrastructure/kafka"
	"github.com/BrianObonyo/order-processing-system/internal/infrastructure/postgres"
	redisinfra "github.com/BrianObonyo/order-processing-system/internal/infrastructure/redis"
	"github.com/BrianObonyo/order-processing-system/internal/messaging"
	"github.com/BrianObonyo/order-processing-system/internal/middleware"
	"github.com/BrianObonyo/order-processing-system/internal/services"
	"github.com/BrianObonyo/order-processing-system/internal/workers"
)

func main() {
	if err := run(); err != nil {
		slog.Error("fatal", "err", err)
		os.Exit(1)
	}
}

func run() error {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))

	cfg, err := config.Load()
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	pool, err := postgres.NewPool(ctx, cfg.DatabaseURL)
	if err != nil {
		return err
	}
	defer pool.Close()
	if cfg.AuthMode == "api_key" {
		if err := postgres.ValidateTenants(ctx, pool, cfg.APIKeys); err != nil {
			return err
		}
	}

	rds := redisinfra.New(cfg.RedisAddr)
	defer func() {
		if err := rds.Close(); err != nil {
			slog.Error("redis close failed", "err", err)
		}
	}()

	if err := kafkainfra.EnsureTopic(ctx, cfg.KafkaBrokers, cfg.KafkaTopic, cfg.KafkaPartitions); err != nil {
		return err
	}
	if err := kafkainfra.EnsureTopic(ctx, cfg.KafkaBrokers, cfg.KafkaDLQTopic, cfg.KafkaPartitions); err != nil {
		return err
	}
	producer := kafkainfra.NewProducer(cfg.KafkaBrokers, cfg.KafkaTopic)
	defer func() {
		if err := producer.Close(); err != nil {
			slog.Error("Kafka producer close failed", "err", err)
		}
	}()
	dlqProducer := kafkainfra.NewProducer(cfg.KafkaBrokers, cfg.KafkaDLQTopic)
	defer func() {
		if err := dlqProducer.Close(); err != nil {
			slog.Error("Kafka DLQ producer close failed", "err", err)
		}
	}()

	// Modules.
	store := postgres.NewStore(pool)
	orderSvc := services.NewOrderService(store)
	productSvc := services.NewProductService(store)
	gatewayAdapter, err := services.NewGatewayRegistry().Build(cfg.PaymentGatewayProvider, services.GatewaySettings{
		DeclineRate: cfg.PaymentFailureRate, TransientRate: cfg.PaymentTransientRate, Latency: cfg.PaymentLatency,
		ProviderBaseURL: cfg.PaymentProviderBaseURL, ProviderClientID: cfg.PaymentProviderClientID, ProviderSecret: cfg.PaymentProviderSecret,
	})
	if err != nil {
		return err
	}
	gateway := services.NewResilientGateway(gatewayAdapter, cfg.PaymentGatewayAttempts, 200*time.Millisecond)
	paymentSvc := services.NewPaymentService(store, gateway)
	recoverySvc := services.NewRecoveryService(store, paymentSvc, cfg.OrderPaymentTimeout, cfg.PaymentRetryAfter, cfg.PaymentMaxRecovery)

	// Background workers.
	var wg sync.WaitGroup
	start := func(fn func(context.Context)) {
		wg.Add(1)
		go func() { defer wg.Done(); fn(ctx) }()
	}
	if cfg.RunWorkers {
		start(messaging.NewOutboxRelay(pool, producer, cfg.OutboxPollInterval).Run)
		for range cfg.PaymentConsumerWorkers {
			start(func(ctx context.Context) {
				runPaymentConsumer(ctx, cfg, paymentSvc, dlqProducer)
			})
		}
		start(workers.NewRecoveryWorker(recoverySvc, rds, cfg.RecoveryInterval).Run)
		start(workers.NewRetentionWorker(pool, cfg.RetentionInterval, cfg.OutboxRetention, cfg.IdempotencyRetention, cfg.RetentionBatchSize).Run)
	}

	var authMiddleware gin.HandlerFunc
	if cfg.AuthMode == "oidc" {
		authMiddleware, err = middleware.OIDCAuth(cfg.OIDCIssuer, cfg.OIDCAudience)
		if err != nil {
			return err
		}
	}
	router := handlers.NewRouter(handlers.Deps{
		Orders:              orderSvc,
		Products:            productSvc,
		Payments:            paymentSvc,
		APIKeys:             cfg.APIKeys,
		Auth:                authMiddleware,
		MetricsToken:        cfg.MetricsToken,
		Locker:              rds,
		MaxRequestBodyBytes: cfg.HTTPMaxBodyBytes,
		HTTPRateLimit:       cfg.HTTPRateLimit,
		HTTPRateBurst:       cfg.HTTPRateBurst,
		Readiness: map[string]func(context.Context) error{
			"postgres": pool.Ping,
			"redis":    rds.Ping,
		},
	})
	srv := &http.Server{
		Addr:              cfg.HTTPAddr,
		Handler:           router,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       60 * time.Second,
		MaxHeaderBytes:    64 << 10,
	}

	errCh := make(chan error, 1)
	go func() {
		slog.Info("http server listening", "addr", cfg.HTTPAddr)
		errCh <- srv.ListenAndServe()
	}()

	select {
	case <-ctx.Done():
		slog.Info("shutting down")
	case err := <-errCh:
		if !errors.Is(err, http.ErrServerClosed) {
			stop()
			wg.Wait()
			return err
		}
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	err = srv.Shutdown(shutdownCtx)
	stop()
	wg.Wait()
	return err
}

func runPaymentConsumer(ctx context.Context, cfg *config.Config, payments *services.PaymentService, dlq messaging.Publisher) {
	for ctx.Err() == nil {
		consumer := kafkainfra.NewConsumer(cfg.KafkaBrokers, cfg.KafkaTopic, cfg.KafkaGroupID)
		err := workers.NewPaymentConsumer(consumer, payments, dlq).Run(ctx)
		closeErr := consumer.Close()
		if ctx.Err() != nil {
			return
		}
		if err != nil || closeErr != nil {
			slog.Error("payment consumer stopped; restarting", "err", errors.Join(err, closeErr))
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(time.Second):
		}
	}
}
