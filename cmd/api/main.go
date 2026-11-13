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

	"github.com/BrianObonyo/order-processing-system/internal/config"
	"github.com/BrianObonyo/order-processing-system/internal/handlers"
	kafkainfra "github.com/BrianObonyo/order-processing-system/internal/infrastructure/kafka"
	"github.com/BrianObonyo/order-processing-system/internal/infrastructure/postgres"
	redisinfra "github.com/BrianObonyo/order-processing-system/internal/infrastructure/redis"
	"github.com/BrianObonyo/order-processing-system/internal/messaging"
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

	rds := redisinfra.New(cfg.RedisAddr)
	defer rds.Close()

	if err := kafkainfra.EnsureTopic(ctx, cfg.KafkaBrokers, cfg.KafkaTopic, 3); err != nil {
		return err
	}
	producer := kafkainfra.NewProducer(cfg.KafkaBrokers, cfg.KafkaTopic)
	defer producer.Close()

	// Modules.
	orderSvc := services.NewOrderService(pool)
	productSvc := services.NewProductService(pool)
	gateway := services.NewResilientGateway(
		services.NewSimulatedGateway(cfg.PaymentFailureRate, cfg.PaymentTransientRate, cfg.PaymentLatency),
		3, 200*time.Millisecond,
	)
	paymentSvc := services.NewPaymentService(pool, gateway)
	recoverySvc := services.NewRecoveryService(pool, cfg.OrderPaymentTimeout, cfg.PaymentRetryAfter, cfg.PaymentMaxRecovery)

	// Background workers.
	var wg sync.WaitGroup
	start := func(fn func(context.Context)) {
		wg.Add(1)
		go func() { defer wg.Done(); fn(ctx) }()
	}
	if cfg.RunWorkers {
		consumer := kafkainfra.NewConsumer(cfg.KafkaBrokers, cfg.KafkaTopic, cfg.KafkaGroupID)
		defer consumer.Close()
		start(messaging.NewOutboxRelay(pool, producer, cfg.OutboxPollInterval).Run)
		start(workers.NewPaymentConsumer(consumer, paymentSvc).Run)
		start(workers.NewRecoveryWorker(recoverySvc, rds, cfg.RecoveryInterval).Run)
	}

	router := handlers.NewRouter(handlers.Deps{
		Orders:   orderSvc,
		Products: productSvc,
		APIKeys:  cfg.APIKeys,
		Locker:   rds,
		Readiness: map[string]func(context.Context) error{
			"postgres": pool.Ping,
			"redis":    rds.Ping,
		},
	})
	srv := &http.Server{Addr: cfg.HTTPAddr, Handler: router, ReadHeaderTimeout: 5 * time.Second}

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
