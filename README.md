# order-processing-system

A distributed backend system designed to reliably manage order lifecycles, inventory reservations and payment workflows under concurrent traffic and service failures.

Built with Go, Gin, PostgreSQL (pgx), Redis and Apache Kafka. A modular monolith: one deployable, clearly separated modules (order, inventory, payment, recovery).

## Quick start

Requirements: Go 1.27.4 for local development and Docker Desktop for the full service stack. `make` is optional; the equivalent Docker Compose commands are shown below.

```bash
make up        # Postgres, Redis, Kafka, API, Prometheus, Grafana
make seed      # two demo products
```

On Windows, run `docker compose up -d --build` and `Get-Content scripts/seed.sql | docker compose exec -T postgres psql -U orders -d orders` instead.

```bash
# List products and live availability
curl -s localhost:8080/api/v1/products -H 'X-API-Key: dev-key-1'

# Create an order (the Idempotency-Key header is required)
curl -s -X POST localhost:8080/api/v1/orders \
  -H 'X-API-Key: dev-key-1' \
  -H 'Idempotency-Key: demo-order-0001' \
  -H 'Content-Type: application/json' \
  -d '{"items":[{"product_id":"11111111-1111-1111-1111-111111111111","quantity":2}]}'

# Repeat the exact same request: 200 + same order, nothing is reserved twice
# Poll the order: PENDING_PAYMENT -> PAID (or CANCELLED)
curl -s localhost:8080/api/v1/orders/<id> -H 'X-API-Key: dev-key-1'
```

Prometheus: http://localhost:9090, Grafana: http://localhost:3000 (Prometheus datasource preconfigured), metrics: `/metrics`. The development stack leaves metrics unauthenticated unless `METRICS_TOKEN` is set. Compose publishes service ports only on localhost; Grafana uses its standard first-login password change.

To run the API on your host instead, start the dependencies with `docker compose up -d postgres redis kafka`, apply migrations with `docker compose run --rm migrate`, then stop the containerized API. In PowerShell, load the example environment and start the host process:

```powershell
Copy-Item .env.example .env
Get-Content .env | ForEach-Object {
  if ($_ -match '^\s*([^#\s=]+)\s*=(.*)$') {
    Set-Item -Path "Env:$($matches[1])" -Value $matches[2].Trim()
  }
}
docker compose stop api
go run ./cmd/api
```

The Go process does not load `.env` files automatically; the commands above set those values in the current PowerShell session.

## Order lifecycle

```
POST /orders ──► PENDING_PAYMENT ──► PAID
                      │
                      └────────────► CANCELLED   (payment timeout or confirmed no-charge)
```

1. **API** (one DB transaction): claim the idempotency key, reserve stock, insert the order, write an `order.created` row to the outbox.
2. **Outbox relay** publishes versioned event envelopes to Kafka and marks outbox rows published.
3. **Payment consumer group** reads `order.created`, creates or loads the current payment attempt, and charges through the configured gateway provider. Multiple group members process separate Kafka partitions concurrently while preserving order within each partition.
4. **Success**: stock reservation becomes a real deduction, order becomes `PAID`. **Decline**: the attempt is recorded as `FAILED`; the order and stock reservation remain pending so another attempt can be started.
5. **Recovery worker** re-emits events for stalled orders with a bounded delay/retry budget, checks the gateway before cancelling an order with an uncertain charge, and retries pending refunds. A late successful charge is refunded by the gateway before it is marked `REFUNDED`.

### Event delivery and retries

Every Kafka message uses an envelope containing an event ID, type, schema version, correlation ID, occurrence time, and event data. The event ID is stored with its outbox row, so duplicate publishes retain the same ID. Malformed supported events are sent to the configured dead-letter topic before their source offset is acknowledged.

Payment and refund processing is attempted once per delivery; transient failures are retried by the database-backed recovery worker instead of sleeping and retrying inside a Kafka partition. `PAYMENT_CONSUMER_WORKERS` controls the number of group members (default 3); effective concurrency is limited by `KAFKA_PARTITIONS` (default 3). Existing Kafka topics are not resized automatically; coordinate partition increases with Kafka administration because changing a keyed topic's partition count can change key-to-partition assignments. `PAYMENT_GATEWAY_MAX_ATTEMPTS` defaults to 1 to avoid multiplying gateway retries by recovery retries.

`PAYMENT_GATEWAY_PROVIDER` selects the registered payment adapter. Development defaults to `simulated`; production must explicitly select a non-simulated provider. `paypal` uses PayPal REST Checkout Orders and Captures APIs. Configure `PAYMENT_PROVIDER_BASE_URL` (defaults to the PayPal sandbox), `PAYMENT_PROVIDER_CLIENT_ID`, and `PAYMENT_PROVIDER_SECRET`. Provider adapters must support idempotent charge/refund operations and charge lookup; gateway contract tests in `internal/services/gateway_registry_test.go` define those expectations.

## Tenant isolation and authorization

Migration `000006_tenant_isolation` creates a `default` tenant and backfills existing catalog, inventory, order, payment-attempt, idempotency, status-history, and outbox rows into it. Products and inventory are tenant-specific, and composite foreign keys prevent an order from referencing another tenant's product or stock. Existing installations must apply the migration before starting this version.

Development can configure each API key as `key:tenant:user:role`, for example `dev-key-1:default:user-1:user` or `dev-admin-key:default:operator:admin`. Set `AUTH_MODE=oidc` for JWT/OIDC authentication and configure `OIDC_ISSUER` and `OIDC_AUDIENCE`. OIDC tokens must contain `tenant_id` (or `tid`), `user_id` (or `sub`), and a `role` or `roles` claim containing `user` or `admin`. API-key tenant IDs must already exist in `tenants`; startup validates configured keys and fails for unknown tenant IDs. Create additional tenant records through the controlled database provisioning process before adding keys for them. The example uses the backfilled `default` tenant.

Regular users can browse their tenant catalog, create orders, and view/cancel their own pending orders. Tenant admins can list and view all orders in their tenant, cancel pending orders, and request a new payment attempt after a decline. Admins cannot access another tenant's data. The retry endpoint queues the attempt; normal workers process it asynchronously.

## Operations and security

- `APP_ENV` defaults to `production`. Startup always requires explicit `DATABASE_URL` and `API_KEYS`; there are no source-code defaults for database credentials or API keys. Production additionally requires explicit `REDIS_ADDR`, `KAFKA_BROKERS`, and `METRICS_TOKEN`, rejects the sample database credentials, and requires API keys and the metrics token to each be at least 32 characters. The checked-in `.env.example` and Compose settings are for local development only.
- The HTTP server has read, write, idle, and header timeouts. Order request bodies are limited by `HTTP_MAX_BODY_BYTES` (default 1 MiB); a process-wide token bucket enforces `HTTP_RATE_LIMIT` and `HTTP_RATE_BURST`. `/healthz` and `/readyz` bypass rate limiting for probes. Readiness responses expose only `ok` or `unavailable`; detailed failures are logged with a request ID.
- When `METRICS_TOKEN` is configured, `/metrics` requires `Authorization: Bearer <token>`. Configure production Prometheus with the same token using a protected credentials file.
- Retention removes up to `RETENTION_BATCH_SIZE` rows from each table per run. Published outbox rows older than `OUTBOX_RETENTION` (default 14 days) are eligible; unpublished rows are never deleted. Idempotency keys older than `IDEMPOTENCY_RETENTION` (default 30 days) are removed, so a client may reuse a key after that period. `RETENTION_INTERVAL` controls cleanup cadence.
- CI runs formatting, vet, golangci-lint, govulncheck, unit tests including `-race`, PostgreSQL integration tests, and a Docker image build.

### Payment reconciliation and refunds

An expired order with a `PROCESSING` payment is not cancelled based only on a timeout. Recovery changes the payment to `RECONCILING` and asks the gateway for the charge outcome:

- A confirmed successful charge completes the order and commits its reserved stock.
- A confirmed decline or charge absence allows the order to be cancelled and its stock released.
- A pending result or gateway lookup error leaves the order and reservation pending for another reconciliation attempt.

If a charge succeeds after its order was cancelled, the payment moves to `REFUND_PENDING` and a `payment.refund_requested` event is written to the outbox. The payment consumer calls the gateway's idempotent refund operation using the payment ID. Only after the gateway confirms the refund does the system mark the payment `REFUNDED` and emit `payment.refunded`. Recovery republishes refund requests that remain pending, so a failed or interrupted refund can be retried safely. Migration `000002_payment_reconciliation` adds the `RECONCILING` and `REFUND_PENDING` payment states.

### Payment attempts and order history

Each charge is stored as a separate row in `payment_attempts`, with a per-order attempt number and its own provider idempotency key. A declined attempt remains in the history; `PaymentService.RetryPayment` creates a new attempt only after the previous attempt failed and while the order is still pending. The order's stock reservation is kept during this retry window and released only if recovery cancels the order.

Order transitions are defined in the domain transition table and validated by the repository's `SetOrderStatus`. The `order_statuses` lookup table replaces the hard-coded SQL status check, and `order_status_history` records each new order's initial status and subsequent allowed transitions, including reason and timestamp. Existing orders receive a baseline snapshot during migration; earlier transitions cannot be reconstructed. Migrations `000003_payment_attempts` and `000004_order_state_history` add these structures. Migration `000005_event_envelopes` adds stable IDs to outbox events.

## How each objective is met

| Objective | Mechanism |
|---|---|
| Prevent overselling | One atomic `UPDATE inventory SET reserved = reserved + n WHERE on_hand - reserved >= n`; `CHECK` constraints as a backstop; stock rows locked in sorted order (no deadlocks). PostgreSQL is the source of truth, Redis never decides stock. |
| Prevent duplicate orders | `Idempotency-Key` is claimed inside the order transaction (primary key `(tenant_id, user_id, key)`). Concurrent duplicates block on the unique index and then replay the stored order. Same key with a different body returns 422. |
| Prevent duplicate charges and preserve attempt history | `payment_attempts` stores every charge attempt; `UNIQUE (order_id, attempt_number)` allocates distinct attempts and gateway calls are idempotent by attempt ID. |
| Handle concurrent requests | Row-level locks, atomic updates, `FOR UPDATE SKIP LOCKED` in the relay, Redis lock to cheaply reject in-flight duplicates (fails open). |
| Recover from failures | Transactional outbox (no lost events), at-least-once consumers that are idempotent, dead-lettering for malformed events, circuit breaker, recovery worker with retry budget and timeout compensation. |
| Consistent state across async steps | Every state change and its event commit in one transaction; transitions are validated by the domain state machine and recorded in `order_status_history`. |

## Layout

```
cmd/api                 entrypoint and wiring
internal/domain         order, inventory, payment: pure types and rules
internal/services       use cases, each owning its transaction
internal/repositories   all SQL (pgx)
internal/handlers       Gin handlers and router
internal/middleware     API-key auth, idempotency, metrics
internal/messaging      broker-agnostic types, outbox relay
internal/workers        payment consumer, recovery worker
internal/infrastructure postgres, redis, kafka
migrations/             golang-migrate SQL
tests/                  integration, concurrency, failure (Testcontainers)
```

## Testing

```bash
make test               # unit tests
make test-integration   # needs Docker; real PostgreSQL via Testcontainers
make loadtest           # k6: 100 orders/s plus 50 buyers fighting over 10 units
```

The integration suite proves the hard guarantees: 100 concurrent buyers on 10 units produce exactly 10 orders, 20 parallel requests with one key produce one order, duplicate event delivery creates only one attempt, a declined attempt can be retried without losing history, invalid order transitions are rejected, stale orders are reconciled before cancellation, and late payments are refunded only after provider confirmation.

## Notes

- Delivery is at-least-once everywhere; idempotency makes it effectively-once.
- Rejected requests (for example out of stock) roll back completely, including the idempotency key, so a client retry re-evaluates against current stock.
- Authentication supports static API keys for development and issuer-verifying JWT/OIDC bearer tokens for deployments. OIDC tokens must include tenant, user, and role claims.
- `pgx` is used directly. sqlc can replace the hand-written `internal/repositories` later without touching services.
