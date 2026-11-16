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

Prometheus: http://localhost:9090, Grafana: http://localhost:3000 (Prometheus datasource preconfigured), metrics: `/metrics`.

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
2. **Outbox relay** publishes outbox rows to Kafka and marks them published.
3. **Payment consumer** reads `order.created`, creates or loads the current payment attempt, and charges through a simulated gateway (retries + circuit breaker).
4. **Success**: stock reservation becomes a real deduction, order becomes `PAID`. **Decline**: the attempt is recorded as `FAILED`; the order and stock reservation remain pending so another attempt can be started.
5. **Recovery worker** re-emits events for stalled orders, checks the gateway before cancelling an order with an uncertain charge, and retries pending refunds. A late successful charge is refunded by the gateway before it is marked `REFUNDED`.

### Payment reconciliation and refunds

An expired order with a `PROCESSING` payment is not cancelled based only on a timeout. Recovery changes the payment to `RECONCILING` and asks the gateway for the charge outcome:

- A confirmed successful charge completes the order and commits its reserved stock.
- A confirmed decline or charge absence allows the order to be cancelled and its stock released.
- A pending result or gateway lookup error leaves the order and reservation pending for another reconciliation attempt.

If a charge succeeds after its order was cancelled, the payment moves to `REFUND_PENDING` and a `payment.refund_requested` event is written to the outbox. The payment consumer calls the gateway's idempotent refund operation using the payment ID. Only after the gateway confirms the refund does the system mark the payment `REFUNDED` and emit `payment.refunded`. Recovery republishes refund requests that remain pending, so a failed or interrupted refund can be retried safely. Migration `000002_payment_reconciliation` adds the `RECONCILING` and `REFUND_PENDING` payment states.

### Payment attempts and order history

Each charge is stored as a separate row in `payment_attempts`, with a per-order attempt number and its own provider idempotency key. A declined attempt remains in the history; `PaymentService.RetryPayment` creates a new attempt only after the previous attempt failed and while the order is still pending. The order's stock reservation is kept during this retry window and released only if recovery cancels the order.

Order transitions are defined in the domain transition table and validated by the repository's `SetOrderStatus`. The `order_statuses` lookup table replaces the hard-coded SQL status check, and `order_status_history` records each new order's initial status and subsequent allowed transitions, including reason and timestamp. Existing orders receive a baseline snapshot during migration; earlier transitions cannot be reconstructed. Migrations `000003_payment_attempts` and `000004_order_state_history` add these structures.

## How each objective is met

| Objective | Mechanism |
|---|---|
| Prevent overselling | One atomic `UPDATE inventory SET reserved = reserved + n WHERE on_hand - reserved >= n`; `CHECK` constraints as a backstop; stock rows locked in sorted order (no deadlocks). PostgreSQL is the source of truth, Redis never decides stock. |
| Prevent duplicate orders | `Idempotency-Key` is claimed inside the order transaction (primary key `(user_id, key)`). Concurrent duplicates block on the unique index and then replay the stored order. Same key with a different body returns 422. |
| Prevent duplicate charges and preserve attempt history | `payment_attempts` stores every charge attempt; `UNIQUE (order_id, attempt_number)` allocates distinct attempts and gateway calls are idempotent by attempt ID. |
| Handle concurrent requests | Row-level locks, atomic updates, `FOR UPDATE SKIP LOCKED` in the relay, Redis lock to cheaply reject in-flight duplicates (fails open). |
| Recover from failures | Transactional outbox (no lost events), at-least-once consumers that are idempotent, retry with backoff, circuit breaker, recovery worker with retry budget and timeout compensation. |
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
- Authentication is a static API-key map for development. Replace `middleware.APIKeyAuth` with JWT/OIDC for real use.
- `pgx` is used directly. sqlc can replace the hand-written `internal/repositories` later without touching services.
