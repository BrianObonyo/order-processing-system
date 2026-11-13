# Architecture

Modular monolith. PostgreSQL is the source of truth; Redis is coordination only; Kafka carries events published through a transactional outbox.

```mermaid
sequenceDiagram
  participant C as Client
  participant A as API (Gin)
  participant P as PostgreSQL
  participant R as Outbox relay
  participant K as Kafka
  participant W as Payment consumer
  C->>A: POST /orders (Idempotency-Key)
  A->>P: TX: claim key, reserve stock, insert order, insert outbox row
  A-->>C: 201 PENDING_PAYMENT
  R->>P: SELECT unpublished ... FOR UPDATE SKIP LOCKED
  R->>K: publish order.created
  R->>P: mark published
  K->>W: order.created (at-least-once)
  W->>P: TX: insert payment (once per order)
  W->>W: charge gateway (retry + circuit breaker)
  W->>P: TX: order PAID + commit stock + outbox order.paid
```

## Decisions

- **Reserve, then commit.** `inventory.reserved` holds stock for pending orders; payment success converts it to a deduction, failure or timeout releases it.
- **One path for terminal transitions.** `cancelPendingOrder` and `markOrderPaid` lock the order row and re-check status, so payment completion and recovery can race safely.
- **Gateway outside transactions.** No DB locks are held during the external call; idempotency comes from keying the charge by payment ID.
- **Late payment compensation.** If a charge succeeds after the order was cancelled, the payment is marked `REFUNDED` and a `payment.refunded` event is emitted.
- **Single topic, keyed by order ID.** Per-order ordering within a partition; consumers filter by `event_type` header.
