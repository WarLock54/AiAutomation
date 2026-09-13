# Order Engine — High-Throughput Order/Payment Engine

Order Engine is a portfolio project exploring the architectural patterns behind
high-throughput distributed order/payment systems: Saga orchestration across
gRPC services, idempotency at every layer to make retries and crash-recovery
safe, resilience (retry + circuit breaker) against downstream failures, and
OpenTelemetry distributed tracing to make a multi-service request debuggable
end-to-end. Rather than being a theoretical exercise, every guarantee in this
repo — no double payments, no double stock deductions, automatic recovery
after a simulated mid-Saga crash — was verified live against a real Docker
Compose environment, including deliberately reproducing failure scenarios to
confirm the system actually behaves correctly under them.

## Architecture Overview

```
                     ┌──────────────────┐
   gRPC client ────▶ │   order-service   │  (Saga Orchestrator)
                     │  + health check   │
                     │  + reflection     │
                     │  + tracing (root) │
                     └────────┬──────────┘
              retry+breaker   │   retry+breaker
              +trace context  │   +trace context
                    ┌─────────┼─────────┐
                    ▼                   ▼
          ┌──────────────────┐  ┌──────────────────┐
          │ inventory-service │  │ payment-service  │
          │ (Reserve/Commit/  │  │  (idempotent     │
          │  Release stock,   │  │   payment via    │
          │  idempotent)      │  │   Redis, itself  │
          │                   │  │   idempotent)    │
          └──────────────────┘  └──────────────────┘
                    │                   │
        ┌───────────┴───────┐           │
        ▼                   ▼           ▼
     Postgres            Kafka ◀────────┘
   (saga_steps,       (order-events,
    order_items)       payment-events)
                             │
                             ▼
                 notification-consumer

   All services ──OTLP/gRPC──▶ Jaeger (http://localhost:16686)
```

Saga flow: `ReserveStock → ProcessPayment → CommitStock`. If payment
fails, a `ReleaseStock` compensating transaction is triggered. Every
step is both persisted to the `saga_steps` table and emitted as an
OpenTelemetry span to Jaeger.

## Proven Guarantees

This isn't just a design on paper — every guarantee below has been
tested end-to-end with `grpcurl` against a real Docker Compose
environment, and most of them were verified by actually simulating a
failure scenario:

| Guarantee | How it's enforced / how it was verified |
|---|---|
| No double stock deduction | `inventory-service`'s `ReserveStock` dedups via `idempotency_key` in Redis |
| No double payment | `payment-service`'s `ProcessPayment` follows the same pattern |
| The same order request always returns the same result | A UNIQUE index on `orders.idempotency_key`; `CreateOrder` returns the existing record (including the real `created_at`) instead of re-running the Saga |
| An idempotency key can't be silently reused for a *different* request | Every idempotency key (in `order-service`, `inventory-service`, and `payment-service`) is bound to a canonical SHA-256 hash of the request's identifying fields (customer/order/amount/items). Reusing the same key with different parameters is rejected with `InvalidArgument` instead of returning the wrong cached result |
| An idempotency lock can't be silently stolen out from under a slow request | The idempotency store uses owner-token leases (Redis + Lua scripts) instead of a bare TTL: only the worker holding the current lease can mark a request complete or release it, so a second worker that raced in after the lease expired can't overwrite the first one's result |
| Order line items are persisted | The `order_items` table is written in the same transaction as the order row; readable via the `GetOrderStatus` API |
| No lost or double-published events | Status updates and outbound events (`order.completed`/`order.failed`, `payment.succeeded`) go through a **transactional outbox**: the DB write and the event row are committed in the same transaction, then published by a separate background worker with retry + exponential backoff and dead-lettering after repeated failures — **verified live**: querying `outbox_events` right after a `CreateOrder` call shows `published_at` populated within under a second, `attempts=0` |
| Stock commit/release can't double-apply | `CommitStock`/`ReleaseStock` use a state-guarded `UPDATE ... WHERE status = 'RESERVED'` (not an unconditional update), making both naturally idempotent against a stale retry hitting an already-committed or already-released reservation |
| **Crash recovery** | Every Saga step is written to `saga_steps` as STARTED/COMPLETED; a periodic background worker (not just a one-shot startup scan) atomically **claims** stuck orders with `SELECT ... FOR UPDATE SKIP LOCKED` plus a lease timeout, so two instances can never re-run the same stuck order at once, and an order is dead-lettered (not retried forever) after 5 failed attempts — **verified live** with a manually-inserted "crashed mid-Saga" order: the first automatic recovery attempt failed (a genuine race from the manual test setup itself — the order row existed slightly before its line items did), the lease correctly held the order for its 2-minute window instead of hammering it, and the very next pass completed the Saga cleanly (`recovery_attempts: 2`, final status `PAID`, no double reservation or payment) |
| Notifications aren't re-sent after a consumer restart | `notification-consumer` deduplicates by `event_id` in Redis with a 7-day TTL (previously an in-memory set that was wiped on every restart) |
| order-service protects itself when a downstream service is down | `pkg/resilience`: retry with exponential backoff (only on transient errors) + circuit breaker (`sony/gobreaker`) — **verified live** by stopping `inventory-service` mid-test and confirming `CreateOrder` fails fast with a clear error, then recovers automatically once the dependency comes back |
| K8s liveness/readiness probes work | `pkg/health` (gRPC Health Checking Protocol) + `grpc_health_probe` wired into real Docker `HEALTHCHECK` directives — shows up as `(healthy)` in `docker compose ps` |
| **A single request's journey across all services is traceable** | OpenTelemetry + Jaeger: a `CreateOrder` call is unified into a single trace together with every gRPC call it makes to `inventory-service`/`payment-service` — **verified live in the Jaeger UI, with client- and server-side duration measured separately for every step** |
| Logs can be filtered by service in a central system | Structured JSON logging via `log/slog`, every line carries a `service` field |
| No need to carry `.proto` files around for debugging | `reflection.Register` — `grpcurl <addr> list` / `describe` work out of the box |

## Requirements

- Go 1.24+
- Docker & Docker Compose
- `protoc` + `protoc-gen-go` + `protoc-gen-go-grpc` (for proto code generation)
- `grpcurl` (for testing)

## Setup

1. **Generate proto code**:
   ```bash
   make proto
   ```
   If `make` isn't available on Windows, run directly:
   ```powershell
   protoc --go_out=gen --go_opt=module=order-engine/gen --go-grpc_out=gen --go-grpc_opt=module=order-engine/gen --proto_path=proto proto/order.proto proto/inventory.proto proto/payment.proto
   ```

2. **Download dependencies:**
   ```bash
   go mod tidy
   ```

3. **Bring the whole stack up** (Kafka, Postgres, Redis, Jaeger + 4 services):
   ```bash
   docker compose up --build -d
   ```
   Both the infrastructure services (`postgres`, `redis`, `kafka`) and
   the application services (`order-service`, `inventory-service`,
   `payment-service` — via `grpc_health_probe`) have real Docker
   `HEALTHCHECK`s; dependent services won't start until the ones they
   depend on are actually ready (shows as `(healthy)` in `docker compose ps`).

4. **Apply the migrations** (see below — there are now 7 order-service
   files, 1 inventory file, and 2 payment files; apply in order).

5. **Watch the logs:**
   ```bash
   docker compose logs -f order-service inventory-service payment-service notification-consumer
   ```

6. **Kafka UI**: http://localhost:8090
   **Jaeger UI** (distributed tracing): http://localhost:16686

7. To shut down: `docker compose down` (add `-v` to also wipe the database —
   without `-v` your orders/stock/trace history is preserved).

## Database Migrations

Each service has its own `migrations/` folder (orders, inventory, and
payments each live in a separate database — a Database-per-Service
pattern, see `deploy/postgres-init`). `order-service`'s 7 migrations
must be applied **in order** (each one adds a column/table on top of
the previous one):

```bash
psql postgres://order_engine:order_engine@localhost:5432/orders -f services/order-service/migrations/001_create_orders.sql
psql postgres://order_engine:order_engine@localhost:5432/orders -f services/order-service/migrations/002_add_idempotency_key.sql
psql postgres://order_engine:order_engine@localhost:5432/orders -f services/order-service/migrations/003_create_order_items.sql
psql postgres://order_engine:order_engine@localhost:5432/orders -f services/order-service/migrations/004_create_saga_steps.sql
# transactional outbox: status update + event are written in one atomic
# DB transaction instead of a separate, unsafe Kafka publish step.
psql postgres://order_engine:order_engine@localhost:5432/orders -f services/order-service/migrations/005_create_outbox.sql
# recovery claim/lease: lets the periodic recovery worker atomically
# claim stuck orders without two instances racing on the same one.
psql postgres://order_engine:order_engine@localhost:5432/orders -f services/order-service/migrations/006_add_recovery_claim.sql
# binds idempotency_key to a canonical request hash so CreateOrder can
# reject a key reused with different order parameters.
psql postgres://order_engine:order_engine@localhost:5432/orders -f services/order-service/migrations/007_add_idempotency_request_hash.sql

psql postgres://order_engine:order_engine@localhost:5432/inventory -f services/inventory-service/migrations/001_create_stock_tables.sql

psql postgres://order_engine:order_engine@localhost:5432/payments  -f services/payment-service/migrations/001_create_payments.sql
# transactional outbox for payment-service.
psql postgres://order_engine:order_engine@localhost:5432/payments  -f services/payment-service/migrations/002_create_outbox.sql
```

## Example Test Flow

```bash
# Create an order (Saga: reserve -> pay -> commit, items persisted)
grpcurl -plaintext -import-path proto -proto order.proto \
  -d '{"customer_id":"11111111-1111-1111-1111-111111111111","items":[{"product_id":"SKU-001","quantity":2,"unit_price_cents":1500}],"idempotency_key":"test-key-001"}' \
  localhost:50051 order.v1.OrderService/CreateOrder

# Look up order details (including items)
grpcurl -plaintext -import-path proto -proto order.proto \
  -d '{"order_id":"<orderId from above>"}' \
  localhost:50051 order.v1.OrderService/GetOrderStatus

# Health check (what a K8s liveness/readiness probe would call)
grpcurl -plaintext localhost:50051 grpc.health.v1.Health/Check

# Discover services via reflection
grpcurl -plaintext localhost:50051 list
```

Re-sending the request with the same `idempotency_key`: `payment-service`
won't charge a second time, `inventory-service` won't deduct stock a
second time, and `order-service` won't create a new order — it returns
the exact same `orderId` and `createdAt` as before.

**To see distributed tracing in action:** after the `CreateOrder` call
above, go to http://localhost:16686 → Service: `order-service` →
Operation: `order.v1.OrderService/CreateOrder` → Find Traces. You'll
see a single trace containing every call made to `inventory-service`/
`payment-service`, each with its own duration.

**To observe the circuit breaker:** `docker compose stop inventory-service`,
then call `CreateOrder` — the first few calls will be delayed a few
hundred ms by retries, and after 5 consecutive failures the circuit
opens; subsequent calls fail instantly with `Unavailable` without ever
calling the downstream service. Bring it back with `docker compose
start inventory-service`.

**To observe idempotency-key reuse detection:** call `CreateOrder` once
with a fresh `idempotency_key`, then call it again with the *same* key
but a different `unit_price_cents` (or a different item/customer). The
second call is rejected with `InvalidArgument` instead of silently
returning the first order's result.

**To observe the insufficient-stock path:** call `CreateOrder` asking
for a quantity larger than what's available for a given `product_id`
(check `stock_items` for current levels). The response comes back with
status `FAILED` and stock is left completely unchanged — nothing is
deducted for a reservation that never succeeded.

**To observe crash recovery:** manually insert an order row into
`orders` with `status='PENDING'` plus its `order_items`, then
`docker compose restart order-service`. A periodic background worker
(not just a one-shot startup scan) atomically claims stuck orders —
using `SELECT ... FOR UPDATE SKIP LOCKED` plus a lease timestamp, so
two instances can never claim the same one — and re-runs the full Saga
for each one it finds. Because every downstream call is idempotent,
nothing happens twice even if a claim attempt itself fails partway
(it's simply retried on the next pass, up to 5 attempts before being
dead-lettered for manual review). This was verified live: a manually
inserted order's first automatic recovery attempt failed (a genuine
race in the manual test setup — the order row existed slightly before
its own line items did), the lease correctly left it alone for its
2-minute window instead of hammering it, and the very next pass
completed the Saga cleanly, ending with `recovery_attempts: 2` and
status `PAID` — with no double reservation or double payment.
