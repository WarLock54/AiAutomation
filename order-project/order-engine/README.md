# Order Engine — High-Throughput Order/Payment Engine

A microservices architecture written in Go + Kafka + gRPC, featuring
Saga Orchestration, an idempotent payment flow, resilience (retry +
circuit breaker), gRPC health checks, and OpenTelemetry distributed
tracing. Every feature has been tested and verified end-to-end in a
real Docker Compose environment (see "Proven Guarantees").

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
| Order line items are persisted | The `order_items` table is written in the same transaction as the order row; readable via the `GetOrderStatus` API |
| **Crash recovery** | Every Saga step is written to `saga_steps` as STARTED/COMPLETED; on restart the service finds stuck orders and safely re-runs the Saga (safe because every step is idempotent) — **verified live with a manually constructed "payment succeeded but crashed before commit" scenario**, confirmed no double payment/reservation occurred |
| order-service protects itself when a downstream service is down | `pkg/resilience`: retry with exponential backoff (only on transient errors) + circuit breaker (`sony/gobreaker`) |
| K8s liveness/readiness probes work | `pkg/health` (gRPC Health Checking Protocol) + `grpc_health_probe` wired into real Docker `HEALTHCHECK` directives — shows up as `(healthy)` in `docker compose ps` |
| **A single request's journey across all services is traceable** | OpenTelemetry + Jaeger: a `CreateOrder` call is unified into a single trace together with every gRPC call it makes to `inventory-service`/`payment-service` — **verified live in the Jaeger UI, with client- and server-side duration measured separately for every step** |
| Logs can be filtered by service in a central system | Structured JSON logging via `log/slog`, every line carries a `service` field |
| No need to carry `.proto` files around for debugging | `reflection.Register` — `grpcurl <addr> list` / `describe` work out of the box |

## Requirements

- Go 1.23+
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

4. **Apply the migrations** (see below — there are 4 files, apply in order).

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
pattern, see `deploy/postgres-init`). `order-service`'s 4 migrations
must be applied **in order** (each one adds a column/table on top of
the previous one):

```bash
psql postgres://order_engine:order_engine@localhost:5432/orders -f services/order-service/migrations/001_create_orders.sql
psql postgres://order_engine:order_engine@localhost:5432/orders -f services/order-service/migrations/002_add_idempotency_key.sql
psql postgres://order_engine:order_engine@localhost:5432/orders -f services/order-service/migrations/003_create_order_items.sql
psql postgres://order_engine:order_engine@localhost:5432/orders -f services/order-service/migrations/004_create_saga_steps.sql

psql postgres://order_engine:order_engine@localhost:5432/inventory -f services/inventory-service/migrations/001_create_stock_tables.sql
psql postgres://order_engine:order_engine@localhost:5432/payments  -f services/payment-service/migrations/001_create_payments.sql
```

(If you're using pgAdmin4: right-click the relevant database → Query
Tool → paste the file's contents → F5. This step could be automated
with `golang-migrate` later on.)

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

**To observe crash recovery:** manually insert an order row into
`orders` with `status='PENDING'` plus its `order_items`, then call
`ReserveStock`/`ProcessPayment` **directly** against `inventory-service`/
`payment-service` (bypassing `order-service` entirely), using the same
`order_id`/`idempotency_key`. This simulates "payment succeeded but
order-service crashed before commit." When you run `docker compose
restart order-service`, the startup recovery scan finds this order and
re-runs the Saga — because `ReserveStock`/`ProcessPayment` are
idempotent, nothing happens twice; only the missing `CommitStock`
actually executes, and the order becomes `PAID`.