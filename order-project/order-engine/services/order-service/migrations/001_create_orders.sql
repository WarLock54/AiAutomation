CREATE TABLE IF NOT EXISTS orders (
    id              UUID PRIMARY KEY,
    customer_id     UUID NOT NULL,
    status          TEXT NOT NULL CHECK (status IN ('PENDING', 'RESERVED', 'PAID', 'FAILED', 'CANCELLED')),
    failure_reason  TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_orders_customer_id ON orders (customer_id);
