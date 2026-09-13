CREATE TABLE IF NOT EXISTS payments (
    id              UUID PRIMARY KEY,
    order_id        UUID NOT NULL,
    customer_id     UUID NOT NULL,
    amount_cents    BIGINT NOT NULL,
    currency        TEXT NOT NULL,
    status          TEXT NOT NULL CHECK (status IN ('SUCCEEDED', 'FAILED', 'REFUNDED')),
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_payments_order_id ON payments (order_id);
