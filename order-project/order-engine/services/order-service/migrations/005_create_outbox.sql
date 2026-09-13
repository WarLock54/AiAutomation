-- Transactional Outbox Pattern: order.completed/order.failed event'leri
-- artık sipariş durumu güncellemesiyle (orders.status) AYNI transaction
-- içinde bu tabloya yazılır (bkz. internal/orchestrator.go Finalize ve
-- pkg/outbox). Böylece "sipariş durumu güncellendi ama event kaybedildi"
-- ya da tam tersi dual-write riski ortadan kalkar (bkz. rapor [K6]).
CREATE TABLE IF NOT EXISTS outbox_events (
    id              UUID PRIMARY KEY,
    aggregate_id    TEXT NOT NULL,
    event_type      TEXT NOT NULL,
    topic           TEXT NOT NULL,
    payload         JSONB NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    published_at    TIMESTAMPTZ,
    attempts        INTEGER NOT NULL DEFAULT 0,
    next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_error      TEXT,
    dead_letter     BOOLEAN NOT NULL DEFAULT false
);

CREATE INDEX IF NOT EXISTS idx_outbox_unpublished
    ON outbox_events (next_attempt_at)
    WHERE published_at IS NULL AND dead_letter = false;