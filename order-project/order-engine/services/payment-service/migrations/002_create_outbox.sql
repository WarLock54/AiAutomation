-- Transactional Outbox Pattern: payment.succeeded/failed event'leri artık
-- payments tablosuna yazımla AYNI transaction içinde bu tabloya kaydedilir
-- (bkz. internal/repository.go InsertPaymentWithOutbox ve pkg/outbox).
-- Böylece "DB commit oldu ama Kafka publish edilemedi" ya da "publish
-- edildi ama DB commit olmadı" (dual-write) riski ortadan kalkar (bkz.
-- rapor [K6]).
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

-- Publisher'ın claim sorgusu (WHERE published_at IS NULL AND dead_letter =
-- false AND next_attempt_at <= now()) için hedeflenmiş kısmi indeks.
CREATE INDEX IF NOT EXISTS idx_outbox_unpublished
    ON outbox_events (next_attempt_at)
    WHERE published_at IS NULL AND dead_letter = false;