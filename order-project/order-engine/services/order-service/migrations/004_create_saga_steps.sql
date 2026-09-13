-- Saga State Machine persistence: her Saga adımının ne zaman, hangi
-- sonuçla gerçekleştiğini kalıcı olarak kaydeder. Bu sayede order-service
-- crash olup yeniden başladığında, yarım kalmış siparişleri (örn. ödeme
-- alındı ama stok commit edilemedi) tespit edip kaldığı yerden devam
-- ettirebilir (bkz. internal/recovery.go).
CREATE TABLE IF NOT EXISTS saga_steps (
    id              BIGSERIAL PRIMARY KEY,
    order_id        UUID NOT NULL REFERENCES orders (id) ON DELETE CASCADE,
    step            TEXT NOT NULL,   -- RESERVE_STOCK, PROCESS_PAYMENT, COMMIT_STOCK, RELEASE_STOCK
    step_status     TEXT NOT NULL,   -- STARTED, COMPLETED, FAILED
    reservation_id  TEXT,
    payment_id      TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_saga_steps_order_id ON saga_steps (order_id, id);