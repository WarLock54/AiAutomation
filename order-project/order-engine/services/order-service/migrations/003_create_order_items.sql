-- Sipariş kalemlerini kalıcı olarak saklar. Daha önce order-service bu
-- bilgiyi sadece Saga akışını sürmek için bellekte tutuyordu; artık
-- audit/history/GetOrderStatus için veritabanında da duruyor.
CREATE TABLE IF NOT EXISTS order_items (
    id                BIGSERIAL PRIMARY KEY,
    order_id          UUID NOT NULL REFERENCES orders (id) ON DELETE CASCADE,
    product_id        TEXT NOT NULL,
    quantity          INTEGER NOT NULL CHECK (quantity > 0),
    unit_price_cents  BIGINT NOT NULL CHECK (unit_price_cents >= 0)
);

CREATE INDEX IF NOT EXISTS idx_order_items_order_id ON order_items (order_id);