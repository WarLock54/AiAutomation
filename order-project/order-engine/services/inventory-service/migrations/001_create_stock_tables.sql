CREATE TABLE IF NOT EXISTS stock_items (
    product_id          TEXT PRIMARY KEY,
    available_quantity  INT NOT NULL CHECK (available_quantity >= 0)
);

CREATE TABLE IF NOT EXISTS reservations (
    id              BIGSERIAL PRIMARY KEY,
    reservation_id  TEXT NOT NULL,
    product_id      TEXT NOT NULL REFERENCES stock_items (product_id),
    quantity        INT NOT NULL,
    status          TEXT NOT NULL CHECK (status IN ('RESERVED', 'RELEASED', 'COMMITTED')),
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_reservations_reservation_id ON reservations (reservation_id);

-- Demo verisi
INSERT INTO stock_items (product_id, available_quantity) VALUES
    ('SKU-001', 100),
    ('SKU-002', 50),
    ('SKU-003', 10)
ON CONFLICT (product_id) DO NOTHING;
