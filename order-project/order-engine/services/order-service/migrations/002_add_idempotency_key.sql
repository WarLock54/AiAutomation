-- Aynı idempotency_key ile CreateOrder tekrar çağrıldığında yeni bir
-- sipariş satırı OLUŞMAMASI için tekillik (unique) kısıtı ekleniyor.
-- NULL'a izin verilmiyor: her sipariş mutlaka bir idempotency_key ile
-- oluşturulmalı (server.go zaten bunu zorunlu kılıyor).
ALTER TABLE orders ADD COLUMN IF NOT EXISTS idempotency_key TEXT;

CREATE UNIQUE INDEX IF NOT EXISTS idx_orders_idempotency_key
    ON orders (idempotency_key);
