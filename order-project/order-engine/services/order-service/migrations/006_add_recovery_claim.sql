-- Saga Recovery Claim/Lease: birden fazla order-service instance'ı (pod)
-- aynı anda çalışırken, aynı takılı siparişi İKİ instance'ın birden
-- kurtarmaya çalışmasını (ve dolayısıyla downstream servislere çift
-- çağrı yapılmasını) engellemek için "kim, ne zaman, kaç kez denedi"
-- bilgisini kalıcı olarak tutuyoruz (bkz. rapor [K7]).
ALTER TABLE orders ADD COLUMN IF NOT EXISTS recovery_attempts INTEGER NOT NULL DEFAULT 0;
ALTER TABLE orders ADD COLUMN IF NOT EXISTS next_recovery_attempt_at TIMESTAMPTZ NOT NULL DEFAULT now();
ALTER TABLE orders ADD COLUMN IF NOT EXISTS recovery_dead_letter BOOLEAN NOT NULL DEFAULT false;

-- Periyodik recovery worker'ın claim sorgusu için hedeflenmiş kısmi indeks.
CREATE INDEX IF NOT EXISTS idx_orders_recovery_candidates
    ON orders (next_recovery_attempt_at)
    WHERE status NOT IN ('PAID', 'FAILED', 'CANCELLED') AND recovery_dead_letter = false;