// Package internal - recovery.go
//
// SAGA STATE MACHINE RECOVERY: order-service bir Saga adımının ortasında
// çökerse (örn. ödeme alındı ama process CommitStock'tan önce öldü),
// yeniden başladığında bu sipariş "PENDING" ya da "RESERVED" durumunda
// takılı kalır (asla PAID/FAILED/CANCELLED'a ulaşamaz).
//
// Kurtarma stratejisi kasıtlı olarak basit tutuldu: saga_steps tablosundaki
// adım geçmişini yorumlayıp "kaldığı yerden" devam etmek yerine, TÜM
// Saga'yı (Execute) aynı orderID/idempotencyKey ile BAŞTAN çalıştırıyoruz.
// Bu güvenlidir çünkü:
//   - ReserveStock zaten idempotency_key ile korunuyor (aynı key ikinci
//     kez çağrıldığında önceki rezervasyonun sonucunu döner, stok tekrar
//     düşürülmez) VE artık istek gövdesi hash'iyle de doğrulanıyor
//     (bkz. rapor [K5]).
//   - ProcessPayment aynı şekilde idempotent (çift ödeme alınmaz).
//   - CommitStock/ReleaseStock artık KOŞULLU (state-guarded) UPDATE'ler
//     olduğu için doğal olarak idempotent (bkz. rapor [K8] ve
//     inventory-service/internal/repository.go).
//
// Böylece "kaldığı adımı bul, sadece onu çalıştır" gibi kırılgan bir
// state-machine yorumlayıcısı yazmak yerine, var olan idempotency
// altyapısını yeniden kullanıyoruz -- daha az kod, daha az hata yüzeyi.
//
// PERİYODİK ÇALIŞMA VE CLAIM/LEASE (bkz. rapor [K7]): eskiden bu kurtarma
// SADECE servis başlangıcında BİR KEZ çalışıyordu ve birden fazla instance
// (pod) aynı takılı siparişi aynı anda claim edip downstream servislere
// çift çağrı yapabilirdi. Artık:
//   - RunRecoveryLoop periyodik olarak (varsayılan 30sn) taranır,
//   - ClaimIncompleteOrders (repository.go), `FOR UPDATE SKIP LOCKED` ile
//     ATOMİK claim yapar: aynı satırı iki instance birden claim edemez,
//   - Her claim bir "lease" iğnesi bırakır (next_recovery_attempt_at ileri
//     itilir); bu recovery denemesi de yarıda kesilirse sipariş sonsuza
//     dek kilitli KALMAZ, lease süresi dolunca tekrar claim edilebilir,
//   - maxAttempts'e ulaşan siparişler recovery_dead_letter=true olarak
//     işaretlenip otomatik kurtarmadan çıkarılır ve KRİTİK seviyede
//     loglanır (manuel inceleme/alert gerektiren "stuck order").
package internal

import (
	"context"
	"time"
)

// RecoveryConfig, periyodik Saga kurtarma worker'ının davranışını belirler.
type RecoveryConfig struct {
	// MinAge: bu süreden daha yeni güncellenmiş siparişler "hâlâ aktif
	// olarak işleniyor olabilir" sayılır, kurtarma adayı DEĞİLDİR -- bu,
	// tam o anda normal akışta işlenmekte olan bir siparişi yanlışlıkla
	// "takıldı" sanıp erken müdahale etmeyi engeller.
	MinAge time.Duration
	// PollInterval: worker'ın orders tablosunu ne sıklıkla tarayacağı.
	PollInterval time.Duration
	// BatchSize: tek bir taramada en fazla kaç sipariş claim edilir.
	BatchSize int
	// MaxAttempts: bu sayıya ulaşan siparişler otomatik kurtarmadan
	// çıkarılıp (recovery_dead_letter=true) manuel incelemeye bırakılır.
	MaxAttempts int
	// LeaseDuration: bir claim'in ne kadar süre "kilitli" sayılacağı.
	LeaseDuration time.Duration
}

func DefaultRecoveryConfig() RecoveryConfig {
	return RecoveryConfig{
		MinAge:        10 * time.Second,
		PollInterval:  30 * time.Second,
		BatchSize:     20,
		MaxAttempts:   5,
		LeaseDuration: 2 * time.Minute,
	}
}

// RunRecoveryLoop, ctx iptal edilene kadar periyodik olarak takılı kalmış
// siparişleri tarar ve kurtarır. Bloklayan bir çağrıdır; main.go'da
// `go orchestrator.RunRecoveryLoop(ctx, cfg)` ile ayrı bir goroutine'de
// başlatılması amaçlanır.
func (o *Orchestrator) RunRecoveryLoop(ctx context.Context, cfg RecoveryConfig) {
	// İlk taramayı hemen yap (eski "startup'ta bir kez" davranışının
	// yerini alır), ardından periyodik olarak devam et.
	o.recoverOnce(ctx, cfg)

	ticker := time.NewTicker(cfg.PollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			o.recoverOnce(ctx, cfg)
		}
	}
}

// recoverOnce, tek bir tarama/kurtarma turu çalıştırır.
func (o *Orchestrator) recoverOnce(ctx context.Context, cfg RecoveryConfig) {
	claimed, err := o.repo.ClaimIncompleteOrders(ctx, cfg.MinAge, cfg.BatchSize, cfg.MaxAttempts, cfg.LeaseDuration)
	if err != nil {
		logger.Error("saga kurtarma taraması başarısız", "error", err)
		return
	}

	for _, id := range claimed.DeadLetteredIDs {
		logger.Error("KRİTİK: sipariş maksimum kurtarma denemesine ulaştı, otomatik kurtarma durduruldu (manuel inceleme/alert gerekiyor)",
			"order_id", id, "max_attempts", cfg.MaxAttempts)
	}

	if len(claimed.Orders) == 0 {
		return
	}

	logger.Warn("takılı kalmış siparişler bulundu (claim edildi), Saga kurtarma başlıyor", "count", len(claimed.Orders))

	var recovered, failed int
	for _, order := range claimed.Orders {
		logger.Info("sipariş kurtarılıyor", "order_id", order.ID, "onceki_durum", order.Status)

		result, execErr := o.Execute(ctx, order.ID, order.CustomerID, order.Items, order.IdempotencyKey)
		if execErr != nil {
			failed++
			logger.Error("sipariş kurtarma başarısız (bir sonraki turda tekrar denenecek)", "order_id", order.ID, "error", execErr)
			continue
		}

		if err := o.Finalize(ctx, result); err != nil {
			failed++
			logger.Error("kurtarma sonrası sipariş sonlandırılamadı (bir sonraki turda tekrar denenecek)", "order_id", order.ID, "error", err)
			continue
		}

		recovered++
		logger.Info("sipariş başarıyla kurtarıldı", "order_id", order.ID, "yeni_durum", result.Status)
	}

	logger.Info("saga kurtarma turu tamamlandı", "recovered", recovered, "failed", failed, "claimed", len(claimed.Orders))
}
