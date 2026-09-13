// Saga Orchestration yaklaşımı: order-service, dağıtık transaction'ın
// merkezi koordinatörüdür. Her adımı sırayla çağırır ve bir adım
// başarısız olursa, önceki başarılı adımları TERS SIRAYLA telafi eder
// (compensating transactions).
//
// Akış:
//  1. ReserveStock  (inventory-service)
//  2. ProcessPayment (payment-service)
//     3a. Başarılı -> CommitStock (inventory-service)
//     3b. Ödeme başarısız -> ReleaseStock (compensating)
//  3. Finalize: sipariş durumu VE order.completed/order.failed event'i TEK
//     bir transaction'da (outbox) atomik olarak yazılır (bkz. rapor [K6]).
//
// SAGA STATE MACHINE PERSISTENCE: Her adımın başlangıcı/bitişi saga_steps
// tablosuna yazılır (bkz. repository.go RecordSagaStep). Bu, order-service
// bir adım ortasında çökerse (örn. ödeme alındı ama stok commit edilemeden
// process öldü), servis yeniden başladığında hangi adımda kaldığını
// anlayabilmesini sağlar (bkz. recovery.go). Kurtarma stratejisi basittir:
// tüm downstream çağrılar (ReserveStock, ProcessPayment) idempotency_key
// ile korunduğu için, Execute() aynı sipariş için GÜVENLE TEKRAR
// çalıştırılabilir -- zaten tamamlanmış adımlar cache'ten anında döner,
// sadece yarım kalan adım(lar) gerçekten yeniden denenir.
//
// NOT: Bu implementasyon Orchestration stilini gösterir. Choreography
// stilinde ise order-service merkezi koordinasyon yapmaz; her servis
// diğerlerinin event'lerini dinleyip kendi kararını verir (örn.
// payment-service "stock.reserved" event'ini dinler, işini yapar ve
// "payment.succeeded/failed" event'i yayınlar). İki stilin de artı/eksisi
// var: Orchestration -> akış tek yerden izlenebilir ama order-service tüm
// diğer servislere bağımlı hale gelir (coupling artar). Choreography ->
// servisler daha bağımsızdır ama uçtan uca akışı izlemek zorlaşır
// (bu yüzden Proje 3'te dağıtık tracing bu sorunu çözmek için ekleniyor).
package internal

import (
	"context"
	"fmt"
	"log/slog"
	"os"

	inventoryv1 "order-engine/gen/inventory/v1"
	paymentv1 "order-engine/gen/payment/v1"
	"order-engine/pkg/outbox"
)

// logger, Saga akışının her adımını order_id bazlı izlenebilir kılan
// yapısal (JSON) log satırları üretir.
var logger = slog.New(slog.NewJSONHandler(os.Stdout, nil)).With("service", "order-service", "component", "orchestrator")

// Saga adım isimleri (saga_steps.step kolonunda kullanılır).
const (
	stepReserveStock   = "RESERVE_STOCK"
	stepProcessPayment = "PROCESS_PAYMENT"
	stepCommitStock    = "COMMIT_STOCK"
	stepReleaseStock   = "RELEASE_STOCK"
)

const (
	stepStatusStarted   = "STARTED"
	stepStatusCompleted = "COMPLETED"
	stepStatusFailed    = "FAILED"
)

// Orchestrator, artık doğrudan bir *kafka.Producer TUTMAZ: event yayınlama
// transactional outbox üzerinden yapılır (bkz. rapor [K6] ve pkg/outbox).
// Gerçek Kafka publish'i main.go'da ayrı bir outbox.Publisher arka plan
// goroutine'i tarafından yönetilir.
type Orchestrator struct {
	inventoryClient inventoryv1.InventoryServiceClient
	paymentClient   paymentv1.PaymentServiceClient
	repo            *PostgresRepository
}

func NewOrchestrator(
	inventoryClient inventoryv1.InventoryServiceClient,
	paymentClient paymentv1.PaymentServiceClient,
	repo *PostgresRepository,
) *Orchestrator {
	return &Orchestrator{
		inventoryClient: inventoryClient,
		paymentClient:   paymentClient,
		repo:            repo,
	}
}

type OrderItem struct {
	ProductID      string
	Quantity       int32
	UnitPriceCents int64
}

type SagaResult struct {
	OrderID    string
	Status     string // RESERVED, PAID, FAILED
	Reason     string
	PaymentID  string
	TotalCents int64
}

// recordStep, saga_steps tablosuna yazarken oluşan hatayı sadece loglar
// (bu bir "en iyi çaba" audit kaydıdır; yazımı başarısız olsa bile Saga'nın
// kendisi durmamalıdır -- asıl doğruluk kaynağı downstream servislerin
// idempotency store'larıdır, saga_steps sadece izlenebilirlik/kurtarma
// için bir yardımcıdır).
func (o *Orchestrator) recordStep(ctx context.Context, orderID, step, status, reservationID, paymentID string) {
	if err := o.repo.RecordSagaStep(ctx, orderID, step, status, reservationID, paymentID); err != nil {
		logger.Warn("saga adımı kaydedilemedi", "order_id", orderID, "step", step, "status", status, "error", err)
	}
}

// Execute, tam Saga akışını yürütür. order-service'in gRPC handler'ı
// (server.go) VE recovery.go (kurtarma sırasında) bu metodu çağırır.
// Aynı orderID/idempotencyKey ile birden çok kez çağrılması güvenlidir
// (bkz. paket üstü yorum). Execute artık event YAYINLAMAZ -- dönen
// SagaResult, çağıran taraf tarafından Finalize'a verilmelidir; bu, hem
// sipariş durumunu hem de karşılık gelen event'i tek bir transaction'da
// atomik olarak yazar (bkz. rapor [K6]).
func (o *Orchestrator) Execute(ctx context.Context, orderID, customerID string, items []OrderItem, idempotencyKey string) (*SagaResult, error) {
	// --- Adım 1: Stok Rezervasyonu ---
	o.recordStep(ctx, orderID, stepReserveStock, stepStatusStarted, "", "")

	reserveReq := &inventoryv1.ReserveStockRequest{
		OrderId:        orderID,
		IdempotencyKey: idempotencyKey,
	}
	for _, it := range items {
		reserveReq.Items = append(reserveReq.Items, &inventoryv1.StockItem{
			ProductId: it.ProductID,
			Quantity:  it.Quantity,
		})
	}

	reserveResp, err := o.inventoryClient.ReserveStock(ctx, reserveReq)
	if err != nil {
		o.recordStep(ctx, orderID, stepReserveStock, stepStatusFailed, "", "")
		return nil, fmt.Errorf("stok rezervasyon çağrısı başarısız: %w", err)
	}
	if !reserveResp.GetSuccess() {
		o.recordStep(ctx, orderID, stepReserveStock, stepStatusFailed, "", "")
		return &SagaResult{OrderID: orderID, Status: "FAILED", Reason: "yetersiz stok"}, nil
	}
	reservationID := reserveResp.GetReservationId()
	o.recordStep(ctx, orderID, stepReserveStock, stepStatusCompleted, reservationID, "")
	logger.Info("stok rezerve edildi", "order_id", orderID, "reservation_id", reservationID)

	// --- Adım 2: Ödeme ---
	o.recordStep(ctx, orderID, stepProcessPayment, stepStatusStarted, reservationID, "")

	var totalCents int64
	for _, it := range items {
		totalCents += it.UnitPriceCents * int64(it.Quantity)
	}

	paymentResp, err := o.paymentClient.ProcessPayment(ctx, &paymentv1.ProcessPaymentRequest{
		OrderId:        orderID,
		CustomerId:     customerID,
		AmountCents:    totalCents,
		Currency:       "TRY",
		IdempotencyKey: idempotencyKey,
	})
	if err != nil || paymentResp.GetStatus() == paymentv1.PaymentStatus_PAYMENT_STATUS_FAILED {
		o.recordStep(ctx, orderID, stepProcessPayment, stepStatusFailed, reservationID, "")

		// --- Compensating Transaction: rezerve edilen stoğu geri al ---
		logger.Warn("ödeme başarısız oldu, stok rezervasyonu geri alınıyor",
			"order_id", orderID, "reservation_id", reservationID)
		o.recordStep(ctx, orderID, stepReleaseStock, stepStatusStarted, reservationID, "")
		if releaseErr := o.compensateReservation(ctx, reservationID); releaseErr != nil {
			// Bu durum manuel müdahale gerektiren kritik bir tutarsızlıktır;
			// production'da alerting + dead-letter kaydı gerekir.
			o.recordStep(ctx, orderID, stepReleaseStock, stepStatusFailed, reservationID, "")
			logger.Error("KRİTİK: compensating stok iadesi başarısız",
				"reservation_id", reservationID, "error", releaseErr)
		} else {
			o.recordStep(ctx, orderID, stepReleaseStock, stepStatusCompleted, reservationID, "")
		}

		return &SagaResult{OrderID: orderID, Status: "FAILED", Reason: "ödeme başarısız"}, nil
	}
	paymentID := paymentResp.GetPaymentId()
	o.recordStep(ctx, orderID, stepProcessPayment, stepStatusCompleted, reservationID, paymentID)

	// --- Adım 3: Rezervasyonu Kalıcı Hale Getir ---
	o.recordStep(ctx, orderID, stepCommitStock, stepStatusStarted, reservationID, paymentID)

	if _, err := o.inventoryClient.CommitStock(ctx, &inventoryv1.CommitStockRequest{
		ReservationId: reservationID,
	}); err != nil {
		// Ödeme alındı ama commit başarısız oldu: kritik tutarsızlık.
		// Bu adım saga_steps'e FAILED olarak yazıldığı için, recovery.go
		// bir sonraki taramada bu siparişi bulup CommitStock'u (idempotent
		// olduğu için güvenle) tekrar deneyecektir.
		o.recordStep(ctx, orderID, stepCommitStock, stepStatusFailed, reservationID, paymentID)
		logger.Error("KRİTİK: stok commit edilemedi ama ödeme alındı",
			"order_id", orderID, "error", err)
		return nil, fmt.Errorf("stok commit edilemedi: %w", err)
	}
	o.recordStep(ctx, orderID, stepCommitStock, stepStatusCompleted, reservationID, paymentID)

	logger.Info("sipariş başarıyla tamamlandı", "order_id", orderID, "payment_id", paymentID, "total_cents", totalCents)

	return &SagaResult{OrderID: orderID, Status: "PAID", PaymentID: paymentID, TotalCents: totalCents}, nil
}

// Finalize, Execute'ün ürettiği SagaResult'a göre sipariş durumunu VE
// karşılık gelen order.completed/order.failed event'ini TEK bir DB
// transaction'ında (outbox) atomik olarak yazar (bkz. rapor [K6]).
// server.go (CreateOrder) VE recovery.go (kurtarma sonrası) bu metodu
// çağırmalıdır; repo.UpdateOrderStatus'u DOĞRUDAN çağırıp ayrıca event
// yayınlamaya çalışmak dual-write riskini geri getirir.
func (o *Orchestrator) Finalize(ctx context.Context, result *SagaResult) error {
	eventType := "order.completed"
	payload := map[string]any{
		"order_id":    result.OrderID,
		"payment_id":  result.PaymentID,
		"total_cents": result.TotalCents,
	}
	if result.Status != "PAID" {
		eventType = "order.failed"
		payload = map[string]any{
			"order_id": result.OrderID,
			"reason":   result.Reason,
		}
	}

	evt, err := outbox.NewEvent("order-events", eventType, result.OrderID, payload)
	if err != nil {
		return fmt.Errorf("event hazırlanamadı: %w", err)
	}

	if err := o.repo.FinalizeOrder(ctx, result.OrderID, result.Status, result.Reason, evt); err != nil {
		return fmt.Errorf("sipariş sonlandırılamadı: %w", err)
	}
	return nil
}

func (o *Orchestrator) compensateReservation(ctx context.Context, reservationID string) error {
	_, err := o.inventoryClient.ReleaseStock(ctx, &inventoryv1.ReleaseStockRequest{
		ReservationId: reservationID,
		Reason:        "payment_failed",
	})
	return err
}
