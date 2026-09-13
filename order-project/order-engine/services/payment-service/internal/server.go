package internal

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"os"

	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	paymentv1 "order-engine/gen/payment/v1"
	"order-engine/pkg/idempotency"
	"order-engine/pkg/outbox"
)

var logger = slog.New(slog.NewJSONHandler(os.Stdout, nil)).With("service", "payment-service", "component", "server")

// Server, paymentv1.PaymentServiceServer arayüzünü uygular.
// NOT: `make proto` çalıştırıldığında gen/payment/v1 paketi üretilecek ve
// UnimplementedPaymentServiceServer embed edilerek forward-compatibility
// sağlanmalıdır: `paymentv1.UnimplementedPaymentServiceServer`.
type Server struct {
	paymentv1.UnimplementedPaymentServiceServer

	repo *PostgresRepository
	idem *idempotency.Store
}

// NewPaymentServer, artık doğrudan bir *kafka.Producer ALMAZ: event
// yayınlama transactional outbox üzerinden yapılır (bkz. rapor [K6] ve
// pkg/outbox). Gerçek Kafka publish'i main.go'da ayrı bir
// outbox.Publisher arka plan goroutine'i tarafından yönetilir.
func NewPaymentServer(repo *PostgresRepository, idem *idempotency.Store) *Server {
	return &Server{repo: repo, idem: idem}
}

// idempotencyRequestKey, canonical hash için kullanılan alanları temsil
// eder. Sadece isteğin KİMLİĞİNİ belirleyen alanlar (order/customer/tutar/
// para birimi) dahil edilir -- idempotency_key'in kendisi HARİÇ tutulur
// (zaten Redis anahtarı o). Aynı key farklı bir order_id/tutar ile
// kullanılırsa hash uyuşmaz ve Acquire ErrKeyReused döner (bkz. rapor [K5]).
type idempotencyRequestKey struct {
	OrderID     string `json:"order_id"`
	CustomerID  string `json:"customer_id"`
	AmountCents int64  `json:"amount_cents"`
	Currency    string `json:"currency"`
}

// ProcessPayment, idempotency_key üzerinden çift işlemeyi engeller:
//  1. Redis'te key, bu isteğin hash'iyle birlikte kilitlenir (Acquire).
//     Aynı key farklı bir istek gövdesiyle geldiyse InvalidArgument döner.
//  2. Key daha önce tamamlanmışsa, kilitlenmeden önceki sonuç direkt döner.
//  3. Key şu an başka bir istek tarafından işleniyorsa (race condition),
//     DUPLICATE / FailedPrecondition döner; istemci retry ile bekleyebilir.
//  4. Yeni bir key ise iş mantığı çalışır; ödeme kaydı VE payment.succeeded
//     event'i TEK bir DB transaction'ında (outbox) yazılır, ardından sonuç
//     SADECE bu çağrının owner token'ı hâlâ geçerliyse Complete edilir.
func (s *Server) ProcessPayment(ctx context.Context, req *paymentv1.ProcessPaymentRequest) (*paymentv1.ProcessPaymentResponse, error) {
	if req.GetIdempotencyKey() == "" {
		return nil, status.Error(codes.InvalidArgument, "idempotency_key zorunludur")
	}
	if req.GetAmountCents() <= 0 {
		return nil, status.Error(codes.InvalidArgument, "amount_cents pozitif olmalıdır")
	}
	if req.GetCurrency() == "" {
		return nil, status.Error(codes.InvalidArgument, "currency zorunludur")
	}

	reqKey := idempotencyRequestKey{
		OrderID:     req.GetOrderId(),
		CustomerID:  req.GetCustomerId(),
		AmountCents: req.GetAmountCents(),
		Currency:    req.GetCurrency(),
	}

	check, err := s.idem.Acquire(ctx, req.GetIdempotencyKey(), reqKey)
	if err != nil {
		switch {
		case errors.Is(err, idempotency.ErrInProgress):
			return nil, status.Error(codes.FailedPrecondition,
				"bu istek zaten işleniyor, lütfen kısa süre sonra tekrar deneyin")
		case errors.Is(err, idempotency.ErrKeyReused):
			return nil, status.Error(codes.InvalidArgument,
				"idempotency_key daha önce FARKLI bir istek (order/customer/tutar) ile kullanılmış")
		default:
			return nil, status.Errorf(codes.Internal, "idempotency kontrolü başarısız: %v", err)
		}
	}

	if !check.IsNew {
		var cached paymentv1.ProcessPaymentResponse
		if err := json.Unmarshal(check.CachedResult, &cached); err != nil {
			return nil, status.Errorf(codes.Internal, "önbelleklenmiş sonuç okunamadı: %v", err)
		}
		cached.Status = paymentv1.PaymentStatus_PAYMENT_STATUS_DUPLICATE
		return &cached, nil
	}

	// --- Asıl ödeme iş mantığı (burada bir ödeme sağlayıcı çağrısı simüle ediliyor) ---
	paymentID := uuid.NewString()
	record := PaymentRecord{
		ID:          paymentID,
		OrderID:     req.GetOrderId(),
		CustomerID:  req.GetCustomerId(),
		AmountCents: req.GetAmountCents(),
		Currency:    req.GetCurrency(),
		Status:      "SUCCEEDED", // TODO: gerçek sağlayıcı entegrasyonu
	}

	evt, err := outbox.NewEvent("payment-events", "payment.succeeded", req.GetOrderId(), map[string]any{
		"payment_id": paymentID,
		"order_id":   req.GetOrderId(),
		"status":     "SUCCEEDED",
	})
	if err != nil {
		_ = s.idem.Release(ctx, req.GetIdempotencyKey(), check.OwnerToken)
		return nil, status.Errorf(codes.Internal, "event hazırlanamadı: %v", err)
	}

	// Ödeme kaydı VE outbox event'i tek transaction'da yazılır: publish
	// artık DB commit'inden BAĞIMSIZ ikinci bir adım değildir (bkz. [K6]).
	if err := s.repo.InsertPaymentWithOutbox(ctx, record, evt); err != nil {
		if relErr := s.idem.Release(ctx, req.GetIdempotencyKey(), check.OwnerToken); relErr != nil {
			logger.Error("KRİTİK: ödeme yazımı başarısız oldu VE idempotency kilidi serbest bırakılamadı",
				"idempotency_key", req.GetIdempotencyKey(), "error", relErr)
		}
		return nil, status.Errorf(codes.Internal, "ödeme kaydedilemedi: %v", err)
	}

	resp := &paymentv1.ProcessPaymentResponse{
		PaymentId: paymentID,
		Status:    paymentv1.PaymentStatus_PAYMENT_STATUS_SUCCEEDED,
	}

	// Sonucu SADECE bu çağrının hâlâ kilidin sahibi olması durumunda
	// önbelleğe alıyoruz. ErrLeaseLost dönerse (bkz. rapor [K4]) bu KRİTİK
	// bir durumdur: ödeme zaten kalıcı olarak yazıldı (yukarıda commit
	// edildi) ama idempotency önbelleği güncellenemedi -- sessizce
	// loglanıp yutulmak yerine burada AÇIKÇA "kritik" seviyesinde
	// işaretleniyor ki alerting/manuel inceleme sürecine girsin (bkz.
	// rapor [K9]: kritik sonuç hataları asla başarı gibi davranılmamalı).
	if err := s.idem.Complete(ctx, req.GetIdempotencyKey(), check.OwnerToken, resp); err != nil {
		logger.Error("KRİTİK: ödeme kalıcı olarak yazıldı ama idempotency sonucu önbelleğe alınamadı (lease kaybedilmiş olabilir); manuel inceleme/alert gerekir",
			"payment_id", paymentID, "order_id", req.GetOrderId(), "idempotency_key", req.GetIdempotencyKey(), "error", err)
	}

	return resp, nil
}

func (s *Server) RefundPayment(ctx context.Context, req *paymentv1.RefundPaymentRequest) (*paymentv1.RefundPaymentResponse, error) {
	// TODO: Saga'nın compensating adımı olarak çağrılır (bkz. Proje 2 Saga Orchestration).
	return nil, status.Error(codes.Unimplemented, "henüz implemente edilmedi")
}
