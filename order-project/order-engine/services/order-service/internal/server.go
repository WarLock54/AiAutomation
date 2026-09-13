package internal

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	orderv1 "order-engine/gen/order/v1"
)

type Server struct {
	orderv1.UnimplementedOrderServiceServer

	repo         *PostgresRepository
	orchestrator *Orchestrator
}

func NewOrderServer(repo *PostgresRepository, orchestrator *Orchestrator) *Server {
	return &Server{repo: repo, orchestrator: orchestrator}
}

// createOrderRequestKey, idempotency_key'in aynı istekle mi yoksa farklı
// bir customer/ürün/tutar kümesiyle mi yeniden kullanıldığını ayırt etmek
// için hash'i alınan alanları temsil eder (bkz. yukarıdaki paket
// yorumu ve migration 007).
type createOrderRequestKey struct {
	CustomerID string                  `json:"customer_id"`
	Items      []createOrderItemHashKV `json:"items"`
}

type createOrderItemHashKV struct {
	ProductID      string `json:"product_id"`
	Quantity       int32  `json:"quantity"`
	UnitPriceCents int64  `json:"unit_price_cents"`
}

func hashCreateOrderRequest(customerID string, items []OrderItem) (string, error) {
	key := createOrderRequestKey{CustomerID: customerID}
	for _, it := range items {
		key.Items = append(key.Items, createOrderItemHashKV{
			ProductID:      it.ProductID,
			Quantity:       it.Quantity,
			UnitPriceCents: it.UnitPriceCents,
		})
	}
	data, err := json.Marshal(key)
	if err != nil {
		return "", fmt.Errorf("istek hash'lenemedi: %w", err)
	}
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:]), nil
}

// buildExistingOrderResponse, bulunan mevcut siparişi HEMEN döndürmeden
// önce, gelen isteğin hash'inin saklanan hash ile eşleştiğini doğrular.
// Eşleşmezse InvalidArgument döner -- aksi halde bu, tamamen farklı bir
// isteğin (örn. farklı bir tutar) yanlışlıkla önceki siparişin sonucunu
// almasına yol açardı (bkz. rapor [K5]; bu kontrolün order-service
// seviyesinde eksik olduğu üretim testinde tespit edildi).
func buildExistingOrderResponse(existing *Order, reqHash string) (*orderv1.CreateOrderResponse, error) {
	if existing.RequestHash != "" && existing.RequestHash != reqHash {
		return nil, status.Error(codes.InvalidArgument,
			"idempotency_key daha önce FARKLI bir istek (customer/items/tutar) ile kullanılmış")
	}
	return &orderv1.CreateOrderResponse{
		OrderId:   existing.ID,
		Status:    toProtoStatus(existing.Status),
		CreatedAt: timestamppb.New(existing.CreatedAt),
	}, nil
}

// CreateOrder, aynı idempotency_key ile tekrar çağrıldığında YENİ bir
// sipariş oluşturmaz: önce mevcut bir kayıt var mı diye bakar, varsa
// isteğin hash'ini doğrulayıp güncel durumunu döner (Saga akışı ikinci
// kez ÇALIŞTIRILMAZ). Bu, daha önce inventory-service/payment-service
// için eklediğimiz idempotency garantisinin sipariş kaydının kendisine de
// taşınmış halidir.
func (s *Server) CreateOrder(ctx context.Context, req *orderv1.CreateOrderRequest) (*orderv1.CreateOrderResponse, error) {
	if len(req.GetItems()) == 0 {
		return nil, status.Error(codes.InvalidArgument, "sipariş en az bir kalem içermelidir")
	}
	if req.GetIdempotencyKey() == "" {
		return nil, status.Error(codes.InvalidArgument, "idempotency_key zorunludur")
	}

	items := make([]OrderItem, 0, len(req.GetItems()))
	for _, it := range req.GetItems() {
		items = append(items, OrderItem{
			ProductID:      it.GetProductId(),
			Quantity:       it.GetQuantity(),
			UnitPriceCents: it.GetUnitPriceCents(),
		})
	}

	reqHash, err := hashCreateOrderRequest(req.GetCustomerId(), items)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "istek hazırlanamadı: %v", err)
	}

	// --- Idempotency kontrolü: bu key ile daha önce bir sipariş oluşmuş mu? ---
	existing, err := s.repo.FindByIdempotencyKey(ctx, req.GetIdempotencyKey())
	if err != nil {
		return nil, status.Errorf(codes.Internal, "idempotency kontrolü başarısız: %v", err)
	}
	if existing != nil {
		return buildExistingOrderResponse(existing, reqHash)
	}

	orderID := uuid.NewString()

	// Sipariş satırı VE kalemleri tek transaction'da yazılır (bkz.
	// repository.go). Böylece "items" artık sadece Saga akışını sürmek
	// için bellekte tutulan geçici veri değil, kalıcı bir kayıttır.
	orderCreatedAt, err := s.repo.CreatePendingOrderWithItems(ctx, orderID, req.GetCustomerId(), req.GetIdempotencyKey(), reqHash, items)
	if err != nil {
		// Race condition: iki istek TAM AYNI ANDA aynı key ile geldiyse,
		// unique index ikincisini burada reddeder. O isteği de "duplicate"
		// olarak ele alıp mevcut kaydı bulup dönüyoruz (retry yerine).
		if errors.Is(err, ErrDuplicateIdempotencyKey) {
			existing, findErr := s.repo.FindByIdempotencyKey(ctx, req.GetIdempotencyKey())
			if findErr != nil || existing == nil {
				return nil, status.Errorf(codes.Internal, "eşzamanlı idempotency çakışması çözülemedi: %v", findErr)
			}
			return buildExistingOrderResponse(existing, reqHash)
		}
		return nil, status.Errorf(codes.Internal, "sipariş kaydedilemedi: %v", err)
	}

	result, err := s.orchestrator.Execute(ctx, orderID, req.GetCustomerId(), items, req.GetIdempotencyKey())
	if err != nil {
		return nil, status.Errorf(codes.Internal, "sipariş işlenemedi: %v", err)
	}

	// Sipariş durumu VE order.completed/order.failed event'i tek
	// transaction'da (outbox) atomik olarak yazılır (bkz. rapor [K6]).
	if err := s.orchestrator.Finalize(ctx, result); err != nil {
		return nil, status.Errorf(codes.Internal, "sipariş sonlandırılamadı: %v", err)
	}

	return &orderv1.CreateOrderResponse{
		OrderId:   orderID,
		Status:    toProtoStatus(result.Status),
		CreatedAt: timestamppb.New(orderCreatedAt),
	}, nil
}

func (s *Server) GetOrderStatus(ctx context.Context, req *orderv1.GetOrderStatusRequest) (*orderv1.GetOrderStatusResponse, error) {
	order, err := s.repo.GetOrder(ctx, req.GetOrderId())
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "sipariş bulunamadı: %v", err)
	}

	protoItems := make([]*orderv1.OrderItem, 0, len(order.Items))
	for _, it := range order.Items {
		protoItems = append(protoItems, &orderv1.OrderItem{
			ProductId:      it.ProductID,
			Quantity:       it.Quantity,
			UnitPriceCents: it.UnitPriceCents,
		})
	}

	return &orderv1.GetOrderStatusResponse{
		OrderId:       order.ID,
		Status:        toProtoStatus(order.Status),
		FailureReason: order.FailureReason,
		Items:         protoItems,
	}, nil
}

func (s *Server) CancelOrder(ctx context.Context, req *orderv1.CancelOrderRequest) (*orderv1.CancelOrderResponse, error) {
	// TODO: Zaten PAID durumundaki bir siparişin iptali, RefundPayment +
	// ReleaseStock compensating adımlarını tetiklemelidir (bkz. Saga State
	// Machine persistence eklendikten sonra bu akış kalıcı/izlenebilir
	// hale getirilecek).
	return nil, status.Error(codes.Unimplemented, "henüz implemente edilmedi")
}

func toProtoStatus(s string) orderv1.OrderStatus {
	switch s {
	case "PAID":
		return orderv1.OrderStatus_ORDER_STATUS_PAID
	case "FAILED":
		return orderv1.OrderStatus_ORDER_STATUS_FAILED
	case "RESERVED":
		return orderv1.OrderStatus_ORDER_STATUS_RESERVED
	case "CANCELLED":
		return orderv1.OrderStatus_ORDER_STATUS_CANCELLED
	default:
		return orderv1.OrderStatus_ORDER_STATUS_PENDING
	}
}
