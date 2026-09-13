package internal

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"os"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	inventoryv1 "order-engine/gen/inventory/v1"
	"order-engine/pkg/idempotency"
)

var logger = slog.New(slog.NewJSONHandler(os.Stdout, nil)).With("service", "inventory-service", "component", "server")

type Server struct {
	inventoryv1.UnimplementedInventoryServiceServer

	repo *PostgresRepository
	idem *idempotency.Store
}

func NewInventoryServer(repo *PostgresRepository, idem *idempotency.Store) *Server {
	return &Server{repo: repo, idem: idem}
}

// reserveRequestKey, idempotency key'in aynı istekle mi yoksa farklı bir
// order/ürün kümesiyle mi yeniden kullanıldığını ayırt etmek için hash'i
// alınan alanları temsil eder (bkz. rapor [K5]).
type reserveRequestKey struct {
	OrderID string          `json:"order_id"`
	Items   []reserveItemKV `json:"items"`
}

type reserveItemKV struct {
	ProductID string `json:"product_id"`
	Quantity  int32  `json:"quantity"`
}

// ReserveStock, idempotency_key üzerinden çift rezervasyonu engeller.
// Aynı key ile art arda gelen istekler (örn. order-service'in retry'ları
// ya da Saga recovery'nin aynı adımı tekrar çalıştırması, bkz. rapor [K7])
// stoğu ikinci kez düşürmez; önceden hesaplanmış sonuç doğrudan döner.
// Aynı key FARKLI bir order_id/ürün kümesiyle kullanılırsa istek reddedilir
// (bkz. rapor [K5]).
func (s *Server) ReserveStock(ctx context.Context, req *inventoryv1.ReserveStockRequest) (*inventoryv1.ReserveStockResponse, error) {
	if len(req.GetItems()) == 0 {
		return nil, status.Error(codes.InvalidArgument, "en az bir ürün gerekli")
	}
	if req.GetIdempotencyKey() == "" {
		return nil, status.Error(codes.InvalidArgument, "idempotency_key zorunludur")
	}

	reqKey := reserveRequestKey{OrderID: req.GetOrderId()}
	for _, it := range req.GetItems() {
		if it.GetQuantity() <= 0 {
			return nil, status.Errorf(codes.InvalidArgument, "quantity pozitif olmalıdır (product_id=%s)", it.GetProductId())
		}
		reqKey.Items = append(reqKey.Items, reserveItemKV{ProductID: it.GetProductId(), Quantity: it.GetQuantity()})
	}

	check, err := s.idem.Acquire(ctx, req.GetIdempotencyKey(), reqKey)
	if err != nil {
		switch {
		case errors.Is(err, idempotency.ErrInProgress):
			return nil, status.Error(codes.FailedPrecondition,
				"bu istek zaten işleniyor, lütfen kısa süre sonra tekrar deneyin")
		case errors.Is(err, idempotency.ErrKeyReused):
			return nil, status.Error(codes.InvalidArgument,
				"idempotency_key daha önce FARKLI bir sipariş/ürün kümesiyle kullanılmış")
		default:
			return nil, status.Errorf(codes.Internal, "idempotency kontrolü başarısız: %v", err)
		}
	}

	if !check.IsNew {
		var cached inventoryv1.ReserveStockResponse
		if err := json.Unmarshal(check.CachedResult, &cached); err != nil {
			return nil, status.Errorf(codes.Internal, "önbelleklenmiş sonuç okunamadı: %v", err)
		}
		return &cached, nil
	}

	items := make([]StockItem, 0, len(req.GetItems()))
	for _, it := range req.GetItems() {
		items = append(items, StockItem{ProductID: it.GetProductId(), Quantity: it.GetQuantity()})
	}

	reservationID, insufficient, err := s.repo.TryReserve(ctx, req.GetOrderId(), items)
	if err != nil {
		if relErr := s.idem.Release(ctx, req.GetIdempotencyKey(), check.OwnerToken); relErr != nil {
			logger.Warn("idempotency kilidi serbest bırakılamadı", "idempotency_key", req.GetIdempotencyKey(), "error", relErr)
		}
		return nil, status.Errorf(codes.Internal, "rezervasyon başarısız: %v", err)
	}

	var resp *inventoryv1.ReserveStockResponse
	if len(insufficient) > 0 {
		resp = &inventoryv1.ReserveStockResponse{
			Success:                false,
			InsufficientProductIds: insufficient,
		}
	} else {
		resp = &inventoryv1.ReserveStockResponse{
			Success:       true,
			ReservationId: reservationID,
		}
	}

	if err := s.idem.Complete(ctx, req.GetIdempotencyKey(), check.OwnerToken, resp); err != nil {
		// bkz. rapor [K4]/[K9]: bu sessizce yutulmamalı. Rezervasyon zaten
		// DB'de gerçekleşti (yukarıda commit edildi); sadece idempotency
		// önbelleği güncellenemedi. Kritik seviyede logluyoruz ki
		// alerting/manuel inceleme sürecine girsin.
		logger.Error("KRİTİK: stok rezerve edildi ama idempotency sonucu önbelleğe alınamadı (lease kaybedilmiş olabilir)",
			"order_id", req.GetOrderId(), "reservation_id", reservationID, "idempotency_key", req.GetIdempotencyKey(), "error", err)
	}

	return resp, nil
}

func (s *Server) ReleaseStock(ctx context.Context, req *inventoryv1.ReleaseStockRequest) (*inventoryv1.ReleaseStockResponse, error) {
	if err := s.repo.Release(ctx, req.GetReservationId()); err != nil {
		return nil, status.Errorf(codes.Internal, "stok iade edilemedi: %v", err)
	}
	return &inventoryv1.ReleaseStockResponse{Success: true}, nil
}

func (s *Server) CommitStock(ctx context.Context, req *inventoryv1.CommitStockRequest) (*inventoryv1.CommitStockResponse, error) {
	if err := s.repo.Commit(ctx, req.GetReservationId()); err != nil {
		return nil, status.Errorf(codes.Internal, "rezervasyon commit edilemedi: %v", err)
	}
	return &inventoryv1.CommitStockResponse{Success: true}, nil
}
