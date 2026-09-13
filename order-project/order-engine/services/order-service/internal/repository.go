package internal

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"order-engine/pkg/outbox"
)

// ErrDuplicateIdempotencyKey, aynı idempotency_key ile ikinci bir sipariş
// satırı oluşturulmaya çalışıldığında döner (unique index ihlali).
var ErrDuplicateIdempotencyKey = errors.New("bu idempotency_key ile zaten bir sipariş var")

type Order struct {
	ID             string
	CustomerID     string
	Status         string
	FailureReason  string
	IdempotencyKey string
	// RequestHash, CreateOrder isteğinin (customer_id + items) kanonik
	// hash'idir. Aynı idempotency_key farklı bir istekle yeniden
	// kullanıldığında bunu tespit etmek için saklanır (bkz. server.go).
	RequestHash string
	CreatedAt   time.Time
	Items       []OrderItem
}

type PostgresRepository struct {
	pool *pgxpool.Pool
}

func NewPostgresRepository(dsn string) (*PostgresRepository, error) {
	pool, err := pgxpool.New(context.Background(), dsn)
	if err != nil {
		return nil, fmt.Errorf("pgxpool oluşturulamadı: %w", err)
	}
	if err := pool.Ping(context.Background()); err != nil {
		return nil, fmt.Errorf("postgres ping başarısız: %w", err)
	}
	return &PostgresRepository{pool: pool}, nil
}

func (r *PostgresRepository) Close() {
	r.pool.Close()
}

// Pool, altındaki bağlantı havuzunu döner. pkg/outbox.NewStore ve
// pkg/outbox.NewPublisher'ın aynı veritabanına erişebilmesi için
// main.go'da kullanılır.
func (r *PostgresRepository) Pool() *pgxpool.Pool {
	return r.pool
}

// CreatePendingOrderWithItems, sipariş satırını VE kalemlerini TEK BİR
// transaction içinde yazar (ya ikisi de yazılır ya hiçbiri) ve
// veritabanının ürettiği GERÇEK created_at değerini döner (Go tarafında
// time.Now() ile tahmin etmek yerine) -- böylece hem ilk isteğin hem de
// aynı key ile gelen sonraki (cache'ten dönen) isteklerin response'undaki
// createdAt alanı birbiriyle tutarlı olur.
//
// requestHash, isteğin (customer_id + items) kanonik hash'idir; aynı
// idempotency_key farklı bir istekle tekrar kullanıldığında bunu tespit
// edebilmek için satırla birlikte saklanır (bkz. FindByIdempotencyKey ve
// server.go -- bu, K5 bulgusunun order-service seviyesindeki karşılığıdır:
// eskiden bu kontrol sadece payment/inventory-service'in kendi
// idempotency store'larında vardı, order-service'in KENDİ erken-dönüş
// kısayolunda YOKTU).
//
// idempotency_key üzerindeki unique index sayesinde aynı key ile eşzamanlı
// (concurrent) iki istek gelse bile veritabanı seviyesinde çift kayıt
// oluşamaz; ikinci istek ErrDuplicateIdempotencyKey alır ve çağıran taraf
// (server.go) mevcut siparişi FindByIdempotencyKey ile bulup onu döner.
func (r *PostgresRepository) CreatePendingOrderWithItems(ctx context.Context, orderID, customerID, idempotencyKey, requestHash string, items []OrderItem) (time.Time, error) {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return time.Time{}, fmt.Errorf("transaction başlatılamadı: %w", err)
	}
	defer tx.Rollback(ctx) // Commit edilmezse otomatik geri alınır.

	var createdAt time.Time
	err = tx.QueryRow(ctx,
		`INSERT INTO orders (id, customer_id, status, idempotency_key, idempotency_request_hash)
		 VALUES ($1, $2, 'PENDING', $3, $4)
		 RETURNING created_at`,
		orderID, customerID, idempotencyKey, requestHash,
	).Scan(&createdAt)
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23505" { // unique_violation
			return time.Time{}, ErrDuplicateIdempotencyKey
		}
		return time.Time{}, fmt.Errorf("pending sipariş eklenemedi: %w", err)
	}

	for _, item := range items {
		_, err := tx.Exec(ctx,
			`INSERT INTO order_items (order_id, product_id, quantity, unit_price_cents)
			 VALUES ($1, $2, $3, $4)`,
			orderID, item.ProductID, item.Quantity, item.UnitPriceCents,
		)
		if err != nil {
			return time.Time{}, fmt.Errorf("sipariş kalemi eklenemedi: %w", err)
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return time.Time{}, fmt.Errorf("transaction commit edilemedi: %w", err)
	}

	return createdAt, nil
}

// loadItems, verilen sipariş için order_items tablosundaki kalemleri okur.
func loadItems(ctx context.Context, q interface {
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
}, orderID string) ([]OrderItem, error) {
	rows, err := q.Query(ctx,
		`SELECT product_id, quantity, unit_price_cents FROM order_items WHERE order_id = $1 ORDER BY id`,
		orderID,
	)
	if err != nil {
		return nil, fmt.Errorf("sipariş kalemleri okunamadı: %w", err)
	}
	defer rows.Close()

	var items []OrderItem
	for rows.Next() {
		var it OrderItem
		if err := rows.Scan(&it.ProductID, &it.Quantity, &it.UnitPriceCents); err != nil {
			return nil, fmt.Errorf("sipariş kalemi okunamadı: %w", err)
		}
		items = append(items, it)
	}
	return items, rows.Err()
}

// FindByIdempotencyKey, verilen key ile daha önce oluşturulmuş bir sipariş
// olup olmadığını kontrol eder. Bulunamazsa (nil, nil) döner (hata değil).
func (r *PostgresRepository) FindByIdempotencyKey(ctx context.Context, idempotencyKey string) (*Order, error) {
	var o Order
	err := r.pool.QueryRow(ctx,
		`SELECT id, customer_id, status, COALESCE(failure_reason, ''), idempotency_key, COALESCE(idempotency_request_hash, ''), created_at
		 FROM orders WHERE idempotency_key = $1`,
		idempotencyKey,
	).Scan(&o.ID, &o.CustomerID, &o.Status, &o.FailureReason, &o.IdempotencyKey, &o.RequestHash, &o.CreatedAt)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("idempotency_key ile sipariş sorgulanamadı: %w", err)
	}

	items, err := loadItems(ctx, r.pool, o.ID)
	if err != nil {
		return nil, err
	}
	o.Items = items

	return &o, nil
}

// UpdateOrderStatus, event yayınlamaya ihtiyaç DUYMAYAN durum güncellemeleri
// için kullanılır (örn. henüz event üretmeyen ara adımlar). Saga'nın
// TERMİNAL sonucu (PAID/FAILED) için bunun yerine FinalizeOrder
// kullanılmalıdır -- o, durum güncellemesini ve karşılık gelen
// order.completed/order.failed outbox event'ini ATOMİK olarak birlikte
// yazar (bkz. rapor [K6]).
func (r *PostgresRepository) UpdateOrderStatus(ctx context.Context, orderID, status, reason string) error {
	_, err := r.pool.Exec(ctx,
		`UPDATE orders SET status = $1, failure_reason = $2, updated_at = now() WHERE id = $3`,
		status, reason, orderID,
	)
	if err != nil {
		return fmt.Errorf("sipariş durumu güncellenemedi: %w", err)
	}
	return nil
}

// FinalizeOrder, sipariş durumunu VE ilgili outbox event'ini (order.completed
// / order.failed) TEK BİR transaction içinde atomik olarak yazar (bkz.
// rapor [K6] ve pkg/outbox). orchestrator.go'daki Finalize metodu, Execute
// tamamlandıktan sonra bunu çağırır; ne server.go ne de recovery.go artık
// UpdateOrderStatus'u doğrudan çağırıp AYRICA bir yerlerde event
// yayınlamaya çalışmamalıdır.
func (r *PostgresRepository) FinalizeOrder(ctx context.Context, orderID, status, reason string, evt outbox.Event) error {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("transaction başlatılamadı: %w", err)
	}
	defer tx.Rollback(ctx) // Commit edilirse no-op olur.

	if _, err := tx.Exec(ctx,
		`UPDATE orders SET status = $1, failure_reason = $2, updated_at = now() WHERE id = $3`,
		status, reason, orderID,
	); err != nil {
		return fmt.Errorf("sipariş durumu güncellenemedi: %w", err)
	}

	outboxStore := outbox.NewStore(r.pool)
	if err := outboxStore.InsertTx(ctx, tx, evt); err != nil {
		return err
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("transaction commit edilemedi: %w", err)
	}
	return nil
}

func (r *PostgresRepository) GetOrder(ctx context.Context, orderID string) (*Order, error) {
	var o Order
	err := r.pool.QueryRow(ctx,
		`SELECT id, customer_id, status, COALESCE(failure_reason, ''), idempotency_key, created_at FROM orders WHERE id = $1`,
		orderID,
	).Scan(&o.ID, &o.CustomerID, &o.Status, &o.FailureReason, &o.IdempotencyKey, &o.CreatedAt)
	if err != nil {
		return nil, fmt.Errorf("sipariş sorgulanamadı: %w", err)
	}

	items, err := loadItems(ctx, r.pool, o.ID)
	if err != nil {
		return nil, err
	}
	o.Items = items

	return &o, nil
}

// RecordSagaStep, Saga akışının bir adımının başladığını/tamamlandığını/
// başarısız olduğunu kalıcı olarak kaydeder. reservationID/paymentID boş
// string olabilir (henüz üretilmemişse).
func (r *PostgresRepository) RecordSagaStep(ctx context.Context, orderID, step, stepStatus, reservationID, paymentID string) error {
	_, err := r.pool.Exec(ctx,
		`INSERT INTO saga_steps (order_id, step, step_status, reservation_id, payment_id)
		 VALUES ($1, $2, $3, NULLIF($4, ''), NULLIF($5, ''))`,
		orderID, step, stepStatus, reservationID, paymentID,
	)
	if err != nil {
		return fmt.Errorf("saga adımı kaydedilemedi: %w", err)
	}
	return nil
}

// SagaStep, saga_steps tablosundaki bir satırı temsil eder.
type SagaStep struct {
	Step          string
	StepStatus    string
	ReservationID string
	PaymentID     string
	CreatedAt     time.Time
}

// GetSagaSteps, verilen siparişin adım geçmişini kronolojik sırayla döner.
func (r *PostgresRepository) GetSagaSteps(ctx context.Context, orderID string) ([]SagaStep, error) {
	rows, err := r.pool.Query(ctx,
		`SELECT step, step_status, COALESCE(reservation_id, ''), COALESCE(payment_id, ''), created_at
		 FROM saga_steps WHERE order_id = $1 ORDER BY id ASC`,
		orderID,
	)
	if err != nil {
		return nil, fmt.Errorf("saga adımları okunamadı: %w", err)
	}
	defer rows.Close()

	var steps []SagaStep
	for rows.Next() {
		var s SagaStep
		if err := rows.Scan(&s.Step, &s.StepStatus, &s.ReservationID, &s.PaymentID, &s.CreatedAt); err != nil {
			return nil, fmt.Errorf("saga adımı okunamadı: %w", err)
		}
		steps = append(steps, s)
	}
	return steps, rows.Err()
}

// ClaimedOrders, ClaimIncompleteOrders'ın sonucudur.
type ClaimedOrders struct {
	Orders          []Order
	DeadLetteredIDs []string // maxAttempts'e ulaşıp otomatik kurtarmadan çıkarılan siparişler
}

// ClaimIncompleteOrders, terminal olmayan (PAID/FAILED/CANCELLED dışında)
// durumda takılı kalmış siparişleri ATOMİK olarak "claim" eder: hem seçer
// hem de recovery_attempts / next_recovery_attempt_at alanlarını TEK bir
// transaction içinde, `SELECT ... FOR UPDATE SKIP LOCKED` kullanarak
// günceller (bkz. rapor [K7]).
//
// Bu, eski implementasyondaki iki temel sorunu çözer:
//  1. Birden fazla order-service instance'ı (pod) aynı anda çalışıyorsa,
//     aynı takılı siparişi SKIP LOCKED sayesinde artık İKİ instance birden
//     claim edemez (eskiden hiç kilitleme/claim yoktu, sadece startup'ta
//     tek seferlik bir SELECT vardı).
//  2. Bu recovery denemesinin KENDİSİ de yarıda kesilirse (örn. process bu
//     kurtarma sırasında tekrar çöker), next_recovery_attempt_at ileri
//     itildiği için sipariş bir süre "lease" altında görünür ama sonsuza
//     dek kilitli KALMAZ; leaseDuration sonunda tekrar claim edilebilir
//     hale gelir. maxAttempts'e ulaşan siparişler recovery_dead_letter=true
//     işaretlenir ve bir daha otomatik denenmez -- bu, sonsuz retry/stuck
//     order operatör müdahalesi gerektiren bir alarm haline gelir (bkz.
//     rapor [K7] önerisi).
func (r *PostgresRepository) ClaimIncompleteOrders(ctx context.Context, minAge time.Duration, batchSize, maxAttempts int, leaseDuration time.Duration) (*ClaimedOrders, error) {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("transaction başlatılamadı: %w", err)
	}
	defer tx.Rollback(ctx)

	rows, err := tx.Query(ctx,
		`SELECT id, recovery_attempts FROM orders
		 WHERE status NOT IN ('PAID', 'FAILED', 'CANCELLED')
		   AND recovery_dead_letter = false
		   AND updated_at < now() - $1::interval
		   AND next_recovery_attempt_at <= now()
		 ORDER BY updated_at ASC
		 LIMIT $2
		 FOR UPDATE SKIP LOCKED`,
		fmt.Sprintf("%d seconds", int(minAge.Seconds())), batchSize,
	)
	if err != nil {
		return nil, fmt.Errorf("takılı kalmış siparişler sorgulanamadı: %w", err)
	}

	type candidate struct {
		id       string
		attempts int
	}
	var candidates []candidate
	for rows.Next() {
		var c candidate
		if err := rows.Scan(&c.id, &c.attempts); err != nil {
			rows.Close()
			return nil, fmt.Errorf("sipariş ID okunamadı: %w", err)
		}
		candidates = append(candidates, c)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}

	result := &ClaimedOrders{}
	leaseSeconds := fmt.Sprintf("%d seconds", int(leaseDuration.Seconds()))

	for _, c := range candidates {
		newAttempts := c.attempts + 1
		deadLetter := newAttempts > maxAttempts

		if _, err := tx.Exec(ctx,
			`UPDATE orders
			 SET recovery_attempts = $1,
			     next_recovery_attempt_at = now() + $2::interval,
			     recovery_dead_letter = $3
			 WHERE id = $4`,
			newAttempts, leaseSeconds, deadLetter, c.id,
		); err != nil {
			return nil, fmt.Errorf("sipariş claim edilemedi: %w", err)
		}

		if deadLetter {
			result.DeadLetteredIDs = append(result.DeadLetteredIDs, c.id)
			continue
		}

		o, err := getOrderTx(ctx, tx, c.id)
		if err != nil {
			return nil, err
		}
		result.Orders = append(result.Orders, *o)
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("transaction commit edilemedi: %w", err)
	}
	return result, nil
}

// getOrderTx, GetOrder'ın transaction-farkındalıklı (tx-aware) halidir.
// ClaimIncompleteOrders içinde, henüz commit edilmemiş bir transaction
// TARAFINDAN kilitlenmiş satırları okumak için kullanılır -- r.pool
// üzerinden ayrı bir bağlantıyla okumaya çalışmak, aynı satırın kilidini
// bekleyen bir DEADLOCK/timeout'a yol açardı.
func getOrderTx(ctx context.Context, tx pgx.Tx, orderID string) (*Order, error) {
	var o Order
	err := tx.QueryRow(ctx,
		`SELECT id, customer_id, status, COALESCE(failure_reason, ''), idempotency_key, created_at FROM orders WHERE id = $1`,
		orderID,
	).Scan(&o.ID, &o.CustomerID, &o.Status, &o.FailureReason, &o.IdempotencyKey, &o.CreatedAt)
	if err != nil {
		return nil, fmt.Errorf("sipariş sorgulanamadı (tx): %w", err)
	}

	items, err := loadItems(ctx, tx, o.ID)
	if err != nil {
		return nil, err
	}
	o.Items = items

	return &o, nil
}
