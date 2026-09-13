package internal

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"

	"order-engine/pkg/outbox"
)

// PaymentRecord, payments tablosundaki bir satırı temsil eder.
type PaymentRecord struct {
	ID          string
	OrderID     string
	CustomerID  string
	AmountCents int64
	Currency    string
	Status      string // SUCCEEDED, FAILED
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

// InsertPaymentWithOutbox, ödeme kaydını VE ilgili outbox event'ini (örn.
// "payment.succeeded") TEK BİR transaction içinde atomik olarak ekler.
//
// Eskiden (bkz. rapor [K6]) bu iki yazım ayrı adımlardı: ödeme DB'ye
// yazılır, ardından AYRI bir Kafka publish çağrısı yapılırdı. Publish
// başarısız olursa event sonsuza dek kaybolurdu; publish sonrası DB
// commit başarısız olursa (ya da retry ile ikinci kez çağrılırsa) event
// iki kez yayınlanabilirdi. Artık event, ödeme satırıyla AYNI transaction
// içinde outbox_events tablosuna yazılıyor: ya ikisi de commit olur ya
// hiçbiri. Gerçek Kafka publish'i, outbox.Publisher tarafından ayrı bir
// arka plan döngüsünde, en-az-bir-kez (at-least-once) garantisiyle
// yapılır (bkz. main.go ve pkg/outbox).
func (r *PostgresRepository) InsertPaymentWithOutbox(ctx context.Context, p PaymentRecord, evt outbox.Event) error {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("transaction başlatılamadı: %w", err)
	}
	defer tx.Rollback(ctx) // Commit edilirse no-op olur.

	if _, err := tx.Exec(ctx, `
		INSERT INTO payments (id, order_id, customer_id, amount_cents, currency, status)
		VALUES ($1, $2, $3, $4, $5, $6)
	`, p.ID, p.OrderID, p.CustomerID, p.AmountCents, p.Currency, p.Status); err != nil {
		return fmt.Errorf("ödeme kaydı eklenemedi: %w", err)
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
