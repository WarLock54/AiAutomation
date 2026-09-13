// Package outbox, "Transactional Outbox Pattern" implementasyonudur.
//
// Sorun (bkz. rapor [K6]): DB'ye bir kayıt yazıp (örn. ödeme kaydı) hemen
// ardından ayrı bir adımda Kafka'ya event publish etmek dual-write'tır:
//   - DB commit başarılı olur ama publish başarısız olursa: event
//     SONSUZA DEK KAYBOLUR (downstream servisler asla haberdar olmaz).
//   - Publish başarılı olur ama süreç DB commit'ten hemen sonra/önce
//     çökerse, ya da retry mantığı publish'i tekrarlarsa: aynı event İKİ
//     KEZ yayınlanabilir.
//
// Çözüm: event, iş verisiyle AYNI veritabanı transaction'ında bir
// outbox_events tablosuna yazılır (InsertTx). Bu satır ya iş verisiyle
// BİRLİKTE commit olur ya da hiç olmaz -- artık "yazıldı ama publish
// edilemedi" durumu yoktur, sadece "yazıldı ama HENÜZ publish edilmedi"
// durumu vardır. Ayrı bir Publisher, bu tabloyu periyodik olarak tarar
// (SELECT ... FOR UPDATE SKIP LOCKED ile, böylece birden fazla instance
// aynı satırı iki kez işlemez) ve gerçek Kafka publish'ini üstel
// geri-çekilme (exponential backoff) ile dener; kalıcı olarak başarısız
// olan event'ler dead-letter olarak işaretlenir.
package outbox

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"order-engine/pkg/kafka"
)

// Event, outbox tablosuna yazılacak bir olayı temsil eder.
type Event struct {
	ID          string
	AggregateID string
	EventType   string
	Topic       string
	Payload     json.RawMessage
}

// NewEvent, ID'si otomatik üretilen (deterministik olmayan ama tüm
// consumer'larda dedup anahtarı olarak kullanılabilecek) bir Event
// oluşturur.
func NewEvent(topic, eventType, aggregateID string, payload any) (Event, error) {
	body, err := json.Marshal(payload)
	if err != nil {
		return Event{}, fmt.Errorf("outbox payload marshal edilemedi: %w", err)
	}
	return Event{
		ID:          uuid.NewString(),
		AggregateID: aggregateID,
		EventType:   eventType,
		Topic:       topic,
		Payload:     body,
	}, nil
}

// Store, outbox_events tablosuna transaction-içi erişim sağlar. Her servis
// kendi PostgresRepository'sinin altındaki *pgxpool.Pool'u NewStore'a
// vererek kullanır; migrations dizinine outbox_events tablosunu
// oluşturan bir migration eklenmelidir (bkz. örn.
// services/payment-service/migrations/002_create_outbox.sql).
type Store struct {
	pool *pgxpool.Pool
}

func NewStore(pool *pgxpool.Pool) *Store {
	return &Store{pool: pool}
}

// InsertTx, verilen event'i VERİLEN transaction İÇİNDE ekler. Çağıran
// taraf, iş verisini yazan AYNI pgx.Tx'i buraya vermelidir ki ikisi atomik
// olarak ya birlikte commit olsun ya da hiç olmasın.
func (s *Store) InsertTx(ctx context.Context, tx pgx.Tx, evt Event) error {
	_, err := tx.Exec(ctx, `
		INSERT INTO outbox_events (id, aggregate_id, event_type, topic, payload)
		VALUES ($1, $2, $3, $4, $5)
	`, evt.ID, evt.AggregateID, evt.EventType, evt.Topic, evt.Payload)
	if err != nil {
		return fmt.Errorf("outbox event eklenemedi: %w", err)
	}
	return nil
}

type claimedRow struct {
	id          string
	aggregateID string
	eventType   string
	topic       string
	payload     []byte
	attempts    int
}

// PublisherConfig, arka plan yayıncısının davranışını belirler.
type PublisherConfig struct {
	PollInterval time.Duration
	BatchSize    int
	MaxAttempts  int
	// next_attempt_at = now() + min(MaxBackoff, BaseBackoff * 2^attempts)
	BaseBackoff time.Duration
	MaxBackoff  time.Duration
}

func DefaultPublisherConfig() PublisherConfig {
	return PublisherConfig{
		PollInterval: 500 * time.Millisecond,
		BatchSize:    50,
		MaxAttempts:  10,
		BaseBackoff:  1 * time.Second,
		MaxBackoff:   2 * time.Minute,
	}
}

// Publisher, outbox_events tablosundaki yayınlanmamış olayları periyodik
// olarak claim edip gerçek Kafka publish işlemini yapar. Birden fazla
// instance (pod) aynı anda Run çalıştırabilir: `FOR UPDATE SKIP LOCKED`
// sayesinde aynı satırı iki instance birden claim edemez (bkz. rapor
// [K7]'deki claim/lease deseniyle aynı ilke).
type Publisher struct {
	pool     *pgxpool.Pool
	producer *kafka.Producer
	cfg      PublisherConfig
	logger   *slog.Logger
}

func NewPublisher(pool *pgxpool.Pool, producer *kafka.Producer, cfg PublisherConfig, logger *slog.Logger) *Publisher {
	if logger == nil {
		logger = slog.Default()
	}
	return &Publisher{pool: pool, producer: producer, cfg: cfg, logger: logger}
}

// Run, ctx iptal edilene kadar periyodik olarak outbox tablosunu tarar.
// Bloklayan bir çağrıdır; `go publisher.Run(ctx)` ile ayrı bir goroutine'de
// başlatılması amaçlanır.
func (p *Publisher) Run(ctx context.Context) {
	ticker := time.NewTicker(p.cfg.PollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := p.publishBatch(ctx); err != nil {
				p.logger.Error("outbox publish batch başarısız", "error", err)
			}
		}
	}
}

func (p *Publisher) publishBatch(ctx context.Context) error {
	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("outbox transaction başlatılamadı: %w", err)
	}
	defer tx.Rollback(ctx) // Commit edilirse no-op olur.

	rows, err := tx.Query(ctx, `
		SELECT id, aggregate_id, event_type, topic, payload, attempts
		FROM outbox_events
		WHERE published_at IS NULL AND dead_letter = false AND next_attempt_at <= now()
		ORDER BY created_at ASC
		LIMIT $1
		FOR UPDATE SKIP LOCKED
	`, p.cfg.BatchSize)
	if err != nil {
		return fmt.Errorf("outbox satırları claim edilemedi: %w", err)
	}

	var claimed []claimedRow
	for rows.Next() {
		var r claimedRow
		if err := rows.Scan(&r.id, &r.aggregateID, &r.eventType, &r.topic, &r.payload, &r.attempts); err != nil {
			rows.Close()
			return fmt.Errorf("outbox satırı okunamadı: %w", err)
		}
		claimed = append(claimed, r)
	}
	if err := rows.Err(); err != nil {
		return err
	}
	rows.Close()

	if len(claimed) == 0 {
		return tx.Commit(ctx)
	}

	for _, r := range claimed {
		evt := kafka.Event{
			ID:          r.id,
			Type:        r.eventType,
			AggregateID: r.aggregateID,
			Payload:     r.payload,
		}

		// NOT: producer aynı transaction'ın parçası DEĞİLDİR (Kafka XA
		// desteklemez); bu yüzden claim + durum güncellemesi bu tek DB
		// transaction'ı içinde yapılır ki SKIP LOCKED ile başka bir
		// instance aynı satırı bu sırada işlemesin. Publish başarısız
		// olursa satır "unlock" olmak yerine backoff ile yeniden
		// denenmek üzere işaretlenir (aşağıda).
		pubErr := p.producer.Publish(ctx, r.aggregateID, evt)
		if pubErr != nil {
			attempts := r.attempts + 1
			backoffExp := attempts
			if backoffExp > 20 {
				backoffExp = 20 // taşmayı (overflow) önle
			}
			backoff := p.cfg.BaseBackoff * time.Duration(int64(1)<<uint(backoffExp))
			if backoff > p.cfg.MaxBackoff || backoff <= 0 {
				backoff = p.cfg.MaxBackoff
			}

			deadLetter := attempts >= p.cfg.MaxAttempts
			if deadLetter {
				p.logger.Error("outbox event maksimum deneme sayısına ulaştı, dead-letter olarak işaretlendi (manuel inceleme gerekiyor)",
					"id", r.id, "event_type", r.eventType, "attempts", attempts, "error", pubErr)
			} else {
				p.logger.Warn("outbox event publish edilemedi, yeniden denenecek",
					"id", r.id, "event_type", r.eventType, "attempts", attempts, "backoff", backoff, "error", pubErr)
			}

			if _, err := tx.Exec(ctx, `
				UPDATE outbox_events
				SET attempts = $1, next_attempt_at = now() + $2::interval, last_error = $3, dead_letter = $4
				WHERE id = $5
			`, attempts, fmt.Sprintf("%d seconds", int(backoff.Seconds())), pubErr.Error(), deadLetter, r.id); err != nil {
				return fmt.Errorf("outbox retry durumu güncellenemedi: %w", err)
			}
			continue
		}

		if _, err := tx.Exec(ctx, `
			UPDATE outbox_events SET published_at = now() WHERE id = $1
		`, r.id); err != nil {
			return fmt.Errorf("outbox published_at güncellenemedi: %w", err)
		}
	}

	return tx.Commit(ctx)
}
