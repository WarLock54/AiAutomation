package internal

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

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

type StockItem struct {
	ProductID string
	Quantity  int32
}

// TryReserve, her ürün satırını `SELECT ... FOR UPDATE` ile kilitleyerek
// eşzamanlı rezervasyon isteklerinde race condition'ı (aynı anda iki
// siparişin aynı son ürünü rezerve etmesi) engeller. Tüm ürünler yeterliyse
// stok düşürülür ve bir reservations kaydı oluşturulur; herhangi biri
// yetersizse transaction rollback edilir ve hangi ürünlerin yetersiz
// olduğu döndürülür.
func (r *PostgresRepository) TryReserve(ctx context.Context, orderID string, items []StockItem) (reservationID string, insufficientProductIDs []string, err error) {
	tx, err := r.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable})
	if err != nil {
		return "", nil, fmt.Errorf("transaction başlatılamadı: %w", err)
	}
	defer tx.Rollback(ctx) // Commit edilirse no-op olur.

	for _, item := range items {
		var available int32
		err := tx.QueryRow(ctx,
			`SELECT available_quantity FROM stock_items WHERE product_id = $1 FOR UPDATE`,
			item.ProductID,
		).Scan(&available)
		if err != nil {
			if err == pgx.ErrNoRows {
				insufficientProductIDs = append(insufficientProductIDs, item.ProductID)
				continue
			}
			return "", nil, fmt.Errorf("stok satırı okunamadı (product=%s): %w", item.ProductID, err)
		}
		if available < item.Quantity {
			insufficientProductIDs = append(insufficientProductIDs, item.ProductID)
		}
	}

	if len(insufficientProductIDs) > 0 {
		return "", insufficientProductIDs, nil // rollback defer ile olur
	}

	reservationID = orderID // basitlik için 1-1 eşleme; production'da ayrı UUID önerilir
	for _, item := range items {
		if _, err := tx.Exec(ctx,
			`UPDATE stock_items SET available_quantity = available_quantity - $1 WHERE product_id = $2`,
			item.Quantity, item.ProductID,
		); err != nil {
			return "", nil, fmt.Errorf("stok düşürülemedi: %w", err)
		}
		if _, err := tx.Exec(ctx,
			`INSERT INTO reservations (reservation_id, product_id, quantity, status) VALUES ($1, $2, $3, 'RESERVED')`,
			reservationID, item.ProductID, item.Quantity,
		); err != nil {
			return "", nil, fmt.Errorf("rezervasyon kaydı eklenemedi: %w", err)
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return "", nil, fmt.Errorf("transaction commit edilemedi: %w", err)
	}
	return reservationID, nil, nil
}

// Release, Saga'nın telafi (compensating) adımıdır: ödeme başarısız
// olduğunda rezerve edilen stoğu geri iade eder.
//
// DİKKAT (bkz. rapor [K8]): eski implementasyon önce `SELECT ... WHERE
// status = 'RESERVED'` ile satırları OKUYOR, sonra stok iadesini yapıyor,
// EN SONDA da `UPDATE reservations SET status = 'RELEASED' WHERE
// reservation_id = $1` çalıştırıyordu -- bu son UPDATE'te status='RESERVED'
// koşulu YOKTU. Bu iki sorun yaratıyordu:
//  1. SELECT satırları kilitlemediği için (FOR UPDATE yoktu), Release aynı
//     anda iki kez çağrılırsa (örn. Saga recovery + orijinal çağrı çakışırsa)
//     her iki çağrı da "hâlâ RESERVED" görüp stoğu İKİ KEZ iade edebilirdi.
//  2. Son UPDATE koşulsuz olduğu için, COMMITTED bir rezervasyon üzerinde
//     de (yanlışlıkla ya da geç gelen bir retry ile) çağrılırsa durumu
//     RELEASED'e çevirip veri bütünlüğünü bozabilirdi.
//
// Artık: satırlar `FOR UPDATE` ile kilitleniyor VE final UPDATE, sadece
// hâlâ RESERVED olan satırları hedefliyor (state-guarded transition).
// Böylece stok fazladan artamaz, COMMITTED bir rezervasyon asla
// RELEASED'e geçemez ve fonksiyon idempotent kalır (zaten RELEASED/
// COMMITTED bir reservation_id için ikinci çağrı sessizce no-op'tur --
// bu, Saga recovery'nin aynı compensating adımı güvenle tekrar
// çalıştırabilmesi için gereklidir, bkz. rapor [K7]).
func (r *PostgresRepository) Release(ctx context.Context, reservationID string) error {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("transaction başlatılamadı: %w", err)
	}
	defer tx.Rollback(ctx)

	rows, err := tx.Query(ctx,
		`SELECT product_id, quantity FROM reservations
		 WHERE reservation_id = $1 AND status = 'RESERVED'
		 FOR UPDATE`,
		reservationID,
	)
	if err != nil {
		return fmt.Errorf("rezervasyonlar okunamadı: %w", err)
	}

	type row struct {
		productID string
		quantity  int32
	}
	var toRelease []row
	for rows.Next() {
		var rr row
		if err := rows.Scan(&rr.productID, &rr.quantity); err != nil {
			rows.Close()
			return fmt.Errorf("satır okunamadı: %w", err)
		}
		toRelease = append(toRelease, rr)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return fmt.Errorf("rezervasyon satırları okunurken hata: %w", err)
	}

	if len(toRelease) == 0 {
		// Bu reservation_id ya hiç yok ya da zaten RELEASED/COMMITTED
		// durumunda: idempotent no-op olarak kabul ediyoruz (retry-safe).
		return tx.Commit(ctx)
	}

	for _, rr := range toRelease {
		if _, err := tx.Exec(ctx,
			`UPDATE stock_items SET available_quantity = available_quantity + $1 WHERE product_id = $2`,
			rr.quantity, rr.productID,
		); err != nil {
			return fmt.Errorf("stok iadesi başarısız: %w", err)
		}
	}

	cmdTag, err := tx.Exec(ctx,
		`UPDATE reservations SET status = 'RELEASED' WHERE reservation_id = $1 AND status = 'RESERVED'`,
		reservationID,
	)
	if err != nil {
		return fmt.Errorf("rezervasyon durumu güncellenemedi: %w", err)
	}
	if int(cmdTag.RowsAffected()) != len(toRelease) {
		// FOR UPDATE ile satırları zaten kilitledik, bu normalde
		// oluşmamalı; oluşursa savunma amaçlı olarak transaction'ı
		// başarısız sayıyoruz (aksi halde kısmi bir stok iadesi olurdu).
		return fmt.Errorf("beklenmeyen eşzamanlı değişiklik: reservation_id=%s (%d/%d satır güncellendi)",
			reservationID, cmdTag.RowsAffected(), len(toRelease))
	}

	return tx.Commit(ctx)
}

// Commit, ödeme başarılı olduktan sonra rezervasyonu kalıcı hale getirir
// (stok zaten düşürülmüştü; burada sadece durum güncelleniyor).
//
// DİKKAT (bkz. rapor [K8]): eski implementasyon `UPDATE reservations SET
// status = 'COMMITTED' WHERE reservation_id = $1` şeklinde KOŞULSUZDU;
// zaten RELEASED (stoğu iade edilmiş) bir rezervasyonu da sessizce
// COMMITTED'e çevirebilirdi -- bu, stoğu iade edilmiş bir üründe hem
// "iade edildi" hem de "satıldı" gibi çelişen bir duruma yol açar. Artık
// state transition `WHERE ... AND status = 'RESERVED'` koşuluna bağlı;
// etkilenen satır sayısı kontrol edilip idempotent (zaten COMMITTED) ile
// gerçek bir tutarsızlık (RELEASED üzerinde commit denemesi) birbirinden
// ayrılıyor.
func (r *PostgresRepository) Commit(ctx context.Context, reservationID string) error {
	cmdTag, err := r.pool.Exec(ctx,
		`UPDATE reservations SET status = 'COMMITTED' WHERE reservation_id = $1 AND status = 'RESERVED'`,
		reservationID,
	)
	if err != nil {
		return fmt.Errorf("rezervasyon commit edilemedi: %w", err)
	}
	if cmdTag.RowsAffected() > 0 {
		return nil
	}

	// Hiçbir satır etkilenmedi: ya reservation_id hiç yok, ya zaten
	// COMMITTED (idempotent retry -- Saga recovery'nin CommitStock'u
	// güvenle tekrar çağırabilmesi için no-op dönülür, bkz. [K7]), ya da
	// RELEASED (gerçek bir tutarsızlık -- bu bir hata olarak raporlanır).
	rows, err := r.pool.Query(ctx,
		`SELECT DISTINCT status FROM reservations WHERE reservation_id = $1`,
		reservationID,
	)
	if err != nil {
		return fmt.Errorf("rezervasyon durumu doğrulanamadı: %w", err)
	}
	defer rows.Close()

	var statuses []string
	for rows.Next() {
		var s string
		if err := rows.Scan(&s); err != nil {
			return fmt.Errorf("rezervasyon durumu okunamadı: %w", err)
		}
		statuses = append(statuses, s)
	}
	if err := rows.Err(); err != nil {
		return err
	}

	if len(statuses) == 0 {
		return fmt.Errorf("rezervasyon bulunamadı, commit edilemedi: %s", reservationID)
	}
	if len(statuses) == 1 && statuses[0] == "COMMITTED" {
		return nil // idempotent tekrar deneme
	}
	return fmt.Errorf(
		"rezervasyon beklenmeyen durumda (%v), commit edilemedi -- olası tutarsızlık: %s",
		statuses, reservationID,
	)
}
