// notification-consumer, "order-events" topic'ini dinleyip müşteriye
// bildirim gönderen basit bir worker'dır. Gerçek bir bildirim sağlayıcısı
// (email, SMS, push) yerine burada sadece loglama yapılmaktadır;
// production'da bu katman bir bildirim gönderim API'sine bağlanır.
//
// Idempotency (bkz. rapor [K10]): Kafka "at-least-once" teslimat garantisi
// verdiği için aynı event iki kez gelebilir. Eskiden bu bellek-içi
// (in-memory) bir set ile filtreleniyordu -- servis restart olduğunda bu
// set SIFIRLANIYORDU, yani bir deploy/crash sonrası aynı bildirim tekrar
// gönderilebiliyordu. Artık işlenmiş event ID'leri Redis'te TTL'li olarak
// kalıcı tutulur; restart sonrası da hafıza korunur.
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"

	"order-engine/pkg/kafka"
)

func main() {
	brokers := []string{mustEnv("KAFKA_BROKERS")}
	groupID := mustEnv("KAFKA_CONSUMER_GROUP")
	redisAddr := mustEnv("REDIS_ADDR")

	consumer := kafka.NewConsumer(brokers, "order-events", groupID)
	defer consumer.Close()

	rdb := redis.NewClient(&redis.Options{Addr: redisAddr})
	defer rdb.Close()

	// 7 gün: Kafka'nın en-az-bir-kez teslimatının makul ölçüde tekrar
	// gönderebileceği bir pencereyi kapsayacak kadar uzun, ama sonsuza dek
	// büyümeyecek kadar sınırlı bir TTL.
	dedup := newRedisDedup(rdb, "notification-consumer", 7*24*time.Hour)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		sigCh := make(chan os.Signal, 1)
		signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
		<-sigCh
		log.Println("kapatma sinyali alındı, tüketim durduruluyor...")
		cancel()
	}()

	log.Println("notification-consumer başlatıldı, order-events dinleniyor...")
	err := consumer.Consume(ctx, func(ctx context.Context, evt kafka.Event) error {
		processed, err := dedup.alreadyProcessed(ctx, evt.ID)
		if err != nil {
			// Dedup deposuna (Redis) erişilemiyorsa, dedup GARANTİSİ
			// olmadan sessizce devam etmek yerine mesajı commit ETMEDEN
			// hata döndürüyoruz -- bir sonraki poll'da tekrar denenecek
			// (bkz. rapor [K10]).
			return fmt.Errorf("dedup deposuna erişilemedi: %w", err)
		}
		if processed {
			log.Printf("event zaten işlenmiş, atlanıyor: id=%s type=%s", evt.ID, evt.Type)
			return nil
		}

		switch evt.Type {
		case "order.completed":
			log.Printf("[BİLDİRİM] Siparişiniz onaylandı: order_id=%s", evt.AggregateID)
		case "order.failed":
			log.Printf("[BİLDİRİM] Siparişiniz işlenemedi: order_id=%s", evt.AggregateID)
		default:
			// Bilinmeyen event tipleri, şemaya sonradan eklenmiş yeni bir
			// event tipi olabilir. Sessizce atlamak yerine gözlemlenebilir
			// (observable) bir uyarı olarak logluyoruz; production'da bu
			// bir metrik/alert'e bağlanmalı ve tekrarlayan "unknown event"
			// tipleri bir dead-letter/inceleme kuyruğuna alınmalıdır.
			log.Printf("UYARI: bilinmeyen event tipi (şema sürüm uyuşmazlığı olabilir), atlanıyor: id=%s type=%s", evt.ID, evt.Type)
		}

		if err := dedup.markProcessed(ctx, evt.ID); err != nil {
			// Bildirim (yukarıda) zaten "gönderildi" ama bunu kalıcı
			// olarak işaretleyemedik: bir sonraki retry aynı bildirimi
			// tekrar gönderebilir. Veri kaybından (bildirim asla
			// gönderilmeme riski) çok daha az kötü bir durum olduğu için
			// bunu KRİTİK seviyede loglayıp hatayı yine de döndürüyoruz ki
			// mesaj commit edilmesin ve durum gözlemlenebilir olsun.
			log.Printf("KRİTİK: event işlenmiş olarak işaretlenemedi (retry aynı bildirimi tekrar gönderebilir): id=%s error=%v", evt.ID, err)
			return fmt.Errorf("dedup işaretleme başarısız: %w", err)
		}
		return nil
	})
	if err != nil && ctx.Err() == nil {
		log.Fatalf("consumer hata ile durdu: %v", err)
	}
}

func mustEnv(key string) string {
	v := os.Getenv(key)
	if v == "" {
		log.Fatalf("zorunlu env değişkeni eksik: %s", key)
	}
	return v
}

// redisDedup, işlenmiş event ID'lerini Redis'te TTL'li anahtarlar olarak
// tutan kalıcı bir tekilleştirme (deduplication) deposudur. Eskiden
// kullanılan bellek-içi map, servis her yeniden başladığında sıfırlanıyordu
// (bkz. rapor [K10]); bu artık mümkün değildir.
type redisDedup struct {
	rdb    *redis.Client
	prefix string
	ttl    time.Duration
}

func newRedisDedup(rdb *redis.Client, prefix string, ttl time.Duration) *redisDedup {
	return &redisDedup{rdb: rdb, prefix: prefix, ttl: ttl}
}

func (d *redisDedup) key(eventID string) string {
	return fmt.Sprintf("%s:processed:%s", d.prefix, eventID)
}

func (d *redisDedup) alreadyProcessed(ctx context.Context, eventID string) (bool, error) {
	n, err := d.rdb.Exists(ctx, d.key(eventID)).Result()
	if err != nil {
		return false, fmt.Errorf("redis exists hatası: %w", err)
	}
	return n > 0, nil
}

func (d *redisDedup) markProcessed(ctx context.Context, eventID string) error {
	if err := d.rdb.Set(ctx, d.key(eventID), "1", d.ttl).Err(); err != nil {
		return fmt.Errorf("redis set hatası: %w", err)
	}
	return nil
}
