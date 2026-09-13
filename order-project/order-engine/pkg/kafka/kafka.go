// Package kafka, servisler arası event yayınlama/tüketme için ince bir
// sarmalayıcı (wrapper) sağlar. Amaç: her servisin kendi kafka-go boilerplate'ini
// tekrar yazmaması ve dual-write sorununa karşı tutarlı bir yaklaşım izlenmesi.
//
// NOT: Proje 1 kapsamında DB yazımı + event publish işlemi iki ayrı adımdır
// (bkz. ilgili repository dosyalarındaki "DİKKAT" notları). Tam bir çözüm
// için Transactional Outbox Pattern gerekir; bu Proje 2'de (.NET + MassTransit)
// uçtan uca implemente edilecek.
package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	kafkago "github.com/segmentio/kafka-go"
)

// Event, tüm servislerin ortak kullandığı zarf (envelope) formatıdır.
// ID alanı consumer tarafında deduplication (processed_events tablosu/seti)
// için kullanılmalıdır.
type Event struct {
	ID            string          `json:"id"`
	Type          string          `json:"type"`
	AggregateID   string          `json:"aggregate_id"` // örn: order_id
	OccurredAt    time.Time       `json:"occurred_at,omitempty"`
	Payload       json.RawMessage `json:"payload"`
	SchemaVersion int             `json:"schema_version,omitempty"`
}

type Producer struct {
	writer *kafkago.Writer
}

func NewProducer(brokers []string, topic string) *Producer {
	return &Producer{
		writer: &kafkago.Writer{
			Addr:         kafkago.TCP(brokers...),
			Topic:        topic,
			Balancer:     &kafkago.Hash{}, // key bazlı partition -> aynı order_id hep aynı partition'a
			RequiredAcks: kafkago.RequireAll,
			Async:        false,
		},
	}
}

// Publish, verilen event'i belirtilen partition key ile yayınlar. Partition
// key olarak genellikle AggregateID (örn. order_id) kullanılır; bu sayede
// aynı sipariş için üretilen event'lerin sıralaması korunur.
func (p *Producer) Publish(ctx context.Context, key string, evt Event) error {
	if evt.OccurredAt.IsZero() {
		evt.OccurredAt = time.Now().UTC()
	}

	body, err := json.Marshal(evt)
	if err != nil {
		return fmt.Errorf("event marshal error: %w", err)
	}

	msg := kafkago.Message{
		Key:   []byte(key),
		Value: body,
		Headers: []kafkago.Header{
			{Key: "event_type", Value: []byte(evt.Type)},
			{Key: "event_id", Value: []byte(evt.ID)},
		},
		Time: evt.OccurredAt,
	}

	return p.writer.WriteMessages(ctx, msg)
}

func (p *Producer) Close() error {
	return p.writer.Close()
}

type ConsumerHandler func(ctx context.Context, evt Event) error

type Consumer struct {
	reader *kafkago.Reader
}

func NewConsumer(brokers []string, topic, groupID string) *Consumer {
	return &Consumer{
		reader: kafkago.NewReader(kafkago.ReaderConfig{
			Brokers:  brokers,
			Topic:    topic,
			GroupID:  groupID,
			MinBytes: 1,
			MaxBytes: 10e6,
		}),
	}
}

// Consume, mesajları tek tek okuyup handler'a iletir. Handler hata döndürürse
// mesaj commit edilmez ve bir sonraki poll'da tekrar işlenir - bu yüzden
// handler'lar Event.ID bazlı deduplication ile idempotent yazılmalıdır.
func (c *Consumer) Consume(ctx context.Context, handler ConsumerHandler) error {
	for {
		msg, err := c.reader.FetchMessage(ctx)
		if err != nil {
			return fmt.Errorf("fetch message error: %w", err)
		}

		var evt Event
		if err := json.Unmarshal(msg.Value, &evt); err != nil {
			// Deserialize edilemeyen mesajlar poison-pill olabilir; şimdilik
			// atlayıp commit ediyoruz. Prod'da dead-letter topic'e taşınmalı.
			_ = c.reader.CommitMessages(ctx, msg)
			continue
		}

		if err := handler(ctx, evt); err != nil {
			return fmt.Errorf("handler error, message will be retried: %w", err)
		}

		if err := c.reader.CommitMessages(ctx, msg); err != nil {
			return fmt.Errorf("commit error: %w", err)
		}
	}
}

func (c *Consumer) Close() error {
	return c.reader.Close()
}
