package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
)

func main() {
	brokers := splitCSV(env("KAFKA_BROKERS", "localhost:9094"))
	topic := env("KAFKA_TOPIC", "orders")
	glob := env("DATA_GLOB", "data/*.json")
	log.Printf("brokers=%v topic=%s glob=%s", brokers, topic, glob)

	w := &kafka.Writer{
		Addr:         kafka.TCP(brokers...),
		Topic:        topic,
		Balancer:     &kafka.Hash{},
		RequiredAcks: kafka.RequireAll,
	}
	defer w.Close()

	paths, _ := filepath.Glob(glob)

	// Если файлов нет, отправим тестовый заказ
	if len(paths) == 0 {
		msg := map[string]any{
			"order_uid":    "b563feb7b2b84b6test",
			"track_number": "WBILMTESTTRACK",
			"entry":        "WBIL",
			"delivery": map[string]any{
				"name": "Test Testov", "phone": "+9720000000", "zip": "2639809", "city": "Kiryat Mozkin", "address": "Ploshad Mira 15", "region": "Kraiot", "email": "test@gmail.com",
			},
			"payment": map[string]any{
				"transaction": "b563feb7b2b84b6test", "request_id": "", "currency": "USD", "provider": "wbpay",
				"amount": 1817, "payment_dt": 1637907727, "bank": "alpha", "delivery_cost": 1500, "goods_total": 317, "custom_fee": 0,
			},
			"items": []map[string]any{{
				"chrt_id": 9934930, "track_number": "WBILMTESTTRACK", "price": 453, "rid": "ab4219087a764ae0btest",
				"name": "Mascaras", "sale": 30, "size": "0", "total_price": 317, "nm_id": 2389212, "brand": "Vivienne Sabo", "status": 202,
			}},
			"locale": "en", "internal_signature": "", "customer_id": "test", "delivery_service": "meest", "shardkey": "9", "sm_id": 99,
			"date_created": "2021-11-26T06:22:19Z", "oof_shard": "1",
		}
		b, _ := json.Marshal(msg)
		if err := w.WriteMessages(context.Background(), kafka.Message{
			Key:   []byte("b563feb7b2b84b6test"),
			Value: b,
			Time:  time.Now(),
			Headers: []kafka.Header{
				{Key: "content-type", Value: []byte("application/json")},
			},
		}); err != nil {
			log.Fatalf("produce: %v", err)
		}
		log.Println("produced 1 message (fallback)")
		return
	}

	// Иначе читаем все файлы и шлём каждый объект
	total := 0
	for _, p := range paths {
		n, err := produceFile(context.Background(), w, p)
		if err != nil {
			log.Printf("file %s: %v", p, err)
		}
		total += n
	}
	log.Printf("done: produced=%d from %d files", total, len(paths))
}

func produceFile(ctx context.Context, w *kafka.Writer, path string) (int, error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, fmt.Errorf("open: %w", err)
	}
	defer f.Close()
	b, err := io.ReadAll(f)
	if err != nil {
		return 0, fmt.Errorf("read: %w", err)
	}

	// поддерживаем и один объект, и массив объектов
	var one map[string]any
	if err := json.Unmarshal(b, &one); err == nil && len(one) > 0 {
		return sendOne(ctx, w, one, filepath.Base(path))
	}
	var many []map[string]any
	if err := json.Unmarshal(b, &many); err == nil && len(many) > 0 {
		sum := 0
		for _, obj := range many {
			n, _ := sendOne(ctx, w, obj, filepath.Base(path))
			sum += n
		}
		return sum, nil
	}
	return 0, fmt.Errorf("invalid JSON in %s: must be object or array of objects", path)
}

func sendOne(ctx context.Context, w *kafka.Writer, obj map[string]any, source string) (int, error) {
	key := "no-order-uid"
	if v, ok := obj["order_uid"].(string); ok && v != "" {
		key = v
	}
	val, _ := json.Marshal(obj)
	err := w.WriteMessages(ctx, kafka.Message{
		Key:   []byte(key),
		Value: val,
		Time:  time.Now(),
		Headers: []kafka.Header{
			{Key: "content-type", Value: []byte("application/json")},
			{Key: "source", Value: []byte(source)},
		},
	})
	if err != nil {
		return 0, err
	}
	log.Printf("produced key=%s src=%s", key, source)
	return 1, nil
}

func env(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
func splitCSV(s string) []string {
	ps := strings.Split(s, ",")
	out := make([]string, 0, len(ps))
	for _, p := range ps {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}
