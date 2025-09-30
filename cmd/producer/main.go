package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/brianvoe/gofakeit/v7"
	"github.com/segmentio/kafka-go"

	"demo/orders/internal/model"
)

func main() {
	gofakeit.Seed(time.Now().UnixNano())

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

	defer func() {
		if err := w.Close(); err != nil {
			log.Printf("close writer: %v", err)
		}
	}()

	paths, err := filepath.Glob(glob)
	if err != nil {
		log.Fatalf("glob %s: %v", glob, err)
	}

	// ФОЛЛБЭК: если файлов нет — генерим N заказов
	if len(paths) == 0 {
		n := mustInt("1", os.Getenv("GEN_COUNT"))
		gap := mustInt("0", os.Getenv("GEN_INTERVAL_MS")) // мс
		for i := 0; i < n; i++ {
			o := fakeOrder()
			if _, err := sendOrder(context.Background(), w, o, "generated"); err != nil {
				log.Fatalf("produce: %v", err)
			}
			if gap > 0 {
				time.Sleep(time.Duration(gap) * time.Millisecond)
			}
		}
		log.Printf("produced %d generated message(s)", n)
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

func fakeOrder() model.Order {
	return model.Order{
		OrderUID:    gofakeit.UUID(),
		TrackNumber: strings.ToUpper(gofakeit.LetterN(4) + gofakeit.DigitN(6)),
		Entry:       "WBIL",
		Delivery: model.Delivery{
			Name:    gofakeit.Name(),
			Phone:   gofakeit.Phone(),
			Zip:     gofakeit.Zip(),
			City:    gofakeit.City(),
			Address: gofakeit.Street(),
			Region:  gofakeit.State(),
			Email:   gofakeit.Email(),
		},
		Payment: model.Payment{
			Transaction:  gofakeit.UUID(),
			Currency:     "USD",
			Provider:     "wbpay",
			Amount:       gofakeit.Number(100, 100000),
			PaymentDT:    time.Now().Unix(),
			Bank:         "alpha",
			DeliveryCost: 500,
			GoodsTotal:   200,
			CustomFee:    0,
		},
		Items: []model.Item{{
			ChrtID:      int64(gofakeit.Number(1000, 999999)),
			TrackNumber: "WB" + strings.ToUpper(gofakeit.LetterN(2)+gofakeit.DigitN(6)),
			Price:       gofakeit.Number(10, 10000),
			RID:         gofakeit.UUID(),
			Name:        gofakeit.ProductName(),
			Sale:        gofakeit.Number(0, 90),
			Size:        "M",
			TotalPrice:  gofakeit.Number(10, 10000),
			NmID:        int64(gofakeit.Number(1000, 999999)),
			Brand:       gofakeit.Company(),
			Status:      200,
		}},
		Locale:      "en",
		CustomerID:  gofakeit.Username(),
		DateCreated: time.Now().UTC(),
	}
}

func sendOrder(ctx context.Context, w *kafka.Writer, o model.Order, source string) (int, error) {
	val, merr := json.Marshal(o)
	if merr != nil {
		return 0, fmt.Errorf("marshal order: %w", merr)
	}
	err := w.WriteMessages(ctx, kafka.Message{
		Key:   []byte(o.OrderUID),
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
	log.Printf("produced key=%s src=%s", o.OrderUID, source)
	return 1, nil
}

func produceFile(ctx context.Context, w *kafka.Writer, path string) (int, error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, fmt.Errorf("open: %w", err)
	}
	defer func() {
		if err := f.Close(); err != nil {
			log.Printf("close %s: %v", path, err)
		}
	}()
	b, err := io.ReadAll(f)
	if err != nil {
		return 0, fmt.Errorf("read: %w", err)
	}
	var one map[string]any
	if err := json.Unmarshal(b, &one); err == nil && len(one) > 0 {
		return sendOne(ctx, w, one, filepath.Base(path))
	}
	var many []map[string]any
	if err := json.Unmarshal(b, &many); err == nil && len(many) > 0 {
		sum := 0
		for _, obj := range many {
			n, err := sendOne(ctx, w, obj, filepath.Base(path))
			if err != nil {
				log.Printf("produce: %v", err)
				continue
			}
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
	val, merr := json.Marshal(obj)
	if merr != nil {
		return 0, fmt.Errorf("marshal order: %w", merr)
	}
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
func mustInt(def string, s string) int {
	if s == "" {
		s = def
	}
	n, err := strconv.Atoi(s)
	if err != nil {
		return 0
	}
	return n
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
