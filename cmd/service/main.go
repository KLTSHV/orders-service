package main

import (
	"context"
	"embed"
	"encoding/json"
	"errors"
	"io/fs"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/segmentio/kafka-go"

	"demo/orders/internal/model"
	"demo/orders/internal/store"
)

var webFS embed.FS

type Cache struct {
	mu   sync.RWMutex
	data map[string]model.Order
}

func NewCache() *Cache { return &Cache{data: make(map[string]model.Order)} }

func (c *Cache) Get(id string) (model.Order, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	v, ok := c.data[id]
	return v, ok
}

func (c *Cache) Set(id string, v model.Order) {
	c.mu.Lock()
	c.data[id] = v
	c.mu.Unlock()
}

func main() {
	httpAddr := env("HTTP_ADDR", ":8082")
	dsn := env("DB_DSN", "postgres://app:app@localhost:5433/orders_db?sslmode=disable")
	kbrokers := strings.Split(env("KAFKA_BROKERS", "localhost:9094"), ",")
	log.Printf("Kafka brokers = %v", kbrokers)
	ktopic := env("KAFKA_TOPIC", "orders")
	kgroup := env("KAFKA_GROUP", "orders-consumers")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		sig := make(chan os.Signal, 1)
		signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
		<-sig
		cancel()
	}()

	// DB pool
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		log.Fatalf("db connect: %v", err)
	}
	defer pool.Close()
	if err := runSchema(ctx, pool); err != nil {
		log.Fatalf("schema: %v", err)
	}

	repo := store.New(pool)
	cache := NewCache()

	if env("CACHE_WARM", "1") == "1" {
		if orders, err := repo.LoadAllOrders(ctx); err == nil {
			for _, o := range orders {
				cache.Set(o.OrderUID, o)
			}
			log.Printf("cache warm: %d orders", len(orders))
		} else {
			log.Printf("cache warm error: %v", err)
		}
	} else {
		log.Printf("cache warm: skipped (CACHE_WARM=0)")
	}

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     kbrokers,
		Topic:       ktopic,
		GroupID:     kgroup,
		MinBytes:    1e3,
		MaxBytes:    10e6,
		StartOffset: kafka.FirstOffset,
	})
	defer reader.Close()

	go func() {
		for {
			m, err := reader.FetchMessage(ctx)
			if err != nil {
				if errors.Is(err, context.Canceled) {
					return
				}
				log.Printf("kafka fetch: %v", err)
				time.Sleep(500 * time.Millisecond)
				continue
			}

			var ord model.Order
			if err := json.Unmarshal(m.Value, &ord); err != nil || ord.OrderUID == "" {
				log.Printf("invalid message at offset %d: %v", m.Offset, err)
				_ = reader.CommitMessages(context.Background(), m)
				continue
			}

			if err := repo.UpsertOrder(ctx, ord); err != nil {
				log.Printf("db upsert failed (offset=%d): %v", m.Offset, err)
				// не коммитим, чтобы можно было перечитать позже
				continue
			}

			cache.Set(ord.OrderUID, ord)
			if err := reader.CommitMessages(ctx, m); err != nil {
				log.Printf("commit failed: %v", err)
			}
		}
	}()

	// HTTP API + UI
	mux := http.NewServeMux()

	// API
	mux.HandleFunc("GET /order/", func(w http.ResponseWriter, r *http.Request) {
		id := strings.TrimPrefix(r.URL.Path, "/order/")
		if id == "" {
			http.Error(w, "missing order id", http.StatusBadRequest)
			return
		}

		if o, ok := cache.Get(id); ok {
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("X-Cache", "HIT")
			_ = json.NewEncoder(w).Encode(o)
			return
		}

		o, ok, err := repo.GetOrder(r.Context(), id)
		if err != nil {
			http.Error(w, "internal error", http.StatusInternalServerError)
			return
		}
		if !ok {
			http.NotFound(w, r)
			return
		}

		cache.Set(id, o)
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("X-Cache", "MISS")
		_ = json.NewEncoder(w).Encode(o)
	})

	// Health
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})

	// UI: отдаём содержимое папки web/ с корня "/"
	sub, err := fs.Sub(webFS, "web")
	if err != nil {
		log.Fatalf("embed FS: %v", err)
	}
	mux.Handle("/", http.FileServer(http.FS(sub)))

	// HTTP server
	srv := &http.Server{Addr: httpAddr, Handler: mux}

	go func() {
		log.Printf("http: listening on %s", httpAddr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("http server: %v", err)
		}
	}()

	<-ctx.Done()
	shCtx, cancel2 := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel2()
	_ = srv.Shutdown(shCtx)
	log.Println("bye")
}

func runSchema(ctx context.Context, pool *pgxpool.Pool) error {
	sql := `
CREATE TABLE IF NOT EXISTS orders (
  order_uid TEXT PRIMARY KEY,
  track_number TEXT NOT NULL,
  entry TEXT NOT NULL,
  locale TEXT,
  internal_signature TEXT,
  customer_id TEXT,
  delivery_service TEXT,
  shardkey TEXT,
  sm_id INTEGER,
  date_created TIMESTAMPTZ NOT NULL,
  oof_shard TEXT
);
CREATE TABLE IF NOT EXISTS deliveries (
  order_uid TEXT PRIMARY KEY REFERENCES orders(order_uid) ON DELETE CASCADE,
  name TEXT, phone TEXT, zip TEXT, city TEXT, address TEXT, region TEXT, email TEXT
);
CREATE TABLE IF NOT EXISTS payments (
  order_uid TEXT PRIMARY KEY REFERENCES orders(order_uid) ON DELETE CASCADE,
  transaction TEXT NOT NULL, request_id TEXT, currency TEXT, provider TEXT, amount INTEGER,
  payment_dt TIMESTAMPTZ, bank TEXT, delivery_cost INTEGER, goods_total INTEGER, custom_fee INTEGER
);
CREATE TABLE IF NOT EXISTS items (
  order_uid TEXT REFERENCES orders(order_uid) ON DELETE CASCADE,
  chrt_id BIGINT, track_number TEXT, price INTEGER, rid TEXT, name TEXT, sale INTEGER, size TEXT,
  total_price INTEGER, nm_id BIGINT, brand TEXT, status INTEGER,
  PRIMARY KEY(order_uid, chrt_id)
);
`
	_, err := pool.Exec(ctx, sql)
	return err
}

func env(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
