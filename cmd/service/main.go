package main

import (
	"context"
	"database/sql"
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/segmentio/kafka-go"

	"demo/orders/internal/model"
	"demo/orders/internal/store"
	"demo/orders/internal/validate"

	"github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/database/postgres"
	"github.com/golang-migrate/migrate/v4/source/iofs"
)

//go:embed web/*
var webFS embed.FS

//go:embed migrations/*.sql
var migFS embed.FS

type cacheEntry struct {
	val     model.Order
	addedAt time.Time
}
type Cache struct {
	mu         sync.RWMutex
	data       map[string]cacheEntry
	ttl        time.Duration
	maxEntries int
}

func NewCache(ttl time.Duration, maxEntries int) *Cache {
	return &Cache{data: make(map[string]cacheEntry), ttl: ttl, maxEntries: maxEntries}
}
func (c *Cache) Get(id string) (model.Order, bool) {
	c.mu.RLock()
	e, ok := c.data[id]
	c.mu.RUnlock()
	if !ok {
		return model.Order{}, false
	}
	if c.ttl > 0 && time.Since(e.addedAt) > c.ttl {
		c.mu.Lock()
		if e2, ok2 := c.data[id]; ok2 && e2.addedAt == e.addedAt {
			delete(c.data, id)
		}
		c.mu.Unlock()
		return model.Order{}, false
	}
	return e.val, true
}
func (c *Cache) Set(id string, v model.Order) {
	c.mu.Lock()
	c.data[id] = cacheEntry{val: v, addedAt: time.Now()}
	if c.maxEntries > 0 && len(c.data) > c.maxEntries {
		if c.ttl > 0 {
			for k, e := range c.data {
				if time.Since(e.addedAt) > c.ttl {
					delete(c.data, k)
				}
			}
		}
		if len(c.data) > c.maxEntries {
			excess := len(c.data) - c.maxEntries
			type kv struct {
				k string
				t time.Time
			}
			cand := make([]kv, 0, len(c.data))
			for k, e := range c.data {
				cand = append(cand, kv{k, e.addedAt})
			}
			sort.Slice(cand, func(i, j int) bool { return cand[i].t.Before(cand[j].t) })
			for i := 0; i < excess && i < len(cand); i++ {
				delete(c.data, cand[i].k)
			}
		}
	}
	c.mu.Unlock()
}
func (c *Cache) StartJanitor(stop <-chan struct{}, every time.Duration) {
	if c.ttl <= 0 || every <= 0 {
		return
	}
	t := time.NewTicker(every)
	go func() {
		defer t.Stop()
		for {
			select {
			case <-t.C:
				cutoff := time.Now().Add(-c.ttl)
				c.mu.Lock()
				for k, e := range c.data {
					if e.addedAt.Before(cutoff) {
						delete(c.data, k)
					}
				}
				c.mu.Unlock()
			case <-stop:
				return
			}
		}
	}()
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
func mustDur(def string, s string) time.Duration {
	if s == "" {
		s = def
	}
	d, err := time.ParseDuration(s)
	if err != nil {
		return 0
	}
	return d
}
func env(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func runMigrations(dsn string) error {
	mode := strings.ToLower(env("DB_MIGRATE", "up")) // up|down|force
	steps := mustInt("0", os.Getenv("DB_MIGRATE_STEPS"))
	ver := mustInt("0", os.Getenv("DB_MIGRATE_VERSION"))

	src, err := iofs.New(migFS, "migrations")
	if err != nil {
		return fmt.Errorf("migrations source: %w", err)
	}

	dbsql, err := sql.Open("pgx", dsn)
	if err != nil {
		return fmt.Errorf("open sql db: %w", err)
	}
	defer dbsql.Close()

	driver, err := postgres.WithInstance(dbsql, &postgres.Config{})
	if err != nil {
		return fmt.Errorf("postgres driver: %w", err)
	}

	m, err := migrate.NewWithInstance("iofs", src, "postgres", driver)
	if err != nil {
		return fmt.Errorf("migrate instance: %w", err)
	}

	switch mode {
	case "up":
		if err := m.Up(); err != nil && !errors.Is(err, migrate.ErrNoChange) {
			return err
		}
		return nil
	case "down":
		if steps <= 0 {
			steps = 1
		}
		return m.Steps(-steps)
	case "force":
		return m.Force(ver)
	default:
		return fmt.Errorf("unknown DB_MIGRATE=%q (use up|down|force)", mode)
	}
}

func main() {
	httpAddr := env("HTTP_ADDR", ":8082")
	dsn := env("DB_DSN", "postgres://app:app@localhost:5433/orders_db?sslmode=disable")
	kbrokers := strings.Split(env("KAFKA_BROKERS", "localhost:9094"), ",")
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

	// миграции до пула
	if err := runMigrations(dsn); err != nil {
		log.Fatalf("migrations: %v", err)
	}

	// DB pool
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		log.Fatalf("db connect: %v", err)
	}
	defer pool.Close()

	repo := store.New(pool)
	ttl := mustDur("30m", os.Getenv("CACHE_TTL"))
	maxN := mustInt("100000", os.Getenv("CACHE_MAX"))
	cache := NewCache(ttl, maxN)

	// janitor
	stopJan := make(chan struct{})
	cache.StartJanitor(stopJan, time.Minute)
	defer close(stopJan)

	// тёплый старт кэша
	if env("CACHE_WARM", "1") == "1" {
		if orders, err := repo.LoadAllOrders(ctx); err == nil {
			for _, o := range orders {
				cache.Set(o.OrderUID, o)
			}
			log.Printf("cache warm: %d orders", len(orders))
		} else {
			log.Printf("cache warm error: %v", err)
		}
	}

	// Kafka consumer
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     kbrokers,
		Topic:       ktopic,
		GroupID:     kgroup,
		MinBytes:    1e3,
		MaxBytes:    10e6,
		StartOffset: kafka.FirstOffset,
	})
	defer reader.Close()

	startConsumer(ctx, reader, repo, cache)

	// HTTP

	// после startConsumer(...)
	mux := makeHTTPMux(repo, cache, webFS)

	srv := &http.Server{Addr: httpAddr, Handler: mux}
	go func() {
		log.Printf("http: listening on %s", httpAddr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("http server: %v", err)
		}
	}()

	<-ctx.Done()
	shCtx, cancel2 := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel2()
	if err := srv.Shutdown(shCtx); err != nil {
		log.Printf("http shutdown: %v", err)
	}
	log.Println("bye")

}
func startConsumer(ctx context.Context, reader *kafka.Reader, repo *store.Repo, cache *Cache) {
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
				if err := reader.CommitMessages(context.Background(), m); err != nil {
					log.Printf("commit failed: %v", err)
				}
				continue
			}
			if err := validate.ValidateOrder(ord); err != nil {
				log.Printf("invalid order at offset %d: %v", m.Offset, err)
				if err := reader.CommitMessages(context.Background(), m); err != nil {
					log.Printf("commit failed: %v", err)
				}
				continue
			}
			if err := repo.UpsertOrder(ctx, ord); err != nil {
				log.Printf("db upsert failed (offset=%d): %v", m.Offset, err)
				continue // не коммитим — перечитаем позже
			}
			cache.Set(ord.OrderUID, ord)
			if err := reader.CommitMessages(ctx, m); err != nil {
				log.Printf("commit failed: %v", err)
			}
		}
	}()
}

func makeHTTPMux(repo *store.Repo, cache *Cache, WebFS embed.FS) *http.ServeMux {
	mux := http.NewServeMux()

	mux.HandleFunc("GET /order/", func(w http.ResponseWriter, r *http.Request) {
		id := strings.TrimPrefix(r.URL.Path, "/order/")
		if id == "" {
			http.Error(w, "missing order id", http.StatusBadRequest)
			return
		}
		if o, ok := cache.Get(id); ok {
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("X-Cache", "HIT")
			if err := json.NewEncoder(w).Encode(o); err != nil {
				http.Error(w, "internal error", http.StatusInternalServerError)
			}
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
		if err := json.NewEncoder(w).Encode(o); err != nil {
			http.Error(w, "internal error", http.StatusInternalServerError)
		}
	})

	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})

	// статика
	sub, err := fs.Sub(webFS, "web")
	if err != nil {
		log.Fatalf("embed FS: %v", err)
	}
	mux.Handle("/", http.FileServer(http.FS(sub)))
	return mux
}
