package main

import (
	"context"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/oz/view-counter/internal/api"
	"github.com/oz/view-counter/internal/ingest"
	"github.com/oz/view-counter/internal/store"
)

func main() {
	dbURL := env("DATABASE_URL", "postgres://viewcounter:viewcounter@localhost:5433/viewcounter")
	addr := env("ADDR", ":8080")
	secret := os.Getenv("HMAC_SECRET")
	if secret == "" {
		log.Fatal("HMAC_SECRET is required")
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	st, err := store.New(ctx, dbURL)
	if err != nil {
		log.Fatalf("connect to postgres: %v", err)
	}
	defer st.Close()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() { defer wg.Done(); pruneLoop(ctx, st) }()

	// The /v2 endpoints exist only when NATS_URL is set.
	var queue api.Queue
	if natsURL := os.Getenv("NATS_URL"); natsURL != "" {
		q, err := ingest.Connect(ctx, natsURL, ingest.Config{})
		if err != nil {
			log.Fatalf("connect to nats: %v", err)
		}
		defer q.Close()
		queue = q
		wg.Add(1)
		go func() { defer wg.Done(); q.Run(ctx, st) }()
	}

	srv := &http.Server{
		Addr:              addr,
		Handler:           api.New(st, []byte(secret), queue),
		ReadHeaderTimeout: 5 * time.Second,
	}
	shutdownDone := make(chan struct{})
	go func() {
		defer close(shutdownDone)
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		srv.Shutdown(shutdownCtx)
	}()

	log.Printf("listening on %s", addr)
	if err := srv.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
		log.Fatal(err)
	}
	// Handlers are done; let the background loops finish their last batch
	// before the deferred pool and NATS closes run.
	<-shutdownDone
	wg.Wait()
}

func pruneLoop(ctx context.Context, st *store.Store) {
	t := time.NewTicker(time.Hour)
	defer t.Stop()
	for {
		if err := st.PruneSeen(ctx); err != nil && ctx.Err() == nil {
			log.Printf("prune seen: %v", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

func env(name, def string) string {
	if v := os.Getenv(name); v != "" {
		return v
	}
	return def
}
