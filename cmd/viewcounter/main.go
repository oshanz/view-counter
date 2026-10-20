package main

import (
	"context"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/oz/view-counter/internal/api"
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

	go pruneLoop(ctx, st)

	srv := &http.Server{
		Addr:              addr,
		Handler:           api.New(st, []byte(secret)),
		ReadHeaderTimeout: 5 * time.Second,
	}
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		srv.Shutdown(shutdownCtx)
	}()

	log.Printf("listening on %s", addr)
	if err := srv.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
		log.Fatal(err)
	}
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
