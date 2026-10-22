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
	"github.com/oz/view-counter/internal/telemetry"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
)

// ingestWorkers is how many fetch loops drain the /v2 stream.
const ingestWorkers = 1

func main() {
	dbURL := env("DATABASE_URL", "postgres://viewcounter:viewcounter@localhost:5433/viewcounter")
	addr := env("ADDR", ":8080")
	secret := os.Getenv("HMAC_SECRET")
	if secret == "" {
		log.Fatal("HMAC_SECRET is required")
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// Traces and metrics go to an OTLP collector (SigNoz) only when
	// OTEL_EXPORTER_OTLP_ENDPOINT is set.
	traced := telemetry.Enabled()
	if traced {
		shutdown, err := telemetry.Setup(ctx, "viewcounter")
		if err != nil {
			log.Fatalf("telemetry: %v", err)
		}
		defer func() {
			flushCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			shutdown(flushCtx)
		}()
	}

	st, err := store.New(ctx, dbURL, traced)
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
		if err := q.ObserveLag(); err != nil {
			log.Fatalf("register ingest metrics: %v", err)
		}
		// Several fetch loops share the one durable consumer; JetStream
		// splits the messages between them.
		for range ingestWorkers {
			wg.Add(1)
			go func() { defer wg.Done(); q.Run(ctx, st) }()
		}
	}

	var handler http.Handler = api.New(st, []byte(secret), queue)
	if traced {
		handler = otelhttp.NewHandler(handler, "viewcounter")
	}
	srv := &http.Server{
		Addr:              addr,
		Handler:           handler,
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
