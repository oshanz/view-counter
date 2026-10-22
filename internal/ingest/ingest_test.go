package ingest_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/server"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"github.com/oz/view-counter/internal/ingest"
	"github.com/oz/view-counter/internal/store"
	"github.com/oz/view-counter/internal/testdb"
)

// startNATS runs an in-process JetStream server so the tests need no Docker.
func startNATS(t *testing.T) string {
	t.Helper()
	s, err := natsserver.NewServer(&natsserver.Options{
		Port:      -1,
		JetStream: true,
		StoreDir:  t.TempDir(),
		NoLog:     true,
		NoSigs:    true,
	})
	if err != nil {
		t.Fatal(err)
	}
	go s.Start()
	if !s.ReadyForConnections(10 * time.Second) {
		t.Fatal("nats server did not start")
	}
	t.Cleanup(s.Shutdown)
	return s.ClientURL()
}

func setup(t *testing.T) (*ingest.Queue, *store.Store) {
	t.Helper()
	ctx := context.Background()
	db := testdb.New(t)
	st, err := store.New(ctx, db.URL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(st.Close)
	q, err := ingest.Connect(ctx, startNATS(t), ingest.Config{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(q.Close)
	return q, st
}

func fp(s string) []byte {
	b := make([]byte, 16)
	copy(b, s)
	return b
}

func waitTotal(t *testing.T, st *store.Store, key string, want int64) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	var got int64
	for time.Now().Before(deadline) {
		var err error
		if got, err = st.Total(context.Background(), key); err != nil {
			t.Fatal(err)
		}
		if got == want {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("total for %s = %d, want %d", key, got, want)
}

func TestPublishedHitsReachPostgresOnce(t *testing.T) {
	q, st := setup(t)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { q.Run(ctx, st); close(done) }()
	t.Cleanup(func() { cancel(); <-done })

	for _, v := range []string{"alice", "alice", "bob"} {
		if err := q.Publish(ctx, "vid1", fp(v)); err != nil {
			t.Fatal(err)
		}
	}
	waitTotal(t, st, "vid1", 2)
}

func TestHitsPublishedBeforeTheWriterStartsAreNotLost(t *testing.T) {
	q, st := setup(t)
	ctx := context.Background()
	for i := 0; i < 2500; i++ { // more than one batch
		if err := q.Publish(ctx, "vid1", fp(fmt.Sprint("v", i))); err != nil {
			t.Fatal(err)
		}
	}

	runCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() { q.Run(runCtx, st); close(done) }()
	t.Cleanup(func() { cancel(); <-done })

	waitTotal(t, st, "vid1", 2500)
}

func TestLagDrainsToZero(t *testing.T) {
	q, st := setup(t)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { q.Run(ctx, st); close(done) }()
	t.Cleanup(func() { cancel(); <-done })

	for i := 0; i < 10; i++ {
		if err := q.Publish(ctx, "vid1", fp(fmt.Sprint("v", i))); err != nil {
			t.Fatal(err)
		}
	}
	waitTotal(t, st, "vid1", 10)

	deadline := time.Now().Add(3 * time.Second)
	for {
		lag, err := q.Lag(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if lag.Pending == 0 && lag.AckPending == 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("lag = %+v, want zero", lag)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func TestBatchSpanJoinsPublishTrace(t *testing.T) {
	exp := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exp))
	prevTP, prevProp := otel.GetTracerProvider(), otel.GetTextMapPropagator()
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.TraceContext{})
	t.Cleanup(func() { otel.SetTracerProvider(prevTP); otel.SetTextMapPropagator(prevProp) })

	q, st := setup(t)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { q.Run(ctx, st); close(done) }()
	t.Cleanup(func() { cancel(); <-done })

	if err := q.Publish(ctx, "vid1", fp("alice")); err != nil {
		t.Fatal(err)
	}
	waitTotal(t, st, "vid1", 1)

	var pub, batch, hit *tracetest.SpanStub
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && (pub == nil || batch == nil || hit == nil) {
		spans := exp.GetSpans()
		for i := range spans {
			switch spans[i].Name {
			case "nats publish":
				pub = &spans[i]
			case "ingest batch":
				batch = &spans[i]
			case "ingest hit":
				hit = &spans[i]
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	if pub == nil || batch == nil || hit == nil {
		t.Fatalf("want publish, batch and hit spans, got %v", exp.GetSpans())
	}
	if batch.SpanContext.TraceID() != pub.SpanContext.TraceID() || batch.Parent.SpanID() != pub.SpanContext.SpanID() {
		t.Fatalf("batch parent = %s in trace %s, want publish span %s in trace %s",
			batch.Parent.SpanID(), batch.SpanContext.TraceID(), pub.SpanContext.SpanID(), pub.SpanContext.TraceID())
	}
	if hit.Parent.SpanID() != pub.SpanContext.SpanID() ||
		len(hit.Links) != 1 || hit.Links[0].SpanContext.SpanID() != batch.SpanContext.SpanID() {
		t.Fatalf("hit span parent = %s links = %v, want publish parent and a link to the batch", hit.Parent.SpanID(), hit.Links)
	}
}
