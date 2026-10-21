package ingest_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/server"

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
