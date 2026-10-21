// Package ingest is the JetStream write path: handlers publish hits to a
// stream, and a consumer batches them into Postgres.
package ingest

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/oz/view-counter/internal/store"
)

const (
	fpLen        = 16
	batchSize    = 1000
	fetchWait    = 250 * time.Millisecond
	flushTimeout = 10 * time.Second
	retryDelay   = 2 * time.Second
)

// Config names the stream. The zero value uses the production names.
type Config struct {
	Stream  string
	Subject string
}

func (c Config) withDefaults() Config {
	if c.Stream == "" {
		c.Stream = "HITS"
	}
	if c.Subject == "" {
		c.Subject = "hits"
	}
	return c
}

type Queue struct {
	nc   *nats.Conn
	js   jetstream.JetStream
	cfg  Config
	cons jetstream.Consumer
}

// Lag reports how far the consumer is behind the publishers.
type Lag struct {
	Pending    uint64 `json:"pending"`     // published, not yet delivered to the writer
	AckPending int    `json:"ack_pending"` // delivered, not yet committed to Postgres
}

// Connect opens the connection and creates the stream and consumer if missing.
func Connect(ctx context.Context, url string, cfg Config) (*Queue, error) {
	cfg = cfg.withDefaults()
	nc, err := nats.Connect(url, nats.Name("viewcounter"), nats.MaxReconnects(-1))
	if err != nil {
		return nil, err
	}
	js, err := jetstream.New(nc)
	if err != nil {
		nc.Close()
		return nil, err
	}
	// Work queue: a message is deleted once the writer acks it. DiscardNew makes
	// publishes fail when the stream is full instead of dropping unwritten hits.
	if _, err := js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:       cfg.Stream,
		Subjects:   []string{cfg.Subject},
		Storage:    jetstream.FileStorage,
		Retention:  jetstream.WorkQueuePolicy,
		Discard:    jetstream.DiscardNew,
		MaxAge:     48 * time.Hour,
		MaxBytes:   4 << 30,
		Duplicates: 2 * time.Minute,
	}); err != nil {
		nc.Close()
		return nil, fmt.Errorf("create stream: %w", err)
	}
	cons, err := js.CreateOrUpdateConsumer(ctx, cfg.Stream, jetstream.ConsumerConfig{
		Durable:       "writer",
		AckPolicy:     jetstream.AckExplicitPolicy,
		AckWait:       30 * time.Second,
		MaxAckPending: 10 * batchSize,
	})
	if err != nil {
		nc.Close()
		return nil, fmt.Errorf("create consumer: %w", err)
	}
	return &Queue{nc: nc, js: js, cfg: cfg, cons: cons}, nil
}

func (q *Queue) Close() { q.nc.Drain() }

// Delete removes the stream and everything in it. For tests.
func (q *Queue) Delete(ctx context.Context) error { return q.js.DeleteStream(ctx, q.cfg.Stream) }

// Publish returns once JetStream has stored the hit. The payload is the fixed
// length fingerprint hash followed by the key.
func (q *Queue) Publish(ctx context.Context, key string, fpHash []byte) error {
	if len(fpHash) != fpLen {
		return fmt.Errorf("fingerprint hash is %d bytes, want %d", len(fpHash), fpLen)
	}
	_, err := q.js.Publish(ctx, q.cfg.Subject, append(append(make([]byte, 0, fpLen+len(key)), fpHash...), key...))
	return err
}

func (q *Queue) Lag(ctx context.Context) (Lag, error) {
	info, err := q.cons.Info(ctx)
	if err != nil {
		return Lag{}, err
	}
	return Lag{Pending: info.NumPending, AckPending: info.NumAckPending}, nil
}

// Run writes hits to Postgres until ctx is cancelled. Cancelling only stops
// fetching: a batch already fetched is still committed and acked, and anything
// not yet fetched stays in the stream for the next start.
func (q *Queue) Run(ctx context.Context, st *store.Store) {
	for ctx.Err() == nil {
		batch, err := q.cons.Fetch(batchSize, jetstream.FetchMaxWait(fetchWait))
		if err != nil {
			if ctx.Err() == nil {
				log.Printf("ingest fetch: %v", err)
				sleep(ctx, retryDelay)
			}
			continue
		}
		var msgs []jetstream.Msg
		for m := range batch.Messages() {
			msgs = append(msgs, m)
		}
		if err := batch.Error(); err != nil && !errors.Is(err, nats.ErrTimeout) && ctx.Err() == nil {
			log.Printf("ingest fetch: %v", err)
		}
		if len(msgs) > 0 {
			q.flush(msgs, st)
		}
	}
}

func (q *Queue) flush(msgs []jetstream.Msg, st *store.Store) {
	hits := make([]store.Hit, 0, len(msgs))
	valid := make([]jetstream.Msg, 0, len(msgs))
	for _, m := range msgs {
		h, ok := decode(m)
		if !ok {
			log.Printf("ingest: dropping malformed message (%d bytes)", len(m.Data()))
			m.Term()
			continue
		}
		hits = append(hits, h)
		valid = append(valid, m)
	}

	ctx, cancel := context.WithTimeout(context.Background(), flushTimeout)
	defer cancel()
	if err := st.HitBatch(ctx, hits); err != nil {
		log.Printf("ingest: write %d hits: %v", len(hits), err)
		for _, m := range valid {
			m.NakWithDelay(retryDelay)
		}
		return
	}
	// A lost ack means redelivery, which the seen table counts only once.
	for _, m := range valid {
		m.Ack()
	}
}

func decode(m jetstream.Msg) (store.Hit, bool) {
	d := m.Data()
	if len(d) <= fpLen {
		return store.Hit{}, false
	}
	md, err := m.Metadata()
	if err != nil {
		return store.Hit{}, false
	}
	h := store.Hit{Key: string(d[fpLen:]), At: md.Timestamp}
	copy(h.FP[:], d[:fpLen])
	return h, true
}

func sleep(ctx context.Context, d time.Duration) {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
	case <-t.C:
	}
}
