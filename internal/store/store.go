// Package store is the Postgres access layer.
package store

import (
	"context"
	"sort"
	"time"

	"github.com/exaring/otelpgx"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Store struct {
	pool *pgxpool.Pool
}

type DayViews struct {
	Day   string `json:"day"`
	Views int64  `json:"views"`
}

type KeyViews struct {
	Key   string `json:"key"`
	Views int64  `json:"views"`
}

// New connects to Postgres. With traced set, queries emit OpenTelemetry spans
// and the pool reports its stats as metrics.
func New(ctx context.Context, url string, traced ...bool) (*Store, error) {
	cfg, err := pgxpool.ParseConfig(url)
	if err != nil {
		return nil, err
	}
	trace := len(traced) > 0 && traced[0]
	if trace {
		cfg.ConnConfig.Tracer = otelpgx.NewTracer()
	}
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, err
	}
	if trace {
		if err := otelpgx.RecordStats(pool); err != nil {
			pool.Close()
			return nil, err
		}
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, err
	}
	return &Store{pool: pool}, nil
}

func (s *Store) Close() { s.pool.Close() }

// Hit counts one view unless this fingerprint already counted for the key in
// the last 7 days. It reports whether the view was counted.
func (s *Store) Hit(ctx context.Context, key string, fpHash []byte) (bool, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return false, err
	}
	defer tx.Rollback(ctx)

	var one int
	err = tx.QueryRow(ctx, `
		INSERT INTO seen (key, fp_hash, seen_at) VALUES ($1, $2, now())
		ON CONFLICT (key, fp_hash) DO UPDATE SET seen_at = now()
			WHERE seen.seen_at < now() - interval '7 days'
		RETURNING 1`, key, fpHash).Scan(&one)
	if err == pgx.ErrNoRows {
		return false, nil
	}
	if err != nil {
		return false, err
	}

	_, err = tx.Exec(ctx, `
		INSERT INTO page_views (key, day, views) VALUES ($1, current_date, 1)
		ON CONFLICT (key, day) DO UPDATE SET views = page_views.views + 1`, key)
	if err != nil {
		return false, err
	}
	return true, tx.Commit(ctx)
}

// Hit is one view waiting to be counted. At is when it happened, which decides
// its day and where its dedupe window starts.
type Hit struct {
	Key string
	FP  [16]byte
	At  time.Time
}

// HitBatch counts many views in one transaction with the same rules as Hit.
// It is idempotent: replaying a batch counts nothing the first run counted.
func (s *Store) HitBatch(ctx context.Context, hits []Hit) error {
	type id struct {
		key string
		fp  [16]byte
	}
	// ON CONFLICT cannot touch one row twice in a statement, so keep the first
	// occurrence of each pair.
	first := make(map[id]time.Time, len(hits))
	for _, h := range hits {
		k := id{h.Key, h.FP}
		if at, ok := first[k]; !ok || h.At.Before(at) {
			first[k] = h.At
		}
	}
	if len(first) == 0 {
		return nil
	}
	pairs := make([]id, 0, len(first))
	for k := range first {
		pairs = append(pairs, k)
	}
	// A stable order keeps concurrent writers from deadlocking on row locks.
	sort.Slice(pairs, func(i, j int) bool {
		if pairs[i].key != pairs[j].key {
			return pairs[i].key < pairs[j].key
		}
		return string(pairs[i].fp[:]) < string(pairs[j].fp[:])
	})
	keys := make([]string, len(pairs))
	fps := make([][]byte, len(pairs))
	ats := make([]time.Time, len(pairs))
	for i, p := range pairs {
		keys[i], fps[i], ats[i] = p.key, p.fp[:], first[p]
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	rows, err := tx.Query(ctx, `
		INSERT INTO seen (key, fp_hash, seen_at)
		SELECT k, f, a FROM unnest($1::text[], $2::bytea[], $3::timestamptz[]) AS t(k, f, a)
		ON CONFLICT (key, fp_hash) DO UPDATE SET seen_at = EXCLUDED.seen_at
			WHERE seen.seen_at < EXCLUDED.seen_at - interval '7 days'
		RETURNING key, (seen_at AT TIME ZONE 'UTC')::date`, keys, fps, ats)
	if err != nil {
		return err
	}
	type kd struct {
		key string
		day time.Time
	}
	counts := map[kd]int64{}
	for rows.Next() {
		var c kd
		if err := rows.Scan(&c.key, &c.day); err != nil {
			rows.Close()
			return err
		}
		counts[c]++
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}
	if len(counts) == 0 {
		return tx.Commit(ctx)
	}

	cs := make([]kd, 0, len(counts))
	for c := range counts {
		cs = append(cs, c)
	}
	sort.Slice(cs, func(i, j int) bool {
		if cs[i].key != cs[j].key {
			return cs[i].key < cs[j].key
		}
		return cs[i].day.Before(cs[j].day)
	})
	ck := make([]string, len(cs))
	cd := make([]time.Time, len(cs))
	cn := make([]int64, len(cs))
	for i, c := range cs {
		ck[i], cd[i], cn[i] = c.key, c.day, counts[c]
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO page_views (key, day, views)
		SELECT k, d, n FROM unnest($1::text[], $2::date[], $3::bigint[]) AS t(k, d, n)
		ON CONFLICT (key, day) DO UPDATE SET views = page_views.views + EXCLUDED.views`,
		ck, cd, cn); err != nil {
		return err
	}

	// cs is sorted by key, so the per-key sums are already in lock order.
	tk := make([]string, 0, len(cs))
	tn := make([]int64, 0, len(cs))
	for i := range cs {
		if len(tk) > 0 && tk[len(tk)-1] == ck[i] {
			tn[len(tn)-1] += cn[i]
			continue
		}
		tk = append(tk, ck[i])
		tn = append(tn, cn[i])
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO page_totals (key, views)
		SELECT k, n FROM unnest($1::text[], $2::bigint[]) AS t(k, n)
		ON CONFLICT (key) DO UPDATE SET views = page_totals.views + EXCLUDED.views`,
		tk, tn); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (s *Store) Total(ctx context.Context, key string) (int64, error) {
	var n int64
	err := s.pool.QueryRow(ctx,
		`SELECT COALESCE(SUM(views), 0) FROM page_views WHERE key = $1`, key).Scan(&n)
	return n, err
}

// TotalFromTotals is the /v2 read: a single-row lookup in page_totals, which
// only HitBatch maintains. Hits counted through Hit (/v1) are not in it.
func (s *Store) TotalFromTotals(ctx context.Context, key string) (int64, error) {
	var n int64
	err := s.pool.QueryRow(ctx,
		`SELECT views FROM page_totals WHERE key = $1`, key).Scan(&n)
	if err == pgx.ErrNoRows {
		return 0, nil
	}
	return n, err
}

// Daily returns one row per day in [from, to], with zeros for missing days.
func (s *Store) Daily(ctx context.Context, key string, from, to time.Time) ([]DayViews, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT to_char(d, 'YYYY-MM-DD'), COALESCE(p.views, 0)
		FROM generate_series($2::date, $3::date, interval '1 day') AS d
		LEFT JOIN page_views p ON p.key = $1 AND p.day = d::date
		ORDER BY d`, key, from, to)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []DayViews{}
	for rows.Next() {
		var d DayViews
		if err := rows.Scan(&d.Day, &d.Views); err != nil {
			return nil, err
		}
		out = append(out, d)
	}
	return out, rows.Err()
}

func (s *Store) Top(ctx context.Context, n int) ([]KeyViews, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT key, SUM(views)::bigint AS views FROM page_views
		GROUP BY key ORDER BY views DESC, key LIMIT $1`, n)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []KeyViews{}
	for rows.Next() {
		var k KeyViews
		if err := rows.Scan(&k.Key, &k.Views); err != nil {
			return nil, err
		}
		out = append(out, k)
	}
	return out, rows.Err()
}

// TopFromTotals is the /v2 read: the top keys straight off the page_totals
// index. See TotalFromTotals.
func (s *Store) TopFromTotals(ctx context.Context, n int) ([]KeyViews, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT key, views FROM page_totals
		ORDER BY views DESC, key LIMIT $1`, n)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []KeyViews{}
	for rows.Next() {
		var k KeyViews
		if err := rows.Scan(&k.Key, &k.Views); err != nil {
			return nil, err
		}
		out = append(out, k)
	}
	return out, rows.Err()
}

// PruneSeen deletes dedupe rows that have left the 7 day window.
func (s *Store) PruneSeen(ctx context.Context) error {
	_, err := s.pool.Exec(ctx, `DELETE FROM seen WHERE seen_at < now() - interval '7 days'`)
	return err
}
