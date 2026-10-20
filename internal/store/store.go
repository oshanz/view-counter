// Package store is the Postgres access layer.
package store

import (
	"context"
	"time"

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

func New(ctx context.Context, url string) (*Store, error) {
	pool, err := pgxpool.New(ctx, url)
	if err != nil {
		return nil, err
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

func (s *Store) Total(ctx context.Context, key string) (int64, error) {
	var n int64
	err := s.pool.QueryRow(ctx,
		`SELECT COALESCE(SUM(views), 0) FROM page_views WHERE key = $1`, key).Scan(&n)
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

// PruneSeen deletes dedupe rows that have left the 7 day window.
func (s *Store) PruneSeen(ctx context.Context) error {
	_, err := s.pool.Exec(ctx, `DELETE FROM seen WHERE seen_at < now() - interval '7 days'`)
	return err
}
