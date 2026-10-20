// Package testdb gives each test its own throwaway Postgres database with the
// migrations applied. It needs the compose database to be running.
package testdb

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
)

const defaultURL = "postgres://viewcounter:viewcounter@localhost:5433/viewcounter"

type DB struct {
	URL  string
	Pool *pgxpool.Pool
}

func New(t *testing.T) *DB {
	t.Helper()
	ctx := context.Background()

	adminURL := os.Getenv("TEST_DATABASE_URL")
	if adminURL == "" {
		adminURL = defaultURL
	}
	admin, err := pgxpool.New(ctx, adminURL)
	if err == nil {
		err = admin.Ping(ctx)
	}
	if err != nil {
		t.Fatalf("cannot reach Postgres at %s (run `mise run db`): %v", adminURL, err)
	}

	var b [6]byte
	rand.Read(b[:])
	name := "vc_test_" + hex.EncodeToString(b[:])
	if _, err := admin.Exec(ctx, "CREATE DATABASE "+name); err != nil {
		admin.Close()
		t.Fatalf("create database: %v", err)
	}

	u, _ := url.Parse(adminURL)
	u.Path = "/" + name
	pool, err := pgxpool.New(ctx, u.String())
	if err != nil {
		t.Fatalf("connect: %v", err)
	}

	t.Cleanup(func() {
		pool.Close()
		admin.Exec(ctx, "DROP DATABASE IF EXISTS "+name+" WITH (FORCE)")
		admin.Close()
	})

	for _, f := range migrations(t) {
		sql, err := os.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := pool.Exec(ctx, string(sql)); err != nil {
			t.Fatalf("apply %s: %v", f, err)
		}
	}
	return &DB{URL: u.String(), Pool: pool}
}

func migrations(t *testing.T) []string {
	_, here, _, _ := runtime.Caller(0)
	files, err := filepath.Glob(filepath.Join(filepath.Dir(here), "..", "..", "migrations", "*.sql"))
	if err != nil || len(files) == 0 {
		t.Fatalf("no migrations found: %v", err)
	}
	sort.Strings(files)
	return files
}
