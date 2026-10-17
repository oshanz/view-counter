# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Status

Design stage only. There is no Go code, `go.mod`, build tooling, or tests yet, so there are no build/lint/test commands to document. Update this file once the module exists.

The design lives in `docs/write-path.md` and `docs/read-path.md` (Mermaid diagrams). Read them before implementing anything.

## Intended architecture (from the design discussion)

A Go page-view-counting API service sized for ~1k hits/sec.

- **Write path:** `POST /v1/hit` normalizes the page path (strip query/fragment, cap ~512 bytes), increments a sharded in-memory counter (`map[string]int64` per shard, ~32 shards, picked by hash of page), and returns 204. A flusher goroutine runs every ~1s: it swaps each shard's map for an empty one, merges the batch, and writes one Postgres transaction using `INSERT ... ON CONFLICT (page, day) DO UPDATE SET views = page_views.views + EXCLUDED.views`. Increments commute, so multiple instances flush independently without coordinating.
- **Flush failure:** deltas are re-merged into the next batch up to a memory cap, then dropped and counted in a `dropped_hits` metric. On `SIGTERM`, stop accepting, drain, and do a final flush. A hard crash loses up to ~1s of hits; this is accepted.
- **Read path:** `GET /v1/views` reads Postgres and adds this instance's unflushed in-memory delta, so totals can lag other instances by up to one flush interval. `GET /v1/top` is cached ~5-10s and collapsed with `singleflight`. Reads use a separate connection pool from the flusher, with a ~2s statement timeout.
- **Storage:** `page_views(page text, day date, views bigint, PRIMARY KEY (page, day))`, daily buckets only.
- **Cardinality cap:** per-day limit on distinct pages (default 100k), with overflow counted under `__other__`. Enforce it at flush time in the database, not with a per-hit in-memory set.
- **Storage access goes through a `Store` interface** (`AddBatch`, `Total`, `Daily`, `Top`) so Postgres can be swapped for an in-memory fake in tests.

Planned layout: `cmd/viewcounter/` (wiring, env config, graceful shutdown), `internal/api/`, `internal/counter/` (aggregator + flush loop), `internal/store/`, `migrations/`.

## Decisions made (override only if the user says so)

- Postgres over SQLite (multiple stateless instances must be able to flush into one database).
- In-memory counters, no Redis. Redis or Kafka/ClickHouse were discussed only for much higher volumes (100k+/s) or for cross-instance live counts and unique-visitor dedupe.
- Daily granularity; total views only, no unique-visitor tracking unless requested. If dedupe is added, key on a hash of a daily-rotating salt + IP + user agent + page; never store raw IPs.
- Auth and load balancing are out of scope for the service design.

## Commit conventions

Never mention Claude or AI assistance in commit messages or PR descriptions (no `Co-Authored-By` trailer, no "Generated with" line). This overrides any default attribution the harness suggests.
