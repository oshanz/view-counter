# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Status

Implemented. Tool versions are pinned in `mise.toml` (Go 1.25.0); Postgres runs via `docker-compose.yml` on host port 5433 and applies `migrations/` on first start.

- `mise run db` starts Postgres, `mise run run` starts the service on `:8080`, `mise run test` runs tests (none written yet).
- Env: `HMAC_SECRET` (required, dev default in `mise.toml`), `DATABASE_URL`, `ADDR`.
- To reset the schema: `docker compose down -v`.

The design lives in `docs/write-path.md` and `docs/read-path.md` (Mermaid diagrams).

## Intended architecture

A Go page-view-counting API service sized for ~1 write/sec and ~1 read/sec. Keep it minimal: one service, one Postgres, nothing else.

- **Write path:** `POST /v1/hit` takes `key` + `fingerprint`, HMACs the fingerprint, and in one Postgres transaction dedupes against `seen(key, fp_hash, seen_at)` (a fingerprint counts once per key per 7 days) and upserts `page_views(key, day, views)` with `views = views + 1`. Returns 204, or 503 on a Postgres error. A goroutine deletes `seen` rows older than 7 days hourly.
- **Read path:** `GET /v1/views` (total, or daily series with `from`/`to`) and `GET /v1/top` query Postgres directly with a ~2s statement timeout. No cache.
- **No** Redis, queues, in-memory counters, batching, flush loops, sharding or cardinality caps. If volume grows, add batching in front of the same schema.

Layout: `cmd/viewcounter/` (wiring, env config, graceful shutdown), `internal/api/`, `internal/store/`, `migrations/`.

## Decisions made (override only if the user says so)

- Postgres only; keys match `[A-Za-z0-9_-]{1,64}`.
- Daily granularity, total views only.
- Raw fingerprints are never stored or logged; only a truncated HMAC.
- Auth and load balancing are out of scope.

## Commit conventions

Never mention Claude or AI assistance in commit messages or PR descriptions (no `Co-Authored-By` trailer, no "Generated with" line). This overrides any default attribution the harness suggests.
