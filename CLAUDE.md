# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Status

Implemented. Tool versions are pinned in `mise.toml` (Go 1.25.0); Postgres runs via `docker-compose.yml` on host port 5433 and applies `migrations/` on first start.

- `mise run db` starts Postgres, `mise run run` starts the service on `:8080`, `mise run test` runs tests (none written yet).
- k6 (pinned in `mise.toml`) non-functional tests live in `loadtest/`: `mise run stress` (ramp to 200 writes/s and 50 reads/s, mostly on one hot key). It starts the service itself.
- Env: `HMAC_SECRET` (required, dev default in `mise.toml`), `DATABASE_URL`, `ADDR`, `NATS_URL` (optional, enables `/v2`).
- To reset the schema: `docker compose down -v`.

The design lives in `docs/postgres-only-benchmark.md` (Mermaid diagrams, decisions, stress test results). The optional JetStream `/v2` write path is documented in `docs/jetstream-v2.md`.

## Intended architecture

A Go page-view-counting API service sized for ~1 write/sec and ~1 read/sec. Keep it minimal: one service, one Postgres, nothing else.

- **Write path:** `POST /v1/hit` takes `key` + `fingerprint`, HMACs the fingerprint, and in one Postgres transaction dedupes against `seen(key, fp_hash, seen_at)` (a fingerprint counts once per key per 7 days) and upserts `page_views(key, day, views)` with `views = views + 1`. Returns 204, or 503 on a Postgres error. A goroutine deletes `seen` rows older than 7 days hourly.
- **Read path:** `GET /v1/views` (total, or daily series with `from`/`to`) and `GET /v1/top` query Postgres directly with a ~2s statement timeout. No cache.
- **No** Redis, queues, in-memory counters, batching, flush loops, sharding or cardinality caps. If volume grows, add batching in front of the same schema.

Layout: `cmd/viewcounter/` (wiring, env config, graceful shutdown), `internal/api/`, `internal/store/`, `migrations/`.

## JetStream write path (`/v2`, optional)

Set `NATS_URL` (compose runs `nats` on `:4222`) to enable it. Without it only `/v1` exists.

- `POST /v2/hit` validates and HMACs like `/v1/hit`, publishes to JetStream stream `HITS` (work queue, file storage, `DiscardNew`), and returns 202 once the publish is acked, or 503 if it fails.
- `internal/ingest` runs a pull consumer that fetches up to 1000 hits per 250ms and calls `store.HitBatch` (one dedupe upsert into `seen`, one aggregated upsert each into `page_views` and `page_totals`), then acks. A failed batch is nak'd and retried; redelivery is safe because `seen` counts each (key, fingerprint) once.
- A hit counts on the day JetStream stored it, not the day the writer ran it.
- `GET /v2/views` (totals) and `GET /v2/top` read `page_totals`, which only `/v2` writes; `/v1` never touches it. A `/v2/views` daily series (`from`/`to`) reads `page_views`. `/v1` reads and writes stay on `page_views` only. `GET /v2/ingest` reports consumer lag (`pending`, `ack_pending`).
- Shutdown: HTTP drains first, then the consumer finishes its fetched batch; unfetched hits stay in the stream.
- Tests start an in-process NATS server, so they need no Docker for NATS.

## Decisions made (override only if the user says so)

- Postgres only; keys match `[A-Za-z0-9_-]{1,64}`.
- Daily granularity, total views only.
- Raw fingerprints are never stored or logged; only a truncated HMAC.
- Auth and load balancing are out of scope.

## Commit conventions

Never mention Claude or AI assistance in commit messages or PR descriptions (no `Co-Authored-By` trailer, no "Generated with" line). This overrides any default attribution the harness suggests.
