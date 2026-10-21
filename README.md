# view-counter

A Go page-view counting API. Counts one view per fingerprint per key per 7 days, stored in Postgres at daily granularity. An optional JetStream write path (`/v2`) batches writes for higher throughput.

## Quick start

Requires [mise](https://mise.jdx.dev/) and Docker.

```sh
mise run db     # Postgres (host port 5433), applies migrations/ on first start
mise run run    # service on :8081 (see mise.toml)
mise run test   # tests
```

To reset the schema: `docker compose down -v`.

## Configuration

| Variable | Notes |
|---|---|
| `HMAC_SECRET` | Required. Dev default in `mise.toml`. |
| `DATABASE_URL` | Postgres connection string. |
| `ADDR` | Listen address. |
| `NATS_URL` | Optional. Enables the `/v2` routes. |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | Optional. Sends traces and metrics to an OTLP collector such as SigNoz. |

## Endpoints

Keys match `[A-Za-z0-9_-]{1,64}`. Raw fingerprints are never stored or logged, only a truncated HMAC.

| Endpoint | Purpose | Details |
|---|---|---|
| `POST /v1/hit` | Record a view (`key`, `fingerprint`). 204 once committed, 503 on a Postgres error. | [postgres-direct-v1.md](docs/postgres-direct-v1.md#write-path) |
| `GET /v1/views` | Total views for a key, or a daily series with `from`/`to` (`YYYY-MM-DD`). | [postgres-direct-v1.md](docs/postgres-direct-v1.md#read-path) |
| `GET /v1/top` | Top keys (`n` limits the count). | [postgres-direct-v1.md](docs/postgres-direct-v1.md#read-path) |
| `POST /v2/hit` | Same body as `/v1/hit`. Publishes to JetStream, returns 202 once acked (not yet counted). | [jetstream-v2.md](docs/jetstream-v2.md#write-path) |
| `GET /v2/views` | Total from `page_totals`, or a daily series from `page_views`. | [jetstream-v2.md](docs/jetstream-v2.md#read-path-and-operations) |
| `GET /v2/top` | Top keys from `page_totals`. | [jetstream-v2.md](docs/jetstream-v2.md#read-path-and-operations) |
| `GET /v2/ingest` | Consumer lag: `pending`, `ack_pending`. | [jetstream-v2.md](docs/jetstream-v2.md#read-path-and-operations) |

The `/v2` routes exist only when `NATS_URL` is set.

## Docs

- [docs/postgres-direct-v1.md](docs/postgres-direct-v1.md): the Postgres-only design, decisions, tables and stress test results.
- [docs/jetstream-v2.md](docs/jetstream-v2.md): the JetStream write path, failure modes, shutdown and read load test.
- [docs/ADR.md](docs/ADR.md): accepted limitations.

## Load tests

k6 tests live in `loadtest/`: `mise run stress` (write and read ramp), `mise run stress-views` (`GET /v2/views`), `mise run stress-all` (mixed, all endpoints). Results are in the docs above.

## Layout

`cmd/viewcounter/` (wiring, config, shutdown), `internal/api/`, `internal/store/`, `internal/ingest/`, `internal/telemetry/`, `migrations/`, `loadtest/`.
