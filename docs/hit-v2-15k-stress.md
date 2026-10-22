# Stress test: `POST /v2/hit` at 15k writes/s (where is the bottleneck?)

Question: what limits `POST /v2/hit` when offered 15,000 writes/s?

Answer: not settled. The host was heavily loaded (about 1 to 2 of 12 cores idle, load average 16), with k6 the largest CPU consumer, so the result cannot be separated from the machine it ran on. Inside the service, nearly all request time is the synchronous `nats publish` ack.

## Setup

- `STEPS=15000 STEP_DURATION=60s MAX_VUS=5000 loadtest/run.sh loadtest/hit_v2.js`, traces sampled at 1% (`OTEL_TRACES_SAMPLER=traceidratio`, arg `0.01`).
- Everything ran on one 12-core, about 34 GB machine: k6, the Go service, NATS, Postgres, the otel collector and SigNoz.
- Numbers come from k6 and from SigNoz over each run's time window. SigNoz metrics come in 60 s buckets, so values are averages that include a few idle seconds at the edges.

## k6 results (three runs)

| | Run 1 | Run 2 (collector on a stale config) | Run 3 |
|---|---|---|---|
| Throughput | 14.5k/s | not recorded | about 14.5k/s |
| p95 | 105 ms | 144 ms | 63 ms |
| p99 | 156 ms | 223 ms | 100 ms |
| Max | 503 ms | 492 ms | 276 ms |
| Dropped iterations | 21,230 (2.4%) | 46,592 | 3,737 (0.4%) |
| Failed requests | 0% | 0% | 0% |

The threshold is under 1% dropped (9,000). Only run 3 passes it. Run-to-run variance is large, so one run says little about the ceiling. Integrity held in the runs where it was checked: the hot key's count in Postgres equalled `acked_hot_hits` (615,185 in run 1), and the writer drained to 0 pending.

## Where the time goes (SigNoz traces, run 1, p99)

| Span | p99 |
|---|---|
| `ingest batch` | 172 ms |
| `INSERT` | 118 ms |
| `POST /v2/hit` | 104 ms |
| `nats publish` | 103 ms |
| `COMMIT` | 8 ms |
| `BEGIN` | 6 ms |
| `pool.acquire` | 3 ms |

`nats publish` is nearly all of the handler's time, because the handler returns 202 only after JetStream acks. The ingest side kept up: `ingest.pending` stayed at 0 and pool waits were negligible. The `ingest batch` and `INSERT` spans are off the request path.

## CPU and RAM (run 3)

Host, 12 cores:

| State | Cores |
|---|---|
| user | 6.8 |
| system | 2.1 |
| softirq | 0.9 |
| nice | 0.2 |
| idle | 2.1 |

Load average (1 min) was 16.3. The 2.1 idle figure is a 60 s bucket average that includes about 7 idle seconds, so idle during the load itself was lower, but I did not measure by how much. Per-core idle during the load was not captured.

Per process (`process.cpu.utilization` times 12 cores, assuming it is normalized by logical cores):

| Process | CPU | RSS |
|---|---|---|
| k6 | about 4.2 cores | 753 MB |
| `viewcounter` | about 2.8 cores | 125 MB |
| containers combined | about 1.2 cores | about 2.9 GB |

Containers, run 1 peak bucket: NATS 0.48 cores / 76 MB, Postgres 0.35 cores / 348 MB, SigNoz ClickHouse 0.23 cores / 1,499 MB.

Host memory: about 23.3 GB used, 8.4 GB cached, 1.2 GB free (down from 2.1 GB), so about 9.7 GB was available. No memory pressure; swap was not checked.

Go runtime: about 9 GC cycles/s, with stop-the-world pauses taking about 2.4% of wall time.

## 20k run (2026-10-22)

`STEPS=20000 STEP_DURATION=60s MAX_VUS=5000` with `NATS_URL` set, traces sampled at 1%. The working tree had `ingestWorkers = 1` (uncommitted; the committed value is 6). CPU, RAM and SigNoz traces were not collected for this run.

| | 20k step |
|---|---|
| Throughput | 15.7k/s of 20k offered (1,149,371 hits acked) |
| p95 | 126 ms |
| p99 | 190 ms |
| Max | 417 ms |
| Dropped iterations | 51,181 (4.3%) |
| Failed requests | 0% |
| VUs used | 3,026 peak (of 5,000) |

The dropped-iterations threshold (under 12,000) failed; latency and failure thresholds passed. Throughput matches the 14.5k/s of the 15k runs rather than scaling with the offered rate, and p95 is above all three 15k runs, so the system saturates at about 15k/s on this machine. As before, this does not separate the service from k6 and the shared host.

Integrity held: the hot key had 804,560 views in Postgres, equal to `acked_hot_hits`, and writer lag was 0 pending, 0 ack_pending after the drain wait. Draining with a single ingest worker kept up at this rate.

## 20k run with a lighter k6 script (2026-10-23)

Same command as the first 20k run (`STEPS=20000 STEP_DURATION=60s MAX_VUS=5000`, `NATS_URL` set, traces sampled at 1%), but `loadtest/hit_v2.js` was changed to cut k6's per-request work: `systemTags: ['status', 'scenario']`, a `responseCallback` of `http.expectedStatuses(202)` instead of `check()`, `preAllocatedVUs` of `max(20, rate/50)`, and a template-string body instead of `JSON.stringify`. `ingestWorkers = 1` is now the committed value. CPU, RAM and SigNoz traces were not collected.

| | 20k step | first 20k run |
|---|---|---|
| Throughput | 16.5k/s of 20k offered (1,146,545 hits acked) | 15.7k/s |
| p95 | 64.6 ms | 126 ms |
| p99 | 104 ms | 190 ms |
| Max | 278 ms | 417 ms |
| Dropped iterations | 53,523 (4.5%) | 51,181 (4.3%) |
| Failed requests | 0% | 0% |
| VUs used | 1,348 peak (1,737 allocated) | 3,026 peak |

The dropped-iterations threshold (under 12,000) failed again; latency and failure thresholds passed. Latency and VU use fell by about half and throughput rose about 5%, so the service still saturates near 16k/s, but each request waits less. This is one run per script version, so the effect of the script change is not separated from run-to-run noise, and k6's own CPU was not measured.

Integrity held: the hot key had 802,583 views in Postgres, equal to `acked_hot_hits`, and writer lag was 0 pending, 0 ack_pending after the drain wait.

## `/v1/hit` at 20k (2026-10-22, for comparison)

`NATS_URL= STEPS=20000 STEP_DURATION=60s MAX_VUS=5000 loadtest/run.sh loadtest/hit_v1.js`, traces sampled at 1%. `loadtest/hit_v1.js` is `hit_v2.js` pointed at `POST /v1/hit` (expects 204, no drain wait). Same machine and the same uncommitted `ingestWorkers = 1`, which does not affect `/v1`. CPU, RAM and traces were not collected.

| | `/v1/hit` 20k step |
|---|---|
| Throughput | about 995 req/s of 20k offered |
| Acked (204) | 39,199 |
| Failed requests | 39.4% (25,457), Postgres timeouts |
| Latency | avg 4.76 s, p95 5 s (the 5 s ceiling), max 5.03 s |
| Dropped iterations | 1,135,332 (threshold 12,000) |
| VUs | 5,000 (the maximum) |

All three thresholds failed. The service log shows about 25k `context deadline exceeded` errors and a few `write tcp ...:5433: i/o timeout`.

Every `/v1` hit is its own synchronous Postgres transaction (dedupe insert plus upsert), and 70% of hits go to one hot key, so they queue on that `page_views` row. The likely cause is that row lock, but I did not confirm it with Postgres lock or wait data. `/v2` batches those writes and reached about 15.7k/s on the same machine.

Integrity: the hot key holds 27,202 views in Postgres against 26,872 acked, so 330 hits committed but were reported as failures (timeouts after the commit). No acked hit was lost.

## Conclusions

- Postgres, the writer, the pool and memory were not the limit.
- k6 used more CPU than the service. With the kernel's networking share (system plus softirq, about 3 cores), the test harness and loopback traffic take most of the machine.
- The earlier claim that NATS is the bottleneck is not proven. `nats publish` dominating the handler is also what a CPU-starved host would look like.

## Next

- Run k6 on another machine, or pin it to separate cores with `taskset`, and repeat three times at 10k, 15k and 20k (20k has two runs so far, with different k6 scripts).
- Capture per-core, per-second CPU during the load (`mpstat -P ALL 1`) to check for a pinned core, such as the one NATS uses for the stream.

## What changed to collect this

- `otel/collector.yaml`: `hostmetrics` `process` scraper for `viewcounter` and `k6`. Restart the collector (`docker restart view-counter-otel-collector-1`) after editing; `docker compose up` does not notice bind-mounted file changes.
- `loadtest/run.sh`: the test binary is named `viewcounter` so the scraper matches it.
- `internal/telemetry/gc.go`: `go.gc.pause.total`, `go.gc.pause.last` and `go.gc.cycles`.
