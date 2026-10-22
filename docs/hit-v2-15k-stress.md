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

## Conclusions

- Postgres, the writer, the pool and memory were not the limit.
- k6 used more CPU than the service. With the kernel's networking share (system plus softirq, about 3 cores), the test harness and loopback traffic take most of the machine.
- The earlier claim that NATS is the bottleneck is not proven. `nats publish` dominating the handler is also what a CPU-starved host would look like.

## Next

- Run k6 on another machine, or pin it to separate cores with `taskset`, and repeat three times at 10k, 15k and 20k.
- Capture per-core, per-second CPU during the load (`mpstat -P ALL 1`) to check for a pinned core, such as the one NATS uses for the stream.

## What changed to collect this

- `otel/collector.yaml`: `hostmetrics` `process` scraper for `viewcounter` and `k6`. Restart the collector (`docker restart view-counter-otel-collector-1`) after editing; `docker compose up` does not notice bind-mounted file changes.
- `loadtest/run.sh`: the test binary is named `viewcounter` so the scraper matches it.
- `internal/telemetry/gc.go`: `go.gc.pause.total`, `go.gc.pause.last` and `go.gc.cycles`.
