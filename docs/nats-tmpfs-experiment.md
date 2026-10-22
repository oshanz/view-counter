# Experiment: JetStream on tmpfs (`POST /v2/hit` publish latency)

Question: is the disk the bottleneck of the `POST /v2/hit` publish ack?

Answer: no. Moving the JetStream store to RAM changed nothing measurable.

## Why we ran it

SigNoz showed `POST /v2/hit` with a 24h p99 of about 69 ms, and a sampled slow trace (28.1 ms) spent 28.05 ms in its only child span, `nats publish`. The handler publishes synchronously and returns 202 only after JetStream acks the message (see [jetstream-v2.md](jetstream-v2.md)). The stream uses file storage, so disk looked like the cause.

Reading the config narrowed that down: NATS runs with `-js -sd /data` and no other options, so `sync_interval` is the default of 2 minutes. The ack does not wait on a per-message fsync. Contention on the stream (acks from six ingest workers delete messages in a work queue) and a shared host (k6, SigNoz, Postgres and NATS on one machine) were the other suspects. The tmpfs run tests the disk hypothesis directly.

## Setup

- `docker-compose.tmpfs.yml` overrides the `nats` service to mount `/data` as an 8 GiB tmpfs. The base `docker-compose.yml` is unchanged.
- Load: `loadtest/hit_v2.js`, steps of 200, 500, 1000, 2000 and 4000 writes/s, 20 s each (about 154k hits, 2 minutes).
- Two runs back to back on the same machine: NATS on the `natsdata` volume (disk), then NATS on tmpfs. Run with `/data` confirmed as `tmpfs` in `mount` inside the container.
- Span numbers come from SigNoz over each run's exact time window. Request numbers come from k6.

## Results

| | Disk | tmpfs |
|---|---|---|
| `nats publish` p50 | 0.34 ms | 0.32 ms |
| `nats publish` p99 | 2.73 ms | 2.85 ms |
| `POST /v2/hit` p95 (k6) | 1.43 ms | 1.37 ms |
| `POST /v2/hit` p99 (k6) | 5.46 ms | 5.45 ms |
| Failed requests | 0 | 0 |
| Dropped iterations | 0 | 0 |

At 4000 writes/s the disk run had a k6 p95 of 2.06 ms and p99 of 7.22 ms.

## Conclusions

- The disk is not the bottleneck. RAM-backed storage gives the same latency within noise, so durable file storage costs nothing measurable here.
- The 28 ms traces were rare outliers, not typical. Up to 4000 writes/s, `nats publish` has a p50 of about 0.3 ms and a p99 of about 2.8 ms.
- `/v2/hit` handles well above the design load (about 1 write/s) with single-digit millisecond p99. The ideas considered next (fewer acks via `LimitsPolicy` + `AckAll`, async publish) are not worth the complexity at this load.
- The 69 ms 24h p99 is not explained by this experiment. It most likely comes from the load-test windows, where the machine is busy with k6 and SigNoz. To find the cause, pull the slowest `POST /v2/hit` traces from that period and look at the spans around them.

## Not measured

- Only single-node NATS (R1) on one machine. A clustered stream replicates before it acks and would behave differently.
- No `sync_interval: always` run. With per-message fsync the disk would matter.
- Consumer-side effects (Postgres batch time, lag) were not compared between the runs.
- One run per configuration, so differences under about 10% are noise.

## Pitfalls hit while running it

- `docker compose up --force-recreate` keeps the old container's mount for the same target, so the tmpfs override was silently ignored. Remove the container first: `docker compose -f docker-compose.yml -f docker-compose.tmpfs.yml rm -sf nats`, then `up -d --wait nats`.
- `mise run stress-hit-v2` runs `docker compose up` as a dependency and can recreate NATS with the base file. Run `NATS_URL=nats://127.0.0.1:4222 mise exec -- loadtest/run.sh loadtest/hit_v2.js` instead.
- The stream's `MaxBytes` is 4 GiB and JetStream may use only 75% of the filesystem by default. A 5 GiB tmpfs failed with `insufficient storage resources available` (err_code 10047); 8 GiB works.
- Check `mount | grep ' /data '` inside the container before trusting a result. Two early runs labelled "tmpfs" were really on disk and were discarded.

## Reproduce

```sh
docker compose -f docker-compose.yml -f docker-compose.tmpfs.yml rm -sf nats
docker compose -f docker-compose.yml -f docker-compose.tmpfs.yml up -d --wait nats
docker exec view-counter-nats-1 sh -c 'mount | grep " /data "'   # must say tmpfs
NATS_URL=nats://127.0.0.1:4222 mise exec -- loadtest/run.sh loadtest/hit_v2.js
```

tmpfs loses every hit still in the stream when NATS restarts. Use it for benchmarks only. To go back: `docker compose rm -sf nats && docker compose up -d --wait nats`.
