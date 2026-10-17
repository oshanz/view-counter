# Read path

Design for ~1k hits/sec: a stateless Go API service. Redis holds the 7-day dedupe state and the read cache, Postgres holds the counts. The service keeps no keys, fingerprints or counters in its own memory, so any instance can be restarted or scaled freely.

See also [write-path.md](write-path.md).

Every accepted hit is already in Postgres, so there is no local delta to add. Reads never aggregate in Postgres on each request: totals, series and top-N are served from the Redis cache, and Postgres is only queried on a miss. Counts can therefore lag by up to the cache TTL.

```mermaid
flowchart TD
    C[Client] -->|GET| RT{Endpoint}

    RT -->|/v1/views?key=| NP[Validate key]
    NP --> CK1{In Redis cache?<br/>views:total:key, 10s TTL}
    CK1 -- yes --> J1[Return JSON total]
    CK1 -- no --> Q1[(Postgres: SUM views<br/>WHERE key = $1)]
    Q1 --> ST1[Store in Redis cache]
    ST1 --> J1b[Return JSON total]

    RT -->|/v1/views?key=&from=&to=| NP2[Validate key, cap range]
    NP2 --> CK2{In Redis cache?<br/>views:series:key:from:to, 10s TTL}
    CK2 -- yes --> J2[Return JSON series]
    CK2 -- no --> Q2[(Postgres: daily rows<br/>BETWEEN from AND to)]
    Q2 --> FILL[Fill missing days with zero]
    FILL --> ST2[Store in Redis cache]
    ST2 --> J2b[Return JSON series]

    RT -->|/v1/top| CK3{In Redis cache?<br/>top:n, 5-10s TTL}
    CK3 -- yes --> J3[Return cached top-N]
    CK3 -- no --> Q3[(Postgres: SUM views GROUP BY key<br/>ORDER BY views DESC LIMIT n)]
    Q3 --> ST3[Store in Redis cache]
    ST3 --> J3b[Return JSON top-N]
```

Reads use a separate Postgres connection pool from the write path, with a statement timeout of about 2s, so a slow dashboard query can't block hits. All caches live in Redis, not the process, to keep the service stateless. Concurrent cache misses on the same cache key can each run the query, which is acceptable at this scale. If Redis is unavailable, reads fall back to querying Postgres directly.
