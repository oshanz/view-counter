# Read path

Design for ~1k hits/sec: a stateless Go API service. Redis holds the 7-day dedupe state, Postgres holds the counts. The service keeps no keys, fingerprints or counters in its own memory, so any instance can be restarted or scaled freely.

See also [write-path.md](write-path.md).

Counts are exact because every accepted hit is already in Postgres, so there is no local delta to add.

```mermaid
flowchart TD
    C[Client] -->|GET| RT{Endpoint}

    RT -->|/v1/views?key=| NP[Validate key]
    NP --> Q1[(Postgres: SUM views<br/>WHERE key = $1)]
    Q1 --> J1[Return JSON total]

    RT -->|/v1/views?key=&from=&to=| NP2[Validate key, cap range]
    NP2 --> Q2[(Postgres: daily rows<br/>BETWEEN from AND to)]
    Q2 --> FILL[Fill missing days with zero]
    FILL --> J2[Return JSON series]

    RT -->|/v1/top| CK{In Redis cache?<br/>5-10s TTL}
    CK -- yes --> J3[Return cached top-N]
    CK -- no --> Q3[(Postgres: SUM views GROUP BY key<br/>ORDER BY views DESC LIMIT n)]
    Q3 --> ST[Store in Redis cache]
    ST --> J3b[Return JSON top-N]
```

Reads use a separate Postgres connection pool from the write path, with a statement timeout of about 2s, so a slow dashboard query can't block hits. The top-N cache lives in Redis, not the process, to keep the service stateless. Concurrent cache misses on `/v1/top` can each run the query, which is acceptable at this scale.
