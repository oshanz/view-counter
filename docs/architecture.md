# Page view counter: architecture

Design for ~1k hits/sec: a Go API service with sharded in-memory counters, flushed to Postgres every second.

## Write path

```mermaid
flowchart TD
    C[Client] -->|POST /v1/hit| H[HTTP handler]
    H --> V{Valid body?<br/>rate limit ok?}
    V -- no --> E[400 / 429]
    V -- yes --> N[Normalize path<br/>strip query and fragment, cap length]
    N --> D{Dedupe enabled and<br/>seen recently?}
    D -- yes --> R204[204, not counted]
    D -- no --> S[Pick shard by hash of page]
    S --> I[Lock shard, increment counter, unlock]
    I --> R204b[204 No Content]

    subgraph Flusher goroutine, every 1s
        T[Ticker] --> SW[Swap each shard map<br/>for an empty one]
        SW --> M[Merge old maps into one batch]
        M --> CAP{Over per-day<br/>page cap?}
        CAP -- yes --> OT[Fold new pages into __other__]
        CAP -- no --> TX
        OT --> TX[One transaction:<br/>upsert views = views + delta]
        TX --> OK{Success?}
        OK -- yes --> DONE[Done]
        OK -- no --> RB[Re-merge into next batch<br/>drop over memory cap, count dropped_hits]
    end

    I -.-> SW
    TX --> PG[(Postgres<br/>page_views: page, day, views)]

    SIG[SIGTERM] --> STOP[Stop accepting, drain] --> FF[Final flush] --> EXIT[Exit]
```

## Read path

```mermaid
flowchart TD
    C[Client] -->|GET| RT{Endpoint}

    RT -->|/v1/views?page=| NP[Normalize path]
    NP --> Q1[(Postgres: SUM views<br/>WHERE page = $1)]
    Q1 --> LD[Add this instance's<br/>unflushed delta for the page]
    LD --> J1[Return JSON total]

    RT -->|/v1/views?page=&from=&to=| NP2[Normalize path, cap range]
    NP2 --> Q2[(Postgres: daily rows<br/>BETWEEN from AND to)]
    Q2 --> FILL[Fill missing days with zero,<br/>add local delta to today]
    FILL --> J2[Return JSON series]

    RT -->|/v1/top| CK{In cache?<br/>5-10s TTL}
    CK -- yes --> J3[Return cached top-N]
    CK -- no --> SF[singleflight: one query<br/>for concurrent requests]
    SF --> Q3[(Postgres: SUM views GROUP BY page<br/>ORDER BY views DESC LIMIT n)]
    Q3 --> ST[Store in cache]
    ST --> J3b[Return JSON top-N]
```

Reads use a separate connection pool from the flusher, with a statement timeout of about 2s, so a slow dashboard query can't block writes.
