# Read path

Design for ~1 read/sec: the same Go service queries Postgres directly. No cache, so reads are always current.

See also [write-path.md](write-path.md).

```mermaid
flowchart TD
    C[Client] -->|GET| RT{Endpoint}

    RT -->|/v1/views?key=| Q1[(Postgres: SUM views<br/>WHERE key = $1)]
    Q1 --> J1[Return JSON total]

    RT -->|/v1/views?key=&from=&to=| NP[Validate key, cap range]
    NP --> Q2[(Postgres: daily rows<br/>BETWEEN from AND to)]
    Q2 --> FILL[Fill missing days with zero]
    FILL --> J2[Return JSON series]

    RT -->|/v1/top| Q3[(Postgres: SUM views GROUP BY key<br/>ORDER BY views DESC LIMIT n)]
    Q3 --> J3[Return JSON top-N]
```

## Decisions

- **No cache.** At 1 read/sec every request can hit Postgres. Counts are exact as of the last committed hit.
- **Statement timeout of ~2s** on read queries, so a slow query can't pile up connections.
- **`/v1/top`** is a plain `GROUP BY` over `page_views`. Cap `n` (for example 100). If the table grows large, restrict it to recent days.
- **Keys** are validated the same way as on the write path.
