CREATE TABLE page_views (
    key   text   NOT NULL,
    day   date   NOT NULL,
    views bigint NOT NULL,
    PRIMARY KEY (key, day)
);

CREATE TABLE seen (
    key     text        NOT NULL,
    fp_hash bytea       NOT NULL,
    seen_at timestamptz NOT NULL,
    PRIMARY KEY (key, fp_hash)
);

CREATE INDEX seen_seen_at_idx ON seen (seen_at);
