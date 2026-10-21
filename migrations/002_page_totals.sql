-- All-time views per key, kept in step with page_views by the write paths in
-- the same transaction. Total and Top read it instead of summing page_views.
CREATE TABLE page_totals (
    key   text   PRIMARY KEY,
    views bigint NOT NULL
);

INSERT INTO page_totals (key, views)
SELECT key, SUM(views) FROM page_views GROUP BY key;

CREATE INDEX page_totals_top_idx ON page_totals (views DESC, key);
