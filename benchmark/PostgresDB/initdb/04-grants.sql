GRANT USAGE  ON SCHEMA public TO rel_extract;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO rel_extract;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO rel_extract;

-- Without this, other sessions' pg_stat_activity.xact_start reads as NULL and
-- the safe high watermark silently degrades to now().
GRANT pg_read_all_stats TO rel_extract;

-- The cost model reads pg_stats; don't wait for the first autovacuum pass.
ANALYZE;
