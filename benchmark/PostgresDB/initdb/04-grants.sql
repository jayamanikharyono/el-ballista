GRANT USAGE  ON SCHEMA public TO rel_extract;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO rel_extract;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO rel_extract;

-- Lets the extraction role read other sessions' pg_stat_activity (diagnostics).
GRANT pg_read_all_stats TO rel_extract;

-- The cost model reads pg_stats; don't wait for the first autovacuum pass.
ANALYZE;
