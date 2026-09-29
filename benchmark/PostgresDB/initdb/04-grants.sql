GRANT USAGE  ON SCHEMA public TO el_ballista;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO el_ballista;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO el_ballista;

-- Lets the extraction role read other sessions' pg_stat_activity (diagnostics).
GRANT pg_read_all_stats TO el_ballista;

-- The cost model reads pg_stats; don't wait for the first autovacuum pass.
ANALYZE;
