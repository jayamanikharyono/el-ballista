-- Read-only extraction role, per docs/connectors/postgres.md §6.

CREATE ROLE rel_extract LOGIN PASSWORD 'rel_extract';

DO $$
BEGIN
  EXECUTE format('GRANT CONNECT ON DATABASE %I TO rel_extract', current_database());
END
$$;
