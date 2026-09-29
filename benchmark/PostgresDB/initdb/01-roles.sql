-- Read-only extraction role, per docs/connectors/postgres.md §6.

CREATE ROLE el_ballista LOGIN PASSWORD 'el_ballista';

DO $$
BEGIN
  EXECUTE format('GRANT CONNECT ON DATABASE %I TO el_ballista', current_database());
END
$$;
