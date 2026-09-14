-- Grow orders toward SCALE_ROWS (a psql variable supplied by run.sh, e.g.
-- `psql -v SCALE_FACTOR=249 -f scale.sql`). run.sh computes the factor from the live
-- row count and skips this file entirely when already at target, so this file itself
-- stays a plain INSERT: no DO-block games, no stale thresholds.
--
-- Duplicated rows reuse past timestamps (fine for full loads) and get fresh identity
-- PKs / uuids from column defaults.

INSERT INTO public.orders (
  user_id, status, amount, currency, item_count, tags, metadata,
  shipped_on, created_at, updated_at
)
SELECT
  user_id, status, amount, currency, item_count, tags, metadata,
  shipped_on, created_at, updated_at
FROM public.orders, generate_series(1, :SCALE_FACTOR) AS g;

ANALYZE public.orders;
ANALYZE public.users;
