-- Deterministic benchmark seed: 500 users, 20k orders spread over the last 90 days.
-- benchmark/run.sh then grows `orders` to SCALE_ROWS with scale.sql.
SELECT setseed(0.42);

INSERT INTO public.users (email, full_name, country, is_active, created_at, updated_at)
SELECT
  format('user%s@example.com', g),
  format('User %s', g),
  (ARRAY['ID', 'SG', 'MY', 'US', 'AU'])[1 + (g % 5)],
  (g % 17) <> 0,
  now() - make_interval(days => 180 - (g % 180)),
  now() - make_interval(days => 180 - (g % 180))
FROM generate_series(1, 500) AS g;

WITH raw AS (
  SELECT
    g,
    1 + floor(random() * 500)::bigint AS user_id,
    random()                          AS r_status,
    random()                          AS r_lag,
    now() - make_interval(
      days  => floor(random() * 90)::int,
      hours => floor(random() * 24)::int,
      mins  => floor(random() * 60)::int
    ) AS created_at
  FROM generate_series(1, 20000) AS g
), typed AS (
  SELECT
    g,
    user_id,
    created_at,
    CASE
      WHEN r_status < 0.55 THEN 'PAID'
      WHEN r_status < 0.75 THEN 'PENDING'
      WHEN r_status < 0.90 THEN 'SHIPPED'
      WHEN r_status < 0.97 THEN 'CANCELLED'
      ELSE 'REFUNDED'
    END::public.order_status AS status,
    -- Row is touched some time after creation, never in the future.
    LEAST(created_at + make_interval(hours => floor(r_lag * 72)::int), now()) AS updated_at,
    CASE
      WHEN r_lag < 0.30 THEN ARRAY['express']
      WHEN r_lag < 0.60 THEN ARRAY['gift', 'fragile']
      ELSE '{}'::text[]
    END AS tags
  FROM raw
)
INSERT INTO public.orders (
  user_id, status, amount, currency, item_count, tags, metadata, shipped_on, created_at, updated_at
)
SELECT
  user_id,
  status,
  round((random() * 4000000 + 10000)::numeric, 2),
  'IDR',
  1 + floor(random() * 8)::int,
  tags,
  jsonb_build_object(
    'channel', (ARRAY['web', 'ios', 'android'])[1 + (g % 3)],
    'promo',   (g % 11) = 0,
    'attempt', 1 + (g % 3)
  ),
  -- Left NULL for unshipped orders, so null_frac is non-trivial in pg_stats.
  CASE WHEN status = 'SHIPPED' THEN (updated_at + interval '1 day')::date END,
  created_at,
  updated_at
FROM typed;
