-- Hostile fixture for the Postgres integration tests: every decode edge in one small,
-- deterministic table. Single source of truth — `tests/common/postgres.rs` (`TestDb`)
-- loads this file into a fresh per-test schema, substituting every `__SCHEMA__` token
-- with that schema's (harness-generated, identifier-safe) name. Do not hard-code a schema.
--
-- `hostile` (ids 1..13, deterministic):
--   * rows 1..8: one row per edge; `updated_at` spread over 2024-01-01..08 (one per day)
--     so window queries can slice it; row 5 and row 8 are the mostly-NULL rows.
--   * rows 9..13: a 5-row tie on `updated_at` = 2024-01-09 00:00:00+00 (keyset/cursor
--     determinism: a `(ts, id)` cursor must return all five), carrying the extreme
--     timestamps/dates: epoch - 1 µs, 2038-01-19 03:14:07/08 (the i32-seconds rollover)
--     and 9999-12-31 23:59:59.999999.
--   * `bin bytea`: 0x00 / 0xFF bytes, an empty value ('' vs NULL) and NULLs.
--   * also: '' vs NULL text, MIN/MAX int4/int8, NaN/±Infinity floats, 1.00 vs 1.5
--     decimals, text[] with NULL and '' elements, jsonb, uuid, an enum, 1970-01-01 and a
--     leap day.
--
-- `hostile_infinity`: ±infinity timestamps/dates live in their OWN table because they have
-- no Arrow representation and extraction must fail with a typed error by design — keeping
-- them out of `hostile` lets every other suite scan `hostile` successfully.

CREATE TYPE __SCHEMA__.mood AS ENUM ('sad', 'ok', 'ecstatic');

CREATE TABLE __SCHEMA__.hostile (
    id bigserial PRIMARY KEY,
    name text,
    nick varchar(20),
    code bpchar(4),
    amount numeric(12,2),
    precise numeric(30,15),
    count integer,
    big bigint,
    ratio double precision,
    f real,
    flag boolean,
    tags text[],
    meta jsonb,
    uid uuid,
    day date,
    ts timestamptz,
    naive timestamp without time zone,
    feeling __SCHEMA__.mood,
    updated_at timestamptz NOT NULL,
    bin bytea
);

INSERT INTO __SCHEMA__.hostile
    (name, nick, code, amount, precise, count, big, ratio, f, flag, tags,
     meta, uid, day, ts, naive, feeling, updated_at, bin) VALUES
    ('Zürich', 'MÜNCHEN', 'ab', 123.45, 3.141592653589793, 2147483647,
     9223372036854775807, 'NaN', 'Infinity', true, '{"a",NULL,""}',
     '{"a":1,"b":[true,null]}', '123e4567-e89b-12d3-a456-426614174000',
     '2024-02-29', '2024-03-01 12:00:00+02', '2024-03-01 12:00:00',
     'ecstatic', '2024-01-01', '\x00ff'),
    ('', '', '', -7.50, 0, -2147483648, -9223372036854775808,
     '-Infinity', 1.5, false, '{}', '{}', '123e4567-e89b-12d3-a456-426614174001',
     NULL, NULL, NULL, 'sad', '2024-01-02', '\x'),
    ('plain', 'plain', 'wxyz', 0.00, -0.5, 0, 0, 0.0, 0.0, NULL,
     '{x,y,z}', '[]', '123e4567-e89b-12d3-a456-426614174002',
     '1970-01-01', '1970-01-01 00:00:00+00', '1970-01-01 00:00:00',
     'ok', '2024-01-03', '\x00'),
    ('MiXeD', 'MiXeD', 'q', 99999999.99, 100.25, 42, 42, 2.5, -3.25, true,
     NULL, NULL, '123e4567-e89b-12d3-a456-426614174003',
     '1999-12-31', '1999-12-31 23:59:59-05', '1999-12-31 23:59:59',
     'ok', '2024-01-04', '\xdeadbeef'),
    ('nulls', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
     NULL, NULL, NULL, NULL, NULL, NULL, NULL, '2024-01-05', NULL),
    ('six', 'six', 'six6', 1.00, 1.5, 6, 6, 6.0, 6.0, true,
     '{s}', '{"n":6}', '123e4567-e89b-12d3-a456-426614174005',
     '2024-06-15', '2024-06-15 06:30:00+00', '2024-06-15 06:30:00',
     'sad', '2024-01-06', '\xff'),
    ('seven', 'seven', 'svn7', 42.42, -2.75, 7, 7, 7.0, 7.0, false,
     '{a,b}', '{"n":7}', '123e4567-e89b-12d3-a456-426614174006',
     '2024-07-07', '2024-07-07 07:07:07+00', '2024-07-07 07:07:07',
     'ecstatic', '2024-01-07', '\x0102'),
    ('eight', 'eight', 'eght', NULL, NULL, 8, 8, 8.0, 8.0, NULL,
     NULL, NULL, NULL, NULL, NULL, NULL, NULL, '2024-01-08', NULL),
    -- 5-row updated_at tie (ids 9..13) with the extreme timestamps.
    ('tie1', 'tie', 'tie1', NULL, NULL, 9, 9, 9.0, 9.0, true,
     NULL, NULL, NULL,
     '1969-12-31', '1969-12-31 23:59:59.999999+00', '1969-12-31 23:59:59.999999',
     'ok', '2024-01-09 00:00:00+00', '\x00'),
    ('tie2', 'tie', 'tie2', -0.01, NULL, 10, 10, 10.0, 10.0, false,
     NULL, NULL, NULL,
     '2038-01-19', '2038-01-19 03:14:07+00', '2038-01-19 03:14:07',
     'sad', '2024-01-09 00:00:00+00', '\xff'),
    ('tie3', 'tie', 'tie3', NULL, NULL, 11, 11, 11.0, 11.0, NULL,
     NULL, NULL, NULL,
     '2038-01-20', '2038-01-19 03:14:08+00', '2038-01-19 03:14:08',
     NULL, '2024-01-09 00:00:00+00', '\x00ff00ff'),
    ('tie4', 'tie', 'tie4', NULL, NULL, 12, 12, 12.0, 12.0, true,
     NULL, NULL, NULL,
     '9999-12-31', '9999-12-31 23:59:59.999999+00', '9999-12-31 23:59:59.999999',
     'ecstatic', '2024-01-09 00:00:00+00', '\xffffffff'),
    ('tie5', 'tie', 'tie5', NULL, NULL, 13, 13, 13.0, 13.0, NULL,
     NULL, NULL, NULL, NULL, NULL, NULL, NULL, '2024-01-09 00:00:00+00', NULL);

ANALYZE __SCHEMA__.hostile;

CREATE TABLE __SCHEMA__.hostile_infinity (
    id bigint PRIMARY KEY,
    ts timestamptz,
    naive timestamp without time zone,
    day date
);

INSERT INTO __SCHEMA__.hostile_infinity VALUES
    (1, '2024-01-01 00:00:00+00', '2024-01-01 00:00:00', '2024-01-01'),
    (2, 'infinity', 'infinity', 'infinity'),
    (3, '-infinity', '-infinity', '-infinity');
