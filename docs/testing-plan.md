# Testing Plan — Unit + Integration Tests

**Status**: Phase A COMPLETE (September 2026) — 83 lib + 81 bin unit tests, all green, no DB.
Phases B–D not started.

## 1. Where we stand

- **83 lib unit tests (81 in the bin target, which recompiles the same modules — see §6),
  all fixture-based, zero live-database coverage.** Every test constructs structs by hand;
  none opens a connection.
- **No `tests/` directory, no `[dev-dependencies]`.** Integration tests don't exist.
- **No Docker** on dev machines → `testcontainers-rs` is out. Integration tests run against a
  real Postgres via `DATABASE_URL` and skip gracefully without one, so `cargo test` stays
  green everywhere.
- **Every live bug so far was in untested territory**: `TEXT[]` decode (`RowBatchBuilder`),
  timestamptz builder timezone, `relid` in the stats SQL, `order_status = text` operator.
  Phase A added regression tests for the shapes that *can* be tested without a database
  (array appending, schema-type agreement, enum normalization); the live-DB halves wait
  for Phase C.

## 2. Strategy: two layers, different jobs

| | Unit (`src/**/tests`) | Integration (`tests/`) |
|---|---|---|
| Needs DB | Never | Yes (`DATABASE_URL`, else skip) |
| Speed | Milliseconds | Seconds |
| Job | Pure logic: translation, decisions, rendering, window math, state machines, schema/type maps | Everything that touches Postgres or the cluster: decode fidelity, SQL validity, stats/index/enum catalog reads, checkpoint durability, distributed execution |
| Rule | If it needs a pool, it doesn't belong here | If it can be a fixture, it doesn't belong here |

## 3. Phase A — unit gaps (no DB) ✅ DONE

All seven items implemented; `cargo test` (no DB) covers all pure logic:

1. **Predicate matrix** ✅ (`pushdown::tests::test_translate_render_matrix`): all 6 comparison
   operators × bool/int/float/text/timestamp literals asserting `(fidelity, SQL)`, plus
   AND/OR/NOT/IS NULL/IS NOT NULL, left-to-right param order, and allowlist rejections
   (arithmetic, LIKE). Cross-dialect equivalence is pinned separately by
   `test_render_sql_is_dialect_neutral`. Subsumed the old spot-check test.
2. **`decide_translated` policy matrix** ✅ (`test_policy_matrix_legacy_fallbacks_and_deny`,
   plus the `always`/`cost_based`/`hinted`/`strict` snapshots): all 5 policies × deny,
   with/without stats — pins the optimistic legacy fallback and strict's no-stats keep.
3. **Window math edge cases** ✅ (`incremental::tests::{test_build_window_skewed_hi_below_lo,
   test_build_window_zero_max_window, test_clamp_to_observed_equal_is_stable}`).
4. **Checkpoint state machine** ✅ (`checkpoint::json_store::tests`, 5 tests: acquire→commit→read
   cycle, live-lease double-acquire rejection, expired-lease reclaim, wrong-`run_id` commit
   rejection, abandon idempotency) against unique temp dirs. Also fixed a stale `JobKey` doc.
5. **Query builder shapes** ✅ (`query_builder::tests`, 4 tests): incremental/full/keyset SQL
   strings, `USER-DEFINED → ::text AS` cast, no-WHERE on full scans. (One test caught a wrong
   expectation during writing — unquoted `public` schema — and now pins the true shape.)
6. **Codec round-trips** ✅ (`table_codec::tests::{test_logical_codec_round_trip,
   test_logical_codec_non_magic_delegates}`): pool-free `from_model` round-trip (descriptor,
   policy, partitioning intent, enum columns survive) + non-magic bytes delegate-and-reject.
7. **Config parsing** ✅ (`config::tests::{test_from_file_example_config,
   test_from_file_missing_is_config_error}`): the checked-in example JSON must parse with
   expected values + defaults.

## 4. Phase B — integration harness + hostile fixture (one-time setup)

`tests/common/mod.rs`:

- `TestDb::connect() -> Option<TestDb>`: reads `DATABASE_URL`, attempts one connection with a
  short timeout, returns `None` (→ test early-returns `Ok(())` with a `SKIP` print) if
  unreachable. **No test may fail for lack of a database.**
- Per test: `CREATE SCHEMA test_<name>_<pid>_<counter>`, run the hostile DDL inside it,
  `ANALYZE`, hand out the schema name; `Drop` runs `DROP SCHEMA ... CASCADE` (best-effort).
  Schema isolation (not database isolation) keeps setup to milliseconds and allows parallel
  tests against one server.
- **Hostile fixture** (one table, every edge): NULLs, empty strings, mixed-case + accented
  text, `NaN`/`±Infinity` floats, `INT_MIN`/`INT_MAX`, high-precision `numeric(30,15)`,
  `text[]` with NULL elements and empty arrays, `jsonb`, `uuid`, `date`, `timestamptz` +
  naive timestamps, a true **enum** column, `bpchar`. ~30 rows. This fixture is the shared
  input to every differential suite.

New dev-dependencies: **none** to start (`tokio`, `sqlx`, `serde_json` already exist;
tempdirs via `std`). Revisit only if needed (`testcontainers` if Docker ever appears).

## 5. Phase C — integration suites (each maps to a past live bug)

1. **Path equivalence** (catches TEXT[]/TZ class): same hostile table through
   `extract_full_table` vs `extract_incremental_window` vs `*_via_cursor` →
   byte-identical `RecordBatch` streams (compare debug-normalized output). Any decode or
   timezone divergence fails loudly.
2. **Pushdown differential** (roadmap `pushdown.md` §6, the single highest-value test):
   every allowlisted predicate × `always` vs `never` on hostile data → identical result sets.
   Catches every fidelity lie, including the enum-operator class (which currently errors —
   the suite asserts equality, so a 42883 fails it before it can be "fixed" by keeping).
3. **Catalog reads** (catches `relid` class): `table_statistics`, `table_indexes`,
   `table_enum_columns` against the real catalog — assert row counts, the known indexes,
   and the enum column set. Pure SQL-shape validation, no extraction needed.
4. **Watermark + leases**: `safe_high_watermark` returns `≤ now()`; full
   `acquire → extract → commit → read` cycle advances the watermark; kill-simulation
   (no commit) leaves reclaimable state. Checkpoint dir under temp.
5. **Distributed end-to-end**: `DistributedContext::standalone` + `register_source` +
   `collect` against the hostile table — exercises codecs, partitioning, and budgeted pools
   with zero external processes. (Multi-process `scheduler`/`worker` runs stay manual;
   the wire format is identical.)
6. **Strict-policy live check**: `status = 'PAID'` under `strict` keeps in Arrow and still
   returns exactly the `always` rows — proves the paranoid mode is correct, not just quiet.

## 6. Phase D — CI + hygiene

- **GitHub Actions**: `postgres:16` service, `DATABASE_URL` set, `ANALYZE` not needed
  (harness runs it), `cargo test --all-targets`. Local runs without `DATABASE_URL` skip
  integration tests and stay green — assert this in CI too (a second job *without* the
  service, proving graceful skip).
- **Hygiene item**: `main.rs` re-declares every module instead of using the lib crate, so
  all unit tests compile and run twice (83 vs 81 counts) and bin-only dead-code warnings
  multiply. Migrate the binary to `use rust_ballista_extraction_layer::...` and delete the
  `mod` declarations. Small, mechanical, kills a whole warning class.
- **Coverage gate (later, optional)**: `cargo-tarpaulin` with a ratchet (fail only if
  coverage *drops*), never an absolute threshold — absolute gates rot into `#[allow]`
  spam on a hobby project.

## 7. Exit criteria

- [x] Every bullet in Phase A has tests; `cargo test` (no DB) covers all pure logic.
- [ ] Every bullet in Phase C runs green against local Postgres; each of the four historical
  live bugs has a named regression test that fails when its fix is reverted (verify by
  revert-check once).
- [ ] CI runs both modes (with/without DB).
- [ ] The plan snapshot tests from Phase 2 keep passing unchanged (no behavior drift).

## 8. Explicitly out of scope

- Multi-process scheduler/worker integration runs (manual; wire format covered by Phase C.5).
- Property/`proptest` suites and the hostile-value differential *at scale* (roadmap §6's
  full vision) — Phase C.2 is the seeded, deterministic core of it.
- Benchmarks/throughput measurement (roadmap exit criteria reference them; they are runs,
  not tests, and belong in docs with numbers, not in CI).
