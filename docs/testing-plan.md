# Testing Plan — Unit + Integration Tests

**Status (September 2026): all four original phases are done.** Unit coverage (Phase A),
the self-provisioning integration harness (Phase B), the per-bug integration suites (Phase
C, minus one item — see §7), and CI (Phase D) are all implemented and green. This document
describes what actually exists today, not a forward-looking plan; §7 lists what's still
open.

The single most important property of the current suite: **nothing skips, and nothing is
hidden behind manual setup.** `cargo test` alone — no `DATABASE_URL`, no Docker, no running
Ballista cluster — runs the full unit + integration + e2e suite on a clean checkout. A test
that cannot reach what it needs fails loudly (`panic!` with a specific message), never
silently reports green or "skipped." That single change surfaced two real, previously
invisible bugs while this suite was being wired up for real (a stale test assertion in
`pg_paths.rs` that assumed the wrong window boundary, and a genuine EXPLAIN-JSON decode bug
in `pushdown::explain`) — both fixed, both now regression-tested.

## 0. Running the tests

```bash
# Bring up the databases first (integration/e2e tests never self-provision).
docker compose -f tests/docker/compose.yaml up -d --wait

# Everything — unit, integration, e2e.
cargo test

# Or: scripts/e2e.sh brings the stack up, runs the suite, and tears it down.
scripts/e2e.sh

# Optional: point every integration/e2e test at a specific server instead of the
# compose default (still Docker either way; this just changes which Postgres).
DATABASE_URL=postgres://postgres:postgres@127.0.0.1:5433/app cargo test

# Unit tests only (no database touched at all, milliseconds).
cargo test --lib --bins

# One integration file at a time.
cargo test --test pg_pushdown
cargo test --test pg_catalog
cargo test --test e2e
```

## 1. Two layers, different jobs

| | Unit (`src/**/tests`) | Integration + e2e (`tests/*.rs`) |
|---|---|---|
| Needs a database | Never | Always — the Docker compose stack, see §3 |
| Speed | Milliseconds | Seconds (compose Postgres + queries); e2e is ~15-75s per file because it also spins up an in-process Ballista scheduler+executor |
| Job | Pure logic: predicate translation, filter parsing/lowering, cost/policy decisions, split state machines, schema/type maps, SQL-string shapes | Everything that actually touches Postgres or a cluster: decode fidelity, catalog reads, checkpoint durability, pushdown correctness, distributed execution |
| Rule | If it needs a pool, it doesn't belong here | If it can be a fixture, it doesn't belong here |
| Skip behavior | N/A | **None.** Every test gets a real database or fails |

## 2. Unit tests — 129 tests across 24 modules, zero database access

All of these run under plain `cargo test --lib` in milliseconds; none opens a network
connection. Breakdown by module (counts from `cargo test --lib -- --list`):

| File | Tests | What it covers |
|---|---:|---|
| `pushdown/mod.rs` | 13 | Predicate translation matrix (6 comparators × 5 literal types), AND/OR/NOT/IS NULL, policy matrix (`always`/`never`/`cost_based`/`hinted`/`strict`), enum-column normalization, dialect-neutral rendering |
| `config/mod.rs` | 13 | Config parsing against the checked-in example JSON (incl. structured `filters`), filter value typing, `is_null` without value, unknown-op rejection at load, validation rejections, password resolution |
| `connector/postgres/row_adapter.rs` | 11 | Arrow schema/array building per Postgres type, including the historical `TEXT[]` and timestamptz-timezone bug classes |
| `connector/postgres/pipeline/mod.rs` | 11 | Shorthand/structured filter lowering equivalence, JSON value typing, null/misuse rejections, timestamp/date coercion with schema (RFC3339 + `%Y-%m-%d`), example config filter parsing |
| `connector/postgres/parallel.rs` | 10 | Pure keyset/ctid partition math (`keyset_partitions_from_bounds`, `ctid_partitions_from_relpages`) — even splits, degenerate ranges (min≥max, zero/negative page counts), off-by-one boundary and inversion checks |
| `pushdown/optimizer_rule.rs` | 9 | DataFusion optimizer rule: conjunct splitting, push/keep decisions, filter reconstruction (single/multiple/empty), enum-aware decisions |
| `pushdown/explain.rs` | 9 | EXPLAIN JSON parsing: access-method mapping, cost/row extraction, and failure paths (invalid JSON, empty plan array, missing `Plan` key, missing fields default conservatively, `Plans Rows` key variant) |
| `pushdown/cost_model.rs` | 9 | Cost-based push/keep decisions: index shortcut (incl. OR-branch requirement), selectivity gate, cost-budget gate, each exercised through `decide_push` itself (not hand-built enum literals) |
| `connector/query_tag.rs` | 6 | Debug SQL comment tag rendering (see §6), including a `*/`-injection defense test |
| `logging.rs` | 5 | CLI/env log-option resolution |
| `connector/postgres/execution_plan.rs` | 5 | Placeholder bind order, `DisplayAs`, partition count, and the debug-tag rendering (fields present, partition index included only when split) |
| `connector/postgres/dialect.rs` | 4 | Identifier quoting / dialect rendering |
| `checkpoint/json_store.rs` | 4 | Split begin/run/complete/fail cycle, retry preserves completed splits, unknown split errors, reset clears |
| `connector/postgres/query_builder.rs` | 3 | Full/keyset SQL string shapes, `USER-DEFINED → ::text AS` cast |
| `distributed/pool_registry.rs` | 3 | Budgeted-pool-per-descriptor registry behavior |
| `pushdown/stats.rs` | 2 | `SourceStatistics::empty()` invariants (no column entries, no cross-table aliasing) |
| `connector/postgres/distributed/table_codec.rs` | 2 | Logical codec round-trip, non-magic-bytes delegation |
| `connector/postgres/api.rs` | 2 | Distributed scheduler default + overrides |
| `connector/mysql/type_mapper.rs` | 2 | MySQL type → Arrow mapping |
| `connector/mysql/dialect.rs` | 2 | MySQL quoting/placeholder rendering |
| `connector/postgres/table_provider.rs` | 1 | Scan pushes a normalized enum predicate |
| `connector/postgres/distributed/plan_codec.rs` | 1 | Physical codec round-trip (see also the model change note in §6) |
| `connector/postgres/arrow_type_mapper.rs` | 1 | Postgres type → Arrow type mapping |
| `connector/mysql/query_builder.rs` | 1 | MySQL query shapes |

Devil's-advocate note: several of these were rewritten from an earlier pass that only
constructed a struct and asserted its own fields back (a tautology — it can't fail). The
cost-model, catalog-stats, optimizer-rule, explain-parsing, and partition-math tests above
were all replaced with tests that exercise the real function and its failure paths, and in
`parallel.rs`'s case the partition math was extracted into pure functions specifically so
it could be unit-tested at all (it previously required a live pool).

## 3. Integration harness — `tests/common/mod.rs`

`TestDb::connect() -> TestDb` (no `Option`, never skips):

- Uses `DATABASE_URL` if set; otherwise connects to the compose stack's default endpoint
  (`postgres://postgres:postgres@127.0.0.1:5432/test`). The stack must be up
  (`docker compose -f tests/docker/compose.yaml up -d --wait`, or `scripts/e2e.sh`
  which handles it automatically) — there is no embedded fallback and no silent skip.
- Any failure to connect or provision — bad URL, connection refused (is the stack up?),
  fixture setup failure — is a `panic!` with a specific message. A broken harness
  is a **test failure**, not a silent skip.
- Per test: `CREATE SCHEMA test_<pid>_<counter>`, builds the hostile fixture inside it,
  hands out the schema name; `Drop` runs `DROP SCHEMA ... CASCADE` (best-effort, via a
  spawned thread with its own runtime since `Drop` has no async context). Schema isolation
  (not database isolation) keeps setup to milliseconds and lets tests run in parallel
  against one server.
- **Hostile fixture** (one `hostile` table, every decode edge in ~8 deterministic rows):
  NULLs, empty strings, mixed-case + accented text, `NaN`/`±Infinity` floats,
  `INT_MIN`/`INT_MAX`, high-precision `numeric(30,15)`, `text[]` with NULL elements and
  empty arrays, `jsonb`, `uuid`, `date`, `timestamptz` + naive timestamps, a true Postgres
  **enum** type (`mood`), `bpchar`. `updated_at` is spread over 2024-01-01..08 so window
  queries can slice it. This fixture is the shared input to every integration/e2e suite.

No test-only dev-dependencies: integration/e2e tests run against the Docker compose
stack (`tests/docker/compose.yaml`).

## 4. Integration + e2e suites — 24 tests across 7 files

Every file below follows the same pattern: a `live!()` macro that just calls
`TestDb::connect().await` (kept as a macro purely so call sites didn't need editing when
skip support was removed).

| File | Tests | Proves |
|---|---:|---|
| `pg_numeric.rs` | 1 | Numeric columns decode with exact magnitude end to end (the historical truncation bug: `123.45` scale 2 must decode to unscaled `12345`, not `123`) |
| `pg_paths.rs` | 1 | `extract_full_table`, keyset partitions, and filtered extraction return identical data (differential: pushed filter vs direct SQL) |
| `pg_edge.rs` | 5 | Duplicate timestamps all extracted, empty filter returns an empty stream that still carries the schema, keyset cursor batching respects boundaries, projection returns only requested columns, date/timestamp/timezone fidelity round-trips |
| `pg_pushdown.rs` | 1 | `always` vs `never` pushdown policy return byte-identical row sets, including the OR-mixing-indexed-and-unindexed-column regression case |
| `pg_distributed.rs` | 1 | `DistributedContext::standalone` + `register_source` + `collect` against the hostile table exercises codecs, keyset partitioning, and budgeted pools with zero external processes; also pins exact decimal fidelity through the distributed path |
| `pg_catalog.rs` | 11 | DB-facing functions with no unit coverage: `table_statistics`/`table_indexes`/`table_enum_columns` (real data, and the not-fabricated-for-unknown-table case), `ExplainEstimator::estimate_cost`/`cached_estimate` (real plan, TTL caching, rejects a nonexistent table) |
| `e2e.rs` | 5 | Full/filtered/selective/distributed extraction, entirely in-process (Ballista scheduler + executor run standalone in the test process — no external cluster), plus split-checkpoint retry (second run skips completed splits, same row count). Oracles are always live Postgres queries |

Historical note: `e2e.rs` used to require `E2E_SCHEDULER_URL` and skip entirely without an
externally-running cluster — which is exactly why its filtered-extraction test carries
a wrong assertion for a long time without anyone noticing. Removing the skip path is what
caught it.

## 5. CI — `.github/workflows/ci.yml`

Runs on every push and PR, `ubuntu-latest`, with a `services:` Postgres container
(`DATABASE_URL` points the suites at it; locally the same role is played by
`tests/docker/compose.yaml`):

1. `cargo fmt --check` — informational (`continue-on-error: true`); the codebase predates
   a formatting pass.
2. `cargo build --workspace --all-targets`.
3. `cargo clippy --workspace --all-targets -- -D warnings` — also informational for now,
   same reason.
4. `cargo test --lib --bins` — unit tests.
5. `cargo test --tests` — integration + e2e against the compose Postgres/MySQL stack
   (the CI job provides them as service containers; locally bring up
   `tests/docker/compose.yaml` or run `scripts/e2e.sh`).

Two caches: the usual `~/.cargo` + `target`.

## 6. Related: debug SQL comment tags

Not a testing feature, but built alongside this suite and worth noting here because it
makes live-Postgres test failures easier to diagnose: `connector::query_tag::QuerySession`
prepends a comment like

```
/* rust-extract query_id=q_1a2b3c4d pipeline=orders_extract run_id=r_9f8e7d6c strategy=full partition=7/23 */
```

to every data-scan query issued through `PostgresExecutionPlan` (the main DataFusion/
Ballista path) and `PostgresExtractor`'s cursor-based methods, so
the running statement is identifiable directly in `pg_stat_activity` / logs during a test
run against a real server, not just in application-side logs. `strategy` is
`full`, optionally suffixed `+pushdown`; `partition=i/n` (1-based) appears
only when the scan is actually split. This added a `run_id: String` field to
`PostgresExecutionPlanModel` (the type serialized across the Ballista wire) — the round
trip is covered by `distributed::plan_codec::tests::test_round_trip` and
`connector::query_tag`'s own 6 unit tests.

## 7. What's still open

- **Strict-policy live check** (the one Phase C item not yet implemented): a live-Postgres
  test proving `PushdownPolicy::Strict` keeps a predicate in Arrow but still returns
  exactly the same rows `always` would — i.e. the paranoid mode is *correct*, not just
  quiet. `pg_pushdown.rs` currently only differentials `always` vs `never`.
- **`main.rs` module duplication (resolved)**: `main.rs` now uses `use rust_ballista_extraction_layer::...`
  imports instead of re-declaring `mod` blocks, so the lib crate and bin crate share the same
  compiled code. Unit tests run once under the lib target (129 tests); `cargo test --bins`
  runs the integration/e2e tests only.
- **Coverage gate (optional, not started)**: `cargo-tarpaulin` with a ratchet (fail only on
  a coverage *drop*, never an absolute threshold).
- **Property/`proptest` suites and hostile-value differential at scale**: the roadmap's
  full vision; the current hostile fixture + `pg_pushdown.rs` differential is the seeded,
  deterministic core of it, not the full property-based version.
- **Multi-process scheduler/worker integration runs**: still manual/out of scope — the
  wire format is exercised in-process by `pg_distributed.rs`/`e2e.rs` and unit-tested by
  `distributed::plan_codec`/`table_codec`.

## 8. Exit criteria

- [x] Every unit-testable pure-logic path has a test; `cargo test --lib` covers it in
      milliseconds, no database.
- [x] Every historical live bug (TEXT[] decode, timestamptz timezone, `relid` stats SQL,
      unbound cursor placeholders, EXPLAIN JSON decode type mismatch) has a named
      regression test that would fail if its fix were reverted.
- [x] No test anywhere skips for lack of a database or cluster; a broken harness fails
      loudly.
- [x] CI runs unit + integration + e2e on every push/PR with no manual setup.
- [ ] Strict-policy live check (§7).
- [ ] `main.rs`/lib crate de-duplication (§7, hygiene only — doesn't block correctness).
