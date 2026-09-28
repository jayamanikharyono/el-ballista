# Phase 1 Implementation Plan — Single-Node PostgreSQL Extractor

> Archived planning document (September 2026). Not a description of the current code — see [../roadmap.md](../roadmap.md) and [../architecture.md](../architecture.md).

Status reference: [roadmap.md](../roadmap.md#phase-1--single-node-postgresql-extraction). This plan
takes the current code in `../../src` (a single-binary proof of concept: schema read → hardcoded window
query → Arrow decode → optional `TableProvider` registration) to the Phase 1 exit criteria:

> A real table extracts incrementally on a schedule for two weeks without intervention. The
> differential correctness suite passes against the hostile-value fixture. A killed process
> mid-run resumes without duplicating or losing a row. Throughput is measured and recorded.

It is ordered so each milestone leaves the tree in a working, demoable state — no milestone
depends on a later one being finished.

---

## 0. Where we are today

What exists and works: `PostgresSchemaReader` (catalog-based schema resolution),
`ArrowTypeMapper`/`PostgresRowAdapter` (decode into Arrow, no intermediate row struct — matches
the connector doc's core rule), a hand-rolled incremental `WHERE col > $1` query, and a
`PostgresTableProvider` / `PostgresExecutionPlan` that register with DataFusion.

What's missing, in the order this plan addresses it: configuration and connection hygiene, a real
checkpoint store, the safe high watermark, window correctness (safety lag, boundary ties, `(lo,
hi]` semantics), COPY-based bulk extraction, actual pushdown of projection/filters/limit into the
scan, a Parquet sink, a CLI, observability, and the differential test suite. Aggregate/join
pushdown, the cost model, and MySQL are explicitly Phase 2/3 — out of scope here.

---

## 1. Crate shape: stay single-crate for Phase 1

`../architecture.md` specifies a nine-crate workspace. Splitting it now, before there is working
end-to-end behavior, is the "scope creep" failure mode the roadmap warns about — premature crate
boundaries without a second connector to validate the SPI just add ceremony. Recommendation:
keep one binary crate through Phase 1, but organize modules so the Phase-3 split is a mechanical
`cargo new` + move, not a redesign:

```
src/
├── config/          # extract.toml parsing, DSN resolution           (new, §2)
├── connector/
│   ├── traits.rs    # Source / SourceTable SPI, real shape this time (new, §2 — replaces extractor/traits.rs)
│   └── postgres/    # existing extractor/postgres/* moves here, mostly unchanged
├── checkpoint/       # CheckpointStore trait + Postgres impl          (new, §4)
├── incremental/       # watermark resolution, window construction     (new, §5)
├── sink/             # Parquet writer + object store                 (new, §7)
├── cli/               # `rel run|plan|checkpoint`                     (new, §8)
└── main.rs
```

Rename `src/extractor/` → `../../src/connector` while doing this (see §3) so module names match the
vocabulary the docs already use (`Source`, `SourceTable`, `ScanPlan`) — this repo currently calls
that layer "extractor," which will get confusing once a checkpoint-driven "incremental extractor"
concept also exists.

---

## 2. Configuration and connection hygiene

**Why first:** every later milestone (checkpoint store DSN, pushdown policy, sink URI) reads from
config, and `postgres.md` §6 treats session hygiene as non-negotiable — building more extraction
logic on top of a connection that skips it just means retrofitting it later under time pressure.

- [ ] `config/mod.rs`: parse the `extract.toml` shape from `../architecture.md` §6 (`[sources.*]`,
      `[checkpoints]`, `[sinks.*]`) with `serde` + `toml`. DSNs resolved from environment variables
      named by `dsn_env`, never inlined.
- [ ] `connector/postgres/connection.rs`: a connect function that, on every new connection, issues
      the session-hygiene block from `postgres.md` §6 (`application_name`, `TIME ZONE UTC`,
      `statement_timeout`, `idle_in_transaction_session_timeout`, `lock_timeout`,
      `default_transaction_read_only`, `jit = off`). Wire this through `sqlx::PgPoolOptions`
      via `after_connect`.
- [ ] `pool_max`, `statement_timeout` become config fields, not hardcoded.
- [ ] Replace `main.rs`'s hardcoded `"localhost", 5432, "postgres", "postgres", "app"` with
      `ExtractConfig::from_file("extract.toml")`.

**Acceptance:** connecting with a bad `statement_timeout` config value fails fast at startup;
`pg_stat_activity.application_name` shows the job id for every connection opened by the tool.

---

## 3. Connector SPI: replace the stub traits

`src/extractor/traits.rs` currently declares generic traits nothing implements. Replace with the
real SPI from `../connectors/README.md` §1, scoped to what Phase 1 needs (drop `estimate`,
`consistent_snapshot` fields that aren't used until parallel scan/cost model land in Phase 2, but
keep the trait shapes so adding them later isn't a breaking change):

- [ ] `connector/traits.rs`: `Source`, `SourceTable`, `ScanPlan`, `SourceCapabilities` (trimmed:
      `filter_pushdown`, `projection_pushdown`, `limit_pushdown`, `incremental`, `bulk_export` —
      the fields Phase 1 actually branches on).
- [ ] `PostgresExtractor` + `PostgresTableProvider` become the `impl Source for PostgresSource` /
      `impl SourceTable for PostgresTable` — collapsing today's two parallel code paths (the
      direct `extract_incremental` call in `main.rs` and the `TableProvider` path) into one, so
      there's a single place that knows how to build and run a scan.
- [ ] Delete the unused generic `Extractor<T>`/`RowAdapter<T>`/`ArrowTypeMapper<T>` traits once
      the concrete structs are wired to the real SPI.

**Acceptance:** `main.rs` has exactly one code path to read a table, not two.

---

## 4. Checkpoint store

The mechanism that makes extraction *incremental* rather than "run a query with a hardcoded date."
Implements `../incremental-extraction.md` §6.

- [ ] `checkpoint/store.rs`: `CheckpointStore` trait (`acquire`, `commit`, `abandon`, `history`) —
      exact signatures from the doc.
- [ ] `checkpoint/postgres_store.rs`: Postgres-backed implementation against the
      `rel_checkpoint` / `rel_run_history` schema in the doc. Reuse the same `PgPool` machinery
      from §2/§3 — this is a second logical database role (metadata store) even if it's physically
      the same server in dev.
- [ ] Lease semantics: `acquire` does a compare-and-swap on `(run_id, lease_expires_at)`; a run
      that can't acquire the lease exits immediately rather than racing another run.
- [ ] `checkpoint/local_store.rs`: file-backed implementation for local dev/testing, so the
      differential test suite (§10) doesn't need a live metadata Postgres.
- [ ] `watermark_value` stored as typed JSON (`{"ts": "..."}`), not a bare string — this is called
      out explicitly in the doc as a correctness requirement, not a style preference.

**Acceptance:** two concurrent `rel run` invocations against the same job — one loses the lease
and exits cleanly; killing a run mid-extraction leaves `state = RUNNING` with an expired lease that
the next invocation can reclaim.

---

## 5. Incremental correctness: watermark and window construction

This is `../incremental-extraction.md` in full — the part of the project that is actually the point.
Replace `execution_plan.rs`'s hardcoded `"updated_at"` / `now() - 30 days` with real window
resolution.

- [ ] `incremental/watermark.rs`: `WatermarkSpec` (mode, column, primary_key, safety_lag,
      max_window) parsed from job config.
- [ ] Implement the Postgres safe high watermark query from `postgres.md` §5.2 /
      `../incremental-extraction.md` §3.1 Mitigation 2 (`LEAST(now() - 1s, MIN(xact_start))` over
      `pg_stat_activity`). Verify `pg_read_all_stats` at startup; fall back to
      `now() - safety_lag` with a logged warning if the privilege is missing — implement this
      fallback now, don't leave it as a silent gap.
- [ ] Mitigation 3 (clamp to observed `max(updated_at)` after the read) — cheap to add now, avoids
      an easy-to-miss bug later.
- [ ] §3.2 boundary handling: truncate `hi` down to the watermark column's actual timestamp
      precision; add the composite `(updated_at, pk) > (:lo_ts, :lo_pk)` keyset rewrite in
      `query_builder.rs` (today's query builder already knows the primary key isn't used at all —
      this needs a `primary_key` field threaded in from config).
- [ ] `max_window` catch-up chunking (§4): a run that's far behind processes one bounded chunk and
      exits successfully rather than issuing one huge window.
- [ ] Wire the resolved `(lo, hi]` into `query_builder.rs` in place of the single hardcoded
      checkpoint parameter it takes today.

**Acceptance:** simulate an open transaction that writes an old-stamped row, confirm the safe
watermark holds `hi` back until it commits, and confirm the row is picked up on the next run
instead of being silently skipped — this is the §3.1 scenario from the doc, and it should be an
actual test, not a read-through.

---

## 6. Bulk and incremental scan paths (COPY vs. portal)

`postgres.md` §2 specifies two extraction paths. Today there's one (`fetch_all` via the extended
protocol, no streaming, whole result set materialized in memory — which also contradicts
`../architecture.md` §5's backpressure argument, since nothing streams yet).

- [ ] Switch the default incremental path to `query_raw` (streaming portal) instead of
      `fetch_all`, and make `PostgresExecutionPlan::execute` yield multiple `RecordBatch`es of
      `batch_size` (default 8192) as they fill, instead of collecting all rows into one batch —
      this is the change that actually makes backpressure real.
- [ ] Add the binary `COPY` path for backfills / large windows (estimated rows > `copy_threshold`,
      default 1,000,000), per §2.1. This needs its own binary-format decoder — the wire format
      documented in §2.1 (`PGCOPY\n\377\r\n\0` header, per-tuple field count and lengths) — reusing
      the same per-column Arrow builders as the portal path so there's one decode target, not two.
- [ ] Request binary result format on both paths (`sqlx` uses binary already for typed columns via
      `try_get`; confirm `numeric`/array decoding still matches §3's wire format notes once COPY is
      added, since COPY binary and extended-protocol binary use the same tuple encoding).

**Acceptance:** a table with >1M matching rows in a window uses COPY; a normal incremental window
uses the portal path; both produce identical Arrow output on the same data (this is checkable with
the same differential harness from §10).

---

## 7. Parquet sink

Currently nonexistent — `main.rs` only calls `df.show()`. Implements the relevant subset of
`sinks.md` (full sink doc wasn't read in this pass; scope here is what the Phase 1 exit criteria
require: local + GCS Parquet, deterministic paths, `_SUCCESS` markers).

- [ ] `sink/parquet.rs`: write a `SendableRecordBatchStream` to Parquet using DataFusion's own
      `DataFrame::write_parquet` or the `parquet` crate directly, partitioned by `_extracted_date`.
- [ ] Append the four metadata columns from `../architecture.md` §4 (`_extracted_at`,
      `_extracted_date`, `_source`, `_watermark_hi`) to every batch before it reaches the sink —
      this belongs in the scan/adapter layer, not the sink, since the sink shouldn't know about
      watermarks.
- [ ] Deterministic object paths from `../incremental-extraction.md` §5:
      `.../w=<lo_epoch>-<hi_epoch>/part-NNNNN.parquet`.
- [ ] `_SUCCESS` marker written last, naming the expected object count; readers/loaders (out of
      scope for Phase 1 itself, but don't build a sink that makes this impossible later) should be
      able to check for it.
- [ ] Local filesystem `ObjectStore` for dev; GCS via `object_store`'s `gcp` feature for the real
      target.
- [ ] Wire the commit protocol ordering from §5: sink finalization (steps 3–4) must complete before
      `CheckpointStore::commit` (step 5) runs. This is the point of §4 and §7 existing as separate
      milestones — make sure `main.rs`/CLI actually calls them in that order with no shortcut.

**Acceptance:** kill the process after Parquet objects are written but before the checkpoint
commits; rerun; confirm the same objects are deterministically overwritten and the checkpoint
advances exactly once (no duplicate rows in the warehouse, per the roadmap's "killed process
mid-run" exit criterion).

---

## 8. Minimal CLI

Roadmap lists `rel run`, `rel plan`, `rel checkpoint`. Phase 1 needs enough to run on a schedule
unattended — doesn't need `rel plan --explain`'s reasoning output (that's meaningfully Phase 2,
once there's a real policy engine to explain).

- [ ] `cli/mod.rs` with `clap`: `rel run --job <name>` (resolve config → acquire checkpoint lease →
      extract → sink → commit), `rel checkpoint show --job <name>` (read current state via
      `CheckpointStore::history`), `rel checkpoint reset --job <name>` (operator escape hatch).
- [ ] `main.rs` becomes the CLI entry point; today's ad hoc demo code in `main.rs` gets deleted
      once `rel run` covers the same path end-to-end.

**Acceptance:** `rel run --job orders_incremental` run from cron/systemd-timer on a real table
with no manual intervention.

---

## 9. Observability

`../architecture.md` §7's metric list, scoped to what Phase 1 actually produces (drop
`rel_pushdown_decision_total` — no pushdown decisions exist yet):

- [ ] `tracing` spans: one per job run, one per partition scan (single partition in Phase 1, but
      keep the span so Phase 2's parallel scan doesn't need new instrumentation), one per sink
      flush.
- [ ] Metrics via a Prometheus exporter (`metrics` + `metrics-exporter-prometheus`):
      `rel_rows_extracted_total`, `rel_bytes_from_source_total`,
      `rel_source_query_duration_seconds`, `rel_watermark_lag_seconds`,
      `rel_null_coerced_total`, `rel_checkpoint_commit_total`.
- [ ] `rel_watermark_lag_seconds` specifically — the doc calls this the one metric worth a standing
      alert; make sure it's derived from the checkpoint store's committed watermark vs. wall clock,
      not from anything in-memory that resets on restart.

**Acceptance:** `rel_watermark_lag_seconds` visibly increases if the scheduler stops triggering the
job, and drops back down once it resumes.

---

## 10. Differential correctness suite

`../pushdown.md` §6 calls this "the single highest-value test in the project," and the roadmap makes
it an explicit exit criterion. Phase 1 scope is narrower than the full doc (no pushdown policy
comparison yet, since there's no cost model) — but the hostile-value fixture and the decode-level
correctness checks apply now, against plain vs. no-pushdown-equivalent reads.

- [ ] Seed fixture: a Postgres table (via `testcontainers` or a docker-compose dev DB) covering
      every type in `postgres.md` §3's mapping table, plus the hostile values it calls out (NULLs,
      `NaN`/`±infinity` numerics, max/min integers, high-precision decimals, empty strings, a
      `timestamp` and a `timestamptz` column side by side, a `text[]` column, an unsupported array
      element type to confirm it errors rather than silently corrupting).
  - Note: this fixture would have caught both bugs just fixed (the missing
    `timestamp without time zone` decode arm and the array schema/decode mismatch) — building it
    now is partly paying down the debt that let those ship.
- [ ] Round-trip test: extract → decode → compare every value against the source, byte-for-byte
      for strings/binary, exact for decimals (the `numeric` wire-format reconstruction §3 flags as
      needing this specifically).
- [ ] COPY-path vs. portal-path equivalence test (once §6 lands both): same window, same table,
      identical Arrow output.
- [ ] Commit-skew scenario test from §5's acceptance criterion, promoted into this suite as a
      permanent regression test, not a one-off manual check.

**Acceptance:** suite runs in CI (or at minimum, as a documented local `cargo test -- --ignored`
against a disposable Postgres) and passes against the hostile fixture.

---

## Suggested sequencing

```
§2 config/hygiene ──► §3 SPI cleanup ──► §4 checkpoint store ──► §5 watermark/window
                                                                        │
                                                                        ▼
§9 observability  ◄── threaded in throughout, not a final pass    §6 COPY/portal streaming
        ▲                                                              │
        │                                                              ▼
        └────────────────────────────────────────────────────── §7 Parquet sink ──► §8 CLI ──► §10 test suite
```

§2–§5 are the core correctness work and should land first — they're also what the June review
flagged as the most consequential gaps. §6 (streaming/COPY) and §7 (sink) can proceed in parallel
once §5 gives them a real `(lo, hi]` window to consume. §8 (CLI) is mostly plumbing once §4–§7
exist. §10 should be written incrementally alongside §5–§6 rather than saved entirely for the end
— the hostile-value fixture is cheap to seed early and expensive to retrofit.

## Explicitly not in this plan

Aggregate/join pushdown, the cost model and `EXPLAIN`-based estimation, parallel scan (`ctid`
ranges, exported snapshots), MySQL, BigQuery load orchestration, and `rel plan --explain` — all
Phase 2/3 per the roadmap. Building any of these now is the scope-creep failure mode the roadmap
warns about explicitly.
