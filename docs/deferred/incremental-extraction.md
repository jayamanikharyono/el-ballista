# Incremental Extraction (DEFERRED — out of scope)

> **Status:** this design is intentionally NOT implemented. The extraction layer
> covers full/filtered extraction, partitioning, split checkpointing, and
> DataFusion/Ballista execution; watermark management, incremental state,
> backfill orchestration, and CDC live in the orchestrator and are expressed as
> caller-provided filter predicates. This document is kept for reference only.

This document specifies the incremental extraction modes, the checkpoint store, the commit protocol, and the failure modes for watermark-based extraction.

The extraction layer is source-aware and outputs native Arrow on DataFusion/Ballista; incremental extraction is one of the extraction patterns it supports.

---

## 1. Watermark modes

| Mode | Predicate | Detects updates | Detects deletes | Requires | Status |
| --- | --- | --- | --- | --- | --- |
| `append_id` | `id > :lo AND id <= :hi` | no | no | Monotonic, gapless-enough surrogate key | Not implemented |
| `timestamp` | `updated_at > :lo AND updated_at <= :hi` | yes | no | An `updated_at` maintained on every write | **Implemented** |
| `snapshot` | none — full table read | yes | yes | Tolerance for reading the whole table | Not implemented as a mode (full loads go through `extract_full_table` / the `full_extraction` example, without checkpointing) |
| `log` (future) | LSN / GTID range | yes | **yes** | Logical replication or binlog access | Roadmap |

`timestamp` is the default and the mode most of this document is about. `append_id` is strictly
safer where it applies (immutable event tables) because integer sequences do not have the
commit-ordering problem described in §3.1 — though they do have their own gap problem, covered in
§3.6 (design only, like the rest of that section).

The declaration in a job spec (JSON — there is no YAML/TOML spec format):

```json
"incremental": {
  "column": "updated_at",
  "safety_lag_secs": 300,
  "max_window_secs": 21600
}
```

There is no `mode`, `primary_key`, or `overlap` field: only `timestamp` mode exists, there is no
composite keyset tiebreaker, and no deliberate re-read. §3.2 below is the design for those, not
the implementation.

---

## 2. Window construction

Each run computes a half-open-on-the-low-side window `(lo, hi]` and pushes it into the source query.

```
     previous checkpoint            candidate high watermark
              lo                              hi
              │                                │
   ───────────┼────────────────────────────────┼────────────►  time
              │◄──────────  this run  ─────────┤
```

```sql
SELECT <projection>
FROM   <table>
WHERE  updated_at >  :lo
  AND  updated_at <= :hi
  AND  <other pushed predicates>
```

The low bound is strict and the high bound is inclusive, consistently, so consecutive windows
neither overlap nor gap. On a job's first run there is no checkpoint, so `lo` is the epoch
(`1970-01-01T00:00:00Z`) and the run walks forward in `max_window` chunks from there (see §4).
Choosing `hi` correctly is the entire problem.

---

## 3. How this loses data, and what we do about it

### 3.1 Commit-time vs. `updated_at` skew — the primary hazard

`updated_at` records when a row was *written*. The row becomes *visible* to other sessions when its
transaction *commits*. These are not the same instant, and the gap is unbounded.

```
   T1  ── transaction begins, writes order 42 with updated_at = 10:00:00
       │
   T2  ── extraction run: hi = 10:00:10, reads rows ≤ 10:00:10
       │   order 42 is not visible (uncommitted) → not extracted
       │   checkpoint advances to 10:00:10
       │
   T3  ── transaction commits at 10:00:30
       │   order 42 is now visible, but its updated_at is 10:00:00
       │
   T4  ── next run: WHERE updated_at > 10:00:10
           order 42 is never seen again. Silently lost.
```

This is worse than it looks, because Postgres `now()` / `CURRENT_TIMESTAMP` return the
*transaction start* time, so a transaction open for ten minutes stamps every row it writes with a
timestamp ten minutes in the past.

**Mitigation 1 — safety lag (default, always on).** Never advance the watermark to "now". Clamp it:

```
hi = now() − safety_lag
```

`safety_lag` must exceed the longest transaction that writes to the table. Five minutes is a
reasonable default; a table written by long batch jobs needs more. The cost is latency: data is
`safety_lag` stale by construction.

**Mitigation 2 — oldest in-flight transaction bound (Postgres, preferred).** Rather than guessing,
ask the database what the oldest running transaction is and never advance past it:

```sql
SELECT LEAST(
         now() - INTERVAL '1 second',
         COALESCE(MIN(xact_start), now())
       )
FROM pg_stat_activity
WHERE backend_type = 'client backend'
  AND state <> 'idle'
  AND datname = current_database();
```

This is exact rather than heuristic and adapts automatically to load. It requires the connecting
role to see other sessions' `xact_start` (superuser, or membership in `pg_read_all_stats`). When
the role lacks that privilege we fall back to Mitigation 1 and log a loud warning every time the
fallback triggers — because silently downgrading a correctness mechanism is not acceptable.
(A `check_pg_read_all_stats_privilege` helper exists for a once-per-startup check, but no caller
wires it up yet, so today the warning fires per fallback, not once at startup.)

**Mitigation 3 — clamp to observed data.** After reading, set the committed checkpoint to
`min(hi, max(updated_at) observed)`. This prevents the watermark from racing ahead of real data
during idle periods, so a burst of late-committing rows still falls inside the next window.

**Mitigation 4 — log-based capture (future).** LSN and GTID are assigned in commit order by
construction, which eliminates the hazard entirely rather than bounding it. This is the real fix,
and it is why `log` mode is on the roadmap.

### 3.2 Boundary ties at coarse timestamp precision

> **Design only — none of this section is implemented.** Windows are always plain
> `(lo, hi]` on the timestamp column: no precision truncation, no composite keyset, no
> `overlap` re-read, no `ORDER BY` tiebreaker. On Postgres `timestamptz` (microseconds)
> ties are rare enough that this has not bitten yet; on a coarse-grained source it would.
> What follows is the spec for when it does.

A `DATETIME` with **zero fractional-second precision** on some sources can hold thousands of rows within a single second. A busy table can
write thousands of rows within a single second. If `hi` lands mid-second, then `updated_at <= hi`
and the next run's `updated_at > hi` split those rows correctly only if the values are truly
comparable at that granularity — which, at one-second resolution, they are not.

**Rule 1:** for `timestamp` mode, `hi` is always truncated *down* to the column's actual precision.
A boundary that cannot fall inside a tied group cannot split one.

**Rule 2:** require a `primary_key` and use a composite keyset when a tie is detected at the
boundary, so an interrupted or split window resumes deterministically:

```sql
WHERE (updated_at, order_id) > (:lo_ts, :lo_pk)
  AND  updated_at <= :hi
ORDER BY updated_at, order_id
```

**Rule 3:** prefer migrating the source column to `DATETIME(6)`. Documented as a recommendation, not
a requirement, since we do not control source schemas.

The `overlap` setting deliberately re-reads the last N seconds of the previous window. It converts
a potential *loss* into a guaranteed *duplicate*, which the sink's `MERGE` deduplicates on primary
key. That is a good trade, and it is the right setting for tables you cannot fix.

### 3.3 Clock skew

If `updated_at` is set by application servers rather than the database, their clocks disagree, and
`now()` on the extraction host is a third clock.

**Rule:** the high watermark is *always* computed from a `SELECT` against the source database, never
from the extractor's local clock. (The second half of the original rule — flagging
application-set timestamps at registration via missing column defaults — is not implemented;
when in doubt, raise `safety_lag_secs` yourself.)

### 3.4 Hard deletes are invisible

No timestamp-based scheme can observe a row that no longer exists. Options, in order of preference:

1. **Soft deletes.** If the source has `deleted_at`, deletion is just an update and everything works.
2. **Log-based capture.** Delete events appear in the WAL/binlog. The real answer, deferred.
3. **Periodic key reconciliation.** On a slow cadence (nightly), read only the primary key column
   for the whole table — cheap, since it is usually an index-only scan — and diff against the keys
   in the warehouse. Emit tombstones for the difference. Designed as a `rel reconcile` job type;
   **not implemented** — there is no reconcile command today.

There is no `rel doctor` command, so nothing currently reports "deletes not captured" for a
`timestamp` table without `deleted_at`. That gap is real and undiscovered-by-tooling: know it
before you rely on it.

### 3.5 Schema drift

> **Design only — no schema comparison happens today.** The connector resolves the schema at
> plan time and proceeds; nothing records a fingerprint, nothing fails a run, and there is no
> `rel migrate`. The policy below is what *should* happen, kept as the spec for when drift
> detection is built.

A column added to the source mid-stream changes the Arrow schema between runs, which downstream
Parquet readers and BigQuery may or may not tolerate.

**Policy:** the connector resolves the schema at plan time and compares it to the schema recorded in
the checkpoint store.

| Change | Action |
| --- | --- |
| Column added | Allowed. New column appears in subsequent files; the sink target is evolved (BigQuery: `ALLOW_FIELD_ADDITION`). |
| Column dropped | Allowed, emitted as all-null to preserve the target schema. Logged. |
| Type widened (`int` → `bigint`, precision increase) | Allowed. |
| Type narrowed or changed incompatibly | **Job fails.** Requires an explicit `rel migrate` acknowledgment. |
| Watermark or primary key column altered | **Job fails.** |

### 3.6 Sequence gaps in `append_id` mode

> **Design only — `append_id` mode does not exist.** There is no `id_safety_gap` setting and
> no sequence-gap handling anywhere in the code. What follows is the hazard analysis for when
> the mode is built.

Sequences that allocate values outside transaction scope (e.g. Postgres sequences) can cause a gap: a
transaction can obtain id 105 and commit *after* one that obtained 106. A run that sets
`hi = 106` then skips 105 forever — the same hazard as §3.1 with the same shape.

**Mitigation:** the same lag idea, expressed in rows rather than time — hold back the high
watermark by `id_safety_gap` (default 0, meaning the mode is only safe out of the box for tables
with short, serialized write transactions), or bound it by the oldest in-flight transaction.

---

## 4. Window sizing and catch-up

An extraction that has been down for two days must not attempt a single window covering two days —
that is a full-table scan with extra steps, and it will time out.

`max_window` caps a single run. When `now() - lo > max_window`, the run processes
`(lo, lo + max_window]` and commits that watermark, leaving the rest behind. The orchestrator's
next trigger picks up the next chunk, and the job walks forward until it catches up. (Backfills
additionally loop over chunks inside one invocation — see §7.)

```
lo ────┬──────┬──────┬──────┬──────► now
       │ run1 │ run2 │ run3 │ run4
       └──────┴──────┴──────┴──────┘
        6h     6h     6h     6h
```

Shrinking time-to-catch-up across runs shows progress; a window that never advances means the
window is too small for the change rate. (The original text referenced a
`rel_watermark_lag_seconds` metric here — it doesn't exist yet; see `architecture.md` §7.)

---

## 5. The commit protocol

Data lands in the sink, *then* the checkpoint advances. There is no distributed transaction between
the sink and the checkpoint store, so we design for at-least-once with idempotent writes rather than
pretending otherwise. (In practice today the "sink" is a local Parquet file or just collected
batches — there are no deterministic window object paths, no `_SUCCESS` markers, and no BigQuery
loader. The protocol below describes the ordering guarantee, which holds regardless.)

```
  1. Resolve lo from checkpoint store              (state: RUNNING, run_id recorded)
  2. Compute hi from the source clock, clamped
  3. Extract (lo, hi] → Arrow → sink
  4. Flush and finalize output
  5. Advance checkpoint to hi (never past what was observed — see Mitigation 3)
                                                   (state: COMMITTED)
```

A crash between steps 4 and 5 leaves the checkpoint at `lo`. The next run re-resolves its window
from that `lo` — note `hi` is *not* reproduced exactly (it is re-read from the source clock, so a
retry normally covers `lo..new-hi`, a superset, not the identical window). At-least-once holds
either way: rows are never skipped, and a rerun may re-extract already-written rows, which is why
sinks must tolerate overwrites once real sink paths exist.

Concurrency is prevented by a lease: a run must acquire the checkpoint (RUNNING + fresh `run_id`
+ expiry) before proceeding, and `commit`/`abandon` verify the caller still holds the recorded
`run_id`. A run that loses its lease aborts before writing. Our store is file-backed (one JSON
file per job+namespace, atomic rename), not a CAS row — same protocol, weaker concurrency
guarantee, sufficient for one scheduler triggering one job at a time.

---

## 6. Checkpoint store

One implementation exists: `JsonCheckpointStore`, one JSON file per `(job_id, namespace)` under
a local directory, written atomically (temp file + rename). The `CheckpointStore` trait behind
it (`acquire` / `commit` / `abandon` / `read`) is the seam a Postgres-backed store would plug
into — but that store does not exist yet, and neither does run history. The real shapes:

```rust
pub struct Checkpoint {
    pub job_id: String,
    pub namespace: String,              // isolates backfills (see §7)
    pub watermark_column: String,
    pub watermark_value: Option<DateTime<Utc>>,  // None = no successful run yet
    pub state: RunState,                // Running | Committed | Failed
    pub run_id: Option<Uuid>,
    pub lease_expires_at: Option<DateTime<Utc>>,
    pub updated_at: DateTime<Utc>,
}

pub struct RunStats {
    pub rows_extracted: u64,
    pub window_lo: Option<DateTime<Utc>>,
    pub window_hi: Option<DateTime<Utc>>,
}
```

Notes against the original Postgres-first design this replaces:

- The watermark is a plain `Option<DateTime<Utc>>`, not typed JSON — timestamp mode is the
  only mode, so there is nothing else to discriminate. A second mode will need the tagged
  representation (or a second column).
- There is no schema fingerprint, no drift detection, and no `history()` method — §3.5's
  table is spec, not implementation.
- The `namespace` field *does* exist and works: a backfill runs under its own namespace with
  its own watermark, without clobbering the live incremental job.

The SQL below is kept as the design for the Postgres-backed store, when the local file stops
being sufficient (multiple schedulers, or history queries worth indexing):

```sql
CREATE TABLE rel_checkpoint (
    job_id            text        NOT NULL,
    source_ref        text        NOT NULL,
    table_name        text        NOT NULL,
    namespace         text        NOT NULL DEFAULT 'default',  -- isolates backfills
    watermark_mode    text        NOT NULL,   -- timestamp | append_id | snapshot | log
    watermark_column  text,
    watermark_value   jsonb       NOT NULL,   -- typed: {"ts":"..."} | {"i64":...} | {"lsn":"..."}
    watermark_pk      jsonb,                  -- composite keyset tiebreaker (§3.2)
    schema_fingerprint text       NOT NULL,   -- drift detection (§3.5)
    state             text        NOT NULL,   -- COMMITTED | RUNNING | FAILED
    run_id            uuid,
    lease_expires_at  timestamptz,
    updated_at        timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (job_id, source_ref, table_name, namespace)
);

CREATE TABLE rel_run_history (
    run_id          uuid PRIMARY KEY,
    job_id          text        NOT NULL,
    window_lo       jsonb,
    window_hi       jsonb,
    rows_extracted  bigint,
    bytes_written   bigint,
    source_millis   bigint,
    pushdown_summary jsonb,      -- what was pushed, for after-the-fact debugging
    state           text        NOT NULL,
    error           text,
    started_at      timestamptz NOT NULL,
    finished_at     timestamptz
);
```

`watermark_value` in the SQL design is typed JSON rather than a string so that a timestamp
watermark cannot be silently compared as text — an ordering bug that produces a
plausible-looking but wrong window. (The JSON store doesn't need this yet: with one mode,
`Option<DateTime<Utc>>` cannot be miscompared.)

The `namespace` column lets a backfill run alongside the live incremental job with its own
watermark, without either clobbering the other — and unlike the rest of this SQL, the
namespace field genuinely exists in the JSON store today.

---

## 7. Backfills

A backfill is the same machinery with an explicit `[from, to]` window instead of one resolved
from the watermark, committed under its own checkpoint namespace:

```bash
cargo run --bin rust-ballista-extraction-layer -- backfill --config <path> --namespace <name> \
    --from <rfc3339> --to <rfc3339>
```

A large range is walked in `max_window_secs`-bounded chunks with one acquire/extract/commit
cycle per chunk, so a crash resumes from the last committed chunk rather than restarting the
whole range. Chunking is strictly sequential — there is no `--chunk`/`--parallel` fan-out and
no `--strategy keyset` primary-key chunking; for very large historical loads, full-table
extraction (see `examples/full_extraction.rs`) is currently the faster path. Sink output, such as
it is, goes wherever the run writes it — there are no window-derived object paths and no
partition layout shared with incremental output yet.
