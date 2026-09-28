# Incremental Extraction (design)

> **Design only — not implemented.** El Ballista keeps no watermark or incremental state and
> has no backfill command. A job spec with an `incremental` or `watermark` block is a load error
> (every config struct rejects unknown fields). This document keeps the design and the hazard
> analysis for the day the layer, or the orchestrator in front of it, needs them.

This document specifies watermark modes, window construction, the ways watermark-based
extraction loses data, the commit protocol and a checkpoint store for it.

---

## Why deferred

- **The orchestrator owns state.** It already schedules runs, retries them and knows which
  ranges have landed. A second copy of that state inside the extraction layer would have to be
  kept consistent with it, and would be the first thing to drift.
- **One clear contract.** The layer takes a job spec and hands out Arrow batches. It does not
  write data, so it cannot know when a window is durably landed; only the consumer can.
- **At-least-once per split.** `run_with(consumer)` records a split as completed only after the
  consumer returns `Ok`, and a retry re-delivers any split that did not complete. That guarantee
  holds for any range the orchestrator asks for, without the layer knowing why it asked.

---

## Incremental today

An incremental or backfill run is a filtered extraction: the orchestrator computes the window
and puts it in the job spec as filters. On the demo database (dvdrental, `public.payment`),
a one-week window (the window of
[`examples/configs/extract.example.json`](../../examples/configs/extract.example.json), with a
window-derived `job_id`):

```json
"job_id": "payment_2007_04_06",
"table": "payment",
"filters": [
  { "column": "payment_date", "op": ">=", "value": "2007-04-06T00:00:00Z" },
  { "column": "payment_date", "op": "<",  "value": "2007-04-13T00:00:00Z" }
]
```

- **The window pushes to Postgres** when an index serves it, or when the column's `pg_stats`
  histogram and most-common values show the window is selective. Both sides of the window are
  estimated together, so a narrow window pushes even without an index; `rel plan` shows the
  decision and its reason. Details: [`pushdown.md`](../pushdown.md).
- **Use one `job_id` per window.** The checkpoint fingerprint includes the filters, so running
  the same `job_id` with a new window fails with `PlanMismatch`, and rerunning a completed
  window does nothing. A window-derived id (as above) keeps each window's split checkpoints
  separate and makes a retry of that window resume where it stopped.
- **Choosing the upper bound is the orchestrator's job.** The hazards in §3 apply unchanged:
  compute `hi` from the source clock, minus a safety lag, and never from the orchestrator's
  local clock.

---

## 1. Watermark modes

| Mode | Predicate | Detects updates | Detects deletes | Requires | Status |
| --- | --- | --- | --- | --- | --- |
| `append_id` | `id > :lo AND id <= :hi` | no | no | Monotonic, gapless-enough surrogate key | Not implemented |
| `timestamp` | `updated_at > :lo AND updated_at <= :hi` | yes | no | An `updated_at` maintained on every write | Not implemented |
| `snapshot` | none — full table read | yes | yes | Tolerance for reading the whole table | Not implemented as a mode (a full load is a job without filters) |
| `log` | LSN / GTID range | yes | yes | Logical replication or binlog access | Not implemented |

`timestamp` would be the default and is the mode most of this document is about. `append_id` is
safer where it applies (immutable event tables) because integer sequences do not have the
commit-ordering problem of §3.1, though they have their own gap problem (§3.6).

A job-spec declaration would look like this (a sketch; the config parser rejects it today):

```json
"incremental": {
  "column": "updated_at",
  "safety_lag_secs": 300,
  "max_window_secs": 21600
}
```

---

## 2. Window construction

Each run computes a window `(lo, hi]`, open on the low side, and pushes it into the source query.

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
(`1970-01-01T00:00:00Z`) and the run walks forward in `max_window` chunks from there (§4).
Choosing `hi` correctly is the entire problem.

---

## 3. How this loses data, and what to do about it

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

**Mitigation 1 — safety lag (always on).** Never advance the watermark to "now". Clamp it:

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

This is exact rather than heuristic and adapts to load. It requires the connecting role to see
other sessions' `xact_start` (superuser, or membership in `pg_read_all_stats`). Without that
privilege the design falls back to Mitigation 1 and logs a loud warning each time, because
silently downgrading a correctness mechanism is not acceptable.

**Mitigation 3 — clamp to observed data.** After reading, set the committed checkpoint to
`min(hi, max(updated_at) observed)`. This keeps the watermark from racing ahead of real data
during idle periods, so a burst of late-committing rows still falls inside the next window.

**Mitigation 4 — log-based capture.** LSN and GTID are assigned in commit order by construction,
which removes the hazard rather than bounding it. This is the real fix, and the reason `log`
mode is in the table above.

### 3.2 Boundary ties at coarse timestamp precision

On Postgres `timestamptz` (microseconds) ties at a window boundary are rare. A `DATETIME` with
zero fractional-second precision on other sources can hold thousands of rows within a single
second. If `hi` lands mid-second, then `updated_at <= hi` and the next run's `updated_at > hi`
split those rows correctly only if the values are truly comparable at that granularity, which at
one-second resolution they are not.

**Rule 1:** in `timestamp` mode, `hi` is always truncated *down* to the column's actual precision.
A boundary that cannot fall inside a tied group cannot split one.

**Rule 2:** require a `primary_key` and use a composite keyset when a tie is detected at the
boundary, so an interrupted or split window resumes deterministically:

```sql
WHERE (updated_at, order_id) > (:lo_ts, :lo_pk)
  AND  updated_at <= :hi
ORDER BY updated_at, order_id
```

**Rule 3:** prefer migrating the source column to `DATETIME(6)`. A recommendation, not a
requirement, since the source schema is not ours to control.

An `overlap` setting would deliberately re-read the last N seconds of the previous window. It
turns a potential *loss* into a guaranteed *duplicate*, which a downstream `MERGE` on primary key
removes. That is a good trade, and the right setting for tables that cannot be fixed.

### 3.3 Clock skew

If `updated_at` is set by application servers rather than the database, their clocks disagree, and
`now()` on the extraction host is a third clock.

**Rule:** the high watermark is *always* computed from a `SELECT` against the source database,
never from the extractor's or orchestrator's local clock. Application-set timestamps (no column
default) are a reason to raise `safety_lag`.

### 3.4 Hard deletes are invisible

No timestamp-based scheme can observe a row that no longer exists. Options, in order of preference:

1. **Soft deletes.** If the source has `deleted_at`, deletion is just an update and everything works.
2. **Log-based capture.** Delete events appear in the WAL / binlog. The real answer, deferred.
3. **Periodic key reconciliation.** On a slow cadence (nightly), read only the primary key column
   for the whole table (cheap, usually an index-only scan) and diff against the keys in the
   warehouse; emit tombstones for the difference. With El Ballista this is a projected full
   extraction of the key column; the diff belongs downstream.

A `timestamp` table without `deleted_at` does not capture deletes, and nothing reports that
automatically. Know it before relying on it.

### 3.5 Schema drift

A column added to the source mid-stream changes the Arrow schema between runs, which downstream
Parquet readers and warehouses may or may not tolerate. Today the connector resolves the schema at
plan time and proceeds; nothing records or compares a schema fingerprint.

**Policy (design):** compare the schema resolved at plan time with the one recorded in the
checkpoint store.

| Change | Action |
| --- | --- |
| Column added | Allowed. The new column appears in later output; the target is evolved (BigQuery: `ALLOW_FIELD_ADDITION`). |
| Column dropped | Allowed, emitted as all-null to preserve the target schema. Logged. |
| Type widened (`int` → `bigint`, precision increase) | Allowed. |
| Type narrowed or changed incompatibly | Job fails until an operator acknowledges the change. |
| Watermark or primary key column altered | Job fails. |

### 3.6 Sequence gaps in `append_id` mode

Sequences that allocate values outside transaction scope (e.g. Postgres sequences) can cause a gap:
a transaction can obtain id 105 and commit *after* one that obtained 106. A run that sets
`hi = 106` then skips 105 forever — the same hazard as §3.1 with the same shape.

**Mitigation:** the same lag idea, in ids rather than time: hold back the high watermark by an
`id_safety_gap`, or bound it by the oldest in-flight transaction.

---

## 4. Window sizing and catch-up

An extraction that has been down for two days must not attempt a single window covering two days:
that is a full-table scan with extra steps, and it will time out.

`max_window` caps a single run. When `now() - lo > max_window`, the run processes
`(lo, lo + max_window]` and commits that watermark, leaving the rest behind. The next trigger picks
up the next chunk, and the job walks forward until it catches up.

```
lo ────┬──────┬──────┬──────┬──────► now
       │ run1 │ run2 │ run3 │ run4
       └──────┴──────┴──────┴──────┘
        6h     6h     6h     6h
```

Shrinking time-to-catch-up across runs shows progress; a window that never advances means the
window is too small for the change rate. A watermark-lag metric would make that visible; none
exists, since there is no watermark.

---

## 5. The commit protocol

Data lands downstream, *then* the checkpoint advances. There is no distributed transaction between
the destination and the checkpoint store, so the design is at-least-once with idempotent writes
rather than pretending otherwise.

```
  1. Resolve lo from the checkpoint store          (state: RUNNING, run_id recorded)
  2. Compute hi from the source clock, clamped
  3. Extract (lo, hi] → Arrow → consumer
  4. Consumer flushes and finalizes its output
  5. Advance checkpoint to hi (never past what was observed — Mitigation 3)
                                                   (state: COMMITTED)
```

A crash between steps 4 and 5 leaves the checkpoint at `lo`. The next run re-resolves its window
from that `lo`; `hi` is re-read from the source clock, so a retry normally covers `lo..new-hi`, a
superset rather than the identical window. Rows are never skipped, and a rerun may re-extract
rows already written, which is why the destination must tolerate overwrites.

Concurrency is prevented by a lease: a run acquires the checkpoint (RUNNING, a fresh `run_id`, an
expiry) before proceeding, and commit / abandon verify that the caller still holds the recorded
`run_id`. A run that loses its lease aborts before writing. (The split checkpoints that exist
today use the same idea: a per-job lock file with a heartbeat.)

---

## 6. Checkpoint store

A watermark checkpoint would extend today's per-job JSON store with a watermark and a namespace:

```rust
pub struct Checkpoint {
    pub job_id: String,
    pub namespace: String,                       // isolates backfills (§7)
    pub watermark_column: String,
    pub watermark_value: Option<DateTime<Utc>>,  // None = no successful run yet
    pub state: RunState,                         // Running | Committed | Failed
    pub run_id: Option<Uuid>,
    pub lease_expires_at: Option<DateTime<Utc>>,
    pub updated_at: DateTime<Utc>,
}
```

A plain `Option<DateTime<Utc>>` is enough while `timestamp` is the only mode; a second mode needs
a tagged value. The `namespace` lets a backfill run alongside the live incremental job with its
own watermark, without either clobbering the other.

When a local file stops being enough (multiple schedulers, history worth indexing), a
Postgres-backed store:

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

`watermark_value` is typed JSON rather than a string so that a timestamp watermark cannot be
silently compared as text, an ordering bug that produces a plausible-looking but wrong window.

---

## 7. Backfills

A backfill is the same machinery with an explicit `[from, to]` window instead of one resolved from
the watermark, committed under its own checkpoint namespace. A large range is walked in
`max_window`-bounded chunks, one acquire / extract / commit cycle per chunk, so a crash resumes
from the last committed chunk rather than restarting the whole range.

Today a backfill is a series of filtered jobs, one per chunk, each with its own `job_id` (see
"Incremental today"). For very large historical loads, a full extraction with keyset
partitioning is usually the faster path.
