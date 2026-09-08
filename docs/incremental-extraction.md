# Incremental Extraction

Extracting *changed* rows instead of all rows is the operational point of this project. It is also
where the subtle data-loss bugs live. This document specifies the watermark modes, the checkpoint
store, the commit protocol, and — at length — the failure modes that a naive
`WHERE updated_at > :last_run` implementation walks straight into.

---

## 1. Watermark modes

| Mode | Predicate | Detects updates | Detects deletes | Requires |
| --- | --- | --- | --- | --- |
| `append_id` | `id > :lo AND id <= :hi` | no | no | Monotonic, gapless-enough surrogate key |
| `timestamp` | `updated_at > :lo AND updated_at <= :hi` | yes | no | An `updated_at` maintained on every write |
| `snapshot` | none — full table read | yes | yes | Tolerance for reading the whole table |
| `log` (future) | LSN / GTID range | yes | **yes** | Logical replication or binlog access |

`timestamp` is the default and the mode most of this document is about. `append_id` is strictly
safer where it applies (immutable event tables) because integer sequences do not have the
commit-ordering problem described in §3.1 — though they do have their own gap problem, covered in
§3.6.

The declaration in a job spec:

```yaml
incremental:
  mode: timestamp
  column: updated_at
  primary_key: [order_id]     # required: tiebreaker and dedup key
  safety_lag: 5m              # see §3.1
  max_window: 6h              # cap on a single run's window, see §4
  overlap: 0s                 # optional deliberate re-read, see §3.2
```

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
neither overlap nor gap. Choosing `hi` correctly is the entire problem.

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

This is worse in Postgres than it looks, because `now()` / `CURRENT_TIMESTAMP` return the
*transaction start* time, so a transaction open for ten minutes stamps every row it writes with a
timestamp ten minutes in the past. MySQL's `CURRENT_TIMESTAMP` is statement-start time, which
narrows but does not close the gap.

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
the role lacks that privilege we fall back to Mitigation 1 and log a warning at startup, because
silently downgrading a correctness mechanism is not acceptable.

MySQL's closest equivalent is `information_schema.INNODB_TRX.trx_started`, which covers InnoDB
transactions that have acquired a transaction ID. It is a weaker guarantee than the Postgres query
— see [the MySQL connector doc](connectors/mysql.md#5-consistency-and-watermark-anchoring).

**Mitigation 3 — clamp to observed data.** After reading, set the committed checkpoint to
`min(hi, max(updated_at) observed)`. This prevents the watermark from racing ahead of real data
during idle periods, so a burst of late-committing rows still falls inside the next window.

**Mitigation 4 — log-based capture (future).** LSN and GTID are assigned in commit order by
construction, which eliminates the hazard entirely rather than bounding it. This is the real fix,
and it is why `log` mode is on the roadmap.

### 3.2 Boundary ties at coarse timestamp precision

MySQL `DATETIME` and `TIMESTAMP` default to **zero fractional-second precision**. A busy table can
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
from the extractor's local clock. Application-set timestamps are flagged at job registration
(detectable by the absence of a column default or `ON UPDATE` clause) with a recommendation to
increase `safety_lag`, since the extractor cannot bound another host's clock drift.

### 3.4 Hard deletes are invisible

No timestamp-based scheme can observe a row that no longer exists. Options, in order of preference:

1. **Soft deletes.** If the source has `deleted_at`, deletion is just an update and everything works.
2. **Log-based capture.** Delete events appear in the WAL/binlog. The real answer, deferred.
3. **Periodic key reconciliation.** On a slow cadence (nightly), read only the primary key column
   for the whole table — cheap, since it is usually an index-only scan — and diff against the keys
   in the warehouse. Emit tombstones for the difference. This is a documented job type
   (`rel reconcile`) rather than something that happens automatically.

Any table configured with `mode: timestamp` and no `deleted_at` column is reported by
`rel doctor` as "deletes not captured", so this is a known gap rather than a surprise.

### 3.5 Schema drift

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

Postgres sequences and MySQL `AUTO_INCREMENT` allocate values outside transaction scope. A
transaction can obtain id 105 and commit *after* one that obtained 106. A run that sets
`hi = 106` then skips 105 forever — the same hazard as §3.1 with the same shape.

**Mitigation:** the same lag idea, expressed in rows rather than time — hold back the high
watermark by `id_safety_gap` (default 0, meaning the mode is only safe out of the box for tables
with short, serialized write transactions), or bound it by the oldest in-flight transaction.
`rel doctor` warns when `append_id` is used on a table whose writes are not known to be short.

---

## 4. Window sizing and catch-up

An extraction that has been down for two days must not attempt a single window covering two days —
that is a full-table scan with extra steps, and it will time out.

`max_window` caps a single run. When `now() - lo > max_window`, the run processes
`(lo, lo + max_window]` and exits successfully, leaving the watermark behind. The orchestrator's
next trigger picks up the next chunk, and the job walks forward until it catches up.

```
lo ────┬──────┬──────┬──────┬──────► now
       │ run1 │ run2 │ run3 │ run4
       └──────┴──────┴──────┴──────┘
        6h     6h     6h     6h
```

`rel_watermark_lag_seconds` decreasing across runs shows catch-up is progressing; flat or rising
lag means the window is too small for the change rate, which is an alert-worthy condition.

---

## 5. The commit protocol

Data lands in the sink, *then* the checkpoint advances. There is no distributed transaction between
GCS and the checkpoint store, so we design for at-least-once with idempotent writes rather than
pretending otherwise.

```
  1. Resolve lo from checkpoint store              (state: RUNNING, run_id recorded)
  2. Compute hi from the source clock, clamped
  3. Extract (lo, hi] → Arrow → Parquet objects at a DETERMINISTIC path
  4. Flush and finalize all objects
  5. Advance checkpoint to hi                      (state: COMMITTED)
```

The deterministic path is what makes retries safe:

```
gs://warehouse/raw/orders/_extracted_date=2026-09-04/
    w=1757030400-1757034000/part-00000.parquet
                └── lo and hi epoch seconds: identical inputs → identical object names
```

A crash between steps 4 and 5 leaves the checkpoint at `lo`. The next run recomputes the *same*
window — because `lo` is unchanged and `hi` is clamped and reproducible for that `lo` — and
overwrites the same objects. No duplicates reach the warehouse.

A crash *during* step 3 leaves partial objects. Object stores make single-object writes atomic, so
partial objects are whole-but-fewer, and the retry overwrites them. The window directory is only
declared complete when a `_SUCCESS` marker naming the expected object count is written; readers and
the BigQuery loader ignore incomplete window directories.

Concurrency is prevented by a lease: a run must acquire the checkpoint row's lease (a
compare-and-swap on `run_id` plus an expiry) before proceeding. A run that loses its lease — because
it stalled long enough for the lease to expire and another run to start — aborts before writing.

---

## 6. Checkpoint store

Pluggable behind a trait, with a Postgres implementation as the default (it needs
read-modify-write with isolation, which object storage does not give you) and a local-file
implementation for development.

```rust
#[async_trait]
pub trait CheckpointStore: Send + Sync {
    async fn acquire(&self, key: &JobKey, run_id: Uuid, lease: Duration)
        -> Result<Checkpoint>;
    async fn commit(&self, key: &JobKey, run_id: Uuid, next: WatermarkValue, stats: RunStats)
        -> Result<()>;
    async fn abandon(&self, key: &JobKey, run_id: Uuid, err: &Error) -> Result<()>;
    async fn history(&self, key: &JobKey, limit: usize) -> Result<Vec<RunRecord>>;
}
```

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

`watermark_value` is stored as typed JSON rather than a string so that a timestamp watermark cannot
be silently compared as text — an ordering bug that produces a plausible-looking but wrong window.

The `namespace` column lets a backfill run alongside the live incremental job with its own
watermark, without either clobbering the other.

---

## 7. Backfills

A backfill is the same machinery with a bounded window and a separate namespace:

```bash
rel backfill --job orders_incremental \
             --from 2024-01-01 --to 2026-09-01 \
             --chunk 7d --parallel 4 \
             --namespace backfill_2026q3
```

Chunks are planned up front, dispatched with bounded parallelism, and each commits its own
checkpoint, so an interrupted backfill resumes at chunk granularity. Because object paths are
derived from the window bounds, backfill output and incremental output coexist in the same
partition layout, and a chunk that overlaps live data simply overwrites identical content.

For very large historical loads, `--strategy keyset` switches from time chunks to primary-key range
chunks, which is faster when there is no index on the watermark column covering old data — a common
situation, since `updated_at` indexes are often added long after a table is created.
