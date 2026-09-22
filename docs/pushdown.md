# Source-Aware Pushdown

This document specifies how an operator is translated to source SQL, how the planner decides whether pushing it down is appropriate, and how semantic correctness is preserved.

The extraction layer is source-aware: operations run in the source database or in DataFusion based on connector capabilities, statistics, and policy. Results are exposed as Arrow `RecordBatch`es.

---

## 1. The three questions

Every candidate operator passes through three gates, in this order. Order matters: a cheap
operator that changes results is still a bug.

```
   Operator (filter / projection / limit / aggregate / sort)
        │
        ▼
  ┌────────────────────────────────────────┐
  │ 1. EXPRESSIBLE?                        │  Can the connector emit valid SQL for it?
  │    → no: keep in Arrow                 │
  └───────────────┬────────────────────────┘
                  ▼
  ┌────────────────────────────────────────┐
  │ 2. SEMANTICALLY IDENTICAL?             │  Same result as Arrow, for every input,
  │    → yes: Exact                        │  including NULLs, collations, overflow, NaN?
  │    → approximately: Inexact            │
  │    → no: keep in Arrow                 │
  └───────────────┬────────────────────────┘
                  ▼
  ┌────────────────────────────────────────┐
  │ 3. WORTH IT?                           │  Cost model + policy: does it save more than
  │    → yes: push                         │  it costs the source?
  │    → no:  keep in Arrow                │
  └────────────────────────────────────────┘
```

Gate 2 maps directly onto DataFusion's `TableProviderFilterPushDown` enum:

- **`Exact`** — the source guarantees it omits *only* rows failing the predicate. DataFusion adds
  no `FilterExec`.
- **`Inexact`** — the source reduces rows but may return some that fail the predicate. DataFusion
  keeps a `FilterExec` above the scan to re-check. Correct, just less efficient.
- **`Unsupported`** — not pushed; DataFusion filters everything itself.

`Inexact` is the safety valve, and we reach for it whenever we are not *certain* the semantics
match. One consequence to keep in mind: when any pushed filter is `Inexact`, DataFusion cannot push
the `LIMIT` down, because an inexact filter does not guarantee that every non-matching row was
removed, so a source-side `LIMIT n` could leave fewer than `n` valid rows.

---

## 2. Expression translation

`pushdown::translate` walks the DataFusion `Expr` tree and attempts to render it into a dialect-specific
`Predicate` IR with bound parameters. Translation is **allowlist-based**: an expression node we do
not explicitly recognize is not pushed. A denylist would mean every new DataFusion release could
silently start pushing something we have never validated.

```rust
/// Per-connector dialect: quoting, placeholders, and fidelity rules
/// (`src/pushdown/dialect.rs`; implemented per backend)
pub trait SqlDialect: Send + Sync {
    fn quote_ident(&self, name: &str) -> String;
    fn placeholder(&self, param_index: usize) -> String;   // "$1" for Postgres
    fn column_literal_fidelity(
        &self,
        column: &ColumnMetadata,
        literal_is_text: bool,
        literal_is_float: bool,
    ) -> Fidelity; // Exact | Inexact
    fn column_column_fidelity(
        &self,
        left_column: &ColumnMetadata,
        right_column: &ColumnMetadata,
    ) -> Fidelity;
}
```

Parameters are always bound, never interpolated. This is not only about injection — bound
parameters let Postgres reuse prepared plans and keep literal formatting (timestamps, decimals,
byte strings) out of our hands.

### Currently in the allowlist

| Category | Pushed | Notes |
| --- | --- | --- |
| Column reference | yes | Quoted per dialect |
| Literals: int (8/16/32/64-bit, unsigned ≤32-bit), float, bool, utf8 | yes | Bound as typed parameters |
| Literals: timestamp-microsecond | yes | Bound as typed parameter |
| Literals: date, decimal, binary, NULL, anything else | **no** | `translate_literal` returns `None` → kept in Arrow |
| Comparison: `= != < <= > >=` | yes | Fidelity depends on column type — see §3 |
| `AND` / `OR` / `NOT` | yes | Fidelity is the *minimum* of the children's |
| `IS NULL` / `IS NOT NULL` | yes | Exact in both dialects |
| `IN (list)`, `BETWEEN`, `LIKE`, `CAST`, arithmetic `+ - * /` | **no** | `translate` returns `None` (deliberate for arithmetic — see §3.4); kept in Arrow. The `Predicate::Cast` variant exists but is only ever produced by internal enum-label normalization, never from user predicates |
| Regex, JSON path, string functions | **no** | Kept in Arrow; usually much faster there anyway |
| UDFs | **no** | By definition not expressible in the source |

Aggregate and join pushdown are deliberately **out of scope for Phase 1**. They are the highest-risk
translations (grouping semantics, NULL handling in `COUNT`, join-side null padding, and a very real
chance of generating a query that flattens a production database), and they deliver the least value
for extraction workloads, which are dominated by selective scans. See [roadmap](roadmap.md).

---

## 3. Where pushdown silently changes results

This section is the reason gate 2 exists. Each item is a real way to get wrong data, along with
the rule we apply.

### 3.1 String collation

A case-insensitive or accent-insensitive collation makes `status = 'PAID'` also match `'paid'`:

```sql
-- case-insensitive collation
SELECT * FROM orders WHERE status = 'PAID';   -- also matches 'paid', 'Paid', 'PÁID'
```

Arrow's string comparison is byte-exact. Push that predicate as `Exact` and DataFusion drops its
own filter — you have just silently loosened the query and included rows the user did not ask for.

**Rule:** for any string comparison, the connector inspects the column's collation.

| Situation | Fidelity |
| --- | --- |
| Postgres, collation is `C` or `POSIX` | `Exact` |
| Postgres, any other deterministic collation, `=` only | `Exact` (equality under deterministic collations is byte equality) |
| Postgres, non-`C` collation, ordering comparison (`< >`) | `Inexact` |
| Postgres, `citext` column or non-deterministic collation | `Inexact` |
| Unknown / unresolvable collation | `Inexact` |

`Inexact` here is not a performance loss worth worrying about: the database still does the work of
reducing the rows, DataFusion just re-checks the survivors, which is a vectorized pass over a small
batch.

**Enum columns.** Postgres has no `enum = text` operator, so a text literal against a true enum
column (detected via `pg_enum`, cached per provider) is rewritten to a label comparison
(`"status"::text = 'PAID'`) and forced `Inexact` — label equality *is* enum equality, so the
cast only narrows. A non-text literal against an enum has no pushable form and stays in Arrow
(an integer literal alone would look `Exact` and then fail at execution with `42883`).
`citext` is deliberately excluded from this rewrite: its native case-insensitive operator is
correct as pushed, and recasting to text would narrow case-sensitively, dropping rows Arrow
would keep.

### 3.2 NULL semantics

SQL `WHERE` returns only rows where the predicate is `TRUE`; `NULL` and `FALSE` are both dropped.
DataFusion's filter has the same three-valued semantics. These agree, so simple predicates are safe
— but `NOT (a = b)` where `a` is NULL is `NULL` in both, and `a NOT IN (1, NULL)` is `NULL`
(never true) in both. The trap is not the semantics; it is any rewrite we perform during
translation. **Rule:** never rewrite a predicate into a "logically equivalent" form during
translation unless the equivalence holds under three-valued logic. Specifically, do not turn
`NOT (a = b)` into `a != b`.

### 3.3 Numeric type width and precision

- Postgres `NUMERIC` has effectively unbounded precision. If we map it to `Decimal128(38, s)` and a
  row exceeds that, the *scan* fails — but a *pushed comparison* against a literal that exceeds
  our declared precision would be evaluated at full precision in the database and at truncated
  precision in Arrow. **Rule:** comparisons on `NUMERIC` columns whose declared precision exceeds
  38 digits are `Unsupported`, not `Inexact`, because the scan itself is already in trouble.
- Float comparison against `NaN` differs: Postgres orders `NaN` as greater than all other values;
  IEEE-754 (and Arrow kernels) treat comparisons with `NaN` as false. **Rule:** any comparison on a
  float column is `Inexact`.

### 3.4 Arithmetic and overflow

`amount * 100` overflows into an error in Postgres (`integer out of range`) but may wrap or produce
a different result in Arrow depending on the kernel. **Rule:** arithmetic inside a
pushed predicate is **not pushed** — `translate` returns `None` for arithmetic operators, so they
stay in Arrow where semantics are under our control. Arithmetic in a *projection* is also not pushed.

### 3.5 Timestamps and time zones

Postgres `timestamptz` is stored as UTC and rendered per the session `TimeZone` setting; `timestamp`
has no zone at all.

**Rule:** every connection sets its session time zone to UTC explicitly at connect time
(`SET TIME ZONE 'UTC'` / `SET time_zone = '+00:00'`), and timestamp literals are always bound as
UTC. A pipeline whose results depend on the server's default time zone is a pipeline that breaks
when someone changes a server default.

---

## 4. The cost model

Gate 3. Given that pushing an operator is *safe*, is it *better*?

The model estimates two quantities and compares them against a policy budget.

```
  bytes_saved  =  (rows_in − rows_out) × avg_row_bytes_of_projection

  source_cost  =  f(access method, rows scanned, expression cost per row)
```

### 4.1 Inputs

Statistics come from the source, cheaply, and are cached with a TTL (default 15 minutes):

| Input | Postgres |
| --- | --- |
| Row count estimate | `pg_class.reltuples` |
| Column selectivity | `pg_stats` — `n_distinct`, `most_common_vals`/`most_common_freqs`, `histogram_bounds` |
| Index availability | `pg_index` + `pg_class` |
| Plan cost / access method | `EXPLAIN (FORMAT JSON)` — plans without executing |
| Table size | `pg_total_relation_size()` |

`EXPLAIN` is the highest-fidelity signal and the one we lean on for the decisive cases: it tells us
whether the candidate predicate produces an index scan or a sequential scan, and at what estimated
cost. It is one cheap round trip and the result is cached per (table, predicate shape).

### 4.2 The decision

```
                     candidate predicate
                              │
                              ▼
                   estimated selectivity s
                              │
              ┌───────────────┴───────────────┐
              ▼                               ▼
     index available                 no index available
              │                               │
    push  (near-free, huge win)               ▼
                                    ┌──────────────────────┐
                                    │ s < keep_threshold?  │  removes most rows
                                    └──────┬───────────────┘
                                    yes    │    no
                                    ▼      │    ▼
                          seq scan cost    │  filter saves few bytes but
                          vs. bytes saved  │  costs a full-scan CPU pass
                                    │      │    │
                                    ▼      │    ▼
                              push if      │  keep in Arrow
                         source_cost < budget
```

Two guardrails override the arithmetic:

- **Source CPU budget.** A source can declare `max_source_cost`. On a production primary this is
  set low: we would rather move bytes than steal CPU from the application. On a dedicated read
  replica it is set high. This single knob encodes "the DB *can* do it, but it *shouldn't*", which
  is the whole motivating insight.
- **Denylist.** Some expressions are never pushed regardless of estimated cost, because the estimate
  is unreliable. Regex evaluation is the canonical example: `EXPLAIN` costs it as if it were a cheap
  function, and it is not.

### 4.3 Policy modes

| Mode | Behavior | When to use |
| --- | --- | --- |
| `always` | Push everything expressible and safe | Dedicated replica, no production impact |
| `never` | Keep everything in Arrow | Emergency: source is under pressure |
| `cost_based` | The model above (default) | Normal operation |
| `strict` | Push a filter only if every referenced column is indexed, selectivity is below `keep_threshold`, and every literal/column involved is primitive (bool/int/timestamp); never push `LIMIT`; `push` hints ignored, `deny` still applies | Source under pressure |
| `hinted` | Per-column and per-predicate overrides in the job spec | When you know something the stats do not |

Configured per source, overridable per job (JSON job spec):

```json
{
  "pushdown": {
    "policy": "cost_based",
    "max_source_cost": 50000,
    "keep_threshold": 0.30,
    "statistics_ttl_secs": 900,
    "deny": ["status", "nick"],
    "push": ["id"]
  }
}
```

Filtered extraction has no always-pushed predicate: every caller-provided filter goes through
the same fidelity + policy decision. (The watermark special case from the deferred
[ incremental extraction](deferred/incremental-extraction.md) design no longer applies.)

---

## 5. Projection and limit pushdown

Much simpler, and much higher value per line of code than filter pushdown.

**Projection** is pushed unconditionally. DataFusion tells the `TableProvider` exactly which column
indices the plan needs, and narrowing `SELECT *` to four columns on a 27-column table is often a
larger win than any filter — it reduces bytes on the wire, decode CPU, and Arrow memory
simultaneously.

**Limit** is pushed when no `Inexact` filter is present (per DataFusion's rule) and no sort is
being applied at the source. A `LIMIT` without `ORDER BY` returns an arbitrary subset, which is
fine for sampling but must never be relied on for deterministic extraction.

---

## 6. Verification strategy

Pushdown bugs return *plausible* wrong answers, so testing needs to be differential rather than
example-based.

1. **Differential correctness tests (implemented, `tests/pg_pushdown.rs`).** For each dialect, run every allowlisted predicate against a
   seeded table with the full hostile-value set (NULLs, empty strings, mixed case, accents, `NaN`,
   `±infinity`, zero dates, max/min integers, high-precision decimals) both with pushdown enabled
   and with `policy = "never"`. The two result sets must be identical. This test catches every
   hazard in §3 and is the single highest-value test in the project.
2. **Property tests (planned).** Generate random predicate trees with `proptest`, then assert the same
   equality. Random trees find the `NOT`/`OR`/NULL interactions that hand-written tests miss.
3. **Plan snapshot tests.** Assert the *decisions*, not just the results, so a stats or cost-model
   change that quietly disables all pushdown shows up as a diff.
4. **Fidelity assertion (planned).** In debug builds, every predicate marked `Exact` is re-evaluated in Arrow
   and the row count compared. A mismatch panics. This turns a silent data bug into a loud test
   failure.
