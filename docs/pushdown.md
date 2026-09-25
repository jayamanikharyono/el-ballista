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
- **`Inexact`** — the source reduces rows but may return some that fail the predicate: its
  result is a **superset** of the correct rows. DataFusion keeps a `FilterExec` above the scan to
  re-check. The re-check can remove rows but never add them back, so a pushed form that could
  return *fewer* rows than Arrow is not `Inexact` — it is not pushed at all.
- **`Unsupported`** — not pushed; DataFusion filters everything itself.

`Inexact` is only a safety valve for "maybe too many rows". When the source might return too
*few*, the predicate stays in Arrow. One consequence to keep in mind: when any pushed filter is `Inexact`, DataFusion cannot push
the `LIMIT` down, because an inexact filter does not guarantee that every non-matching row was
removed, so a source-side `LIMIT n` could leave fewer than `n` valid rows.

---

## 2. Expression translation

`pushdown::translate_with` walks the DataFusion `Expr` tree and renders it into an engine-agnostic
`Predicate` IR with bound parameters. Translation is **allowlist-based**: an expression node we do
not explicitly recognize is not pushed. A denylist would mean every new DataFusion release could
silently start pushing something we have never validated.

Fidelity is decided *during translation*, from the `ColumnKind` the connector assigns each column
from its catalog (Postgres: `connector::postgres::dialect::column_kind`). The IR is rendered
through a per-connector dialect:

```rust
/// Per-connector rendering (`src/pushdown/dialect.rs`; implemented per backend)
pub trait SqlDialect: Send + Sync {
    fn quote_ident(&self, name: &str) -> String;
    fn placeholder(&self, param_index: usize) -> String;           // "$1" for Postgres
    fn cast_type_name(&self, to: CastType) -> &'static str;         // CastType::Text -> "text"
    fn collation_name(&self, collation: Collation) -> &'static str; // Binary -> "\"C\""
}
```

Every composite node renders inside its own parentheses (`("flag" IS NULL)`, `(NOT ...)`,
`CAST(... AS text)`, `(... COLLATE "C")`), so the SQL never depends on the source's operator
precedence (Postgres binds `IS NULL` tighter than `NOT`). Operators, cast targets and collations
are closed enums in the serialized plan, never SQL text.

Parameters are always bound, never interpolated. This is not only about injection — bound
parameters let Postgres reuse prepared plans and keep literal formatting (timestamps, decimals,
byte strings) out of our hands.

### Currently in the allowlist

| Category | Pushed | Notes |
| --- | --- | --- |
| Column reference | yes | Quoted per dialect; must be a known column of the table |
| Literals: int (8/16/32/64-bit, unsigned ≤32-bit), float, bool, utf8 | yes | Bound as typed parameters |
| Literals: timestamp-microsecond | yes | Bound as typed parameter |
| Literals: date, decimal, binary, NULL, anything else | **no** | kept in Arrow |
| Comparison `= <> < <= > >=` against a literal | per column kind | See the table below and §3 |
| Column-to-column comparison | same-kind integer / boolean / timestamp / text only | Text sides both `COLLATE "C"` |
| `AND` / `OR` | yes | Fidelity is the *minimum* of the children's |
| `NOT`, `IS [NOT] NULL` over a predicate | only over an `Exact` child | The negation of a superset is a subset |
| `IS NULL` / `IS NOT NULL` on a column | yes, any known column | `Exact` |
| `IN (list)`, `BETWEEN`, `LIKE`, `CAST`, arithmetic `+ - * /` | **no** | Kept in Arrow (deliberate for arithmetic — see §3.4) |
| Regex, JSON path, string functions | **no** | Kept in Arrow; usually much faster there anyway |
| UDFs | **no** | By definition not expressible in the source |

| Column kind (Postgres types) | Pushed comparisons | Fidelity |
| --- | --- | --- |
| Integer (`smallint`/`integer`/`bigint`), Boolean, Timestamp (`timestamp[tz]`) | all six | `Exact` |
| Text (`text`, `varchar`) | all six, as `("col" COLLATE "C") op $n` | `Exact` |
| Label (true enums from `pg_enum`) | all six, as `(CAST("col" AS text) COLLATE "C") op $n` | `Exact` |
| TextCast (`uuid`, `json`, `jsonb`, other `USER-DEFINED` incl. `citext`) | `=` and `<>` only, as `(CAST("col" AS text) COLLATE "C") op $n` | `Exact` |
| Float (`real`, `double precision`) | `=` only | `Inexact` |
| Date, `character(n)`, numeric, bytea, arrays, other | none (only `IS [NOT] NULL`) | — |

`COLLATE "C"` orders the **server-encoded** bytes, which equals Arrow's UTF-8 byte order only
when `server_encoding = UTF8`. The provider reads `SHOW server_encoding` once; on any other
encoding (or if the lookup fails) Text and Label columns are treated as TextCast — `=` / `<>`
only, since byte equality does not depend on the encoding.

The rules above are also checked by a seeded property-based differential test
(`tests/pg_pushdown_prop.rs`: random nested predicates over ICU / case-insensitive / plain text,
`-0.0`/`NaN`/NULL floats, integer extremes, booleans and an enum, `always` vs `never`).

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

The same hazard hits ordering: under ICU `en-US`, `'B' < 'a'` is false, while Arrow compares bytes
and says true. Pushing `name < 'a'` even as `Inexact` would drop `'B'` — and a re-check cannot
restore it.

**Rule:** every text comparison is rendered with an explicit binary collation —
`("name" COLLATE "C") < $1` on Postgres. `"C"` compares the UTF-8 bytes, which is exactly Rust/
Arrow `str` ordering, so the comparison is `Exact` under every column collation (including
nondeterministic ICU collations and `citext`, whose value is compared through its text form).

The cost: an explicit `COLLATE "C"` can only use an index built with the `C` collation. The
cost model therefore assumes an index serves a text comparison only when the column's own
collation is `C`/`POSIX` (from `information_schema.columns.collation_name`); otherwise it falls back
to selectivity and cost estimates.

**Enum, uuid, json, `citext`.** Postgres has no `enum = text` (or `uuid = text`) operator, so a
text literal is compared against the value's text form: `(CAST("status" AS text) COLLATE "C") =
$1`. That text is what the extractor emits into Arrow, so the comparison is `Exact`. Enum labels
push all six operators (label order, byte-wise — which is what Arrow compares, not the enum's
declaration order). For `uuid`/`json`/`jsonb`/other user-defined types only `=` and `<>` push;
ordering comparisons stay in Arrow. A non-text literal against any of these has no pushable form
and stays in Arrow. (`jsonb` equality is `Exact` because extraction selects `jsonb` as `::text` too, so Arrow
holds exactly the text the source compared — keep the two in sync.)

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
- Floats: Arrow compares with IEEE total order (`-0.0 < +0.0`, `NaN = NaN`); Postgres treats
  `-0 = 0` and orders `NaN` above everything. So `x < 0.0` or `x <> 0.0` pushed to Postgres would
  drop `-0.0` rows. **Rule:** float `=` is `Inexact` (the source returns a superset); every other
  float comparison — and `NOT` over float `=` — stays in Arrow.

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

Statistics come from the source, cheaply, and are cached per provider with a TTL (default 15
minutes). They are refreshed lazily: the first `scan` after the TTL expires re-reads them
(best-effort; a failed refresh keeps the previous snapshot). A refresh only affects later plans —
filters already accepted at planning are never re-decided. Providers decoded from a serialized
plan (Ballista scheduler/executors) carry no statistics and never refresh.

| Input | Postgres |
| --- | --- |
| Row count estimate | `pg_class.reltuples` |
| Column selectivity | `pg_stats` — `n_distinct` (negative values are fractions of `reltuples` and are converted), `null_frac` |
| Index availability | `pg_index` + `pg_class` |
| Plan cost / access method | `EXPLAIN (FORMAT JSON)` — plans without executing |
| Table size | `pg_total_relation_size()` |

`EXPLAIN` is the highest-fidelity signal and the one we lean on for the decisive cases: it tells us
whether the candidate predicate produces an index scan or a sequential scan, and at what estimated
cost. It is one cheap round trip and the result is cached per (table, predicate shape). A plan
without `Total Cost` is treated as *unknown* (the statistics estimate is used), never as free.

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

"Index available" means: EXPLAIN chose an index path, or a catalog index is **plain** (not
partial, no expression keys), **btree** (hash for `=` only), and has the compared column as its
**leading** key. `<>`, `NOT`, `IS NOT NULL` and column-to-column comparisons never count; `OR`
counts only when every branch does; `AND` when either side does.

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
| `strict` | Push a filter only if every referenced column leads a plain index, selectivity is below `keep_threshold`, and every literal/column involved is primitive (bool/int/timestamp/date); keeps everything without statistics; never pushes `LIMIT`; `push` hints ignored, `deny` still applies. Pushed fidelity is the translated one | Source under pressure |
| `hinted` | Per-column and per-predicate overrides in the job spec | When you know something the stats do not |

Policy names are case-insensitive; an unknown name is a configuration error (it never falls
back to a default, so a mistyped emergency `never` cannot fail open). `deny` matches column names
case-insensitively. Configured per source, overridable per job (JSON job spec):

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

**Where decisions are made.** `supports_filters_pushdown` is the only place the policy runs.
`scan` pushes every filter DataFusion hands it (DataFusion only passes filters that were accepted),
translating it again — deterministically, from column kinds that travel inside serialized plans —
and fails loudly if one cannot be translated rather than dropping it. So a Ballista scheduler,
which rebuilds the provider without statistics, pushes exactly what the planning client decided,
under every policy. There is no separate optimizer rule: DataFusion's own `PushDownFilter` drives
`supports_filters_pushdown`.

Filtered extraction has no always-pushed predicate: every caller-provided filter goes through
the same fidelity + policy decision.

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

1. **Differential correctness tests (implemented, `tests/pg_pushdown.rs`).** Postgres only so far:
   a table of hazards (ICU-collated text ranges, `NOT` under a nondeterministic collation, `-0.0`
   and `NaN` floats, `(NOT flag) IS NULL`, enum `OR`, `uuid`/`jsonb` equality, `IS NULL`, backslash
   text) run with `policy = "always"` and `"never"`; the result sets must be identical, and each
   case also asserts whether it was actually pushed, so the differential cannot pass vacuously.
2. **Property tests (planned).** Generate random predicate trees with `proptest`, then assert the same
   equality. Random trees find the `NOT`/`OR`/NULL interactions that hand-written tests miss.
3. **Plan snapshot tests.** Assert the *decisions*, not just the results, so a stats or cost-model
   change that quietly disables all pushdown shows up as a diff.
4. **Fidelity assertion (planned).** In debug builds, every predicate marked `Exact` is re-evaluated in Arrow
   and the row count compared. A mismatch panics. This turns a silent data bug into a loud test
   failure.
