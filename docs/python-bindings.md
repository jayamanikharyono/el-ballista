# Python Bindings — Placeholder (Deferred)

> **Status: not being built.** This document exists so that the Rust API does not evolve into
> something that cannot be bound later, and so that "Python support" has a known shape rather than
> being an open question. No work on `rel-python` should start before
> [Phase 3](roadmap.md#phase-3--mysql-and-multi-source) is complete.

---

## Why it is deferred

A Python wrapper written too early does real damage. It freezes the Rust API while that API is
still wrong, it doubles the surface area of every change, and it invites the exact usage pattern
this project is trying to move away from — a Python driver process with Python-level row handling
in the middle of a data path.

There is also no urgency. The consumer of this tool is an orchestrator (Airflow, Dagster,
Cloud Composer) invoking a job spec. That works today with a CLI binary and needs no bindings at
all:

```python
# What "Python support" looks like right now, and it is fine
BashOperator(
    task_id="extract_orders",
    bash_command="rel run --config extract.toml --job orders_incremental",
)
```

Bindings become worth building when someone wants to compose plans programmatically in Python and
pull results into pandas or Polars — not before.

---

## The intended shape, when it happens

PyO3 for the binding layer, maturin for the build. The critical design constraint is that **Arrow
data must cross the boundary without being copied or converted row by row**, via the Arrow C Data
Interface (`pyo3-arrow` or `arro3`), so a `RecordBatch` produced in Rust becomes a
`pyarrow.RecordBatch` referencing the same buffers.

```python
from rust_extract import ExtractContext, col, lit

ctx = ExtractContext.from_config("extract.toml")

df = (
    ctx.source("orders_pg", "public.orders")
       .incremental(column="updated_at")
       .filter(col("status") == lit("PAID"))
       .select("order_id", "user_id", "amount")
)

df.write_parquet("gs://warehouse/raw/orders/")
ctx.commit_checkpoints()

# Zero-copy handoff for interactive work
table = df.to_arrow()          # pyarrow.Table, no serialization
frame = df.to_polars()         # also zero-copy, via Arrow
```

Note what is deliberately absent: there is no way to write a Python function that runs per row.
Plan construction happens in Python; execution stays entirely in Rust. Allowing Python UDFs would
reintroduce the serialization boundary that motivated the project.

## Constraints this places on the Rust API today

The reason to write this document now rather than later — these are cheap to maintain and expensive
to retrofit:

- **Keep the public API on owned, `Send + Sync` types.** Lifetimes on public structs are painful to
  express through PyO3.
- **Errors carry structured context**, not just strings, so they can map onto a Python exception
  hierarchy (`ExtractError`, `SourceError`, `SchemaError`, `CheckpointError`) instead of collapsing
  into one opaque `RuntimeError`.
- **Every operation reachable from the DataFrame builder must also be reachable from a
  serializable job spec.** If a capability is only expressible in Rust code, it will not be
  bindable, and it will not be usable from the CLI either — which is a good design constraint
  regardless of whether Python ever happens.
- **Async boundaries stay inside `rel-engine`.** The binding layer should call synchronous
  wrappers that own a Tokio runtime, rather than exposing Rust futures to Python's event loop.
- **`ExtractContext` owns its runtime and is cheap to construct**, so a Python object can hold one
  without lifetime gymnastics.

## Open questions to answer before starting

- Does the GIL need to be released around every execution call, and what does that mean for
  progress reporting and interrupt handling (`Ctrl-C` during a long extraction)?
- Which pyarrow versions to support, given the Arrow C Data Interface's compatibility guarantees?
- Does anyone actually want this, or is the CLI plus a job spec sufficient? Worth asking before
  writing a line of it.
