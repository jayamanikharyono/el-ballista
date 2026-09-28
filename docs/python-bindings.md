# Python Bindings (deferred)

> **Status: deferred, design only.** This document exists so that the Rust API does not evolve
> into something that cannot be bound later, and so that "Python support" has a known shape
> rather than being an open question. The module would be called `el_ballista`. Work on it
> should not start before [Phase 5 — MySQL connector](roadmap.md#phase-5--mysql-connector) is
> complete (multi-source is what would make a programmatic Python surface worth freezing).

---

## Why it is deferred

A Python wrapper written too early does real damage. It freezes the Rust API while that API is
still wrong, it doubles the surface area of every change, and it invites the exact usage pattern
this project is trying to move away from — a Python driver process with Python-level row handling
in the middle of a data path.

There is also no urgency. The consumer of this tool is an orchestrator (Airflow, Dagster,
Cloud Composer) invoking a job spec (a JSON file). That works today with a Rust binary and needs
no bindings at all. `rel run` itself is a diagnostic (it counts rows, delivers no data and writes
no checkpoint), so the production job is a small Rust binary built on the library's
checkpointed `PostgresConnector::…run_with(consumer)`, which hands each split's Arrow stream to
a writer:

```python
# What "Python support" looks like right now, and it is fine
BashOperator(
    task_id="extract_payments",
    # a small Rust binary: PostgresConnector::from_config_file(..)?.extract().standalone().run_with(..)
    bash_command="payment-extract --config extract.json",
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
from el_ballista import ExtractContext, PostgresConnector, col, lit
import pyarrow.parquet as pq

ctx = ExtractContext.from_config("extract.json")

df = (
    ctx.source("postgres", "public.payment")
       .filter(col("customer_id") >= lit(300))
       .select("payment_id", "customer_id", "amount")
)

# Arrow C Data Interface handoff (buffers shared, not serialized)
table = df.to_arrow()              # pyarrow.Table, whole result in memory
frame = df.to_polars()             # via the same Arrow buffers
for batch in df.to_batches():      # iterator of pyarrow.RecordBatch, bounded memory
    ...

# Checkpointed job, the run_with contract: the callback receives one split's batches and the
# split is recorded as completed only if it returns without raising.
def write_split(split, reader):    # reader: pyarrow.RecordBatchReader
    with pq.ParquetWriter(f"out/{split.split_id}.parquet", reader.schema) as writer:
        for batch in reader:
            writer.write_batch(batch)

outcome = PostgresConnector.from_config("extract.json").extract().standalone().run_with(write_split)
```

Note what is deliberately absent: there is no way to write a Python function that runs per row.
Plan construction happens in Python; execution stays entirely in Rust. The `run_with` callback
runs once per split and receives Arrow batches, never rows. The bindings add no sink: writing is
the caller's code, as in Rust. Allowing Python UDFs would
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
- **Async boundaries stay inside the Rust library** (the `connector::postgres` builders /
  `ExtractContext`). The binding layer should call synchronous
  wrappers that own a Tokio runtime, rather than exposing Rust futures to Python's event loop.
- **`ExtractContext` owns its runtime and is cheap to construct**, so a Python object can hold one
  without lifetime gymnastics.

## Open questions to answer before starting

- Does the GIL need to be released around every execution call, and what does that mean for
  progress reporting and interrupt handling (`Ctrl-C` during a long extraction)?
- Which pyarrow versions to support, given the Arrow C Data Interface's compatibility guarantees?
- Does anyone actually want this, or is the CLI plus a job spec sufficient? Worth asking before
  writing a line of it.
