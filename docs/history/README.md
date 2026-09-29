# History: original phase plans

These are the original implementation plans for phases 1 to 4, kept for the record. Each was
written before or during the work it describes, so it records intentions, early numbers and
status claims from that moment, not the code as it is now.

Several designs in them were later changed or removed:

- the Parquet sink (the layer hands out Arrow batches; writing them is the caller's job);
- watermark / incremental state and backfill (ranges are caller-supplied filters; the design
  is kept in [`../deferred/incremental-extraction.md`](../deferred/incremental-extraction.md));
- the in-process Ballista cluster (standalone is plain DataFusion; distributed is real
  `el-ballista scheduler` + `el-ballista worker` processes);
- the `SourceAwarePushdownRule` optimizer rule (push/keep is decided in the table provider's
  `supports_filters_pushdown`).

Module paths in the plans also predate the move of the Postgres code under
`src/connector/postgres/`. For current status see [`../roadmap.md`](../roadmap.md); for the
current design see [`../architecture.md`](../architecture.md).

| File | Phase |
|---|---|
| [`phase-one-implementation-plan.md`](phase-one-implementation-plan.md) | 1 — single-node PostgreSQL extraction, checkpoints, CLI |
| [`phase-two-implementation-plan.md`](phase-two-implementation-plan.md) | 2 — DataFrame API, statistics and the cost model |
| [`phase-three-implementation-plan.md`](phase-three-implementation-plan.md) | 3 — streaming execution with bounded memory |
| [`phase-four-implementation-plan.md`](phase-four-implementation-plan.md) | 4 — distributed execution on Ballista |
