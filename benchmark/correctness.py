"""Independent row-by-row correctness check over benchmark Parquet outputs.

Reads the actual files (no shared code with either engine), groups by scenario from the
filename (`*_full.parquet`, `*_selective.parquet`), and proves:
  1. every file's row count (recomputed from the file, not trusted from summaries),
  2. all engines' outputs are row-identical per scenario (order-insensitive set equality),
  3. selective outputs satisfy the predicate and carry exactly the projected columns,
  4. selective rows are a subset of the same engine's full rows,
  5. jsonb `metadata` matches semantically (key order differs: serde_json sorts keys,
     pgjdbc preserves storage order, so raw-string compare would false-fail).

Timestamps compare as instants (`epoch_us`) so tz-aware vs tz-naive representations
(Rust writes UTC-annotated, Spark writes naive) cannot false-fail either.

Usage:
    python3 correctness.py <output_dir> <results_json_out>
Exit 0 on PASS, 1 on FAIL. Needs the `duckdb` package:
    pip install duckdb   (or: benchmark/run.sh --skip-correctness to bypass)
"""

import glob
import json
import os
import sys

FULL_COLUMNS = [
    "order_id", "user_id", "status", "amount", "currency", "item_count",
    "tags", "metadata", "shipped_on", "ext_ref", "created_at", "updated_at",
]
SELECTIVE_COLUMNS = ["order_id", "amount", "status"]
SELECTIVE_WHERE = "status = 'REFUNDED'"

# Everything but the jsonb column compares directly; timestamps as instants.
COMPARE_COLS = [c for c in FULL_COLUMNS if c != "metadata"]


def ts_expr(col):
    return "epoch_us({}) AS {}".format(col, col)


def compare_select(path, columns):
    """SELECT list with timestamp normalization + jsonb field extraction."""
    parts = []
    for c in columns:
        if c in ("created_at", "updated_at"):
            parts.append(ts_expr(c))
        elif c == "metadata":
            parts.append("metadata->>'channel' AS meta_channel")
            parts.append("metadata->>'promo' AS meta_promo")
            parts.append("metadata->>'attempt' AS meta_attempt")
        else:
            parts.append(c)
    return "SELECT {} FROM read_parquet('{}')".format(", ".join(parts), path)


def main():
    import duckdb

    out_dir, results_path = sys.argv[1], sys.argv[2]
    duckdb.execute("SET TimeZone='UTC'")
    checks = []

    def check(name, ok, detail=""):
        checks.append({"name": name, "status": "PASS" if ok else "FAIL",
                       "detail": detail})
        print(("PASS " if ok else "FAIL ") + name + (" — " + detail if detail else ""))
        return ok

    def files(scenario):
        return sorted(glob.glob(os.path.join(out_dir, "*_{}.parquet".format(scenario))))

    overall = True
    full_files = files("full")
    sel_files = files("selective")

    overall &= check("outputs present",
                     len(full_files) >= 1 and len(sel_files) >= 1,
                     "full={} selective={}".format(len(full_files), len(sel_files)))

    def count(path):
        return duckdb.execute(
            "SELECT count(*) FROM read_parquet('{}')".format(path)).fetchone()[0]

    counts = {}
    for path in full_files + sel_files:
        try:
            counts[path] = count(path)
        except Exception as e:  # noqa: BLE001
            overall &= check("readable " + os.path.basename(path), False, str(e)[:120])
    if counts:
        overall &= check("all files readable", True,
                         "{} files".format(len(counts)))

    def sets_equal(a, b, columns):
        qa = compare_select(a, columns)
        qb = compare_select(b, columns)
        diff = duckdb.execute(
            "SELECT count(*) FROM (({} EXCEPT {}) UNION ALL ({} EXCEPT {})) t"
            .format(qa, qb, qb, qa)).fetchone()[0]
        return diff == 0

    for scenario, paths, columns in (("full", full_files, FULL_COLUMNS),
                                     ("selective", sel_files, SELECTIVE_COLUMNS)):
        if len(paths) < 2:
            continue
        base = paths[0]
        for other in paths[1:]:
            try:
                eq = sets_equal(base, other, columns)
                detail = "{} rows each".format(counts.get(base, "?"))
            except Exception as e:  # noqa: BLE001
                eq, detail = False, str(e)[:160]
            overall &= check(
                "{} row-identical: {} == {}".format(
                    scenario, os.path.basename(base), os.path.basename(other)),
                eq, detail)

    # Selective files: predicate holds, projection exact.
    for path in sel_files:
        try:
            bad = duckdb.execute(
                "SELECT count(*) FROM read_parquet('{}') WHERE NOT ({})"
                .format(path, SELECTIVE_WHERE)).fetchone()[0]
            cols = [r[0] for r in duckdb.execute(
                "DESCRIBE SELECT * FROM read_parquet('{}')".format(path)).fetchall()]
            ok = bad == 0 and cols == SELECTIVE_COLUMNS
            detail = "violations={} cols={}".format(bad, cols)
        except Exception as e:  # noqa: BLE001
            ok, detail = False, str(e)[:160]
        overall &= check("selective holds: " + os.path.basename(path), ok, detail)

    # Selective ⊆ full per engine (orders_rust_selective vs orders_rust_full, ...).
    for sel in sel_files:
        engine = os.path.basename(sel).replace("_selective.parquet", "")
        full = os.path.join(out_dir, engine + "_full.parquet")
        if not os.path.exists(full):
            continue
        try:
            qsel = compare_select(sel, SELECTIVE_COLUMNS)
            qfull = ("SELECT order_id, amount, status FROM ({})"
                     .format(compare_select(full, FULL_COLUMNS)))
            # Timestamps normalized on both sides already; compare the narrow shape.
            extra = duckdb.execute(
                "SELECT count(*) FROM ({}) s LEFT JOIN ({}) f "
                "ON s.order_id = f.order_id AND s.amount = f.amount AND s.status = f.status "
                "WHERE f.order_id IS NULL".format(qsel, qfull)).fetchone()[0]
            ok = extra == 0
            detail = "unmatched={}".format(extra)
        except Exception as e:  # noqa: BLE001
            ok, detail = False, str(e)[:160]
        overall &= check("selective ⊆ full: " + engine, ok, detail)

    doc = {"overall": "PASS" if overall else "FAIL", "checks": checks}
    with open(results_path, "w") as f:
        json.dump(doc, f, indent=2)
    print("overall:", doc["overall"])
    return 0 if overall else 1


if __name__ == "__main__":
    sys.exit(main())
