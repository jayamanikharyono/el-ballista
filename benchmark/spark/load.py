"""PySpark baseline for the initial-load benchmarks.

Fair-comparison counterpart to examples/bench_full_load.rs: read the benchmark table
over JDBC (partitioned on the PK, same fan-out as the Rust side), write ONE Snappy
Parquet file (like the Rust side), print a JSON summary for benchmark/run.sh.

Usage:
    python3 load.py <host> <port> <db> <user> <password> <table> <output> <partitions>
                    [filter_sql] [columns_csv] [scenario]

Empty filter/columns means full load. When given, both must match the Rust side exactly.

Timed section is end to end, the same span as the Rust side: from this program's entry
point (before the SparkSession / JVM starts) to the last byte of the Parquet file
written. It includes session start-up and the partition-bounds query. The row count
comes from reading the written output back AFTER the timer stops (and after the memory
peak is taken), so a short write cannot fake it and the read-back is not timed.

Settings are Spark defaults except what the benchmark spec fixes on both engines: the
JDBC partition count (default = engine cores, Spark's own default parallelism for
local[*]), the driver heap (solved from the container budget) and the JDBC fetch size
(run.sh passes Rust's batch size: 8192 rows by default, or --batch-size). The output is repartitioned to a single task so
Spark writes one file, as the Rust side does; the read stays partitioned.
"""

import json
import os
import sys
import time


def output_bytes(path):
    total = 0
    for dirpath, _, filenames in os.walk(path):
        for name in filenames:
            total += os.path.getsize(os.path.join(dirpath, name))
    return total


def main():
    # Timed section starts at the entry point: SparkSession/JVM start-up is inside it.
    t0 = time.time()
    args = sys.argv[1:12]
    host, port, db, user, password, table, output, partitions = args[0:8]
    partitions = int(partitions)
    filt = args[8] if len(args) > 8 else ""
    columns = args[9] if len(args) > 9 else ""
    scenario = args[10] if len(args) > 10 else "full"

    from pyspark.sql import SparkSession

    spark = (
        SparkSession.builder.appName("rel-bench-spark")
        # local[N] caps concurrent tasks: each task buffers its partition, so fewer
        # cores = lower peak memory at the cost of speed. Size N to the box.
        .master(os.environ.get("BENCH_SPARK_MASTER") or "local[*]")
        # Belt and braces with PYSPARK_SUBMIT_ARGS (set in the Dockerfile): plain-python
        # launches otherwise ignore driver-memory config when forking the JVM gateway.
        .config("spark.driver.memory", os.environ.get("SPARK_DRIVER_MEM", "2g"))
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    url = "jdbc:postgresql://{}:{}/{}".format(host, port, db)
    properties = {
        "user": user,
        "password": password,
        "driver": "org.postgresql.Driver",
    }
    # BENCH_FETCHSIZE (run.sh always sets it: 8192 = Rust's default batch, or --batch-size)
    # makes the Postgres driver stream each partition in chunks of that many rows. Without
    # it (Spark's default fetchsize 0) the driver holds each partition's whole result in the
    # heap, and a large table runs out of memory. Unset/empty here keeps that default.
    fetchsize = os.environ.get("BENCH_FETCHSIZE")
    if fetchsize:
        properties["fetchsize"] = fetchsize

    # Partition bounds first, so the N-way read matches the Rust keyset fan-out.
    bounds = (
        spark.read.jdbc(
            url,
            "(select min(order_id) as lo, max(order_id) as hi from {}) bounds".format(table),
            properties=properties,
        ).first()
    )
    assert bounds["lo"] is not None, "benchmark table is empty"

    t_start_epoch_ms = int(t0 * 1000)
    df = spark.read.jdbc(
        url,
        table,
        column="order_id",
        lowerBound=bounds["lo"],
        upperBound=bounds["hi"],
        numPartitions=partitions,
        properties=properties,
    )
    if columns:
        df = df.select(*[c.strip() for c in columns.split(",")])
    if filt:
        df = df.filter(filt)
    # One output file, like the Rust side: the read stays partitioned (parallel JDBC
    # scans); repartition(1) funnels the rows into a single write task. (coalesce(1)
    # would also collapse the read to one task, i.e. one JDBC connection.)
    df.repartition(1).write.mode("overwrite").option("compression", "snappy").parquet(output)
    t_end = time.time()
    elapsed_ms = int((t_end - t0) * 1000)
    # Taken before the untimed read-back, so it covers the timed work only.
    mem_peak = cgroup_memory_peak()
    rows = spark.read.parquet(output).count()

    print(
        json.dumps(
            {
                "engine": "pyspark-3.5.4",
                "scenario": scenario,
                "table": table,
                "filter": filt,
                "projection": columns if columns else "*",
                "partitions": partitions,
                "rows": rows,
                "elapsed_ms": elapsed_ms,
                # Timed-section bounds (epoch ms): run.sh restricts CPU/memory stats to it.
                "t_start_epoch_ms": t_start_epoch_ms,
                "t_end_epoch_ms": int(t_end * 1000),
                # Exact cgroup high-water mark of this container (includes page cache).
                "mem_peak_bytes": mem_peak,
                "output_files": output_files(output),
                "output_bytes": output_bytes(output),
                "output": output,
            }
        )
    )
    spark.stop()


def output_files(path):
    """Number of Parquet data files written (1 expected)."""
    return sum(
        1
        for _, _, names in os.walk(path)
        for n in names
        if n.endswith(".parquet")
    )


def cgroup_memory_peak():
    """/sys/fs/cgroup/memory.peak of this container (cgroup v2), or None."""
    try:
        with open("/sys/fs/cgroup/memory.peak") as f:
            return int(f.read().strip())
    except (OSError, ValueError):
        return None


if __name__ == "__main__":
    main()
