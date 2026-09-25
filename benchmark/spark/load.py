"""PySpark baseline for the initial-load benchmarks.

Fair-comparison counterpart to examples/bench_full_load.rs: read the benchmark table
over JDBC (partitioned on the PK, same fan-out as the Rust side), write Snappy Parquet,
print a JSON summary for benchmark/run.sh.

Usage:
    python3 load.py <host> <port> <db> <user> <password> <table> <output> <partitions>
                    [filter_sql] [columns_csv] [scenario]

Empty filter/columns means full load. When given, both must match the Rust side exactly.

Timed section is read + write only (session boot excluded on both sides by convention).
Row count comes from reading the written output back, so a short write cannot fake it.
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
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    url = "jdbc:postgresql://{}:{}/{}".format(host, port, db)
    properties = {
        "user": user,
        "password": password,
        "driver": "org.postgresql.Driver",
    }
    # BENCH_FETCHSIZE set (manual run) -> stream in chunks on both engines.
    # Unset/empty (auto run) -> omit the property: Spark's fetchsize default (0 =
    # JDBC driver default) applies. For Postgres that buffers each partition fully
    # client-side, which is exactly the out-of-box behavior being measured.
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

    t0 = time.time()
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
    df.write.mode("overwrite").option("compression", "snappy").parquet(output)
    rows = spark.read.parquet(output).count()
    t_end = time.time()
    elapsed_ms = int((t_end - t0) * 1000)

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
                "mem_peak_bytes": cgroup_memory_peak(),
                "output_bytes": output_bytes(output),
                "output": output,
            }
        )
    )
    spark.stop()


def cgroup_memory_peak():
    """/sys/fs/cgroup/memory.peak of this container (cgroup v2), or None."""
    try:
        with open("/sys/fs/cgroup/memory.peak") as f:
            return int(f.read().strip())
    except (OSError, ValueError):
        return None


if __name__ == "__main__":
    main()
