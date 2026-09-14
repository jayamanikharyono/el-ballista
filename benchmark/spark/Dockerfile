# Benchmark image for the PySpark baseline.
#   docker build -f benchmark/spark/Dockerfile -t rel-bench-spark:latest benchmark/spark
#
# Pinned, multi-arch base (amd64 + arm64). The PostgreSQL JDBC driver is baked in via
# remote ADD (no curl needed) so runs work offline. SPARK_HOME=/opt/spark is the
# long-stable path in the official images.

ARG SPARK_VERSION=3.5.4

FROM apache/spark:${SPARK_VERSION}

# postgresql-42.7.13, verified on Maven Central. Downloaded with the image's own
# python3 rather than ADD+chmod: remote-ADD files plus a separate chmod hit EPERM
# under rootless builders, while created files get a normal umask (world-readable,
# so the runtime user can load the driver whatever UID it runs as).
ARG PG_JDBC_VERSION=42.7.13
RUN python3 -c "import urllib.request, os; \
    v = os.environ.get('PG_JDBC_VERSION', '42.7.13'); \
    urllib.request.urlretrieve( \
      f'https://repo1.maven.org/maven2/org/postgresql/postgresql/{v}/postgresql-{v}.jar', \
      f\"{os.environ.get('SPARK_HOME', '/opt/spark')}/jars/postgresql-{v}.jar\")"

COPY load.py /opt/bench/load.py

# Same contract as the Rust image: config via environment, parquet to /output.
# (Runs as root, like the image default for custom entrypoints; output files may
# end up root-owned — chown them if your umask complains.)
ENV BENCH_PG_HOST=bench-pg \
    BENCH_PG_PORT=5432 \
    BENCH_PG_DB=app \
    BENCH_PG_USER=postgres \
    BENCH_PG_PASSWORD=postgres \
    BENCH_TABLE=orders \
    BENCH_PARTITIONS=2 \
    BENCH_SPARK_MASTER="local[*]" \
    BENCH_OUTPUT=/output/orders.parquet \
    BENCH_FILTER= \
    BENCH_COLUMNS= \
    BENCH_SCENARIO=full \
    SPARK_DRIVER_MEM=2g

VOLUME /output
# PySpark ships as source under $SPARK_HOME/python (plus a py4j zip), not installed into
# site-packages — expose both via PYTHONPATH. The py4j filename carries its version, so
# resolve it with a glob rather than pinning.
ENTRYPOINT ["sh", "-c", "export PYSPARK_SUBMIT_ARGS=\"--driver-memory ${SPARK_DRIVER_MEM:-2g} pyspark-shell\"; export SPARK_HOME=\"${SPARK_HOME:-/opt/spark}\"; export PYTHONPATH=\"$SPARK_HOME/python:$(ls \"$SPARK_HOME\"/python/lib/py4j-*-src.zip | head -1)\"; exec python3 /opt/bench/load.py \"$BENCH_PG_HOST\" \"$BENCH_PG_PORT\" \"$BENCH_PG_DB\" \"$BENCH_PG_USER\" \"$BENCH_PG_PASSWORD\" \"$BENCH_TABLE\" \"$BENCH_OUTPUT\" \"$BENCH_PARTITIONS\" \"$BENCH_FILTER\" \"$BENCH_COLUMNS\" \"$BENCH_SCENARIO\""]
