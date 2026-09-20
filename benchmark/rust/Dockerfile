# Benchmark image: the extraction layer as bench client, scheduler, and worker.
# Build from the repo root so the crate sources are in context:
#   docker build -f benchmark/rust/Dockerfile -t rel-bench-rust:latest .
#
# Multi-arch base (amd64 + arm64) so Apple-Silicon hosts work unmodified.
# One image, three roles selected by BENCH_ROLE (bench|scheduler|worker):
#   bench      full/selective scan client (default)
#   scheduler  rel scheduler (our Ballista scheduler with Postgres codecs)
#   worker     rel worker (our Ballista executor with Postgres codecs)
# Stock Ballista binaries can never substitute for scheduler/worker: our scan plans
# only decode with the compiled-in Postgres codecs.
#
# cargo-chef splits dependency compilation (slow, rarely changes) from our crates
# (fast, changes constantly): rebuilds after a source edit recompile only our code
# instead of all ~400 dependency crates. First build is still long (release Ballista);
# for a prebuilt image see benchmark/README.md ("prebuilt images" section).

ARG RUST_VERSION=1-slim-bookworm

FROM rust:${RUST_VERSION} AS chef
# Slim base lacks a C toolchain; ring (via rustls) needs cc to build.
# lld replaces GNU ld: linking this workspace's ~400 rlibs in release mode OOM-kills
# GNU ld on small hosts (collect2: ld terminated with signal 9); lld uses a fraction
# of the RAM for the same link.
RUN apt-get update \
    && apt-get install -y --no-install-recommends gcc ca-certificates lld \
    && rm -rf /var/lib/apt/lists/* \
    && cargo install --locked cargo-chef
ENV RUSTFLAGS="-C link-arg=-fuse-ld=lld"
# Serial codegen: release defaults run 16 codegen units in parallel per crate, which
# OOM-kills rustc itself (SIGKILL, not a compile error) on small hosts. On a 1-CPU
# builder there is no parallelism to lose; on bigger hosts override to taste.
ENV CARGO_PROFILE_RELEASE_CODEGEN_UNITS=1
WORKDIR /app

FROM chef AS planner
COPY Cargo.toml Cargo.lock ./
COPY src ./src
COPY examples ./examples
RUN cargo chef prepare --recipe-path recipe.json

FROM chef AS builder
COPY --from=planner /app/recipe.json recipe.json
# Dependencies only — cached unless Cargo.toml/lock change. Cache mounts persist the
# registry and incremental artifacts across builds (requires BuildKit or recent buildah;
# plain `docker build` — no compose needed).
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/app/target \
    cargo chef cook --locked --release --bins --examples --recipe-path recipe.json
# Our crates only — fast on rebuilds. Binaries must be copied OUT during this RUN:
# with a mounted target dir, artifacts never enter the layer filesystem.
COPY src ./src
COPY examples ./examples
COPY benchmark/rust/bench-config.json ./benchmark/rust/bench-config.json
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/app/target \
    cargo build --locked --release --bins --example bench_full_load && \
    cp /app/target/release/examples/bench_full_load /usr/local/bin/bench && \
    cp /app/target/release/rust-ballista-extraction-layer /usr/local/bin/rel

FROM debian:bookworm-slim AS runtime
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# sqlx uses rustls (static) — no libpq/OpenSSL needed at runtime.
COPY --from=builder /usr/local/bin/bench /usr/local/bin/bench
COPY --from=builder /usr/local/bin/rel /usr/local/bin/rel
COPY benchmark/rust/bench-config.json /etc/bench/config.json

ENV BENCH_ROLE=bench \
    BENCH_CONFIG=/etc/bench/config.json \
    BENCH_WORKERS=2 \
    BENCH_CONCURRENT_TASKS= \
    BENCH_OUTPUT=/output/orders.parquet \
    BENCH_FILTER= \
    BENCH_COLUMNS= \
    BENCH_SCENARIO=full \
    BENCH_SCHEDULER_URL=

VOLUME /output
ENTRYPOINT ["sh", "-c", "case \"$BENCH_ROLE\" in \
  scheduler) exec rel scheduler --scheduler-url \"$BENCH_SCHEDULER_URL\" --bind-host 0.0.0.0;; \
  worker) args=\"worker --scheduler-url $BENCH_SCHEDULER_URL --bind-host 0.0.0.0 --external-host $(hostname)\"; \
    if [ -n \"$BENCH_CONCURRENT_TASKS\" ]; then args=\"$args --concurrent-tasks $BENCH_CONCURRENT_TASKS\"; fi; \
    exec rel $args;; \
  *) exec bench \"$BENCH_CONFIG\" \"$BENCH_WORKERS\" \"$BENCH_OUTPUT\" \"$BENCH_FILTER\" \"$BENCH_COLUMNS\" \"$BENCH_SCENARIO\";; \
esac"]
