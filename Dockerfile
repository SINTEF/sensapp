FROM rust:1.89-slim-bookworm AS builder

ARG FEATURES="postgres,sqlite,timescaledb,duckdb,clickhouse,rrdcached"
ARG NO_DEFAULT_FEATURES="true"
ARG DUCKDB_DOWNLOAD_LIB="1"

ENV DUCKDB_DOWNLOAD_LIB=${DUCKDB_DOWNLOAD_LIB}

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        build-essential \
        clang \
        cmake \
        libssl-dev \
        pkg-config \
    && rm -rf /var/lib/apt/lists/*

# Prime the dependency cache before copying the full source tree.
COPY Cargo.toml Cargo.lock build.rs settings.toml ./
COPY src ./src

RUN if [ "$NO_DEFAULT_FEATURES" = "true" ]; then \
        cargo build --locked --release --bin sensapp --no-default-features --features "$FEATURES"; \
    else \
        cargo build --locked --release --bin sensapp --features "$FEATURES"; \
    fi

FROM debian:bookworm-slim AS runtime

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        ca-certificates \
        libgcc-s1 \
        libssl3 \
        libstdc++6 \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --system --gid 10001 sensapp \
    && useradd --system --uid 10001 --gid sensapp --home-dir /var/lib/sensapp --create-home sensapp

WORKDIR /var/lib/sensapp

COPY --from=builder /app/target/release/sensapp /usr/local/bin/sensapp

ENV SENSAPP_ENDPOINT=0.0.0.0 \
    SENSAPP_PORT=3000 \
    SENSAPP_STORAGE_CONNECTION_STRING=sqlite:///var/lib/sensapp/sensapp.db \
    RUST_LOG=info

EXPOSE 3000

USER sensapp:sensapp

ENTRYPOINT ["/usr/local/bin/sensapp"]
