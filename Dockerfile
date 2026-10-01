FROM rust:1.98-slim-bookworm AS chef

ARG DUCKDB_DOWNLOAD_LIB="1"

ENV DUCKDB_DOWNLOAD_LIB=${DUCKDB_DOWNLOAD_LIB}

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        build-essential \
        ca-certificates \
        clang \
        cmake \
        curl \
        libssl-dev \
        pkg-config \
        xz-utils \
    && rm -rf /var/lib/apt/lists/*

# Prebuilt static cargo-chef: compiling it from source took minutes on every cold cache.
ARG CARGO_CHEF_VERSION=0.1.78
RUN arch="$(uname -m)" \
    && curl --proto '=https' --tlsv1.2 -fsSL \
        "https://github.com/LukeMathWalker/cargo-chef/releases/download/v${CARGO_CHEF_VERSION}/cargo-chef-${arch}-unknown-linux-musl.tar.xz" \
    | tar -xJ --strip-components=1 -C /usr/local/bin "cargo-chef-${arch}-unknown-linux-musl/cargo-chef"

FROM chef AS planner

COPY Cargo.toml Cargo.lock build.rs settings.toml ./
COPY src ./src
RUN cargo chef prepare --recipe-path recipe.json

FROM chef AS builder

ARG FEATURES="postgres,sqlite,timescaledb,duckdb,clickhouse,rrdcached"
ARG NO_DEFAULT_FEATURES="true"

COPY --from=planner /app/recipe.json recipe.json
RUN if [ "$NO_DEFAULT_FEATURES" = "true" ]; then \
        cargo chef cook --release --recipe-path recipe.json --no-default-features --features "$FEATURES"; \
    else \
        cargo chef cook --release --recipe-path recipe.json --features "$FEATURES"; \
    fi

COPY Cargo.toml Cargo.lock build.rs settings.toml ./
COPY src ./src

RUN if [ "$NO_DEFAULT_FEATURES" = "true" ]; then \
        cargo build --locked --release --bin sensapp --no-default-features --features "$FEATURES"; \
    else \
        cargo build --locked --release --bin sensapp --features "$FEATURES"; \
    fi

# The duckdb feature links the prebuilt libduckdb dynamically (DUCKDB_DOWNLOAD_LIB):
# stage it so the runtime image can ship it. Empty when duckdb is not enabled.
RUN mkdir -p /out/lib \
    && if [ -d target/duckdb-download ]; then \
        find target/duckdb-download -name 'libduckdb.so' -exec cp {} /out/lib/ \; ; \
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
COPY --from=builder /out/lib/ /usr/local/lib/
RUN ldconfig

ENV SENSAPP_ENDPOINT=0.0.0.0 \
    SENSAPP_PORT=3000 \
    SENSAPP_STORAGE_CONNECTION_STRING=sqlite:///var/lib/sensapp/sensapp.db \
    RUST_LOG=info

EXPOSE 3000

USER sensapp:sensapp

ENTRYPOINT ["/usr/local/bin/sensapp"]
