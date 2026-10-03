# syntax=docker/dockerfile:1

# rrdcached built from the upstream RRDtool release.
# Debian's rrdtool package is stuck on 1.7.2 (maintained through non-maintainer uploads),
# so we build the latest upstream release instead. Only rrdcached and librrd are built,
# without graphing, bindings, docs or examples.
#
# Copy of rrdcached.Dockerfile of https://github.com/SINTEF/rrdcached-client.

ARG DEBIAN_RELEASE=trixie

FROM debian:${DEBIAN_RELEASE}-slim AS builder

ARG RRDTOOL_VERSION=1.11.0
ARG RRDTOOL_SHA256=24c345b5c077c1b2b2fcbc1a364f1da051650fb6743ad5213096adc04c862ed4

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    ca-certificates \
    curl \
    libglib2.0-dev \
    libxml2-dev \
    pkg-config \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /src
RUN curl -fsSL -o rrdtool.tar.gz \
    "https://github.com/oetiker/rrdtool-1.x/releases/download/v${RRDTOOL_VERSION}/rrdtool-${RRDTOOL_VERSION}.tar.gz" \
    && echo "${RRDTOOL_SHA256}  rrdtool.tar.gz" | sha256sum -c - \
    && tar xzf rrdtool.tar.gz --strip-components=1 \
    && rm rrdtool.tar.gz

RUN ./configure \
        --prefix=/opt/rrdtool \
        --disable-static \
        --disable-rrd_graph \
        --disable-rrdcgi \
        --disable-docs \
        --disable-examples \
        --disable-nls \
        --disable-libdbi \
        --disable-libwrap \
        --disable-perl \
        --disable-python \
        --disable-ruby \
        --disable-lua \
        --disable-tcl \
    && make -j"$(nproc)" \
    && make install-strip

# Stage only what the runtime image needs, preserving the library symlinks.
RUN mkdir -p /out/usr/local/bin /out/usr/local/lib \
    && cp -a /opt/rrdtool/bin/rrdcached /out/usr/local/bin/ \
    && cp -a /opt/rrdtool/lib/librrd.so* /out/usr/local/lib/

FROM debian:${DEBIAN_RELEASE}-slim

# Runtime libraries must match the -dev packages of the builder stage.
RUN apt-get update && apt-get install -y --no-install-recommends \
    libglib2.0-0t64 \
    libxml2 \
    tini \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /out/ /
RUN ldconfig

RUN useradd -r -s /bin/false rrdcached && \
    mkdir -p /data/db /data/journal /var/run/rrdcached && \
    chown -R rrdcached:rrdcached /data /var/run/rrdcached
USER rrdcached

EXPOSE 42217
WORKDIR /data
VOLUME [ "/data/db", "/data/journal" ]

ENTRYPOINT ["/usr/bin/tini", "--"]

CMD ["/usr/local/bin/rrdcached", "-g", "-F", "-B", "-R", "-l", ":42217", "-p", "/var/run/rrdcached/rrdcached.pid", "-b", "/data/db", "-j", "/data/journal", "-U", "rrdcached", "-G", "rrdcached", "-w", "300", "-f", "3600", "-t", "4", "-V", "LOG_INFO"]
