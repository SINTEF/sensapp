FROM debian:bookworm-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends rrdcached netcat-openbsd \
    && rm -rf /var/lib/apt/lists/*

RUN mkdir -p /var/lib/rrdcached/db /var/lib/rrdcached/journal

EXPOSE 42217

CMD ["rrdcached", "-g", "-B", "-R", "-l", "0.0.0.0:42217", "-b", "/var/lib/rrdcached/db", "-j", "/var/lib/rrdcached/journal", "-w", "1", "-f", "1"]
