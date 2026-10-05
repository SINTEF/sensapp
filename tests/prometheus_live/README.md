# Live Prometheus compatibility test

This test runs a real Prometheus (v3.15.0, or the image of `PROMETHEUS_IMAGE`) against SensApp backed by PostgreSQL. It checks remote write and remote read independently, and that what Prometheus computes from the samples of a remote read, with the hints it sends (`avg_over_time(x[1h])` with a step...), is what the raw samples give. The test uses ports 3017 (SensApp) and 9099 (Prometheus), and removes the Prometheus container when done.

Start a local PostgreSQL server with an empty database, then run:

```bash
SENSAPP_STORAGE_CONNECTION_STRING=postgres://postgres:postgres@127.0.0.1:5432/sensapp-test \
  bash tests/prometheus_live/run.sh
```

The runner needs Cargo, Python 3, and Docker. PostgreSQL data remains in the selected database; use a test database. On failure, the runner prints both service logs.
