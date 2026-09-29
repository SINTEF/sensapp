# Live Prometheus compatibility test

This test runs a real Prometheus v2.55.1 against SensApp backed by PostgreSQL. It checks remote write and remote read independently. The test uses ports 3017 (SensApp) and 9099 (Prometheus), and removes the Prometheus container when done.

Start a local PostgreSQL server with an empty database, then run:

```bash
SENSAPP_STORAGE_CONNECTION_STRING=postgres://postgres:postgres@127.0.0.1:5432/sensapp-test \
  bash tests/prometheus_live/run.sh
```

The runner needs Cargo, Python 3, and Docker. PostgreSQL data remains in the selected database; use a test database. On failure, the runner prints both service logs.
