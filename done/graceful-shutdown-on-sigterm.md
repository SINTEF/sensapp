# Graceful shutdown on SIGTERM

Source: code review of 10 November 2026. Confirmed by reading.

## Problem

`shutdown_signal` (`src/http/server.rs:354-359`) only waits for `tokio::signal::ctrl_c()` (SIGINT). The image
runs SensApp as PID 1 (`Dockerfile:106`, `ENTRYPOINT ["/usr/local/bin/sensapp"]`), and the kernel ignores an
unhandled SIGTERM for PID 1. Every `docker stop`, Kubernetes rollout or scale-down therefore waits the grace
period and ends in SIGKILL. In-flight writes are cut mid-request, which leaves partial batches and duplicates
on retry (`ideas/atomic-write-requests.md`).

## Suggested solution

Wait for either signal:

```rust
async fn shutdown_signal() {
    let ctrl_c = async { tokio::signal::ctrl_c().await.expect("...") };
    #[cfg(unix)]
    let terminate = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("...")
            .recv()
            .await;
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();
    tokio::select! { _ = ctrl_c => {}, _ = terminate => {} }
}
```

Check that the drain is bounded: in-flight requests are already limited by the request timeouts, and the Helm
chart's `terminationGracePeriodSeconds` should be at least the write timeout (300 s) or the write timeout
should be stated as the limit. Make `/health/ready` answer 503 once shutdown has started so the load balancer
stops sending requests.

## Tests

`tests/integration/cli.rs` style: start the binary, send SIGTERM, expect exit code 0 within a few seconds. If
practical, start a slow write first and check it completes.

## Done when

The test passes and `docs/` (deployment section) states the shutdown behaviour.

## Outcome (10 November 2026)

**Confirmed and fixed.** `cli::sigterm_lets_the_requests_in_flight_finish_and_exits` starts the real binary
on an SQLite file of its own and sends the headers and half the body of a write. It then sends SIGTERM
and the rest of the body. Before the fix the process died on the signal and the write got no answer. Now
`shutdown_signal` waits for Ctrl+C or SIGTERM: the write is answered with 204 and the process exits with
0. The test only runs in builds with SQLite, since the signals do not depend on the storage. The test
first used `TEST_DATABASE_URL`, which made the server start on the raw `sensapp-test` database. The test
helpers rename that database per checkout (`isolate_test_database_url`), so the raw one was stale and
the server refused to migrate it.

Decisions:

- **The drain is bounded by the request timeouts**: at most 300 s for a write, or the maintenance timeout
  while a vacuum runs. An instance with nothing in flight stops at once. There is no separate drain
  timeout. The Helm chart now sets `terminationGracePeriodSeconds: 330` (a value), so that a rollout does
  not cut writes. `docs/CONFIGURATION.md` has a Shutdown section with the behaviour and the Docker
  settings (`docker stop --time`, `stop_grace_period`).
- **`/health/ready` does not answer 503 during the drain.** axum stops accepting connections when the
  signal arrives, and hyper closes the kept-alive connections once their request is done, so no probe
  could see a 503. Kubernetes takes a terminating pod out of its Service by itself.

Suites: SQLite full (305 unit, 347 integration) passed with the test. PostgreSQL full: everything else
passed. After the test was moved to its own SQLite file, the `cli::` tests passed on both builds (7 on
PostgreSQL, where the test is compiled out, and 8 on SQLite). `helm template` renders the grace period.
`cargo clippy --all-features --tests` is clean.
