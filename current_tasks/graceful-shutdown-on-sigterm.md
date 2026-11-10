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
