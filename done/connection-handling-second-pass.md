# Connection handling, second pass: silent connections, idle timeout, drain

Source: review of pull request 60 on 11 November 2026 (HEAD `7d8b396`). Read in the code, hyper 1.12.0 and
hyper-util 0.1.21; not executed.

## Problem

`src/http/serve.rs` replaces `axum::serve` with its own accept loop: a cap of 4,096 connections and a 30 s
header-read timeout.

1. **A connection that sends nothing is never timed out.** The builder is hyper-util's auto builder with
   both HTTP/1 and HTTP/2 enabled (`server-auto`). Every connection starts in `ReadVersion`
   (`hyper-util` `server/conn/auto/mod.rs:305-374`), which waits for the first bytes with no timer. The
   HTTP/1 header timer only exists after the first byte. The same holds for a client that sends the HTTP/2
   preface and then nothing: no HTTP/2 keep-alive is configured.
   One unauthenticated client opening 4,096 silent connections holds every permit: all later connections,
   the liveness and readiness probes included, wait in the backlog. Before this change the same attack
   needed as many sockets as the file-descriptor limit.
   `docs/HTTP_LIMITS.md:7` and `CHANGELOG.md:90` say silent connections are covered. The test sends a partial
   HTTP/1 request, which leaves `ReadVersion` at once.
2. **The header timeout is also a 30 s idle keep-alive timeout** (`hyper` `proto/h1/conn.rs:1155-1161`).
   `axum::serve` never closed idle connections. Behind a proxy with a 60 s upstream idle timeout (AWS ALB,
   nginx), the proxy can reuse a connection SensApp just closed: sporadic 502s on POSTs. The value is
   hard-coded and the docs only call it a header timeout.
3. **The shutdown drain is not bounded** (`serve.rs:83`, `graceful.shutdown().await`): the timeout layers
   bound the handler, not the response write, so a client that stops reading keeps the process alive until
   SIGKILL. A second SIGTERM or Ctrl+C does nothing. The listener closes at once on SIGTERM, before
   Kubernetes removes the endpoint, so connections are refused for a moment at each rollout.
4. **408 on a slow body** (`src/http/server.rs:445-452`): Prometheus remote write does not retry 4xx except
   429, so the batch is dropped. `docs/CONFIGURATION.md:134` says a 408 is retried; the same table says a 500
   is not retried, which contradicts its own line 141. The code comment at `server.rs:140-142` already says
   408 is not retried.
5. **`run_blocking` work outlives its request** (`src/http/app_error.rs:191-199`): `spawn_blocking` cannot be
   cancelled, and the write permit is released when the request future is dropped. A client that sends
   large bodies and disconnects piles parses up to the blocking pool's 512 threads instead of the 16 write
   slots. (Suspected.)
6. The SIGTERM handler is installed after `create_or_migrate`, so PID 1 still ignores SIGTERM during a long
   startup migration.

## Suggested solution

1. Serve HTTP/1 only (`http1_only()` or hyper's `http1::Builder` directly): its header timer starts at the
   first poll, bytes or not. SensApp sits behind proxies that speak HTTP/1 upstream, so HTTP/2 cleartext is
   not needed; say so in the docs. If HTTP/2 must stay, wrap the connection future in a timeout until the
   first request arrives and set HTTP/2 keep-alive interval and timeout.
2. Make the idle timeout a setting with a default above common proxy idle timeouts (for example 75 s), keep
   the header timeout at 30 s if hyper lets the two differ; if it does not, one setting of 75 s, documented
   as both.
3. Bound the drain with a configurable grace period (default below the chart's
   `terminationGracePeriodSeconds`), exit on a second signal, and add a `preStop` sleep of a few seconds to
   the Helm chart.
4. Answer 503 with `Retry-After` for a slow body, so Prometheus and Telegraf retry, and fix the table in
   `docs/CONFIGURATION.md`.
5. Move the write permit into the blocking closure (hold an owned permit until the parse returns), so the
   limit bounds the parsing work itself.
6. Install the signal handlers at the start of `main`.

## Tests

- 100 connections that send nothing against a server capped at 100: a new request is served after the
  timeout.
- An idle keep-alive connection stays open for the configured idle time.
- A client that stops reading its response does not block shutdown past the grace period.
- A slow body answers 503.

## Done when

The tests pass and `docs/HTTP_LIMITS.md`, `docs/CONFIGURATION.md` and the changelog describe the behaviour.

## Outcome (11 November 2026)

All six items were real; items 1 and 5 were reproduced by a failing test first.

1. **Silent connections**: `connections_that_send_nothing_are_dropped_and_free_their_permits` (100 silent
   connections against a cap of 100) failed with the auto builder. `serve.rs` now uses hyper's `http1::Builder`
   directly, whose header timer starts at the first poll. HTTP/1 only; `http1_only()` on the auto builder does
   nothing with `serve_connection_with_upgrades`, so it was not an option. Checked on the real binary: h2c prior
   knowledge is refused, a silent connection is closed after the timeout (2.0 s with a setting of 2).
2. **Idle timeout**: hyper has one timer for the headers and the idle time, so one setting,
   `SENSAPP_HTTP_HEADER_READ_TIMEOUT_SECONDS`, default 75 (above the 60 s of AWS ALB and nginx), documented as
   both. Test: an idle keep-alive connection stays open for the timeout.
3. **Drain**: `SENSAPP_HTTP_SHUTDOWN_GRACE_SECONDS` (300) bounds it, a second signal cuts it at once
   (`ShutdownSignals` counts the signals). Test: a client that stops reading 64 MiB, with and without the second
   signal. The chart has a `preStop` sleep (`preStopSleepSeconds`, 5, Kubernetes 1.30 native sleep, no shell
   needed in the image); not run on a cluster.
4. **Slow body**: a 503 with a jittered `Retry-After` (1 to 10 s) instead of a 408; the integration test is
   renamed `a_slow_body_is_a_503_that_frees_the_write_slot`. The table of `docs/CONFIGURATION.md` lost its 408 row.
5. **Permit outliving the parse**: `the_write_slot_is_held_until_the_blocking_work_ends` failed (a second write
   was served while the parse of the dropped request ran). The slot is a task-local that `run_blocking` moves
   into its closure, with no change in the handlers.
6. **Signals at the start of `main`**, and a signal during `create_or_migrate` stops SensApp (the storage
   preparation is raced against the first signal).

Ran: SQLite and PostgreSQL in full (344 + 387 + 2 each), clippy with and without all features.
