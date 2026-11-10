# Request input limits: query length, recursion, metric labels

Source: code review of 10 November 2026. The metrics label issue was confirmed by reading. The two stack
overflows were reported from reading the dependency sources and were not executed: reproduce each one first.

## Problem

1. **Unbounded recursion in the PromQL parser.** `rusty_promql_parser::expr()` is called on request input at
   `src/http/simple_promql.rs:216`/`:239`, `src/http/crud.rs:223` (`/series?selector=`) and
   `src/http/metrics.rs:359`. The parser (0.2.1) is recursive descent with no depth limit, and `parse_expr`
   in `simple_promql.rs` recurses too. A query of tens of thousands of `(` or `-` fits in a URI and is
   expected to overflow the 2 MiB worker stack. A stack overflow aborts the process and no panic layer
   contains it.
2. **`simplify` runs recursive Douglas-Peucker on a worker thread** (`src/storage/common.rs:322`, called from
   `src/http/crud.rs:996`). `simplify-polyline` 0.5.0 recurses on both halves; depth is O(n) on adversarial
   data (alternating sign with decaying amplitude, tiny tolerance), with up to 100,000 points.
3. **Unauthenticated memory growth through the metrics `path` label** (`src/http/metrics.rs:543-558`).
   The label falls back to the raw request path when there is no `MatchedPath`: every 404 and every
   `/ui/<anything>` (served through `nest_service`, 200 with the `index.html` fallback). Each distinct path
   stays forever in the counter and histogram families. The `method` label has the same issue with extension
   methods.
4. **`sensapp_http_requests_in_flight` drifts upward** (`metrics.rs:554-557`): increment and decrement
   around `next.run(request).await` with no drop guard, so a client disconnect leaks one.
5. **Slow uploads hold write slots** (`src/http/server.rs:246-257`): the permit is taken before the body is
   read, up to the 300 s write timeout. 16 slow connections from one token block the other writers of that
   instance. This is a documented trade-off; a bound is still missing.

## Suggested solution

1. Reject `query` and `selector` strings above a small length (4 KiB) with a 400 before parsing, in one
   helper used by the three call sites. Optionally also count nesting depth.
2. Run `simplify` in `tokio::task::spawn_blocking` on a thread with an explicit large stack, or replace the
   crate with an iterative Douglas-Peucker (an explicit stack of index ranges, about 30 lines). The iterative
   version is preferred: no dependency on stack size.
3. Label unmatched requests with constants: `"unmatched"` for the fallback and `"/ui/*"` for the UI. Map
   methods outside GET/POST/PUT/DELETE/HEAD/OPTIONS/PATCH to `"OTHER"`.
4. A small guard struct that decrements on `Drop`.
5. A body-read deadline shorter than the write timeout (for example 30 s, configurable), applied where the
   body is buffered.

## Tests

- A 50,000-character nested query answers 400 on the three endpoints.
- `simplify` on the adversarial series of 100,000 points returns (unit test of the algorithm).
- After 1,000 requests to random unknown paths and random `/ui/x` paths, `/prometheus/metrics` has a constant
  number of `path` values.
- In-flight gauge returns to 0 after a cancelled request.

## Done when

The tests pass and `docs/HTTP_LIMITS.md` lists the new limits.

## Outcome (10 November 2026)

Every finding was reproduced by a failing test first, then fixed.

1. **PromQL recursion: confirmed.** Measured on a 2 MiB thread (a Tokio worker) in a debug build, the
   parser overflows between 100 and 150 nested parentheses, between 500 and 700 unary minus signs, and at
   20,000 terms of a `+` chain. A release build goes about ten times deeper (parentheses overflow between
   1,000 and 5,000). Before the fix, `deeply_nested_or_long_queries_are_refused` aborted the test process
   with SIGABRT. Now `limits::check_promql_size` runs before every parse. It refuses queries and selectors
   over 4,096 bytes, and nesting deeper than 64: open brackets plus the unary signs within them, skipping
   quoted strings. The three call sites are `/api/v1/query`, `/series?selector=` and
   `/prometheus/metrics?selector=` (through `parse_selector_to_matchers`). The 50,000-character query of
   the suggested test does not fit: a URI over 64 KiB is refused before the router anyway, so the test
   uses 300 parentheses, 1,000 minus signs and a 5,001-character chain, plus a quoted label full of
   brackets that must still be accepted.
2. **simplify recursion: confirmed.** The adversarial series of 100,000 points (alternating sign,
   decaying amplitude, tolerance 1e-12) overflowed a 2 MiB stack. `simplify-polyline` is replaced by an
   iterative Douglas-Peucker in `storage::common` (an explicit stack of index ranges), and the crate
   stays as a dev-dependency. `simplify_keeps_the_points_of_the_recursive_version` compares both on 200
   random series, with every tolerance and both quality modes. The overflow test runs on a 128 KiB
   thread with 5,000 points, because the crate already overflows that stack at 1,000 points (measured).
   The first version, with 100,000 points on 2 MiB, took 187 s in a debug build, since every point is kept
   and the work is quadratic. The test now takes 0.5 s.
3. **Metric labels: confirmed.** Ten random paths, ten random `/ui/` paths and ten random extension
   methods added 30 label values. Requests without a route are now `path="unmatched"`, UI files are
   `path="/ui/*"`, and methods outside the standard seven are `OTHER`. Test:
   `real_router::unmatched_paths_and_methods_do_not_grow_the_metrics`.
4. **In-flight gauge: confirmed.** The gauge stayed at 1 after an aborted request. It is now decremented by
   a drop guard. Test: `real_router::a_cancelled_request_is_not_in_flight_anymore`.
5. **Slow bodies: confirmed.** A write whose body stalled held the only write slot until the write timeout,
   then got a 504. The body is already buffered by `enforce_request_body_limit` for every write and for
   the remote read, so that read now has a deadline: `SENSAPP_HTTP_BODY_READ_TIMEOUT_SECONDS` (default
   30 s), answered with 408. Test: `real_router::a_slow_body_is_a_408_that_frees_the_write_slot`. The
   408 arrives after 200 ms, and the next write then succeeds.

Documented in `docs/HTTP_LIMITS.md` and `docs/CONFIGURATION.md`.

Suites: `cargo test --lib` 312 passed. SQLite full: 305 unit and 346 integration tests passed.
PostgreSQL full: 311 unit and 347 integration tests passed. Only HTTP code and the storage-independent
simplification changed, so no other backend was run. `cargo clippy` and
`cargo clippy --all-features --tests` are clean.
