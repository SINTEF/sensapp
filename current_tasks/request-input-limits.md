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
