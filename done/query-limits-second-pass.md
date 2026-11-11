# Query limits, second pass: PromQL nesting bypass and simplify CPU

Source: review of pull request 60 on 11 November 2026 (HEAD `7d8b396`). The divergences were confirmed by
reading; the aborts were not executed. Reproduce each on a thread with a 2 MiB stack first.

## Problem

1. **The nesting check can be bypassed** (`src/http/limits.rs:130-184`, `check_promql_size`).
   - Comments: the parser treats `#` to the end of line as whitespace
     (`rusty-promql-parser` 0.2.1 `lexer/whitespace.rs:52-64`). The checker does not, and reads a quote inside
     a comment as the start of a string. Input: `#"`, a newline, then 4,090 `(`. The checker skips everything
     after the quote and counts a nesting of 0; the parser skips the comment and recurses 4,090 levels.
     Reachable on `/api/v1/query`, `/series?selector=` and `/prometheus/metrics?selector=` with the newline
     as `%0A`.
   - `^` is right-associative and recurses once per operator (`parser/expr.rs:117-131`); the checker does not
     count it. `x^x^x…` with 2,047 operators has a counted nesting of 0.
   - The unit test never calls the parser, so the checker and the parser are not compared.
2. **The iterative Douglas-Peucker is quadratic on the adversarial series and runs on an async worker**
   (`src/storage/common.rs:408-431`, called inline at `src/http/crud.rs:988-991`). The stack overflow is
   fixed. The task file measured 187 s in a debug build for 100,000 points, then cut the test to 5,000
   points. The request timeout cannot interrupt CPU-bound code, so a few such requests occupy every worker.

## Suggested solution

1. Do not try to mirror the parser. Run the parse on a thread whose stack is sized for the cap: with the
   length cap of 4,096 bytes the depth is at most 4,096 levels, so measure the frame size per level in a
   release and a debug build and spawn the parse with `std::thread::Builder::stack_size` (or
   `stacker::maybe_grow`) accordingly, through one helper used by the three entry points. Keep the length
   cap. Then the nesting checker can be removed, or kept as a cheap early 400.
   If the checker is kept as the only defence instead: make it skip `#` comments, count every binary
   operator, and add a test that feeds the same generated inputs to the checker and to the parser on a
   2 MiB thread.
2. Run `simplify_sensor_data` through `run_blocking` (`src/http/app_error.rs`), and give it a work budget:
   count the distance evaluations and return a 400 ("tolerance too small for this series") above a limit
   that keeps the worst case under about a second in release. Put the 100,000-point adversarial series back
   in a test, asserting the 400 or the time.

## Tests

- `#"\n((((…`, `x^x^x…` and the bracket and minus shapes, each at the length cap, on the three endpoints:
  an answer (400 or a result), and the process survives. Run them in a debug build, where the stack is the
  tightest.
- The adversarial 100,000-point series with a tiny tolerance and `simplify_high_quality=true`.

## Done when

The tests pass in debug and release, and `docs/HTTP_LIMITS.md` states the limits.

## Outcome (11 November 2026)

Both findings were real.

1. **Nesting**: the new test `hostile_queries_get_an_answer_on_a_small_stack` (13 shapes at the length cap on a
   2 MiB thread, `#"` then 4,090 `(`, `x^x^…`, brackets, signs, function calls, subquery and `unless` chains)
   aborted the test process with a stack overflow. The text scan for the nesting is gone. The parse of both entry
   points (`parse_promql_query`, `parse_selector_to_matchers`, which serve the three endpoints) runs on a scoped
   thread with a 256 MiB stack in a debug build and 64 MiB in a release build (`limits::on_promql_parser_stack`),
   and the length cap of 4,096 bytes stays. Measured with a temporary example, smallest power-of-two stack that
   parses each shape at the cap (the figures are upper bounds): debug at most 128 MiB (nested parentheses, and
   `#"` plus parentheses), release at most 16 MiB; the same shapes take 15 to 43 ms in a debug build and 1 to 2 ms
   in a release one. The 64 nested levels limit no longer exists, a 100-level query is parsed. The integration
   test of `request_panics.rs` goes through the three endpoints. Tests pass in debug and release (lib).
2. **Simplify**: `simplify_indices` counts the distance evaluations and returns `SimplifyBudgetExceeded` above
   100 million, which is a 400 (`AppError::from`). On the 100,000-point zigzag the budget runs out in 0.59 s in a
   release build (the whole series was 187 s in a debug build). `simplify_sensor_data` runs through `run_blocking`
   at the HTTP call site. The test uses 5,000 points and a small budget (`simplify_indices_within`), to stay small.

Ran: SQLite and PostgreSQL in full (347 + 387 + 2), clippy with and without all features.
