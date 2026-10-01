# `GET /series/{uuid}`: latest N points, truncation signal, `max_points`

Follow-up to `done/series-limit-and-simplify-semantics.md`.

- **Latest N samples.** `limit` returns the oldest N samples of the window. `GET /series/{uuid}/last` returns one sample. Consider `order=desc` (then reverse so the response stays in ascending time order, or document otherwise) so a client can ask for "the last 1 000 points". It needs a `desc` option in `query_sensor_data` for every backend, served by the `(sensor_id, timestamp_us)` index.
- **Truncation signal.** An explicit `limit` truncates silently (documented in `docs/HTTP_LIMITS.md`). Option: read `limit + 1`, drop the extra row and send `X-SensApp-Truncated: true`.
- **`max_points`.** A parameter that picks `step` server-side from the window, to save clients the arithmetic. `step` + `aggregation` already covers it.
- **Larger scan budget for simplify (option B).** Simplify could read more than 100 000 raw rows and only require the output to fit. Needs memory/time numbers on a 1 M+ series first (Douglas-Peucker O(n log n) typical, O(n²) worst case; `simplify_indices_f64` also does a linear `position` search per kept point). Only worth it if rejecting turns out too restrictive.
