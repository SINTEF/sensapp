# `aggregation` on a string series answers 500

Found on 4 October 2026 while building the chart of the web UI.

`GET /series/{uuid}?step=1m&aggregation=avg` on a string series (a sensor published with `vs`) answers
`500 Internal Server Error`, on SQLite at least. The same request without `step` answers 200. An average of
strings is a client mistake, so the answer should be a `400` that says which types an aggregation supports,
and it should be the same on every backend. Add a backend-generic test (string and boolean series with
every aggregation) next to the other series query tests.

The UI works around it: it only aggregates `float`, `integer` and `numeric` series.
