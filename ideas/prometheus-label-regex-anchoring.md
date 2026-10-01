# Label regex matchers are not anchored

Found 1 Oct 2026 while measuring selectors on PostgreSQL: `{__name__="cpu usage",core=~"c7"}`
matched 111 series (`c7`, `c70`..`c79`, `c700`..`c799`, ...). In Prometheus a regex matcher is fully
anchored, so it matches only the series whose label is exactly `c7`.

The backends use the unanchored regex operator of their database (`~` on PostgreSQL and
TimescaleDB, `match()` on ClickHouse, a `regexp` function on SQLite). Measured on PostgreSQL only;
the others are expected to behave the same.

Fix idea: anchor in one place, when the matcher is built (`^(?:pattern)$`), and add a backend
generic test next to `tests/integration/query_sensors_by_labels.rs`. Check `!~` as well: a negative
matcher must exclude exactly the full matches. This changes what existing dashboards select, which is
the point (Prometheus compatibility), so it is worth a line in the release notes.
