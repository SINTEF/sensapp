# Drop the RUSTSEC-2026-0459 Ignore

`cargo make security-audit` ignores RUSTSEC-2026-0459 (CVE-2026-119375) since 0.4.2. SQLite before
3.54.0 can return uninitialized memory when it opens a database together with a `-wal` file whose page
size differs. The attacker must be able to write both files, and the leak is local.

SensApp gets SQLite through `sqlx-sqlite` 0.9.0, which bundles it through `libsqlite3-sys`
`>=0.30.1, <0.38.0` (0.37.0 bundles SQLite 3.51.3). The fix is `libsqlite3-sys` 0.39 (SQLite 3.54).
No sqlx release accepts it yet (checked 10 November 2026). SensApp only opens the database file the
operator configures, so the exposure is that whoever writes there can already change the data.

When sqlx releases a version that accepts `libsqlite3-sys` 0.39:

1. Update sqlx and check `cargo tree -i libsqlite3-sys` shows 0.39 or later.
2. Remove the `--ignore` and its comment from `Makefile.toml`.
3. Run the SQLite test suite and `cargo audit`.
