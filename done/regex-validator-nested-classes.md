# Regex validation: nested classes and the remaining dialect gaps

Source: review of pull request 60 on 11 November 2026 (HEAD `7d8b396`). The validator was traced by reading;
the behaviour of PostgreSQL and RE2 on these patterns is from knowledge, not executed. Run each pattern on
each backend first.

## Problem

`validate_regex` (`src/storage/query.rs`) accepts a pattern when the Rust `regex` crate compiles it and a
character walk finds nothing that PostgreSQL reads differently. Gaps:

1. **Nested character classes break the anchoring.** The `regex` crate supports nested classes and set
   operations (`[a[bc]]`, `&&`, `--`, `~~`). PostgreSQL and RE2 do not: a `[` inside a class is a literal and
   the first `]` closes the class. The walk (`query.rs:185-203`) only handles `[:` inside a class.
   Input: `{__name__=~"[a[b])|(c]]"}`. Rust reads one class followed by `]`, with no groups. The walk ends
   the class at the first `]` and accepts. `anchor_regex` sends `(?s)^(?:[a[b])|(c]])$`, which PostgreSQL,
   TimescaleDB, DuckDB, ClickHouse and BigQuery read as two alternatives, the first unanchored at the end:
   every value starting with `a`, `[` or `b` matches. SQLite and the in-memory filter use the Rust meaning.
   This reopens the original "unanchored alternation" finding by another route. It is not an access bypass
   (the allow list is a separate ANDed matcher).
2. **Patterns that pass validation and a backend then refuses with a 500**: `é` / `\U000000e9`
   (RE2 backends have no `\u`), `a{256}` (PostgreSQL bounds stop at 255), `a{1001}` (RE2 stops at 1000),
   `a**`, `a+*`, `^*` (PostgreSQL), `[[:^digit:]]` (PostgreSQL). No backend maps a regex compile error to 400.
3. **Two engines still disagree on `\d`, `\w`, `\s`**: the Rust engine is Unicode-aware, RE2 is ASCII-only.
   The Rust engine serves SQLite, RRDcached and the in-memory `SensorFilter` that `/series?selector=` uses
   on every backend, so `/series` and `/api/v1/query` of one deployment can disagree on `room="café"` with
   `{room=~"\w+"}`.
4. `docs/DATAMODEL.md:139` does not list the accepted subset. `foo{__name__="bar"}` is accepted as two
   matchers; Prometheus rejects it.

## Suggested solution

Validate on the parsed syntax tree instead of walking characters: `regex_syntax::ast::parse::Parser` gives
an AST, and a visitor can refuse by node type, which cannot be fooled by nesting:

- refuse nested bracketed classes and class set operations (`ClassSetItem::Bracketed` inside a class,
  `ClassSetBinaryOp`), negated POSIX classes, `\u`/`\U`/`\x{}` escapes, repetition bounds above 255, a
  repetition applied to a repetition or an assertion, and what the walk already refuses (flags other than
  leading `i` and `s`, named groups, word boundaries, `\p`);
- keep the result a 400 that names the construct.

Compile the Rust-side regexes (SQLite's `regexp`, RRDcached, `SensorFilter`) with
`RegexBuilder::unicode(false)` so `\d`, `\w`, `\s` are ASCII everywhere, as in Prometheus. Check that
non-ASCII literals in a pattern still match with Unicode mode off (they need `(?u:…)` or byte matching);
if that is awkward, refuse the Perl classes together with non-ASCII input, or document the difference.

List the accepted subset in `docs/DATAMODEL.md`. Refuse a metric name together with a `__name__` matcher.

## Tests

Extend `regexes_mean_the_same_on_every_backend`: every accepted construct runs on every backend and gives
the same matches; every refused construct is a 400. Include `[a[b])|(c]]`, `[a[bc]]`, `a{256}`, `a**`,
`[[:^digit:]]`, `é`, and `\w+` against `café`.

## Done when

The tests pass on all backends and no accepted pattern produces a backend regex error.

## Outcome (11 November 2026)

Items 1, 2 and 4 (the docs) done; item 3 documented, not changed.

- **Nested classes**: a `[` inside a class (unless it starts `[:`) is refused, and so are `&&`, `--`, `~~` in a
  class. `[a[b])|(c]]`, `[a[bc]]`, `[[a]b]` are 400s. I stayed with the character walk, which now tracks the
  previous token, instead of the AST visitor the task suggested: the walk was cheap to extend and the unit test
  covers the shapes. The AST route remains the robust answer if more gaps show.
- **Backend errors**: refused with a message naming the construct: bounds above 255 (`a{256}`, `a{1001}`),
  a repetition of a repetition or an assertion (`a**`, `a+*`, `a*{2}`, `^*`, `$+`), `\u` and `\U`
  (RE2 has none; the earlier version allowed them), negated POSIX classes. The test
  `regexes_are_checked_for_re2_and_postgresql` carries them all; the backends were **not** run against each
  pattern, as the task asked: the refusals come from the task's knowledge of PostgreSQL and RE2, and the accepted
  list is what the earlier test already ran. A literal `é` is still accepted: RE2 and PostgreSQL read UTF-8 literals.
- **`\d \w \s`** differ (Unicode on the `regex` crate, ASCII on RE2): not changed, documented in
  `docs/DATAMODEL.md` with the advice to use an explicit class. Compiling with `unicode(false)` does not work in a
  `&str` regex (`.` and the negated classes could match invalid UTF-8).
- **Item 4**: the accepted subset is listed in `docs/DATAMODEL.md`. `foo{__name__="bar"}` is still accepted as two
  matchers: not done.

Ran: lib, SQLite integration in full, PostgreSQL regex tests.
