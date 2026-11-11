# CSV import: timezone sign and timestamp inference

Source: code review of 10 November 2026. The timezone sign was confirmed by reading the code and the
`iso8601` crate; the rest was reported by a reviewer and read in part.

## Problem

1. **Timezone offsets are added instead of subtracted** (`src/infer/parsing.rs:126-131`). The `iso8601`
   crate returns `+2` for `+02:00`. `2024-06-01T12:00:00+02:00` is stored as 14:00Z; the right value is
   10:00Z. The unit test at `parsing.rs:604-609` asserts the wrong result (`14:21:32+05:35` expected as
   19:56:32Z, the right value is 08:46:32Z).
2. **Row index used as the timestamp** (`src/importers/csv.rs:111-114`). In long format (`name` + `value`
   columns), when no datetime column is detected, samples are stored at 1970-01-01 plus the row number, and
   the request succeeds. Detection fails easily: the parser requires a `T` separator, so
   `2024-01-01 12:00:00` is a String column, and one empty cell makes the whole column String
   (`src/infer/columns.rs:58-61`). Wide format returns an error in the same situation.
3. **Integer time columns are always seconds** (`csv.rs:218-220`). Epoch milliseconds are stored around year
   55,970.
4. **False-positive time column** (`src/infer/datetime_guesser.rs:43-63`, threshold at `:100`): an integer
   column whose name contains `time` or `date`, or ends in `at`, becomes the time axis when the real date
   column is not parsed (`date,temperature,uptime` with date-only values).
5. **Float epoch column**: wins the guess but `parse_datetime_from_inferred` only handles DateTime and
   Integer; the request fails with a 500 whose log line dumps the whole column (`csv.rs:228-231`).
6. **Silent loss in datetime parsing** (`parsing.rs:114`, `:156-158`): fractions truncated to milliseconds,
   trailing text after the datetime ignored (`2024-01-01T12:00:00 +0200` loses its offset), a datetime
   without an offset taken as UTC, week dates computed from 1 January.

## Suggested solution

- Fix the sign (subtract the offset) and the test. Add cases for `+02:00`, `-05:00`, `+05:30`.
- Long format without a datetime column: return a 400 that names the columns found, like wide format.
- Accept a space as the date/time separator (normalise it to `T` before parsing), since it is the pandas
  default.
- Integer and float time columns: infer the unit from the magnitude (seconds, milliseconds, microseconds,
  nanoseconds), the usual 1e11 / 1e14 / 1e17 thresholds, or better an explicit query parameter with the
  inference as the default. Document it.
- Require the whole cell to be consumed by the datetime parser; reject trailing input.
- Keep microsecond fractions (parse the fraction ourselves if the crate keeps three digits).
- Raise the bar for a time column guessed from its name alone: when another column looks like dates but
  failed to parse, answer 400 instead of picking the integer column.
- Consider whether the `iso8601` crate is still the right parser; `chrono` or `time` handle offsets,
  fractions and the space separator.

## Tests

Parser unit tests for each case above, and integration tests through `/publish` for: offset datetimes, long
format without a datetime column (400), epoch milliseconds, a `date` + `uptime` file, a float epoch column.

## Done when

The tests pass and `docs/` describes how CSV timestamps are read.

## Outcome (10 November 2026)

| Item | Result | Commit |
| --- | --- | --- |
| 1. Offset sign | Confirmed, and two more unit tests asserted the wrong sign (`00:00+01:00` expected at 01:00Z, `00:00+02:00` at 02:00Z). | `34e49d8` |
| 2. Row index as time | Confirmed (long format stored 1970 + row). **A CSV without a time column is now refused (400)** in both layouts, with the columns found and what a time column looks like. Rows are never numbered as times. | `719ebe2` |
| 3. Integer epochs always seconds | Confirmed (milliseconds became year 56387, and since task 4 that is a 400). The unit comes from the largest value of the column: under 10^11 seconds, 10^14 milliseconds, 10^17 microseconds, nanoseconds above. No query parameter: the thresholds cover today's data in every unit, and KISS. | `719ebe2` |
| 4. False-positive time column | Confirmed with `01/06/2024` dates. Two changes: a date alone (`2024-06-01`) is now a time, so the reported `date,temperature,uptime` file works; and a number column that does not read as times only counts as the time axis when its name is an exact time name (`time`, `date`, `created_at`…), not when it only contains `time` (`uptime`) — then the CSV is refused. The `time` column of relative times (the ebike test data) is still chosen. "Reads as times" now accepts all four epoch units. | `34e49d8`, `719ebe2` |
| 5. Float epoch column | Confirmed (500 with the whole column in the message). Same unit rule, rounded to the microsecond. The error for any other column type is short and names the column. | `719ebe2` |
| 6. Silent loss | Confirmed. **Decision: the `iso8601` crate is replaced by chrono** (already in the tree through `sindit-senml`). Fractions keep microseconds; trailing text makes the cell a string; an offset after a space (`… +0200`) is applied instead of dropped; a space is accepted as the date/time separator; no offset is UTC (documented). Week and ordinal dates are no longer recognised (they were computed from 1 January, wrongly): they are strings. | `34e49d8` |

One more change, found on the way: a column of times that is not the time axis was stored as RFC 3339 text
(`2024-07-01` became `2024-07-01T00:00:00 UTC`). It is now a string series with its cells as written. Without it,
parsing dates alone would have rewritten date-valued string series.

Tests: parser unit tests (`infer::parsing::tests::datetime`: offsets, local forms, fractions, other text), guesser
unit tests, and `tests/integration/csv_timestamps.rs` through `/publish`: offset datetimes, a space separator,
epochs in four units, a float epoch, a `date` + `uptime` file, a value column of dates, and three CSVs without a
usable time column (400). `parser_edge_cases` now expects a date alone to be a time.

Docs: `docs/CSV_IMPORT.md` describes the layouts, the choice of the time column, the time formats and the epoch
units.

Suites: SQLite and PostgreSQL in full (no backend code changed). `cargo clippy --tests`: clean.
