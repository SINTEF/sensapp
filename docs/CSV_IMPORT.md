# CSV import

`POST /publish` with `Content-Type: text/csv` (also the default for an unknown content type) reads a CSV with a
header row, comma separated. A CSV that cannot be read answers `400` with the reason.

## Two layouts

- **Long**: one sample per row, with a series name column (`sensor_name`, `metric`, `name` or `sensor`) and a
  value column (`value`, `reading` or `measurement`), and optionally a `unit` (or `units`) column.

  ```csv
  datetime,sensor_name,value,unit
  2024-06-01T12:00:00Z,temperature,21.5,°C
  ```

- **Wide**: one column per series, named by its header. A header given twice is refused.

  ```csv
  datetime,temperature,humidity
  2024-06-01T12:00:00Z,21.5,48
  ```

Both need a time column. Without one the CSV is refused: rows are never numbered as times.

## The time column

Each column gets a score from its name and its values, and the best one above a threshold is the time axis:

- Its name: `datetime`, `timestamp`, `date`, `time`, `created_at`, `updated_at`, `recorded_at` count the
  most. A name that only contains `date` or `time`, or ends in `at`, counts a little.
- Its values: a column whose every cell is an ISO 8601 time counts the most; a column of numbers that all read
  as times between 2000 and 2118 (in seconds, milliseconds, microseconds or nanoseconds) counts too.
- A column of numbers that do not read as times can still be the time axis when its name is one of the exact
  names above (relative times in a `time` column), but not when its name only contains `time` or `date`:
  `uptime` is never chosen because the dates of a `date` column did not parse.

## Times

**ISO 8601 calendar dates and times**, the whole cell:

| Cell | Read as |
| --- | --- |
| `2024-06-01T12:00:00Z` | 12:00 UTC |
| `2024-06-01T14:00:00+02:00`, `2024-06-01T14:00:00+0200`, `2024-06-01T14:00:00 +0200` | 12:00 UTC: the offset is subtracted |
| `2024-06-01 12:00:00` | 12:00 UTC: a space works as the separator (pandas writes it), no offset means UTC |
| `2024-06-01T12:00` | seconds are optional |
| `2024-06-01T12:00:00.123456Z` | microseconds are kept; finer fractions are rounded down |
| `2024-06-01` | midnight UTC |

Week dates (`2024-W22-6`), ordinal dates (`2024-153`), other orders (`01/06/2024`) and cells with anything after
the time are not times.

**Unix epoch numbers**, integer or decimal. The unit comes from the largest value of the column:

| Largest value | Unit |
| --- | --- |
| under 10^11 | seconds |
| under 10^14 | milliseconds |
| under 10^17 | microseconds |
| above | nanoseconds |

Decimal epochs are rounded to the nearest microsecond. Every time must be within years 1 to 9999, as for every
write (see [DATAMODEL.md](DATAMODEL.md)).

## Values

Each value column gets one type from all its cells: integer, float, boolean, JSON or string (one string cell
makes the column a string series). A column of times that is not the time axis is a string series, with its
cells as written.
