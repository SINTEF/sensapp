# sindit-senml: checked arithmetic for relative times

`sindit_senml::time::convert_senml_time` (0.3.0, `src/time.rs:59`) reads a time under 2^28 as relative to
now and computes `now + Duration::seconds(whole_seconds) + Duration::nanoseconds(..)` without a check. A
record with `"t": -1e13` panics (`DateTime + TimeDelta overflowed`), and `Duration::seconds` itself panics
past ±9.2 × 10^15 s.

SensApp works around it since November 2026 (`SenMLImporter::check_times` refuses a `t` or `bt` below
-10^11 s before calling the parser, task `handler-panics-kill-the-instance.md`).

## Idea

In `~/work/sindit-senml`, use `Duration::try_seconds` and `checked_add_signed`, returning `None` like the
absolute branch does, and release a patch version. SensApp can then drop `check_times`, or keep it as a
clearer error message.
