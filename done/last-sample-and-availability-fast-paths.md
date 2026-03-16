# Last Sample And Availability Fast Paths

## Goal

Replace the generic full-series fallback used by the new last-sample and availability APIs with backend-native query paths.

## Scope

- add storage-native latest-sample queries
- add storage-native availability summary queries
- wire the HTTP availability endpoint to the summary API
- cover backend behavior with targeted tests

## Notes

- keep the generic fallback in the trait as a safety net
- prioritize the same backends that already have native advanced-query pushdown
