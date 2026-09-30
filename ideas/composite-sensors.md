# Virtual Composite Sensors

## Status

Design idea only, see `docs/DATAMODEL.md`. Not implemented.

## Notes

Query-time cross-series aggregation now covers the common case. A composite sensor could become a stored definition of such a query (selector, aggregation, grouping, step), exposed as a read-only series and materialised only if read performances require it.

## Revisit If

Users repeatedly save or share the same aggregation queries, or need to alert or authorise on the combined series.
