# Virtual Composite Sensors

## Status

Design idea only, see `docs/DATAMODEL.md`. Not implemented.

## Notes

Query-time cross-series aggregation now covers the common case. A composite sensor could become a stored definition of such a query (selector, aggregation, grouping, step), exposed as a read-only series and materialised only if read performances require it.

Reconsidered in November 2026: labels and selectors already do the grouping (`{station="oslo-1"}` is the weather station), so a stored composite would be a second way to say the same thing. The part that is missing is a table with one column per sensor, and that is an output format, not a new kind of sensor. Clients can pivot the long format themselves.

## Revisit If

Users repeatedly save or share the same aggregation queries, or need to alert or authorise on the combined series.

For the table alone: a user asks for a spreadsheet-ready export. Start with the Download button of the frontend, not the API.
