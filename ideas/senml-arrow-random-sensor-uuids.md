# SenML and Arrow imports create a new series on every publish

Found while testing the delete endpoints against the real server.

`src/importers/senml.rs` and `src/importers/arrow.rs` build sensors with `Uuid::new_v4()`. CSV, InfluxDB
and Prometheus remote write use `Sensor::new_without_uuid`, which derives the UUID from the name,
type, unit and labels. So publishing SenML for `"n":"temp"` three times creates three different
`temp` series, each with its own samples, and never appends to the first one.

Consequences:

- a series cannot be extended over several SenML/Arrow requests, which is the main use of a sensor API;
- the correction recipe in `docs/DATA_LIFECYCLE.md` (delete a range, publish it again) does not put
  the data back in the original series.

Fix to evaluate: use `Sensor::new_without_uuid` in both importers. That changes existing series
identities for anyone already ingesting SenML, which is acceptable at this stage (pre-production,
breaking changes are fine). Add tests that two identical SenML publishes produce one series.
