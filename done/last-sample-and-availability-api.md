# Last Sample And Availability API

## Goal

Add the two follow-up APIs identified after the series downsampling work:

- a dedicated last-sample endpoint for a single series
- a time-window availability endpoint exposing existence and optional bucket coverage

## Delivered

- added `GET /series/{series_uuid}/last`
- added `GET /series/{series_uuid}/availability`
- added a generic storage fallback for latest-sample retrieval
- added endpoint tests covering latest sample, bounded latest sample, and availability coverage
- cleaned up repository-wide clippy blockers encountered during validation

## API Shape

### Last sample

`GET /series/{series_uuid}/last`

Optional query parameters:

- `start`
- `end`

Response includes:

- `series_uuid`
- `sensor_name`
- `sensor_type`
- `unit`
- `timestamp`
- `value`

### Availability

`GET /series/{series_uuid}/availability`

Required query parameters:

- `start`
- `end`

Optional query parameter:

- `step`

Response includes:

- `present`
- `sample_count`
- `first_sample_at`
- `last_sample_at`
- optional `covered_buckets`
- optional `total_buckets`
- optional `coverage_ratio`

## Validation

- `cargo check`
- `cargo test --test query_export`
- `cargo clippy --tests -- -D warnings`