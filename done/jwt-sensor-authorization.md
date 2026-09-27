# JWT Sensor Authorization

## Goal

Close the gap between the JWT `sensors` claim and the authorization actually enforced by the HTTP API.

## Context

Read and write middleware currently validates the JWT and its scope, but ignores the sensor allow list. `/prometheus/metrics` can also expose latest sensor samples without authentication.

## Work

- Decide whether to enforce sensor-level authorization on every data path or remove the unsupported claim and reject restricted tokens. Keep the rule simple and explicit.
- Prevent the public service metrics route from exposing sensor sample values without suitable authorization.
- Update token generation, documentation, and tests to match the chosen behavior.

## Done when

Restricted tokens cannot read or write sensors outside their permission. With JWT enabled, public monitoring cannot reveal sensor values.

## Progress

- Added a per-request storage authorization wrapper for the HTTP data paths.
- Auth middleware now passes the validated access context to handlers.
- Latest-sample metrics require a read token when JWT auth is enabled.
- Preserved batch errors so forbidden writes return 403.
- Added a focused integration test for allowed and denied writes, catalog and query filtering, direct series access, and metrics scraping.
