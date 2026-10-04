# Two `http::metrics` unit tests only pass when another test loaded the configuration

`http::metrics::tests::latest_prometheus_line_uses_original_timestamp` and
`scrape_compatibility_requires_numeric_sensor_and_valid_names` call `config::get()`, which is only set once some
test ran `load_configuration_for_tests()`. The full `cargo test --lib` passes, but
`cargo test --lib http::metrics` (or `http::`) alone fails with "Configuration not loaded". Seen on `main` on
4 October 2026. Call `load_configuration_for_tests()` at the start of both tests.
