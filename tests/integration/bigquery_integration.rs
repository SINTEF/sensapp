//! What is specific to BigQuery. The backend-generic suites run on it too
//! (`TEST_DATABASE_URL=bigquery://... cargo test --no-default-features --features bigquery
//! --test integration`), see `docs/BIGQUERY.md` for the setup and the cost.
//!
//! These tests need a real project: without a `bigquery:` `TEST_DATABASE_URL` they say so and pass.

#[cfg(feature = "bigquery")]
mod bigquery_tests {
    use crate::common::{DatabaseType, TestDb};
    use anyhow::Result;
    use rust_decimal::Decimal;
    use sensapp::config::load_configuration_for_tests;
    use sensapp::datamodel::batch_builder::BatchBuilder;
    use sensapp::datamodel::sensapp_vec::SensAppLabels;
    use sensapp::datamodel::unit::Unit;
    use sensapp::datamodel::{Sample, Sensor, SensorType, TypedSamples};
    use sensapp::storage::query::LabelMatcher;
    use sensapp::storage::storage_factory::create_storage_from_connection_string;
    use sensapp::storage::{StorageError, StorageInstance};
    use serial_test::serial;
    use smallvec::smallvec;
    use std::str::FromStr;
    use std::sync::Arc;
    use uuid::Uuid;

    static INIT: std::sync::Once = std::sync::Once::new();

    /// The test database, or `None` (and a message) when no BigQuery dataset is configured.
    async fn open() -> Result<Option<TestDb>> {
        INIT.call_once(|| {
            load_configuration_for_tests().expect("Failed to load configuration for tests");
        });
        if !std::env::var("TEST_DATABASE_URL").is_ok_and(|url| url.starts_with("bigquery:")) {
            eprintln!("skipping: TEST_DATABASE_URL is not a bigquery:// connection string");
            return Ok(None);
        }
        Ok(Some(TestDb::new_with_type(DatabaseType::BigQuery).await?))
    }

    fn at(seconds: f64) -> hifitime::Epoch {
        hifitime::Epoch::from_unix_seconds(seconds)
    }

    fn sensor(name: &str, sensor_type: SensorType, labels: &[(&str, &str)]) -> Arc<Sensor> {
        let labels: SensAppLabels = labels
            .iter()
            .map(|(name, value)| (name.to_string(), value.to_string()))
            .collect();
        Arc::new(Sensor::new(
            Uuid::new_v4(),
            name.to_string(),
            sensor_type,
            None,
            Some(labels),
        ))
    }

    async fn publish(
        storage: &Arc<dyn StorageInstance>,
        sensors: Vec<(Arc<Sensor>, TypedSamples)>,
    ) -> Result<()> {
        let mut builder = BatchBuilder::new()?;
        for (sensor, samples) in sensors {
            builder.add(sensor, samples).await?;
        }
        builder.send_what_is_left(storage.clone()).await?;
        Ok(())
    }

    #[tokio::test]
    #[serial]
    async fn migration_can_run_again_and_the_health_check_passes() -> Result<()> {
        let Some(db) = open().await? else {
            return Ok(());
        };
        let storage = db.storage();
        storage.create_or_migrate().await?;
        storage.create_or_migrate().await?;
        storage.health_check().await?;
        Ok(())
    }

    /// Floats and locations went through `f32`, JSON values became `""`, NUMERIC lost its digits
    /// beyond the ninth: every type comes back as it was written, microseconds included.
    #[tokio::test]
    #[serial]
    async fn every_type_comes_back_as_written() -> Result<()> {
        let Some(db) = open().await? else {
            return Ok(());
        };
        let storage = db.storage();
        let when = at(1_704_067_200.123_456);

        let float = sensor("bq_float", SensorType::Float, &[("site", "oslo")]);
        let integer = sensor("bq_integer", SensorType::Integer, &[]);
        let numeric = sensor("bq_numeric", SensorType::Numeric, &[]);
        let string = sensor("bq_string", SensorType::String, &[]);
        let boolean = sensor("bq_boolean", SensorType::Boolean, &[]);
        let location = sensor("bq_location", SensorType::Location, &[]);
        let json = sensor("bq_json", SensorType::Json, &[]);
        let blob = sensor("bq_blob", SensorType::Blob, &[]);
        let json_value = serde_json::json!({"a": [1, 2.5, "x"], "b": {"c": null}, "d": true});
        let blob_value: Vec<u8> = (0..=255).collect();

        publish(
            &storage,
            vec![
                (
                    float.clone(),
                    TypedSamples::Float(smallvec![
                        Sample {
                            datetime: when,
                            value: 0.1 + 0.2
                        },
                        Sample {
                            datetime: at(1_704_067_201.0),
                            value: f64::MAX
                        }
                    ]),
                ),
                (
                    integer.clone(),
                    TypedSamples::Integer(smallvec![Sample {
                        datetime: when,
                        value: i64::MIN
                    }]),
                ),
                (
                    numeric.clone(),
                    TypedSamples::Numeric(smallvec![Sample {
                        datetime: when,
                        value: Decimal::from_str("-12345678901234567890.123456789")?
                    }]),
                ),
                (
                    string.clone(),
                    TypedSamples::String(smallvec![Sample {
                        datetime: when,
                        value: "héllo \"wörld\" 🌍\n".to_string()
                    }]),
                ),
                (
                    boolean.clone(),
                    TypedSamples::Boolean(smallvec![Sample {
                        datetime: when,
                        value: false
                    }]),
                ),
                (
                    location.clone(),
                    TypedSamples::Location(smallvec![Sample {
                        datetime: when,
                        value: geo::Point::new(10.123_456_789_012, 59.987_654_321_098)
                    }]),
                ),
                (
                    json.clone(),
                    TypedSamples::Json(smallvec![Sample {
                        datetime: when,
                        value: json_value.clone()
                    }]),
                ),
                (
                    blob.clone(),
                    TypedSamples::Blob(smallvec![Sample {
                        datetime: when,
                        value: blob_value.clone()
                    }]),
                ),
            ],
        )
        .await?;

        let read = |sensor: &Arc<Sensor>| {
            let storage = storage.clone();
            let uuid = sensor.uuid.to_string();
            async move {
                Ok::<_, anyhow::Error>(
                    storage
                        .query_sensor_data(&uuid, None, None, None)
                        .await?
                        .expect("the sensor is stored"),
                )
            }
        };

        let data = read(&float).await?;
        assert_eq!(
            data.sensor.labels.as_slice(),
            &[("site".to_string(), "oslo".to_string())]
        );
        let TypedSamples::Float(samples) = &data.samples else {
            panic!("floats")
        };
        assert_eq!(samples.len(), 2);
        assert_eq!(samples[0].value, 0.1 + 0.2);
        assert_eq!(samples[0].datetime, when);
        assert_eq!(samples[1].value, f64::MAX);

        let TypedSamples::Integer(samples) = &read(&integer).await?.samples else {
            panic!("integers")
        };
        assert_eq!(samples[0].value, i64::MIN);

        let TypedSamples::Numeric(samples) = &read(&numeric).await?.samples else {
            panic!("numerics")
        };
        assert_eq!(
            samples[0].value,
            Decimal::from_str("-12345678901234567890.123456789")?
        );

        let TypedSamples::String(samples) = &read(&string).await?.samples else {
            panic!("strings")
        };
        assert_eq!(samples[0].value, "héllo \"wörld\" 🌍\n");

        let TypedSamples::Boolean(samples) = &read(&boolean).await?.samples else {
            panic!("booleans")
        };
        assert!(!samples[0].value);

        let TypedSamples::Location(samples) = &read(&location).await?.samples else {
            panic!("locations")
        };
        assert_eq!(samples[0].value.x(), 10.123_456_789_012);
        assert_eq!(samples[0].value.y(), 59.987_654_321_098);

        let TypedSamples::Json(samples) = &read(&json).await?.samples else {
            panic!("json")
        };
        assert_eq!(samples[0].value, json_value);

        let TypedSamples::Blob(samples) = &read(&blob).await?.samples else {
            panic!("blobs")
        };
        assert_eq!(samples[0].value, blob_value);
        Ok(())
    }

    #[tokio::test]
    #[serial]
    async fn a_unit_and_a_time_window_are_kept() -> Result<()> {
        let Some(db) = open().await? else {
            return Ok(());
        };
        let storage = db.storage();
        let temperature = Arc::new(Sensor::new(
            Uuid::new_v4(),
            "bq_temperature".to_string(),
            SensorType::Float,
            Some(Unit::new(
                "°C".to_string(),
                Some("degrees Celsius".to_string()),
            )),
            None,
        ));
        publish(
            &storage,
            vec![(
                temperature.clone(),
                TypedSamples::Float(
                    (0..10)
                        .map(|i| Sample {
                            datetime: at(1_704_067_200.0 + f64::from(i) * 60.0),
                            value: f64::from(i),
                        })
                        .collect(),
                ),
            )],
        )
        .await?;

        let uuid = temperature.uuid.to_string();
        let window = storage
            .query_sensor_data(
                &uuid,
                Some(at(1_704_067_200.0 + 120.0)),
                Some(at(1_704_067_200.0 + 300.0)),
                None,
            )
            .await?
            .unwrap();
        let unit = window.sensor.unit.clone().unwrap();
        assert_eq!(unit.name, "°C");
        assert_eq!(unit.description.as_deref(), Some("degrees Celsius"));
        let TypedSamples::Float(samples) = &window.samples else {
            panic!("floats")
        };
        let values: Vec<f64> = samples.iter().map(|s| s.value).collect();
        assert_eq!(
            values,
            vec![2.0, 3.0, 4.0, 5.0],
            "both bounds are inclusive"
        );

        let limited = storage
            .query_sensor_data(&uuid, None, None, Some(3))
            .await?
            .unwrap();
        assert_eq!(limited.samples.len(), 3);

        let latest = storage
            .query_sensor_data_latest(&uuid, None, None)
            .await?
            .unwrap();
        let TypedSamples::Float(samples) = &latest.samples else {
            panic!("floats")
        };
        assert_eq!(samples.len(), 1);
        assert_eq!(samples[0].value, 9.0);
        Ok(())
    }

    /// Two instances (their own caches) and many concurrent writers register the same series: one
    /// series is listed, with its labels once, and every sample is there.
    #[tokio::test]
    #[serial]
    async fn concurrent_instances_registering_one_series_leave_one_series() -> Result<()> {
        let Some(db) = open().await? else {
            return Ok(());
        };
        let url = std::env::var("TEST_DATABASE_URL")?;
        let other: Arc<dyn StorageInstance> = create_storage_from_connection_string(&url).await?;
        let storage = db.storage();
        let shared = sensor(
            "bq_shared",
            SensorType::Integer,
            &[("a", "1"), ("b", "2"), ("c", "3")],
        );

        let writers = (0..8).map(|writer| {
            let storage = if writer % 2 == 0 {
                storage.clone()
            } else {
                other.clone()
            };
            let shared = shared.clone();
            async move {
                publish(
                    &storage,
                    vec![(
                        shared,
                        TypedSamples::Integer(smallvec![Sample {
                            datetime: at(1_704_067_200.0 + f64::from(writer)),
                            value: i64::from(writer),
                        }]),
                    )],
                )
                .await
            }
        });
        for result in futures::future::join_all(writers).await {
            result?;
        }

        let listed = storage.list_series(Some("bq_shared"), None, None).await?;
        assert_eq!(listed.series.len(), 1, "{:?}", listed.series);
        assert_eq!(listed.series[0].labels.len(), 3, "labels once each");
        let data = storage
            .query_sensor_data(&shared.uuid.to_string(), None, None, None)
            .await?
            .unwrap();
        assert_eq!(data.samples.len(), 8);
        assert_eq!(data.sensor.labels.len(), 3);
        Ok(())
    }

    #[tokio::test]
    #[serial]
    async fn the_listing_pages_through_the_series_and_filters_by_metric() -> Result<()> {
        let Some(db) = open().await? else {
            return Ok(());
        };
        let storage = db.storage();
        let sensors: Vec<_> = (0..5)
            .map(|i| {
                (
                    sensor(
                        "bq_paged",
                        SensorType::Integer,
                        &[("index", &i.to_string())],
                    ),
                    TypedSamples::one_integer(i, at(1_704_067_200.0)),
                )
            })
            .chain([(
                sensor("bq_other", SensorType::Integer, &[]),
                TypedSamples::one_integer(0, at(1_704_067_200.0)),
            )])
            .collect();
        let expected: std::collections::BTreeSet<_> = sensors
            .iter()
            .filter(|(sensor, _)| sensor.name == "bq_paged")
            .map(|(sensor, _)| sensor.uuid)
            .collect();
        publish(&storage, sensors).await?;

        let mut seen = Vec::new();
        let mut bookmark: Option<String> = None;
        let mut pages = Vec::new();
        loop {
            let page = storage
                .list_series(Some("bq_paged"), Some(2), bookmark.as_deref())
                .await?;
            pages.push(page.series.len());
            seen.extend(page.series.iter().map(|sensor| sensor.uuid));
            bookmark = page.bookmark;
            if bookmark.is_none() {
                break;
            }
        }
        assert_eq!(pages, vec![2, 2, 1]);
        assert_eq!(seen.len(), 5);
        assert_eq!(
            seen.iter()
                .copied()
                .collect::<std::collections::BTreeSet<_>>(),
            expected
        );

        let metrics = storage.list_metrics().await?;
        let paged = metrics.iter().find(|m| m.name == "bq_paged").unwrap();
        assert_eq!(paged.series_count, 5);
        assert!(
            storage
                .list_series(None, Some(1), Some("not a number"))
                .await
                .is_err()
        );
        Ok(())
    }

    #[tokio::test]
    #[serial]
    async fn label_selectors_use_prometheus_semantics() -> Result<()> {
        let Some(db) = open().await? else {
            return Ok(());
        };
        let storage = db.storage();
        let oslo = sensor(
            "bq_sel",
            SensorType::Float,
            &[("site", "oslo"), ("env", "prod")],
        );
        let bergen = sensor("bq_sel", SensorType::Float, &[("site", "bergen")]);
        let text = sensor(
            "bq_sel",
            SensorType::String,
            &[("site", "oslo"), ("kind", "text")],
        );
        publish(
            &storage,
            vec![
                (
                    oslo.clone(),
                    TypedSamples::Float(smallvec![Sample {
                        datetime: at(1_704_067_200.0),
                        value: 1.0
                    }]),
                ),
                (
                    bergen.clone(),
                    TypedSamples::Float(smallvec![Sample {
                        datetime: at(1_704_067_200.0),
                        value: 2.0
                    }]),
                ),
                (
                    text.clone(),
                    TypedSamples::String(smallvec![Sample {
                        datetime: at(1_704_067_200.0),
                        value: "x".to_string()
                    }]),
                ),
            ],
        )
        .await?;

        let find = |matchers: Vec<LabelMatcher>, numeric_only: bool| {
            let storage = storage.clone();
            async move {
                let mut uuids: Vec<Uuid> = storage
                    .query_sensors_by_labels(&matchers, None, None, None, numeric_only)
                    .await?
                    .into_iter()
                    .map(|data| data.sensor.uuid)
                    .collect();
                uuids.sort();
                Ok::<_, anyhow::Error>(uuids)
            }
        };
        let sorted = |mut uuids: Vec<Uuid>| {
            uuids.sort();
            uuids
        };
        let name = || LabelMatcher::eq("__name__", "bq_sel");

        assert_eq!(
            find(vec![name(), LabelMatcher::eq("site", "oslo")], false).await?,
            sorted(vec![oslo.uuid, text.uuid])
        );
        assert_eq!(
            find(vec![name(), LabelMatcher::eq("site", "oslo")], true).await?,
            vec![oslo.uuid],
            "numeric only leaves the string series out"
        );
        assert_eq!(
            find(vec![name(), LabelMatcher::neq("env", "prod")], true).await?,
            vec![bergen.uuid],
            "a series without the label is not equal to it"
        );
        assert_eq!(
            find(vec![name(), LabelMatcher::regex("site", "o.*")], true).await?,
            vec![oslo.uuid]
        );
        assert_eq!(
            find(vec![name(), LabelMatcher::regex("site", "o")], true).await?,
            Vec::<Uuid>::new(),
            "regexes are anchored"
        );
        assert_eq!(
            find(
                vec![
                    LabelMatcher::regex("__name__", "bq_s.l"),
                    LabelMatcher::not_regex("site", "oslo")
                ],
                true
            )
            .await?,
            vec![bergen.uuid]
        );
        assert!(find(vec![], false).await?.is_empty());
        // A quote in a value is data, not SQL
        assert!(
            find(vec![LabelMatcher::eq("site", "o' OR '1'='1")], false)
                .await?
                .is_empty()
        );
        assert!(matches!(
            storage
                .query_sensor_data("not a uuid", None, None, None)
                .await
                .unwrap_err()
                .downcast_ref::<StorageError>(),
            Some(StorageError::InvalidDataFormat { .. })
        ));
        Ok(())
    }

    /// Aggregation is done by BigQuery (a `GROUP BY` of buckets), not on the samples read back: every
    /// numeric type and aggregation gives what the local reference computes from the raw samples.
    /// The values are multiples of a quarter, so that no order of summation changes a result.
    #[tokio::test]
    #[serial]
    async fn aggregation_in_bigquery_equals_the_local_reference() -> Result<()> {
        use sensapp::storage::common::apply_query_options;
        use sensapp::storage::{Aggregation, SensorDataQueryOptions};

        let Some(db) = open().await? else {
            return Ok(());
        };
        let storage = db.storage();
        let start = 1_704_067_200.0 + 1_234.0; // not aligned on anything
        // Three days, a sample every 37 minutes
        let times: Vec<f64> = (0..117).map(|i| start + f64::from(i) * 2_220.0).collect();
        let integer = sensor("bq_agg_integer", SensorType::Integer, &[]);
        let float = sensor("bq_agg_float", SensorType::Float, &[]);
        let numeric = sensor("bq_agg_numeric", SensorType::Numeric, &[]);
        publish(
            &storage,
            vec![
                (
                    integer.clone(),
                    TypedSamples::Integer(
                        times
                            .iter()
                            .enumerate()
                            .map(|(i, t)| Sample {
                                datetime: at(*t),
                                value: (i as i64 * 7) % 23 - 11,
                            })
                            .collect(),
                    ),
                ),
                (
                    float.clone(),
                    TypedSamples::Float(
                        times
                            .iter()
                            .enumerate()
                            .map(|(i, t)| Sample {
                                datetime: at(*t),
                                value: ((i * 5) % 17) as f64 * 0.25 - 2.0,
                            })
                            .collect(),
                    ),
                ),
                (
                    numeric.clone(),
                    TypedSamples::Numeric(
                        times
                            .iter()
                            .enumerate()
                            .map(|(i, t)| Sample {
                                datetime: at(*t),
                                value: Decimal::new(((i * 3) % 19) as i64 * 25, 2),
                            })
                            .collect(),
                    ),
                ),
            ],
        )
        .await?;

        // A window that starts and ends inside buckets of 6 hours
        let window_start = Some(at(start + 3_000.0));
        let window_end = Some(at(start + 2.5 * 86_400.0));
        for sensor in [&integer, &float, &numeric] {
            let uuid = sensor.uuid.to_string();
            for aggregation in [
                Aggregation::Avg,
                Aggregation::Min,
                Aggregation::Max,
                Aggregation::Sum,
                Aggregation::Count,
                Aggregation::First,
                Aggregation::Last,
                Aggregation::Latest,
            ] {
                let options = SensorDataQueryOptions {
                    start_time: window_start,
                    end_time: window_end,
                    limit: None,
                    step_ms: Some(6 * 3_600_000),
                    aggregation: Some(aggregation),
                    simplify: None,
                };
                let raw = storage
                    .query_sensor_data(&uuid, window_start, window_end, None)
                    .await?
                    .unwrap();
                let expected = apply_query_options(raw, &options)?;
                let got = storage
                    .query_sensor_data_advanced(&uuid, &options)
                    .await?
                    .unwrap();

                let what = format!("{} {aggregation:?}", sensor.name);
                assert_eq!(
                    got.sensor.sensor_type, expected.sensor.sensor_type,
                    "{what}"
                );
                assert_eq!(got.sensor.unit, expected.sensor.unit, "{what}");
                // Averages: BigQuery's AVG is not the exact sum over the count in the last bits
                // (-5.5e-17 for integers that average to 0), and it rounds NUMERIC to 9 digits
                match (&got.samples, &expected.samples) {
                    (TypedSamples::Float(got), TypedSamples::Float(expected))
                        if aggregation == Aggregation::Avg =>
                    {
                        assert_eq!(got.len(), expected.len(), "{what}");
                        for (got, expected) in got.iter().zip(expected) {
                            assert_eq!(got.datetime, expected.datetime, "{what}");
                            assert!(
                                (got.value - expected.value).abs() < 1e-9,
                                "{what}: {} against {}",
                                got.value,
                                expected.value
                            );
                        }
                    }
                    (TypedSamples::Numeric(got), TypedSamples::Numeric(expected))
                        if aggregation == Aggregation::Avg =>
                    {
                        assert_eq!(got.len(), expected.len(), "{what}");
                        for (got, expected) in got.iter().zip(expected) {
                            assert_eq!(got.datetime, expected.datetime, "{what}");
                            assert!(
                                (got.value - expected.value).abs() < Decimal::new(1, 8),
                                "{what}"
                            );
                        }
                    }
                    (got, expected) => assert_eq!(got, expected, "{what}"),
                }
                assert!(got.samples.len() > 8, "{what}: several buckets");
            }

            // The limit counts buckets
            let limited = storage
                .query_sensor_data_advanced(
                    &uuid,
                    &SensorDataQueryOptions {
                        start_time: window_start,
                        end_time: window_end,
                        limit: Some(3),
                        step_ms: Some(6 * 3_600_000),
                        aggregation: Some(Aggregation::Max),
                        simplify: None,
                    },
                )
                .await?
                .unwrap();
            assert_eq!(limited.samples.len(), 3);
        }

        // Only numbers are aggregated
        let text = sensor("bq_agg_text", SensorType::String, &[]);
        publish(
            &storage,
            vec![(
                text.clone(),
                TypedSamples::String(smallvec![Sample {
                    datetime: at(start),
                    value: "x".to_string()
                }]),
            )],
        )
        .await?;
        assert!(
            storage
                .query_sensor_data_advanced(
                    &text.uuid.to_string(),
                    &SensorDataQueryOptions {
                        start_time: None,
                        end_time: None,
                        limit: None,
                        step_ms: Some(1000),
                        aggregation: Some(Aggregation::Count),
                        simplify: None,
                    },
                )
                .await
                .is_err()
        );
        Ok(())
    }

    /// The first answer of a query is at most 10 MB: a result over that needs the next pages.
    #[tokio::test]
    #[serial]
    async fn a_result_over_one_page_is_read_whole() -> Result<()> {
        let Some(db) = open().await? else {
            return Ok(());
        };
        let storage = db.storage();
        const COUNT: usize = 150_000;
        let big = sensor("bq_big", SensorType::Float, &[]);
        publish(
            &storage,
            vec![(
                big.clone(),
                TypedSamples::Float(
                    (0..COUNT)
                        .map(|i| Sample {
                            datetime: at(1_704_067_200.0 + i as f64),
                            value: i as f64 + 0.5,
                        })
                        .collect(),
                ),
            )],
        )
        .await?;

        let data = storage
            .query_sensor_data(&big.uuid.to_string(), None, None, None)
            .await?
            .unwrap();
        let TypedSamples::Float(samples) = &data.samples else {
            panic!("floats")
        };
        assert_eq!(samples.len(), COUNT);
        assert!(
            samples
                .windows(2)
                .all(|pair| pair[0].datetime < pair[1].datetime)
        );
        assert_eq!(samples[COUNT - 1].value, COUNT as f64 - 0.5);
        Ok(())
    }

    #[tokio::test]
    #[serial]
    async fn the_cost_cap_of_the_connection_string_stops_expensive_queries() -> Result<()> {
        let Some(db) = open().await? else {
            return Ok(());
        };
        db.storage().create_or_migrate().await?;
        let url = std::env::var("TEST_DATABASE_URL")?;
        let separator = if url.contains('?') { '&' } else { '?' };
        let capped =
            create_storage_from_connection_string(&format!("{url}{separator}max_bytes_billed=1"))
                .await?;
        // The statement that looks at the metadata of the dataset bills at least 10 MB. (A read of
        // rows that were just written is not billed: they are in the streaming buffer, so a cap
        // cannot be tested on them.)
        let error = capped
            .create_or_migrate()
            .await
            .expect_err("a statement over the cap fails");
        assert!(
            matches!(
                error.downcast_ref::<StorageError>(),
                Some(StorageError::OperationFailed { .. })
            ),
            "{error:#}"
        );
        Ok(())
    }
}
