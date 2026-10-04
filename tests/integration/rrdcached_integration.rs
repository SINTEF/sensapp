//! The RRDCached backend against a real `rrdcached` (see `docker/rrdcached/Dockerfile`). The daemon keeps its files between runs, so every test writes
//! series of its own, with a new UUID.
//!
//! What the backend does differently from the others is in `docs/RRDCACHED.md`: RRDtool stores
//! numbers in rows of a fixed step, so what is read back is the value of a row, stamped with the
//! end of its step. The tests write at multiples of the step (10 seconds), where the rows and the
//! samples have the same times.

#[cfg(feature = "rrdcached")]
mod rrdcached_tests {
    use crate::common::{DatabaseType, TestDb};
    use anyhow::Result;
    use sensapp::config::load_configuration_for_tests;
    use sensapp::datamodel::{
        Sample, SensAppDateTime, Sensor, SensorType, TypedSamples,
        batch::{Batch, SingleSensorBatch},
        sensapp_datetime::SensAppDateTimeExt,
        sensapp_vec::SensAppVec,
    };
    use sensapp::storage::{
        LabelMatcher, SelectorLimitExceeded, StorageError, StorageInstance,
        storage_factory::create_storage_from_connection_string,
    };
    use serial_test::serial;
    use smallvec::SmallVec;
    use std::sync::Arc;
    use uuid::Uuid;

    static INIT: std::sync::Once = std::sync::Once::new();
    fn ensure_config() {
        INIT.call_once(|| {
            load_configuration_for_tests().expect("Failed to load configuration for tests");
        });
    }

    /// The address of the daemon of the tests, `host:port`
    fn daemon_address(db: &TestDb) -> String {
        let url = url::Url::parse(&db.connection_string).expect("connection string");
        format!(
            "{}:{}",
            url.host_str().expect("host"),
            url.port().expect("port")
        )
    }

    async fn connect() -> Result<TestDb> {
        ensure_config();
        TestDb::new_with_type(DatabaseType::RRDcached).await
    }

    fn sensor_of(sensor_type: SensorType) -> Arc<Sensor> {
        Arc::new(Sensor {
            uuid: Uuid::new_v4(),
            name: "rrdcached_test".to_string(),
            sensor_type,
            unit: None,
            labels: SmallVec::new(),
        })
    }

    fn new_sensor() -> Arc<Sensor> {
        sensor_of(SensorType::Float)
    }

    fn at(seconds: i64) -> SensAppDateTime {
        SensAppDateTime::from_unix_seconds_i64(seconds)
    }

    fn unix(datetime: &SensAppDateTime) -> i64 {
        datetime.to_unix_seconds().floor() as i64
    }

    /// The start of the current ten seconds
    fn now() -> i64 {
        let now = unix(&SensAppDateTime::now().expect("clock"));
        now - now % 10
    }

    fn float_batch(sensor: &Arc<Sensor>, points: &[(i64, f64)]) -> Arc<Batch> {
        let samples: Vec<Sample<f64>> = points
            .iter()
            .map(|(time, value)| Sample {
                datetime: at(*time),
                value: *value,
            })
            .collect();
        let mut sensors = SensAppVec::new();
        sensors.push(SingleSensorBatch::new(
            sensor.clone(),
            TypedSamples::Float(samples.into()),
        ));
        Arc::new(Batch::new(sensors))
    }

    async fn publish(
        storage: &Arc<dyn StorageInstance>,
        sensor: &Arc<Sensor>,
        points: &[(i64, f64)],
    ) -> Result<()> {
        storage.publish(float_batch(sensor, points)).await
    }

    /// The `(time, value)` rows of a series in a window, `None` when the series does not exist
    async fn read_window(
        storage: &Arc<dyn StorageInstance>,
        sensor: &Sensor,
        start: Option<i64>,
        end: Option<i64>,
        limit: Option<usize>,
    ) -> Result<Option<Vec<(i64, f64)>>> {
        let data = storage
            .query_sensor_data(&sensor.uuid.to_string(), start.map(at), end.map(at), limit)
            .await?;
        Ok(data.map(|data| match data.samples {
            TypedSamples::Float(samples) => samples
                .iter()
                .map(|sample| (unix(&sample.datetime), sample.value))
                .collect(),
            other => panic!("RRDCached only returns floats, got {other:?}"),
        }))
    }

    async fn read(
        storage: &Arc<dyn StorageInstance>,
        sensor: &Sensor,
        start: i64,
        end: i64,
    ) -> Result<Vec<(i64, f64)>> {
        Ok(read_window(storage, sensor, Some(start), Some(end), None)
            .await?
            .expect("the series exists"))
    }

    /// `count` samples every `step` seconds from `first`, with the value `index * scale`
    fn ramp(first: i64, step: i64, count: i64, scale: f64) -> Vec<(i64, f64)> {
        (0..count)
            .map(|i| (first + i * step, i as f64 * scale))
            .collect()
    }

    // ---- connection

    #[tokio::test]
    #[serial]
    async fn the_daemon_is_reachable_and_lists_series() -> Result<()> {
        let db = connect().await?;
        let storage = db.storage();
        storage.create_or_migrate().await?;
        storage.health_check().await?;
        storage.list_series(None, Some(1), None).await?;
        storage.vacuum().await?;
        Ok(())
    }

    #[tokio::test]
    #[serial]
    async fn a_daemon_that_is_not_there_is_an_error_at_startup() {
        ensure_config();
        assert!(
            create_storage_from_connection_string("rrdcached://127.0.0.1:1?preset=hoarder")
                .await
                .is_err()
        );
        assert!(
            create_storage_from_connection_string("rrdcached://127.0.0.1:42217?preset=nope")
                .await
                .is_err()
        );
    }

    // ---- what is written is read back

    #[tokio::test]
    #[serial]
    async fn values_at_the_step_come_back_unchanged() -> Result<()> {
        let db = connect().await?;
        let storage = db.storage();
        let sensor = new_sensor();
        let first = now() - 600;
        let points = ramp(first, 10, 30, 1.5);

        publish(&storage, &sensor, &points).await?;

        // No wait: the daemon flushes what a fetch asks for
        assert_eq!(read(&storage, &sensor, first, first + 290).await?, points);
        Ok(())
    }

    #[tokio::test]
    #[serial]
    async fn samples_a_minute_or_more_apart_are_kept() -> Result<()> {
        // The data source tolerates a gap of an hour: with a gap of 20 seconds, as it used to,
        // only the first of these samples was left
        let db = connect().await?;
        let storage = db.storage();
        let first = now() - 4 * 3600;

        for gap in [60, 600, 1800] {
            let sensor = new_sensor();
            let points = ramp(first, gap, 5, 1.0);
            publish(&storage, &sensor, &points).await?;
            let rows = read(&storage, &sensor, first, first + 4 * gap).await?;
            for (time, value) in &points {
                assert!(
                    rows.contains(&(*time, *value)),
                    "gap {gap}: the sample {time}={value} is missing from {rows:?}"
                );
            }
        }
        Ok(())
    }

    #[tokio::test]
    #[serial]
    async fn the_heartbeat_of_the_connection_string_decides_what_a_gap_is() -> Result<()> {
        let db = connect().await?;
        let strict = create_storage_from_connection_string(&format!(
            "{}&heartbeat=30",
            db.connection_string
        ))
        .await?;
        let sensor = new_sensor();
        let first = now() - 3600;

        // The samples are a minute apart: more than the 30 seconds this connection tolerates
        publish(&strict, &sensor, &ramp(first, 60, 3, 1.0)).await?;
        let rows = read(&strict, &sensor, first, first + 120).await?;
        assert_eq!(
            rows,
            vec![(first, 0.0)],
            "only the first sample has a known value"
        );
        Ok(())
    }

    #[tokio::test]
    #[serial]
    async fn integers_numbers_and_booleans_are_stored_as_floats() -> Result<()> {
        use rust_decimal::Decimal;

        let db = connect().await?;
        let storage = db.storage();
        let first = now() - 60;
        let integer = sensor_of(SensorType::Integer);
        let numeric = sensor_of(SensorType::Numeric);
        let boolean = sensor_of(SensorType::Boolean);

        let mut sensors = SensAppVec::new();
        sensors.push(SingleSensorBatch::new(
            integer.clone(),
            TypedSamples::Integer(
                vec![
                    Sample {
                        datetime: at(first),
                        value: 42i64,
                    },
                    Sample {
                        datetime: at(first + 10),
                        value: -7,
                    },
                ]
                .into(),
            ),
        ));
        sensors.push(SingleSensorBatch::new(
            numeric.clone(),
            TypedSamples::Numeric(
                vec![Sample {
                    datetime: at(first),
                    value: Decimal::new(1234, 2),
                }]
                .into(),
            ),
        ));
        sensors.push(SingleSensorBatch::new(
            boolean.clone(),
            TypedSamples::Boolean(
                vec![
                    Sample {
                        datetime: at(first),
                        value: true,
                    },
                    Sample {
                        datetime: at(first + 10),
                        value: false,
                    },
                ]
                .into(),
            ),
        ));
        storage.publish(Arc::new(Batch::new(sensors))).await?;

        assert_eq!(
            read(&storage, &integer, first, first + 10).await?,
            vec![(first, 42.0), (first + 10, -7.0)]
        );
        assert_eq!(
            read(&storage, &numeric, first, first).await?,
            vec![(first, 12.34)]
        );
        assert_eq!(
            read(&storage, &boolean, first, first + 10).await?,
            vec![(first, 1.0), (first + 10, 0.0)]
        );
        // The type is not stored: everything comes back as a float
        let data = storage
            .query_sensor_data(
                &integer.uuid.to_string(),
                Some(at(first)),
                Some(at(first)),
                None,
            )
            .await?
            .unwrap();
        assert_eq!(data.sensor.sensor_type, SensorType::Float);
        Ok(())
    }

    #[tokio::test]
    #[serial]
    async fn what_is_not_a_number_is_left_out_and_the_rest_is_stored() -> Result<()> {
        let db = connect().await?;
        let storage = db.storage();
        let float = new_sensor();
        let text = sensor_of(SensorType::String);
        let first = now() - 60;

        let mut sensors = SensAppVec::new();
        sensors.push(SingleSensorBatch::new(
            text.clone(),
            TypedSamples::String(
                vec![Sample {
                    datetime: at(first),
                    value: "hello".to_string(),
                }]
                .into(),
            ),
        ));
        sensors.push(SingleSensorBatch::new(
            float.clone(),
            TypedSamples::Float(
                vec![Sample {
                    datetime: at(first),
                    value: 1.0,
                }]
                .into(),
            ),
        ));
        storage.publish(Arc::new(Batch::new(sensors))).await?;

        assert_eq!(
            read(&storage, &float, first, first).await?,
            vec![(first, 1.0)]
        );
        assert!(
            read_window(&storage, &text, Some(first), Some(first), None)
                .await?
                .is_none()
        );
        Ok(())
    }

    #[tokio::test]
    #[serial]
    async fn unknown_and_infinite_values_do_not_fail_a_write() -> Result<()> {
        let db = connect().await?;
        let storage = db.storage();
        let sensor = new_sensor();
        let first = now() - 100;

        // A NaN is how Prometheus marks a stale series: an unknown value
        publish(
            &storage,
            &sensor,
            &[
                (first, 1.0),
                (first + 10, f64::NAN),
                (first + 20, 3.0),
                (first + 30, f64::INFINITY),
            ],
        )
        .await?;

        let rows = read(&storage, &sensor, first, first + 30).await?;
        assert_eq!(rows[0], (first, 1.0));
        assert!(
            !rows.iter().any(|(time, _)| *time == first + 10),
            "{rows:?}"
        );
        assert!(rows.contains(&(first + 20, 3.0)), "{rows:?}");
        Ok(())
    }

    // ---- writes that RRDtool would refuse

    #[tokio::test]
    #[serial]
    async fn unsorted_samples_and_samples_of_the_same_second_are_written_in_order() -> Result<()> {
        let db = connect().await?;
        let storage = db.storage();
        let sensor = new_sensor();
        let first = now() - 100;

        publish(
            &storage,
            &sensor,
            &[
                (first + 20, 3.0),
                (first, 1.0),
                (first + 10, 2.0),
                (first + 10, 2.5),
            ],
        )
        .await?;

        // One value per second: the last given
        assert_eq!(
            read(&storage, &sensor, first, first + 20).await?,
            vec![(first, 1.0), (first + 10, 2.5), (first + 20, 3.0)]
        );
        Ok(())
    }

    #[tokio::test]
    #[serial]
    async fn a_batch_that_is_sent_again_is_not_an_error() -> Result<()> {
        // A client that did not get the answer sends it again: this is what Prometheus does
        let db = connect().await?;
        let storage = db.storage();
        let sensor = new_sensor();
        let first = now() - 200;
        let points = ramp(first, 10, 10, 1.0);

        publish(&storage, &sensor, &points).await?;
        publish(&storage, &sensor, &points).await?;

        assert_eq!(read(&storage, &sensor, first, first + 90).await?, points);
        Ok(())
    }

    #[tokio::test]
    #[serial]
    async fn samples_older_than_the_series_are_ignored_and_the_others_are_stored() -> Result<()> {
        let db = connect().await?;
        let storage = db.storage();
        let sensor = new_sensor();
        let first = now() - 300;

        publish(&storage, &sensor, &ramp(first, 10, 6, 1.0)).await?;
        // One sample older than the last update, and a new one
        publish(&storage, &sensor, &[(first + 20, 99.0), (first + 60, 6.0)]).await?;

        let rows = read(&storage, &sensor, first, first + 60).await?;
        assert_eq!(rows.len(), 7);
        assert_eq!(rows[2], (first + 20, 2.0), "the old value stays: {rows:?}");
        assert_eq!(rows[6], (first + 60, 6.0));
        Ok(())
    }

    #[tokio::test]
    #[serial]
    async fn samples_before_1970_do_not_fail_a_write() -> Result<()> {
        let db = connect().await?;
        let storage = db.storage();
        let sensor = new_sensor();
        let first = now() - 20;
        publish(&storage, &sensor, &[(-100, 1.0), (first, 2.0)]).await?;
        assert_eq!(
            read(&storage, &sensor, first, first).await?,
            vec![(first, 2.0)]
        );
        Ok(())
    }

    // ---- several writers

    #[tokio::test]
    #[serial]
    async fn a_new_instance_does_not_erase_what_the_previous_one_stored() -> Result<()> {
        // The restart of SensApp: nothing is remembered, the files are there
        let db = connect().await?;
        let before = db.storage();
        let sensor = new_sensor();
        let first = now() - 400;
        publish(&before, &sensor, &ramp(first, 10, 10, 1.0)).await?;

        let after = create_storage_from_connection_string(&db.connection_string).await?;
        publish(&after, &sensor, &ramp(first + 100, 10, 10, 1.0)).await?;

        let mut expected = ramp(first, 10, 10, 1.0);
        expected.extend(ramp(first + 100, 10, 10, 1.0));
        assert_eq!(read(&after, &sensor, first, first + 190).await?, expected);
        assert_eq!(read(&before, &sensor, first, first + 190).await?, expected);
        Ok(())
    }

    #[tokio::test]
    #[serial]
    async fn writers_that_create_the_same_series_at_once_lose_nothing() -> Result<()> {
        // Several instances see a series for the first time together, and must not replace each
        // other's file: the daemon is asked not to overwrite.
        let db = connect().await?;
        let sensor = new_sensor();
        let first = now() - 600;

        let mut instances = Vec::new();
        for _ in 0..4 {
            instances.push(create_storage_from_connection_string(&db.connection_string).await?);
        }
        let writes = instances.iter().map(|storage| {
            let storage = storage.clone();
            let batch = float_batch(&sensor, &[(first, 1.0)]);
            tokio::spawn(async move { storage.publish(batch).await })
        });
        for write in futures::future::join_all(writes).await {
            write??;
        }

        // The same instance, the same series, several requests together
        let storage = instances[0].clone();
        let other = new_sensor();
        let writes = (0..8).map(|i| {
            let storage = storage.clone();
            let batch = float_batch(&other, &[(first + i * 10, i as f64)]);
            tokio::spawn(async move { storage.publish(batch).await })
        });
        for write in futures::future::join_all(writes).await {
            write??;
        }

        publish(&instances[1], &sensor, &ramp(first + 10, 10, 5, 1.0)).await?;
        let rows = read(&instances[2], &sensor, first, first + 50).await?;
        assert_eq!(rows[0], (first, 1.0), "{rows:?}");
        assert_eq!(rows.len(), 6, "{rows:?}");

        // Whichever order the requests ran in, the file was created once: the first sample of
        // the series is the one of the writers that won, never missing
        let rows = read(&storage, &other, first, first + 70).await?;
        assert!(!rows.is_empty());
        Ok(())
    }

    // ---- windows

    #[tokio::test]
    #[serial]
    async fn a_window_returns_the_rows_that_meet_it() -> Result<()> {
        let db = connect().await?;
        let storage = db.storage();
        let sensor = new_sensor();
        let first = now() - 300;
        publish(&storage, &sensor, &ramp(first, 10, 11, 1.0)).await?;
        let at_step = |i: i64| (first + i * 10, i as f64);

        // Both ends are included
        assert_eq!(
            read(&storage, &sensor, first + 20, first + 40).await?,
            vec![at_step(2), at_step(3), at_step(4)]
        );
        // Between two rows: the rows that hold these seconds
        assert_eq!(
            read(&storage, &sensor, first + 21, first + 39).await?,
            vec![at_step(3), at_step(4)]
        );
        assert_eq!(
            read(&storage, &sensor, first + 25, first + 25).await?,
            vec![at_step(3)]
        );
        // The first and the last
        assert_eq!(
            read(&storage, &sensor, first, first).await?,
            vec![at_step(0)]
        );
        assert_eq!(
            read(&storage, &sensor, first + 100, first + 100).await?,
            vec![at_step(10)]
        );
        Ok(())
    }

    #[tokio::test]
    #[serial]
    async fn the_limit_keeps_the_first_rows() -> Result<()> {
        let db = connect().await?;
        let storage = db.storage();
        let sensor = new_sensor();
        let first = now() - 300;
        publish(&storage, &sensor, &ramp(first, 10, 11, 1.0)).await?;

        let rows = read_window(&storage, &sensor, Some(first), Some(first + 100), Some(3)).await?;
        assert_eq!(
            rows.unwrap(),
            vec![(first, 0.0), (first + 10, 1.0), (first + 20, 2.0)]
        );
        Ok(())
    }

    #[tokio::test]
    #[serial]
    async fn missing_empty_and_backwards_windows() -> Result<()> {
        let db = connect().await?;
        let storage = db.storage();
        let sensor = new_sensor();
        let first = now() - 300;

        // No file, no series
        assert!(
            read_window(&storage, &sensor, Some(first), Some(first + 10), None)
                .await?
                .is_none()
        );
        assert!(
            read_window(&storage, &sensor, Some(200), Some(100), None)
                .await?
                .is_none()
        );

        publish(&storage, &sensor, &ramp(first, 10, 5, 1.0)).await?;
        // A window before the first sample, one after the last, and one that ends before it starts
        for (start, end) in [
            (first - 1000, first - 500),
            (first + 1000, first + 2000),
            (first + 40, first),
        ] {
            let rows = read_window(&storage, &sensor, Some(start), Some(end), None).await?;
            assert_eq!(rows, Some(vec![]), "{start}..{end}");
        }
        Ok(())
    }

    #[tokio::test]
    #[serial]
    async fn an_open_window_reads_up_to_now_and_back_to_the_beginning() -> Result<()> {
        let db = connect().await?;
        let storage = db.storage();
        let sensor = new_sensor();
        let first = now() - 200;
        let points = ramp(first, 10, 20, 1.0);
        publish(&storage, &sensor, &points).await?;

        // Without an end: until now
        let rows = read_window(&storage, &sensor, Some(first), None, None)
            .await?
            .unwrap();
        assert_eq!(rows, points);

        // Without a start: from the beginning of the longest archive, so the rows come from a
        // coarse archive, and the freshest of them are completed with finer rows
        let rows = read_window(&storage, &sensor, None, Some(first + 190), None)
            .await?
            .unwrap();
        assert!(!rows.is_empty());
        let last = rows.last().unwrap();
        assert_eq!(*last, (first + 190, 19.0), "{rows:?}");

        let rows = read_window(&storage, &sensor, None, None, None)
            .await?
            .unwrap();
        assert_eq!(*rows.last().unwrap(), (first + 190, 19.0), "{rows:?}");
        Ok(())
    }

    #[tokio::test]
    #[serial]
    async fn the_freshest_rows_of_a_long_window_are_there() -> Result<()> {
        // Three days are served by the archive of ten minutes, in which the last row only
        // exists once its ten minutes are over: the rows of the last minutes are read from the
        // archive of ten seconds
        let db = connect().await?;
        let storage = db.storage();
        let sensor = new_sensor();
        let end = now();
        // Twenty-five minutes of data: the rows of ten minutes before the last one are complete
        let points = ramp(end - 1500, 10, 151, 1.0);
        publish(&storage, &sensor, &points).await?;

        let rows = read(&storage, &sensor, end - 3 * 86400, end).await?;
        assert_eq!(rows.last(), points.last(), "{rows:?}");
        // The older rows are the averages of ten minutes, the newest ones are rows of ten seconds
        assert!(rows.contains(&(end - 10, 149.0)), "{rows:?}");
        assert!(rows.len() >= 3 && rows[0].0 % 600 == 0, "{rows:?}");
        Ok(())
    }

    #[tokio::test]
    #[serial]
    async fn the_munin_preset_keeps_five_minutes_rows() -> Result<()> {
        let db = connect().await?;
        let munin = create_storage_from_connection_string(&format!(
            "rrdcached://{}?preset=munin",
            daemon_address(&db)
        ))
        .await?;
        let sensor = new_sensor();
        let first = now() - 3600;
        let first = first - first % 300;
        publish(&munin, &sensor, &ramp(first, 10, 180, 1.0)).await?;

        let rows = read(&munin, &sensor, first, first + 1790).await?;
        assert!(!rows.is_empty());
        assert!(rows.iter().all(|(time, _)| time % 300 == 0), "{rows:?}");
        Ok(())
    }

    // ---- listing

    #[tokio::test]
    #[serial]
    async fn every_series_is_listed_once_in_order_page_after_page() -> Result<()> {
        let db = connect().await?;
        let storage = db.storage();
        let first = now() - 100;
        let sensors: Vec<Arc<Sensor>> = (0..5).map(|_| new_sensor()).collect();
        for sensor in &sensors {
            publish(&storage, sensor, &[(first, 1.0)]).await?;
        }

        let mut listed: Vec<Uuid> = Vec::new();
        let mut bookmark: Option<String> = None;
        loop {
            let page = storage
                .list_series(None, Some(100), bookmark.as_deref())
                .await?;
            assert!(page.series.len() <= 100);
            listed.extend(page.series.iter().map(|sensor| sensor.uuid));
            bookmark = page.bookmark;
            if bookmark.is_none() {
                break;
            }
        }

        assert!(
            listed.windows(2).all(|pair| pair[0] < pair[1]),
            "the pages are sorted and have no repeat"
        );
        for sensor in &sensors {
            assert!(
                listed.contains(&sensor.uuid),
                "{} is not listed",
                sensor.uuid
            );
        }

        // The filter finds a series by its name, which is its UUID
        let page = storage
            .list_series(Some(&sensors[0].uuid.to_string()), None, None)
            .await?;
        assert_eq!(page.series.len(), 1);
        assert_eq!(page.series[0].uuid, sensors[0].uuid);
        assert_eq!(page.series[0].name, sensors[0].uuid.to_string());
        Ok(())
    }

    #[tokio::test]
    #[serial]
    async fn files_that_are_not_series_are_not_listed() -> Result<()> {
        // Another application can share the daemon: its files have no UUID of ours
        use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

        let db = connect().await?;
        let storage = db.storage();
        let stream = tokio::net::TcpStream::connect(daemon_address(&db)).await?;
        let (read_half, mut write_half) = stream.into_split();
        let mut lines = BufReader::new(read_half).lines();
        let name = format!("munin-load-{}", Uuid::new_v4().simple());
        write_half
            .write_all(
                format!("CREATE {name}.rrd -s 10 DS:v:GAUGE:60:U:U RRA:AVERAGE:0.5:1:10\n")
                    .as_bytes(),
            )
            .await?;
        assert_eq!(
            lines.next_line().await?.as_deref(),
            Some("0 RRD created OK")
        );

        let page = storage.list_series(None, Some(16384), None).await?;
        assert!(
            page.series
                .iter()
                .all(|sensor| sensor.name == sensor.uuid.to_string())
        );
        assert!(
            !page
                .series
                .iter()
                .any(|sensor| sensor.name.contains("munin-load"))
        );
        Ok(())
    }

    // ---- selectors

    #[tokio::test]
    #[serial]
    async fn a_selector_finds_series_by_their_uuid_name() -> Result<()> {
        let db = connect().await?;
        let storage = db.storage();
        let sensor = new_sensor();
        let first = now() - 100;
        let points = ramp(first, 10, 5, 1.0);
        publish(&storage, &sensor, &points).await?;
        let name = sensor.uuid.to_string();
        let window = (Some(at(first)), Some(at(first + 40)));

        let found = storage
            .query_sensors_by_labels(
                &[LabelMatcher::eq("__name__", name.clone())],
                window.0,
                window.1,
                None,
                true,
            )
            .await?;
        assert_eq!(found.len(), 1);
        assert_eq!(found[0].sensor.uuid, sensor.uuid);

        let found = storage
            .query_sensors_by_labels(
                &[LabelMatcher::regex("__name__", name.clone())],
                window.0,
                window.1,
                None,
                true,
            )
            .await?;
        assert_eq!(found.len(), 1);

        // The series have no labels
        let found = storage
            .query_sensors_by_labels(
                &[
                    LabelMatcher::eq("__name__", name.clone()),
                    LabelMatcher::eq("job", "x"),
                ],
                window.0,
                window.1,
                None,
                true,
            )
            .await?;
        assert!(found.is_empty());

        let read = storage
            .query_selector(
                &[LabelMatcher::eq("__name__", name.clone())],
                window.0,
                window.1,
                true,
                10,
                100,
            )
            .await?
            .expect("within the limits");
        assert_eq!(read.len(), 1);
        match &read[0].samples {
            TypedSamples::Float(samples) => assert_eq!(samples.len(), 5),
            other => panic!("{other:?}"),
        }

        let read = storage
            .query_selector(
                &[LabelMatcher::eq("__name__", name)],
                window.0,
                window.1,
                true,
                10,
                3,
            )
            .await?;
        assert_eq!(read.unwrap_err(), SelectorLimitExceeded::Samples);
        Ok(())
    }

    // ---- through the HTTP API

    use axum::{
        Router,
        body::Body,
        http::{Request, StatusCode},
    };
    use prost::Message;
    use sensapp::datamodel::Sensor as SensorModel;
    use sensapp::http::{
        metrics::HttpMetrics,
        server::{RouterSettings, build_router},
        state::HttpServerState,
    };
    use sensapp::parsing::prometheus::remote_write_models::{
        Label, Sample as PromSample, TimeSeries, WriteRequest,
    };
    use std::time::Duration;
    use tower::ServiceExt;

    fn router(db: &TestDb) -> Router {
        let state = HttpServerState {
            name: Arc::new("SensApp Test".to_string()),
            storage: db.storage(),
            metrics: Arc::new(HttpMetrics::new()),
            influxdb_with_numeric: false,
            auth: None,
        };
        build_router(
            state,
            &RouterSettings {
                max_body_bytes: 64 * 1024 * 1024,
                request_timeout: Duration::from_secs(30),
                maintenance_timeout: Duration::from_secs(3600),
                max_concurrent_writes: 16,
                ui_dir: None,
            },
        )
    }

    async fn send(router: &Router, request: Request<Body>) -> (StatusCode, String) {
        let response = router.clone().oneshot(request).await.unwrap();
        let (parts, body) = response.into_parts();
        let body = axum::body::to_bytes(body, usize::MAX).await.unwrap();
        (parts.status, String::from_utf8_lossy(&body).to_string())
    }

    fn get(path: &str) -> Request<Body> {
        Request::builder().uri(path).body(Body::empty()).unwrap()
    }

    fn iso(seconds: i64) -> String {
        format!("{}Z", at(seconds).to_isoformat())
    }

    /// The `(time, value)` pairs of a SenML answer
    fn senml_rows(body: &str) -> Vec<(i64, f64)> {
        let records: Vec<serde_json::Value> = serde_json::from_str(body).expect("SenML JSON");
        let mut base = 0.0;
        records
            .iter()
            .map(|record| {
                if let Some(bt) = record.get("bt").and_then(|bt| bt.as_f64()) {
                    base = bt;
                }
                let time = base + record.get("t").and_then(|t| t.as_f64()).unwrap_or(0.0);
                (time.round() as i64, record["v"].as_f64().expect("a value"))
            })
            .collect()
    }

    #[tokio::test]
    #[serial]
    async fn prometheus_remote_write_then_read_the_series() -> Result<()> {
        // The story of this backend: Prometheus scrapes every minute, writes to SensApp, the
        // data is in an RRD file. The series is read by its UUID: the name and the labels are
        // not stored, so Prometheus cannot read it back by name.
        let db = connect().await?;
        let router = router(&db);
        let name = format!("rrd_http_{}", Uuid::new_v4().simple());
        let first = (now() - 1800) / 60 * 60;
        let points: Vec<(i64, f64)> = ramp(first, 60, 10, 2.0);

        let write = WriteRequest {
            timeseries: vec![TimeSeries {
                labels: vec![Label {
                    name: "__name__".to_string(),
                    value: name.clone(),
                }],
                samples: points
                    .iter()
                    .map(|(time, value)| PromSample {
                        value: *value,
                        timestamp: time * 1000,
                    })
                    .collect(),
            }],
        };
        let body = snap::raw::Encoder::new().compress_vec(&write.encode_to_vec())?;
        let request = Request::builder()
            .method("POST")
            .uri("/api/v1/prometheus_remote_write")
            .header("content-type", "application/x-protobuf")
            .header("content-encoding", "snappy")
            .header("x-prometheus-remote-write-version", "0.1.0")
            .body(Body::from(body))?;
        assert_eq!(send(&router, request).await.0, StatusCode::NO_CONTENT);

        // The same request again, as Prometheus does when it did not get the answer
        let body = snap::raw::Encoder::new().compress_vec(&write.encode_to_vec())?;
        let request = Request::builder()
            .method("POST")
            .uri("/api/v1/prometheus_remote_write")
            .header("content-type", "application/x-protobuf")
            .header("content-encoding", "snappy")
            .header("x-prometheus-remote-write-version", "0.1.0")
            .body(Body::from(body))?;
        assert_eq!(send(&router, request).await.0, StatusCode::NO_CONTENT);

        let uuid = SensorModel::new_without_uuid(
            name.clone(),
            SensorType::Float,
            None,
            Some(vec![("__name__".to_string(), name)].into_iter().collect()),
        )?
        .uuid;
        let (status, body) = send(
            &router,
            get(&format!(
                "/series/{uuid}?start={}&end={}",
                iso(first),
                iso(first + 540)
            )),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{body}");
        let rows = senml_rows(&body);
        for point in &points {
            assert!(rows.contains(point), "{point:?} is missing from {rows:?}");
        }

        // The last sample, without any window
        let (status, body) = send(&router, get(&format!("/series/{uuid}/last"))).await;
        assert_eq!(status, StatusCode::OK, "{body}");
        let last: serde_json::Value = serde_json::from_str(&body)?;
        assert_eq!(
            last["value"].as_f64(),
            points.last().map(|(_, value)| *value),
            "{body}"
        );
        Ok(())
    }

    #[tokio::test]
    #[serial]
    async fn senml_samples_an_hour_apart_and_what_the_api_cannot_do() -> Result<()> {
        let db = connect().await?;
        let router = router(&db);
        let name = format!("rrd_senml_{}", Uuid::new_v4().simple());
        let first = (now() - 6 * 3600) / 3600 * 3600;
        let records: Vec<serde_json::Value> = (0..4)
            .map(|i| {
                serde_json::json!({
                    "bn": format!("{name}:"), "n": "t", "bt": first + i * 3600, "v": 20.0 + i as f64
                })
            })
            .collect();
        let request = Request::builder()
            .method("POST")
            .uri("/publish")
            .header("content-type", "application/json")
            .body(Body::from(serde_json::to_vec(&records)?))?;
        let (status, body) = send(&router, request).await;
        assert_eq!(status, StatusCode::OK, "{body}");

        // The series of this name: found by listing, the only one with this UUID name is new
        let uuid =
            SensorModel::new_without_uuid(format!("{name}:t"), SensorType::Float, None, None)?.uuid;
        let (status, body) = send(
            &router,
            get(&format!(
                "/series/{uuid}?start={}&end={}",
                iso(first),
                iso(first + 3 * 3600)
            )),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{body}");
        let rows = senml_rows(&body);
        assert!(rows.contains(&(first + 3 * 3600, 23.0)), "{rows:?}");

        // A series that is not there
        let (status, _) = send(&router, get(&format!("/series/{}", Uuid::new_v4()))).await;
        assert_eq!(status, StatusCode::NOT_FOUND);

        // Deleting is not implemented
        let request = Request::builder()
            .method("DELETE")
            .uri(format!("/series/{uuid}"))
            .body(Body::empty())?;
        assert_eq!(send(&router, request).await.0, StatusCode::NOT_IMPLEMENTED);

        // The service is ready and its listing works
        assert_eq!(send(&router, get("/health/ready")).await.0, StatusCode::OK);
        assert_eq!(
            send(&router, get("/series?limit=2")).await.0,
            StatusCode::OK
        );
        Ok(())
    }

    // ---- what the backend does not do

    #[tokio::test]
    #[serial]
    async fn deleting_is_not_supported() -> Result<()> {
        let db = connect().await?;
        let storage = db.storage();
        let error = storage
            .delete_series(&Uuid::new_v4().to_string())
            .await
            .unwrap_err();
        assert!(matches!(
            error.downcast_ref::<StorageError>(),
            Some(StorageError::Unsupported(_))
        ));
        let error = storage
            .delete_series_samples(&Uuid::new_v4().to_string(), at(0), at(10))
            .await
            .unwrap_err();
        assert!(matches!(
            error.downcast_ref::<StorageError>(),
            Some(StorageError::Unsupported(_))
        ));
        assert!(storage.list_metrics().await?.is_empty());
        Ok(())
    }

    /// Timings of the common operations, printed for a human: `cargo test ... -- --ignored --nocapture
    /// rrdcached_performance`. Not a pass/fail test, it only asserts that the data comes back.
    #[tokio::test]
    #[serial]
    #[ignore = "benchmark, run by hand"]
    async fn rrdcached_performance() -> Result<()> {
        use std::time::Instant;

        ensure_config();
        let test_db = TestDb::new_with_type(DatabaseType::RRDcached).await?;
        let storage = test_db.storage();

        let sensor_count = 50usize;
        let samples_per_sensor = 8000usize;
        let now = SensAppDateTime::now()?.to_unix_seconds().floor();
        let first = now - (samples_per_sensor as f64) * 10.0;
        let sensors: Vec<Arc<Sensor>> = (0..sensor_count)
            .map(|i| {
                Arc::new(Sensor {
                    uuid: Uuid::new_v4(),
                    name: format!("perf_{i}"),
                    sensor_type: SensorType::Float,
                    unit: None,
                    labels: SmallVec::new(),
                })
            })
            .collect();

        // Bulk load: batches of about 8192 samples, like SENSAPP_BATCH_SIZE
        let started = Instant::now();
        let per_batch = 8192 / sensor_count; // samples of each sensor in a batch
        let mut offset = 0;
        while offset < samples_per_sensor {
            let end = (offset + per_batch).min(samples_per_sensor);
            let mut vec = SensAppVec::new();
            for sensor in &sensors {
                let samples: Vec<Sample<f64>> = (offset..end)
                    .map(|i| Sample {
                        datetime: SensAppDateTime::from_unix_seconds(first + i as f64 * 10.0),
                        value: i as f64,
                    })
                    .collect();
                vec.push(SingleSensorBatch::new(
                    sensor.clone(),
                    TypedSamples::Float(samples.into()),
                ));
            }
            storage.publish(Arc::new(Batch::new(vec))).await?;
            offset = end;
        }
        let elapsed = started.elapsed();
        let total = sensor_count * samples_per_sensor;
        println!(
            "bulk load: {total} samples in {:.0} ms ({:.0} samples/s)",
            elapsed.as_secs_f64() * 1000.0,
            total as f64 / elapsed.as_secs_f64()
        );

        // The first write of many series: every one needs its file
        let fresh: Vec<Arc<Sensor>> = (0..1000)
            .map(|i| {
                Arc::new(Sensor {
                    uuid: Uuid::new_v4(),
                    name: format!("fresh_{i}"),
                    sensor_type: SensorType::Float,
                    unit: None,
                    labels: SmallVec::new(),
                })
            })
            .collect();
        let started = Instant::now();
        let mut vec = SensAppVec::new();
        for sensor in &fresh {
            vec.push(SingleSensorBatch::new(
                sensor.clone(),
                TypedSamples::Float(
                    vec![Sample {
                        datetime: SensAppDateTime::from_unix_seconds(now),
                        value: 1.0,
                    }]
                    .into(),
                ),
            ));
        }
        storage.publish(Arc::new(Batch::new(vec))).await?;
        println!(
            "new series: {:.2} ms for {} files, {:.2} ms each",
            started.elapsed().as_secs_f64() * 1000.0,
            fresh.len(),
            started.elapsed().as_secs_f64() * 1000.0 / fresh.len() as f64
        );

        // Scrapes of the series that exist: one new sample for each, every 10 seconds
        let scrapes = 30;
        let started = Instant::now();
        for i in 1..=scrapes {
            let mut vec = SensAppVec::new();
            for sensor in &fresh {
                vec.push(SingleSensorBatch::new(
                    sensor.clone(),
                    TypedSamples::Float(
                        vec![Sample {
                            datetime: SensAppDateTime::from_unix_seconds(now + i as f64 * 10.0),
                            value: i as f64,
                        }]
                        .into(),
                    ),
                ));
            }
            storage.publish(Arc::new(Batch::new(vec))).await?;
        }
        println!(
            "scrapes: {:.2} ms per publish of {} series",
            started.elapsed().as_secs_f64() * 1000.0 / scrapes as f64,
            fresh.len()
        );

        // Small writes: one new sample for each of the sensors, like a scrape
        let writes = 100;
        let started = Instant::now();
        for i in 0..writes {
            let mut vec = SensAppVec::new();
            for sensor in &sensors {
                let samples = vec![Sample {
                    datetime: SensAppDateTime::from_unix_seconds(now + 10.0 + i as f64 * 10.0),
                    value: 1.0,
                }];
                vec.push(SingleSensorBatch::new(
                    sensor.clone(),
                    TypedSamples::Float(samples.into()),
                ));
            }
            storage.publish(Arc::new(Batch::new(vec))).await?;
        }
        println!(
            "small writes: {:.2} ms per publish of {sensor_count} sensors",
            started.elapsed().as_secs_f64() * 1000.0 / writes as f64
        );

        // Reads of a day, of the whole history
        for (label, window) in [("1 hour", 3600.0), ("22 hours", 80_000.0)] {
            let started = Instant::now();
            let mut found = 0;
            for sensor in &sensors {
                let data = storage
                    .query_sensor_data(
                        &sensor.uuid.to_string(),
                        Some(SensAppDateTime::from_unix_seconds(now - window)),
                        Some(SensAppDateTime::from_unix_seconds(now)),
                        None,
                    )
                    .await?;
                found += data.map(|d| d.samples.len()).unwrap_or(0);
            }
            println!(
                "read of {label}: {:.2} ms per series, {found} samples in total",
                started.elapsed().as_secs_f64() * 1000.0 / sensor_count as f64
            );
            assert!(found > 0);
        }

        let started = Instant::now();
        for _ in 0..100 {
            storage.health_check().await?;
        }
        println!(
            "health check: {:.2} ms",
            started.elapsed().as_secs_f64() * 1000.0 / 100.0
        );

        let started = Instant::now();
        let listed = storage.list_series(None, Some(256), None).await?;
        println!(
            "list_series: {:.2} ms for {} series",
            started.elapsed().as_secs_f64() * 1000.0,
            listed.series.len()
        );
        Ok(())
    }
}
