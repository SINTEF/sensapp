//! Regex matchers match the whole value, like in Prometheus: `core=~"c7"` selects `c7`, not
//! `c70` nor `xc7`. These tests run on the backend selected by `TEST_DATABASE_URL`.

use crate::common::TestDb;
use crate::common::http::TestApp;
use anyhow::Result;
use sensapp::config::load_configuration_for_tests;
use sensapp::datamodel::batch_builder::BatchBuilder;
use sensapp::datamodel::sensapp_vec::SensAppLabels;
use sensapp::datamodel::{Sample, Sensor, SensorType, TypedSamples};
use sensapp::storage::StorageInstance;
use sensapp::storage::query::LabelMatcher;
use serial_test::serial;
use std::collections::BTreeSet;
use std::sync::Arc;
use uuid::Uuid;

static INIT: std::sync::Once = std::sync::Once::new();

fn ensure_config() {
    INIT.call_once(|| {
        load_configuration_for_tests().expect("Failed to load configuration for tests");
    });
}

/// The `core` labels of the sensors of one run, and the names of two more sensors.
const CORES: [&str; 6] = ["c7", "c70", "c700", "xc7", "C7", "c8"];

struct Fixture {
    run: String,
}

async fn publish(storage: &Arc<dyn StorageInstance>) -> Result<Fixture> {
    let run = Uuid::new_v4().to_string();
    let mut batch_builder = BatchBuilder::new()?;
    let add = |name: String, core: &str| -> Result<(Arc<Sensor>, TypedSamples)> {
        let labels: SensAppLabels = [
            ("run".to_string(), run.clone()),
            ("core".to_string(), core.to_string()),
        ]
        .into_iter()
        .collect();
        let sensor = Arc::new(Sensor::new_without_uuid(
            name,
            SensorType::Float,
            None,
            Some(labels),
        )?);
        let samples = TypedSamples::Float(
            vec![Sample {
                datetime: hifitime::Epoch::from_unix_seconds(1_704_067_200.0),
                value: 1.0,
            }]
            .into(),
        );
        Ok((sensor, samples))
    };

    let mut pending = Vec::new();
    for core in CORES {
        pending.push(add(format!("rx_{run}"), core)?);
    }
    pending.push(add(format!("rx_cpu_{run}"), "cpu")?);
    pending.push(add(format!("rx_cpu_usage_{run}"), "cpu")?);
    for (sensor, samples) in pending {
        batch_builder.add(sensor, samples).await?;
    }
    batch_builder.send_what_is_left(storage.clone()).await?;
    Ok(Fixture { run })
}

/// What a selector selects, as `core` label values (and names for the name matchers), in
/// every way the storage reads a selector.
async fn selected(
    storage: &Arc<dyn StorageInstance>,
    fixture: &Fixture,
    extra: Vec<LabelMatcher>,
) -> Result<BTreeSet<String>> {
    let mut matchers = vec![LabelMatcher::eq("run", fixture.run.clone())];
    matchers.extend(extra);

    let describe = |data: &sensapp::datamodel::SensorData| {
        let core = data
            .sensor
            .labels
            .iter()
            .find(|(name, _)| name == "core")
            .map(|(_, value)| value.clone())
            .unwrap_or_default();
        format!("{core}@{}", data.sensor.name.replace(&fixture.run, "RUN"))
    };

    let by_labels: BTreeSet<String> = storage
        .query_sensors_by_labels(&matchers, None, None, Some(1), false)
        .await?
        .iter()
        .map(describe)
        .collect();
    let by_selector: BTreeSet<String> = storage
        .query_selector(&matchers, None, None, false, 1000, 1_000_000)
        .await?
        .expect("within limits")
        .iter()
        .map(describe)
        .collect();
    assert_eq!(
        by_labels, by_selector,
        "both reads must select the same series"
    );
    Ok(by_labels)
}

fn cores(values: &[&str]) -> BTreeSet<String> {
    values.iter().map(|core| format!("{core}@rx_RUN")).collect()
}

#[tokio::test]
#[serial]
async fn label_regexes_match_the_whole_value() -> Result<()> {
    ensure_config();
    let test_db = TestDb::new().await?;
    let storage = test_db.storage();
    let fixture = publish(&storage).await?;
    let select = |matchers: Vec<LabelMatcher>| selected(&storage, &fixture, matchers);

    // Exactly c7: not c70, c700, xc7 nor C7
    assert_eq!(
        select(vec![LabelMatcher::regex("core", "c7")]).await?,
        cores(&["c7"])
    );
    // A wildcard still reaches the longer values, but not what comes before
    assert_eq!(
        select(vec![LabelMatcher::regex("core", "c7.*")]).await?,
        cores(&["c7", "c70", "c700"])
    );
    // One character is one character
    assert_eq!(
        select(vec![LabelMatcher::regex("core", "c7.")]).await?,
        cores(&["c70"])
    );
    // An alternation is anchored as a whole, not branch by branch
    assert_eq!(
        select(vec![LabelMatcher::regex("core", "c7|c70")]).await?,
        cores(&["c7", "c70"])
    );
    assert_eq!(
        select(vec![LabelMatcher::regex("core", "x.*|c8")]).await?,
        cores(&["xc7", "c8"])
    );
    // No value is empty
    assert!(
        select(vec![LabelMatcher::regex("core", "")])
            .await?
            .is_empty()
    );
    Ok(())
}

#[tokio::test]
#[serial]
async fn negated_label_regexes_exclude_exact_matches_only() -> Result<()> {
    ensure_config();
    let test_db = TestDb::new().await?;
    let storage = test_db.storage();
    let fixture = publish(&storage).await?;
    let select = |matchers: Vec<LabelMatcher>| selected(&storage, &fixture, matchers);

    // Everything but the series whose core is exactly c7 (the two cpu sensors have core=cpu)
    let mut everything_but_c7 = cores(&["c70", "c700", "xc7", "C7", "c8"]);
    everything_but_c7.insert("cpu@rx_cpu_RUN".to_string());
    everything_but_c7.insert("cpu@rx_cpu_usage_RUN".to_string());
    assert_eq!(
        select(vec![LabelMatcher::not_regex("core", "c7")]).await?,
        everything_but_c7
    );
    assert_eq!(
        select(vec![
            LabelMatcher::not_regex("core", "c7.*"),
            LabelMatcher::not_regex("core", "cpu"),
        ])
        .await?,
        cores(&["xc7", "C7", "c8"])
    );
    Ok(())
}

#[tokio::test]
#[serial]
async fn name_regexes_match_the_whole_name() -> Result<()> {
    ensure_config();
    let test_db = TestDb::new().await?;
    let storage = test_db.storage();
    let fixture = publish(&storage).await?;
    let run = fixture.run.clone();
    let select = |matchers: Vec<LabelMatcher>| selected(&storage, &fixture, matchers);

    let only_cpu: BTreeSet<String> = ["cpu@rx_cpu_RUN".to_string()].into();
    assert_eq!(
        select(vec![LabelMatcher::regex(
            "__name__",
            format!("rx_cpu_{run}")
        )])
        .await?,
        only_cpu,
        "rx_cpu_RUN must not select rx_cpu_usage_RUN"
    );
    // A prefix is not a name
    assert!(
        select(vec![LabelMatcher::regex("__name__", "rx_cpu")])
            .await?
            .is_empty()
    );
    assert_eq!(
        select(vec![LabelMatcher::regex("__name__", "rx_cpu.*")])
            .await?
            .len(),
        2
    );
    let without_cpu = select(vec![LabelMatcher::not_regex(
        "__name__",
        format!("rx_cpu_{run}"),
    )])
    .await?;
    assert!(!without_cpu.contains("cpu@rx_cpu_RUN"));
    assert!(without_cpu.contains("cpu@rx_cpu_usage_RUN"));
    Ok(())
}

#[tokio::test]
#[serial]
async fn case_insensitive_flags_still_work_in_front_of_the_anchors() -> Result<()> {
    ensure_config();
    let test_db = TestDb::new().await?;
    let storage = test_db.storage();
    let fixture = publish(&storage).await?;
    let select = |matchers: Vec<LabelMatcher>| selected(&storage, &fixture, matchers);

    assert_eq!(
        select(vec![LabelMatcher::regex("core", "(?i)c7")]).await?,
        cores(&["c7", "C7"])
    );
    assert_eq!(
        select(vec![LabelMatcher::regex("core", "(?i)c7.*")]).await?,
        cores(&["c7", "C7", "c70", "c700"])
    );
    Ok(())
}

/// The HTTP selectors: the series listing evaluates regexes in memory, the query endpoint
/// uses the storage.
#[tokio::test]
#[serial]
async fn http_selectors_anchor_regexes_too() -> Result<()> {
    ensure_config();
    let test_db = TestDb::new().await?;
    let storage = test_db.storage();
    let fixture = publish(&storage).await?;
    let app = TestApp::new(storage.clone()).await;

    let cores_listed = |body: &str| -> BTreeSet<String> {
        let json: serde_json::Value = serde_json::from_str(body).expect("json");
        let mut found = BTreeSet::new();
        collect_cores(&json, &mut found);
        found
    };

    for (selector, expected) in [
        (r#"core=~"c7""#, vec!["c7"]),
        (r#"core=~"c7.*""#, vec!["c7", "c70", "c700"]),
        (r#"core!~"c7|c70|c700|xc7|C7|cpu""#, vec!["c8"]),
    ] {
        let selector = format!(r#"{{run="{}",{selector}}}"#, fixture.run);
        let response = app
            .get(&format!(
                "/series?selector={}",
                urlencoding::encode(&selector)
            ))
            .await?;
        response.assert_status(axum::http::StatusCode::OK);
        let expected: BTreeSet<String> = expected.iter().map(|core| core.to_string()).collect();
        assert_eq!(cores_listed(response.body()), expected, "{selector}");
    }
    Ok(())
}

/// Every `core` label value found in a JSON document (the DCAT catalog spells labels in the
/// dataset id: `rx_.. {core="c7",run="..."}`).
fn collect_cores(value: &serde_json::Value, found: &mut BTreeSet<String>) {
    match value {
        serde_json::Value::String(text) => {
            if let Some(start) = text.find("core=\"") {
                let rest = &text[start + 6..];
                if let Some(end) = rest.find('"') {
                    found.insert(rest[..end].to_string());
                }
            }
        }
        serde_json::Value::Array(items) => items.iter().for_each(|item| collect_cores(item, found)),
        serde_json::Value::Object(map) => map.values().for_each(|item| collect_cores(item, found)),
        _ => {}
    }
}

/// A regex means the same on every backend, and the same as in Prometheus, which anchors it as
/// `^(?s:pattern)$`: `.` matches a newline too. PostgreSQL did, the RE2 and Rust backends did not.
#[tokio::test]
#[serial]
async fn regexes_mean_the_same_on_every_backend() -> Result<()> {
    ensure_config();
    let test_db = TestDb::new().await?;
    let storage = test_db.storage();
    let run = Uuid::new_v4().to_string();
    let mut batch_builder = BatchBuilder::new()?;
    for (index, core) in ["a\nb", "a b", "ab", "42"].into_iter().enumerate() {
        let labels: SensAppLabels = [
            ("run".to_string(), run.clone()),
            ("core".to_string(), core.to_string()),
        ]
        .into_iter()
        .collect();
        let sensor =
            Sensor::new_without_uuid(format!("rx_{index}"), SensorType::Float, None, Some(labels))?;
        let samples = TypedSamples::Float(
            vec![Sample {
                datetime: hifitime::Epoch::from_unix_seconds(1_704_067_200.0),
                value: 1.0,
            }]
            .into(),
        );
        batch_builder.add(Arc::new(sensor), samples).await?;
    }
    batch_builder.send_what_is_left(storage.clone()).await?;

    let select = |pattern: &str| {
        let matchers = vec![
            LabelMatcher::eq("run", run.clone()),
            LabelMatcher::try_new(
                "core",
                pattern,
                sensapp::storage::query::MatcherType::RegexMatch,
            )
            .expect("a supported regex"),
        ];
        let storage = storage.clone();
        let pattern = pattern.to_string();
        async move {
            let by_labels: BTreeSet<String> = storage
                .query_sensors_by_labels(&matchers, None, None, Some(1), false)
                .await?
                .iter()
                .flat_map(|data| data.sensor.labels.iter())
                .filter(|(name, _)| name == "core")
                .map(|(_, value)| value.clone())
                .collect();
            let by_selector: BTreeSet<String> = storage
                .query_selector(&matchers, None, None, false, 1000, 1_000_000)
                .await?
                .expect("within limits")
                .iter()
                .flat_map(|data| data.sensor.labels.iter())
                .filter(|(name, _)| name == "core")
                .map(|(_, value)| value.clone())
                .collect();
            assert_eq!(by_labels, by_selector, "{pattern}");
            anyhow::Ok(by_labels)
        }
    };
    let set = |values: &[&str]| {
        values
            .iter()
            .map(|v| v.to_string())
            .collect::<BTreeSet<_>>()
    };

    assert_eq!(select("a.b").await?, set(&["a\nb", "a b"]));
    assert_eq!(select("(?i)A.B").await?, set(&["a\nb", "a b"]));
    assert_eq!(select(r"\d+").await?, set(&["42"]));
    assert_eq!(select("[[:digit:]]{2}").await?, set(&["42"]));
    assert_eq!(select(r"a\sb").await?, set(&["a\nb", "a b"]));
    assert_eq!(select("(?:a|4)(?:b|2)").await?, set(&["ab", "42"]));
    Ok(())
}

/// A regex that Prometheus refuses, or that PostgreSQL would read otherwise than RE2 does, is a
/// 400 on every read that takes a selector. It reached the database before: a 500, or another
/// answer than the one asked for.
#[tokio::test]
#[serial]
async fn unsupported_regexes_are_refused() -> Result<()> {
    ensure_config();
    let test_db = TestDb::new().await?;
    let app = TestApp::new(test_db.storage()).await;
    for pattern in [
        // Unbalanced: anchored as `^(?:a)|(b)$`, which is not anchored
        "a)|(b",
        // A back-reference and a lookahead: not RE2
        r"(a)\1",
        "(?=a)a",
        // A word boundary in RE2, a backspace in PostgreSQL
        r"\bc7",
        // Unicode classes and inline flags PostgreSQL does not have, or reads otherwise
        r"\p{L}+",
        "(?-i)c7",
        "(?m)c7",
        "(?x)c7 # comment",
        "c(?i)7",
        "(?P<core>c7)",
        // Exactly two hex digits in RE2, as many as there are in PostgreSQL
        r"\x41B",
    ] {
        let selector = format!(r#"{{core=~"{}"}}"#, pattern.replace('\\', r"\\"));
        for path in ["/series?selector=", "/api/v1/query?query="] {
            let response = app
                .get(&format!("{path}{}", urlencoding::encode(&selector)))
                .await?;
            assert_eq!(
                response.status(),
                axum::http::StatusCode::BAD_REQUEST,
                "{path} {pattern}: {}",
                response.body()
            );
        }
    }
    Ok(())
}
