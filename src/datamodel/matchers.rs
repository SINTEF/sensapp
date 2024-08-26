use anyhow::{bail, Result};
use prometheus_parser::{
    Expression as PrometheusExpression, Label as PrometheusLabel, LabelOp as PrometheusLabelOp,
};

#[derive(Debug, Default, PartialEq)]
pub enum StringMatcher {
    #[default]
    All,
    Equal(String),
    NotEqual(String),
    Match(String),
    NotMatch(String),
}

impl StringMatcher {
    fn from_prometheus_label_op(op: PrometheusLabelOp, value: String) -> Self {
        match op {
            PrometheusLabelOp::Equal => StringMatcher::Equal(value),
            PrometheusLabelOp::NotEqual => StringMatcher::NotEqual(value),
            PrometheusLabelOp::RegexEqual => StringMatcher::Match(value),
            PrometheusLabelOp::RegexNotEqual => StringMatcher::NotMatch(value),
        }
    }
}

#[derive(Debug, PartialEq)]
pub struct LabelMatcher {
    name: String,
    matcher: StringMatcher,
}

impl LabelMatcher {
    fn from_prometheus_label(label: PrometheusLabel) -> Self {
        let name = label.key;
        let matcher = StringMatcher::from_prometheus_label_op(label.op, label.value);
        Self { name, matcher }
    }
}

#[derive(Debug, Default, PartialEq)]
pub struct SensorMatcher {
    name_matcher: StringMatcher,
    label_matchers: Option<Vec<LabelMatcher>>,
}

impl SensorMatcher {
    /// Create a new sensor matcher from a prometheus query.
    ///
    /// PLease note that this is a subset of the prometheus query language.
    /// I find the labels selection syntax neat. The more advanced features,
    /// perhaps not so much.
    pub fn from_prometheus_query(query: &str) -> Result<Self> {
        let ast = prometheus_parser::parse_expr(query)?;
        Self::from_prometheus_query_ast(ast)
    }

    pub fn from_prometheus_query_ast(ast: PrometheusExpression) -> Result<Self> {
        let selector = match ast {
            PrometheusExpression::Selector(selector) => selector,
            _ => bail!("Invalid query: it must be a prometheus query selector"),
        };

        if selector.subquery.is_some() || selector.offset.is_some() || selector.range.is_some() {
            bail!(
                "Invalid query: it must be a simple prometheus query selector, nothing more. sorry"
            );
        }

        let mut name_matcher = match selector.metric {
            Some(metric_name) => StringMatcher::Equal(metric_name),
            None => StringMatcher::All,
        };

        let mut label_matchers = Vec::with_capacity(selector.labels.len());

        for label in selector.labels {
            if label.key == "__name__" {
                name_matcher = StringMatcher::from_prometheus_label_op(label.op, label.value);
            } else {
                label_matchers.push(LabelMatcher::from_prometheus_label(label));
            }
        }

        Ok(Self {
            name_matcher,
            label_matchers: match label_matchers.is_empty() {
                true => None,
                false => Some(label_matchers),
            },
        })
    }
}

// Test with:
// http_requests_total
// http_requests_total{job="prometheus",group="canary"}
// http_requests_total{environment=~"staging|testing|development",method!="GET"}
// http_requests_total{environment=""}
// http_requests_total{replica!="rep-a",replica=~"rep.*"}
// {__name__=~"job:.*"}
//
// should fail:
// http_requests_total{job="prometheus"}[5m]
// rate(http_requests_total[5m] offset 1w)
// http_requests_total @ 1609746000
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sensor_matcher_from_prometheus_query() {
        let matcher = SensorMatcher::from_prometheus_query("http_requests_total").unwrap();
        assert_eq!(
            matcher.name_matcher,
            StringMatcher::Equal("http_requests_total".to_string())
        );
        assert!(matcher.label_matchers.is_none());

        let matcher = SensorMatcher::from_prometheus_query(
            "http_requests_total{job=\"prometheus\",group=\"canary\"}",
        )
        .unwrap();
        assert_eq!(
            matcher.name_matcher,
            StringMatcher::Equal("http_requests_total".to_string())
        );
        assert_eq!(
            matcher.label_matchers.unwrap(),
            vec![
                LabelMatcher {
                    name: "job".to_string(),
                    matcher: StringMatcher::Equal("prometheus".to_string())
                },
                LabelMatcher {
                    name: "group".to_string(),
                    matcher: StringMatcher::Equal("canary".to_string())
                }
            ]
        );

        let matcher = SensorMatcher::from_prometheus_query(
            "http_requests_total{environment=~\"staging|testing|development\",method!=\"GET\"}",
        )
        .unwrap();
        assert_eq!(
            matcher.name_matcher,
            StringMatcher::Equal("http_requests_total".to_string())
        );
        assert_eq!(
            matcher.label_matchers.unwrap(),
            vec![
                LabelMatcher {
                    name: "environment".to_string(),
                    matcher: StringMatcher::Match("staging|testing|development".to_string())
                },
                LabelMatcher {
                    name: "method".to_string(),
                    matcher: StringMatcher::NotEqual("GET".to_string())
                }
            ]
        );

        let matcher = SensorMatcher::from_prometheus_query(
            "http_requests_total{environment=\"\",replica!=\"rep-a\",replica=~\"rep.*\"}",
        )
        .unwrap();
        assert_eq!(
            matcher.name_matcher,
            StringMatcher::Equal("http_requests_total".to_string())
        );
        assert_eq!(
            matcher.label_matchers.unwrap(),
            vec![
                LabelMatcher {
                    name: "environment".to_string(),
                    matcher: StringMatcher::Equal("".to_string())
                },
                LabelMatcher {
                    name: "replica".to_string(),
                    matcher: StringMatcher::NotEqual("rep-a".to_string())
                },
                LabelMatcher {
                    name: "replica".to_string(),
                    matcher: StringMatcher::Match("rep.*".to_string())
                }
            ]
        );

        let matcher = SensorMatcher::from_prometheus_query("{__name__=~\"job:.*\"}").unwrap();
        assert_eq!(
            matcher.name_matcher,
            StringMatcher::Match("job:.*".to_string())
        );
        assert!(matcher.label_matchers.is_none());

        let matcher =
            SensorMatcher::from_prometheus_query("{__name__=\"\\\"quoted_named\\\"\"}").unwrap();
        assert_eq!(
            matcher.name_matcher,
            StringMatcher::Equal("\"quoted_named\"".to_string())
        );
        assert!(matcher.label_matchers.is_none());
    }

    #[test]
    fn test_sensor_matcher_errors() {
        assert!(SensorMatcher::from_prometheus_query("").is_err());
        assert!(SensorMatcher::from_prometheus_query("\"wrong{[(@").is_err());
        assert!(SensorMatcher::from_prometheus_query(
            "http_requests_total{job=\"prometheus\"}[5m]"
        )
        .is_err());
        assert!(
            SensorMatcher::from_prometheus_query("rate(http_requests_total[5m] offset 1w)")
                .is_err()
        );
        assert!(SensorMatcher::from_prometheus_query("http_requests_total @ 1609746000").is_err());
    }
}
