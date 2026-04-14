use crate::storage::Aggregation;

pub(super) fn sqlite_bucketed_cte(table_name: &str) -> String {
    format!(
        r#"
        WITH bucketed AS (
            SELECT
                timestamp_us,
                value,
                (?5 + ((timestamp_us - ?5) / ?4) * ?4) AS bucket_us
            FROM {table_name}
            WHERE sensor_id = ?1
              AND (?2 IS NULL OR timestamp_us >= ?2)
              AND (?3 IS NULL OR timestamp_us <= ?3)
        )
        "#
    )
}

pub(super) fn sqlite_group_by_clause() -> &'static str {
    "FROM bucketed GROUP BY 1 ORDER BY 1 ASC LIMIT ?6"
}

pub(super) fn sqlite_first_last_query(
    table_name: &str,
    aggregation: Aggregation,
    value_expression: &str,
) -> String {
    let direction = match aggregation {
        Aggregation::First => "ASC",
        Aggregation::Last => "DESC",
        _ => unreachable!("only first/last use sqlite_first_last_query"),
    };

    format!(
        r#"
        WITH bucketed AS (
            SELECT
                timestamp_us,
                {value_expression} AS value,
                (?5 + ((timestamp_us - ?5) / ?4) * ?4) AS bucket_us
            FROM {table_name}
            WHERE sensor_id = ?1
              AND (?2 IS NULL OR timestamp_us >= ?2)
              AND (?3 IS NULL OR timestamp_us <= ?3)
        ),
        ranked AS (
            SELECT
                bucket_us,
                value,
                ROW_NUMBER() OVER (PARTITION BY bucket_us ORDER BY timestamp_us {direction}) AS row_num
            FROM bucketed
        )
        SELECT bucket_us AS timestamp_us, value
        FROM ranked
        WHERE row_num = 1
        ORDER BY bucket_us ASC
        LIMIT ?6
        "#
    )
}

pub(super) fn sqlite_integer_expression(aggregation: Aggregation) -> &'static str {
    match aggregation {
        Aggregation::Min => "MIN(value)",
        Aggregation::Max => "MAX(value)",
        Aggregation::Sum => "SUM(value)",
        Aggregation::First | Aggregation::Last | Aggregation::Avg | Aggregation::Count => {
            unreachable!("handled separately")
        }
    }
}

pub(super) fn sqlite_float_expression(aggregation: Aggregation) -> &'static str {
    match aggregation {
        Aggregation::Avg => "AVG(value)",
        Aggregation::Min => "MIN(value)",
        Aggregation::Max => "MAX(value)",
        Aggregation::Sum => "SUM(value)",
        Aggregation::First | Aggregation::Last | Aggregation::Count => {
            unreachable!("handled separately")
        }
    }
}

pub(super) fn sqlite_numeric_expression(aggregation: Aggregation) -> &'static str {
    match aggregation {
        Aggregation::Avg => "AVG(value)",
        Aggregation::Min => "MIN(value)",
        Aggregation::Max => "MAX(value)",
        Aggregation::Sum => "SUM(value)",
        Aggregation::First | Aggregation::Last | Aggregation::Count => {
            unreachable!("handled separately")
        }
    }
}
