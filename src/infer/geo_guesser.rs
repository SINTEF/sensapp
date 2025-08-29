use super::columns::InferedColumn;
use regex::Regex;
use rust_decimal::Decimal;

// Additional coordinate validation helpers available if needed

static LATITUDE_REGEX: once_cell::sync::Lazy<Regex> = once_cell::sync::Lazy::new(|| {
    Regex::new(r"(?i)^(gps_?(loc_?|location_?|position_?|)|geo_?(loc_?|location_?|position_?|)|position_?|pos_?|coord_?|coordinates_?|)(lat|latitude)$")
        .expect("Failed to compile latitude regex")
});

static LONGITUDE_REGEX: once_cell::sync::Lazy<Regex> = once_cell::sync::Lazy::new(|| {
    Regex::new(
        r"(?i)^(gps_?(loc_?|location_?|position_?|)|geo_?(loc_?|location_?|position_?|)|position_?|pos_?|coord_?|coordinates_?|)(lng|lon|long|longitude)$",
    )
    .expect("Failed to compile longitude regex")
});

pub enum GeoType {
    Latitude,
    Longitude,
}

pub fn lat_long_guesser(mode: GeoType, column_name: &str, column: &InferedColumn) -> isize {
    let regex = match mode {
        GeoType::Latitude => LATITUDE_REGEX.clone(),
        GeoType::Longitude => LONGITUDE_REGEX.clone(),
    };

    let mut sum = 0_isize;

    sum += if regex.is_match(column_name) {
        100
    } else {
        -200
    };

    sum += match column {
        InferedColumn::Float(values) => {
            let validator = match mode {
                GeoType::Latitude => |v: f64| (-90.0..=90.0).contains(&v),
                GeoType::Longitude => |v: f64| (-180.0..=180.0).contains(&v),
            };
            if values.iter().all(|value| validator(*value)) {
                99
            } else {
                -101
            }
        }
        InferedColumn::Numeric(values) => {
            let (min_val, max_val) = match mode {
                GeoType::Latitude => (Decimal::new(-90, 0), Decimal::new(90, 0)),
                GeoType::Longitude => (Decimal::new(-180, 0), Decimal::new(180, 0)),
            };
            if values
                .iter()
                .all(|value| *value >= min_val && *value <= max_val)
            {
                98
            } else {
                -102
            }
        }
        _ => -103,
    };

    sum
}

#[derive(Debug, Clone, PartialEq)]
pub struct LatLonColumnNames {
    pub lat: String,
    pub lon: String,
}

pub fn likely_geo_columns(
    column_names: &[String],
    columns: &[InferedColumn],
) -> Option<LatLonColumnNames> {
    let latitude_best_candidate = column_names
        .iter()
        .zip(columns.iter())
        .map(|(column_name, column)| {
            (
                column_name,
                lat_long_guesser(GeoType::Latitude, column_name, column),
            )
        })
        .filter(|(_, score)| *score > 0)
        .max_by_key(|(_, score)| *score);

    // No need to find a longitude if we don't have a latitude
    latitude_best_candidate?;

    let longitude_best_candidate = column_names
        .iter()
        .zip(columns.iter())
        .map(|(column_name, column)| {
            (
                column_name,
                lat_long_guesser(GeoType::Longitude, column_name, column),
            )
        })
        .filter(|(_, score)| *score > 0)
        .max_by_key(|(_, score)| *score);

    let (lon_column_name, score_lon) = longitude_best_candidate?;
    let (lat_column_name, score_lat) = latitude_best_candidate?;

    // If the score isn't identical, this is weird.
    // So no automatic guessing.
    if score_lat != score_lon {
        return None;
    }

    Some(LatLonColumnNames {
        lat: lat_column_name.clone(),
        lon: lon_column_name.clone(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_lat_long_guesser() {
        let column = InferedColumn::Float(vec![0.0, 1.0, 2.0]);
        assert_eq!(lat_long_guesser(GeoType::Latitude, "lat", &column), 199);
        assert_eq!(lat_long_guesser(GeoType::Longitude, "lon", &column), 199);
        assert_eq!(
            lat_long_guesser(GeoType::Longitude, "geo_longitude", &column),
            199
        );
        assert_eq!(
            lat_long_guesser(GeoType::Longitude, "geoloc_lng", &column),
            199
        );
        assert_eq!(lat_long_guesser(GeoType::Longitude, "speed", &column), -101);
        assert_eq!(
            lat_long_guesser(GeoType::Longitude, "altitude", &column),
            -101
        );

        let column = InferedColumn::Float(vec![0.0, 1.0, 2.0, 200.0]);
        assert_eq!(lat_long_guesser(GeoType::Latitude, "lat", &column), -1);
        assert_eq!(lat_long_guesser(GeoType::Longitude, "lon", &column), -1);
        assert_eq!(lat_long_guesser(GeoType::Longitude, "speed", &column), -301);
        assert_eq!(
            lat_long_guesser(GeoType::Longitude, "altitude", &column),
            -301
        );

        let column = InferedColumn::Numeric(vec![
            Decimal::new(0, 0),
            Decimal::new(1, 0),
            Decimal::new(2, 0),
        ]);
        assert_eq!(lat_long_guesser(GeoType::Latitude, "lat", &column), 198);
        assert_eq!(lat_long_guesser(GeoType::Longitude, "lon", &column), 198);

        let column = InferedColumn::Numeric(vec![
            Decimal::new(0, 0),
            Decimal::new(1, 0),
            Decimal::new(2, 0),
            Decimal::new(200, 0),
        ]);
        assert_eq!(lat_long_guesser(GeoType::Latitude, "lat", &column), -2);
        assert_eq!(lat_long_guesser(GeoType::Longitude, "lon", &column), -2);

        let column = InferedColumn::Boolean(vec![true, false, true]);
        assert_eq!(lat_long_guesser(GeoType::Latitude, "lat", &column), -3);
        assert_eq!(lat_long_guesser(GeoType::Longitude, "ready", &column), -303);
    }

    #[test]
    fn test_likely_geo_columns() {
        let column_names = vec![
            "lat".to_string(),
            "lon".to_string(),
            "speed".to_string(),
            "altitude".to_string(),
        ];
        let columns = vec![
            InferedColumn::Float(vec![0.0, 1.0, 2.0]),
            InferedColumn::Float(vec![0.0, 1.0, 2.0]),
            InferedColumn::Float(vec![0.0, 1.0, 2.0]),
            InferedColumn::Float(vec![0.0, 1.0, 2.0]),
        ];
        let result = likely_geo_columns(&column_names, &columns);
        assert!(result.is_some());
        let result = result.unwrap();
        assert_eq!(result.lat, "lat");
        assert_eq!(result.lon, "lon");

        let column_names = vec![
            "weight".to_string(),
            "height".to_string(),
            "speed".to_string(),
            "altitude".to_string(),
        ];
        let result = likely_geo_columns(&column_names, &columns);
        assert!(result.is_none());

        let column_names = vec![
            "geo_position_latitude".to_string(),
            "height".to_string(),
            "speed".to_string(),
            "altitude".to_string(),
        ];
        let result = likely_geo_columns(&column_names, &columns);
        assert!(result.is_none());

        let column_names = vec![
            "geoposition_latitude".to_string(),
            "geoposition_longitude".to_string(),
            "speed".to_string(),
            "altitude".to_string(),
        ];
        let columns = vec![
            InferedColumn::Float(vec![0.0, 1.0, 2.0]),
            // Weird coordinates
            InferedColumn::Float(vec![0.0, 1.0, -1000.0]),
            InferedColumn::Float(vec![0.0, 1.0, 2.0]),
            InferedColumn::Float(vec![0.0, 1.0, 2.0]),
        ];
        let result = likely_geo_columns(&column_names, &columns);
        assert!(result.is_none());

        let columns = vec![
            InferedColumn::Float(vec![0.0, 1.0, 2.0]),
            // Different type
            InferedColumn::Numeric(vec![
                Decimal::new(0, 0),
                Decimal::new(1, 0),
                Decimal::new(2, 0),
            ]),
            InferedColumn::Float(vec![0.0, 1.0, 2.0]),
            InferedColumn::Float(vec![0.0, 1.0, 2.0]),
        ];
        let result = likely_geo_columns(&column_names, &columns);
        assert!(result.is_none());
    }
}
