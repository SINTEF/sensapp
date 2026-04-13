from __future__ import annotations

from typing import Any

from sensapp._models import (
    MetricsCatalog,
    SeriesCatalog,
    parse_metrics_catalog,
    parse_series_catalog,
)


def _make_metric_dataset(**overrides: Any) -> dict[str, Any]:
    base: dict[str, Any] = {
        "@type": "dcat:Dataset",
        "@id": "temperature",
        "dct:identifier": "metric:temperature",
        "dct:title": "temperature",
        "dct:description": "Aggregated metric 'temperature'",
        "dcat:keyword": ["metric", "aggregated", "time-series", "float"],
        "sensor:type": "float",
        "sensor:seriesCount": 3,
        "sensor:labelDimensions": ["room", "floor"],
        "dct:temporal": {"@type": "dct:PeriodOfTime"},
        "dcat:distribution": [
            {
                "@type": "dcat:Distribution",
                "dcat:accessURL": "/series?metric=temperature",
                "dcat:mediaType": "application/json",
                "dct:format": "DCAT Series Catalog",
            }
        ],
    }
    base.update(overrides)
    return base


def _make_series_dataset(**overrides: Any) -> dict[str, Any]:
    base: dict[str, Any] = {
        "@type": "dcat:Dataset",
        "@id": 'temperature{room="lab"}',
        "dct:identifier": "abc-123",
        "dct:title": "temperature",
        "dct:description": "Sensor data from temperature (float)",
        "dcat:keyword": ["sensor", "IoT", "time-series", "float", "room"],
        "sensor:type": "float",
        "sensor:labels": [{"room": "lab"}],
        "dct:temporal": {"@type": "dct:PeriodOfTime"},
        "dcat:distribution": [
            {
                "@type": "dcat:Distribution",
                "dcat:downloadURL": "/series/abc-123?format=senml",
                "dcat:mediaType": "application/senml+json",
                "dct:format": "SenML JSON",
            },
        ],
    }
    base.update(overrides)
    return base


def test_parse_metrics_catalog_basic() -> None:
    raw = {
        "@type": "dcat:Catalog",
        "dcat:dataset": [_make_metric_dataset()],
    }
    catalog = parse_metrics_catalog(raw)

    assert isinstance(catalog, MetricsCatalog)
    assert len(catalog.metrics) == 1

    m = catalog.metrics[0]
    assert m.name == "temperature"
    assert m.identifier == "metric:temperature"
    assert m.sensor_type == "float"
    assert m.series_count == 3
    assert m.label_dimensions == ["room", "floor"]
    assert m.unit is None
    assert len(m.distributions) == 1


def test_parse_metrics_catalog_with_unit() -> None:
    raw = {
        "dcat:dataset": [_make_metric_dataset(**{"sensor:unit": "°C"})],
    }
    catalog = parse_metrics_catalog(raw)

    assert catalog.metrics[0].unit == "°C"


def test_parse_metrics_catalog_empty() -> None:
    catalog = parse_metrics_catalog({"dcat:dataset": []})
    assert catalog.metrics == []


def test_parse_series_catalog_basic() -> None:
    raw = {
        "@type": "dcat:Catalog",
        "dcat:dataset": [_make_series_dataset()],
    }
    catalog = parse_series_catalog(raw)

    assert isinstance(catalog, SeriesCatalog)
    assert len(catalog.series) == 1

    s = catalog.series[0]
    assert s.uuid == "abc-123"
    assert s.name == "temperature"
    assert s.sensor_type == "float"
    assert s.labels == {"room": "lab"}
    assert catalog.next_bookmark is None


def test_parse_series_catalog_pagination() -> None:
    raw = {
        "dcat:dataset": [_make_series_dataset()],
        "hydra:view": {
            "@type": "hydra:PartialCollectionView",
            "hydra:next": "/series?limit=10&bookmark=page2&metric=temperature",
            "hydra:itemsPerPage": 10,
        },
    }
    catalog = parse_series_catalog(raw)

    assert catalog.next_bookmark == "page2"


def test_parse_series_catalog_no_pagination() -> None:
    raw = {"dcat:dataset": [_make_series_dataset()]}
    catalog = parse_series_catalog(raw)

    assert catalog.next_bookmark is None


def test_parse_series_catalog_multiple_labels() -> None:
    ds = _make_series_dataset(**{"sensor:labels": [{"room": "lab"}, {"floor": "2"}]})
    raw = {"dcat:dataset": [ds]}
    catalog = parse_series_catalog(raw)

    assert catalog.series[0].labels == {"room": "lab", "floor": "2"}


def test_parse_series_catalog_with_unit() -> None:
    ds = _make_series_dataset(**{"sensor:unit": "hPa"})
    raw = {"dcat:dataset": [ds]}
    catalog = parse_series_catalog(raw)

    assert catalog.series[0].unit == "hPa"
