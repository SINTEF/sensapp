# ![SensApp](./docs/sensapp_logo.png)

SensApp is an open-source sensor data platform developed by SINTEF for research purposes. SensApp manages time series data from edge devices to big data digital twins at scale.

It works well as a standalone platform, or along existing time-series systems such as Prometheus or InfluxDB.

## Possible use cases

It supports various deployment scenarios such as:

- MQTT Sensor data recorder with a SQLite database as storage. Queried via the simple HTTP REST API.
- Prometheus long-term archives with DuckDB storage, that can still be queried via Prometheus.
- OPCUA data recorder, with PostgreSQL storage, and queried through PostGreSQL directly.
- Big data pipelines, with Apache Avro messages received from Kafka, and stored in ClickHouse. That you can query via Prometheus.
- Drop'in replacement of InfluxDB with SensApp and RRDtool *(this one is more a fun experiment)*.
- Python jobs loading data in BigQuery.
- And many more…

## Features

- **Flexible Time Series DataBase Storage**: Supports various time-series databases such as SQLite, PostgreSQL, TimeScaleDB, DuckDB, BigQuery, rrdcached, and soon ClickHouse. With the potential to extend support to other databases in the future.
- **(Planned) Data Lake Storage**: Supports Parquet files over S3 compatible object stores for long-term time-series data storage.
- **Multiple Data Ingestion Protocols**: Easy data ingestion via HTTP REST API, MQTT, AMQP, KAFKA, OPCUA, and NATS.
- **Compatibility with Existing Pipelines**: Offers Prometheus Remote Write and InfluxDB line format support for seamless integration into existing sensor data pipelines.
- **Data formats**: Supports various data formats like JSON, CSV, Parquet, or SenML.
- **(Planned) Python Module**: Provides a Python module for more advanced configurations and data pipelines.

## Architecture

SensApp should be stateless and scale from the edge to big data. But the diffult problems are handled next to SensApp. To scale, you would probably need a message queue, potentially distributed, and a good database. SensApp is a simple adapter.

- SensApp supports simple deployments without requiring a message queue and only an embedded SQLite or DuckDB database.
- SensApp supports medium deployments with a single message broker and a PostgreSQL or TimeScaleDB database.
- For larger deployments, SensApp advises a distributed message queue, an automatic load balancer for the SensApp instances, and a ClickHouse cluster as a database. BigQuery can also be considered.

Check the [ARCHITECTURE.md](docs/ARCHITECTURE.md) file for more details.

## Built With Rust™️

SensApp is developed using Rust, a language known for its good performances, its above average memory safety model, and its annoying borrow checker. SensApp used to be written in Scala about a decade ago, but the new author prefers Rust.

Not only the language, it's also the extensive high quality open-source ecosystem that makes Rust a great choice for SensApp:

- [Tokio](https://tokio.rs/) asynchronous runtime
- [Serde](https://serde.rs/) serialization framework
- [Axum](https://github.com/tokio-rs/axum) web framework
- [SQLx](https://github.com/launchbadge/sqlx) database driver
- [Polars](https://pola.rs) data frame library
- [nom](https://github.com/rust-bakery/nom) parser combinator library
- *and many more…*

## Contributing

We appreciate your interest in contributing to SensApp! Contributing is as simple as submitting an issue or a merge/pull request. Please read the [CONTRIBUTING.md](CONTRIBUTING.md) file for more details.

## License

This project is licensed under the Apache License 2.0 - see the [LICENSE](LICENSE) file for details.

The SensApp software is provided "as is," with no warranties, and the creators of SensApp are not liable for any damages that may arise from its use.

## You may not want to use it in production (yet)

SensApp is currently under development. It is not yet ready for production.

## Acknowledgments

We thank [the historical authors of SensApp](https://github.com/SINTEF/sensapp/graphs/contributors) who created the first version a decade ago.

SensApp is developed by
[SINTEF](https://www.sintef.no) ([Digital division](https://www.sintef.no/en/digital/), [Sustainable Communication Technologies department](https://www.sintef.no/en/digital/departments-new/department-of-sustainable-communication-technologies/), [Smart Data research group](https://www.sintef.no/en/expertise/digital/sustainable-communication-technologies/smart-data/)).

It is made possible thanks to the research and development of many research projects, founded notably by the [European Commission](https://ec.europa.eu/programmes/horizon2020/en) and the [Norwegian Research Council](https://www.forskningsradet.no/en/).

We also thank the open-source community for all the tools they create and maintain that allow SensApp to exist.
