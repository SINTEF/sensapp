# ![SensApp](./docs/sensapp_logo.png)

SensApp is an open-source sensor data platform developed by SINTEF for research purposes. It manages time-series data from a multitude of sensors.

It enables the handling of small time series data of the edge efficiently to large-scale big data digital twins.

## Features

- **Flexible Time Series DataBase Storage**: Supports various time-series databases like SQLite, PostgreSQL (with optional TimeScaleDB plugin), and ClickHouse, with the potential to extend support to other databases in the future.
- **Data Lake Storage**: Supports Parquet files over S3 compatible object stores for long-term time-series data storage.
- **Multiple Data Ingestion Protocols**: Easy data ingestion via HTTP REST API, MQTT, AMQP, KAFKA, and NATS.
- **Compatibility with Existing Pipelines**: Offers Prometheus Remote Write and InfluxDB line format support for seamless integration into existing sensor data pipelines.
- **Data formats**: Supports various data formats like JSON, CSV, Parquet, or SenML.

## Architecture

SensApp should be stateless and scale from the edge to big data. The message queue software and the database software solve the complex problems. SensApp is a simple adapter between.

* SensApp supports simple deployments without requiring a message queue and only an embedded SQLite database.
* SensApp supports medium deployments with a single message broker and a PostgreSQL database.
* For larger deployments, SensApp advises a distributed message queue, an automatic load balancer for the SensApp instances, and a ClickHouse cluster.

Check the [ARCHITECTURE.md](docs/ARCHITECTURE.md) file for more details.

## Built With Rust™️

SensApp is developed using Rust, a language known for its performance, memory safety, and annoying borrow checker. SensApp used to be written in Scala, but the new author prefers Rust.

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

We also thank the open-source community for all the tools they create and maintain that allow SensApp to exist.
