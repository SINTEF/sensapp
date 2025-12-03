# ![SensApp](./docs/sensapp_logo.png)

SensApp is an open-source sensor data platform developed by SINTEF.

It handles time-series data ingestion, storage, and retrieval. From small edge devices to big data digital twins, SensApp *may* be useful.

## SensApp allows you to process years of sensor data efficiently

SensApp is compatible with Prometheus and InfluxDB, but with an alternative architecture that prioritise data analysis and long-term storage over ingestion performance and real-time monitoring.

Handling CPU stats for the last 24 hours? InfluxDB or Prometheus are excellent choices. Fetching average bathroom temperature over the last 10 years grouped by day? SensApp will compute that instantly while InfluxDB or Prometheus will take a while as they must read many chunks of data.

But you don't have to chose, both InfluxDB and Prometheus can replicate their data to SensApp for long-term storage and analysis.

Of course you can also use Sensapp as a standalone time-series database.

## Features

- **HTTP REST API**
- **Compatible with existing sensor data pipelines**:
  - **Prometheus Remote Write**: Prometheus can push data to SensApp.
  - **Prometheus Remote Read**: Prometheus can also read data from SensApp.
  - **InfluxDB Line Protocol**: InfluxDB can push data to SensApp, or you can use SensApp instead of InfluxDB, with [Telegraf](https://github.com/influxdata/telegraf) for example.
- **Data formats**:
  - **JSON**: Simple and widely used format for data interchange.
  - **CSV**: Many users *love* CSV.
  - **SenML**: Standardized format for sensor data representation, that is almost unheard of but actually pretty good.
  - **Apache Arrow IPC Support**: Efficient IPC format for high-performance data interchange.
- **Flexible Time Series DataBase Storage**:
  - **SQLite**: Lightweight embedded database for edge deployments.
  - **DuckDB**: Alternative to SQLite, potentially faster for analytical queries. *Not enabled by default*.
  - **PostgreSQL**: Robust relational database for medium to large deployments, with optional TimeScaleDB plugin for enhanced time-series capabilities.
  - **ClickHouse**: Columnar database management system for high-performance analytical queries on large volumes of data.
  - **BigQuery**: Fully-managed serverless data warehouse for scalable analysis. *Not enabled by default*.
  - **RRDCached**: Integration with RRDtools, mostly implemented for fun. *Not enabled by default*.

## Architecture

SensApp's architecture is relatively simple as the complex problems are delegated to existing databases. It's a stateless adapter between HTTP clients and the chosen time-series database(s).

Most of the complexity lies in the [database schema design](docs/DATAMODEL.md). After that, it's mostly some code glue.

- On the **edge**, SensApp can be deployed as a single lightweight instance with an embedded SQLite database.
- For **medium** deployments, SensApp can be deployed with a single message broker and a PostgreSQL database.
- For **larger** deployments, many SensApp instances can be deployed behind a load balancer, connected to a ClickHouse database cluster.

SensApp storage is based on the findings of the paper [TSM-Bench: Benchmarking Time Series Database Systems for Monitoring Applications](https://dl.acm.org/doi/abs/10.14778/3611479.3611532). ClickHouse also released [an experimental time-series engine](https://clickhouse.com/docs/engines/table-engines/special/time_series) that is somewhat similar to SensApp's storage schema.

Check the [ARCHITECTURE.md](docs/ARCHITECTURE.md) file for more details.

## Development

```bash
# Build
cargo build

# Test
cargo test
cargo make test-all         # all storage backends

# Lint (format + clippy)
cargo make lint
cargo make lint-all         # all storage backends

# Full validation
cargo make check-all        # working features (postgres + sqlite)
cargo make check-all-storage # all storage backends

# Setup (runs migrations)
cargo make setup-dev
```

Override environment variables as needed: `DATABASE_URL`, `POSTGRES_USER`, etc.

## Built With Rust™️

SensApp is developed using Rust, a language known for its performance, memory safety, and annoying borrow checker. SensApp used to be written in Scala, but the new author prefers Rust.

Another reason is from the results from the paper [Energy efficiency across programming languages: how do energy, time, and memory relate?](https://dl.acm.org/doi/10.1145/3136014.3136031), which shows Rust as one of the most energy-efficient programming languages while having memory safety.

Not only the language, it's also the extensive high quality open-source ecosystem that makes Rust a great choice for SensApp.

*Here ends the mandatory Rust promotion paragraph.*

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
