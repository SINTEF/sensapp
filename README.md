# ![SensApp](./docs/sensapp_logo.png)

SensApp is an open-source sensor data platform developed by SINTEF.

It handles time-series data ingestion, storage, and retrieval. From small edge devices to big data digital twins, SensApp *may* be useful.

## SensApp allows you to process years of sensor data efficiently

SensApp is compatible with Prometheus and InfluxDB, but with an alternative architecture that prioritise data analysis and long-term storage over ingestion performance and real-time monitoring.

Dealing with system statistics for the last 24 hours? InfluxDB or Prometheus are excellent choices. Fetching average bathroom temperatures over the last 10 years grouped by day? SensApp will compute that instantly while InfluxDB or Prometheus will take a little while.

But you don't have to chose, both InfluxDB and Prometheus can replicate their data to SensApp for long-term storage and analysis. So you get the best of both worlds.

You can also use Sensapp as a standalone time-series database.

## Quickstart

The quickest way to run SensApp is with SQLite so no external database is required.

Start SensApp with SQLite:

```bash
SENSAPP_STORAGE_CONNECTION_STRING=sqlite://sensapp.db \
cargo run
```

By default, SensApp listens on [http://127.0.0.1:3000](http://127.0.0.1:3000).

Ingest one sample:

```bash
curl --json '[{"n": "temperature", "v": 21.5}]' \
  http://127.0.0.1:3000/publish
```

Query the stored data:

```bash
curl 'http://127.0.0.1:3000/api/v1/query?query=temperature'
# or
curl 'http://127.0.0.1:3000/api/v1/query?query=temperature&format=csv'
```

## Python Quickstart

Check the [python/sensapp](./python/sensapp) documentation for more details.

```python
import asyncio
from sensapp import SensAppClient

async def main():
    async with SensAppClient() as client:
        await client.publish("temperature", 21.5)
  [series] = await client.query("temperature[1h]")
  print(series.frame)

asyncio.run(main())
```

### Using Containers

```bash
docker compose up
```

You can deploy SensApp on Kubernetes using [the included Helm chart](./charts/sensapp).

```bash
helm install sensapp ./charts/sensapp
```

## Features

- **HTTP REST API**
- **Prometheus Compatibility**
  - **Prometheus Remote Write**: Prometheus can push data to SensApp.
  - **Prometheus Remote Read**: Prometheus can also read data from SensApp.
  - **Prometheus Scrape Endpoint**: SensApp exposes internal service metrics on `/prometheus/metrics`.
- **InfluxDB Compatibility**:
  - **InfluxDB Line Protocol**: You can use SensApp instead of InfluxDB, with [Telegraf](https://github.com/influxdata/telegraf) for example.
  - **InfluxDB Data Replication**: InfluxDB [can replicate its data to SensApp](https://docs.influxdata.com/influxdb/cloud/write-data/replication/replicate-data/).
- **Data formats**:
  - **JSON**: Simple and widely used format for data interchange.
  - **CSV**: The classic.
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

## Authentication

SensApp supports **optional JWT authentication**. By default, all endpoints are open. Visit [docs/JWT_AUTH.md](./docs/JWT_AUTH.md) for the authentication documentation.

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

SensApp is currently under development. It is not ready for production.

## Acknowledgments

We thank [the historical authors of SensApp](https://github.com/SINTEF/sensapp/graphs/contributors) who created the first version a decade ago.

SensApp is developed by
[SINTEF](https://www.sintef.no) ([Digital division](https://www.sintef.no/en/digital/), [Sustainable Communication Technologies department](https://www.sintef.no/en/digital/departments-new/department-of-sustainable-communication-technologies/), [Smart Data research group](https://www.sintef.no/en/expertise/digital/sustainable-communication-technologies/smart-data/)).

It is made possible thanks to the research and development of many research projects, founded notably by the [European Commission](https://ec.europa.eu/programmes/horizon2020/en) and the [Norwegian Research Council](https://www.forskningsradet.no/en/).

We also thank the open-source community for all the tools they create and maintain that allow SensApp to exist.
