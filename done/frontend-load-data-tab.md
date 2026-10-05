# Frontend: a "Load Data" tab

Branch: `frontend-good-enough`.

## Goal

The header gets two tabs: **Data Explorer** (`/ui/`) and **Load Data** (`/ui/load`). The second one explains how
to get data into SensApp, with snippets that are ready to copy (same dark code block and copy button as the Code
dialog), filled with the address of the server the page is served from.

Ways, as tabs of the page (`?via=` in the address): Python SDK, Telegraf, Prometheus, curl. Kept short on purpose:
enough to start, not a reference (the reference is `/docs`).

- **Python SDK**: a big DataFrame (sent by slices), one sample at a time, a few samples at a time.
- **Telegraf**: `outputs.influxdb_v2` against `/api/v2/write`, with inputs.
- **Prometheus**: `remote_write` and `remote_read`.
- **curl**: SenML JSON, CSV, InfluxDB line protocol.

When the server asks for a token the snippets read it from `SENSAPP_TOKEN` and say how to make a `write` one.

## Steps

1. [x] Header tabs, `/load` route (lazy), shared code block, snippets in `src/lib/loadSnippets.ts` + tests
2. [x] Check every snippet against a real SensApp (Python, Telegraf, curl, with and without a JWT secret)
3. [x] `docs/FRONTEND.md`, move this file to `done/`

## Findings

- All snippets were run against a real SensApp (PostgreSQL, one instance open and one with a JWT secret): 43 200
  samples of a DataFrame arrived in slices, curl's three formats, Telegraf with `--once`, and `promtool check config`
  accepts the Prometheus files. (No live Prometheus run: the endpoints are covered by the server tests.)
- **Telegraf's `token` does not work with JWT**: it sends `Authorization: Token ...`, SensApp wants `Bearer`, and
  answered `401`. The snippet overrides the header with `http_headers`, checked live. Written in
  `ideas/influxdb-token-authorization-scheme.md`.
- InfluxDB measurement and field make the series name with a space (`cpu usage_idle`), not an underscore.
- Prometheus's token is a file: the tab gives the command, with a year of validity (the default is an hour).
- The Code button is about the explorer, so it is hidden on the Load Data tab.
- Not done on purpose: Grafana, MQTT, other languages, reading data back on this tab (the Code dialog does it).
