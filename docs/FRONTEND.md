# Web UI

SensApp ships a small explorer: pick a metric, pick series, draw them over a time range. A second tab, **Load Data**, explains how to get data in, and a third, **Credentials**, makes tokens. It is a React single page application in [`frontend/`](../frontend), built to static files that SensApp serves itself. There is no separate container.

## Serving

- The files are served under `/ui/`, and `/` redirects there. The container image carries them (the Dockerfile builds them in its own Node stage); with `cargo run`, build them first with `npm run build` in `frontend/`.
- `SENSAPP_UI_ENABLED=false` turns it off, `SENSAPP_UI_DIR` says where the files are. See [CONFIGURATION.md](CONFIGURATION.md#web-ui). A missing build is a warning at startup, never a failure.
- The files are public. Responses carry a `Content-Security-Policy` (the page only talks to its own origin, and cannot be framed), `X-Content-Type-Options: nosniff` and `Cache-Control: no-cache`.
- The UI calls the SensApp API of its own origin: `/metrics`, `/series`, `/series/{uuid}` and `/health/ready`.

## The header

From 768 px (`md`) the header is one row: the logo, the three tabs, then the Code, Download, API Docs and sign-in buttons. Below, the logo and the buttons keep the first row and the tabs get a row of their own, side by side across the width, so nothing scrolls sideways and the logo never shrinks (the small logo, without the name, is used under 640 px).

Under 1024 px (`lg`) the page scrolls, and the lists of metrics and series are each capped at 70% of the screen height and scroll inside their card; without it, hundreds of metrics would push the series list thousands of pixels down. From `lg` the explorer fills the window and the cards share it.

## Charts

Two selectors next to the time range control what the server computes, as in the Influx and Prometheus UIs:

- **Step**: `Auto`, `Raw`, or a fixed duration (`5s` … `1d`). `Auto` reads ranges under about 2.8 hours as they are, and above that cuts the range in at most 2 000 buckets of a round duration, because the server refuses more than 100 000 raw samples of a series ([HTTP_LIMITS.md](HTTP_LIMITS.md)). `Raw` on a range that is too wide gets that refusal.
- **Aggregation**: `avg`, `min`, `max`, `sum`, `count`, `first`, `last`, applied to the samples of each step. Disabled on `Raw`.

Series are drawn as a `line`, `step`, `area`, `stacked` or `bars` chart, on a linear or `log` scale. The chart has no legend of its own: the list of series shows the color of each selected series, which it keeps for as long as it is selected. A series that is selected or unselected comes and goes with an animation, the others stay. Hovering a row of the list makes its series stand out in the chart.

### The time window

There is one time window, the one of the explorer, and the chart draws exactly that. The chart has no zoom of its own (a zoom on points that were cut for another window shows the same coarse points bigger):

- **Drag on the chart** to choose a window: it is asked of the server again, at the step that fits it (raw samples when zoomed in far enough). The old points stay, on the new axis, until the new ones arrive; a thin bar over the chart says it is loading.
- **In the header of the chart**, with the style and the scale: `↶` goes back to the window before (the last 20 windows the user left; what the clock does to a preset is not one), `‹` and `›` move by half a window (`›` never goes past now, and is off on a live preset), `−` shows twice the window (a live preset goes to the next one), then the presets, the dates, the step and the aggregation.
- **The tooltip** over the chart gives the value of each series at the time under the pointer. With many series it would cover the chart, so it shows 10 rows at most (the 10 largest values, and "and N more"), the metric name is said once in the header when the series are all of one metric (the rows keep only their labels), a long name is cut with an ellipsis, never the value after it, and the tooltip stays under 480 px wide. The full names are in the list of series.
- A preset window is **live**: its end is now, and moves every minute while the tab is on screen. A window that was dragged, moved or typed is not.

### Colors

The first 8 series have the eight hues of the dataviz palette, validated for color-blind readers (neighbours are 8.4 or more apart in OKLab ×100 under protan and deutan vision, 19 or more with normal vision). The 16 colors after them are there so that no series repeats a color: each was chosen as the farthest, under normal and color-blind vision, from the ones before it. They are not equally good: the 9th to 11th are 10 or more from the others, from the 12th it goes down to 5 or 6. Past about 8 series the colors do not say which line is which, which is why hovering a row of the list highlights its line. The 25th series and the following ones take the colors again. A selected series keeps its color while it is selected. The colors are in `src/lib/palette.ts`, for the light and the dark theme. Booleans are drawn as a 0/1 step line on an axis of their own, whatever the style, and always read as they are (the server cannot aggregate them). Strings and locations are listed but not drawn.

## Code

The **Code** button of the header opens the code that loads what the explorer shows, in two tabs: the [Python SDK](../python/sensapp/README.md) (`get_series`, which gives a Polars DataFrame) and `curl` (one request for each series, as CSV). A button copies it.

- It is made from the state of the explorer: the selected series (their uuid, named in a comment: what every series has is said once, as in the list), the time window, the step and the aggregation. A series the step cannot average (booleans, strings) is read as it is, as in the chart.
- A preset window is **relative to now** in Python (`datetime.now(UTC) - timedelta(hours=24)`), so the script can be run again later; a window of dates, and every curl request, have the dates written out.
- The server is the origin of the page. When the server asked for a token, or one is in use, the code reads it from `SENSAPP_TOKEN` (`os.environ["SENSAPP_TOKEN"]`, `$SENSAPP_TOKEN`) and says how to make one. **The token of the session is never written in the code**: a snippet is meant to be pasted, saved and shared.
- **A selector** typed in the box of the series list (`{host="a"}`, with the metric) is what the code asks for, instead of the uuids: `list_series(metric=…, selector=…)` then `get_series` of each in Python, and `curl -G --data-urlencode` into `jq` and a loop in the shell (`jq` is needed). The code then reads every series that matches, whenever it runs, which is not always what is checked: the checkbox above the code goes back to the selected series. The selector is forgotten when another metric is chosen. As the series are not known when the code is written, a step is only asked for the numbers among them (Python looks at `sensor_type`, which the server writes `Float`; the shell loop leaves the others out and says so). A selector that matches more than a page (256 series) reads the first page.
- With no series selected, and no selector, it lists the series of the metric, and with no metric the metrics.
- Whatever a label or a name says is quoted (Python and shell) or put on one line (comments): a hostile label cannot add a line of code to what is copied.
- The code is always on a dark ground, in both themes, in JetBrains Mono, coloured by highlight.js (Python and Bash only). Long lines wrap instead of scrolling sideways (a uuid or an address with no space breaks where it must); the copied text is unchanged. The dialog, the highlighter and the font are loaded when it is first opened: 13 kB gzipped and 40 kB of font, nothing for a page that never opens it. The copy works on a page served by plain http too, where the clipboard API does not exist.
- The Python SDK is not on PyPI yet. The script starts with a [PEP 723](https://peps.python.org/pep-0723/) block (`# /// script`) that says it needs `sensapp` and that it comes from GitHub (`[tool.uv.sources.sensapp]`), so `uv run script.py` is enough: uv makes the environment. The `uv pip install` command is there too, for an environment of one's own. Both are in `src/lib/snippets.ts` (`SCRIPT_METADATA`, `INSTALL_COMMENT`): change them when the SDK is published, and the Python the block asks for follows `requires-python` of the SDK.

## Download

The **Download** button of the header saves the selected series as files, **one file per series**. It is off while no series is selected.

- The dialog asks the **format** (CSV, JSON Lines, SenML, Arrow), the **window** (the chart's, or all the data: no `start` and `end`) and the **samples** (raw, or aggregated with a step and an aggregation). It starts from what the chart draws: its step (`auto` resolved for the window) and aggregation, or raw. Text and boolean series are always downloaded raw, as on the chart.
- The series are fetched **one after the other** from `GET /series/{uuid}?…&download=true`, each with its status in the dialog (waiting, downloading, saved, or the message of the server). Closing the dialog cancels what is left; there is no job queue on the server.
- **The sample cap** (100,000 by default, `SENSAPP_HTTP_MAX_QUERY_SAMPLES`) applies to each series: a series over it is not saved and shows the server's message ("narrow the time range or use aggregation"), the others are saved. A 401 or 403 opens the sign-in dialog and stops.
- **Why not a plain link**: the token of the UI is sent in the `Authorization` header, which a link the browser follows cannot carry. The dialog fetches the file with the token, holds it in a `Blob` (a few MB at most under the cap) and saves it with `<a download>`. Bigger downloads, without the copy in memory, would need signed links (`ideas/signed-download-links.md`).
- **The file name** is the server's, from the `Content-Disposition` that `download=true` adds: the series name, its labels sorted by key, the window in UTC and the step, `temperature_room-kitchen_20261006T090000Z_20261006T100000Z_5m-avg.csv` (`json` for SenML, `jsonl`, `arrow`). The labels are there because a single-series CSV is only `timestamp,value`, and the selected series often share a name. The header has a UTF-8 `filename*` and an ASCII `filename` (what `curl -OJ` uses): characters no file system takes become `_`, a leading dot is dropped, and the name is cut at 150 characters. The code is in `src/http/download.rs`.
- Browsers may ask, once for the site, to allow several downloads at a time.
- The dialog is loaded when it is first opened (3 kB gzipped). The code is in `src/components/DownloadDialog.tsx` and `src/lib/download.ts`.

## Load Data

The second tab of the header (`/ui/load`) explains how to get data into SensApp, with code to copy (the same dark block and copy button as the Code dialog). It is short on purpose, enough to start; `/docs` is the reference. No card, no title (the tab is the title): the page uses the whole width and scrolls by itself (the explorer fills the window, this page does not), with underlined tabs like the header's, and a section is a title and a sentence on the left, its code on the right (stacked on a narrow screen). The way is a tab of the page and is in the address (`/ui/load?via=telegraf`; `python`, `telegraf`, `prometheus`, `curl`; Python when it is missing or unknown).

- **Python SDK**: a whole DataFrame (sent by slices of 100 000 samples, as [PYTHON_SDK.md](PYTHON_SDK.md#timeouts) advises), one sample at a time, a few samples at a time. Same `uv run` header as the Code dialog.
- **Telegraf**: `outputs.influxdb_v2` against `/api/v2/write` with a few inputs (see [INFLUX_DB.md](INFLUX_DB.md)), and how to check it (`--once`).
- **Prometheus**: `remote_write` and `remote_read` (`read_recent: true`), and the address to use from a container.
- **curl**: SenML JSON, CSV and InfluxDB line protocol, to `/publish` and `/api/v2/write`.
- The server is the origin of the page. When it asked for a token (or one is in use) every way starts with the same **A token** section: where the code finds it (`SENSAPP_TOKEN`), the command that makes a `write` one (`sensapp generate-token me --scope write`) and a link to the [Credentials](#credentials) tab; **no token is ever written in the code**. Prometheus reads its token from a file, so its section gives the command that writes it (a year long, for reading and writing). With an open server there is no such section.
- **Telegraf and tokens.** Telegraf sends its `token` as `Authorization: Token …`, the InfluxDB way, and SensApp reads that scheme as well as `Bearer …`. The snippet gives Telegraf its own `token = "${SENSAPP_TOKEN}"`, no header to rewrite. Checked against a server with a secret.
- The snippets are in `src/lib/loadSnippets.ts` (one function for each way, tested), the page in `src/pages/LoadPage.tsx`. The page is loaded when its tab is first opened (5 kB gzipped, the highlighter is shared with the Code dialog).
- All of them were run against a real SensApp (PostgreSQL), with and without a JWT secret (the Telegraf one before SensApp read the `Token` scheme, with a `Bearer` header): the Python scripts, the curl commands, Telegraf (`--once`), and the Prometheus configuration with `promtool check config`.

## Credentials

The third tab (`/ui/credentials`) makes tokens for the clients of SensApp (see [JWT_AUTH.md](JWT_AUTH.md)). It has the layout of Load Data: a title and a sentence on the left, the choice on the right, the whole width, no card. The choices, by weight:

1. **Name**: who or what the token is for. It is the `sub` of the token, so it is in the logs of every request the token makes; it grants nothing.
2. **Valid for**: 1 hour, 1 day (the default), 30 days, 1 year. A token cannot be revoked, so this is the main safeguard.
3. **What it may do**: `read` and `write` by default, `delete` on demand, and `admin`, which only the command line makes.
4. **Only these sensors**: type a name and press Enter (or Add), remove it with its ×. A name is kept exactly as typed, spaces and commas included, and the sensors of the server are offered to click when the token in use can read them (an admin-only token cannot).
5. **Command line**: the `sensapp generate-token …` command for what the form says, updated as it changes, with a `--sensor` for each name and everything quoted for the shell. It works without an admin token, needs the secret (so it runs where SensApp runs: `docker exec`, `kubectl exec`), and is the only way to make an admin token.
6. **Make it here**: the button, for an admin token (`POST /api/v1/admin/tokens`). Without one the section says how to make one and opens the sign-in dialog, which then suggests the admin command (`sensapp generate-token me --scope read,admin`: `read` too, so that the same token browses the data and the explorer does not ask for another) instead of a read token.

The token is then **shown once** at the top of the page, with a Copy button, what it was made with and the `export SENSAPP_TOKEN=…` line that [Load Data](#load-data) reads. SensApp keeps nothing: the page has no list of tokens and cannot revoke one (rotating the secret does, see [JWT_AUTH.md](JWT_AUTH.md#rotating-the-secret-and-revoking-tokens)). The page does not keep the token either: it is dropped from the cache of the requests when the page is left, and a refresh forgets it.

With no token, a server that answers anyway has authentication disabled: the page says so and shows nothing else. The catalog (for the sensors to click in) is only read when the token in use can read: an `admin` token alone cannot, and the refusal would ask for a token again.

The page is in `src/pages/CredentialsPage.tsx` (the command and the checks of a name in `src/lib/credentials.ts`) and loaded when its tab is first opened (it shows code, with the highlighter).

## Address

The explorer is in the address, so a link shares a view and a reload keeps it: `/ui/?metric=cpu&series=<uuid>&series=<uuid>&range=24h&step=5m&agg=max&style=area&log=1`.

| Parameter | Meaning | Left out when |
| --- | --- | --- |
| `metric` | the selected metric | none |
| `series` | a selected series, repeated | none |
| `range` | a preset (`15m`, `1h`, `6h`, `24h`, `7d`, `30d`, `1y`), ending now | `1h` |
| `from`, `to` | an absolute range (ISO 8601), once dates were typed | a preset is used |
| `step`, `agg` | the step (`raw` or a duration) and the aggregation | `auto`, `avg` |
| `style`, `log` | the chart style, `log=1` for the log scale | `line`, linear |

The address follows the explorer without adding history entries. What is wrong in an address is ignored; a series that no longer exists is dropped. A series is named by its uuid only: the UI asks the server for the first sample of each one (its name, labels and type), and on a server with authentication a link asks for a token like any other page.

A preset range ends now: it moves on every minute while the tab is on screen, and when the tab comes back. Dates that were typed do not move.

The metrics have a radio button, as the series have a checkbox, and when the page opens on a metric the list starts there. The filters of both lists are in the row of their title, with the count (and the pager of the series).

The list of series has a column for each label dimension, sorted by the first one (numbers inside the values as numbers: `node-2` before `node-10`; a click on a header sorts by it, again reverses it; the server pages by creation order so the sort is of the page on screen), and the uuid in small. A label that every series has the same is said once above the list instead of a column: the InfluxDB endpoint adds `influxdb_org` and `influxdb_bucket` on purpose, they are part of the identity of a series. The selector box suggests a selector made of labels of the first series. The checkbox of a selected series has the color of its line, and the one of the header selects what is on screen. The Type column is there only when the series of a name are not all of one type: the server groups the metrics by name and type, so a name that exists as a float and as a string is two rows of the metrics list and one list of series. Choosing a metric with at most 8 series (the size of the palette) selects them all, the ones that can be drawn (numbers and booleans); with more, the choice is the user's.

The list of series is paged by the server (256 per page, cursor based): Previous and Next appear when there is more than one page, and the selection is kept from page to page. The theme follows the OS (light or dark), the chart included.

## Authentication

SensApp is not open by default (see [JWT_AUTH.md](JWT_AUTH.md)). When authentication is on, the API answers `401` and the UI shows a dialog asking for a token: paste the output of `sensapp generate-token <name> --scope read`. With authentication disabled (`SENSAPP_AUTH_DISABLED`) the UI works without further ado.

- **A local SensApp without a secret prints a link** (`http://127.0.0.1:3000/ui/#token=…`) that opens the UI signed in. The token is in the *fragment* of the address, which the browser never sends to the server, so it is not logged. The UI takes it out of the address as soon as the page loads (`src/lib/tokenFromAddress.ts`), so that it stays out of the history and out of a copied link.
- The token is kept in `sessionStorage`: it is gone when the tab closes and never shared with other tabs.
- An expired or invalid token (`401`), or a token without the `read` scope (`403`), shows the dialog again with the answer of the server. So does a refused request to make a token.
- "Sign out" in the header forgets the token.
- A token with a sensor allow list shows only its sensors, as with any client.

SensApp has no login endpoint: it does not know users, only signed tokens.

## Development

```bash
cd frontend
npm ci
npm run dev        # http://localhost:5173/ui/, proxies the API to http://localhost:3000
npm run lint && npm run typecheck && npm test && npm run build
```

Run SensApp on port 3000 next to it (`cargo run`: it prints a token and a link on a local run, or start it with `SENSAPP_AUTH_DISABLED=true` to work without one).

`src/client` is generated from `openapi.json`, and both are committed. `openapi.json` is the OpenAPI document of the server: a Rust test fails when it is out of date. After an API change:

```bash
UPDATE_OPENAPI=1 cargo test frontend_openapi_document   # rewrites frontend/openapi.json
cd frontend && npm run openapi-ts                       # regenerates src/client
```

`VITE_SENSAPP_LIVE_URL=http://localhost:3000 npm test` also runs the test that talks to a running SensApp (without authentication: start it with `SENSAPP_AUTH_DISABLED=true`).
