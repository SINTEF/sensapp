# Junior Developer Findings: SensApp Python SDK Demo

> **Who wrote this:** Me, a junior dev who just joined the project and tried to make something simple with SensApp.
> **Date:** 2026-03-16
> **Demo code:** `python/demo-dashboard/`

---

## What I built

A tiny Streamlit dashboard that:
1. Connects to a local SensApp instance
2. Pushes a random temperature reading (18–28 °C) to the `demo_temperature` sensor every second
3. Queries the last 5 minutes of data back and renders it as a live line chart

Stack: `uv` for project management, `streamlit` for the GUI, `sensapp-sdk` for talking to the API.

---

## How to run it

```bash
# Make sure SensApp is running on http://127.0.0.1:3000 with a database behind it

cd python/demo-dashboard
uv run streamlit run app.py --server.port 8502
# open http://localhost:8502 in your browser
# press ▶ Start and watch the chart fill up
```

No manual `pip install`, no `venv activate`, no pain. `uv` is great.

---

## What worked well 👍

### The SDK is really clean

`publish_samples` + `SamplePoint` is all you need to push data.
`query_rows` returns plain Python dicts — perfect for handing straight to `pandas`.
The `with SensAppClient(...) as client:` pattern is nice and explicit.

### The Arrow-first design is smart

Even though I never touched Arrow directly, the SDK wraps it so publishing just feels like "here is a list of `(timestamp, value)` pairs". Under the hood it sends efficient binary Arrow frames — you get the performance for free.

### Streamlit + uv = zero-friction prototype

`uv add streamlit` and `uv add --editable ../sensapp-sdk` and you are done.
Streamlit's `st.line_chart` needed zero configuration to render the data.

---

## Problems I hit 🐛

### 1. Sensor names with hyphens break the query

I first named my sensor `demo-temperature`. Pushing worked (mostly), but querying it back with `demo-temperature[5m]` returned a 400 error:

```
Binary operations (like +, -, *, /) are not supported.
Only simple selectors like 'metric_name{label="value"}' or 'metric_name[5m]' are supported.
```

The query parser treats the hyphen as a subtraction operator. I had to rename the sensor to `demo_temperature`.

**Suggestion:** Either document this restriction clearly in the SDK (a docstring on `publish_samples` / `query_rows` explaining allowed name characters would help), or validate / reject names with hyphens at publish time, or make the parser handle quoted metric names. As a newbie I had no idea that sensor names were also query tokens.

### 2. Push returns status 500 when the database is not connected

When PostgreSQL was down I got a generic `500 Internal Server Error` with no body explaining what was wrong. The `/health/ready` endpoint *does* return a useful error message, but `publish` just returns 500.

**Suggestion:** When the storage backend is unavailable, `/publish` should return a more descriptive error (e.g. 503 with `{"error": "database unavailable"}`) so client code can tell the difference between a bug and a configuration issue. Right now the SDK raises `SensAppHTTPError` with just the status code, which is not very helpful for debugging.

### 3. No "readiness" check helper in the SDK

I had to manually call `client.health_ready()` and inspect the dict myself to figure out if the database was up. It would be handy to have a `client.is_ready() -> bool` convenience method, or for `SensAppClient.__init__` to optionally raise if the server is not ready.

Not a blocker at all, just a nice-to-have for quick scripts.

### 4. Streamlit session state resets on page reload

When you hard-refresh the browser, `st.session_state` is wiped. The push counter goes back to 0 and the app shows "pushed 0 samples" even though data is already in the database. This is normal Streamlit behaviour, not a SensApp problem, but it confused me for a second.

**Not a SensApp issue**, just worth knowing if you are building something production-ish.

### 5. The `time.sleep(1) + st.rerun()` polling loop is a hack

Streamlit does not natively support background threads or websocket push, so the "stream every second" is implemented as:

```python
if st.session_state.running:
    time.sleep(1)
    st.rerun()
```

This blocks the Streamlit server thread for 1 second on every cycle, which means the UI is unresponsive during that sleep. For a one-user demo it is fine. For anything real you would want a background thread or a proper async approach (or use a different tool like Dash/Panel).

---

## What I would do differently next time

- Use `client.query_arrow(...)` directly and pass the resulting `pyarrow.Table` to `pandas` — skipping `query_rows` avoids the list-of-dicts roundtrip.
- Move `SENSAPP_URL` to an environment variable or a `st.text_input` so it is easier to point at a remote server.
- Use `st.empty()` placeholders to update only the chart widget instead of re-rendering the whole page.

---

## Summary

The SDK is solid for a v0.1. Publish and query both work exactly as advertised. The main rough edge for a first-time user is the invisible restriction on sensor names — a hyphened name silently pushes (or 500s) but then fails to query, which wastes a lot of debugging time. A small validation or a better error message would make the onboarding experience much smoother.

Overall: it took me about 30 minutes to go from zero to a live streaming chart. That is a good sign.
