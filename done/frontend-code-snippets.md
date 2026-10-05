# Frontend: code snippets modal

Branch: `frontend-good-enough`.

## Goal

A button in the header (next to API Docs) opens a modal with a snippet that loads what the explorer shows:
the Python SDK (`sensapp`) in one tab, curl in the other. Simple, but well done.

## Decisions

- The snippet is generated from the state of the explorer: the selected series (uuid, with the name and labels in
  a comment), the time window, the step, the aggregation. No series selected: the metric's series, or the metrics.
- A preset window (live) is written relative to now in Python (`datetime.now(UTC) - timedelta(...)`); in curl the
  dates are absolute. A window of dates is absolute in both.
- The server is the origin of the page. Authentication: when the server asked for a token (or one is in use) the
  snippet reads it from `SENSAPP_TOKEN`. The real token is never written in a snippet.
- Dark code block in both themes. JetBrains Mono (`@fontsource-variable`, latin, same as Roboto and Sora),
  highlight.js core with Python and Bash only, loaded with the modal (not in the main bundle). A copy button.

## Steps

1. [x] Snippet generators (`src/lib/snippets.ts`) with tests
2. [x] Modal, highlighting, font, copy button
3. [x] Header button, tests, live check in a browser, docs (`docs/FRONTEND.md`)

## Results

- `src/lib/snippets.ts` (generators, 14 tests), `CodeDialog.tsx` and `CodeButton.tsx` (9 tests), `lib/copyText.ts`.
  Full suite: 198 tests, lint, typecheck and build clean.
- Checked against a live SensApp (SQLite, seeded through the InfluxDB endpoint): the Python snippet and the curl
  snippet were copied out of the dialog and run, they return the data of the chart (72 averaged points of 5 minutes
  for 6 hours, and the same in CSV). With JWT enabled: the 401 makes the snippet use `SENSAPP_TOKEN`, and the same
  snippet runs with a token in the environment and fails with the server's 401 without one.
- Every state (preset, dates, mixed types, auth, metric only, nothing, hostile labels) was generated and parsed by
  Python's `ast` and `bash -n`; the ones with real uuids pass `ruff check` and `ruff format --check` of the SDK.
- Looked at in a browser, dark and light, desktop and a 375 px phone.
- The dialog chunk is 33.9 kB (13.6 kB gzipped), loaded on first use, and the font file (40 kB) is fetched only then.

## Not done

- The copy button was exercised by tests (the clipboard API and the `execCommand` fallback), not by a click in a
  real browser: the pane does not grant the clipboard.
- Not checked in Safari or Firefox.
