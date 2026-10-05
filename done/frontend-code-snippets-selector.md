# Frontend: code snippets, PEP 723 header and selectors

Follow-up of `done/frontend-code-snippets.md`, same branch.

- The Python snippet gets a PEP 723 block (`# /// script`) so `uv run script.py` installs the SDK from GitHub, next
  to the `uv pip install` comment.
- The selector box of the series list (`{host="a"}`) moves from the local state of `SeriesTable` to the store, so the
  dialog sees it. When it is set, the code asks the server for the series that match it (`list_series(metric=,
  selector=)`; `curl -G --data-urlencode` and `jq` in the shell) instead of listing uuids, and a checkbox of the
  dialog goes back to the selected series: the user may have unchecked some of what matches.
- A step only averages numbers. Series that match a selector are not known when the code is written, so the code
  checks the type of each (`sensor_type` is `Float` on the wire, not `float`: lower-case it).

## Steps

1. [x] Selector in the store
2. [x] Generators: PEP 723, selector mode, tests
3. [x] Dialog checkbox, live check (uv run with the block, selector snippets against a server), docs

## Results

- `uv run` with only the PEP 723 block, from a directory outside the project, installed the SDK from GitHub `main`
  and ran the script. ruff (88 columns) objected to the one-line `[tool.uv.sources]` table: it is the multi-line
  `[tool.uv.sources.sensapp]` form now.
- Selector snippets run against a live SensApp: Python and curl return the same series as the chart for
  `{host="a"}`; with a step on a selector that matches 4 series of 3 types (float, boolean, string) the floats come
  back aggregated (12 points of 30 minutes) and the others raw (349), as the guard intends; the curl loop leaves the
  non-numeric ones out.
- Found by reading the output: `\$uuid` (the escape of the page's text also escaped the variable) and a loop body
  indented one level too far. Fixed, and tested.
- A hostile selector, metric and address (quotes, `$(…)`, backticks, newlines) were run in a real shell with `curl`
  and `jq` stubbed: no command ran, and the loop got the literal address.
- 210 tests, lint, typecheck and build clean. Checkbox looked at in the browser.

## Not done

- curl and Python differ for a step on a selector: Python reads the non-numeric series raw, the shell loop leaves
  them out (a per-series choice in `jq` would read worse than the rule it replaces).
- Pages: a selector that matches more than 256 series reads the first page only.
