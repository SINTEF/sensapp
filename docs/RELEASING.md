# Releasing SensApp

A release is a GitHub Release published for a `vX.Y.Z` tag on `main`. Publishing it is the trigger: a tag push alone
only runs CI. What CI does with it is in [CI.md](CI.md).

## Before

- `main` is green on GitHub (backend matrix, frontend, Python SDK, audit, Helm, Docker smoke, live Prometheus).
- `cargo audit` and `npm audit` findings reviewed ([PREPRODUCTION_RELEASE_PLAN.md](PREPRODUCTION_RELEASE_PLAN.md)).
- Nothing in `current_tasks/` that should ship is still open.

## Version

All of these move together, on a branch merged into `main`:

| Where | What |
| --- | --- |
| `Cargo.toml` and `Cargo.lock` | `version`, the crate |
| `charts/sensapp/Chart.yaml` | `appVersion` (the same as the crate) and `version` (the chart's own, bumped on every release: the registry refuses to overwrite a chart version) |
| `frontend/openapi.json` | the `info.version` of the document, rewritten by `UPDATE_OPENAPI=1 cargo test frontend_openapi_document` (a test fails when it is out of date) |
| `CHANGELOG.md` | a section for the version, breaking changes first |

CI refuses a release when the tag, the crate version and `appVersion` disagree. The Python SDK
(`python/sensapp/pyproject.toml`) has its own version and is not published by the workflow.

## Publish

```bash
git switch main && git pull
git tag -a vX.Y.Z -m "SensApp X.Y.Z"
git push origin vX.Y.Z
gh release create vX.Y.Z --verify-tag --title "SensApp X.Y.Z" --notes-file <the changelog section>
```

The release job then pushes the image to `ghcr.io/sintef/sensapp` (`X.Y.Z`, `X.Y`, `sha-...`; `linux/amd64` and
`linux/arm64`) and the chart to `oci://ghcr.io/sintef/charts`. The crate is not published to crates.io.

## After

- Pull the image by its version, and `helm install` the chart from the OCI registry.
- Run the publish, query and `/ui/` smoke test against those artifacts.
- First time only for a package: check its visibility in GitHub (a new GHCR package starts private).
- Move the finished task files to `done/` and tick `TODO.md`.
