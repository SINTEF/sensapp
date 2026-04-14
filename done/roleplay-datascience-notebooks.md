# Roleplay Data Science Notebooks

## Goal

Create a beginner-friendly, notebook-based workflow that uploads the new parquet files to a local SensApp instance and then queries the uploaded data back for plotting and lightweight analysis.

## Scope

- run SensApp locally with DuckDB on a non-default port
- use `uv` for the Python environment in `python/roleplay-datascience`
- use the in-repo `sensapp-sdk` as an editable dependency
- create one notebook for upload and one notebook for analysis
- validate the workflow end to end and note issues or suggestions

## Notes

- The parquet files are large enough that upload chunking is likely required under the default HTTP body limit.
- Sensor names should avoid hyphens because the current simple query parser treats them as subtraction.
- Completed on 2026-03-17 with a validated DuckDB-backed workflow on port `3017`.
