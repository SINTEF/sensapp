# Publish Helm chart as an OCI package

Publish the already packaged SensApp Helm chart to GHCR when a GitHub release is published. Document the OCI install path and chart versioning. Keep the existing CI artifact and local chart install workflow.

Completed: the release workflow logs Helm into GHCR and pushes the packaged chart to `oci://ghcr.io/sintef/charts`. The chart README documents versioned installs and initial package visibility. Helm lint/package and workflow YAML parsing passed. No release was published from this workspace.
