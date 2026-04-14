# Containerization and Helm Packaging

## Goal

Add a production-oriented Docker image, a reusable Helm chart, and CI validation for both.

## Scope

- Create a multi-stage Dockerfile for the SensApp server
- Add a Helm chart for Kubernetes deployment
- Extend GitHub Actions to validate and build container artifacts
- Keep the published image focused on practical deployment backends

## Notes

- Primary image target: self-hosted deployments on Kubernetes and Docker
- Compile-time feature matrix is broader than the primary runtime image
- Helm should configure runtime behavior, not Rust compile-time features

## Status

Completed.
