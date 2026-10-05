# SensApp Helm chart

The chart deploys SensApp only. It does not install or manage a database. Each SensApp release connects to one storage backend through `SENSAPP_STORAGE_CONNECTION_STRING`.

## Install from a release

Published releases push the chart to GHCR as an OCI package. Install a specific chart version with:

```bash
helm install sensapp oci://ghcr.io/sintef/charts/sensapp --version 0.1.1
```

The chart version is the `version` in `Chart.yaml`, which is separate from the SensApp `appVersion`. Bump the chart version for each release so an existing OCI tag is not reused. GHCR creates new packages as private by default; an organization admin must make the chart package public for anonymous installs after its first publication.

## SQLite default

```bash
helm install sensapp ./charts/sensapp
```

By default, SensApp uses SQLite at `/var/lib/sensapp/sensapp.db` on an `emptyDir` volume. Data is lost when the pod is replaced. For a durable single replica SQLite deployment, set `persistence.enabled=true` to create a PVC. Do not scale replicas against the same SQLite database.

## External database

Run PostgreSQL, TimescaleDB, or ClickHouse separately, using your preferred operator, managed service, or other deployment. Supply a connection string in a Kubernetes Secret in the same namespace as SensApp:

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: sensapp-storage
type: Opaque
stringData:
  SENSAPP_STORAGE_CONNECTION_STRING: postgres://user:password@postgres.example.com:5432/sensapp
```

Then install SensApp with:

```bash
helm install sensapp ./charts/sensapp --set storage.existingSecret=sensapp-storage
```

The chart reads the `SENSAPP_STORAGE_CONNECTION_STRING` key from that Secret. Set `storage.existingSecretKey` if your Secret uses a different key. When `storage.existingSecret` is set, it takes precedence over `storage.connectionString`; the chart does not copy the database URL into its own Secret. With the default `persistence.enabled=false`, no local data volume is mounted.

Supported URL forms in the default container image include:

| Backend | Connection string example |
| --- | --- |
| PostgreSQL | `postgres://user:password@postgres.example.com:5432/sensapp` |
| TimescaleDB | `timescaledb://user:password@timescale.example.com:5432/sensapp` |
| ClickHouse HTTP | `clickhouse://user:password@clickhouse.example.com:8123/sensapp` |
| ClickHouse HTTPS | `clickhouses://user:password@clickhouse.example.com:8443/sensapp` |

For a quick setup, you can set `storage.connectionString` directly instead of referencing a Secret. Helm stores that value in the release configuration, so use an existing Secret for credentials. External databases own their storage and lifecycle; leave `persistence.enabled=false` for these backends.

## Authentication

SensApp does not run open: the container image listens on every address, so it refuses to start without a secret. By default the chart **makes a random secret on the first install** and keeps it across upgrades (it reads it back from the Secret of the release). Make an admin token, which makes the other tokens in the UI (Credentials tab):

```bash
kubectl exec deploy/sensapp -- sensapp generate-token me --scope read,admin
```

| Value | Effect |
| --- | --- |
| `auth.jwtSecret` | A secret of your choice (at least 32 characters). Stored in the Secret of the release and in the release values: prefer `existingSecret`. |
| `auth.existingSecret`, `auth.existingSecretKey` | A Secret of yours holding `SENSAPP_JWT_SECRET`. Takes precedence over `jwtSecret`. |
| `auth.previousSecrets` | Comma-separated secrets that still verify tokens but never sign: the way to rotate the secret, see [JWT_AUTH.md](../../docs/JWT_AUTH.md#rotating-the-secret-and-revoking-tokens). |
| `auth.disabled` | Every endpoint is open (`SENSAPP_AUTH_DISABLED`), for a network that authenticates in front of SensApp. Ignored when a secret is given. |

The made secret needs the chart to read the cluster (`lookup`). Tools that only render the chart (`helm template`, Argo CD) cannot, and would make a new secret at each render: for those, set `auth.jwtSecret` or `auth.existingSecret`.
