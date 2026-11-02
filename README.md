# Aircan

Aircan is a collection of Apache Airflow DAGs for building and operating data pipelines. It provides reusable components for common data engineering tasks — including cloud storage integration, data warehouse loading, schema management, validation, and notifications — and can be extended to support any data pipeline use case.

## Repository Structure

```
aircan/
├── aircan/
│   ├── dags/
│   │   ├── pipeline_ckan_to_bigquery.py      # Main DAG: CKAN CSV → GCS → BigQuery
│   │   ├── pipeline_ckan_to_bigquery_legacy.py  # Main DAG: CKAN CSV → GCS → BigQuery (legacy version)
│   │   ├── pipeline_ckan_to_postgres_legacy.py  # Main DAG: CKAN CSV → GCS → Postgres Datstore (legacy version)
│   │   └── other_dag.py              # Other DAGs can be added here
│   └── dependencies_legacy/  # Legacy dependencies for older DAG versions (to be deprecated)
│   └── dependencies/
│       ├── cloud/
│       │   ├── clients.py            # BigQuery / GCS Airflow hook wrappers
│       │   ├── storage.py            # GCS upload / download / signed URL helpers
│       │   └── warehouse.py          # BigQuery load, upsert, schema helpers
│       └── utils/
│           ├── ckan.py               # CKAN status update (async, non-blocking)
│           ├── email.py              # SMTP alert email helpers
│           ├── schema.py             # Frictionless ↔ BigQuery schema conversion
│           └── validation.py         # CSV validation via frictionless
├── docker.compose.yaml               # Local development Airflow cluster
└── config/                           # Airflow config overrides (airflow.cfg)
```

## Triggers

Aircan DAGs can be triggered in several ways depending on your use case:

- **CKAN integration** — Install the [ckanext-aircan](https://github.com/datopian/ckanext-aircan/tree/feat/airflow-v3-support) extension, an alternative to DataPusher and XLoader, which automatically triggers the DAG whenever a new CSV resource is added or updated in CKAN.
- **Scheduled runs** — Configure a cron-based schedule directly on the DAG to ingest data at regular intervals (e.g. nightly, hourly).
- **Manual triggers** — Run a DAG on demand via the Airflow UI, passing any required parameters at runtime.
- **REST API** — Trigger DAGs programmatically using the [Airflow REST API](https://airflow.apache.org/docs/apache-airflow/stable/stable-rest-api-ref.html), suitable for integration with external systems or CI/CD pipelines.

## Quick Start (Docker Compose)

### 1. Create a `.env` file

Copy the template below and fill in your values. The Docker Compose file reads this file automatically.

```dotenv
# ── Airflow infrastructure ──────────────────────────────────────────────────
AIRFLOW_IMAGE_NAME=apache/airflow:3.1.7   # Docker image to use
AIRFLOW_UID=50000                          # UID inside containers (use $(id -u) on Linux)
AIRFLOW_PROJ_DIR=.                         # Host directory mounted into containers

# ── Admin credentials ───────────────────────────────────────────────────────
_AIRFLOW_WWW_USER_USERNAME=airflow
_AIRFLOW_WWW_USER_PASSWORD=airflow

# ── Optional: custom env file path ──────────────────────────────────────────
# ENV_FILE_PATH=.env                       # Default: .env in project root
```

> **Note:** `.env` is git-ignored. Never commit credentials.

### 3. Start the containers

```bash
docker compose -f docker.compose.yaml up -d
```

Airflow UI will be available at http://localhost:8080 (default credentials: `airflow` / `airflow`).

### 4. Register Airflow Connections

Each pipeline run is namespaced by a **`site_id`** (set in the DAG trigger params). Airflow connection IDs follow the pattern `{site_id}_{type}`. Register the three connections below before triggering any DAG.

---

## DAGs

| DAG ID | Description | Documentation |
|--------|-------------|---------------|
| `pipeline_ckan_to_bigquery` | Load a CSV from CKAN into BigQuery via GCS | [pipeline_ckan_to_bigquery.md](docs/pipeline_ckan_to_bigquery.md) |
| `pipeline_ckan_to_bigquery_legacy` | Legacy version of the above DAG |   |
| `pipeline_ckan_to_postgres_legacy` | Legacy DAG for loading CKAN CSV into Postgres Datastore  |  | 