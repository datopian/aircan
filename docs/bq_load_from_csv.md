# DAG: `bq_load_from_csv`

Loads a CSV resource from a CKAN instance into a Google BigQuery table via Google Cloud Storage (GCS). Supports three ingestion modes: **overwrite**, **append**, and **upsert**.

## Prerequisites

To use the CKAN integration features (status updates and authenticated resource downloads), the target CKAN instance must have the [ckanext-aircan](https://github.com/datopian/ckanext-aircan) extension installed and enabled. This extension exposes the `/api/3/action/aircan_submit` and `/api/3/action/aircan_status_update` endpoints that the DAG uses to trigger and report pipeline progress and failures back to CKAN.

## Table of Contents

- [Pipeline Overview](#pipeline-overview)
- [Task Flow](#task-flow)
- [Airflow Connections](#airflow-connections)
- [Trigger Parameters](#trigger-parameters)
  - [resource](#resource)
  - [ckan\_config](#ckan_config)
  - [gcs\_config](#gcs_config)
  - [other\_config](#other_config)
- [Ingestion Methods](#ingestion-methods)
- [Schema Handling](#schema-handling)
- [Notifications](#notifications)
- [Full Trigger Payload Example](#full-trigger-payload-example)

---

## Pipeline Overview

```
CKAN resource URL
      │
      ▼
[upload_to_gcs_task]     ← streams CSV to GCS bucket
      │
      ▼
[infer_schema_task]      ← uses provided schema or infers from file
      │
      ▼
[validate_csv_task]      ← validates rows against schema (frictionless)
      │
      ▼
[branch_write_method]    ← routes to overwrite/append OR upsert
      │
   ┌──┴──────────────────────┐
   ▼                         ▼
[overwrite_or_append]    [upsert_table_task]
   └──────────┬──────────────┘
              ▼
        [cleanup_gcs]        ← deletes the temporary GCS object
```

On **success** the DAG calls CKAN's `aircan_status_update` API with `state=success`.
On **failure** (task or DAG level) it calls CKAN with `state=failed` and sends an alert email.

---

## Task Flow

| Task ID | Description |
|---------|-------------|
| `collect_config_task` | Reads DAG trigger params from context; notifies CKAN that the pipeline is starting. |
| `upload_to_gcs_task` | Streams the CSV from the CKAN resource URL directly into GCS. Detects gzip compression automatically. |
| `infer_schema_task` | Uses the schema provided in params, or infers it by reading the uploaded GCS file via a signed URL. Column names are sanitized to BigQuery-safe identifiers. |
| `validate_csv_task` | Validates every row of the CSV against the schema using the frictionless library. Fails the task with a detailed error report if validation errors are found. |
| `branch_write_method` | Branches to `overwrite_or_append_table_task` or `upsert_table_task` based on `ingestion_method`. |
| `overwrite_or_append_table_task` | Loads the GCS file directly into BigQuery with `WRITE_TRUNCATE` (overwrite) or `WRITE_APPEND` disposition. |
| `upsert_table_task` | Loads the GCS file into a temporary staging table, then runs a BigQuery `MERGE` statement to upsert into the target table using the specified unique keys. The staging table is dropped afterwards. |
| `cleanup_gcs` | Deletes the temporary GCS object uploaded in `upload_to_gcs_task`. Runs after either write branch succeeds. |

---

## Airflow Connections

All connection IDs are namespaced by the `site_id` value from the trigger params (e.g. if `site_id = "my_ckan"` then register connections named `my_ckan_google_cloud`, `my_ckan_api_key`, `my_ckan_email`).

### `{site_id}_google_cloud`

**Type:** `google_cloud_platform`

Used by both `upload_to_gcs_task` and BigQuery write tasks to authenticate against GCP.

| Field | Value |
|-------|-------|
| Conn Type | `Google Cloud` |
| Project | Your GCP project ID |
| Keyfile JSON / Keyfile Path | Service account credentials with roles: `roles/storage.objectAdmin` (GCS bucket) and `roles/bigquery.dataEditor` + `roles/bigquery.jobUser` (BigQuery) |

**Register via UI:** Admin → Connections → Add
**Register via CLI:**
```bash
airflow connections add {site_id}_google_cloud \
  --conn-type google_cloud_platform \
  --conn-extra '{"project": "your-gcp-project", "key_path": "/path/to/sa.json"}'
```

---

### `{site_id}_api_key`

**Type:** `generic` (only the `password` field is used)

Holds the CKAN API key used to authenticate the CSV download request and CKAN status update calls.

| Field | Value |
|-------|-------|
| Conn Type | `HTTP` |
| Password | Your CKAN API key (e.g. `xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx`) |

**Register via CLI:**
```bash
airflow connections add {site_id}_api_key \
  --conn-type generic \
  --conn-password "your-ckan-api-key"
```

---

### `{site_id}_email`

**Type:** `smtp`

Used to send failure alert emails. The connection stores all SMTP server details. Email sending is non-fatal — a failure here will not fail the pipeline task.

| Field | Value |
|-------|-------|
| Conn Type | `SMTP` |
| Host | Your SMTP server hostname (e.g. `smtp.gmail.com`) |
| Port | SMTP port (e.g. `587` for STARTTLS) |
| Login | SMTP username / email address |
| Password | SMTP password or app-specific password |

**Register via CLI:**
```bash
airflow connections add {site_id}_email \
  --conn-type smtp \
  --conn-host smtp.gmail.com \
  --conn-port 587 \
  --conn-login sender@example.com \
  --conn-password "your-smtp-password"
```

---

## Trigger Parameters

The DAG is triggered manually (`schedule=None`) with a JSON params payload. All fields are nested under the four top-level keys below.

> **Note:** The top-level key for non-GCS options is `other_config` (not `others_config`). The `DEFAULT_PARAMS` in the source file uses `others_config` as a placeholder only — use `other_config` in actual trigger payloads.

---

### `resource`

Configuration for the source CKAN resource.

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `id` | string | Yes | — | CKAN resource UUID. Used as the BigQuery table name and in CKAN status update calls. |
| `url` | string | Yes | — | Full HTTP/HTTPS URL to download the CSV file. Can be a CKAN download URL or any publicly accessible URL. Gzip-compressed files (`.csv.gz` / `.gz`) are detected automatically. |
| `schemas` | object or null | No | `{}` (auto-infer) | A [frictionless Table Schema](https://specs.frictionlessdata.io/table-schema/) descriptor as a JSON object. When provided, schema inference is skipped and this descriptor is used directly. When empty or omitted, schema is inferred from the uploaded file. |
| `ingestion_method` | string | Yes | `"overwrite"` | One of `overwrite`, `append`, or `upsert`. See [Ingestion Methods](#ingestion-methods). |

---

### `ckan_config`

CKAN instance settings.

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `site_url` | string | Yes | — | Base URL of the CKAN instance (e.g. `https://ckan.example.com`). Used to send status update API calls to `/api/3/action/aircan_status_update`. |
| `site_id` | string | Yes | — | Short identifier for this CKAN site. **Determines the Airflow connection name prefix.** E.g. `"my_ckan"` means the pipeline looks for connections `my_ckan_google_cloud`, `my_ckan_api_key`, and `my_ckan_email`. |

---

### `gcs_config`

Google Cloud Storage and project settings.

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `project_id` | string | Yes | — | GCP project ID where BigQuery datasets and GCS buckets reside. |
| `dataset_id` | string | Yes | — | BigQuery dataset ID to load data into. Created automatically if it does not exist. |
| `bucket` | string | Yes | — | GCS bucket name used as temporary staging for the CSV upload. The object is deleted after the pipeline completes. |
| `chunk_size` | integer | No | `8388608` (8 MB) | Chunk size in bytes for streaming the CSV from CKAN to GCS. Increase for very large files on fast networks. |
| `signed_url_expiration_seconds` | integer | No | `900` (15 min) | How long (in seconds) the GCS signed URL used for schema inference and CSV validation remains valid. Increase if validation of large files takes longer than 15 minutes. |
| `upload_ready_timeout_seconds` | integer | No | `120` | Timeout in seconds for the GCS upload operation. |

---

### `other_config`

Miscellaneous pipeline options.

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `skip_leading_rows` | integer | No | `1` | Number of header rows to skip when loading into BigQuery. Set to `1` for standard CSV files with a header row. |
| `temp_table_prefix` | string | No | `"_temp_"` | Prefix for the temporary staging table created during upsert operations. E.g. with prefix `_temp_` and resource ID `abc123`, the staging table is `_temp_abc123`. |
| `notification_to_email` | list[string] | No | `[]` | List of email addresses to notify on pipeline failure. Requires the `{site_id}_email` Airflow connection to be configured. |
| `notification_from_email` | string | No | `""` | Sender email address shown in failure alert emails. |

---

## Ingestion Methods

Set via `resource.ingestion_method`.

### `overwrite`

Truncates the target BigQuery table and replaces all data with the new CSV contents (`WRITE_TRUNCATE`). Safe to re-run; previous data is always replaced.

### `append`

Appends new rows from the CSV to the existing BigQuery table (`WRITE_APPEND`). New columns present in the schema but absent from the existing table are added automatically.

### `upsert`

Performs an insert-or-update (MERGE) operation using one or more unique key columns:

1. Loads the CSV into a temporary staging table.
2. Runs a BigQuery `MERGE` statement: rows matching on unique keys are updated; non-matching rows are inserted.
3. Drops the staging table.

Requires unique key columns to be declared in the schema descriptor with `"constraints": {"unique": true}`.

**Schema example with unique keys:**
```json
{
  "fields": [
    {
      "name": "id",
      "type": "integer",
      "constraints": { "unique": true }
    },
    {
      "name": "name",
      "type": "string"
    }
  ]
}
```

---

## Schema Handling

### Provided schema (`resource.schemas`)

Pass a frictionless Table Schema descriptor object. Column names are sanitized to BigQuery-compliant identifiers (alphanumeric and underscores only, max 128 chars, must start with a letter or underscore).

### Auto-inferred schema

When `resource.schemas` is empty or omitted, the DAG:
1. Generates a GCS signed URL for the uploaded file.
2. Uses the frictionless `Resource.infer()` method to detect field names and types.
3. Sanitizes the inferred field names.

### Frictionless → BigQuery type mapping

| Frictionless type | BigQuery type |
|-------------------|---------------|
| `string`, `any` | `STRING` |
| `number` | `NUMERIC` |
| `integer`, `year` | `INT64` |
| `boolean` | `BOOL` |
| `object`, `array`, `list` | `JSON` |
| `datetime` | `DATETIME` |
| `date` | `DATE` |
| `time` | `TIME` |
| `yearmonth`, `duration` | `STRING` |
| `geopoint`, `geojson` | `GEOGRAPHY` |
| *(unknown)* | `STRING` |

---

## Notifications

### CKAN status updates

> **Requires:** [ckanext-aircan](https://github.com/datopian/ckanext-aircan) installed on the CKAN instance.

At each significant pipeline step the DAG posts to the CKAN endpoint:
```
POST {ckan_config.site_url}/api/3/action/aircan_status_update
```
with payload `{ resource_id, state, type, message }`. Status updates are fire-and-forget: a failed update is logged as a warning and does not interrupt the pipeline.

### Failure email alerts

On any task failure an HTML alert email is sent via the `{site_id}_email` SMTP connection. The email includes the resource ID, timestamp, and error details. Email sending is non-fatal.

---

## Full Trigger Payload Example

```json
{
  "resource": {
    "id": "c8efed04-7100-4d85-9615-c6f12a7abe18",
    "url": "https://ckan.example.com/dataset/my-dataset/resource/c8efed04/download/data.csv",
    "schemas": {},
    "ingestion_method": "upsert"
  },
  "ckan_config": {
    "site_url": "https://ckan.example.com",
    "site_id": "my_ckan"
  },
  "gcs_config": {
    "project_id": "my-gcp-project",
    "dataset_id": "ckan_data",
    "bucket": "my-aircan-staging-bucket",
    "chunk_size": 8388608,
    "signed_url_expiration_seconds": 900,
    "upload_ready_timeout_seconds": 120
  },
  "other_config": {
    "skip_leading_rows": 1,
    "temp_table_prefix": "_temp_",
    "notification_to_email": ["ops@example.com"],
    "notification_from_email": "aircan-alerts@example.com"
  }
}
```

For **overwrite** or **append** with an explicit schema:

```json
{
  "resource": {
    "id": "abc123",
    "url": "https://ckan.example.com/resource/abc123/download/sales.csv",
    "schemas": {
      "fields": [
        { "name": "sale_id",   "type": "integer", "constraints": { "required": true } },
        { "name": "amount",    "type": "number" },
        { "name": "sale_date", "type": "date" },
        { "name": "region",    "type": "string" }
      ]
    },
    "ingestion_method": "append"
  },
  "ckan_config": {
    "site_url": "https://ckan.example.com",
    "site_id": "my_ckan"
  },
  "gcs_config": {
    "project_id": "my-gcp-project",
    "dataset_id": "ckan_data",
    "bucket": "my-aircan-staging-bucket"
  },
  "other_config": {
    "skip_leading_rows": 1,
    "notification_to_email": ["ops@example.com"],
    "notification_from_email": "aircan-alerts@example.com"
  }
}
```
