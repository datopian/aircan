# DAG: `pipeline_ckan_to_bigquery`

Loads a resource from a CKAN instance into a Google BigQuery table via Google Cloud Storage (GCS). Supports CSV, TSV, JSON array, NDJSON/JSONL, and Parquet formats. Supports three ingestion modes: **overwrite**, **append**, and **upsert**. Optionally exports the final BigQuery table to S3.

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
  - [others\_config](#others_config)
  - [s3\_config](#s3_config)
- [Ingestion Methods](#ingestion-methods)
- [Supported Formats](#supported-formats)
- [Row Number Column](#row-number-column)
- [Record Updated-At Column](#record-updated-at-column)
- [Schema Handling](#schema-handling)
- [S3 Export](#s3-export)
- [Notifications](#notifications)
- [Full Trigger Payload Example](#full-trigger-payload-example)

---

## Pipeline Overview

```
CKAN resource URL
      │
      ▼
[prepare_and_upload_task]  ← infers schema, optionally validates, injects row
      │                       numbers, and streams file to GCS (all in one pass)
      ▼
[branch_write_method]      ← routes to overwrite/append OR upsert
      │
   ┌──┴──────────────────────┐
   ▼                         ▼
[overwrite_or_append]    [upsert_table_task]
   └──────────┬──────────────┘
              ▼
   [export_and_publish_task] ← exports BQ table → GCS → S3 (skipped if no S3 bucket)
              ▼
        [cleanup_gcs]        ← deletes the temporary GCS staging object
```

On **success** the DAG calls CKAN's `aircan_status_update` API with `state=success`.
On **failure** (task or DAG level) it calls CKAN with `state=failed` and sends an alert email.

---

## Task Flow

| Task ID | Description |
|---------|-------------|
| `collect_config_task` | Reads DAG trigger params from context; notifies CKAN that the pipeline is starting. |
| `prepare_and_upload_task` | Single task that: (1) infers or loads schema, (2) optionally validates records, (3) queries `MAX(row_number_column)` for append/upsert continuity, and (4) streams the source file to GCS with row numbers injected on the fly. Supports CSV, TSV, JSON array, NDJSON/JSONL, and Parquet. |
| `branch_write_method` | Branches to `overwrite_or_append_table_task` or `upsert_table_task` based on `ingestion_method`. |
| `overwrite_or_append_table_task` | Loads the GCS file directly into BigQuery with `WRITE_TRUNCATE` (overwrite) or `WRITE_APPEND` disposition. Sets `record_updated_at_column` timestamp on all new rows. |
| `upsert_table_task` | Loads the GCS file into a temporary staging table, then runs a BigQuery `MERGE` statement to upsert into the target table using the specified unique keys. `row_number_column` is preserved on matched rows (not overwritten). Sets `record_updated_at_column` only for rows that changed. The staging table is dropped afterwards. |
| `export_and_publish_task` | Exports the final BigQuery table back to GCS (ordered by `row_number_column`), then uploads to S3. Skipped if `s3_config.bucket` is not set. |
| `cleanup_gcs` | Deletes the temporary GCS staging object uploaded in `prepare_and_upload_task`. Runs after `export_and_publish_task`. |

---

## Airflow Connections

All connection IDs are namespaced by the `site_id` value from the trigger params (e.g. if `site_id = "my_ckan"` then register connections named `my_ckan_google_cloud`, `my_ckan_api_key`, `my_ckan_email`).

### `{site_id}_google_cloud`

**Type:** `google_cloud_platform`

Used by both `prepare_and_upload_task` and BigQuery write tasks to authenticate against GCP.

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

Holds the CKAN API key used to authenticate the resource download request and CKAN status update calls.

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

### `{site_id}_s3` *(or custom `s3_config.conn_id`)*

**Type:** `aws`

Required only when `s3_config.bucket` is set. Holds AWS credentials for S3 export.

| Field | Value |
|-------|-------|
| Conn Type | `Amazon Web Services` |
| AWS Access Key ID | Your IAM access key |
| AWS Secret Access Key | Your IAM secret key |

**Register via CLI:**
```bash
airflow connections add {site_id}_s3 \
  --conn-type aws \
  --conn-login "YOUR_ACCESS_KEY_ID" \
  --conn-password "YOUR_SECRET_ACCESS_KEY"
```

---

## Trigger Parameters

The DAG is triggered manually (`schedule=None`) with a JSON params payload. All fields are nested under the five top-level keys below.

> **Note:** The key for miscellaneous options is `others_config` (with an `s`).

---

### `resource`

Configuration for the source resource.

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `id` | string | Yes | — | CKAN resource UUID. Used as the BigQuery table name and in CKAN status update calls. |
| `url` | string | Yes | — | Full HTTP/HTTPS URL to download the file. Gzip-compressed files (`.csv.gz` / `.gz`) are detected automatically. |
| `format` | string | No | `"csv"` | Explicit format override. One of `csv`, `tsv`, `json`, `ndjson`, `jsonl`, `parquet`. When omitted the pipeline defaults to `csv`. |
| `schemas` | object or null | No | `{}` (auto-infer) | A [frictionless Table Schema](https://specs.frictionlessdata.io/table-schema/) descriptor as a JSON object. When provided, schema inference is skipped. When empty or omitted, schema is inferred from the source URL. |
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
| `bucket` | string | Yes | — | GCS bucket name used as temporary staging for the upload. The object is deleted after the pipeline completes. |
| `chunk_size` | integer | No | `8388608` (8 MB) | GCS resumable upload chunk size in bytes. |

---

### `others_config`

Miscellaneous pipeline options.

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `skip_leading_rows` | integer | No | `1` | Number of header rows to skip when loading CSV into BigQuery. Set to `1` for standard CSV files with a header row. Ignored for JSON and Parquet. |
| `temp_table_prefix` | string | No | `"_temp_"` | Prefix for the temporary staging table created during upsert operations. E.g. with prefix `_temp_` and resource ID `abc123`, the staging table is `_temp_abc123`. |
| `validate_records` | boolean | No | `false` | When `true`, validates every row against the schema using the frictionless library before uploading. Validation failures abort the pipeline with a detailed error report. |
| `row_number_column` | string | No | `"_id"` | Name of the auto-incremented row number column injected into every record. See [Row Number Column](#row-number-column). |
| `record_updated_at_column` | string | No | `"_updated_at"` | Name of the timestamp column set to the job run time when a row is inserted or updated. See [Record Updated-At Column](#record-updated-at-column). |
| `notification_to_email` | list[string] | No | `[]` | List of email addresses to notify on pipeline failure. Requires the `{site_id}_email` Airflow connection to be configured. |
| `notification_from_email` | string | No | `""` | Sender email address shown in failure alert emails. |
| `http_connect_timeout` | integer | No | `10` | HTTP connection timeout in seconds for the source file download. |
| `http_read_timeout` | integer | No | `1200` | HTTP read timeout in seconds for the source file download. Increase for very large files on slow connections. |

---

### `s3_config`

Optional S3 export destination. The `export_and_publish_task` is skipped entirely when `s3_config.bucket` is not set.

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `bucket` | string | No | `""` | S3 bucket name to export the final BigQuery table into. Leave empty to skip S3 export. |
| `key_prefix` | string | No | `""` | S3 key prefix (folder path) for the exported file. |
| `conn_id` | string | No | `"{site_id}_s3"` | Airflow connection ID for S3 credentials. Defaults to `{site_id}_s3`. |
| `endpoint_url` | string | No | `""` | Custom S3-compatible endpoint URL (e.g. for MinIO). Leave empty for AWS S3. |
| `region` | string | No | `""` | AWS region for the S3 bucket. |

---

## Ingestion Methods

Set via `resource.ingestion_method`.

### `overwrite`

Truncates the target BigQuery table and replaces all data with the new file contents (`WRITE_TRUNCATE`). Safe to re-run; previous data is always replaced. `row_number_column` starts at 1.

### `append`

Appends new rows from the file to the existing BigQuery table (`WRITE_APPEND`). New columns present in the schema but absent from the existing table are added automatically. `row_number_column` continues from `MAX + 1` of the existing table.

### `upsert`

Performs an insert-or-update (MERGE) operation using one or more unique key columns:

1. Queries `MAX(row_number_column)` from the target table to determine the starting row number for new records.
2. Loads the file into a temporary staging table.
3. Runs a BigQuery `MERGE` statement: rows matching on unique keys are updated (non-key, non-preserved columns only); non-matching rows are inserted with new row numbers continuing from MAX + 1.
4. `row_number_column` is **preserved** on matched rows (not overwritten during UPDATE).
5. `record_updated_at_column` is set to the job timestamp only for rows where data actually changed (NULL-safe change detection), not for unchanged matched rows.
6. Drops the staging table.

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

## Supported Formats

Set via `resource.format`. The pipeline detects and handles each format natively during the streaming upload to GCS — no intermediate local files are created.

| Format value | BigQuery source format | Notes |
|---|---|---|
| `csv` (default) | `CSV` | Streamed in pandas chunks. Gzip compression auto-detected from URL. |
| `tsv` | `CSV` | Tab-separated; normalised to comma-separated CSV at stream time. |
| `json` | `NEWLINE_DELIMITED_JSON` | Full JSON array loaded via streaming parser (ijson); converted to NDJSON on the fly. |
| `ndjson` / `jsonl` | `NEWLINE_DELIMITED_JSON` | Streamed line by line. |
| `parquet` | `PARQUET` | Streamed in row-group batches via pyarrow. |

Exported files follow the same format convention: CSV exports become `.csv`, JSON/NDJSON/JSONL exports become `.ndjson`, Parquet exports become `.parquet`.

---

## Row Number Column

Every record loaded by the pipeline receives an auto-incremented integer column (default name: `_id`, configurable via `others_config.row_number_column`). This column is the **first column** in the BigQuery table.

| Ingestion method | Matched rows | New / all rows |
|---|---|---|
| `overwrite` | N/A (table truncated) | Starts at `1` |
| `append` | N/A (inserts only) | Continues from `MAX + 1` |
| `upsert` | `row_number_column` **preserved** (not overwritten) | Continues from `MAX + 1` |

For `append` and `upsert`, the pipeline queries `SELECT COALESCE(MAX(row_number_column), 0)` on the target table before the upload so that new rows receive unique sequential numbers even across incremental loads.

---

## Record Updated-At Column

An optional `TIMESTAMP` column (default name: `_updated_at`, configurable via `others_config.record_updated_at_column`) is automatically added to the BigQuery table to track when each row was last inserted or changed.

| Ingestion method | Behaviour |
|---|---|
| `overwrite` | All rows receive the current job timestamp. |
| `append` | Only newly inserted rows receive the current job timestamp. |
| `upsert` | Inserted rows receive the current job timestamp. Matched rows are updated **only if at least one data column changed** (NULL-safe); unchanged matched rows retain their existing timestamp. |

This column is not part of the source file schema — it is managed entirely by the pipeline.

---

## Schema Handling

### Provided schema (`resource.schemas`)

Pass a frictionless Table Schema descriptor object. Column names are sanitized to BigQuery-compliant identifiers (alphanumeric and underscores only, max 128 chars, must start with a letter or underscore).

### Auto-inferred schema

When `resource.schemas` is empty or omitted, the DAG uses the frictionless `Resource.infer()` method to detect field names and types from the source URL, then sanitizes the inferred field names.

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

## S3 Export

When `s3_config.bucket` is set, the `export_and_publish_task` runs after the BigQuery write completes:

1. Exports the target BigQuery table to GCS as sharded files, ordered by `row_number_column` ASC.
2. For CSV: composes shards into a single GCS object (with header prepended), then uploads to S3.
3. For NDJSON: composes shards into a single GCS object, then uploads to S3.
4. For Parquet: uploads each shard individually (Parquet files cannot be concatenated). Multiple shards are named `{stem}_001.parquet`, `{stem}_002.parquet`, etc.
5. Deletes all GCS export objects after a successful S3 upload.

The export filename is derived from the source resource URL (e.g. `sales.csv`). The S3 key is `{s3_config.key_prefix}/{filename}`.

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
    "format": "csv",
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
    "chunk_size": 8388608
  },
  "others_config": {
    "skip_leading_rows": 1,
    "temp_table_prefix": "_temp_",
    "validate_records": false,
    "row_number_column": "_id",
    "record_updated_at_column": "_updated_at",
    "notification_to_email": ["ops@example.com"],
    "notification_from_email": "aircan-alerts@example.com",
    "http_connect_timeout": 10,
    "http_read_timeout": 1200
  },
  "s3_config": {
    "conn_id": "my_ckan_s3",
    "bucket": "my-s3-export-bucket",
    "key_prefix": "ckan-exports/",
    "endpoint_url": "",
    "region": "us-east-1"
  }
}
```

