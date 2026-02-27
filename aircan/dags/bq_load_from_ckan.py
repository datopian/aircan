from __future__ import annotations

import json
import os
import logging
import tempfile

from datetime import timedelta, datetime, timezone
from typing import Any, Dict, List, Optional
from airflow import DAG
from airflow.sdk import task, get_current_context
from airflow.task.trigger_rule import TriggerRule
from airflow.exceptions import AirflowException
from airflow.sdk.bases.hook import BaseHook
from airflow.sdk import ObjectStoragePath

from frictionless import Resource
from google.cloud import bigquery

from aircan.dependencies.utils.schema import (
    _load_frictionless_descriptor,
    _sanitize_frictionless_descriptor,
    extract_unique_keys_from_schema,
    schema_fields_from_descriptor,
)
from aircan.dependencies.cloud.clients import bq_client, gcs_client
from aircan.dependencies.cloud.storage import (
    gcs_object_name,
    download_http_to_file,
    upload_file_to_gcs,
    _add_row_number_to_csv,
)
from aircan.dependencies.cloud.warehouse import (
    table_fqn,
    ensure_dataset_exists,
    append_or_overwrite_flow,
    upsert_flow,
)
from aircan.dependencies.utils.ckan import ckan_status_update_async
from aircan.dependencies.utils.email import send_email, build_alert_html
from aircan.dependencies.utils.validation import validate_csv


DAG_ID = "bq_load_from_csv"
logger = logging.getLogger(__name__)
DEFAULT_CHUNK_SIZE = 8 * 1024 * 1024

DEFAULT_PARAMS = {
    "resource": {
        "resource_id": "63b3d77e-032f-4ef0-8790-cc81d0509d5f",
        "url": "https://example.com/dataset/266c57d5-ea6a-4b16-89e6-2f9e072f333d/resource/c8efed04-7100-4d85-9615-c6f12a7abe18/download/date.csv",
        "schemas": {},
        "ingestion_method": "overwrite",
    },
    "ckan_config": {
        "site_url": "https://ckan.com",
        "site_id": "local_dev",
    },
    "gcs_config": {
        "project_id": "google-gcp-project",
        "dataset_id": "ckan_dataset",
        "bucket": "ckan_storage",
        "chunk_size": 8388608,
    },
    "others_config": {
        "skip_leading_rows": 1,
        "temp_table_prefix": "_temp_",
        "validate_records": False,
        "row_number_column": "_row",
        "record_updated_at_column": "record_updated_at",
        "notification_to_email": ["example@gmail.com"],
        "notification_from_email": "sender@gmail.com",
    },
}

START_DATE = datetime.now() - timedelta(days=1)

DEFAULT_ARGS = {
    "start_date": START_DATE,
    "params": DEFAULT_PARAMS,
    "on_failure_callback": lambda ctx: _notify_failure(ctx),
}


# ----------------------------
# Helpers
# ----------------------------

def _row_number_start(
    conn_id: str, project_id: str, target_fqn: str, column: str, ingestion_method: str
) -> int:
    """Return the next row number start value for append/upsert (MAX + 1), or 1 otherwise."""
    if ingestion_method not in ("append", "upsert"):
        return 1
    try:
        result = bq_client(conn_id, project_id).query(
            f"SELECT COALESCE(MAX(`{column}`), 0) AS max_rn FROM `{target_fqn}`"
        ).result()
        for row in result:
            return int(row.max_rn) + 1
    except Exception:
        logger.warning(
            "Could not query MAX(%s) from %s — starting at 1", column, target_fqn, exc_info=True
        )
    return 1


def _build_schema_fields(
    schema_descriptor: dict, row_number_column: Optional[str]
) -> List[bigquery.SchemaField]:
    """Build BQ schema fields, prepending the row number column as INT64 if set."""
    fields = schema_fields_from_descriptor(schema_descriptor)
    if row_number_column:
        fields = [bigquery.SchemaField(row_number_column, "INT64", mode="NULLABLE")] + fields
    return fields


# ----------------------------
# Airflow callbacks
# ----------------------------

def dag_success_callback(context: Dict[str, Any]) -> None:
    ckan_status_update_async(
        context.get("params"), state="success", message="Pipeline completed successfully"
    )


def _notify_failure(context: Dict[str, Any]) -> None:
    exc = context.get("exception")
    params = context.get("params") or {}

    logger.error("Task failed: %s", exc)

    if isinstance(exc, AirflowException) and exc.args:
        try:
            error_payload = json.loads(exc.args[0])
        except (ValueError, TypeError):
            error_payload = {"message": str(exc.args[0])}
    else:
        error_payload = {"message": str(exc) if exc else "DAG run failed"}

    try:
        ckan_status_update_async(params, state="failed", message=error_payload)
    except Exception:
        logger.warning("CKAN status update raised unexpectedly; continuing.", exc_info=True)

    resource_id = params.get("resource", {}).get("id", "unknown")
    site_id = params.get("ckan_config", {}).get("site_id", "")
    others = params.get("others_config", {})

    send_email(
        to=others.get("notification_to_email", []),
        subject=f"Aircan pipeline failed: {resource_id}",
        html_content=build_alert_html(resource_id, error_payload),
        from_email=others.get("notification_from_email", ""),
        conn_id=f"{site_id}_email",
    )


def dag_run_failure_callback(context: Dict[str, Any]) -> None:
    dag_run = context.get("dag_run")
    if dag_run and dag_run.get_task_instances(state="failed"):
        logger.info("DAG-level callback skipped — task-level callback already handled it.")
        return
    _notify_failure(context)


# ----------------------------
# Tasks
# ----------------------------

@task(task_id="collect_config_task")
def collect_config_task() -> Dict[str, Any]:
    context = get_current_context()
    params = context.get("params")
    ckan_status_update_async(params, state="running", message="Pipeline is running preparing the configuration")
    return dict(params)


@task(task_id="prepare_and_upload_task")
def prepare_and_upload_task() -> Dict[str, Any]:
    """
    Single-download task:
      1. Download CSV from HTTP to a local temp file
      2. Infer (or load) schema from local file
      3. Validate records if enabled
      4. Prepend _row_number column (continuing from MAX for append/upsert)
      5. Upload processed file to GCS exactly once
    """
    ctx = get_current_context()
    ti = ctx["ti"]
    config: Dict[str, Any] = ti.xcom_pull(task_ids="collect_config_task")

    # Read others_config from both XCom and raw params — XCom deserialization can drop
    # nested keys in some Airflow SDK versions, so merge both sources (XCom takes precedence).
    raw_params = dict(ctx.get("params") or {})
    merged_others = {
        **dict(raw_params.get("others_config") or {}),
        **dict(config.get("others_config") or {}),
    }

    resource_dict = config.get("resource", {})
    gcs_config = config.get("gcs_config", {})

    file_source = resource_dict.get("url")
    resource_id = resource_dict.get("id")
    schemas = resource_dict.get("schemas")
    ingestion_method = resource_dict.get("ingestion_method", "overwrite")

    if not file_source:
        raise RuntimeError("Resource file URL is missing")

    site_id = config.get("ckan_config", {}).get("site_id", "")
    conn_id = f"{site_id}_google_cloud"
    bucket = gcs_config.get("bucket")
    chunk_size = gcs_config.get("chunk_size", DEFAULT_CHUNK_SIZE)
    row_number_column = merged_others.get("row_number_column") or "_row"
    record_updated_at_column = merged_others.get("record_updated_at_column") or "record_updated_at"
    validate_records = merged_others.get("validate_records", False)

    compression = "GZIP" if file_source.lower().endswith(".gz") else "NONE"
    object_name = gcs_object_name(file_source, resource_id, bucket)

    try:
        conn = BaseHook.get_connection(f"{site_id}_api_key")
        ckan_api_key = conn.password if conn else None
    except Exception:
        ckan_api_key = None

    storage_client = gcs_client(conn_id)
    tmp_fd, tmp_path = tempfile.mkstemp(suffix=".csv")
    os.close(tmp_fd)
    proc_fd, processed_path = tempfile.mkstemp(suffix="_processed.csv")
    os.close(proc_fd)

    try:
        # Step 1: Download
        ckan_status_update_async(config, state="running", message="Downloading CSV file")
        download_http_to_file(file_source, tmp_path, chunk_size, ckan_api_key)

        # Step 2: Schema
        if schemas:
            descriptor = _sanitize_frictionless_descriptor(_load_frictionless_descriptor(schemas))
        else:
            fr_resource = Resource(path=tmp_path)
            fr_resource.infer()
            descriptor = _sanitize_frictionless_descriptor(fr_resource.schema.to_descriptor())
        logger.info("Schema ready: %d fields", len(descriptor.get("fields", [])))

        # Step 3: Validate (optional)
        if validate_records:
            ckan_status_update_async(config, state="running", message="Validating CSV records")
            report = validate_csv(source_file=tmp_path, schema=descriptor, limit_rows=None, limit_errors=1000)
            if not report.valid:
                report_dict = report.to_dict()
                task_stats = ((report_dict.get("tasks") or [{}])[0]).get("stats", {})
                total_rows = report.stats.get("rows") or task_stats.get("rows", 0)
                total_errors = report.stats.get("errors") or task_stats.get("errors", 0)
                raise AirflowException(json.dumps({
                    "message": "Records validation failed. rows=%d errors=%d" % (total_rows, total_errors),
                    "report": report_dict,
                }))
            logger.info("CSV validation passed")
        else:
            logger.info("Validation skipped — set others_config.validate_records=true to enable")

        # Step 4: Add _row column
        project_id = gcs_config.get("project_id")
        dataset_id = gcs_config.get("dataset_id")
        target_fqn = table_fqn(project_id, dataset_id, resource_id)
        start = _row_number_start(conn_id, project_id, target_fqn, row_number_column, ingestion_method)
        logger.info("%s mode: %s starts at %d", ingestion_method, row_number_column, start)
        _add_row_number_to_csv(tmp_path, processed_path, row_number_column, start=start)

        # Step 5: Upload to GCS
        ckan_status_update_async(config, state="running", message="Uploading processed CSV to GCS")
        gcs_uri = upload_file_to_gcs(storage_client, processed_path, bucket, object_name)
        ckan_status_update_async(config, state="running", message=f"CSV uploaded: {gcs_uri}")

        return {
            "gcs_uri": gcs_uri,
            "compression": compression,
            "schema_descriptor": descriptor,
            "row_number_column": row_number_column,
            "record_updated_at_column": record_updated_at_column,
        }

    finally:
        for path in [tmp_path, processed_path]:
            try:
                os.unlink(path)
            except OSError:
                pass


@task.branch(trigger_rule=TriggerRule.NONE_FAILED)
def branch_write_method() -> str:
    ctx = get_current_context()
    ti = ctx["ti"]
    config: Dict[str, Any] = ti.xcom_pull(task_ids="collect_config_task")
    prepare_result: Dict[str, Any] = ti.xcom_pull(task_ids="prepare_and_upload_task")

    method = config.get("resource", {}).get("ingestion_method")
    if method == "upsert":
        unique_keys = extract_unique_keys_from_schema(prepare_result.get("schema_descriptor", {}))
        if not unique_keys:
            raise RuntimeError("No unique keys specified for upsert ingestion method.")
        return "upsert_table_task"
    return "overwrite_or_append_table_task"


@task(task_id="overwrite_or_append_table_task")
def overwrite_or_append_table_task() -> None:
    ctx = get_current_context()
    ti = ctx["ti"]
    config: Dict[str, Any] = ti.xcom_pull(task_ids="collect_config_task")
    prepare_result: Dict[str, Any] = ti.xcom_pull(task_ids="prepare_and_upload_task")

    resource_dict = config.get("resource", {})
    gcs_config = config.get("gcs_config", {})
    write_method = resource_dict.get("ingestion_method", "overwrite")

    site_id = config.get("ckan_config", {}).get("site_id", "")
    conn_id = f"{site_id}_google_cloud"
    project_id = gcs_config.get("project_id")
    dataset_id = gcs_config.get("dataset_id")
    resource_id = resource_dict.get("id")
    skip_leading_rows = int(config.get("others_config", {}).get("skip_leading_rows", 1))

    client = bq_client(conn_id, project_id)
    ensure_dataset_exists(client, project_id, dataset_id)

    ckan_status_update_async(
        config,
        state="running",
        message=f"{'Appending' if write_method == 'append' else 'Overwriting'} to BigQuery",
    )
    append_or_overwrite_flow(
        client,
        prepare_result["gcs_uri"],
        prepare_result["compression"],
        table_fqn(project_id, dataset_id, resource_id),
        write_method,
        _build_schema_fields(prepare_result["schema_descriptor"], prepare_result.get("row_number_column")),
        skip_leading_rows,
        record_updated_at_column=prepare_result.get("record_updated_at_column"),
        job_timestamp=datetime.now(timezone.utc),
    )
    ckan_status_update_async(config, state="running", message=f"{write_method.capitalize()} complete")


@task(task_id="upsert_table_task")
def upsert_table_task() -> None:
    ctx = get_current_context()
    ti = ctx["ti"]
    config: Dict[str, Any] = ti.xcom_pull(task_ids="collect_config_task")
    prepare_result: Dict[str, Any] = ti.xcom_pull(task_ids="prepare_and_upload_task")

    resource_dict = config.get("resource", {})
    gcs_config = config.get("gcs_config", {})
    others_config = config.get("others_config", {})

    site_id = config.get("ckan_config", {}).get("site_id", "")
    conn_id = f"{site_id}_google_cloud"
    project_id = gcs_config.get("project_id")
    dataset_id = gcs_config.get("dataset_id")
    resource_id = resource_dict.get("id")
    skip_leading_rows = int(others_config.get("skip_leading_rows", 1))
    temp_table_prefix = others_config.get("temp_table_prefix", "_temp_")

    schema_descriptor = prepare_result["schema_descriptor"]
    row_number_column = prepare_result.get("row_number_column")

    client = bq_client(conn_id, project_id)
    ensure_dataset_exists(client, project_id, dataset_id)

    ckan_status_update_async(config, state="running", message="Upserting to BigQuery")
    upsert_flow(
        client,
        prepare_result["gcs_uri"],
        prepare_result["compression"],
        table_fqn(project_id, dataset_id, resource_id),
        table_fqn(project_id, dataset_id, f"{temp_table_prefix}{resource_id}"),
        extract_unique_keys_from_schema(schema_descriptor),
        _build_schema_fields(schema_descriptor, row_number_column),
        skip_leading_rows,
        preserve_columns=[row_number_column] if row_number_column else None,
        record_updated_at_column=prepare_result.get("record_updated_at_column"),
        job_timestamp=datetime.now(timezone.utc),
    )
    ckan_status_update_async(config, state="running", message="Upsert complete")


@task(task_id="cleanup_gcs", trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)
def cleanup_gcs_task() -> None:
    ctx = get_current_context()
    ti = ctx["ti"]
    config: Dict[str, Any] = ti.xcom_pull(task_ids="collect_config_task") or {}
    prepare_result: Dict[str, Any] = ti.xcom_pull(task_ids="prepare_and_upload_task") or {}

    site_id = config.get("ckan_config", {}).get("site_id", "")
    ckan_status_update_async(config, state="running", message="Cleanup: deleting temp GCS object")
    ObjectStoragePath(prepare_result["gcs_uri"], conn_id=f"{site_id}_google_cloud").unlink()
    ckan_status_update_async(config, state="running", message="Cleanup completed — awaiting final status")


with DAG(
    dag_id=DAG_ID,
    schedule=None,
    catchup=False,
    tags=["bigquery", "gcs", "csv"],
    default_args=DEFAULT_ARGS,
    on_success_callback=dag_success_callback,
    on_failure_callback=dag_run_failure_callback,
) as dag:
    config = collect_config_task()
    prepare = prepare_and_upload_task()
    branch = branch_write_method()
    append = overwrite_or_append_table_task()
    upsert = upsert_table_task()
    cleanup = cleanup_gcs_task()

    config >> prepare >> branch
    branch >> [append, upsert]
    [append, upsert] >> cleanup
