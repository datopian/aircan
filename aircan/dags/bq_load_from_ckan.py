from __future__ import annotations

import json
import os
import re
import logging
import time
import tempfile

from datetime import timedelta, datetime
from typing import Any, Dict, List, Mapping, Optional, Tuple
from urllib.parse import unquote, urlparse
from concurrent.futures import ThreadPoolExecutor

import requests
from airflow import DAG
from airflow.sdk import task, get_current_context
from airflow.task.trigger_rule import TriggerRule
from airflow.exceptions import AirflowException, AirflowSkipException
from airflow.sdk.bases.hook import BaseHook
from airflow.sdk import ObjectStoragePath

from frictionless import Resource, Schema, system
from google.cloud import bigquery
from google.cloud import storage

# Import helper utilities from dependencies
from aircan.dependencies.utils.schema import (
    frictionless_to_bigquery_schema,
    _load_frictionless_descriptor,
    sanitize_column_name,
    _sanitize_frictionless_descriptor,
    extract_unique_keys_from_schema,
    schema_fields_from_descriptor,
)
from aircan.dependencies.cloud.clients import (
    bq_client,
    gcs_client,
)
from aircan.dependencies.cloud.storage import (
    is_http_url,
    filename_from_source,
    gcs_object_name,
    stream_http_to_gcs,
    upload_source_to_gcs,
    gcs_signed_url,
)
from aircan.dependencies.cloud.warehouse import (
    table_fqn,
    ensure_dataset_exists,
    load_gcs_to_bq_table,
    ensure_target_exists_from_stage,
    ensure_table_has_fields,
    resolve_unique_keys_from_stage,
    merge_upsert_anyvalue_dedup,
    append_or_overwrite_flow,
    upsert_flow,
)
from aircan.dependencies.utils.ckan import (
    ckan_status_update_async,
)
from aircan.dependencies.utils.email import send_email, build_alert_html
from aircan.dependencies.utils.validation import validate_csv


DAG_ID = "bq_load_from_csv"

logger = logging.getLogger(__name__)
_ckan_executor = ThreadPoolExecutor(max_workers=4)

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
        "signed_url_expiration_seconds": 900,
    },
    "othesr_config": {
        "skip_leading_rows": 1,
        "temp_table_prefix": "_temp_",
        "infer_schema": True,
        "validate_records": False,
        "notification_to_email": ["example@gmail.com"],
        "notification_from_email": "sender@gmail.com",
    },
}

START_DATE = datetime.now() - timedelta(days=1)

DEFAULT_ARGS = {
    "start_date": START_DATE,
    "params": DEFAULT_PARAMS,
    "on_failure_callback": lambda ctx: failure_callback(ctx),
}


# ----------------------------
# Airflow callbacks (final state)
# ----------------------------
def dag_success_callback(context: Dict[str, Any]) -> None:
    """
    Called when the entire DAG run succeeds.
    Only update CKAN status to 'success' on complete pipeline success.
    """
    params = context.get("params")
    ckan_status_update_async(
        params, state="success", message="Pipeline completed successfully"
    )


def _notify_failure(context: Dict[str, Any]) -> None:
    """Shared logic: update CKAN to failed and send alert email."""
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
        logger.warning(
            "CKAN status update raised unexpectedly; continuing.", exc_info=True
        )

    resource_id = params.get("resource", {}).get("id", "unknown")
    site_id = params.get("ckan_config", {}).get("site_id", "")
    notification_to_email = params.get("others_config", {}).get(
        "notification_to_email", []
    )
    notification_from_email = params.get("others_config", {}).get(
        "notification_from_email", ""
    )

    smtp_conn_id = f"{site_id}_email"

    send_email(
        to=notification_to_email,
        subject=f"Aircan pipeline failed: {resource_id}",
        html_content=build_alert_html(resource_id, error_payload),
        from_email=notification_from_email,
        conn_id=smtp_conn_id,
    )


def failure_callback(context: Dict[str, Any]) -> None:
    """Task-level callback (via default_args). Always notifies."""
    _notify_failure(context)


def dag_run_failure_callback(context: Dict[str, Any]) -> None:
    """DAG-level callback. Skips if a task already failed (task-level callback handled it).
    Only fires for DAG-level failures with no failed tasks (e.g. dagrun_timeout)."""
    dag_run = context.get("dag_run")
    if dag_run and dag_run.get_task_instances(state="failed"):
        logger.info(
            "DAG-level callback skipped — task-level callback already handled it."
        )
        return
    _notify_failure(context)


@task(task_id="collect_config_task")
def collect_config_task() -> Dict[str, Any]:
    context = get_current_context()
    params = context.get("params")
    ckan_status_update_async(
        params,
        state="running",
        message="Pipeline is running preparing the configuration",
    )
    return dict(params)


@task(task_id="upload_to_gcs_task")
def upload_to_gcs_task() -> Dict[str, Any]:
    ctx = get_current_context()
    ti = ctx["ti"]
    config: Dict[str, Any] = ti.xcom_pull(task_ids="collect_config_task")

    ckan_status_update_async(
        config, state="running", message="Uploading file to GCS started"
    )

    resource_dict = config.get("resource", {})
    gcs_config = config.get("gcs_config", {})
    others_config = config.get("others_config", {})

    file_source = resource_dict.get("url")
    resource_id = resource_dict.get("id")

    if not file_source:
        raise RuntimeError("Resource file URL is missing")

    site_id = config.get("ckan_config", {}).get("site_id", "")
    conn_id = f"{site_id}_google_cloud"
    project_id = gcs_config.get("project_id")
    bucket = gcs_config.get("bucket")
    chunk_size = others_config.get("chunk_size", DEFAULT_CHUNK_SIZE)
    timeout_seconds = int(gcs_config.get("upload_ready_timeout_seconds", 120))

    storage_client = gcs_client(conn_id)

    conn = BaseHook.get_connection(f"{site_id}_api_key")
    ckan_api_key = conn.password if conn else None

    gcs_uri, compression, object_name = upload_source_to_gcs(
        storage_client, file_source, resource_id, bucket, chunk_size, ckan_api_key
    )

    ckan_status_update_async(
        config,
        state="running",
        message=f"Uploading CSV to GCS complete: {gcs_uri} (compression={compression})",
    )
    return {
        "gcs_uri": gcs_uri,
        "compression": compression,
    }


@task(task_id="infer_schema_task")
def infer_schema_task() -> Dict[str, Any]:
    ctx = get_current_context()
    ti = ctx["ti"]
    config: Dict[str, Any] = ti.xcom_pull(task_ids="collect_config_task")
    upload_result: Dict[str, Any] = ti.xcom_pull(task_ids="upload_to_gcs_task")

    resource_dict = config.get("resource", {})
    gcs_config = config.get("gcs_config", {})

    schemas = resource_dict.get("schemas")

    if schemas:
        descriptor = _load_frictionless_descriptor(schemas)
        result = _sanitize_frictionless_descriptor(descriptor)
        ckan_status_update_async(
            config,
            state="running",
            message="Schema loaded from provided descriptor",
        )
        return result

    site_id = config.get("ckan_config", {}).get("site_id", "")
    conn_id = f"{site_id}_google_cloud"
    bucket = gcs_config.get("bucket")

    expiration_seconds = gcs_config.get("signed_url_expiration_seconds", 900)
    object_name = urlparse(upload_result["gcs_uri"]).path.lstrip("/")

    storage_client = gcs_client(conn_id)

    file_source = gcs_signed_url(
        storage_client, bucket, object_name, expiration_seconds
    )

    fr_resource = Resource(path=file_source)
    fr_resource.infer()
    descriptor = fr_resource.schema.to_descriptor()

    result = _sanitize_frictionless_descriptor(descriptor)
    ckan_status_update_async(
        config, state="running", message="Schema infer successfully."
    )
    return result


@task(task_id="validate_records_task")
def validate_records_task() -> None:
    ctx = get_current_context()
    ti = ctx["ti"]
    config: Dict[str, Any] = ti.xcom_pull(task_ids="collect_config_task")
    upload_result: Dict[str, Any] = ti.xcom_pull(task_ids="upload_to_gcs_task")
    schema_descriptor: Dict[str, Any] = ti.xcom_pull(task_ids="infer_schema_task")

    others_config = config.get("others_config", {})

    if not others_config.get("validate_records", False):
        logger.info("Records validation skipped — set others_config.validate_records=true to enable")
        raise AirflowSkipException("Validation disabled in params")

    ckan_status_update_async(
        config, state="running", message="Validating CSV file against schema"
    )

    gcs_config = config.get("gcs_config", {})

    site_id = config.get("ckan_config", {}).get("site_id", "")
    conn_id = f"{site_id}_google_cloud"
    bucket = gcs_config.get("bucket")
    expiration_seconds = gcs_config.get("signed_url_expiration_seconds", 900)
    object_name = urlparse(upload_result["gcs_uri"]).path.lstrip("/")

    storage_client = gcs_client(conn_id)

    file_source = gcs_signed_url(
        storage_client, bucket, object_name, expiration_seconds
    )

    report = validate_csv(
        source_file=file_source,
        schema=schema_descriptor,
        limit_rows=None,
        show_progress=True,
        limit_errors=1000,
    )

    if not report.valid:
        report_dict = report.to_dict()
        task_stats = ((report_dict.get("tasks") or [{}])[0]).get("stats", {})
        total_rows = report.stats.get("rows") or task_stats.get("rows", 0)
        total_errors = report.stats.get("errors") or task_stats.get("errors", 0)

        error_summary = (
            "Records validation failed. Results: rows=%d errors=%d. Check report for details."
            % (total_rows, total_errors)
        )
        raise AirflowException(
            json.dumps({"message": error_summary, "report": report_dict})
        )

    ckan_status_update_async(
        config, state="running", message="CSV validation passed successfully"
    )
    logger.info("CSV validation completed successfully")


@task.branch(trigger_rule=TriggerRule.NONE_FAILED)
def branch_write_method() -> str:
    ctx = get_current_context()
    ti = ctx["ti"]
    config: Dict[str, Any] = ti.xcom_pull(task_ids="collect_config_task")
    schema_descriptor: Dict[str, Any] = ti.xcom_pull(task_ids="infer_schema_task")
    resource_dict = config.get("resource", {})

    method = resource_dict.get("ingestion_method")
    chosen = "overwrite_or_append_table_task"

    if method == "upsert":
        unique_keys = extract_unique_keys_from_schema(schema_descriptor)
        if not unique_keys:
            raise RuntimeError("No unique keys specified for upsert ingestion method.")
        chosen = "upsert_table_task"

    return chosen


@task(task_id="overwrite_or_append_table_task")
def overwrite_or_append_table_task() -> None:
    ctx = get_current_context()
    ti = ctx["ti"]
    config: Dict[str, Any] = ti.xcom_pull(task_ids="collect_config_task")
    upload_result: Dict[str, Any] = ti.xcom_pull(task_ids="upload_to_gcs_task")
    schema_descriptor: Dict[str, Any] = ti.xcom_pull(task_ids="infer_schema_task")

    resource_dict = config.get("resource", {})
    gcs_config = config.get("gcs_config", {})

    write_method = resource_dict.get("ingestion_method", "overwrite")

    ckan_status_update_async(
        config,
        state="running",
        message=f"{'Appending' if write_method == 'append' else 'Overwriting'} to BigQuery started",
    )

    resource_id = resource_dict.get("id")
    skip_leading_rows = int(config.get("skip_leading_rows", 1))

    site_id = config.get("ckan_config", {}).get("site_id", "")
    conn_id = f"{site_id}_google_cloud"
    project_id = gcs_config.get("project_id")
    dataset_id = gcs_config.get("dataset_id")

    client = bq_client(conn_id, project_id)
    ensure_dataset_exists(client, project_id, dataset_id)

    target_fqn = table_fqn(project_id, dataset_id, resource_id)
    schema_fields = schema_fields_from_descriptor(schema_descriptor)

    append_or_overwrite_flow(
        client,
        upload_result["gcs_uri"],
        upload_result["compression"],
        target_fqn,
        write_method,
        schema_fields,
        skip_leading_rows,
    )

    ckan_status_update_async(
        config,
        state="running",
        message=f"{'Appended' if write_method == 'append' else 'Overwrote'} table {target_fqn} successfully.",
    )


@task(task_id="upsert_table_task")
def upsert_table_task() -> None:
    ctx = get_current_context()
    ti = ctx["ti"]
    config: Dict[str, Any] = ti.xcom_pull(task_ids="collect_config_task")
    upload_result: Dict[str, Any] = ti.xcom_pull(task_ids="upload_to_gcs_task")
    schema_descriptor: Dict[str, Any] = ti.xcom_pull(task_ids="infer_schema_task")

    ckan_status_update_async(
        config, state="running", message="Upserting to BigQuery started"
    )
    resource_dict = config.get("resource", {})
    gcs_config = config.get("gcs_config", {})
    others_config = config.get("others_config", {})

    resource_id = resource_dict.get("id")
    unique_keys = extract_unique_keys_from_schema(schema_descriptor)

    skip_leading_rows = int(others_config.get("skip_leading_rows", 1))

    site_id = config.get("ckan_config", {}).get("site_id", "")
    conn_id = f"{site_id}_google_cloud"
    project_id = gcs_config.get("project_id")
    dataset_id = gcs_config.get("dataset_id")

    temp_table_prefix = others_config.get("temp_table_prefix", "_temp_")

    client = bq_client(conn_id, project_id)
    ensure_dataset_exists(client, project_id, dataset_id)

    target_fqn = table_fqn(project_id, dataset_id, resource_id)
    stage_fqn = table_fqn(project_id, dataset_id, f"{temp_table_prefix}{resource_id}")
    schema_fields = schema_fields_from_descriptor(schema_descriptor)

    upsert_flow(
        client,
        upload_result["gcs_uri"],
        upload_result["compression"],
        target_fqn,
        stage_fqn,
        unique_keys,
        schema_fields,
        skip_leading_rows,
    )

    ckan_status_update_async(
        config,
        state="running",
        message="Upserting to BigQuery completed.",
    )


@task(task_id="cleanup_gcs", trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)
def cleanup_gcs_task() -> None:
    ctx = get_current_context()
    ti = ctx["ti"]
    config: Dict[str, Any] = ti.xcom_pull(task_ids="collect_config_task") or {}
    upload_result: Dict[str, Any] = ti.xcom_pull(task_ids="upload_to_gcs_task") or {}

    ckan_status_update_async(
        config, state="running", message="Cleanup: deleting temp GCS object"
    )

    site_id = config.get("ckan_config", {}).get("site_id", "")
    conn_id = f"{site_id}_google_cloud"

    gcs_path = ObjectStoragePath(upload_result["gcs_uri"], conn_id=conn_id)
    gcs_path.unlink()

    ckan_status_update_async(
        config, state="running", message="Cleanup completed — awaiting final status"
    )


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
    upload = upload_to_gcs_task()

    schema = infer_schema_task()
    validate = validate_records_task()

    branch_write_method = branch_write_method()
    append = overwrite_or_append_table_task()
    upsert = upsert_table_task()
    cleanup = cleanup_gcs_task()

    config >> upload >> schema >> validate >> branch_write_method

    branch_write_method >> [append, upsert]
    [append, upsert] >> cleanup
