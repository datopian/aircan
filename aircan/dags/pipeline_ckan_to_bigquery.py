from __future__ import annotations

import json
import os
import logging
import re
import requests

from datetime import timedelta, datetime, timezone
from typing import Any, Dict, List, Optional
from airflow import DAG
from airflow.sdk import task, get_current_context
from airflow.task.trigger_rule import TriggerRule
from airflow.exceptions import AirflowException
from airflow.sdk.bases.hook import BaseHook
from airflow.sdk import ObjectStoragePath
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

from frictionless import resources, system
from google.cloud import bigquery

from aircan.dependencies.utils.schema import (
    _load_frictionless_descriptor,
    _sanitize_frictionless_descriptor,
    build_schema_fields,
    descriptor_from_arrow_schema,
    extract_primary_keys_from_schema,
)
from aircan.dependencies.cloud.clients import bq_client, gcs_client, s3_client
from aircan.dependencies.cloud.storage import (
    gcs_object_name,
    filename_from_resource,
    read_parquet_footer_schema,
    HttpToGCSStreamer,
    compose_gcs_shards_to_s3,
    upload_parquet_shards_to_s3,
)
from aircan.dependencies.cloud.warehouse import (
    table_fqn,
    ensure_dataset_exists,
    get_row_number_start,
    bq_destination_format,
    export_file_ext,
    append_or_replace_flow,
    upsert_flow,
    export_bq_to_gcs,
    get_table_header,
)
from aircan.dependencies.utils.ckan import ckan_status_update_async
from aircan.dependencies.utils.email import send_email, build_alert_html
from aircan.dependencies.utils.validation import ResourceValidator


DAG_ID = "pipeline_ckan_to_bigquery"
logger = logging.getLogger(__name__)

DEFAULT_PARAMS = {
    "resource": {
        "id": "63b3d77e-032f-4ef0-8790-cc81d0509d5f",
        "url": "https://example.com/dataset/266c57d5-ea6a-4b16-89e6-2f9e072f333d/resource/c8efed04-7100-4d85-9615-c6f12a7abe18/download/date.csv",
        "format": "",  # optional explicit format override: "csv", "json", or "parquet"
        "schema": {},
        "ingestion_mode": "replace", # or "append" or "upsert"
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
        "row_number_column": "_id",
        "record_updated_at_column": "_updated_at",
        "notification_to_email": ["example@gmail.com"],
        "notification_from_email": "sender@gmail.com",
        "http_connect_timeout": 10,
        "http_read_timeout": 1200,
    },
    "s3_config": {
        "conn_id": "aws_default",
        "bucket": "my-s3-export-bucket",
        "key_prefix": "ckan-exports/",
        "endpoint_url": "",
    },
}

START_DATE = datetime.now() - timedelta(days=1)

DEFAULT_ARGS = {
    "start_date": START_DATE,
    "params": DEFAULT_PARAMS,
    "on_failure_callback": lambda ctx: _notify_failure(ctx),
}


# ----------------------------
# Airflow helpers
# ----------------------------


def _get_task_context() -> tuple[Dict[str, Any], Dict[str, Any]]:
    """Pull config and prepare_result from XCom for the current task."""
    ctx = get_current_context()
    ti = ctx["ti"]
    config = ti.xcom_pull(task_ids="collect_config_task") or {}
    prepare_result = ti.xcom_pull(task_ids="prepare_and_upload_task") or {}
    return config, prepare_result


def _run_suffix(ctx: Dict[str, Any]) -> str:
    """GCS-safe suffix unique to this DAG run.

    Scopes GCS objects per run so concurrent runs of the same resource can't
    overwrite or clean up each other's files mid-load/export.
    """
    run_id = str(ctx.get("run_id") or "")
    return re.sub(r"[^A-Za-z0-9_.-]", "_", run_id)


# ----------------------------
# Airflow callbacks
# ----------------------------


def dag_success_callback(context: Dict[str, Any]) -> None:
    ckan_status_update_async(
        context.get("params"),
        state="success",
        message="Pipeline completed successfully",
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
        logger.warning(
            "CKAN status update raised unexpectedly; continuing.", exc_info=True
        )

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
        logger.info(
            "DAG-level callback skipped — task-level callback already handled it."
        )
        return
    _notify_failure(context)


# ----------------------------
# Tasks
# ----------------------------


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


@task(task_id="prepare_and_upload_task")
def prepare_and_upload_task() -> Dict[str, Any]:
    """
    Prepare and upload task — no local temp files, no intermediate GCS blobs.

    All formats follow the same pipeline:
      1. Infer schema (parquet: footer read via pyarrow; others: frictionless sample)
      2. Validate from source URL if validate_records=True
      3. Stream source → GCS with the row-number column injected first and the
         record-updated-at column appended last, so the BQ load is one atomic job.
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
    schema = resource_dict.get("schema")
    ingestion_mode = resource_dict.get("ingestion_mode", "replace")

    if not file_source:
        raise RuntimeError("Resource file URL is missing")

    site_id = config.get("ckan_config", {}).get("site_id", "")
    conn_id = f"{site_id}_google_cloud"
    bucket = gcs_config.get("bucket")
    row_number_column = merged_others.get("row_number_column") or "_id"
    record_updated_at_column = (
        merged_others.get("record_updated_at_column") or "_updated_at"
    )
    validate_records = merged_others.get("validate_records", False)
    http_connect_timeout = merged_others.get("http_connect_timeout", 10)
    http_read_timeout = merged_others.get("http_read_timeout", 1200)

    fmt = (resource_dict.get("format") or "csv").strip().lower()

    logger.info("Format from resource dict: %s", fmt)

    object_name = gcs_object_name(file_source, resource_id, bucket, _run_suffix(ctx))
    project_id = gcs_config.get("project_id")
    dataset_id = gcs_config.get("dataset_id")
    target_fqn = table_fqn(project_id, dataset_id, resource_id)

    # Single timestamp for the whole run: baked into the uploaded file as the
    # record-updated-at column and reused by the upsert MERGE's @job_ts.
    job_timestamp = datetime.now(timezone.utc)

    try:
        conn = BaseHook.get_connection(f"{site_id}_api_key")
        ckan_api_key = conn.password if conn else None
    except Exception:
        ckan_api_key = None

    storage_client = gcs_client(conn_id)
    bq = bq_client(conn_id, project_id)

    # BQ always receives an uncompressed file (the streamer re-writes the data).
    compression = "NONE"

    # -----------------------------------------------------------------------
    # 1. Schema inference (all formats)
    # -----------------------------------------------------------------------
    ckan_http_session = requests.Session()
    if ckan_api_key:
        ckan_http_session.headers.update({"Authorization": ckan_api_key})

    # System-generated columns the pipeline injects itself. If the source file
    # already carries them, the streamer strips them during the GCS upload so
    # they don't collide with the pipeline's own columns or break BigQuery's
    # positional column mapping (which would otherwise fail the load).
    system_columns = [row_number_column, record_updated_at_column]

    if schema:
        # User-provided schema is taken as-is (normally omits the system columns).
        descriptor = _sanitize_frictionless_descriptor(
            _load_frictionless_descriptor(schema)
        )
    elif fmt == "parquet":
        # Parquet declares its types in the footer — read only that (~KBs via
        # range requests) instead of frictionless's full-file download, which
        # also drops the CKAN auth header for remote files.
        ckan_status_update_async(
            config, state="running", message="Inferring schema from Parquet footer"
        )
        arrow_schema = read_parquet_footer_schema(file_source, ckan_http_session)
        descriptor = descriptor_from_arrow_schema(arrow_schema, system_columns)
    else:
        ckan_status_update_async(
            config, state="running", message=f"Inferring schema for {fmt.upper()}"
        )
        with system.use_context(http_session=ckan_http_session):
            fr_resource = resources.TableResource(path=file_source)
            fr_resource.infer()
            inferred = fr_resource.schema.to_descriptor()
            # Inference reads the raw file, so drop any system columns it picked up
            # to stay aligned with the stripped upload.
            inferred["fields"] = [
                f
                for f in inferred.get("fields", [])
                if str(f.get("name", "")).strip() not in system_columns
            ]
            descriptor = _sanitize_frictionless_descriptor(inferred)
    logger.info("Schema ready: %d fields", len(descriptor.get("fields", [])))

    # -----------------------------------------------------------------------
    # 2. Validation (all formats)
    # -----------------------------------------------------------------------
    if validate_records:
        ckan_status_update_async(
            config, state="running", message=f"Validating {fmt.upper()} records"
        )
        report = ResourceValidator(
            ckan_http_session,
            fmt=fmt,
            source_file=file_source,
            schema=descriptor,
            limit_errors=1000,
        ).validate()
        if not report.valid:
            report_dict = report.to_dict()
            task_stats = ((report_dict.get("tasks") or [{}])[0]).get("stats", {})
            raise AirflowException(
                json.dumps(
                    {
                        "message": "Records validation failed. rows=%d errors=%d"
                        % (
                            report.stats.get("rows") or task_stats.get("rows", 0),
                            report.stats.get("errors") or task_stats.get("errors", 0),
                        ),
                        "report": report_dict,
                    }
                )
            )
        logger.info("%s validation passed", fmt.upper())
    else:
        logger.info(
            "Validation skipped — set others_config.validate_records=true to enable"
        )

    # -----------------------------------------------------------------------
    # 3. Row number start
    # -----------------------------------------------------------------------
    start = get_row_number_start(bq, target_fqn, row_number_column, ingestion_mode)
    logger.info("%s mode: %s starts at %d", ingestion_mode, row_number_column, start)

    # -----------------------------------------------------------------------
    # 4. Upload (format-specific)
    # -----------------------------------------------------------------------
    ckan_status_update_async(
        config,
        state="running",
        message=f"Processing and uploading {fmt.upper()} to GCS",
    )

    # Map date/datetime/time columns (declared with a non-ISO frictionless
    # format, e.g. "%d/%m/%Y") to their source format by CSV position, so the
    # streamer can rewrite them to ISO for BigQuery. Position-based to match
    # BigQuery's positional schema mapping. "default"/"any" are already ISO.
    date_formats = {
        idx: (str(field.get("type")).lower(), field.get("format"))
        for idx, field in enumerate(descriptor.get("fields", []))
        if str(field.get("type")).lower() in ("date", "datetime", "time", "timestamp")
        and field.get("format")
        and field.get("format") not in ("default", "any")
    }
    if date_formats:
        logger.info("Reformatting date columns to ISO at positions: %s", list(date_formats))

    # Emit CSV columns in the schema's field order, aligned to the source by
    # header name (not source position). This lets append/upsert accept files
    # whose columns are reordered, added, or omitted: values always land in the
    # BigQuery column of the matching name, and a schema field absent from the
    # source is written as NULL. date_formats above is keyed by this same field
    # index, so it stays aligned.
    field_order = [
        str(field.get("name", "")).strip()
        for field in descriptor.get("fields", [])
        if str(field.get("name", "")).strip() not in system_columns
    ]

    streamer_kwargs = {}
    if gcs_config.get("chunk_size"):
        streamer_kwargs["gcs_chunk_size"] = int(gcs_config["chunk_size"])

    gcs_uri = HttpToGCSStreamer(
        http_url=file_source,
        storage_client=storage_client,
        bucket_name=bucket,
        object_name=object_name,
        row_number_column=row_number_column,
        ckan_http_session=ckan_http_session,
        fmt=fmt,
        start=start,
        timeout=(http_connect_timeout, http_read_timeout),
        date_formats=date_formats,
        system_columns=system_columns,
        record_updated_at_column=record_updated_at_column,
        job_timestamp=job_timestamp,
        field_order=field_order,
        **streamer_kwargs,
    ).stream()

    ckan_status_update_async(
        config, state="running", message=f"{fmt.upper()} uploaded: {gcs_uri}"
    )

    return {
        "gcs_uri": gcs_uri,
        "compression": compression,
        "schema_descriptor": descriptor,
        "row_number_column": row_number_column,
        "record_updated_at_column": record_updated_at_column,
        "job_timestamp": job_timestamp.isoformat(),
        "source_format": fmt,
    }


@task.branch(trigger_rule=TriggerRule.NONE_FAILED)
def branch_write_method() -> str:
    config, prepare_result = _get_task_context()

    method = config.get("resource", {}).get("ingestion_mode")
    if method == "upsert":
        primary_keys = extract_primary_keys_from_schema(
            prepare_result.get("schema_descriptor", {})
        )
        if not primary_keys:
            raise RuntimeError("No primary keys specified for upsert ingestion method.")
        return "upsert_table_task"
    return "replace_or_append_table_task"


@task(task_id="replace_or_append_table_task")
def replace_or_append_table_task() -> None:
    config, prepare_result = _get_task_context()

    resource_dict = config.get("resource", {})
    gcs_config = config.get("gcs_config", {})
    write_method = resource_dict.get("ingestion_mode", "replace")

    site_id = config.get("ckan_config", {}).get("site_id", "")
    conn_id = f"{site_id}_google_cloud"
    project_id = gcs_config.get("project_id")
    dataset_id = gcs_config.get("dataset_id")
    resource_id = resource_dict.get("id")
    skip_leading_rows = int(config.get("others_config", {}).get("skip_leading_rows", 1))
    source_format = prepare_result.get("source_format", "csv")

    client = bq_client(conn_id, project_id)
    ensure_dataset_exists(client, project_id, dataset_id)

    ckan_status_update_async(
        config,
        state="running",
        message=f"{'Appending' if write_method == 'append' else 'Replacing'} to BigQuery",
    )
    append_or_replace_flow(
        client,
        prepare_result["gcs_uri"],
        prepare_result["compression"],
        table_fqn(project_id, dataset_id, resource_id),
        write_method,
        build_schema_fields(
            prepare_result["schema_descriptor"],
            prepare_result.get("row_number_column"),
            prepare_result.get("record_updated_at_column"),
        ),
        skip_leading_rows,
        source_format=source_format,
        schema_descriptor=prepare_result["schema_descriptor"],
    )
    ckan_status_update_async(
        config, state="running", message=f"{write_method.capitalize()} complete"
    )


@task(task_id="upsert_table_task")
def upsert_table_task() -> None:
    config, prepare_result = _get_task_context()

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
    source_format = prepare_result.get("source_format", "csv")

    schema_descriptor = prepare_result["schema_descriptor"]
    row_number_column = prepare_result.get("row_number_column")

    client = bq_client(conn_id, project_id)
    ensure_dataset_exists(client, project_id, dataset_id)

    ckan_status_update_async(config, state="running", message="Upserting to BigQuery")
    # Reuse the prepare task's timestamp so the MERGE's @job_ts matches the
    # record-updated-at values baked into the staged file.
    job_timestamp = datetime.fromisoformat(prepare_result["job_timestamp"])
    upsert_flow(
        client,
        prepare_result["gcs_uri"],
        prepare_result["compression"],
        table_fqn(project_id, dataset_id, resource_id),
        table_fqn(project_id, dataset_id, f"{temp_table_prefix}{resource_id}"),
        extract_primary_keys_from_schema(schema_descriptor),
        build_schema_fields(
            schema_descriptor,
            row_number_column,
            prepare_result.get("record_updated_at_column"),
        ),
        skip_leading_rows,
        preserve_columns=[row_number_column] if row_number_column else None,
        record_updated_at_column=prepare_result.get("record_updated_at_column"),
        job_timestamp=job_timestamp,
        source_format=source_format,
        schema_descriptor=schema_descriptor,
    )
    ckan_status_update_async(config, state="running", message="Upsert complete")


@task(task_id="cleanup_gcs", trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)
def cleanup_gcs_task() -> None:
    config, prepare_result = _get_task_context()
    gcs_uri = prepare_result.get("gcs_uri")
    if not gcs_uri:
        logger.warning(
            "cleanup_gcs_task: no gcs_uri in prepare_result — nothing to delete"
        )
        return

    site_id = config.get("ckan_config", {}).get("site_id", "")
    ckan_status_update_async(
        config, state="running", message="Cleanup: deleting temp GCS object"
    )
    ObjectStoragePath(gcs_uri, conn_id=f"{site_id}_google_cloud").unlink()
    ckan_status_update_async(
        config, state="running", message="Cleanup completed — awaiting final status"
    )


@task(
    task_id="export_and_publish_task",
    trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
)
def export_and_publish_task() -> None:
    """
    Final publish step:
      1. Export the target BQ table to GCS (CSV, NDJSON, or Parquet — matches ingestion format)
      2. Upload the exported file(s) from GCS to S3
      3. Clean up GCS export objects
    S3 export is skipped if s3_config.bucket is not set.
    """
    config, prepare_result = _get_task_context()

    resource_dict = config.get("resource", {})
    gcs_config = config.get("gcs_config", {})
    s3_config = config.get("s3_config") or {}

    resource_id = resource_dict.get("id")
    project_id = gcs_config.get("project_id")
    dataset_id = gcs_config.get("dataset_id")
    site_id = config.get("ckan_config", {}).get("site_id", "")
    gcs_conn_id = f"{site_id}_google_cloud"
    row_number_column = prepare_result.get("row_number_column")
    source_format = prepare_result.get("source_format", "csv")

    target_fqn_str = table_fqn(project_id, dataset_id, resource_id)

    bq = bq_client(gcs_conn_id, project_id)
    gcs = gcs_client(gcs_conn_id)

    # Export table → GCS → S3 (optional)
    s3_bucket = s3_config.get("bucket")
    if not s3_bucket:
        logger.info("No s3_config.bucket configured — skipping S3 export")
        return

    s3_conn_id = s3_config.get("conn_id") or f"{site_id}_s3"
    s3_key_prefix = s3_config.get("key_prefix", "")
    gcs_bucket = gcs_config.get("bucket")
    resource_filename = filename_from_resource(resource_dict)
    stem, _ = os.path.splitext(resource_filename)

    destination_format = bq_destination_format(source_format)
    export_ext = export_file_ext(source_format)
    is_csv = destination_format == bigquery.DestinationFormat.CSV
    is_parquet = destination_format == bigquery.DestinationFormat.PARQUET

    # Run-scoped prefix: shard compose/cleanup lists by prefix, so concurrent
    # runs of the same resource must not share it.
    run_suffix = _run_suffix(get_current_context())
    export_gcs_uri = (
        f"gs://{gcs_bucket}/exports/{resource_id}/{run_suffix}/{stem}_*.{export_ext}"
    )
    s3_key = f"{s3_key_prefix.rstrip('/')}/{stem}.{export_ext}"

    # Get CSV header before export; JSON/Parquet have no header concept
    header_row = get_table_header(bq, target_fqn_str) if is_csv else None

    ckan_status_update_async(config, state="running", message="Exporting table to GCS")
    export_bq_to_gcs(
        bq,
        target_fqn_str,
        export_gcs_uri,
        order_by_column=row_number_column,
        print_header=False,
        destination_format=destination_format,
    )

    hook = S3Hook(aws_conn_id=s3_conn_id)
    creds = hook.get_credentials()
    s3 = s3_client(
        access_key_id=creds.access_key,
        secret_access_key=creds.secret_key,
        endpoint_url=s3_config.get("endpoint_url") or None,
        region_name=s3_config.get("region") or None,
    )
    ckan_status_update_async(config, state="running", message="Uploading export to S3")
    if is_parquet:
        # Parquet shards cannot be concatenated — upload each shard individually
        s3_uri = upload_parquet_shards_to_s3(gcs, export_gcs_uri, s3, s3_bucket, s3_key)
    else:
        # CSV (with header_row prepended) or NDJSON (header_row=None, shards composable)
        s3_uri = compose_gcs_shards_to_s3(
            gcs, export_gcs_uri, header_row, s3, s3_bucket, s3_key
        )
    logger.info("Uploaded to S3: %s", s3_uri)
    ckan_status_update_async(
        config,
        state="running",
        message=f"Export complete — {s3_uri}",
    )


with DAG(
    dag_id=DAG_ID,
    schedule=None,
    catchup=False,
    tags=["bigquery", "gcs", "csv", "json", "parquet"],
    default_args=DEFAULT_ARGS,
    on_success_callback=dag_success_callback,
    on_failure_callback=dag_run_failure_callback,
) as dag:
    config = collect_config_task()
    prepare = prepare_and_upload_task()
    branch = branch_write_method()
    append = replace_or_append_table_task()
    upsert = upsert_table_task()
    cleanup = cleanup_gcs_task()
    publish = export_and_publish_task()

    config >> prepare >> branch
    branch >> [append, upsert]
    [append, upsert] >> publish >> cleanup
