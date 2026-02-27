"""BigQuery data warehouse operations."""

import logging
from datetime import datetime, timezone
from typing import List, Optional

from google.api_core.exceptions import NotFound
from google.cloud import bigquery

logger = logging.getLogger(__name__)


def table_fqn(project_id: str, dataset_id: str, table_name: str) -> str:
    """Generate fully qualified table name for BigQuery."""
    return f"{project_id}.{dataset_id}.{table_name}"


def ensure_dataset_exists(
    client: bigquery.Client, project_id: str, dataset_id: str
) -> None:
    """Create dataset if it doesn't exist."""
    ds_id = f"{project_id}.{dataset_id}"
    try:
        client.get_dataset(ds_id)
    except NotFound:
        client.create_dataset(bigquery.Dataset(ds_id))
        logger.info("Created dataset: %s", ds_id)


def load_gcs_to_bq_table(
    client: bigquery.Client,
    source_uri: str,
    dest_fqn: str,
    compression: str,
    write_disposition: str,
    schema_fields: Optional[List[bigquery.SchemaField]] = None,
    allow_field_addition: bool = False,
    skip_leading_rows: int = 1,
) -> None:
    """Load CSV from GCS to BigQuery table."""
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        autodetect=schema_fields is None,
        skip_leading_rows=skip_leading_rows,
        write_disposition=write_disposition,
        create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED,
    )

    if schema_fields is not None:
        job_config.schema = schema_fields

    if allow_field_addition:
        job_config.schema_update_options = [
            bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION
        ]

    try:
        job_config.compression = compression
    except AttributeError:
        job_config._properties.setdefault("load", {})["compression"] = compression

    logger.info(
        "BigQuery load start: source=%s dest=%s disposition=%s",
        source_uri,
        dest_fqn,
        write_disposition,
    )
    job = client.load_table_from_uri(source_uri, dest_fqn, job_config=job_config)
    job.result()

    if job.errors:
        raise RuntimeError(f"Load job failed: {job.errors}")

    logger.info("BigQuery load complete: dest=%s", dest_fqn)


def ensure_target_exists_from_stage(
    client: bigquery.Client, target_fqn: str, stage_fqn: str
) -> None:
    """Ensure target table exists with same schema as stage table."""
    try:
        client.get_table(target_fqn)
        return
    except NotFound:
        pass

    client.query(
        f"CREATE TABLE `{target_fqn}` AS SELECT * FROM `{stage_fqn}` WHERE 1=0"
    ).result()
    logger.info("Created target table: %s", target_fqn)


def ensure_table_has_fields(
    client: bigquery.Client,
    table_fqn: str,
    fields: List[bigquery.SchemaField],
) -> None:
    """Add missing fields to existing table."""
    try:
        table = client.get_table(table_fqn)
    except NotFound:
        return

    existing = {field.name for field in table.schema}
    to_add = [field for field in fields if field.name not in existing]
    if not to_add:
        return

    table.schema = list(table.schema) + to_add
    client.update_table(table, ["schema"])
    logger.info("Added columns to %s: %s", table_fqn, ", ".join(f.name for f in to_add))


def resolve_unique_keys_from_stage(
    client: bigquery.Client,
    stage_fqn: str,
    user_keys: List[str],
    debug: bool = True,
) -> List[str]:
    """Resolve and validate unique keys exist in stage table."""
    if not user_keys:
        return []

    stage_table = client.get_table(stage_fqn)
    cols = [f.name for f in stage_table.schema]

    if debug:
        logger.info("Detected staging columns: %s", cols)

    missing = [k for k in user_keys if k not in cols]
    if missing:
        raise RuntimeError(
            f"Unique key column(s) not found in CSV schema: {missing}. Detected columns: {cols}"
        )

    return user_keys


def merge_upsert_anyvalue_dedup(
    client: bigquery.Client,
    target_fqn: str,
    stage_fqn: str,
    unique_keys: List[str],
    preserve_columns: Optional[List[str]] = None,
    record_updated_at_column: Optional[str] = None,
    job_timestamp: Optional[datetime] = None,
) -> None:
    """MERGE stage → target with change detection and record_updated_at tracking.

    preserve_columns: excluded from UPDATE SET (e.g. _row_number), still included in INSERT.
    record_updated_at_column: set to job_timestamp on INSERT and on matched-but-changed rows;
        left unchanged for matched rows where no data column changed.
    """
    stage_table = client.get_table(stage_fqn)
    cols = [f.name for f in stage_table.schema]

    missing = [k for k in unique_keys if k not in cols]
    if missing:
        raise RuntimeError(f"Unique key column(s) not found in CSV schema: {missing}")

    _preserve = set(preserve_columns or [])
    non_key_cols = [c for c in cols if c not in unique_keys]
    if not non_key_cols:
        raise RuntimeError("No non-key columns found to update/insert.")

    # Data columns: excluded from UPDATE SET only if preserved; used for change detection.
    update_cols = [c for c in non_key_cols if c not in _preserve]
    if not update_cols:
        raise RuntimeError("No columns left to update after excluding preserved columns.")

    on_clause = " AND ".join([f"T.`{k}` = S.`{k}`" for k in unique_keys])

    # Change detection: any data column differs (NULL-safe).
    change_condition = " OR\n        ".join(
        [f"T.`{c}` IS DISTINCT FROM S.`{c}`" for c in update_cols]
    )

    # UPDATE SET: data columns + record_updated_at for actually-changed rows.
    update_parts = [f"T.`{c}` = S.`{c}`" for c in update_cols]
    if record_updated_at_column:
        update_parts.append(f"T.`{record_updated_at_column}` = @job_ts")
    update_set = ",\n        ".join(update_parts)

    # INSERT: all stage columns + record_updated_at (not in stage, set via param).
    insert_col_names = cols + ([record_updated_at_column] if record_updated_at_column else [])
    insert_val_exprs = [f"S.`{c}`" for c in cols] + (["@job_ts"] if record_updated_at_column else [])
    insert_cols = ", ".join([f"`{c}`" for c in insert_col_names])
    insert_vals = ", ".join(insert_val_exprs)

    key_select = ", ".join([f"`{k}`" for k in unique_keys])
    any_select = ",\n          ".join(
        [f"ANY_VALUE(`{c}`) AS `{c}`" for c in non_key_cols]
    )

    sql = f"""
    MERGE `{target_fqn}` T
    USING (
      SELECT
        {key_select},
        {any_select}
      FROM `{stage_fqn}`
      GROUP BY {key_select}
    ) S
    ON {on_clause}
    WHEN MATCHED AND (
        {change_condition}
    ) THEN
      UPDATE SET
        {update_set}
    WHEN NOT MATCHED THEN
      INSERT ({insert_cols})
      VALUES ({insert_vals})
    """

    query_params = []
    if record_updated_at_column:
        ts = job_timestamp or datetime.now(timezone.utc)
        query_params.append(bigquery.ScalarQueryParameter("job_ts", "TIMESTAMP", ts))

    job_config = bigquery.QueryJobConfig(query_parameters=query_params) if query_params else None
    logger.info("BigQuery MERGE start: target=%s stage=%s keys=%s", target_fqn, stage_fqn, unique_keys)
    client.query(sql, job_config=job_config).result()
    logger.info("Upsert complete into %s", target_fqn)


def append_or_overwrite_flow(
    client: bigquery.Client,
    gcs_uri: str,
    compression: str,
    target_fqn: str,
    write_method: str,
    schema_fields: Optional[List[bigquery.SchemaField]],
    skip_leading_rows: int,
    record_updated_at_column: Optional[str] = None,
    job_timestamp: Optional[datetime] = None,
) -> None:
    """Load data from GCS with append or overwrite disposition."""
    if schema_fields:
        ensure_table_has_fields(client, target_fqn, schema_fields)

    disposition = (
        bigquery.WriteDisposition.WRITE_APPEND
        if write_method == "append"
        else bigquery.WriteDisposition.WRITE_TRUNCATE
    )
    allow_field_addition = disposition == bigquery.WriteDisposition.WRITE_APPEND

    logger.info("Loading into target: %s using %s", target_fqn, write_method)
    load_gcs_to_bq_table(
        client=client,
        source_uri=gcs_uri,
        dest_fqn=target_fqn,
        compression=compression,
        write_disposition=disposition,
        schema_fields=schema_fields,
        allow_field_addition=allow_field_addition,
        skip_leading_rows=skip_leading_rows,
    )

    if record_updated_at_column:
        # Ensure column exists (WRITE_TRUNCATE recreates table without it).
        ensure_table_has_fields(client, target_fqn, [
            bigquery.SchemaField(record_updated_at_column, "TIMESTAMP", mode="NULLABLE")
        ])
        ts = job_timestamp or datetime.now(timezone.utc)
        # For overwrite: all rows are new (NULL). For append: only newly inserted rows are NULL.
        client.query(
            f"UPDATE `{target_fqn}` SET `{record_updated_at_column}` = @ts"
            f" WHERE `{record_updated_at_column}` IS NULL",
            job_config=bigquery.QueryJobConfig(query_parameters=[
                bigquery.ScalarQueryParameter("ts", "TIMESTAMP", ts)
            ]),
        ).result()
        logger.info("Set %s for new rows in %s", record_updated_at_column, target_fqn)

    logger.info("Load complete into %s", target_fqn)


def upsert_flow(
    client: bigquery.Client,
    gcs_uri: str,
    compression: str,
    target_fqn: str,
    stage_fqn: str,
    unique_keys: List[str],
    schema_fields: Optional[List[bigquery.SchemaField]],
    skip_leading_rows: int,
    preserve_columns: Optional[List[str]] = None,
    record_updated_at_column: Optional[str] = None,
    job_timestamp: Optional[datetime] = None,
) -> None:
    """Complete upsert flow: load stage, merge to target."""
    logger.info("Loading into staging: %s", stage_fqn)
    load_gcs_to_bq_table(
        client=client,
        source_uri=gcs_uri,
        dest_fqn=stage_fqn,
        compression=compression,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        schema_fields=schema_fields,
        skip_leading_rows=skip_leading_rows,
    )
    logger.info("Staging load complete")

    unique_keys = resolve_unique_keys_from_stage(client, stage_fqn, unique_keys, debug=True)
    logger.info("Resolved UNIQUE_KEYS -> %s", unique_keys)

    ensure_target_exists_from_stage(client, target_fqn, stage_fqn)
    stage_table = client.get_table(stage_fqn)
    ensure_table_has_fields(client, target_fqn, stage_table.schema)

    # record_updated_at is not in the CSV/stage — ensure it exists on target before MERGE.
    if record_updated_at_column:
        ensure_table_has_fields(client, target_fqn, [
            bigquery.SchemaField(record_updated_at_column, "TIMESTAMP", mode="NULLABLE")
        ])

    logger.info("Upserting into %s using UNIQUE_KEYS=%s", target_fqn, unique_keys)
    merge_upsert_anyvalue_dedup(
        client, target_fqn, stage_fqn, unique_keys,
        preserve_columns=preserve_columns,
        record_updated_at_column=record_updated_at_column,
        job_timestamp=job_timestamp,
    )

    client.delete_table(stage_fqn, not_found_ok=True)
    logger.info("Deleted staging table: %s", stage_fqn)
