"""BigQuery data warehouse operations."""

import json
import logging
import re
from collections import defaultdict
from datetime import datetime, timezone
from typing import List, Optional

from google.api_core.exceptions import GoogleAPICallError, NotFound
from google.cloud import bigquery

logger = logging.getLogger(__name__)


# BigQuery type names → user-facing names (matches Frictionless type vocabulary).
_FRIENDLY_BQ_TYPE: dict = {
    "int64":      "integer",
    "double":     "number",
    "float64":    "number",
    "numeric":    "number",
    "bignumeric": "number",
    "bool":       "boolean",
    "boolean":    "boolean",
    "string":     "string",
    "date":       "date",
    "datetime":   "datetime",
    "timestamp":  "timestamp",
    "time":       "time",
    "json":       "object",
    "bytes":      "string",
}


def _friendly_type(bq_type: str) -> str:
    return _FRIENDLY_BQ_TYPE.get((bq_type or "").lower(), bq_type or "the declared type")


# Parses BigQuery's per-row load-error detail messages. Example:
#   Error while reading data, error message: Invalid NUMERIC value: 0.123;
#   byte_offset_to_start_of_line: 386321157 column_index: 16
#   column_name: "acceptanceRatio" column_type: NUMERIC value: "0.123" File: gs://...
_BQ_ROW_ERROR_RE = re.compile(
    r'error message:\s*(?P<kind>[^;]+?);.*?'
    r'column_name:\s*"(?P<col>[^"]+)"\s+'
    r'column_type:\s*(?P<type>\w+)\s+'
    r'value:\s*"(?P<value>[^"]*)"',
    re.DOTALL,
)


def _canonical_kind(kind: str) -> str:
    """Strip the value-dependent tail off a BQ error `kind` so same-shape errors group.

    e.g. 'Invalid NUMERIC value: 0.123' and 'Invalid NUMERIC value: 0.456' both
    canonicalise to 'Invalid NUMERIC value'.
    """
    k = kind.strip()
    # Patterns that embed the offending value after a colon.
    for prefix_pat in (
        r"^(Invalid \w+(?: value)?)\s*:.*$",
        r"^(Bad \w+ value)\s*:.*$",
        r"^(Could not (?:cast literal|parse))\s+.+$",
        r"^(.*?out of range(?:\s+for\s+\w+)?)\s*:?.*$",
        r"^(Required field).+$",
    ):
        m = re.match(prefix_pat, k, re.IGNORECASE)
        if m:
            return m.group(1).rstrip(".")
    return k.rstrip(".")


def _describe_error(kind: str, col_type: str) -> str:
    """Translate BigQuery's raw `kind` text into a friendly explanation.

    Dispatches on the column type so every BQ scalar type (INT64, FLOAT64,
    NUMERIC, BIGNUMERIC, BOOL, STRING, DATE, DATETIME, TIME, TIMESTAMP, JSON,
    BYTES) gets a uniform "value is not a valid <friendly>" message — with
    type-specific hints where the failure mode is actionable.
    """
    friendly = _friendly_type(col_type)
    k = kind.lower()
    ct = (col_type or "").upper()

    # "Invalid <TYPE> value: ..." — works for every BQ type.
    if "invalid" in k and "value" in k:
        if ct == "NUMERIC":
            return (
                f"value doesn't fit {friendly}  — has more than 9 fractional "
                "digits. Switch the column to FLOAT64 or round the source."
            )
        if ct == "BIGNUMERIC":
            return f"value doesn't fit {friendly} . Update the correct field type"
        return f"value is not a valid {friendly}."

    # "Bad <type> value: ..." — alternate phrasing for INT64/DOUBLE/BOOL etc.
    if k.startswith("bad ") and " value" in k:
        return f"value is not a valid {friendly}."

    if "could not parse" in k or "could not cast" in k:
        return f"value could not be parsed as {friendly}."

    if "out of range" in k:
        return f"value is out of range for {friendly}."

    if "required field" in k and "is missing" in k:
        return f"required (NOT NULL) column has missing/empty values. Mark NULLABLE or fill the source."

    # Fallback: original kind text, but with friendly type.
    return f"{kind.strip()} (expected {friendly})."


def _describe_summary_only_error(message: str) -> str:
    """Translate BigQuery's summary-only load error messages into a human form.

    These show up when BQ aborts before producing per-row errors — usually because
    every row failed the same way (column-count mismatch, encoding, wrong format,
    schema mismatch). The summary string itself is unhelpful without context, so
    we recognise common patterns and surface a likely cause + remediation.
    """
    msg = message or "BigQuery returned no error details."

    # "CSV table encountered too many errors, giving up. Rows: 0; errors: 100"
    m = re.search(
        r"encountered too many errors.*?Rows:\s*(\d+);\s*errors:\s*(\d+)",
        msg,
        re.IGNORECASE,
    )
    if m:
        rows_ok, err_count = m.group(1), m.group(2)
        if rows_ok == "0":
            return (
                f"BigQuery load failed: 0 rows loaded, {err_count}+ row errors. "
                "Every row failed — likely causes: header doesn't match the schema "
                "(wrong/missing columns, extra columns, mismatched order), wrong "
                "delimiter/quoting, or a type mismatch on every row. Verify the "
                "first data row against the declared schema."
            )
        return (
            f"BigQuery load failed: {rows_ok} rows loaded before aborting at "
            f"{err_count}+ row errors. The data is partially valid — inspect a "
            "few failing rows against the schema."
        )

    if "CSV table references column position" in msg:
        return (
            "BigQuery load failed: row has fewer columns than the schema declares. "
            "Either the source has missing trailing values, a wrong delimiter, or "
            "the schema has more fields than the CSV."
        )

    if "Error detected while parsing row" in msg:
        return (
            "BigQuery load failed: CSV parser couldn't read a row. Likely an "
            "unescaped quote, embedded newline, or wrong delimiter. Check the "
            "source file with `--csv_quote`/`--field_delimiter` in mind."
        )

    if "Unable to determine source format" in msg or "Unsupported value" in msg:
        return (
            "BigQuery load failed: source format couldn't be determined. Confirm "
            "the file is the expected CSV/JSON/Parquet and that compression is "
            "set correctly."
        )

    if "is not a valid value" in msg.lower() and "schema" in msg.lower():
        return f"BigQuery load failed: schema rejected by BigQuery — {msg}"

    # Last-resort: still echo the original, but at least strip the noisy
    # boilerplate ("Error while reading data, error message: ...").
    cleaned = re.sub(r"^Error while reading data,\s*error message:\s*", "", msg)
    cleaned = re.sub(r"\s*Please look into the errors\[\] collection.*$", "", cleaned)
    return f"BigQuery load failed: {cleaned}"


def format_bq_load_errors(job: bigquery.LoadJob, max_examples: int = 3, max_groups: int = 5) -> str:
    """Format a BigQuery LoadJob's errors into a compact, human-readable summary.

    Groups per-row errors by (column, column_type, error_kind) and lists a few
    sample values, translating BQ's raw wording into Frictionless-style type names.
    """
    errors = list(job.errors or [])
    if not errors:
        return "BigQuery load failed but no error details were returned."

    groups: dict = defaultdict(lambda: {"count": 0, "examples": []})
    skipped_top_level = 0

    for err in errors:
        message = err.get("message", "")
        # A single `message` can carry many row errors concatenated with " File: ".
        for chunk in message.split(" File: "):
            m = _BQ_ROW_ERROR_RE.search(chunk)
            if not m:
                # Job-level summaries like "N errors occurred during load" don't
                # carry column/value detail — count them separately, not as failures.
                skipped_top_level += 1
                continue
            key = (m["col"], m["type"], m["kind"].strip())
            g = groups[key]
            g["count"] += 1
            if len(g["examples"]) < max_examples and m["value"] not in g["examples"]:
                g["examples"].append(m["value"])

    if not groups:
        top = errors[0].get("message", "")
        return _describe_summary_only_error(top)

    total = sum(g["count"] for g in groups.values())
    lines = [f"BigQuery load failed: {total} row error(s) across {len(groups)} column(s)."]
    sorted_groups = sorted(groups.items(), key=lambda kv: -kv[1]["count"])
    for (col, col_type, kind), g in sorted_groups[:max_groups]:
        examples = ", ".join(repr(v) for v in g["examples"])
        desc = _describe_error(kind, col_type)
        lines.append(f'  • column "{col}": {g["count"]} row(s) — {desc} Examples: {examples}')
    if len(sorted_groups) > max_groups:
        lines.append(f"  • … {len(sorted_groups) - max_groups} more column group(s) omitted")
    return "\n".join(lines)


# Patterns for BigQuery *query* errors (MERGE/INSERT) — different surface than
# load errors. Each pattern returns a friendly one-liner; unrecognised input
# falls through to the original exception.
def translate_bq_query_error(exc: Exception) -> Optional[str]:
    """Return a friendly message for a recognised BigQuery query error, else None."""
    msg = str(exc)

    if "Scalar subquery produced more than one element" in msg:
        return (
            "Duplicate rows with the same primary key were found in the source. "
            "Deduplicate the input batch (or check your unique keys) and retry."
        )

    m = re.search(
        r"Bad (int64|double|float64|bool|numeric|bignumeric) value: (.+?)(?:;|\n|$)",
        msg,
        re.IGNORECASE,
    )
    if m:
        return f"Value {m.group(2).strip()!r} is not a valid {_friendly_type(m.group(1))}."

    m = re.search(
        r"Could not (?:cast literal|parse) ['\"](.+?)['\"]\s+(?:to type|as)\s+(\w+)",
        msg,
    )
    if m:
        return f"Value {m.group(1)!r} could not be parsed as {_friendly_type(m.group(2))}."

    m = re.search(
        r"(?:Value\s+)?out of range(?:\s+for\s+(\w+))?:?\s*(.+?)(?:;|\n|$)",
        msg,
        re.IGNORECASE,
    )
    if m:
        return f"Value {m.group(2).strip()!r} is out of range for {_friendly_type(m.group(1) or '')}."

    m = re.search(
        r"Invalid (date|timestamp|datetime|time)(?:\s+value)?:\s*(.+?)(?:;|\n|$)",
        msg,
        re.IGNORECASE,
    )
    if m:
        return f"Value {m.group(2).strip()!r} is not a valid {m.group(1).lower()}."

    return None


def get_row_number_start(
    client: bigquery.Client, target_fqn: str, column: str, ingestion_mode: str
) -> int:
    """Return the next row number start value for append/upsert (MAX + 1), or 1 otherwise."""
    if ingestion_mode not in ("append", "upsert"):
        return 1
    try:
        result = client.query(
            f"SELECT COALESCE(MAX(`{column}`), 0) AS max_rn FROM `{target_fqn}`"
        ).result()
        for row in result:
            return int(row.max_rn) + 1
    except Exception:
        logger.exception(
            "Could not query MAX(%s) from %s — starting at 1",
            column,
            target_fqn,
        )
    return 1


def bq_destination_format(fmt: str) -> str:
    """Map a canonical format string to a BigQuery DestinationFormat constant."""
    return {
        "json": bigquery.DestinationFormat.NEWLINE_DELIMITED_JSON,
        "ndjson": bigquery.DestinationFormat.NEWLINE_DELIMITED_JSON,
        "jsonl": bigquery.DestinationFormat.NEWLINE_DELIMITED_JSON,
        "parquet": bigquery.DestinationFormat.PARQUET,
        "tsv": bigquery.DestinationFormat.CSV,
    }.get(fmt, bigquery.DestinationFormat.CSV)


def export_file_ext(fmt: str) -> str:
    """Return the export file extension for a given format string."""
    return {
        "json": "ndjson",
        "ndjson": "ndjson",
        "jsonl": "ndjson",
        "parquet": "parquet",
        "tsv": "csv",  # BQ exports as CSV; TSV was normalised to CSV at ingest
    }.get(fmt, "csv")


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
    source_format: str = "csv",
) -> None:
    """Load data from GCS to BigQuery table.

    source_format: canonical format string ("csv", "json", "ndjson", "jsonl", "parquet").
    skip_leading_rows is only applied for CSV (ignored for JSON/Parquet).
    """
    _format_map = {
        "csv": bigquery.SourceFormat.CSV,
        "tsv": bigquery.SourceFormat.CSV,  # normalised to CSV at stream time
        "json": bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        "ndjson": bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        "jsonl": bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        "parquet": bigquery.SourceFormat.PARQUET,
    }
    bq_source_format = _format_map.get(source_format.strip().lower())

    job_config = bigquery.LoadJobConfig(
        source_format=bq_source_format,
        autodetect=schema_fields is None,
        write_disposition=write_disposition,
        create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED,
    )

    if bq_source_format == bigquery.SourceFormat.CSV:
        job_config.skip_leading_rows = skip_leading_rows

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
        "BigQuery load start: source=%s dest=%s disposition=%s format=%s",
        source_uri,
        dest_fqn,
        write_disposition,
        source_format,
    )
    job = client.load_table_from_uri(source_uri, dest_fqn, job_config=job_config)
    try:
        job.result()
    except GoogleAPICallError:
        summary = format_bq_load_errors(job)
        logger.error("%s", summary)
        raise RuntimeError(summary) from None

    if job.errors:
        summary = format_bq_load_errors(job)
        logger.error("%s", summary)
        raise RuntimeError(summary)

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


def resolve_primary_keys_from_stage(
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
    primary_keys: List[str],
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
    # The stage physically carries record_updated_at (baked in at stream time),
    # but the MERGE manages that column exclusively via @job_ts — keep it out of
    # the data-column list so change detection / UPDATE SET / INSERT don't
    # reference it twice.
    cols = [
        f.name
        for f in stage_table.schema
        if f.name != record_updated_at_column
    ]

    missing = [k for k in primary_keys if k not in cols]
    if missing:
        raise RuntimeError(f"Unique key column(s) not found in CSV schema: {missing}")

    _preserve = set(preserve_columns or [])
    non_key_cols = [c for c in cols if c not in primary_keys]
    if not non_key_cols:
        raise RuntimeError("No non-key columns found to update/insert.")

    # Data columns: excluded from UPDATE SET only if preserved; used for change detection.
    update_cols = [c for c in non_key_cols if c not in _preserve]
    if not update_cols:
        raise RuntimeError(
            "No columns left to update after excluding preserved columns."
        )

    on_clause = " AND ".join([f"T.`{k}` = S.`{k}`" for k in primary_keys])

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
    insert_col_names = cols + (
        [record_updated_at_column] if record_updated_at_column else []
    )
    insert_val_exprs = [f"S.`{c}`" for c in cols] + (
        ["@job_ts"] if record_updated_at_column else []
    )
    insert_cols = ", ".join([f"`{c}`" for c in insert_col_names])
    insert_vals = ", ".join(insert_val_exprs)

    key_select = ", ".join([f"`{k}`" for k in primary_keys])
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

    job_config = (
        bigquery.QueryJobConfig(query_parameters=query_params) if query_params else None
    )
    logger.info(
        "BigQuery MERGE start: target=%s stage=%s keys=%s",
        target_fqn,
        stage_fqn,
        primary_keys,
    )
    try:
        client.query(sql, job_config=job_config).result()
    except GoogleAPICallError as e:
        friendly = translate_bq_query_error(e)
        if friendly:
            logger.error("MERGE failed: %s", friendly)
            raise RuntimeError(f"Upsert failed: {friendly}") from None
        raise
    logger.info("Upsert complete into %s", target_fqn)


def encode_table_description(schema_descriptor: Optional[dict]) -> str:
    """JSON-encode a Frictionless schema under the `datastore` key.

    Produces the same `{"datastore": {"schema_version": 1, "schema": {...}}}`
    blob the datastore-api stores in a table's description, with engine-managed
    system columns stripped from `fields`.
    """
    schema = dict(schema_descriptor or {})
    schema["fields"] = [
        f
        for f in schema.get("fields", [])
        if isinstance(f, dict) and f.get("name") not in frozenset({"_id", "_updated_at"})
    ]
    payload = {
        "datastore": {
            "schema_version": 1,
            "schema": schema,
        }
    }
    return json.dumps(payload, ensure_ascii=False, separators=(",", ":"))


def apply_table_options(
    client: bigquery.Client,
    table_fqn: str,
    schema_descriptor: Optional[dict],
) -> None:
    """Set the table description (encoded schema) and `datastore_managed` label.

    Idempotent: re-applying overwrites the description and ensures the label.
    No-op if the table doesn't exist yet. Failures are logged, not raised — the
    load itself has already succeeded, so metadata tagging shouldn't fail the run.
    """
    try:
        table = client.get_table(table_fqn)
    except NotFound:
        logger.warning("apply_table_options: %s not found — skipping", table_fqn)
        return

    try:
        table.description = encode_table_description(schema_descriptor)
        labels = dict(table.labels or {})
        labels["datastore_managed"] = "false"
        table.labels = labels
        client.update_table(table, ["description", "labels"])
        logger.info("Applied datastore table options to %s", table_fqn)
    except Exception:
        logger.exception("Could not apply table options to %s", table_fqn)


def append_or_replace_flow(
    client: bigquery.Client,
    gcs_uri: str,
    compression: str,
    target_fqn: str,
    write_method: str,
    schema_fields: dict,
    skip_leading_rows: int,
    source_format: str = "csv",
    schema_descriptor: Optional[dict] = None,
) -> None:
    """Load data from GCS with append or replace disposition.

    A single atomic load job: the record-updated-at column is baked into the
    uploaded file and schema_fields, so no post-load schema patch or backfill
    UPDATE is needed — those steps raced with concurrent runs of the same
    resource ("Invalid schema update. Cannot add fields").
    """
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
        source_format=source_format,
    )

    apply_table_options(client, target_fqn, schema_descriptor)
    logger.info("Load complete into %s", target_fqn)


def append_flow(
    client: bigquery.Client,
    gcs_uri: str,
    compression: str,
    target_fqn: str,
    stage_fqn: str,
    schema_fields: Optional[List[bigquery.SchemaField]],
    skip_leading_rows: int,
    source_format: str = "csv",
    schema_descriptor: Optional[dict] = None,
) -> None:
    """Append rows to the target table, matching columns to it by NAME.

    The file is loaded into a staging table first and then INSERTed into the
    target by column name — never loaded positionally into the target. This
    lets an append accept files whose columns are reordered and lets brand-new
    columns appear anywhere in the file (not only at the end): any column the
    target lacks is added before the INSERT, and a target column absent from the
    file is left NULL for the new rows. Works uniformly for every source format.

    The row-number and record-updated-at values are baked into the staged file
    (see the streamer), so the INSERT carries them straight through — no
    post-load schema patch or backfill UPDATE that would race concurrent runs.
    """
    logger.info("Loading into staging: %s", stage_fqn)
    load_gcs_to_bq_table(
        client=client,
        source_uri=gcs_uri,
        dest_fqn=stage_fqn,
        compression=compression,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        schema_fields=schema_fields,
        skip_leading_rows=skip_leading_rows,
        source_format=source_format,
    )
    logger.info("Staging load complete")

    ensure_target_exists_from_stage(client, target_fqn, stage_fqn)
    stage_table = client.get_table(stage_fqn)
    # Add any columns the staged file introduced (new columns can appear anywhere
    # in the file — they are matched by name, not position).
    ensure_table_has_fields(client, target_fqn, stage_table.schema)

    col_list = ", ".join(f"`{field.name}`" for field in stage_table.schema)
    logger.info("Appending into %s (columns: %s)", target_fqn, col_list)
    client.query(
        f"INSERT INTO `{target_fqn}` ({col_list}) "
        f"SELECT {col_list} FROM `{stage_fqn}`"
    ).result()

    apply_table_options(client, target_fqn, schema_descriptor)

    client.delete_table(stage_fqn, not_found_ok=True)
    logger.info("Append complete into %s", target_fqn)


def get_table_header(client: bigquery.Client, source_fqn: str) -> str:
    """Return the CSV header row for a BigQuery table (comma-separated column names + newline)."""
    table = client.get_table(source_fqn)
    return ",".join(f.name for f in table.schema) + "\n"


def _iso_select_list(client: bigquery.Client, source_fqn: str) -> str:
    """Build a SELECT list that formats date/time columns as ISO 8601 strings.

    BigQuery's CSV export writes TIMESTAMP as `YYYY-MM-DD HH:MM:SS[.ffffff] UTC`
    and DATETIME as `YYYY-MM-DD HH:MM:SS[.ffffff]` — neither is ISO 8601. This
    helper wraps temporal columns in `FORMAT_*` so the exported CSV has clean
    ISO values; non-temporal columns are passed through unchanged.
    """
    table = client.get_table(source_fqn)
    parts = []
    for f in table.schema:
        name = f.name
        ft = f.field_type.upper()
        if ft == "TIMESTAMP":
            parts.append(
                f"FORMAT_TIMESTAMP('%Y-%m-%dT%H:%M:%E*SZ', `{name}`, 'UTC') AS `{name}`"
            )
        elif ft == "DATETIME":
            parts.append(f"FORMAT_DATETIME('%Y-%m-%dT%H:%M:%E*S', `{name}`) AS `{name}`")
        elif ft == "DATE":
            parts.append(f"FORMAT_DATE('%Y-%m-%d', `{name}`) AS `{name}`")
        elif ft == "TIME":
            parts.append(f"FORMAT_TIME('%H:%M:%E*S', `{name}`) AS `{name}`")
        else:
            parts.append(f"`{name}`")
    return ", ".join(parts)


def export_bq_to_gcs(
    client: bigquery.Client,
    source_fqn: str,
    gcs_uri: str,
    order_by_column: Optional[str] = None,
    print_header: bool = True,
    destination_format: str = bigquery.DestinationFormat.CSV,
) -> None:
    """Export a BigQuery table to GCS (wildcard URI supported).

    destination_format: bigquery.DestinationFormat constant. Defaults to CSV.
    print_header: Only applies to CSV exports.
    For CSV exports, date/time columns are reformatted to ISO 8601 via a
    short-lived temp table (and that table is also where ordering is applied).
    """
    is_csv = destination_format == bigquery.DestinationFormat.CSV
    needs_temp = is_csv or bool(order_by_column)

    if not needs_temp:
        _do_extract(
            client,
            source_fqn,
            gcs_uri,
            print_header=print_header,
            destination_format=destination_format,
        )
        return

    tmp_fqn = f"{source_fqn}_export_tmp"
    select_list = _iso_select_list(client, source_fqn) if is_csv else "*"
    order_clause = f" ORDER BY `{order_by_column}` ASC" if order_by_column else ""
    logger.info(
        "Creating export temp table %s (iso_dates=%s, order_by=%s)",
        tmp_fqn,
        is_csv,
        order_by_column,
    )
    client.query(
        f"CREATE OR REPLACE TABLE `{tmp_fqn}` AS"
        f" SELECT {select_list} FROM `{source_fqn}`{order_clause}"
    ).result()
    try:
        _do_extract(
            client,
            tmp_fqn,
            gcs_uri,
            print_header=print_header,
            destination_format=destination_format,
        )
    finally:
        client.delete_table(tmp_fqn, not_found_ok=True)
        logger.info("Deleted temp export table: %s", tmp_fqn)


def _do_extract(
    client: bigquery.Client,
    table_fqn_str: str,
    gcs_uri: str,
    print_header: bool = True,
    destination_format: str = bigquery.DestinationFormat.CSV,
) -> None:
    job_config = bigquery.ExtractJobConfig(destination_format=destination_format)
    if destination_format == bigquery.DestinationFormat.CSV:
        job_config.print_header = print_header
    job = client.extract_table(table_fqn_str, gcs_uri, job_config=job_config)
    job.result()
    if job.errors:
        raise RuntimeError(f"BQ export job failed: {job.errors}")
    logger.info("Exported %s -> %s", table_fqn_str, gcs_uri)


def upsert_flow(
    client: bigquery.Client,
    gcs_uri: str,
    compression: str,
    target_fqn: str,
    stage_fqn: str,
    primary_keys: List[str],
    schema_fields: Optional[List[bigquery.SchemaField]],
    skip_leading_rows: int,
    preserve_columns: Optional[List[str]] = None,
    record_updated_at_column: Optional[str] = None,
    job_timestamp: Optional[datetime] = None,
    source_format: str = "csv",
    schema_descriptor: Optional[dict] = None,
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
        source_format=source_format,
    )
    logger.info("Staging load complete")

    primary_keys = resolve_primary_keys_from_stage(
        client, stage_fqn, primary_keys, debug=True
    )
    logger.info("Resolved UNIQUE_KEYS -> %s", primary_keys)

    ensure_target_exists_from_stage(client, target_fqn, stage_fqn)
    stage_table = client.get_table(stage_fqn)
    # Stage schema includes record_updated_at (baked in at stream time), so this
    # also guarantees the column exists on target before the MERGE references it.
    ensure_table_has_fields(client, target_fqn, stage_table.schema)

    logger.info("Upserting into %s using UNIQUE_KEYS=%s", target_fqn, primary_keys)
    merge_upsert_anyvalue_dedup(
        client,
        target_fqn,
        stage_fqn,
        primary_keys,
        preserve_columns=preserve_columns,
        record_updated_at_column=record_updated_at_column,
        job_timestamp=job_timestamp,
    )

    apply_table_options(client, target_fqn, schema_descriptor)

    client.delete_table(stage_fqn, not_found_ok=True)
    logger.info("Deleted staging table: %s", stage_fqn)
