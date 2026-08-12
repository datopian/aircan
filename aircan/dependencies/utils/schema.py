"""Frictionless schema and BigQuery schema conversion utilities."""

import json
import logging
import re
from typing import Any, List, Mapping, Optional

from google.cloud import bigquery

logger = logging.getLogger(__name__)


def frictionless_to_bigquery_schema(field_type: str) -> str:
    """Convert frictionless field type to BigQuery type."""
    mapper = {
        "string": "STRING",
        "number": "BIGNUMERIC",
        "integer": "INT64",
        "boolean": "BOOL",
        "object": "JSON",
        "array": "JSON",
        "list": "JSON",
        "datetime": "TIMESTAMP",
        "date": "DATE",
        "time": "TIME",
        "year": "INT64",
        "yearmonth": "STRING",
        "duration": "INTERVAL",
        "geopoint": "GEOGRAPHY",
        "geojson": "GEOGRAPHY",
        "any": "STRING",
    }
    return mapper.get(str(field_type).lower(), "STRING")


def _load_frictionless_descriptor(schema_arg: Any) -> dict:
    """Load and validate a frictionless schema descriptor."""
    if isinstance(schema_arg, dict):
        return schema_arg
    try:
        parsed = json.loads(schema_arg)
    except json.JSONDecodeError as exc:
        raise RuntimeError(
            "Invalid frictionless schema. Provide a JSON object string."
        ) from exc
    if not isinstance(parsed, dict):
        raise RuntimeError("Frictionless schema must be a JSON object.")
    return parsed


def sanitize_column_name(name: str) -> str:
    """Sanitize column name for BigQuery compliance."""
    name = name.strip()
    name = re.sub(r"[^a-zA-Z0-9_]+", "_", name)
    name = re.sub(r"_+", "_", name).strip("_")
    if not re.match(r"^[A-Za-z_]", name):
        name = f"f_{name}"
    return name[:128]


def _sanitize_frictionless_descriptor(descriptor: Mapping[str, Any]) -> dict:
    """Sanitize field names in a frictionless schema descriptor."""
    fields = descriptor.get("fields", [])
    if not isinstance(fields, list):
        raise RuntimeError("Frictionless schema fields must be a list.")

    seen: dict[str, str] = {}
    sanitized_fields: list[dict] = []

    for field in fields:
        if not isinstance(field, dict):
            raise RuntimeError("Frictionless schema field must be an object.")
        original = str(field.get("name", ""))
        sanitized = sanitize_column_name(original)
        if sanitized in seen and seen[sanitized] != original:
            raise RuntimeError(
                f"Sanitized field name collision: {seen[sanitized]} and {original} -> {sanitized}"
            )
        updated = dict(field)
        updated["name"] = sanitized
        constraints = updated.get("constraints")
        if isinstance(constraints, dict):
            constraints = {key: value for key, value in constraints.items() if value != ""}
            if constraints:
                updated["constraints"] = constraints
            else:
                updated.pop("constraints", None)
        sanitized_fields.append(updated)
        seen[sanitized] = original

    updated_descriptor = dict(descriptor)
    updated_descriptor["fields"] = sanitized_fields
    return updated_descriptor


def _arrow_to_frictionless_type(arrow_type) -> str:
    """Map a pyarrow DataType to a frictionless field type."""
    import pyarrow as pa

    if pa.types.is_boolean(arrow_type):
        return "boolean"
    if pa.types.is_integer(arrow_type):
        return "integer"
    if pa.types.is_floating(arrow_type) or pa.types.is_decimal(arrow_type):
        return "number"
    if pa.types.is_timestamp(arrow_type):
        return "datetime"
    if pa.types.is_date(arrow_type):
        return "date"
    if pa.types.is_time(arrow_type):
        return "time"
    if pa.types.is_list(arrow_type) or pa.types.is_large_list(arrow_type):
        return "array"
    if pa.types.is_struct(arrow_type) or pa.types.is_map(arrow_type):
        return "object"
    # string / large_string / binary / anything exotic -> string
    return "string"


def descriptor_from_arrow_schema(arrow_schema, system_columns: List[str]) -> dict:
    """Build a frictionless schema descriptor from a parquet file's arrow schema.

    Parquet embeds authoritative types, so no data sampling is needed — reading
    the footer replaces a full-file frictionless inference download. Columns in
    system_columns are dropped to stay aligned with the stripped upload.
    """
    stripped = {str(c).strip() for c in system_columns if c}
    fields = [
        {"name": field.name, "type": _arrow_to_frictionless_type(field.type)}
        for field in arrow_schema
        if field.name.strip() not in stripped
    ]
    return _sanitize_frictionless_descriptor({"fields": fields})


def extract_primary_keys_from_schema(schema_descriptor: dict) -> List[str]:
    """Extract primary key field names from schema descriptor.

    Looks for fields with constraints.unique = True

    Args:
        schema_descriptor: Frictionless schema descriptor dict

    Returns:
        List of field names marked as unique in constraints
    """
    primary_keys = []
    return schema_descriptor.get("primaryKey") 


def schema_fields_from_descriptor(
    descriptor: Mapping[str, Any],
) -> List[bigquery.SchemaField]:
    """Convert frictionless schema descriptor to BigQuery SchemaFields."""
    fields: List[bigquery.SchemaField] = []
    for field in descriptor.get("fields", []):
        if not isinstance(field, dict):
            continue
        field_type = field.get("type") or "string"
        bq_type = frictionless_to_bigquery_schema(field_type)
        constraints = field.get("constraints") or {}
        required = bool(field.get("required") or constraints.get("required"))
        mode = "REQUIRED" if required else "NULLABLE"
        description = field.get("description")
        name = sanitize_column_name(str(field.get("name", "")))
        fields.append(
            bigquery.SchemaField(
                name=name, field_type=bq_type, mode=mode, description=description
            )
        )
    return fields


def build_schema_fields(
    schema_descriptor: dict,
    row_number_column: Optional[str],
    record_updated_at_column: Optional[str] = None,
) -> List[bigquery.SchemaField]:
    """Build BQ schema fields, prepending the row number column as INT64 if set.

    record_updated_at_column is appended LAST as TIMESTAMP — it must mirror the
    column the streamer bakes into the uploaded file (CSV loads map by position).
    """
    fields = schema_fields_from_descriptor(schema_descriptor)
    if row_number_column:
        fields = [
            bigquery.SchemaField(row_number_column, "INT64", mode="NULLABLE")
        ] + fields
    if record_updated_at_column:
        fields = fields + [
            bigquery.SchemaField(record_updated_at_column, "TIMESTAMP", mode="NULLABLE")
        ]
    return fields
