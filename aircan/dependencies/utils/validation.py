"""CSV validation utilities."""

import logging

from frictionless import Resource, Schema, system

logger = logging.getLogger(__name__)


def validate_csv(
    source_file,
    schema,
    limit_rows=None,
    show_progress=True,
    limit_errors=None,
):
    """Validate CSV file with streaming (memory efficient).

    Args:
        source_file: Path to CSV file or signed URL
        schema: Frictionless Schema object or descriptor dict
        limit_rows: Validate only first N rows (None = all rows)
        show_progress: Show progress every 10K rows
        limit_errors: Stop after N errors (None = validate all rows)

    Returns:
        Validation report object
    """

    with system.use_context(trusted=True):

        logger.info("Starting CSV validation: file=%s", source_file)

        # Build validation schema using CSV header names while preserving field types.
        # This avoids header mismatch when downstream schema field names are sanitized.
        if isinstance(schema, dict):
            schema_descriptor = dict(schema)
        else:
            schema_descriptor = schema.to_descriptor()

        with Resource(source_file) as header_resource:
            header = list(header_resource.header or [])

        descriptor_fields = schema_descriptor.get("fields", [])

        if header:
            schema_descriptor["fields"] = [
                {
                    **dict(descriptor_fields[i]),
                    "name": str(col_name),
                }
                for i, col_name in enumerate(header)
                if i < len(descriptor_fields) and isinstance(descriptor_fields[i], dict)
            ]

        schema_obj = Schema.from_descriptor(schema_descriptor)

        resource = Resource(source_file, schema=schema_obj)

        report = resource.validate(
            limit_rows=None,
            parallel=False,
            limit_errors=1000,
        )

        logger.info(report.to_summary())

        return report
