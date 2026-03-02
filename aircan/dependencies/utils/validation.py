"""Resource validation utilities (CSV, JSON, Parquet)."""

import logging
import requests

from frictionless import Resource, Schema, system

logger = logging.getLogger(__name__)


class ResourceValidator:
    """Validates a single resource using the authenticated CKAN HTTP session."""

    def __init__(
        self,
        ckan_http_session: requests.Session,
        fmt: str,
        source_file,
        schema: dict,
        limit_rows: int = None,
        limit_errors: int = 1000,
    ):
        self.session = ckan_http_session
        self.fmt = fmt
        self.source_file = source_file
        self.schema = schema
        self.limit_rows = limit_rows
        self.limit_errors = limit_errors

    def _context(self):
        return system.use_context(trusted=True, http_session=self.session)

    def validate_csv(self):
        with self._context():
            logger.info("Starting CSV validation: file=%s", self.source_file)

            if isinstance(self.schema, dict):
                schema_descriptor = dict(self.schema)
            else:
                schema_descriptor = self.schema.to_descriptor()

            with Resource(self.source_file) as header_resource:
                header = list(header_resource.header or [])

            descriptor_fields = schema_descriptor.get("fields", [])
            if header:
                schema_descriptor["fields"] = [
                    {**dict(descriptor_fields[i]), "name": str(col_name)}
                    for i, col_name in enumerate(header)
                    if i < len(descriptor_fields)
                    and isinstance(descriptor_fields[i], dict)
                ]

            schema_obj = Schema.from_descriptor(schema_descriptor)
            resource = Resource(self.source_file, schema=schema_obj)
            report = resource.validate(
                limit_rows=self.limit_rows,
                parallel=False,
                limit_errors=self.limit_errors,
            )
            logger.info(report.to_summary())
            return report

    def validate_json(self):
        with self._context():
            logger.info(
                "Starting JSON validation: file=%s format=%s",
                self.source_file,
                self.fmt,
            )
            schema_obj = Schema.from_descriptor(self.schema)
            resource = Resource(self.source_file, schema=schema_obj, format=self.fmt)
            report = resource.validate(
                limit_rows=self.limit_rows,
                parallel=False,
                limit_errors=self.limit_errors,
            )
            logger.info(report.to_summary())
            return report

    def validate_parquet(self):
        with self._context():
            logger.info("Starting Parquet validation: file=%s", self.source_file)
            kwargs = {"format": "parquet"}
            if self.schema:
                kwargs["schema"] = Schema.from_descriptor(self.schema)
            resource = Resource(self.source_file, **kwargs)
            report = resource.validate(
                limit_rows=self.limit_rows,
                parallel=False,
                limit_errors=self.limit_errors,
            )
            logger.info(report.to_summary())
            return report

    def validate(self):
        """Dispatch to the correct validator based on fmt set at construction."""
        if self.fmt in ("json", "ndjson", "jsonl"):
            return self.validate_json()
        if self.fmt in ("parquet", "parq"):
            return self.validate_parquet()
        return self.validate_csv()
