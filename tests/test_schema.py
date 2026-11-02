from aircan.dependencies.utils.schema import (
    _sanitize_frictionless_descriptor,
    extract_primary_keys_from_schema,
)


def test_sanitize_descriptor_sanitizes_composite_primary_key() -> None:
    descriptor = {
        "fields": [{"name": "Unit ID"}, {"name": "Time From"}],
        "primaryKey": ["Unit ID", "Time From"],
    }

    sanitized = _sanitize_frictionless_descriptor(descriptor)

    assert sanitized["primaryKey"] == ["Unit_ID", "Time_From"]
    assert extract_primary_keys_from_schema(sanitized) == ["Unit_ID", "Time_From"]


def test_extract_primary_keys_wraps_and_sanitizes_single_key() -> None:
    assert extract_primary_keys_from_schema({"primaryKey": "Unit ID"}) == ["Unit_ID"]
