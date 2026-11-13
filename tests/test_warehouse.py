import sqlite3
from datetime import datetime, timezone
from unittest.mock import Mock, patch

import pytest
from google.api_core.exceptions import NotFound
from google.cloud import bigquery

from aircan.dependencies.utils.schema import ColumnMismatchError

from aircan.dependencies.cloud.warehouse import (
    append_flow,
    ensure_no_new_columns,
    ensure_primary_keys_populated,
    get_table_column_names,
    merge_upsert_anyvalue_dedup,
    upsert_flow,
)


def render_merge(row_number_column="_id", updated_at=None):
    client = Mock()
    fields = [
        bigquery.SchemaField("_id", "INT64"),
        bigquery.SchemaField("site_code", "STRING"),
        bigquery.SchemaField("reading_number", "INT64"),
        bigquery.SchemaField("energy_mwh", "FLOAT64"),
    ]
    if updated_at:
        fields.append(bigquery.SchemaField(updated_at, "TIMESTAMP"))
    client.get_table.return_value.schema = fields
    merge_upsert_anyvalue_dedup(
        client, "target", "stage", ["site_code", "reading_number"],
        preserve_columns=["_id"],
        row_number_column=row_number_column,
        record_updated_at_column=updated_at,
        job_timestamp=datetime(2026, 10, 11, tzinfo=timezone.utc),
    )
    client.query.return_value.result.assert_called_once_with()
    return client.query.call_args


@pytest.mark.parametrize("layout", ["appended", "interleaved", "new_first", "duplicates", "empty", "noop"])
def test_upsert_numbers_only_new_deduplicated_composite_keys(layout):
    sql = render_merge().args[0]
    source = sql.split("USING (", 1)[1].rsplit(") S", 1)[0]
    # Execute the actual generated source query locally, without cloud writes.
    # SQLite supports the join/window syntax; MIN is a valid ANY_VALUE for
    # these duplicate rows, whose user data is identical.
    db = sqlite3.connect(":memory:")
    db.executescript("""
        CREATE TABLE target (_id INTEGER, site_code TEXT, reading_number INTEGER, energy_mwh REAL);
        CREATE TABLE stage (_id INTEGER, site_code TEXT, reading_number INTEGER, energy_mwh REAL);
    """)
    existing = [(i + 1, f"SITE_{i % 10 + 1:02d}", i // 10 + 1, float(i)) for i in range(20)]
    new = [(0, f"SITE_{i + 1:02d}", 3, float(i + 20)) for i in range(10)]
    if layout != "empty":
        db.executemany("INSERT INTO target VALUES (?, ?, ?, ?)", existing)
    incoming = existing + new
    if layout == "interleaved":
        incoming = [row for i in range(10) for row in (existing[i * 2], new[i], existing[i * 2 + 1])]
    elif layout == "new_first":
        incoming = new + existing
    elif layout == "duplicates":
        incoming += new
    elif layout == "noop":
        incoming = existing
    start = 1 if layout == "empty" else 21
    db.executemany("INSERT INTO stage VALUES (?, ?, ?, ?)", [
        (start + i, *row[1:]) for i, row in enumerate(incoming)
    ])
    rows = db.execute(source.replace("ANY_VALUE(", "MIN(")).fetchall()
    old_keys = set() if layout == "empty" else {(row[1], row[2]) for row in existing}
    baseline = 0 if layout == "empty" else 20
    # D.* is ordered as keys, then non-key columns (_id and energy_mwh).
    ids = sorted(baseline + row[-1] for row in rows if (row[0], row[1]) not in old_keys)
    count = 30 if layout == "empty" else 0 if layout == "noop" else 10
    assert ids == list(range(baseline + 1, baseline + count + 1))
    assert "COALESCE(MAX(`_id`), 0) FROM `target`) + S._aircan_insert_rn" in sql
    assert "D.`site_code` = X.`site_code` AND D.`reading_number` = X.`reading_number`" in sql
    assert "T.`_id` =" not in sql
    db.close()


def test_upsert_preserves_change_detection_and_timestamp_parameter():
    call = render_merge(updated_at="_updated_at")
    sql = call.args[0]
    assert "T.`energy_mwh` IS DISTINCT FROM S.`energy_mwh`" in sql
    assert "T.`_id` IS DISTINCT FROM" not in sql
    assert "T.`_updated_at` = @job_ts" in sql
    assert call.kwargs["job_config"].query_parameters[0].name == "job_ts"


def test_upsert_without_row_number_configuration_keeps_staged_values():
    sql = render_merge(row_number_column=None).args[0]
    assert "ROW_NUMBER()" not in sql
    assert "VALUES (S.`_id`," in sql


def test_upsert_rejects_unpreserved_row_number_column():
    client = Mock()
    client.get_table.return_value.schema = [
        bigquery.SchemaField("_id", "INT64"),
        bigquery.SchemaField("key", "STRING"),
        bigquery.SchemaField("value", "STRING"),
    ]
    with pytest.raises(ValueError, match="preserved staging column"):
        merge_upsert_anyvalue_dedup(client, "target", "stage", ["key"], row_number_column="_id")
    client.query.assert_not_called()


def test_upsert_flow_forwards_custom_row_number_column():
    module = "aircan.dependencies.cloud.warehouse"
    client = Mock()
    with (
        patch(f"{module}.load_gcs_to_bq_table"),
        patch(f"{module}.resolve_primary_keys_from_stage", return_value=["key"]),
        patch(f"{module}.ensure_primary_keys_populated"),
        patch(f"{module}.ensure_target_exists_from_stage"),
        patch(f"{module}.ensure_no_new_columns"),
        patch(f"{module}.apply_table_options"),
        patch(f"{module}.merge_upsert_anyvalue_dedup") as merge,
    ):
        upsert_flow(
            client, "gs://bucket/test.csv", "NONE", "target", "stage",
            ["key"], None, 1, preserve_columns=["row_id"], row_number_column="row_id",
        )
    assert merge.call_args.kwargs["row_number_column"] == "row_id"


def _key_check_client(counts, fields=None):
    client = Mock()
    client.get_table.return_value.schema = fields or [
        bigquery.SchemaField("site_code", "STRING"),
        bigquery.SchemaField("reading_number", "INT64"),
    ]
    client.query.return_value.result.return_value = [counts]
    return client


def test_primary_key_check_passes_when_every_key_has_a_value():
    client = _key_check_client({"total": 0, "c0": 0, "c1": 0})

    ensure_primary_keys_populated(client, "stage", ["site_code", "reading_number"])

    sql = client.query.call_args.args[0]
    assert "`site_code` IS NULL OR TRIM(`site_code`) = ''" in sql
    # Blank-string handling only applies to text columns.
    assert "TRIM(`reading_number`)" not in sql
    assert "`reading_number` IS NULL" in sql


def test_primary_key_check_rejects_missing_values_and_names_the_columns():
    client = _key_check_client({"total": 5, "c0": 2, "c1": 4})

    with pytest.raises(RuntimeError) as exc:
        ensure_primary_keys_populated(client, "stage", ["site_code", "reading_number"])

    message = str(exc.value)
    assert "5 row(s)" in message
    assert "`site_code` (2 rows)" in message
    assert "`reading_number` (4 rows)" in message
    assert "No data was changed" in message


def test_primary_key_check_only_names_columns_with_missing_values():
    client = _key_check_client({"total": 3, "c0": 0, "c1": 3})

    with pytest.raises(RuntimeError) as exc:
        ensure_primary_keys_populated(client, "stage", ["site_code", "reading_number"])

    assert "site_code" not in str(exc.value)
    assert "`reading_number` (3 rows)" in str(exc.value)


def test_primary_key_check_is_skipped_without_keys():
    client = Mock()

    ensure_primary_keys_populated(client, "stage", [])

    client.query.assert_not_called()


def test_upsert_flow_rejects_before_touching_the_target():
    module = "aircan.dependencies.cloud.warehouse"
    with (
        patch(f"{module}.load_gcs_to_bq_table"),
        patch(f"{module}.resolve_primary_keys_from_stage", return_value=["key"]),
        patch(
            f"{module}.ensure_primary_keys_populated",
            side_effect=RuntimeError("Upsert rejected"),
        ),
        patch(f"{module}.ensure_target_exists_from_stage") as ensure_target,
        patch(f"{module}.merge_upsert_anyvalue_dedup") as merge,
    ):
        with pytest.raises(RuntimeError, match="Upsert rejected"):
            upsert_flow(
                Mock(), "gs://bucket/test.csv", "NONE", "target", "stage",
                ["key"], None, 1,
            )

    ensure_target.assert_not_called()
    merge.assert_not_called()



def _table_with(*names):
    table = Mock()
    table.schema = [bigquery.SchemaField(n, "STRING") for n in names]
    return table


def _fields(*names):
    return [bigquery.SchemaField(n, "STRING") for n in names]


def test_target_columns_are_none_when_the_table_does_not_exist():
    client = Mock()
    client.get_table.side_effect = NotFound("missing")
    assert get_table_column_names(client, "target") is None


def test_target_columns_are_listed_in_table_order():
    client = Mock()
    client.get_table.return_value = _table_with("_id", "a", "b")
    assert get_table_column_names(client, "target") == ["_id", "a", "b"]


def test_no_new_columns_passes_when_the_stage_matches_the_table():
    client = Mock()
    client.get_table.return_value = _table_with("_id", "a", "b", "_updated_at")
    ensure_no_new_columns(
        client, "target", _fields("_id", "b", "a", "_updated_at"), ["_id", "_updated_at"]
    )
    client.update_table.assert_not_called()


def test_no_new_columns_rejects_a_column_the_table_lacks():
    client = Mock()
    client.get_table.return_value = _table_with("_id", "a", "_updated_at")
    with pytest.raises(ColumnMismatchError, match="not in the table: b"):
        ensure_no_new_columns(
            client, "target", _fields("_id", "a", "b", "_updated_at"), ["_id", "_updated_at"]
        )
    client.update_table.assert_not_called()


def test_no_new_columns_still_adds_a_missing_system_column():
    client = Mock()
    table = _table_with("_id", "a")
    client.get_table.return_value = table
    ensure_no_new_columns(
        client, "target", _fields("_id", "a", "_updated_at"), ["_id", "_updated_at"]
    )
    assert [f.name for f in table.schema] == ["_id", "a", "_updated_at"]
    client.update_table.assert_called_once()


def test_append_flow_rejects_new_column_before_inserting():
    module = "aircan.dependencies.cloud.warehouse"
    client = Mock()
    # append_flow reads the stage table first, then the target.
    client.get_table.side_effect = [_table_with("_id", "a", "b"), _table_with("_id", "a")]
    with (
        patch(f"{module}.load_gcs_to_bq_table"),
        patch(f"{module}.ensure_target_exists_from_stage"),
    ):
        with pytest.raises(ColumnMismatchError):
            append_flow(
                client, "gs://bucket/test.csv", "NONE", "target", "stage", None, 1,
                system_columns=["_id"],
            )
    client.query.assert_not_called()
    client.delete_table.assert_called_once_with("stage", not_found_ok=True)


def test_append_flow_keeps_the_column_error_when_stage_cleanup_fails():
    module = "aircan.dependencies.cloud.warehouse"
    client = Mock()
    client.get_table.side_effect = [_table_with("_id", "a", "b"), _table_with("_id", "a")]
    client.delete_table.side_effect = RuntimeError("cleanup failed")
    with (
        patch(f"{module}.load_gcs_to_bq_table"),
        patch(f"{module}.ensure_target_exists_from_stage"),
    ):
        with pytest.raises(ColumnMismatchError):
            append_flow(
                client, "gs://bucket/test.csv", "NONE", "target", "stage", None, 1,
                system_columns=["_id"],
            )


def test_upsert_flow_drops_the_stage_table_on_column_mismatch():
    module = "aircan.dependencies.cloud.warehouse"
    client = Mock()
    client.get_table.side_effect = [_table_with("_id", "a", "b"), _table_with("_id", "a")]
    with (
        patch(f"{module}.load_gcs_to_bq_table"),
        patch(f"{module}.resolve_primary_keys_from_stage", return_value=["a"]),
        patch(f"{module}.ensure_primary_keys_populated"),
        patch(f"{module}.ensure_target_exists_from_stage"),
        patch(f"{module}.merge_upsert_anyvalue_dedup") as merge,
    ):
        with pytest.raises(ColumnMismatchError):
            upsert_flow(
                client, "gs://bucket/test.csv", "NONE", "target", "stage",
                ["a"], None, 1, system_columns=["_id"],
            )
    merge.assert_not_called()
    client.delete_table.assert_called_once_with("stage", not_found_ok=True)
