import pytest

from aircan.dependencies.cloud.storage import check_columns_match
from aircan.dependencies.utils.schema import ColumnMismatchError

SYSTEM = ["_id", "_updated_at"]
TABLE = ["_id", "a", "b", "c", "_updated_at"]


def test_same_columns_pass():
    check_columns_match(["a", "b", "c"], TABLE, SYSTEM)


def test_order_does_not_matter():
    check_columns_match(["c", "a", "b"], TABLE, SYSTEM)


def test_system_columns_in_the_file_are_ignored():
    check_columns_match(["_id", "a", "b", "c", "_updated_at"], TABLE, SYSTEM)


def test_new_column_is_rejected_and_named():
    with pytest.raises(ColumnMismatchError, match="not in the table: d"):
        check_columns_match(["a", "b", "c", "d"], TABLE, SYSTEM)


def test_missing_column_is_rejected_and_named():
    with pytest.raises(ColumnMismatchError, match="missing from the file: c"):
        check_columns_match(["a", "b"], TABLE, SYSTEM)


def test_renamed_column_reports_both_sides():
    with pytest.raises(ColumnMismatchError) as exc_info:
        check_columns_match(["a", "b", "c_renamed"], TABLE, SYSTEM)

    message = str(exc_info.value)
    assert "not in the table: c_renamed" in message
    assert "missing from the file: c" in message


def test_message_points_to_replace_and_datastore_create():
    with pytest.raises(ColumnMismatchError, match="Replace.*datastore_create"):
        check_columns_match(["a", "b", "c", "d"], TABLE, SYSTEM)


def test_headers_are_sanitised_like_the_streamer_does():
    check_columns_match(["Price-GBP", "Unit/ID"], ["Price_GBP", "Unit_ID"], SYSTEM)


def test_blank_header_cells_are_ignored():
    check_columns_match(["a", "b", "c", " "], TABLE, SYSTEM)


def test_missing_columns_are_tolerated_when_asked():
    check_columns_match(["a"], TABLE, SYSTEM, allow_missing=True)


def test_new_columns_still_fail_when_missing_ones_are_tolerated():
    with pytest.raises(ColumnMismatchError, match="not in the table: d"):
        check_columns_match(["a", "d"], TABLE, SYSTEM, allow_missing=True)


def _streamer(expected):
    from unittest.mock import Mock

    from aircan.dependencies.cloud.storage import HttpToGCSStreamer

    return HttpToGCSStreamer(
        http_url="http://example.test/data.json",
        storage_client=Mock(),
        bucket_name="bucket",
        object_name="obj",
        row_number_column="_id",
        ckan_http_session=Mock(),
        fmt="ndjson",
        system_columns=SYSTEM,
        field_order=["a", "b", "c"],
        expected_columns=expected,
    )


def test_json_row_with_an_unknown_key_is_rejected_not_dropped():
    with pytest.raises(ColumnMismatchError, match="not in the table: d"):
        _streamer(TABLE)._align_obj({"a": 1, "b": 2, "c": 3, "d": 4})


def test_json_row_that_omits_a_key_is_null_filled():
    assert _streamer(TABLE)._align_obj({"a": 1}) == {"a": 1, "b": None, "c": None}


def test_json_rows_are_not_checked_without_expected_columns():
    assert _streamer(None)._align_obj({"a": 1, "d": 4}) == {
        "a": 1,
        "b": None,
        "c": None,
    }
