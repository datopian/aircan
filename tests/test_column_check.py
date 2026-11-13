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
