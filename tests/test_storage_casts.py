import csv
import io
import json
import os
from unittest.mock import MagicMock

import pytest

from aircan.dependencies.cloud.storage import (
    CELL_CASTS,
    HttpToGCSStreamer,
    _to_geojson_geometry,
    _to_json_array,
    _to_wkt_point,
)


@pytest.mark.parametrize(
    "value, expected",
    [
        ('["wind","solar"]', ["wind", "solar"]),
        ("wind,solar", ["wind", "solar"]),
        ("[wind, solar]", ["wind", "solar"]),
        ("['wind','solar']", ["wind", "solar"]),
        ("gas", ["gas"]),
        ("gas,5,true", ["gas", "5", "true"]),
        ("[1, 2]", [1, 2]),
        ("[]", []),
    ],
)
def test_array_accepts_json_and_comma_separated(value, expected) -> None:
    assert json.loads(_to_json_array(value)) == expected


def test_array_empty_and_parsed_values() -> None:
    assert _to_json_array("") == ""
    assert _to_json_array("  ") == ""
    assert _to_json_array(None) is None
    assert _to_json_array(["a"]) == ["a"]


@pytest.mark.parametrize(
    "value",
    [
        "-0.1276, 51.5072",
        "-0.1276,51.5072",
        "[-0.1276, 51.5072]",
        '{"lon": -0.1276, "lat": 51.5072}',
        [-0.1276, 51.5072],
        {"lon": -0.1276, "lat": 51.5072},
    ],
)
def test_geopoint_formats_become_wkt_point(value) -> None:
    assert _to_wkt_point(value) == "POINT(-0.1276 51.5072)"


def test_geopoint_passthrough_and_empty() -> None:
    geometry = '{"type": "Point", "coordinates": [1.0, 2.0]}'
    assert json.loads(_to_wkt_point(geometry)) == json.loads(geometry)
    assert _to_wkt_point("POINT(1 2)") == "POINT(1 2)"
    assert _to_wkt_point("not a point") == "not a point"
    assert _to_wkt_point("") == ""
    assert _to_wkt_point(None) is None


POLYGON = {"type": "Polygon", "coordinates": [[[1.6, 53.8], [2.0, 53.8], [2.0, 54.0], [1.6, 53.8]]]}


def test_geojson_geometry_passes_through() -> None:
    assert json.loads(_to_geojson_geometry(json.dumps(POLYGON))) == POLYGON
    assert json.loads(_to_geojson_geometry(POLYGON)) == POLYGON


def test_geojson_feature_is_unwrapped_to_geometry() -> None:
    feature = {"type": "Feature", "properties": {"a": 1}, "geometry": POLYGON}
    assert json.loads(_to_geojson_geometry(json.dumps(feature))) == POLYGON
    assert _to_geojson_geometry({"type": "Feature", "geometry": None}) is None


def test_geojson_feature_collection_becomes_geometry_collection() -> None:
    point = {"type": "Point", "coordinates": [1.0, 2.0]}
    collection = {
        "type": "FeatureCollection",
        "features": [
            {"type": "Feature", "geometry": point},
            {"type": "Feature", "geometry": POLYGON},
        ],
    }
    assert json.loads(_to_geojson_geometry(json.dumps(collection))) == {
        "type": "GeometryCollection",
        "geometries": [point, POLYGON],
    }


def test_geojson_passthrough_and_empty() -> None:
    assert _to_geojson_geometry("POINT(1 2)") == "POINT(1 2)"
    assert _to_geojson_geometry("") == ""
    assert _to_geojson_geometry(None) is None


def _stream(fmt, body, fields):
    written = io.BytesIO()
    blob = MagicMock()
    blob.open.return_value.__enter__.return_value = written
    storage = MagicMock()
    storage.bucket.return_value.blob.return_value = blob

    response = MagicMock()
    response.raw = io.BytesIO(body.encode("utf-8"))
    response.iter_lines.return_value = body.encode("utf-8").splitlines()
    session = MagicMock()
    session.get.return_value.__enter__.return_value = response

    HttpToGCSStreamer(
        http_url="https://example.com/file",
        storage_client=storage,
        bucket_name="bucket",
        object_name="object",
        row_number_column="_id",
        ckan_http_session=session,
        fmt=fmt,
        field_order=[f["name"] for f in fields],
        cast_columns={
            i: f["type"] for i, f in enumerate(fields) if f["type"] in CELL_CASTS
        },
    ).stream()
    return written.getvalue().decode("utf-8")


FIELDS = [
    {"name": "id", "type": "integer"},
    {"name": "tags", "type": "array"},
    {"name": "location", "type": "geopoint"},
    {"name": "notes", "type": "any"},
]


def test_csv_stream_casts_array_and_geopoint_columns() -> None:
    body = (
        "id,tags,location,notes\n"
        '1,"wind,solar","-0.1276, 51.5072","a, b"\n'
        '2,"[""gas""]","[1.62, 52.215]",x\n'
        "3,,,\n"
    )
    rows = list(csv.reader(io.StringIO(_stream("csv", body, FIELDS))))

    assert rows[0] == ["_id", "id", "tags", "location", "notes"]
    assert rows[1] == ["1", "1", '["wind", "solar"]', "POINT(-0.1276 51.5072)", "a, b"]
    assert rows[2] == ["2", "2", '["gas"]', "POINT(1.62 52.215)", "x"]
    assert rows[3] == ["3", "3", "", "", ""]


def test_ndjson_stream_casts_array_and_geopoint_columns() -> None:
    body = (
        '{"id": 1, "tags": "wind,solar", "location": "-0.1276, 51.5072", "notes": "n"}\n'
        '{"id": 2, "tags": ["gas"], "location": {"lon": 1.62, "lat": 52.215}}\n'
        '{"id": 3, "tags": "", "location": null}\n'
    )
    rows = [json.loads(line) for line in _stream("ndjson", body, FIELDS).splitlines()]

    assert rows[0]["tags"] == ["wind", "solar"]
    assert rows[0]["location"] == "POINT(-0.1276 51.5072)"
    assert rows[1]["tags"] == ["gas"]
    assert rows[1]["location"] == "POINT(1.62 52.215)"
    assert rows[2]["tags"] is None
    assert rows[2]["location"] is None


ALL_TYPES_FIELDS = [
    {"name": "id", "type": "integer"},
    {"name": "price", "type": "number"},
    {"name": "name", "type": "string"},
    {"name": "accepted", "type": "boolean"},
    {"name": "delivery_date", "type": "date"},
    {"name": "gate_closure", "type": "time"},
    {"name": "delivery_start", "type": "datetime"},
    {"name": "period", "type": "yearmonth"},
    {"name": "length", "type": "duration"},
    {"name": "metadata", "type": "object"},
    {"name": "tags", "type": "array"},
    {"name": "location", "type": "geopoint"},
    {"name": "boundary", "type": "geojson"},
    {"name": "notes", "type": "any"},
]


def test_all_types_sample_csv_streams_bigquery_loadable_values() -> None:
    path = os.path.join(os.path.dirname(__file__), "fixtures", "all_types.csv")
    with open(path, encoding="utf-8") as f:
        source = list(csv.DictReader(f))
        f.seek(0)
        body = f.read()

    out = list(csv.DictReader(io.StringIO(_stream("csv", body, ALL_TYPES_FIELDS))))

    assert len(out) == len(source) == 10
    for src, row in zip(source, out):
        # Every array cell is now a JSON array of the comma-separated items.
        assert json.loads(row["tags"]) == [t.strip() for t in src["tags"].split(",")]
        lon, lat = (v.strip() for v in src["location"].split(","))
        assert row["location"] == f"POINT({float(lon)!r} {float(lat)!r})"
        assert json.loads(row["boundary"]) == json.loads(src["boundary"])
        # Columns without a cast are written exactly as in the source.
        for name in ("price", "delivery_date", "period", "length", "metadata", "notes"):
            assert row[name] == src[name]
    assert out[7]["tags"] == '["gas", "5", "true"]'
