import sys
from types import ModuleType

# email.py only needs BaseHook for sending; stub it so these tests run
# without installing Airflow.
if "airflow.sdk" not in sys.modules:
    try:
        import airflow.sdk  # noqa: F401
    except ImportError:
        airflow = ModuleType("airflow")
        sdk = ModuleType("airflow.sdk")
        sdk.BaseHook = object
        airflow.sdk = sdk
        sys.modules.update({"airflow": airflow, "airflow.sdk": sdk})

from aircan.dependencies.utils.email import alert_content, build_alert_html


def test_validation_failure_uses_dq_wording() -> None:
    payload = {"message": "bad rows", "failed_type": "Data Validation"}

    assert (
        alert_content(payload)["subject"]
        == "[Alert] NESO Data Portal DQ Validation rule failed"
    )
    html = build_alert_html("res-1", payload)
    assert "Data Quality Validation Rule Failure" in html
    assert "Data Ingestion Pipeline Failure" not in html


def test_ingestion_failure_uses_ingestion_wording() -> None:
    payload = {"message": "boom", "failed_type": "Data Ingestion"}

    assert (
        alert_content(payload)["subject"]
        == "[Alert] NESO Data Portal data ingestion pipeline failed"
    )
    html = build_alert_html("res-1", payload)
    assert "Data Ingestion Pipeline Failure" in html
    assert "Data Quality Validation Rule Failure" not in html


def test_untagged_failure_defaults_to_ingestion() -> None:
    payload = {"message": "datatype mismatch"}

    assert "ingestion pipeline failed" in alert_content(payload)["subject"]
    assert "Data Ingestion" in build_alert_html("res-1", payload)


def _report(n_errors: int = 1) -> dict:
    return {
        "tasks": [
            {
                "name": "data.csv",
                "labels": ["id", "value"],
                "errors": [
                    {
                        "type": "type-error",
                        "title": "Type Error",
                        "description": "Value does not match the field type.",
                        "message": f'Type error in row {i}: "abc" is not an integer',
                        "rowNumber": i,
                        "fieldNumber": 2,
                        "cells": [str(i), "abc"],
                    }
                    for i in range(2, 2 + n_errors)
                ],
            }
        ]
    }


def test_validation_report_is_rendered_like_the_portal() -> None:
    payload = {
        "message": "Records validation failed. rows=5 errors=1",
        "failed_type": "Data Validation",
        "report": _report(),
    }

    html = build_alert_html("res-1", payload)

    assert "Type Error" in html
    assert "Value does not match the field type." in html
    assert 'Type error in row 2: &quot;abc&quot; is not an integer' in html
    assert "background:#fee2e2;color:#7f1d1d;" in html
    assert "<th" in html


def test_validation_report_is_capped() -> None:
    payload = {"failed_type": "Data Validation", "report": _report(40)}

    html = build_alert_html("res-1", payload)

    assert "and 20 more" in html
    assert "Showing 10 of 40 failing rows" in html


def test_no_report_renders_no_report_section() -> None:
    html = build_alert_html("res-1", {"message": "boom"})

    assert "Full list of error messages" not in html
