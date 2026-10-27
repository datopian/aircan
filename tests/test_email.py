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

from aircan.dependencies.utils.email import alert_subject, build_alert_html


def test_validation_failure_is_labelled_dq() -> None:
    payload = {"message": "bad rows", "failed_type": "Data Validation"}

    assert (
        alert_subject(payload)
        == "Aircan data pipeline failure: Data Quality Validation Rule Failure"
    )
    html = build_alert_html("res-1", payload)
    assert "Aircan Data Pipeline Failure Alert" in html
    assert "Data Quality Validation Rule Failure" in html
    assert "Data Ingestion Failure" not in html


def test_ingestion_failure_is_labelled_ingestion() -> None:
    payload = {"message": "boom", "failed_type": "Data Ingestion"}

    assert (
        alert_subject(payload)
        == "Aircan data pipeline failure: Data Ingestion Failure"
    )
    html = build_alert_html("res-1", payload)
    assert "Aircan Data Pipeline Failure Alert" in html
    assert "Data Ingestion Failure" in html
    assert "Data Quality Validation Rule Failure" not in html


def test_untagged_failure_defaults_to_ingestion() -> None:
    payload = {"message": "datatype mismatch"}

    assert alert_subject(payload).endswith("Data Ingestion Failure")
    assert "Data Ingestion Failure" in build_alert_html("res-1", payload)


def test_email_has_no_client_specific_wording() -> None:
    html = build_alert_html("res-1", {"message": "boom"})

    assert "NESO" not in html


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
    assert "<table" not in html.split("Full list of error messages")[1]


def test_validation_report_is_capped() -> None:
    payload = {"failed_type": "Data Validation", "report": _report(40)}

    html = build_alert_html("res-1", payload)

    assert "and 20 more" in html
    assert "failing rows" not in html


def test_no_report_renders_no_report_section() -> None:
    html = build_alert_html("res-1", {"message": "boom"})

    assert "Full list of error messages" not in html
