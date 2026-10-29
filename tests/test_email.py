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
        == "[Alert] Aircan data pipeline failure: Data Quality Validation Rule Failure"
    )
    html = build_alert_html("res-1", payload)
    assert "Aircan Data Pipeline Failure Alert" in html
    assert "Data Quality Validation Rule Failure" in html
    assert "Data Ingestion Failure" not in html


def test_ingestion_failure_is_labelled_ingestion() -> None:
    payload = {"message": "boom", "failed_type": "Data Ingestion"}

    assert (
        alert_subject(payload)
        == "[Alert] Aircan data pipeline failure: Data Ingestion Failure"
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


def test_validation_report_is_aggregated_by_error_type() -> None:
    payload = {
        "failed_type": "Data Validation",
        "report": _report(3),
    }

    html = build_alert_html("res-1", payload)

    assert "Type Error" in html
    assert "Value does not match the field type." in html
    assert ">3</span>" in html
    assert "is not an integer" not in html
    assert "Full list of error messages" not in html


def test_error_types_are_sorted_by_count() -> None:
    report = _report(2)
    report["tasks"][0]["errors"] += [
        {"type": "missing-cell", "title": "Missing Cell", "message": f"m{i}"}
        for i in range(5)
    ]

    html = build_alert_html(
        "res-1", {"failed_type": "Data Validation", "report": report}
    )

    assert html.index("Missing Cell") < html.index("Type Error")


def test_report_link_note_is_shown_when_url_given() -> None:
    url = "https://portal.example/datasets/ds/resources/res-1/validation-report"
    payload = {"failed_type": "Data Validation", "report": _report()}

    html = build_alert_html("res-1", payload, url)

    assert f'href="{url}"' in html
    assert "full log" in html


def test_no_link_note_without_url() -> None:
    payload = {"failed_type": "Data Validation", "report": _report()}

    assert "full log" not in build_alert_html("res-1", payload)


def test_no_report_renders_no_report_section() -> None:
    html = build_alert_html("res-1", {"message": "boom"}, "https://x/y")

    assert "full log" not in html


def test_subject_includes_organization_name_when_given() -> None:
    payload = {"message": "boom"}

    assert (
        alert_subject(payload, "NESO")
        == "[Alert] NESO Aircan data pipeline failure: Data Ingestion Failure"
    )
    assert alert_subject(payload, "") == alert_subject(payload)
    assert "NESO" not in build_alert_html("res-1", payload)


def test_blank_organization_name_is_treated_as_unset() -> None:
    payload = {"message": "boom"}

    assert alert_subject(payload, "   ") == alert_subject(payload)
    assert alert_subject(payload, "  NESO ").startswith("[Alert] NESO Aircan")


def test_ingestion_failure_links_to_pipeline_page() -> None:
    base = "https://portal.example/datasets/ds/resources/res-1"
    html = build_alert_html(
        "res-1", {"failed_type": "Data Ingestion"}, f"{base}/validation-report"
    )

    assert f'href="{base}/aircan"' in html
    assert "validation-report" not in html


def test_validation_failure_does_not_link_to_pipeline_page() -> None:
    url = "https://portal.example/datasets/ds/resources/res-1/validation-report"
    payload = {"failed_type": "Data Validation", "report": _report()}

    html = build_alert_html("res-1", payload, url)

    assert f'href="{url}"' in html
    assert "/aircan" not in html


def test_ingestion_link_skipped_for_unexpected_url_or_none() -> None:
    payload = {"failed_type": "Data Ingestion"}

    assert "pipeline log" not in build_alert_html("res-1", payload)
    assert "pipeline log" not in build_alert_html("res-1", payload, "https://x/y")
