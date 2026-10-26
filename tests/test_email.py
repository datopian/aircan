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
