from __future__ import annotations

import argparse
import csv
import io
import json
import os
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests


DAG_ID = "pipeline_ckan_to_bigquery"


@dataclass(frozen=True)
class TestCase:
    name: str
    purpose: str
    expected: str
    schema: dict[str, Any]
    csv_content: str
    expect_success: bool


@dataclass
class TestResult:
    case: TestCase
    resource_id: str = ""
    dag_run_id: str = ""
    actual_state: str = "not-started"
    actual_behavior: str = ""
    task_states: dict[str, str] | None = None
    log_excerpt: str = ""
    datastore_result: dict[str, Any] | None = None
    passed: bool = False


def field(
    name: str,
    field_type: str,
    *,
    field_format: str | None = None,
    **constraints: Any,
) -> dict[str, Any]:
    value: dict[str, Any] = {"name": name, "type": field_type}
    if field_format is not None:
        value["format"] = field_format
    if constraints:
        value["constraints"] = constraints
    return value


def schema(*fields: dict[str, Any], **metadata: Any) -> dict[str, Any]:
    return {"fields": list(fields), **metadata}


def cases() -> list[TestCase]:
    return [
        TestCase(
            "valid-baseline",
            "Confirm a fully valid descriptor and CSV are ingested.",
            "The DAG succeeds and the datastore contains both rows.",
            schema(
                field("id", "integer", required=True, unique=True, minimum=1),
                field("status", "string", required=True, enum=["active", "inactive"]),
                field("code", "string", required=True, pattern="^[A-Z]{3}$"),
                field("amount", "number", required=True, minimum=0, maximum=100),
            ),
            "id,status,code,amount\n1,active,ABC,10\n2,inactive,XYZ,20\n",
            True,
        ),
        TestCase(
            "required",
            "Reject an empty value for a required field.",
            "Validation fails before upload because status is empty.",
            schema(field("status", "string", required=True)),
            "status\n\n",
            False,
        ),
        TestCase(
            "string-min-length",
            "Reject strings shorter than minLength.",
            "Validation fails because code has fewer than three characters.",
            schema(field("code", "string", minLength=3)),
            "code\nAB\n",
            False,
        ),
        TestCase(
            "string-max-length",
            "Reject strings longer than maxLength.",
            "Validation fails because code has more than three characters.",
            schema(field("code", "string", maxLength=3)),
            "code\nABCD\n",
            False,
        ),
        TestCase(
            "pattern",
            "Reject a string that does not match the regular expression.",
            "Validation fails because code is lowercase.",
            schema(field("code", "string", pattern="^[A-Z]{3}$")),
            "code\nabc\n",
            False,
        ),
        TestCase(
            "string-enum",
            "Reject a string outside its allowed enum.",
            "Validation fails because status is not active or inactive.",
            schema(field("status", "string", enum=["active", "inactive"])),
            "status\npending\n",
            False,
        ),
        TestCase(
            "number-minimum",
            "Reject a number below minimum.",
            "Validation fails because amount is less than zero.",
            schema(field("amount", "number", minimum=0)),
            "amount\n-1\n",
            False,
        ),
        TestCase(
            "number-maximum",
            "Reject a number above maximum.",
            "Validation fails because amount is greater than 100.",
            schema(field("amount", "number", maximum=100)),
            "amount\n101\n",
            False,
        ),
        TestCase(
            "number-enum",
            "Reject a number outside its numeric enum.",
            "Validation fails because amount is not one of 10, 20, or 30.",
            schema(field("amount", "number", enum=[10, 20, 30])),
            "amount\n40\n",
            False,
        ),
        TestCase(
            "integer-type",
            "Reject a value that cannot be parsed as an integer.",
            "Validation fails because id contains a decimal value.",
            schema(field("id", "integer")),
            "id\n1.5\n",
            False,
        ),
        TestCase(
            "number-type",
            "Reject a value that cannot be parsed as a number.",
            "Validation fails because amount is not numeric.",
            schema(field("amount", "number")),
            "amount\nnot-a-number\n",
            False,
        ),
        TestCase(
            "boolean-type",
            "Reject a value outside the supported boolean values.",
            "Validation fails because enabled is neither true nor false.",
            schema(field("enabled", "boolean")),
            "enabled\nmaybe\n",
            False,
        ),
        TestCase(
            "date-minimum",
            "Reject a date earlier than minimum.",
            "Validation fails because the date is before 2020-01-01.",
            schema(field("date", "date", field_format="default", minimum="2020-01-01")),
            "date\n2019-12-31\n",
            False,
        ),
        TestCase(
            "date-maximum",
            "Reject a date later than maximum.",
            "Validation fails because the date is after 2024-12-31.",
            schema(field("date", "date", field_format="default", maximum="2024-12-31")),
            "date\n2025-01-01\n",
            False,
        ),
        TestCase(
            "date-format",
            "Reject a date that does not use the declared format.",
            "Validation fails because the value is not YYYY-MM-DD.",
            schema(field("date", "date", field_format="default")),
            "date\n01/02/2024\n",
            False,
        ),
        TestCase(
            "datetime-format",
            "Reject a datetime that cannot be parsed.",
            "Validation fails because the timestamp is malformed.",
            schema(field("timestamp", "datetime", field_format="default")),
            "timestamp\nnot-a-timestamp\n",
            False,
        ),
        TestCase(
            "email-format",
            "Reject a string that is not an email address.",
            "Validation fails because the email has no valid domain.",
            schema(field("email", "string", field_format="email")),
            "email\ninvalid-email\n",
            False,
        ),
        TestCase(
            "uri-format",
            "Reject a string that is not a URI.",
            "Validation fails because the URL is malformed.",
            schema(field("uri", "string", field_format="uri")),
            "uri\nnot a uri\n",
            False,
        ),
        TestCase(
            "uuid-format",
            "Reject a string that is not a UUID.",
            "Validation fails because the value is not a UUID.",
            schema(field("id", "string", field_format="uuid")),
            "id\nnot-a-uuid\n",
            False,
        ),
        TestCase(
            "unique",
            "Reject duplicate values for a unique field.",
            "Validation fails because id appears twice.",
            schema(field("id", "integer", unique=True)),
            "id\n1\n1\n",
            False,
        ),
        TestCase(
            "multiple-constraints",
            "Report multiple field violations in one validation run.",
            "Validation fails with required, enum, pattern, and maximum violations.",
            schema(
                field("id", "integer", required=True, unique=True, minimum=1),
                field("status", "string", required=True, enum=["active", "inactive"]),
                field("code", "string", required=True, pattern="^[A-Z]{3}$", maxLength=3),
                field("amount", "number", required=True, maximum=100),
            ),
            "id,status,code,amount\n1,pending,ab,101\n",
            False,
        ),
    ]


class Runner:
    def __init__(self, args: argparse.Namespace) -> None:
        self.ckan_url = args.ckan_url.rstrip("/")
        self.datastore_url = args.datastore_url.rstrip("/")
        self.airflow_url = args.airflow_url.rstrip("/")
        self.airflow_user = args.airflow_user
        self.airflow_password = args.airflow_password
        self.timeout = args.timeout
        self.session = requests.Session()
        self.session.headers.update({"Authorization": args.ckan_token})
        self.airflow_session = requests.Session()
        self._authenticate_airflow()
        self.results: list[TestResult] = []

    def _authenticate_airflow(self) -> None:
        """Use Airflow 3's local token endpoint for API diagnostics."""
        try:
            response = self.airflow_session.post(
                f"{self.airflow_url}/auth/token",
                json={"username": self.airflow_user, "password": self.airflow_password},
                timeout=30,
            )
            response.raise_for_status()
            token = response.json().get("access_token")
            if token:
                self.airflow_session.headers.update(
                    {"Authorization": f"Bearer {token}"}
                )
                return
        except requests.RequestException:
            pass
        self.airflow_session.auth = (self.airflow_user, self.airflow_password)

    def ckan(self, method: str, path: str, **kwargs: Any) -> dict[str, Any]:
        response = self.session.request(
            method, f"{self.ckan_url}{path}", timeout=30, **kwargs
        )
        response.raise_for_status()
        payload = response.json()
        if not payload.get("success"):
            raise RuntimeError(json.dumps(payload.get("error", payload)))
        return payload

    def create_target(self) -> tuple[str, str]:
        suffix = str(int(time.time()))
        org = self.ckan(
            "POST",
            "/api/3/action/organization_create",
            data={"name": f"aircan-constraint-matrix-{suffix}", "title": f"Aircan Constraint Matrix {suffix}"},
        )
        package = self.ckan(
            "POST",
            "/api/3/action/package_create",
            data={
                "name": f"aircan-constraint-matrix-{suffix}",
                "title": f"Aircan Constraint Matrix {suffix}",
                "owner_org": org["result"]["id"],
            },
        )
        return package["result"]["id"], package["result"]["name"]

    def upload(self, package_id: str, case: TestCase) -> str:
        response = self.session.post(
            f"{self.ckan_url}/api/3/action/resource_create",
            data={
                "package_id": package_id,
                "name": f"{case.name}.csv",
                "format": "CSV",
                # CKAN's resource contract is singular `schema`.
                "schema": json.dumps(case.schema),
            },
            files={
                "upload": (
                    f"{case.name}.csv",
                    case.csv_content.encode(),
                    "text/csv",
                )
            },
            timeout=30,
        )
        response.raise_for_status()
        payload = response.json()
        if not payload.get("success"):
            raise RuntimeError(json.dumps(payload.get("error", payload)))
        return payload["result"]["id"]

    def status(self, resource_id: str) -> dict[str, Any]:
        return self.ckan(
            "POST",
            "/api/3/action/aircan_status",
            data={"resource_id": resource_id},
        )["result"]

    def airflow_details(self, dag_run_id: str) -> tuple[dict[str, str], str]:
        task_states: dict[str, str] = {}
        log_excerpt = ""
        if not dag_run_id:
            return task_states, log_excerpt

        base = f"{self.airflow_url}/api/v2/dags/{DAG_ID}/dagRuns/{dag_run_id}"
        try:
            response = self.airflow_session.get(f"{base}/taskInstances", timeout=30)
            response.raise_for_status()
            instances = response.json().get("task_instances", [])
            for instance in instances:
                task_id = instance.get("task_id", "unknown")
                state = instance.get("state", "unknown")
                task_states[task_id] = state
                if state == "failed" and not log_excerpt:
                    log_response = self.airflow_session.get(
                        f"{base}/taskInstances/{task_id}/logs/1", timeout=30
                    )
                    if log_response.ok:
                        log_excerpt = log_response.text[-3000:].strip()
        except requests.RequestException as exc:
            log_excerpt = f"Unable to retrieve Airflow task details: {exc}"
        return task_states, log_excerpt

    def datastore_check(self, resource_id: str) -> dict[str, Any] | None:
        try:
            response = self.session.get(
                f"{self.datastore_url}/api/3/action/datastore_search",
                params={"resource_id": resource_id, "limit": 10},
                timeout=60,
            )
            response.raise_for_status()
            payload = response.json()
            return payload.get("result") if payload.get("success") else payload
        except requests.RequestException as exc:
            return {"error": str(exc)}

    def run_case(self, package_id: str, case: TestCase, number: int) -> TestResult:
        result = TestResult(case=case)
        print(f"\n[{number:02d}/{len(cases())}] {case.name}")
        print(f"  What:      {case.purpose}")
        print(f"  Expected:  {case.expected}")

        result.resource_id = self.upload(package_id, case)
        deadline = time.monotonic() + self.timeout
        status: dict[str, Any] = {}
        while time.monotonic() < deadline:
            status = self.status(result.resource_id)
            result.actual_state = status.get("state", "unknown")
            result.dag_run_id = (status.get("dag") or {}).get("dag_run_id", "")
            if result.actual_state in {"success", "failed"}:
                break
            time.sleep(5)

        result.task_states, result.log_excerpt = self.airflow_details(result.dag_run_id)
        if result.case.expect_success and result.actual_state == "success":
            result.datastore_result = self.datastore_check(result.resource_id)
            result.actual_behavior = (
                f"DAG succeeded; datastore returned "
                f"{(result.datastore_result or {}).get('total', 'an unknown number of')} rows."
            )
        elif not result.case.expect_success and result.actual_state == "failed":
            failed_tasks = [name for name, state in (result.task_states or {}).items() if state == "failed"]
            result.actual_behavior = (
                "DAG failed as expected during validation. "
                f"Failed tasks: {', '.join(failed_tasks) or 'not available'}."
            )
        else:
            result.actual_behavior = f"DAG ended in unexpected state: {result.actual_state}."
        result.passed = (result.actual_state == "success") == result.case.expect_success
        print(f"  Actual:    {result.actual_behavior}")
        print(f"  Result:    {'PASS' if result.passed else 'FAIL'}")
        return result

    def run(self) -> tuple[str, list[TestResult]]:
        package_id, package_name = self.create_target()
        print(f"\nPackage: {package_name} ({package_id})")
        for number, case in enumerate(cases(), 1):
            try:
                self.results.append(self.run_case(package_id, case, number))
            except Exception as exc:
                result = TestResult(case=case, actual_state="runner-error")
                result.actual_behavior = f"Test runner failed before completion: {exc}"
                result.passed = False
                self.results.append(result)
                print(f"  Actual:    {result.actual_behavior}")
                print("  Result:    FAIL")
        return package_name, self.results


def render_report(
    path: Path,
    package_name: str,
    results: list[TestResult],
    args: argparse.Namespace,
) -> None:
    passed = sum(result.passed for result in results)
    lines = [
        "# Aircan Constraint Matrix Report",
        "",
        f"- Generated: `{datetime.now(timezone.utc).isoformat()}`",
        f"- CKAN: `{args.ckan_url}`",
        f"- Airflow DAG: `{DAG_ID}`",
        f"- Package: `{package_name}`",
        f"- Summary: **{passed}/{len(results)} passed**",
        "",
        "## Summary",
        "",
        "| # | Case | Expected | Actual state | Result | Resource ID | DAG run ID |",
        "|---:|---|---|---|---|---|---|",
    ]
    for number, result in enumerate(results, 1):
        lines.append(
            f"| {number} | `{result.case.name}` | "
            f"{'success' if result.case.expect_success else 'failed'} | "
            f"`{result.actual_state}` | {'**PASS**' if result.passed else '**FAIL**'} | "
            f"`{result.resource_id}` | `{result.dag_run_id}` |"
        )

    lines.extend(["", "## Detailed Results", ""])
    for number, result in enumerate(results, 1):
        lines.extend(
            [
                f"### {number}. {result.case.name}",
                "",
                f"**What we are testing:** {result.case.purpose}",
                "",
                f"**Expected behavior:** {result.case.expected}",
                "",
                f"**Actual behavior:** {result.actual_behavior}",
                "",
                f"**Result:** {'PASS' if result.passed else 'FAIL'}",
                "",
                "**Schema submitted:**",
                "",
                "```json",
                json.dumps(result.case.schema, indent=2),
                "```",
                "",
                "**CSV submitted:**",
                "",
                "```csv",
                result.case.csv_content.rstrip(),
                "```",
                "",
                "**Airflow task states:**",
                "",
                "```json",
                json.dumps(result.task_states or {}, indent=2),
                "```",
                "",
            ]
        )
        if result.datastore_result is not None:
            lines.extend(
                [
                    "**Datastore result:**",
                    "",
                    "```json",
                    json.dumps(result.datastore_result, indent=2),
                    "```",
                    "",
                ]
            )
        if result.log_excerpt:
            lines.extend(["**Airflow diagnostic log excerpt:**", "", "```text", result.log_excerpt, "```", ""])

    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def parser() -> argparse.ArgumentParser:
    value = os.environ.get
    parser = argparse.ArgumentParser(description="Run the Aircan Table Schema constraint matrix.")
    parser.add_argument("--ckan-url", default=value("CKAN_URL", "http://localhost:5000"))
    parser.add_argument("--datastore-url", default=value("DATASTORE_URL", value("CKAN_URL", "http://localhost:5000")))
    parser.add_argument("--airflow-url", default=value("AIRFLOW_URL", "http://localhost:8082"))
    parser.add_argument("--airflow-user", default=value("AIRFLOW_USERNAME", "airflow"))
    parser.add_argument("--airflow-password", default=value("AIRFLOW_PASSWORD", "airflow"))
    parser.add_argument("--ckan-token", default=value("CKAN_API_TOKEN", ""))
    parser.add_argument("--ckan-token-file", default=value("CKAN_API_TOKEN_FILE", ""))
    parser.add_argument("--timeout", type=int, default=int(value("AIRCAN_TEST_TIMEOUT", "180")))
    parser.add_argument("--report", type=Path, default=Path(value("REPORT_FILE", "reports/aircan-constraint-matrix.md")))
    return parser


def main() -> int:
    args = parser().parse_args()
    if not args.ckan_token and args.ckan_token_file:
        args.ckan_token = Path(args.ckan_token_file).read_text(encoding="utf-8").strip()
    if not args.ckan_token:
        print("Provide --ckan-token or CKAN_API_TOKEN_FILE.", file=sys.stderr)
        return 2

    runner = Runner(args)
    package_name, results = runner.run()
    render_report(args.report, package_name, results, args)
    print(f"\nReport: {args.report}")
    print(f"Summary: {sum(result.passed for result in results)}/{len(results)} passed")
    return 0 if all(result.passed for result in results) else 1


if __name__ == "__main__":
    raise SystemExit(main())
