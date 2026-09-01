from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from aircan.dependencies.utils import ckan


def _config():
    return {
        "resource": {"id": "resource-id"},
        "ckan_config": {
            "site_url": "https://ckan.example.test/",
            "site_id": "neso",
        },
    }


def test_update_resource_last_modified(monkeypatch):
    modified_at = datetime(2026, 9, 1, 12, 30, tzinfo=timezone.utc)
    response = Mock()
    response.json.return_value = {"success": True}
    monkeypatch.setattr(
        ckan.BaseHook,
        "get_connection",
        Mock(return_value=SimpleNamespace(password="api-key")),
    )
    post = Mock(return_value=response)
    monkeypatch.setattr(ckan.requests, "post", post)

    ckan.update_resource_last_modified(_config(), modified_at)

    post.assert_called_once_with(
        "https://ckan.example.test/api/3/action/resource_patch",
        json={
            "id": "resource-id",
            "last_modified": "2026-09-01T12:30:00",
        },
        headers={"Content-Type": "application/json", "Authorization": "api-key"},
        timeout=10,
    )
    response.raise_for_status.assert_called_once_with()


def test_update_resource_last_modified_propagates_ckan_failure(monkeypatch):
    monkeypatch.setattr(
        ckan.BaseHook,
        "get_connection",
        Mock(return_value=SimpleNamespace(password="api-key")),
    )
    response = Mock()
    response.raise_for_status.side_effect = RuntimeError("CKAN unavailable")
    monkeypatch.setattr(ckan.requests, "post", Mock(return_value=response))

    with pytest.raises(RuntimeError, match="CKAN unavailable"):
        ckan.update_resource_last_modified(_config())


def test_update_resource_last_modified_rejects_ckan_error_envelope(monkeypatch):
    monkeypatch.setattr(
        ckan.BaseHook,
        "get_connection",
        Mock(return_value=SimpleNamespace(password="api-key")),
    )
    response = Mock()
    response.json.return_value = {
        "success": False,
        "error": {"message": "Resource not found"},
    }
    monkeypatch.setattr(ckan.requests, "post", Mock(return_value=response))

    with pytest.raises(RuntimeError, match="Resource not found"):
        ckan.update_resource_last_modified(_config())
