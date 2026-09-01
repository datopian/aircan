"""CKAN status update utilities."""

import json
import logging
from datetime import datetime, timezone
from typing import Any, Mapping

import requests
from airflow.sdk import BaseHook

logger = logging.getLogger(__name__)


def update_resource_last_modified(
    config: Mapping[str, Any], modified_at: datetime | None = None
) -> None:
    """Stamp the CKAN resource after a successful warehouse write."""
    ckan_config = config.get("ckan_config", {})
    resource_dict = config.get("resource", {})
    ckan_url = ckan_config.get("site_url")
    resource_id = resource_dict.get("id")
    if not ckan_url or not resource_id:
        raise ValueError("CKAN site_url and resource id are required")

    site_id = ckan_config.get("site_id", "")
    conn = BaseHook.get_connection(f"{site_id}_api_key")
    ckan_api_key = conn.password if conn else None
    if not ckan_api_key:
        raise RuntimeError("CKAN API key is required to update the resource")

    timestamp = modified_at or datetime.now(timezone.utc)
    if timestamp.tzinfo is not None:
        timestamp = timestamp.astimezone(timezone.utc).replace(tzinfo=None)
    response = requests.post(
        ckan_url.rstrip("/") + "/api/3/action/resource_patch",
        json={"id": resource_id, "last_modified": timestamp.isoformat()},
        headers={
            "Content-Type": "application/json",
            "Authorization": ckan_api_key,
        },
        timeout=10,
    )
    response.raise_for_status()
    result = response.json()
    if not result.get("success"):
        raise RuntimeError(f"CKAN resource_patch failed: {result.get('error')}")
    logger.info("CKAN resource last_modified updated: resource_id=%s", resource_id)


def ckan_status_update_async(
    config: Mapping[str, Any], state: str, message: str
) -> None:
    """Fire-and-forget CKAN status update.

    Skips silently if required config fields (site_url, resource id) are absent.
    Never raises — failures are logged as warnings so the caller is not interrupted.
    """
    ckan_config = config.get("ckan_config", {})
    resource_dict = config.get("resource", {})

    ckan_url = ckan_config.get("site_url")
    resource_id = resource_dict.get("id")

    site_id = ckan_config.get("site_id", "")

    conn = BaseHook.get_connection(f"{site_id}_api_key")
    ckan_api_key = conn.password if conn else None

    if not ckan_url or not resource_id:
        logger.warning(
            "CKAN status update skipped: missing site_url or resource id "
            "(state=%s, resource_id=%s)",
            state,
            resource_id,
        )
        return

    try:
        url = ckan_url.rstrip("/") + "/api/3/action/aircan_hook"
        headers = {"Content-Type": "application/json"}

        if ckan_api_key:
            headers["Authorization"] = ckan_api_key

        # Determine message type based on state
        message_type = "error" if state == "failed" else "info"

        # Ensure `message` is a string when sending to CKAN. If a dict
        # or other object is passed, serialize it to JSON so CKAN receives
        # a stable string payload.
        payload = {
            "resource_id": resource_id,
            "type": message_type,
            "state": state,
            "message": message if isinstance(message, str) else json.dumps(message),
        }

        resp = requests.post(url, json=payload, headers=headers, timeout=10)
        resp.raise_for_status()
        if message_type == "error":
            logger.error(
                "CKAN status updated: state=%s type=%s message=%s",
                state,
                message_type,
                message,
            )
        else:
            logger.info(
                "CKAN status updated: state=%s type=%s message=%s",
                state,
                message_type,
                message,
            )
    except Exception:
        logger.warning(
            "CKAN status update failed (non-fatal). state=%s", state, exc_info=True
        )
