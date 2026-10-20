"""CKAN status update utilities."""

import json
import logging
from typing import Any, Mapping

import requests
from airflow.sdk import BaseHook

logger = logging.getLogger(__name__)


def ckan_status_update(config: Mapping[str, Any], state: str, message: str) -> None:
    """Send a status update to CKAN's aircan_hook.

    Raises if site_url or the resource id is missing, or if the request fails,
    so callers that must reach CKAN (e.g. the final success update) can retry.
    """
    ckan_config = config.get("ckan_config", {})
    resource_dict = config.get("resource", {})

    ckan_url = ckan_config.get("site_url")
    resource_id = resource_dict.get("id")

    if not ckan_url or not resource_id:
        raise ValueError(
            "CKAN status update needs ckan_config.site_url and resource.id "
            f"(state={state}, resource_id={resource_id})"
        )

    site_id = ckan_config.get("site_id", "")

    conn = BaseHook.get_connection(f"{site_id}_api_key")
    ckan_api_key = conn.password if conn else None

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


def ckan_status_update_async(
    config: Mapping[str, Any], state: str, message: str
) -> None:
    """Fire-and-forget CKAN status update.

    Never raises — failures (including missing site_url or resource id) are
    logged as warnings so the caller is not interrupted.
    """
    try:
        ckan_status_update(config, state, message)
    except Exception:
        logger.warning("CKAN status update failed (non-fatal). state=%s", state, exc_info=True)
