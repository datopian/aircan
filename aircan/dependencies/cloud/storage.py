"""Google Cloud Storage utilities."""

import logging
import os
import tempfile
from datetime import timedelta
from typing import Tuple
from urllib.parse import unquote, urlparse

import requests
from google.cloud import storage

logger = logging.getLogger(__name__)


def is_http_url(value: str) -> bool:
    """Check if value is a valid HTTP/HTTPS URL."""
    try:
        parsed = urlparse(value)
        return parsed.scheme in ("http", "https") and bool(parsed.netloc)
    except Exception:
        return False


def filename_from_source(source: str) -> str:
    """Extract filename from source URL or path."""
    if is_http_url(source):
        return unquote(os.path.basename(urlparse(source).path)) or "input.csv"
    return os.path.basename(source) or "input.csv"


def gcs_object_name(source: str, resource_id: str, gcs_prefix: str) -> str:
    """Generate GCS object name from source and resource ID."""
    filename = filename_from_source(source).replace("/", "_")
    return f"{gcs_prefix}/{resource_id}/{filename}"


def stream_http_to_gcs(
    storage_client,
    http_url,
    bucket_name,
    object_name,
    chunk_size=8 * 1024 * 1024,
    api_key=None,
):
    """Stream HTTP URL directly to GCS bucket."""
    blob = storage_client.bucket(bucket_name).blob(object_name)
    
    if api_key:
        headers = {"Authorization": f"{api_key}"}
    else:
        headers = {}

    with tempfile.NamedTemporaryFile() as tmp:
        with requests.get(
            http_url, stream=True, headers=headers, timeout=(10, 1200)
        ) as response:
            response.raise_for_status()
            for chunk in response.iter_content(chunk_size=chunk_size):
                if chunk:
                    tmp.write(chunk)
        tmp.flush()
        blob.upload_from_filename(tmp.name)

    blob.reload()
    logger.info(
        "Upload complete: gs://%s/%s (size=%d bytes)",
        bucket_name,
        object_name,
        blob.size,
    )
    return f"gs://{bucket_name}/{object_name}"


def upload_source_to_gcs(
    storage_client: storage.Client,
    source: str,
    resource_id: str,
    bucket: str,
    chunk_size: int,
    ckan_api_key: str = None,
) -> Tuple[str, str, str]:
    """Upload source file to GCS and return GCS URI with compression info."""
    s = source.lower()
    compression = "GZIP" if s.endswith(".gz") or s.endswith(".csv.gz") else "NONE"
    object_name = gcs_object_name(source, resource_id, bucket)
    logger.info("Uploading -> GCS: gs://%s/%s", bucket, object_name)
    gcs_uri = stream_http_to_gcs(
        storage_client, source, bucket, object_name, chunk_size, ckan_api_key
    )
    return gcs_uri, compression, object_name


def delete_gcs_object(
    storage_client: storage.Client, bucket: str, object_name: str
) -> None:
    """Delete object from GCS bucket."""
    blob = storage_client.bucket(bucket).blob(object_name)
    blob.delete()
    logger.info("Deleted GCS object: gs://%s/%s", bucket, object_name)


def gcs_signed_url(
    storage_client: storage.Client,
    bucket: str,
    object_name: str,
    expiration_seconds: int,
) -> str:
    """Generate a signed URL for GCS object."""
    blob = storage_client.bucket(bucket).blob(object_name)
    return blob.generate_signed_url(
        version="v4",
        expiration=timedelta(seconds=expiration_seconds),
        method="GET",
    )
