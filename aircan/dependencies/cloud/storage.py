"""Google Cloud Storage utilities."""

import csv
import logging
import os
import tempfile
from datetime import timedelta
from typing import Optional, Tuple
from urllib.parse import unquote, urlparse

import requests
from google.cloud import storage

logger = logging.getLogger(__name__)

DEFAULT_CHUNK_SIZE = 8 * 1024 * 1024


def is_http_url(value: str) -> bool:
    """Check if value is a valid HTTP/HTTPS URL."""
    try:
        parsed = urlparse(value)
        return parsed.scheme in ("http", "https") and bool(parsed.netloc)
    except Exception:
        return False


def filename_from_resource(resource_dict: dict) -> str:
    """Return the export filename for a resource.

    Uses the filename from the resource URL if available,
    falls back to resource_id.csv.
    """
    resource_id = resource_dict.get("id") or "unknown"
    url = resource_dict.get("url", "")
    name = (
        unquote(os.path.basename(urlparse(url).path))
        if is_http_url(url)
        else os.path.basename(url)
    )
    return name or f"{resource_id}.csv"


def gcs_object_name(source: str, resource_id: str, gcs_prefix: str) -> str:
    """Generate GCS object name from source and resource ID."""
    name = (
        unquote(os.path.basename(urlparse(source).path))
        if is_http_url(source)
        else os.path.basename(source)
    )
    filename = (name or resource_id).replace("/", "_")
    return f"{gcs_prefix}/{resource_id}/{filename}"


def download_http_to_file(
    http_url: str,
    dest_path: str,
    chunk_size: int = DEFAULT_CHUNK_SIZE,
    api_key: Optional[str] = None,
) -> None:
    """Download HTTP URL to a local file path in chunks."""
    headers = {"Authorization": api_key} if api_key else {}
    with requests.get(
        http_url, stream=True, headers=headers, timeout=(10, 1200)
    ) as response:
        response.raise_for_status()
        with open(dest_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=chunk_size):
                if chunk:
                    f.write(chunk)
    logger.info("Downloaded: %s -> %s", http_url, dest_path)


def upload_file_to_gcs(
    storage_client,
    local_path: str,
    bucket_name: str,
    object_name: str,
) -> str:
    """Upload a local file to GCS and return the gs:// URI."""
    blob = storage_client.bucket(bucket_name).blob(object_name)
    blob.upload_from_filename(local_path)
    blob.reload()
    logger.info(
        "Uploaded: %s -> gs://%s/%s (%d bytes)",
        local_path,
        bucket_name,
        object_name,
        blob.size,
    )
    return f"gs://{bucket_name}/{object_name}"


def _add_row_number_to_csv(
    src_path: str,
    dst_path: str,
    column_name: str,
    start: int = 1,
) -> None:
    """Read CSV at src_path, prepend a row number column starting at `start`, write to dst_path."""
    with open(src_path, "r", newline="", encoding="utf-8") as infile, open(
        dst_path, "w", newline="", encoding="utf-8"
    ) as outfile:
        reader = csv.reader(infile)
        writer = csv.writer(outfile)
        for i, row in enumerate(reader):
            if i == 0:
                writer.writerow([column_name] + row)
            else:
                writer.writerow([start + i - 1] + row)
    logger.info(
        "Added row number column '%s' (start=%d): %s -> %s",
        column_name,
        start,
        src_path,
        dst_path,
    )


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


def upload_file_to_s3(
    s3_client,
    local_path: str,
    s3_bucket: str,
    s3_key: str,
) -> str:
    """Upload a local file to S3 and return its s3:// URI."""
    s3_client.upload_file(local_path, s3_bucket, s3_key)
    uri = f"s3://{s3_bucket}/{s3_key}"
    logger.info("Uploaded: %s -> %s", local_path, uri)
    return uri


def upload_gcs_to_s3(
    gcs_client,
    gcs_uri: str,
    s3_client,
    s3_bucket: str,
    s3_key: str,
    delete_after_upload: bool = True,
) -> str:
    """Stream a single GCS object directly to S3.

    gcs_uri: exact gs:// URI, e.g. gs://bucket/prefix/export.csv.
    delete_after_upload: delete the GCS object after a successful upload (default True).
    Returns the s3:// URI.
    """
    parsed = urlparse(gcs_uri)
    gcs_bucket = parsed.netloc
    blob_name = parsed.path.lstrip("/")

    blob = gcs_client.bucket(gcs_bucket).blob(blob_name)
    with blob.open("rb") as f:
        s3_client.upload_fileobj(f, s3_bucket, s3_key)
    uri = f"s3://{s3_bucket}/{s3_key}"
    logger.info("Streamed %s -> %s", gcs_uri, uri)
    if delete_after_upload:
        try:
            blob.delete()
            logger.info("Deleted GCS export object: %s", gcs_uri)
        except Exception as e:
            logger.warning("GCS delete failed for %s: %s", gcs_uri, e)
    return uri


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
