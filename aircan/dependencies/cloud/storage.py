"""Google Cloud Storage utilities."""

import codecs
import concurrent.futures
import csv
import io
import itertools
import logging
import os
import json as _json
from queue import Queue
from threading import Thread
from typing import Optional
from urllib.parse import unquote, urlparse

import pyarrow as pa
import pyarrow.parquet as pq

import requests

logger = logging.getLogger(__name__)

DEFAULT_CHUNK_SIZE = 8 * 1024 * 1024


class HttpToGCSStreamer:
    """Streams an HTTP resource directly to GCS with row numbers injected."""

    def __init__(
        self,
        http_url: str,
        storage_client,
        bucket_name: str,
        object_name: str,
        row_number_column: str,
        ckan_http_session: requests.Session,
        fmt: str,
        start: int = 1,
        timeout: tuple = (10, 1200),
        gcs_chunk_size: int = 32 * 1024 * 1024,
        csv_row_chunk: int = 200_000,
        json_row_chunk: int = 200_000,
        pandas_compression: Optional[str] = None,
    ):
        self.http_url = http_url
        self.storage_client = storage_client
        self.bucket_name = bucket_name
        self.object_name = object_name
        self.row_number_column = row_number_column
        self.session = ckan_http_session
        self.fmt = fmt
        self.start = start
        self.timeout = timeout
        self.gcs_chunk_size = gcs_chunk_size
        self.csv_row_chunk = csv_row_chunk
        self.json_row_chunk = json_row_chunk
        self.pandas_compression = pandas_compression

    def _blob(self):
        return self.storage_client.bucket(self.bucket_name).blob(self.object_name)

    def _stream_parquet(self) -> str:
        import fsspec

        headers = dict(self.session.headers)
        with fsspec.open(self.http_url, "rb", headers=headers) as f:
            pf = pq.ParquetFile(f)
            original_schema = pf.schema_arrow
            new_schema = pa.schema(
                [pa.field(self.row_number_column, pa.int64())] + list(original_schema)
            )
            counter = self.start
            total_rows = 0
            with self._blob().open("wb", chunk_size=self.gcs_chunk_size) as gcs_out:
                with pq.ParquetWriter(gcs_out, new_schema) as writer:
                    for batch in pf.iter_batches():
                        num_rows = len(batch)
                        row_nums = pa.array(
                            range(counter, counter + num_rows), type=pa.int64()
                        )
                        new_batch = pa.RecordBatch.from_arrays(
                            [row_nums]
                            + [batch.column(i) for i in range(batch.num_columns)],
                            schema=new_schema,
                        )
                        writer.write_batch(new_batch)
                        counter += num_rows
                        total_rows += num_rows

        logger.info(
            "Streamed HTTP Parquet with row numbers: %s -> gs://%s/%s (rows=%d)",
            self.http_url,
            self.bucket_name,
            self.object_name,
            total_rows,
        )
        return f"gs://{self.bucket_name}/{self.object_name}"

    def _stream_csv(self) -> str:
        sep = "\t" if self.fmt == "tsv" else ","
        queue: Queue = Queue(maxsize=4)
        sentinel = object()
        exc_holder: list = [None]
        rows_written: list = [0]

        def producer():
            try:
                counter = self.start
                with self.session.get(self.http_url, stream=True, timeout=self.timeout) as resp:
                    resp.raise_for_status()
                    resp.raw.decode_content = True
                    text_in = codecs.getreader("utf-8")(resp.raw)
                    reader = csv.reader(text_in, delimiter=sep)

                    buf = io.StringIO()
                    writer = csv.writer(buf, delimiter=sep)
                    header_done = False

                    for row in reader:
                        if not header_done:
                            writer.writerow([self.row_number_column] + row)
                            header_done = True
                        else:
                            writer.writerow([counter] + row)
                            counter += 1

                        if buf.tell() >= self.gcs_chunk_size:
                            queue.put(buf.getvalue().encode("utf-8"))
                            buf.seek(0)
                            buf.truncate(0)

                    if buf.tell() > 0:
                        queue.put(buf.getvalue().encode("utf-8"))

                    rows_written[0] = counter - self.start
            except Exception as e:
                exc_holder[0] = e
            finally:
                queue.put(sentinel)

        Thread(target=producer, daemon=True).start()

        with self._blob().open("wb", chunk_size=self.gcs_chunk_size) as gcs_out:
            while True:
                chunk = queue.get()
                if chunk is sentinel:
                    break
                gcs_out.write(chunk)

        if exc_holder[0]:
            raise exc_holder[0]

        logger.info(
            "Streamed HTTP CSV/TSV with row numbers: %s -> gs://%s/%s (rows=%d)",
            self.http_url,
            self.bucket_name,
            self.object_name,
            rows_written[0],
        )
        return f"gs://{self.bucket_name}/{self.object_name}"

    def _stream_ndjson(self) -> str:
        counter = self.start
        with self.session.get(
            self.http_url, stream=True, timeout=self.timeout
        ) as response:
            response.raise_for_status()
            with self._blob().open("wb", chunk_size=self.gcs_chunk_size) as gcs_out:
                for raw_line in response.iter_lines():
                    if not raw_line:
                        continue
                    obj = _json.loads(raw_line)
                    obj = {self.row_number_column: counter, **obj}
                    gcs_out.write(_json.dumps(obj).encode("utf-8") + b"\n")
                    counter += 1

        logger.info(
            "Streamed HTTP NDJSON with row numbers: %s -> gs://%s/%s (rows=%d)",
            self.http_url,
            self.bucket_name,
            self.object_name,
            counter - self.start,
        )
        return f"gs://{self.bucket_name}/{self.object_name}"

    def _stream_json_array(self) -> str:
        import ijson

        counter = self.start
        with self.session.get(
            self.http_url, stream=True, timeout=self.timeout
        ) as response:
            response.raise_for_status()
            response.raw.decode_content = True
            with self._blob().open("wb", chunk_size=self.gcs_chunk_size) as gcs_out:
                for obj in ijson.items(response.raw, "item"):
                    gcs_out.write(
                        _json.dumps({self.row_number_column: counter, **obj}).encode(
                            "utf-8"
                        )
                        + b"\n"
                    )
                    counter += 1

        logger.info(
            "Streamed HTTP JSON array with row numbers: %s -> gs://%s/%s (rows=%d)",
            self.http_url,
            self.bucket_name,
            self.object_name,
            counter - self.start,
        )
        return f"gs://{self.bucket_name}/{self.object_name}"

    def stream(self) -> str:
        """Dispatch to the correct method based on fmt set at construction."""
        if self.fmt == "parquet":
            return self._stream_parquet()
        if self.fmt in ["ndjson", "jsonl"]:
            return self._stream_ndjson()
        if self.fmt == "json":
            return self._stream_json_array()
        return self._stream_csv()


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


def upload_gcs_to_s3(
    gcs_client,
    gcs_uri: str,
    s3_client,
    s3_bucket: str,
    s3_key: str,
    delete_after_upload: bool = True,
    max_concurrency: int = 10,
    chunk_size: int = 32 * 1024 * 1024,  # 32 MB per part
) -> str:
    """Copy a GCS object to S3 using parallel byte-range reads + S3 multipart upload.

    Opens N simultaneous GCS range-read connections (one per multipart part) and
    uploads each part to S3 concurrently — saturates both GCS read and S3 write
    bandwidth simultaneously.  No intermediate disk.  Memory usage is bounded by
    max_concurrency * chunk_size (default ~320 MB).

    gcs_uri: exact gs:// URI, e.g. gs://bucket/prefix/export.csv.
    delete_after_upload: delete the GCS object after a successful upload.
    max_concurrency: parallel part workers (GCS reads + S3 part uploads).
    chunk_size: bytes per part; must be >= 5 MB (S3 minimum).
    Returns the s3:// URI.
    """
    parsed = urlparse(gcs_uri)
    gcs_bucket_name = parsed.netloc
    blob_name = parsed.path.lstrip("/")

    blob = gcs_client.bucket(gcs_bucket_name).blob(blob_name)
    blob.reload()  # fetch size metadata
    total_size = blob.size

    # Fall back to single-stream for small files (avoids multipart overhead)
    if total_size <= chunk_size:
        with blob.open("rb") as f:
            s3_client.upload_fileobj(f, s3_bucket, s3_key)
        uri = f"s3://{s3_bucket}/{s3_key}"
        logger.info(
            "Uploaded %s -> %s (single-stream, %d bytes)", gcs_uri, uri, total_size
        )
        if delete_after_upload:
            try:
                blob.delete()
            except Exception as e:
                logger.warning("GCS delete failed for %s: %s", gcs_uri, e)
        return uri

    num_parts = (total_size + chunk_size - 1) // chunk_size
    mpu = s3_client.create_multipart_upload(Bucket=s3_bucket, Key=s3_key)
    upload_id = mpu["UploadId"]

    def _upload_part(part_number: int, start: int, end: int):
        # GCS range: start inclusive, end inclusive
        data = blob.download_as_bytes(start=start, end=end)
        resp = s3_client.upload_part(
            Bucket=s3_bucket,
            Key=s3_key,
            UploadId=upload_id,
            PartNumber=part_number,
            Body=data,
        )
        return {"PartNumber": part_number, "ETag": resp["ETag"]}

    parts = []
    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_concurrency) as pool:
            futures = {
                pool.submit(
                    _upload_part,
                    i + 1,
                    i * chunk_size,
                    min((i + 1) * chunk_size - 1, total_size - 1),
                ): i
                + 1
                for i in range(num_parts)
            }
            for future in concurrent.futures.as_completed(futures):
                parts.append(future.result())

        parts.sort(key=lambda p: p["PartNumber"])
        s3_client.complete_multipart_upload(
            Bucket=s3_bucket,
            Key=s3_key,
            UploadId=upload_id,
            MultipartUpload={"Parts": parts},
        )
    except Exception:
        s3_client.abort_multipart_upload(
            Bucket=s3_bucket, Key=s3_key, UploadId=upload_id
        )
        raise

    uri = f"s3://{s3_bucket}/{s3_key}"
    logger.info(
        "Copied %s -> %s (%d bytes, %d parts, concurrency=%d)",
        gcs_uri,
        uri,
        total_size,
        num_parts,
        max_concurrency,
    )
    if delete_after_upload:
        try:
            blob.delete()
            logger.info("Deleted GCS export object: %s", gcs_uri)
        except Exception as e:
            logger.warning("GCS delete failed for %s: %s", gcs_uri, e)
    return uri


def _gcs_compose_recursive(bucket_obj, blobs: list, dest_name: str):
    """Compose blobs into dest_name, handling >32 sources via recursive batching.

    GCS compose supports a maximum of 32 source objects per call.
    For larger lists this function composes in batches and then recursively
    composes the intermediate results.
    Returns the composed blob object.
    """
    MAX_COMPOSE = 32

    if len(blobs) <= MAX_COMPOSE:
        dest_blob = bucket_obj.blob(dest_name)
        dest_blob.compose(blobs)
        return dest_blob

    intermediates = []
    for i in range(0, len(blobs), MAX_COMPOSE):
        chunk = blobs[i : i + MAX_COMPOSE]
        inter_blob = bucket_obj.blob(f"{dest_name}_batch_{i}")
        inter_blob.compose(chunk)
        intermediates.append(inter_blob)

    result = _gcs_compose_recursive(bucket_obj, intermediates, dest_name)

    for b in intermediates:
        try:
            b.delete()
        except Exception as e:
            logger.warning("GCS delete failed for intermediate blob %s: %s", b.name, e)

    return result


def compose_gcs_shards_to_s3(
    gcs_client,
    gcs_uri_wildcard: str,
    header_row: Optional[str],
    s3_client,
    s3_bucket: str,
    s3_key: str,
    delete_after_upload: bool = True,
) -> str:
    """Compose GCS export shards into a single GCS object, optionally prepend header, then upload to S3.

    For CSV: pass header_row as the CSV header string — it is prepended via GCS compose.
    For JSON (NDJSON): pass header_row=None — shards are composed directly (NDJSON concat is valid).
    Returns the s3:// URI.
    """
    parsed = urlparse(gcs_uri_wildcard)
    gcs_bucket_name = parsed.netloc
    blob_path = parsed.path.lstrip("/")
    prefix = blob_path.split("*")[0]

    bucket_obj = gcs_client.bucket(gcs_bucket_name)

    # Derive temp blob names early so we can exclude them from the shard list.
    # This prevents leftover blobs from a previous failed run being included in
    # compose on retry, which would silently corrupt the output.
    _, _ext = os.path.splitext(s3_key)
    _ext = _ext or ".csv"
    header_blob_name = f"{prefix}_header{_ext}"
    composed_blob_name = f"{prefix}_composed{_ext}"
    _temp_names = {header_blob_name, composed_blob_name}

    blobs = sorted(
        [b for b in bucket_obj.list_blobs(prefix=prefix) if b.name not in _temp_names],
        key=lambda b: b.name,
    )

    if not blobs:
        raise FileNotFoundError(f"No GCS objects found matching {gcs_uri_wildcard}")

    logger.info(
        "Composing %d GCS shard(s)%s for %s -> s3://%s/%s",
        len(blobs),
        " with header" if header_row else "",
        gcs_uri_wildcard,
        s3_bucket,
        s3_key,
    )

    header_blob = None
    composed_blob = None
    upload_succeeded = False
    try:
        if header_row:
            # Upload header as a tiny GCS blob (CSV only)
            header_blob = bucket_obj.blob(header_blob_name)
            header_blob.upload_from_string(header_row.encode("utf-8"))
            compose_sources = [header_blob] + blobs
        else:
            compose_sources = blobs

        composed_blob = _gcs_compose_recursive(
            bucket_obj, compose_sources, composed_blob_name
        )
        composed_uri = f"gs://{gcs_bucket_name}/{composed_blob.name}"
        logger.info("GCS compose complete: %s", composed_uri)

        s3_uri = upload_gcs_to_s3(
            gcs_client,
            composed_uri,
            s3_client,
            s3_bucket,
            s3_key,
            delete_after_upload=True,  # deletes composed_blob on success
        )
        upload_succeeded = True
    finally:
        if header_blob:
            try:
                header_blob.delete()
            except Exception as e:
                logger.warning(
                    "GCS delete failed for header blob %s: %s", header_blob_name, e
                )
        # If upload failed, composed_blob was not deleted by upload_gcs_to_s3 — clean it up
        # so retries start clean and don't accumulate stale GCS objects.
        if not upload_succeeded and composed_blob is not None:
            try:
                composed_blob.delete()
                logger.info(
                    "Cleaned up composed blob after failed upload: %s",
                    composed_blob_name,
                )
            except Exception as e:
                logger.warning(
                    "GCS delete failed for composed blob %s: %s", composed_blob_name, e
                )

    if delete_after_upload:
        for blob in blobs:
            try:
                blob.delete()
                logger.info("Deleted GCS shard: gs://%s/%s", gcs_bucket_name, blob.name)
            except Exception as e:
                logger.warning(
                    "GCS delete failed for gs://%s/%s: %s",
                    gcs_bucket_name,
                    blob.name,
                    e,
                )

    return s3_uri


def upload_parquet_shards_to_s3(
    gcs_client,
    gcs_uri_wildcard: str,
    s3_client,
    s3_bucket: str,
    s3_key: str,
    delete_after_upload: bool = True,
) -> str:
    """Upload Parquet shards from GCS to S3 individually.

    Parquet files cannot be concatenated (each is a self-contained binary file
    with header/footer metadata), so each GCS shard is uploaded as a separate
    S3 object.

    Single shard  → uses s3_key as-is.
    Multiple shards → uses s3_key stem with zero-padded suffix (_001, _002, …).

    Returns the s3:// URI of the first uploaded object.
    """
    parsed = urlparse(gcs_uri_wildcard)
    gcs_bucket_name = parsed.netloc
    blob_path = parsed.path.lstrip("/")
    prefix = blob_path.split("*")[0]

    bucket_obj = gcs_client.bucket(gcs_bucket_name)
    blobs = sorted(bucket_obj.list_blobs(prefix=prefix), key=lambda b: b.name)

    if not blobs:
        raise FileNotFoundError(f"No GCS objects found matching {gcs_uri_wildcard}")

    logger.info(
        "Uploading %d Parquet shard(s) to s3://%s/%s",
        len(blobs),
        s3_bucket,
        s3_key,
    )

    stem, ext = os.path.splitext(s3_key)
    first_uri = None
    for i, blob in enumerate(blobs):
        shard_key = s3_key if len(blobs) == 1 else f"{stem}_{i + 1:03d}{ext}"
        gcs_uri = f"gs://{gcs_bucket_name}/{blob.name}"
        uri = upload_gcs_to_s3(
            gcs_client,
            gcs_uri,
            s3_client,
            s3_bucket,
            shard_key,
            delete_after_upload=delete_after_upload,
        )
        if first_uri is None:
            first_uri = uri

    return first_uri
