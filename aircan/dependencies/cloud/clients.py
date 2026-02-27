import logging
from typing import Optional

from google.cloud import bigquery
from google.cloud import storage

from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.providers.google.cloud.hooks.gcs import GCSHook

logger = logging.getLogger(__name__)


def s3_client(
    access_key_id: str,
    secret_access_key: str,
    endpoint_url: Optional[str] = None,
    region_name: Optional[str] = None,
):
    """Create and return a boto3 S3 client.

    endpoint_url: optional custom endpoint for S3-compatible stores
    (MinIO, Wasabi, Cloudflare R2, etc.).
    """
    import boto3

    client = boto3.client(
        "s3",
        endpoint_url=endpoint_url or None,
        aws_access_key_id=access_key_id,
        aws_secret_access_key=secret_access_key,
        region_name=region_name or None,
    )
    if endpoint_url:
        logger.info("S3 client using custom endpoint: %s", endpoint_url)
    else:
        logger.info("S3 client created (access_key=%s, region=%s)", access_key_id, region_name)
    return client


def bq_client(conn_id: str, project_id: Optional[str] = None) -> bigquery.Client:
    """
    Create a BigQuery client from an Airflow connection.

    :param conn_id: Airflow GCP connection ID
    :param project_id: Optional GCP project ID
    :return: google.cloud.bigquery.Client
    """

    hook = BigQueryHook(
        gcp_conn_id=conn_id,
        use_legacy_sql=False,
    )

    return hook.get_client(project_id=project_id)


def gcs_client(conn_id: str) -> storage.Client:
    """
    Create a Google Cloud Storage client from an Airflow connection.

    :param conn_id: Airflow GCP connection ID
    :return: google.cloud.storage.Client
    """
    hook = GCSHook(gcp_conn_id=conn_id)

    return hook.get_conn()