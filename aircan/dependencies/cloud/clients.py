import logging
from typing import Optional

from google.cloud import bigquery
from google.cloud import storage

from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.providers.google.cloud.hooks.gcs import GCSHook

logger = logging.getLogger(__name__)


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