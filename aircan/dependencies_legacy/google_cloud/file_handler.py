from google.cloud import storage
from google.cloud.exceptions import Conflict
from google.oauth2 import service_account
from datetime import date
import logging as log


def create_file(default_path, filename):
    storage_client = storage.Client()
    bucket_name = default_path
    try:
        storage_client.create_bucket(bucket_name)
    except Conflict:
        pass
    bucket = storage_client.get_bucket(bucket_name)
    log.info('Creating file: ' + filename)
    blob = bucket.blob('dag_conversions/' + filename)
    if not blob.exists():
        blob.upload_from_string("")
    return blob.path


def get_blob(default_path, filename):
    log.info('Getting JSON file path')
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(default_path)
    blob = bucket.blob('dag_conversions/' + filename)
    return blob

def update_file_content_from_string(blob, data):
    log.info("Updating file content")
    blob.upload_from_string(data)