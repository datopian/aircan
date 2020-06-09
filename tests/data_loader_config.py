import os

from dotenv import load_dotenv
from sqlalchemy import create_engine

load_dotenv()


def get_config():
    config = {}
    for env in os.environ:
        config[env] = os.environ[env]
    return config


def get_connection():
    engine = create_engine(get_config()['CKAN_DATASTORE_WRITE_URL'])
    return engine.raw_connection()
