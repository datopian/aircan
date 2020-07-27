import json
import os
import unittest

import psycopg2
from mock import patch

from aircan.dependencies.google_cloud.bigquery_handler import bq_import_csv, bq_schema_from_table_schema

TABLE_ID = 'bigquerytest-271707.nhs_test.r3'
GCS_PATH = 'gs://europe-west2-aircan-airflow-1f8e0bdc-bucket/dags/aircan/dags/r3.csv'
GCS_PATH_NOT_EXISTS = 'gs://europe-west2-aircan-airflow-1f8e0bdc-bucket/dags/aircan/dags/r33333.csv'
GCS_PATH_FILE_WITH_COLUMN_NOT_IN_SCHEMA = 'gs://europe-west2-aircan-airflow-1f8e0bdc-bucket/dags/aircan/dags/r3_with_redundant_field.csv'
GCS_PATH_FILE_WITH_COLUMN_NAME_CHANGED = 'gs://europe-west2-aircan-airflow-1f8e0bdc-bucket/dags/aircan/dags/r3_with_column_name_changed.csv'
TABLE_SCHEMA = {
        "fields": [
            {
                "name": "FID",
                "title": "FID",
                "type": "number",
                "description": "FID`"
            },
            {
                "name": "MktRF",
                "title": "MktRF",
                "type": "number",
                "description": "MktRF`"
            },
            {
                "name": "SMB",
                "title": "SMB",
                "type": "number",
                "description": "SMB`"
            },
            {
                "name": "HML",
                "title": "HML",
                "type": "number",
                "description": "HML`"
            },
            {
                "name": "RF",
                "title": "RF",
                "type": "number",
                "description": "RF`"
            },
            {
                "name": "RFFF",
                "title": "RFFF",
                "type": "number",
                "description": "RFFF`"
            }
        ]
    }
TABLE_SCHEMA_WITH_REDUNDANT_FIELD_NOT_EXISITNG_IN_FILE = {
        "fields": [
            {
                "name": "FID",
                "title": "FID",
                "type": "number",
                "description": "FID`"
            },
            {
                "name": "MktRF",
                "title": "MktRF",
                "type": "number",
                "description": "MktRF`"
            },
            {
                "name": "SMB",
                "title": "SMB",
                "type": "number",
                "description": "SMB`"
            },
            {
                "name": "HML",
                "title": "HML",
                "type": "number",
                "description": "HML`"
            },
            {
                "name": "RF",
                "title": "RF",
                "type": "number",
                "description": "RF`"
            },
            {
                "name": "RFFF",
                "title": "RFFF",
                "type": "number",
                "description": "RFFF`"
            },
            { # the field doesn't really exist in file 
                "name": "FID2",
                "title": "FID2",
                "type": "number",
                "description": "FID2"
            }
        ]
    }
TABLE_SCHEMA_UPDATED = {
        "fields": [
            {
                "name": "FID2",
                "title": "FID2",
                "type": "string",
                "description": "FID2"
            },
            {
                "name": "MktRF",
                "title": "MktRF",
                "type": "number",
                "description": "MktRF`"
            },
            {
                "name": "SMB2",
                "title": "SMB2",
                "type": "string",
                "description": "SMB2"
            },
            {
                "name": "HML",
                "title": "HML",
                "type": "number",
                "description": "HML`"
            },
            {
                "name": "RF2",
                "title": "RF2",
                "type": "string",
                "description": "RF2"
            },
            {
                "name": "RFFF",
                "title": "RFFF",
                "type": "number",
                "description": "RFFF`"
            }
        ]
    }
TABLE_SCHEMA_JSON = json.dumps(TABLE_SCHEMA)
TABLE_SCHEMA_JSON_WRONG = json.dumps(TABLE_SCHEMA_WITH_REDUNDANT_FIELD_NOT_EXISITNG_IN_FILE)
TABLE_SCHEMA_JSON_UPDATED = json.dumps(TABLE_SCHEMA_UPDATED)

def script_path():
        return os.path.dirname(os.path.abspath(__file__))
        
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = script_path() + '/' + 'google.json'

class CkanResourceImportToBQ(unittest.TestCase):

    def test_bq_import_csv(self):
        with patch('requests.post') as mock_bq_import_csv:
            
            # Validate Success - bq table created
            mocked_res = {
                'message': 'Loaded 1125 rows',
                'success': True
            }
            mock_bq_import_csv.return_value.json.return_value = \
                mocked_res
            self.assertEqual(bq_import_csv(TABLE_ID,
                                           GCS_PATH,
                                           TABLE_SCHEMA_JSON), mocked_res)
            
            # Validate Failure - not exisitng gcs uri
            mocked_res_error = {
                "success": False,
                "message": '404 Not found: URI ' \
                            'gs://europe-west2-aircan-airflow-1f8e0bdc-bucket/dags/aircan/dags/r33333.csv',
            }
            mock_bq_import_csv.return_value.json.return_value = \
                mocked_res_error
            self.assertEqual(bq_import_csv(TABLE_ID,
                                           GCS_PATH_NOT_EXISTS,
                                           TABLE_SCHEMA_JSON), mocked_res_error)

            # Validate Failure - wrong table schema(including field not presented in file)
            mocked_res_error_schema = {
                "success": False,
                "message": '400 Error while reading data, error message: CSV table ' \
                'encountered too many errors, giving up. Rows: 1; errors: 1. ' \
                'Please look into the errors[] collection for more details.'
            }
            mock_bq_import_csv.return_value.json.return_value = \
                mocked_res_error_schema
            self.assertEqual(bq_import_csv(TABLE_ID,
                                           GCS_PATH,
                                           TABLE_SCHEMA_JSON_WRONG), mocked_res_error_schema)
            # Validate Failure - wrong gcs file (including column not presented in schema)
            mocked_res_error_file = {
                "success": False,
                "message": '400 Error while reading data, error message: CSV table ' \
                'encountered too many errors, giving up. Rows: 1; errors: 1. ' \
                'Please look into the errors[] collection for more details.'
            }
            mock_bq_import_csv.return_value.json.return_value = \
                mocked_res_error_file
            self.assertEqual(bq_import_csv(TABLE_ID,
                                           GCS_PATH_FILE_WITH_COLUMN_NOT_IN_SCHEMA,
                                           TABLE_SCHEMA_JSON), mocked_res_error_file)
            # Validate Success - bq table updated/truncated (column name changed + data in that column changed)
            mocked_res_update_file = {
                'message': 'Loaded 1125 rows',
                'success': True
            }
            mock_bq_import_csv.return_value.json.return_value = \
                mocked_res_update_file
            self.assertEqual(bq_import_csv(TABLE_ID,
                                           GCS_PATH_FILE_WITH_COLUMN_NAME_CHANGED,
                                           TABLE_SCHEMA_JSON), mocked_res_update_file)
            # Validate Success - bq table updated/truncated (table schema updated)
            mocked_res_update_schema = {
                'message': 'Loaded 1125 rows',
                'success': True
            }
            mock_bq_import_csv.return_value.json.return_value = \
                mocked_res_update_schema
            self.assertEqual(bq_import_csv(TABLE_ID,
                                           GCS_PATH_FILE_WITH_COLUMN_NAME_CHANGED,
                                           TABLE_SCHEMA_JSON_UPDATED), mocked_res_update_schema)

    