import json
import unittest

import mock
from google.cloud import bigquery

from aircan.dependencies.google_cloud.bigquery_handler import bq_import_csv


class TestBigQueryImport(unittest.TestCase):
    @classmethod
    def setup_class(self):
        self.mock_bq = mock.patch.object(bigquery, 'Client', autospec=True).start()
        project_id = 'bigquerytest-271707'
        self.dataset = 'nhs_testing'
        table_name = 'test2'
        self.table_id = '%s.%s.' % (project_id, self.dataset) + table_name
        self.table_schema = {
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
                    }
            ]
        }
        self.table_schema_json = json.dumps(self.table_schema)
        self.fqpath = 'gs://datopian-nhs/csv/EPD_201401.csv'

    def test_bq_import(self):
        with mock.patch('requests.post') as mock_bq_import_csv:
            # Validate Success - bq table created
            mocked_res = {
                'success': True,
                'message': 'BigQuery Table created successfully.'
            }
            mock_bq_import_csv.return_value.json.return_value = mocked_res
            self.assertEqual(bq_import_csv(self.table_id,
                                           self.fqpath,
                                           self.table_schema_json), mocked_res)
            # Validate Failure - not exisitng gcs uri
            mocked_res_error = {
                "success": False,
                "message": 'Failed to Create BigQuery Table.'
            }
            mock_bq_import_csv.return_value.json.return_value = \
                mocked_res_error
            self.assertEqual(bq_import_csv(self.table_id,
                                           self.fqpath,
                                           self.table_schema), mocked_res_error)
