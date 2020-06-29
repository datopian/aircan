import unittest

import psycopg2
from mock import patch

from aircan.lib.hybrid_load import (_generate_index_name, create_datastore_table, delete_datastore_table, delete_index,
                                    load_csv_to_postgres_via_copy, restore_indexes_and_set_datastore_active)

RESOURCE_ID = '6f6b1c93-21ff-47ec-a0d6-e5be7c36d082'
CKAN_URL = 'http://ckan-dev:5000'
CKAN_API_KEY = 'dummy_key'


class HybridApiTest(unittest.TestCase):

    def test_create_datastore_table(self):
        with patch('requests.post') as mock_create_datastore_table:
            mocked_res = {
                'success': True
            }
            mock_create_datastore_table.return_value.status_code = 200
            mock_create_datastore_table.return_value.json.return_value = \
                mocked_res
            resource_fields = ['f1', 'f2']
            assert create_datastore_table(RESOURCE_ID,
                                          resource_fields,
                                          CKAN_API_KEY,
                                          CKAN_URL) == mocked_res

    def test_failed_create_datastore_table(self):
        with patch('requests.post') as mock_create_datastore_table:
            mocked_res = {
                "success": False,
                "response": {
                    "Error": "Failed to Create Datastore Table."
                }
            }
            mock_create_datastore_table.return_value.status_code = 400
            mock_create_datastore_table.return_value.json.return_value = {
                "Error": "Failed to Create Datastore Table."
            }
            resource_fields = ['f1', 'f2', 'f3']
            assert create_datastore_table(RESOURCE_ID,
                                          resource_fields,
                                          CKAN_API_KEY,
                                          CKAN_URL) == mocked_res

    def test_delete_datastore_table(self):
        with patch('requests.post') as mock_delete_datastore_table:
            mocked_res = {
                'success': True
            }
            mock_delete_datastore_table.return_value.status_code = 200
            mock_delete_datastore_table.return_value.json.return_value = \
                mocked_res
            assert delete_datastore_table(RESOURCE_ID,
                                          CKAN_API_KEY,
                                          CKAN_URL) == mocked_res

    def test_failed_delete_datastore_table(self):
        with patch('requests.post') as mock_delete_datastore_table:
            mocked_res = {
                "response": {
                    "Error": "Failed to Create Datastore Table."
                }
            }
            mock_delete_datastore_table.return_value.status_code = 400
            mock_delete_datastore_table.return_value.json.return_value = \
                mocked_res
            assert delete_datastore_table(RESOURCE_ID,
                                          CKAN_API_KEY,
                                          CKAN_URL) == mocked_res

    def test_delete_index(self):
        data_resource = {
            'ckan_resource_id': RESOURCE_ID,
        }
        mocked_res = {
            'success': True
        }
        with patch('psycopg2.connect') as mock_connect:
            mock_connect.cursor.return_value.execute.return_value. \
                fetchall.return_value = ['res1', 'res2']
            assert delete_index(data_resource, {}, mock_connect) == mocked_res

    def test_failed_delete_index(self):
        data_resource = {
            'ckan_resource_id': RESOURCE_ID,
        }
        error_str = 'invalid data \xc3\xbc'
        mocked_res = {
            'success': False,
            'message': 'Error during deleting index: {}'.format(error_str)
        }
        with patch('psycopg2.connect') as mock_connect:
            mock_connect.cursor.return_value.execute.side_effect = \
                psycopg2.DataError(error_str)
            assert delete_index(data_resource, {}, mock_connect) == mocked_res

    def test__generate_index_name(self):
        ckan_resource_id = RESOURCE_ID
        fields = ['f1', 'f2', 'f3']
        assert _generate_index_name(ckan_resource_id, fields[0], {}, {}) == \
            'efd4327661fd0ece716786e65703aa713a2cb09e'
        assert _generate_index_name(ckan_resource_id, fields[1], {}, {}) == \
            '769f66fe6d7864b7d0de07c1743f66cf40d33e6d'
        assert _generate_index_name(ckan_resource_id, fields[2], {}, {}) == \
            'c2c5303b5ff6588db37764c66aea1e503968f1e4'

    def test_restore_indexes_and_set_datastore_active(self):
        data_resource = {
            'path': './r2.csv',
            'ckan_resource_id': '6f6b1c93-21ff-47ec-a0d6-e5be7c36d082',
            'schema': {
                'fields': [
                    {
                        'name': 'FID',
                        'type': 'text'
                    }
                ]
            }
        }
        error_str = 'relation "6f6b1c93-21ff-47ec-a0d6-e5be7c36d082" does not exist'
        mocked_res = {
            'success': True,

        }
        mocked_res_error = {
            'success': False,
            'message': 'Error during reindexing: {}'.format(error_str)
        }
        with patch('psycopg2.connect') as mock_connect:
            mock_connect.cursor.return_value.execute.return_value = 'success'
            assert restore_indexes_and_set_datastore_active(data_resource,
                                                            {},
                                                            mock_connect) == \
                mocked_res
            mock_connect.cursor.return_value.execute.side_effect = \
                psycopg2.errors.UndefinedTable(error_str)
            assert restore_indexes_and_set_datastore_active(data_resource,
                                                            {},
                                                            mock_connect) == \
                mocked_res_error

    def test_load_csv_to_postgres_via_copy(self):
        data_resource = {
            'path': './r2.csv',
            'ckan_resource_id': '6f6b1c93-21ff-47ec-a0d6-e5be7c36d082',
            'schema': {
                'fields': [
                        {
                            'name': 'FID',
                            'type': 'text'
                        }
                ]
            }
        }
        error_str = 'missing data for column "field2"'
        mocked_res = {
            'success': True
        }
        mocked_res_data_error = {
            'success': False,
            'message': 'Data Error during COPY command: {}'.format(error_str)
        }
        mocked_res_exception = {
            'success': False,
            'message': 'Generic Error during COPY: {}'.format(error_str)
        }
        with patch('psycopg2.connect') as mock_connect:
            mock_connect.copy_expert.return_value = 'success'
            assert load_csv_to_postgres_via_copy(data_resource,
                                                 {},
                                                 mock_connect) == mocked_res
            mock_connect.copy_expert.return_value.side_effect = \
                psycopg2.DataError(error_str)
            assert load_csv_to_postgres_via_copy(data_resource,
                                                 {},
                                                 mock_connect) == mocked_res_data_error
            mock_connect.copy_expert.return_value.side_effect = \
                Exception('Generic Error during COPY: {}'.format(error_str))
            assert load_csv_to_postgres_via_copy(data_resource,
                                                 {},
                                                 mock_connect) == mocked_res_exception
