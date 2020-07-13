import os
import unittest

import psycopg2
from mock import patch

from aircan.dependencies.hybrid_load import (_generate_index_name,
                                    create_datastore_table,
                                    delete_datastore_table,
                                    delete_index,
                                    load_csv_to_postgres_via_copy,
                                    restore_indexes_and_set_datastore_active)

RESOURCE_ID = '6f6b1c93-21ff-47ec-a0d6-e5be7c36d082'
CKAN_URL = 'http://ckan-dev:5000'
CKAN_API_KEY = 'dummy_key'

THIS_PATH = os.path.dirname(os.path.realpath(__file__))


class HybridApiTest(unittest.TestCase):

    '''
        Test to validate the create datastore table
    '''
    def test_create_datastore_table(self):
        with patch('requests.post') as mock_create_datastore_table:
            # Validate Success
            mocked_res = {
                'message': 'Table created Successfully.',
                'success': True
            }
            # Set the status code as 200
            mock_create_datastore_table.return_value.status_code = 200
            mock_create_datastore_table.return_value.json.return_value = \
                mocked_res
            resource_fields = ['f1', 'f2']
            self.assertEqual(create_datastore_table(RESOURCE_ID,
                                                    resource_fields,
                                                    CKAN_API_KEY,
                                                    CKAN_URL), mocked_res)
            # Validate Failure
            mocked_res_error = {
                "success": False,
                "message": "Failed to Create Datastore Table.",
            }
            # Set the status code as 400
            mock_create_datastore_table.return_value.status_code = 400
            mock_create_datastore_table.return_value.json.return_value = {
                "error": "Failed to Create Datastore Table."
            }
            resource_fields = ['f1', 'f2', 'f3']
            self.assertEqual(create_datastore_table(RESOURCE_ID,
                                                    resource_fields,
                                                    CKAN_API_KEY,
                                                    CKAN_URL),
                             str(mocked_res_error))

    '''
        Test to validate the delete datastore table
    '''
    def test_delete_datastore_table(self):
        with patch('requests.post') as mock_delete_datastore_table:
            # Validate Success
            mocked_res = {
                'success': True,
                'message': 'Table deleted successfully.'
            }
            # Set the status code as 200
            mock_delete_datastore_table.return_value.status_code = 200
            mock_delete_datastore_table.return_value.json.return_value = \
                mocked_res
            self.assertEqual(delete_datastore_table(RESOURCE_ID,
                                                    CKAN_API_KEY,
                                                    CKAN_URL), mocked_res)

            # Validate Failure
            mocked_res_error = {
                "success": False,
                "message": "Failed to Delete Datastore Table.",
            }
            mocked_res = {
                "error": "Failed to Delete Datastore Table."
            }
            # Set the status code as 400
            mock_delete_datastore_table.return_value.status_code = 400
            mock_delete_datastore_table.return_value.json.return_value = \
                mocked_res
            self.assertEqual(delete_datastore_table(RESOURCE_ID,
                                                    CKAN_API_KEY,
                                                    CKAN_URL),
                             str(mocked_res_error))

    '''
        Test to validate the delete index functionality
    '''
    def test_delete_index(self):
        # payload
        data_resource = {
            'ckan_resource_id': RESOURCE_ID,
        }
        # Validate Success
        mocked_res = {
            'success': True
        }
        # Mock the SQL Connection
        with patch('psycopg2.connect') as mock_connect:
            mock_connect.cursor.return_value.execute.return_value. \
                fetchall.return_value = ['res1', 'res2']
            assert delete_index(data_resource, {}, mock_connect) == mocked_res

        # Validate Failure
        error_str = 'invalid data \xc3\xbc'
        mocked_res_error = {
            'success': False,
            'message': 'Error during deleting indexes: {}'.format(error_str)
        }
        with patch('psycopg2.connect') as mock_connect:
            mock_connect.cursor.return_value.execute.side_effect = \
                psycopg2.DataError(error_str)
            self.assertEqual(delete_index(data_resource, {}, mock_connect),
                             str(mocked_res_error))

    '''
        Test to validate the generate index namefunctionality
    '''
    def test__generate_index_name(self):
        ckan_resource_id = RESOURCE_ID
        fields = ['f1', 'f2', 'f3']
        assert _generate_index_name(ckan_resource_id, fields[0], {}, {}) == \
            'efd4327661fd0ece716786e65703aa713a2cb09e'
        assert _generate_index_name(ckan_resource_id, fields[1], {}, {}) == \
            '769f66fe6d7864b7d0de07c1743f66cf40d33e6d'
        assert _generate_index_name(ckan_resource_id, fields[2], {}, {}) == \
            'c2c5303b5ff6588db37764c66aea1e503968f1e4'

    '''
        Test to validate the reindexing functionality
    '''
    def test_restore_indexes_and_set_datastore_active(self):
        # payload
        path = THIS_PATH + '/r2.csv'
        data_resource = {
            'path': path,
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
        with patch('psycopg2.connect') as mock_connect:
            # Validate Success
            mocked_res = {
                'success': True,
                'message': 'Reindex Successful.'

            }

            mock_connect.cursor.return_value.execute.return_value = 'success'
            assert restore_indexes_and_set_datastore_active(data_resource,
                                                            {},
                                                            mock_connect) == \
                mocked_res
            # Validate Exception
            error_str = 'relation "6f6b1c93-21ff-47ec-a0d6-e5be7c36d082" does not exist'
            mocked_res_error = {
                'success': False,
                'message': 'Error during reindexing: {}'.format(error_str)
            }
            mock_connect.cursor.return_value.execute.side_effect = \
                psycopg2.errors.UndefinedTable(error_str)
            self.assertEqual(restore_indexes_and_set_datastore_active(
                                                            data_resource,
                                                            {},
                                                            mock_connect),
                             str(mocked_res_error))

    '''
        Test to Load CSV to Postgres via copy_expert
    '''
    def test_load_csv_to_postgres_via_copy(self):
        # payload
        path = THIS_PATH + '/r2.csv'
        data_resource = {
            'path': path,
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
        with patch('psycopg2.connect') as mock_connect:
            # Validate Success
            mocked_res = {
                'success': True
            }

            mock_connect.cursor.return_value.copy_expert.return_value = 'success'
            assert load_csv_to_postgres_via_copy(data_resource,
                                                 {},
                                                 mock_connect) == mocked_res

            # Validate Exception
            error_str = 'missing data for column "field2"'
            mocked_res_data_error = {
                'success': False,
                'message': 'Data Error during COPY command: {}'.format(error_str)
            }
            mock_connect.cursor.return_value.copy_expert.side_effect = \
                psycopg2.DataError(error_str)
            self.assertEqual(load_csv_to_postgres_via_copy(data_resource,
                                                           {},
                                                           mock_connect),
                             str(mocked_res_data_error))
            mocked_res_exception = {
                'success': False,
                'message': 'Generic Error during COPY: {}'.format(error_str)
            }
            mock_connect.cursor.return_value.copy_expert.side_effect = \
                Exception(format(error_str))
            self.assertEqual(load_csv_to_postgres_via_copy(data_resource,
                                                           {},
                                                           mock_connect),
                             str(mocked_res_exception))
