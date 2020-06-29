import unittest

import requests
from mock import Mock, patch

from aircan.lib.api_load import load_resource_via_api

RESOURCE_ID = '6f6b1c93-21ff-47ec-a0d6-e5be7c36d082'
CKAN_URL = 'http://ckan-dev:5000'
CKAN_API_KEY = 'dummy_key'


class LoadResourceAPITest(unittest.TestCase):

    def test_load_resource_via_api(self):
        with patch.object(requests, 'post') as mock_load_resource_api:
            mocked_res = {
                'response_resource':
                    {'help': 'http://dummy_url/api/3/action/help_show?name=datastore_create',
                     'result': {
                         'method': 'insert',
                         'resource_id': RESOURCE_ID},
                        'success': True},
                    'success': True
            }
            mock_load_resource_api.return_value.json.return_value = mocked_res
            records = [{'FID': 192607, 'Mkt-RF': 2.96, 'SMB': -2.3, 'HML': -2.87, 'RF': 0.22}]
            assert load_resource_via_api(RESOURCE_ID,
                                         records, CKAN_API_KEY, CKAN_URL) == mocked_res

    def test_load_resource_via_api_failure(self):
        with patch.object(requests, 'post') as mock_load_resource_api:
            mocked_res = {
                'help': 'http://dummy_url/api/3/action/help_show?name=datastore_create',
                'error': {
                    'resource_id': ['Not found: Resource'],
                    '__type': 'Validation Error'},
                'success': False
            }
            mock_load_resource_api.return_value = mock_load_resource_api_res = Mock()
            mock_load_resource_api_res.status_code == 404
            mock_load_resource_api.return_value.json.return_value = mocked_res

            records = [{'FID': 192607, 'Mkt-RF': 2.96, 'SMB': -2.3, 'HML': -2.87, 'RF': 0.22}]
            assert load_resource_via_api('notfound_resource_id',
                                         records, CKAN_API_KEY, CKAN_URL) == mocked_res
