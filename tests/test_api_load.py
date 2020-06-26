import unittest

import requests
from mock import Mock, patch

from aircan.lib.api_load import load_resource_via_api


class APITest(unittest.TestCase):

    def test_load_resource_via_api(self):
        with patch.object(requests, 'post') as mock_load_resource_api:
            mocked_res = {
                'response_resource':
                    {'help': 'http://dummy_url/api/3/action/help_show?name=datastore_create',
                     'result': {
                         'method': 'insert',
                         'resource_id': '6f6b1c93-21ff-47ec-a0d6-e5be7c36d082'},
                        'success': True},
                    'success': True
            }
            mock_load_resource_api.return_value.json.return_value = mocked_res

            assert load_resource_via_api('6f6b1c93-21ff-47ec-a0d6-e5be7c36d082',
                                         'test', 'dummy_key', 'dummy_url') == mocked_res

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

            assert load_resource_via_api('notfound_resource_id',
                                         'test', 'dummy_key', 'dummy_url') == mocked_res
