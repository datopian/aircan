# Standard library imports
from urllib.parse import urljoin
import hashlib
import datetime
import decimal
import logging

# Third-party library imports
import json
import requests
from aircan.dependencies.hybrid_load import aircan_status_update
from frictionless import Resource, Layout

class DatastoreEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime.datetime):
            return obj.isoformat()
        if isinstance(obj, decimal.Decimal):
            return str(obj)

        return json.JSONEncoder.default(self, obj)


def load_resource_via_api(resource_dict, ckan_api_key, ckan_site_url):
    logging.info("Loading resource via API lib")
    try:
        offset_rows = 0
        row_chunk = 16384

        # Push data untill records is empty 
        while True:
            if offset_rows == 0:
                layout = Layout(limit_rows=row_chunk)
            else:
                layout = Layout(limit_rows=row_chunk, offset_rows=offset_rows)
            with Resource(resource_dict['path'], layout=layout) as resource:
                    records = [row.to_dict(json=True) for row in resource.row_stream]
                    if not records:
                        status_dict = { 
                                'res_id': resource_dict['ckan_resource_id'],
                                'state': 'complete',
                                'message': 'Successfully pushed {0} entries to "{1}"'
                                        .format(offset_rows,resource_dict['ckan_resource_id'])
                            }
                        aircan_status_update(ckan_site_url, ckan_api_key, status_dict)
                        return {'success': True}
                    else:
                        offset_rows += len(records)
                        payload = {
                            'resource_id': resource_dict['ckan_resource_id'],
                            'force': True,
                            'records': records,
                            'method': 'insert'
                        }
                        url = urljoin(ckan_site_url, '/api/3/action/datastore_upsert')
                        response = requests.post(url,
                                    data=json.dumps(payload, cls=DatastoreEncoder),
                                    headers={'Content-Type': 'application/json',
                                            'Authorization': ckan_api_key})
                        response.raise_for_status()
                        if response.status_code == 200:
                            status_dict = { 
                                'res_id': resource_dict['ckan_resource_id'],
                                'state': 'complete',
                                'message': 'Pushed {0} entries of records.'.format(offset_rows)
                            }
                            aircan_status_update(ckan_site_url, ckan_api_key, status_dict)
                        else:
                            raise requests.HTTPError('Failed to make request on CKAN API.')
    except Exception as err:
        status_dict = { 
                'res_id': resource_dict['ckan_resource_id'],
                'state': 'error',
                'message': 'Failed to push data into datastore DB.',
                'error': str(err)
            }
        aircan_status_update(ckan_site_url, ckan_api_key, status_dict)
        return {"success": False}