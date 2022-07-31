# Standard library imports
from urllib.parse import urljoin
import datetime
import decimal
import logging
import itertools
# Third-party library imports
import json
import requests
from aircan.dependencies.hybrid_load import aircan_status_update
from frictionless import Resource
from airflow.exceptions import AirflowFailException
from frictionless.plugins.remote import RemoteControl
from airflow.models import Variable

CHUNK = Variable.get('DATASTORE_CHUNK_INSERT_ROWS', 250)

class DatastoreEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime.datetime):
            return obj.isoformat()
        if isinstance(obj, decimal.Decimal):
            return str(obj)

        return json.JSONEncoder.default(self, obj)

def chunky(iterable, n):
    """
    Generates chunks of data that can be loaded into ckan
    :param n: Size of each chunks
    :type n: int
    """
    it = iter([item.to_dict(json=True) for item in iterable])
    item = list(itertools.islice(it, n))
    while item:
        yield item
        item = list(itertools.islice(it, n))


def load_resource_via_api(resource_dict, ckan_api_key, ckan_site_url):
    """
    Push records in CKAN datastore
    """
    logging.info("Loading resource via API lib")
    try:
        # Push data untill records is empty 
        control = RemoteControl(http_timeout=50)
        with Resource(resource_dict['path'], control=control) as resource:
            count = 0
            for i, records in enumerate(chunky(resource.row_stream, int(CHUNK))):
                count += len(records)
                payload = {
                        'resource_id': resource_dict['ckan_resource_id'],
                        'force': True,
                        'records': list(records),
                        'method': 'insert'
                    }
                logging.info('Saving chunk {number}'.format(number=i))
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
                        'message': 'Pushed {0} entries of records.'.format(count)
                    }
                    aircan_status_update(ckan_site_url, ckan_api_key, status_dict)
                else:
                    raise requests.HTTPError('Failed to make request on CKAN API.')
        if count:
            logging.info('Successfully pushed {n} entries to "{res_id}".'.format(
                    n=count, res_id=resource_dict['ckan_resource_id']))
            status_dict = { 
                    'res_id': resource_dict['ckan_resource_id'],
                    'state': 'complete',
                    'message': 'Successfully pushed {n} entries to "{res_id}".'.format(
                    n=count, res_id=resource_dict['ckan_resource_id'])
                }
            aircan_status_update(ckan_site_url, ckan_api_key, status_dict)
            return {'success': True}
            
    except Exception as err:
        status_dict = { 
            'res_id': resource_dict['ckan_resource_id'],
            'state': 'error',
            'message': 'Failed to push data into datastore DB.',
            'error': str(err)
        }
        aircan_status_update(ckan_site_url, ckan_api_key, status_dict)
        raise AirflowFailException("Failed to push the data into CKAN.")
