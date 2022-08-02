import datetime
import decimal
import json
import itertools
import logging
import requests
from urllib.parse import urljoin

from airflow.hooks.base_hook import BaseHook
from sqlalchemy import create_engine


def aircan_status_update(site_url, ckan_api_key, status_dict):
    """
    Update aircan run status like pending, error, process, complete 
    on ckan with message.
    """
    logging.info('Updating data loading status')
    try:
        request_data = { 
            'resource_id': status_dict.get('res_id', ''),
            'state': status_dict.get('state', ''),
            'last_updated': str(datetime.datetime.utcnow()),
            'message': status_dict.get('message', ''),
        }

        if status_dict.get('error', False):
            request_data.update({'error': {
                'message' : status_dict.get('error', '')
            }})

        url = urljoin(site_url, '/api/3/action/aircan_status_update')
        response = requests.post(url,
                        data=json.dumps(request_data),
                        headers={'Content-Type': 'application/json',
                                'Authorization': ckan_api_key})
        if response.status_code == 200:
            resource_json = response.json()
            logging.info('Loading status updated successfully in CKAN.')
            return {'success': True}
        else:
            return response.json()
    except Exception as e:
        logging.error('Failed to update status in CKAN. {0}'.format(e))

def string_chunky(iterable, n):
    iterable = iter(iterable)
    count = 0
    group = ''
    while True:
        try:
            group += next(iterable)
            count += 1
            if count % n == 0:
                yield group
                group = ''
        except StopIteration:
            yield group
            break

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

class DatastoreEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime.datetime):
            return obj.isoformat()
        if isinstance(obj, decimal.Decimal):
            return str(obj)

        return json.JSONEncoder.default(self, obj)

def get_connection():
    connection = BaseHook.get_connection('ckan_postgres')
    conn_uri = connection.get_uri()
    logging.info(conn_uri)
    engine = create_engine(conn_uri)
    return engine.raw_connection()

def to_bool(value):
    valid = {'true': True, 't': True, '1': True,
             'false': False, 'f': False, '0': False,
             }   
    if isinstance(value, bool):
        return value
        
    if not isinstance(value, str):
        raise ValueError('invalid literal for boolean. Not a string.')

    lower_value = value.lower()
    if lower_value in valid:
        return valid[lower_value]
    else:
        raise ValueError('invalid literal for boolean: "%s"' % value)