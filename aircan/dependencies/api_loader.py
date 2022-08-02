import logging
import json
import requests
from urllib.parse import urljoin

from aircan.dependencies.postgres_loader import aircan_status_update
from airflow.models import Variable
from airflow.exceptions import AirflowFailException
from frictionless import Resource
from frictionless.plugins.remote import RemoteControl
from frictionless import describe

from aircan.dependencies.utils import chunky, DatastoreEncoder
from aircan import RequestError

CHUNK = Variable.get('DATASTORE_CHUNK_INSERT_ROWS', 250)

def fetch_and_read(resource_dict, site_url, api_key):
    """
    Fetch and read source type, metadata and schema from
    ckan resource URl.
    """
    logging.info('Fetching resource data from url')
    try:
        resource = describe(path=resource_dict['path'], type="resource")
        status_dict = { 
                'res_id': resource_dict['ckan_resource_id'],
                'state': 'progress',
                'message': 'Fetching datafile from {0}.'.format(resource_dict['path']),
            }
        aircan_status_update(site_url, api_key, status_dict)
        return {'sucess': True, 'resource': resource}

    except Exception as err:
        status_dict = { 
                'res_id': resource_dict['ckan_resource_id'],
                'state': 'error',
                'message': 'Failed to fetch data file.',
                'error': str(err)
            }
        aircan_status_update(site_url, api_key, status_dict)
        return {"success": False, "errors": [err]}

def compare_schema(site_url, ckan_api_key, res_id, schema):
    """
    compare old datastore schema with new schema to know wheather 
    it changed or not.
    """
    logging.info('fetching old data dictionary {0}'.format(res_id))
    try:
        url = urljoin(site_url, '/api/3/action/datastore_info')
        response = requests.post(url,
                        data=json.dumps({'id': res_id }),
                        headers={'Content-Type': 'application/json',
                                'Authorization': ckan_api_key})
        if response.status_code == 200:
            resource_json = response.json()
            old_schema = resource_json['result']['schema'].keys()
            new_schema = [field_name['name'] for field_name in schema]
            if set(old_schema) == set(new_schema):
                return True
        else:
            return False
    except Exception as e: 
        logging.log('Failed to fetch data dictionary for {0}'.format(res_id))
        return True

def delete_datastore_table(data_resource_id, ckan_api_key, ckan_site_url):
    header = {'Authorization': ckan_api_key}
    try:
        response = requests.post(
            urljoin(ckan_site_url, '/api/3/action/datastore_delete'),
            headers=header,
            json={
                "resource_id": data_resource_id,
                'force': True
            }
        )
        if response.status_code == 200 or response.status_code == 404:
            logging.info('Table was deleted successfuly')

            status_dict = { 
                'res_id': data_resource_id,
                'state': 'progress',
                'message': 'New table detected, existing table is being deleted.'
            }
            aircan_status_update(ckan_site_url, ckan_api_key, status_dict)
            return {'success': True, 'message': 'Table deleted successfully.'}
        else:
            raise RequestError(response.json()['error'])
    except Exception as e:
        status_dict = { 
                'res_id': data_resource_id,
                'state': 'error',
                'message': 'Failed to clean up table.'
            }
        aircan_status_update(ckan_site_url, ckan_api_key, status_dict)
        return str(e)

def create_datastore_table(data_resource_id, resource_schema, ckan_api_key, ckan_site_url):
    logging.info('Create Datastore Table method starts')
    # schema field type to postgres field type mapping  
    DATASTORE_TYPE_MAPPING = {
      'integer': 'integer',
      'number': 'numeric',
      'datetime': 'timestamp', 
      'date': 'date',
      'time': 'time', 
      'string': 'text',
      'duration': 'interval',
      'boolean': 'boolean',
      'object': 'jsonb',
      'array': 'array',
      'year': 'text',
      'yearmonth': 'text',
      'geopoint': 'text',
      'geojson': 'jsonb',
      'any': 'text'
    }

    data_dict = dict(
        # resource={'package_id': 'my-first-dataset', 'name' : 'Test1'},
        resource_id=data_resource_id,
        fields=[
            {
                'id': f['name'],
                'type': DATASTORE_TYPE_MAPPING.get(f['type'], 'text'),
            } for f in resource_schema],
        )
    data_dict['records'] = None  # just create an empty table
    data_dict['force'] = True
    try:
        response = requests.post(
            urljoin(ckan_site_url, '/api/3/action/datastore_create'),
            headers={'Authorization': ckan_api_key},
            json=data_dict
        )
        if response.status_code == 200:
            logging.info('Table was created successfuly')
            status_dict = { 
                'res_id': data_resource_id,
                'state': 'progress',
                'message': 'Determined headers and types: {0}'.format(json.dumps(data_dict['fields']))
            }
            aircan_status_update(ckan_site_url, ckan_api_key, status_dict)
            return {'success': True, 'message': 'Table created Successfully.'}
        else:
            raise RequestError(response.json()['error'])
    except Exception as e:
        status_dict = { 
                'res_id': data_resource_id,
                'state': 'error',
                'message': 'Failed to create table in database.'
            }
        aircan_status_update(ckan_site_url, ckan_api_key, status_dict)
        return str(e)

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
                        'state': 'progress',
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
