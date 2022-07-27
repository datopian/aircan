# Standard library imports
import hashlib
import logging
import datetime

# Third-party library imports
import json
import psycopg2
import requests
from urllib.parse import urljoin
from frictionless import describe

from aircan import RequestError, DatabaseError

# =============== API ACCESS ===============

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
            return {'success': True, 'response_resource': resource_json}
        else:
            return response.json()
    except Exception as e:
        return {"success": False, "errors": [e]}

def fetch_and_read(resource_dict, site_url, api_key):
    """
    Fetch and read source type, metadata and schema from
    ckan resource URl.
    """
    logging.info('Fetching resource data from url')
    try:
        resource = describe(resource_dict['path'], type="resource")
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

## Fetch previous data dictionary and compare
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


# =============== POTSGRES ACCESS ===============


def load_csv(data_resource, config={}, connection=None):
    '''Loads a CSV into DataStore. Does not create the indexes.'''
    # prepare_good_csv() # TODO come back to this later ..
    # This should work over API (not inside CKAN process) or do direct ...

    delete_datastore_table(data_resource, config=config)
    create_datastore_table(data_resource, config=config)
    delete_index(data_resource, config=config, connection=connection)
    load_csv_to_postgres_via_copy(
        data_resource, config=config, connection=connection)
    restore_indexes_and_set_datastore_active(
        data_resource, config=config, connection=connection)


def delete_index(data_resource, config={}, connection=None):
    sql_drop_index = u'DROP INDEX "{0}" CASCADE'
    sql_get_index_string = """
        SELECT indexname
            FROM pg_indexes
            WHERE schemaname = 'public' AND tablename='{res_id}';
        """.format(res_id=data_resource['ckan_resource_id'])

    try:
        cur = connection.cursor()
        try:
            query_obj = cur.execute(sql_get_index_string)
            if query_obj is not None:
                indexes_to_drop = cur.execute(sql_get_index_string).fetchall()
                for index in indexes_to_drop:
                    cur.execute(sql_drop_index.format(index))
            return {'success': True}
        except psycopg2.DataError as e:
            error_str = str(e)
            logging.warning(error_str)
            raise DatabaseError(f"Error during deleting indexes: {error_str}")
        except Exception as e:
            raise DatabaseError(f"Error during deleting indexes: {error_str}")
        finally:
            cur.close()
    except Exception as e:
        return str(e)
    finally:
        connection.commit()

def restore_indexes_and_set_datastore_active(data_resource,
                                             config={},
                                             connection=None):
    cur = connection.cursor()
    # How are we going to get primary keys, schema?
    primary_key = data_resource['schema'].get('primary_key', '')

    sql_index_string = u'CREATE {unique} INDEX "{name}" ON "{res_id}" ({flds})'
    sql_index_strings = []
    fields = data_resource['schema'].get('fields', '')
    json_fields = [x['id'] for x in fields if x['type'] == 'nested']

    indexes = [primary_key]

    for index in indexes:
        fields_string = u', '.join(
            ['(("{0}").json::text)'.format(field['name'])
                if field in json_fields else
                '"%s"' % field['name']
                for field in fields])
        sql_index_strings.append(sql_index_string.format(
            res_id=data_resource['ckan_resource_id'],
            unique='unique' if index == primary_key else '',
            name=_generate_index_name(
                data_resource['ckan_resource_id'], fields_string),
            flds=fields_string))

    # Not sure what this doess
    sql_index_strings = map(lambda x: x.replace('%', '%%'), sql_index_strings)
    try:
        try:
            for sql_index_string in sql_index_strings:
                cur.execute(sql_index_string)
        except psycopg2.errors.UndefinedTable as e:
                error_str = str(e)
                logging.warning(error_str)
                raise DatabaseError(f"Error during reindexing: {error_str}")
    except Exception as e:
        return str(e)
    return {'success': True, 'message': 'Reindex Successful.'}


def _generate_index_name(resource_id, field, config={}, connection=None):
    value = (resource_id + field).encode('utf-8')
    return hashlib.sha1(value).hexdigest()




def load_csv_to_postgres_via_copy(data_resource, config={}, connection=None):
    '''
    Options for loading into postgres:

    1. \\copy - can't use as that is a psql meta-command and not accessible
    via psycopg2
    2. COPY - requires the db user to have superuser privileges.
    This is dangerous. It is also not available on AWS, for example.
    3. pgloader method? - as described in its docs:
    Note that while the COPY command is restricted to read either from its
    standard input or from a local file on the server's file system, the
    command line tool psql implements a \\copy command that knows how to
    stream a file local to the client over the network and into the PostgreSQL
    server, using the same protocol as pgloader uses.
    4. COPY FROM STDIN - not quite as fast as COPY from a file,
    but avoids the superuser issue. <-- picked
    '''
    try:
        cur = connection.cursor()
        try:
            with open(data_resource['path'], 'rb') as f:
                # Can't use :param for table name because params are only
                # For filter values that are single quoted.
                try:
                    cur.copy_expert(
                        "COPY \"{resource_id}\" "
                        "FROM STDIN "
                        "WITH (DELIMITER '{delimiter}', FORMAT csv, HEADER 1, "
                        "      ENCODING '{encoding}');"
                        .format(
                            resource_id=data_resource['ckan_resource_id'],
                            # This is a bit risky cause it may not have
                            # delimiter in schema and not be ","
                            delimiter=data_resource['schema'].get(
                                'delimiter', ','),
                            encoding='UTF8',
                            ),
                        f)
                except psycopg2.DataError as e:
                    # E is a str but with foreign chars e.g.
                    # 'extra data: "paul,pa\xc3\xbcl"\n'
                    # But logging and exceptions need a normal (7 bit) str
                    error_str = str(e)
                    logging.warning(error_str)
                    raise DatabaseError(f"Data Error during COPY command: {error_str}")
                except Exception as e:
                    raise DatabaseError(f"Generic Error during COPY: {e}")
                finally:
                    cur.close()
        except Exception as e:
            return str(e)
        finally:
            cur.close()
    finally:
        connection.commit()

    return {'success': True}
