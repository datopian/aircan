import io
import hashlib
import logging
import psycopg2
from urllib.request import urlopen 

from aircan import RequestError, DatabaseError
from aircan.dependencies.utils import AirflowCKANException, string_chunky, aircan_status_update
from frictionless import Resource
from frictionless.plugins.remote import RemoteControl


POSTGRES_LOAD_CHUNK = 50000

def delete_index(data_resource, connection=None):
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

def restore_indexes_and_set_datastore_active(resource_dict, schema,
                                             connection=None):
    cur = connection.cursor()
    # How are we going to get primary keys, schema?
    primary_key = schema.get('primary_key', '')

    sql_index_string = u'CREATE {unique} INDEX "{name}" ON "{res_id}" ({flds})'
    sql_index_strings = []
    fields =schema.get('fields', '')
    json_fields = [x['id'] for x in fields if x['type'] == 'nested']

    indexes = [primary_key]

    for index in indexes:
        fields_string = u', '.join(
            ['(("{0}").json::text)'.format(field['name'])
                if field in json_fields else
                '"%s"' % field['name']
                for field in fields])
        sql_index_strings.append(sql_index_string.format(
            res_id=resource_dict['ckan_resource_id'],
            unique='unique' if index == primary_key else '',
            name=_generate_index_name(
                resource_dict['ckan_resource_id'], fields_string),
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

def load_csv_to_postgres_via_copy(connection=None, **kwargs):
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
        schema = kwargs['schema']
        resource_dict = kwargs['resource_dict']
        site_url = kwargs['site_url']
        api_key = kwargs['api_key']
        fields = schema.get('fields', [])
        column_names = ', '.join(['"{0}"'.format(field['name']) for field in fields])
        cur = connection.cursor()
        try:
            control = RemoteControl(http_timeout=50)
            with Resource(path=resource_dict['path'], control=control) as resource:    
                for i, records in enumerate(string_chunky(resource.text_stream, int(POSTGRES_LOAD_CHUNK))):
                    f = io.StringIO(records)
                        # Can't use :param for table name because params are only
                        # For filter values that are single quoted.
                    try:
                        cur.copy_expert(
                            "COPY \"{resource_id}\" ({column_names}) "
                            "FROM STDIN "
                            "WITH (DELIMITER '{delimiter}', FORMAT csv, HEADER 1, "
                            "      ENCODING '{encoding}');"
                            .format(
                                resource_id = resource_dict['ckan_resource_id'],
                                column_names = column_names,
                                delimiter = ',',
                                encoding = 'UTF8',
                            ),
                            f)
                        logging.info('Pushed {count} chunk of records.'.format(count=i+1))
                        status_dict = {
                            'res_id': resource_dict['ckan_resource_id'],
                            'state': 'progress',
                            'message':'Pushed {count} chunk of records.'.format(count=i+1)
                            }
                        
                        aircan_status_update(site_url, api_key, status_dict)
                    except psycopg2.DataError as err:
                        # E is a str but with foreign chars e.g.
                        # 'extra data: "paul,pa\xc3\xbcl"\n'
                        # But logging and exceptions need a normal (7 bit) str
                        error_str = str(err)
                        logging.warning(error_str)
                        raise DatabaseError('Data Error during COPY command: {error_str}')
                    except Exception as err:
                        raise DatabaseError('Generic Error during COPY: {0}'.format(err))
        except Exception as err:
            raise AirflowCKANException('Failed to push data into postgres DB.', str(err))
        finally:
            cur.close()
    finally:
        status_dict = {
            'res_id': resource_dict['ckan_resource_id'],
            'state': 'complete',
            'message': 'Successfully pushed entries to "{res_id}"'.format(
                                                res_id = resource_dict['ckan_resource_id'])
            }
    
        aircan_status_update(site_url, api_key, status_dict)
        connection.commit()

    return {'success': True}
