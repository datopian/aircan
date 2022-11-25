import io
import os
import hashlib
import logging
import psycopg2
from urllib.request import urlopen 
import pandas as pd

from aircan import RequestError, DatabaseError
from aircan.dependencies.utils import AirflowCKANException, string_chunky, aircan_status_update
from frictionless import Resource
from frictionless.plugins.remote import RemoteControl


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
        resource_tmp_file = resource_dict['resource_tmp_file']
        column_names = ', '.join(['"{0}"'.format(field['name']) for field in fields])
        unique_keys = resource_dict.get('datastore_unique_keys', False)
        cur = connection.cursor()

        insert_sql = '''
            COPY \"{resource_id}\" ({column_names}) 
            FROM STDIN
            WITH (DELIMITER '{delimiter}', FORMAT csv, HEADER 1, ENCODING '{encoding}');
            '''

        upsert_sql = '''
            CREATE TEMPORARY TABLE \"temp_{resource_id}\" (LIKE \"{resource_id}\") ON COMMIT DROP; 
            ALTER TABLE \"temp_{resource_id}\" DROP COLUMN _id;
            
            COPY \"temp_{resource_id}\" ({column_names}) 
            FROM STDIN
            WITH (DELIMITER '{delimiter}', FORMAT csv, HEADER 1, ENCODING '{encoding}');

            INSERT INTO \"{resource_id}\"({column_names})
            SELECT {column_names} FROM \"temp_{resource_id}\" 
            ON CONFLICT ({unique_keys}) DO UPDATE SET {update_set};
            '''

        try:
            control = RemoteControl(http_timeout=50)
            if unique_keys: 
                sql_str = upsert_sql
            else:
                sql_str = insert_sql

            with Resource(resource_tmp_file) as resource: 
                logging.info('Data records are being ingested.')
                status_dict = {
                    'res_id': resource_dict['ckan_resource_id'],
                    'state': 'progress',
                    'message':'Data records are being ingested.'
                    }
                aircan_status_update(site_url, api_key, status_dict)
                try:
                    df = pd.read_csv(resource.text_stream, converters={'column_name': str}, keep_default_na=False)
                    buffer_data = io.StringIO()
                    df.to_csv(buffer_data, index=False)
                    buffer_data.seek(0)
                    cur.copy_expert(sql_str
                        .format(
                            resource_id = resource_dict['ckan_resource_id'],
                            column_names = column_names,
                            unique_keys =  ', '.join('"{0}"'.format(key) for key in unique_keys) if unique_keys else '',
                            delimiter = ',',
                            encoding = 'UTF8',
                            update_set = ','.join(['"{0}"=EXCLUDED."{0}"'.format(field['name']) for field in fields])
                        ),
                        buffer_data)
                        
                    if not resource_dict['datastore_append_enabled']:
                        # Do not mark yet as complete if append is enabled
                        status_dict = {
                            'res_id': resource_dict['ckan_resource_id'],
                            'state': 'complete',
                            'message': 'Data ingestion completed successfully for "{res_id}".'.format(
                                        res_id = resource_dict['ckan_resource_id'])}
                        aircan_status_update(site_url, api_key, status_dict)
                                    
                except psycopg2.DataError as err:
                    # E is a str but with foreign chars e.g.
                    # 'extra data: "paul,pa\xc3\xbcl"\n'
                    # But logging and exceptions need a normal (7 bit) str
                    raise Exception(str(err))
                except Exception as err:
                    raise Exception(str(err))
                    
            # Delete the temporary resource file
            os.unlink(resource_tmp_file)
        except Exception as err:
            raise AirflowCKANException('Data ingestion has failed.', str(err))
        finally:
            cur.close()

    finally:
        connection.commit()

    return {'success': True}
