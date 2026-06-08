import io
import os
import hashlib
import logging
import psycopg2
from urllib.request import urlopen 
import pandas as pd

from aircan.dependencies_legacy.utils import AirflowCKANException, string_chunky, aircan_status_update
from frictionless import Resource

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
            raise psycopg2.DatabaseError(f"Error during deleting indexes: {error_str}")
        except Exception as e:
            raise psycopg2.DatabaseError(f"Error during deleting indexes: {error_str}")
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
                raise psycopg2.DatabaseError(f"Error during reindexing: {error_str}")
    except Exception as e:
        return str(e)
    return {'success': True, 'message': 'Reindex Successful.'}

def _generate_index_name(resource_id, field, config={}, connection=None):
    value = (resource_id + field).encode('utf-8')
    return hashlib.sha1(value).hexdigest()

CHUNK_SIZE = 85000


def _coerce_date_columns(data, date_formats):
    """Force declared date columns to datetime in-place.

    Parsing is explicit so ``to_csv`` always emits unambiguous ISO dates to
    COPY. When the frictionless schema declares an exact ``format`` we use it.
    Otherwise we use ``format='mixed'`` with ``dayfirst=True``, which infers
    the format per value: it reads day-first slash dates as day-first
    (``09/05/2026`` -> 9 May) while leaving ISO values intact
    (``2026-05-09`` stays 9 May, not flipped to Sep). A plain ``dayfirst=True``
    would corrupt ISO; ``format='mixed'`` applies it only where ambiguous.
    Unparseable values become ``NaT`` (empty -> NULL).
    """
    for col, fmt in date_formats.items():
        if col not in data.columns:
            continue
        if fmt and fmt not in ('default', 'any'):
            data[col] = pd.to_datetime(data[col], format=fmt, errors='coerce')
        else:
            data[col] = pd.to_datetime(data[col], format='mixed', dayfirst=True, errors='coerce')
    return data


def _notify_status(site_url, api_key, res_id, state, message):
    """Push a single datastore status update for ``res_id``."""
    aircan_status_update(site_url, api_key, {
        'res_id': res_id,
        'state': state,
        'message': message,
    })


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
        resource_dict = kwargs['resource_dict']
        site_url = kwargs['site_url']
        api_key = kwargs['api_key']
        fields = kwargs['schema']
        resource_tmp_file = resource_dict['resource_tmp_file']
        resource_id = resource_dict['ckan_resource_id']
        column_names = ', '.join(['"{0}"'.format(field['name']) for field in fields])
        unique_keys = resource_dict.get('datastore_unique_keys', False)
        # Map each date/datetime column to its declared frictionless format
        # (a strptime pattern) when present. Columns without an explicit
        # format fall back to day-first parsing below.
        date_formats = {
            f['name']: f.get('format')
            for f in fields
            if f['type'] in ('date', 'datetime', 'timestamp')
        }
        cur = connection.cursor()
        # We emit unambiguous ISO dates ourselves (see chunk loop), so ISO is
        # the primary style; DMY is only the fallback ordering for any value
        # that is not already ISO.
        cur.execute("SET datestyle = 'ISO, DMY'")

        # Pre-build the per-chunk strings/SQL once — these never change between
        # chunks, so formatting them inside the loop only wastes CPU.
        unique_keys_str = ', '.join('"{0}"'.format(key) for key in unique_keys) if unique_keys else ''
        update_set = ','.join(['"{0}"=EXCLUDED."{0}"'.format(field['name']) for field in fields])

        insert_sql = '''
            COPY "{resource_id}" ({column_names})
            FROM STDIN
            WITH (DELIMITER ',', FORMAT csv, HEADER 1, ENCODING 'UTF8');
            '''.format(resource_id=resource_id, column_names=column_names)

        upsert_table_creation = '''
            CREATE TEMPORARY TABLE "temp_{resource_id}" (LIKE "{resource_id}") ON COMMIT DROP;
            ALTER TABLE "temp_{resource_id}" DROP COLUMN _id;
            '''.format(resource_id=resource_id)

        # TRUNCATE first so each chunk only re-processes its own rows instead of
        # re-inserting every prior chunk's rows on each iteration.
        upsert_sql = '''
            TRUNCATE "temp_{resource_id}";
            COPY "temp_{resource_id}" ({column_names})
            FROM STDIN
            WITH (DELIMITER ',', FORMAT csv, HEADER 1, ENCODING 'UTF8');

            INSERT INTO "{resource_id}"({column_names})
            SELECT {column_names} FROM "temp_{resource_id}"
            ON CONFLICT ({unique_keys}) DO UPDATE SET {update_set};
            '''.format(resource_id=resource_id, column_names=column_names,
                       unique_keys=unique_keys_str, update_set=update_set)

        try:
            sql_str = upsert_sql if unique_keys else insert_sql

            with Resource(resource_tmp_file) as resource:
                logging.info('Data records are being ingested.')
                _notify_status(site_url, api_key, resource_id,
                               'progress', 'Data records are being ingested.')
                try:
                    # Read date columns as raw strings (no parse_dates); we
                    # convert them explicitly per chunk so parsing is
                    # deterministic and never silently leaks raw D/M/Y vs
                    # M/D/Y strings into COPY.
                    df = pd.read_csv(resource.text_stream, keep_default_na=False, chunksize=CHUNK_SIZE)
                    iter_count = 0
                    rows_processed = 0
                    if unique_keys:
                        logging.info("Upsert Temp Table Creation")
                        cur.execute(upsert_table_creation)

                    for data in df:
                        _coerce_date_columns(data, date_formats)
                        rows_processed += len(data)
                        iter_count += 1
                        logging.info('Data Inserted for %s rows using %s iterations for resource %s', rows_processed, iter_count, resource_id)
                        buffer_data = io.StringIO()
                        data.to_csv(buffer_data, index=False)
                        buffer_data.seek(0)
                        cur.copy_expert(sql_str, buffer_data)
                        # Release the chunk + buffer before reading the next one.
                        buffer_data.close()
                        del data
                    logging.info('Data Ingestion is completed for %s rows by %s iterations for resource %s', rows_processed, iter_count, resource_id)

                    if not resource_dict['datastore_append_enabled']:
                        # Do not mark yet as complete if append is enabled
                        _notify_status(
                            site_url, api_key, resource_id, 'complete',
                            'Data ingestion completed successfully for "{res_id}".'.format(res_id=resource_id))

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
