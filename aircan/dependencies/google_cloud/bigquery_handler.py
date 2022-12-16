from google.cloud import bigquery
import google.api_core.exceptions
from aircan.dependencies.utils import AirflowCKANException, aircan_status_update
import json
import logging

def replace_all(dict, string):
    for key in dict:
        string = string.replace(key, dict[key])
    return string

def bq_import_csv(table_id, gcs_path, table_schema, ckan_conf):
    try:
        client = bigquery.Client()

        job_config = bigquery.LoadJobConfig()

        schema = bq_schema_from_table_schema(table_schema)
        job_config.schema = schema

        job_config.skip_leading_rows = 1
        job_config.source_format = bigquery.SourceFormat.CSV
        # overwrite a Table
        job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
        # set 'True' for schema autodetect but turning it off since we define schema in explicitly when publishing data using datapub
        # job_config.autodetect = True
        load_job = client.load_table_from_uri(
            gcs_path, table_id, job_config=job_config
        )

        load_job.result()  # Waits for table load to complete.
        destination_table = client.get_table(table_id)
        status_dict = {
            'res_id': ckan_conf.get('resource_id'),
            'state': 'progress',
            'message': 'Data ingestion is in progress.'
        }
        aircan_status_update(ckan_conf.get('site_url'), ckan_conf.get('api_key'), status_dict)
        if destination_table:
            status_dict = {
                'res_id': ckan_conf.get('resource_id'),
                'state': 'complete',
                'message': "Ingession Completed"
            }
            aircan_status_update(ckan_conf.get('site_url'), ckan_conf.get('api_key'), status_dict)
            return {'success': True, 'message': 'BigQuery Table created successfully.'}
    except Exception as e:
        replacers = {
            'gs://dx-nhs-staging-giftless/': '',
            'gs://dx-nhs-production-giftless/': '',
            'gs://dx-nhs-prod-giftless/': '',
            'https://bigquery.googleapis.com/bigquery/v2/projects/datopian-dx/jobs?prettyPrint=false': '',
            'datopian-dx': '',
            'bigquery': '',
            'googleapi': '',
            'google': ''

        }
        e = replace_all(replacers,str(e))
        logging.info(e)
        status_dict = {
            'res_id': ckan_conf.get('resource_id'),
            'state': 'failed',
            'message': str(e)
        }
        aircan_status_update(ckan_conf.get('site_url'), ckan_conf.get('api_key'), status_dict)
        raise AirflowCKANException('Data ingestion has failed.', str(e))


def bq_schema_from_table_schema(table_schema):
    mapping = {
        'string': 'string',
        'number': 'numeric',
        'integer': 'numeric',
        'boolean': 'boolean',
        'object': 'string',
        'array': 'string',
        'date': 'date',
        'time': 'time',
        'datetime': 'datetime',
        'year': 'numeric',
        'yearmonth': 'string',
        'duration': 'datetime',
        'geopoint': 'string',
        'geojson': 'string',
        'any': 'string'
    }

    def _convert(field):
        # Â TODO: support for e.g. required
        return bigquery.SchemaField(field['name'],
                                    mapping.get(field['type'], field['type']),
                                    'NULLABLE'
                                    )
    return [_convert(field) for field in table_schema]
