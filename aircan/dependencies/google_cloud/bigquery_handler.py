from google.cloud import bigquery
import google.api_core.exceptions
import json
import logging

def bq_import_csv(table_id, gcs_path, table_schema):
    try:
        client = bigquery.Client()

        job_config = bigquery.LoadJobConfig()

        schema = bq_schema_from_table_schema(table_schema)
        #job_config.schema = schema

        job_config.skip_leading_rows = 1
        job_config.source_format = bigquery.SourceFormat.CSV
        # overwrite a Table
        job_config.write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
        # set schema autodetect
        job_config.autodetect=True
        load_job = client.load_table_from_uri(
            gcs_path, table_id, job_config=job_config
        )

        load_job.result()  # Waits for table load to complete.
        destination_table = client.get_table(table_id)
        if destination_table:
            return {'success': True, 'message': 'BigQuery Table created successfully.'}
    except Exception as e:
        return {'success': False, 'message': 'Failed to create BigQuery Table.'}
    
def bq_schema_from_table_schema(table_schema):
    mapping = {
        'number': 'float',
        'year': 'integer'
        }
    def _convert(field):
        # TODO: support for e.g. required
        return bigquery.SchemaField(field['name'],
            mapping.get(field['type'], field['type']),
            'NULLABLE'
            )
    return [ _convert(field) for field in table_schema ]
