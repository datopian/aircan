from google.cloud import bigquery
import google.api_core.exceptions
import json


def bq_import_csv(table_id, gcs_path, table_schema):
    
    client = bigquery.Client()

    #table = bigquery.Table(table_id)

    job_config = bigquery.LoadJobConfig()

    schema = bq_schema_from_table_schema(table_schema)
    job_config.schema = schema

    job_config.skip_leading_rows = 1
    job_config.source_format = bigquery.SourceFormat.CSV

    load_job = client.load_table_from_uri(
        gcs_path, table_id, job_config=job_config
    )

    load_job.result()  # Waits for table load to complete.

def bq_schema_from_table_schema(table_schema):
    mapping = {
        'number': 'float'
        }
    def _convert(field):
        # TODO: support for e.g. required
        return bigquery.SchemaField(field['name'],
            mapping.get(field['type'], field['type']),
            'NULLABLE'
            )
    return [ _convert(field) for field in table_schema['fields'] ]
