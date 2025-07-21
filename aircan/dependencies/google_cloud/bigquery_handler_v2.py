from google.cloud import bigquery
import google.api_core.exceptions
from aircan.dependencies.utils import AirflowCKANException, aircan_status_update_nhs as aircan_status_update
import json
import logging

def replace_all(dict, string):
    for key in dict:
        string = string.replace(key, dict[key])
    return string

def bq_import_csv(table_id, gcs_path, table_schema, ckan_conf):
    try:
        client = bigquery.Client()

        try:
            job_config = bigquery.LoadJobConfig()

            schema = bq_schema_from_table_schema(table_schema)
            job_config.schema = schema

            job_config.skip_leading_rows = 1
            job_config.source_format = bigquery.SourceFormat.CSV
            # overwrite a Table
            job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
            load_job = client.load_table_from_uri(
                gcs_path, table_id, job_config=job_config
            )

            load_job.result()  # Waits for table load to complete.
            destination_table = client.get_table(table_id)
        except Exception as e:
            logging.info(e)
            # Use a list to build the string components efficiently.
            error_lines = []
            error_lines.append(
                "BigQuery Load Job Failed with a BadRequest."
            )
            error_lines.append(f"Original API message: {e}")

            # The key part: Iterate through the e.errors list and append to our list.
            if load_job.errors:
                error_lines.append("\n--- Detailed Error Breakdown ---")
                logging.info(load_job.errors)
                for i, error in enumerate(load_job.errors):
                    # Format each error dictionary into a readable line.
                    line = (
                        f"Error {i+1}: "
                        f"Reason: {error.get('reason', 'N/A')}, "
                        f"Location: {error.get('location', 'N/A')}, "
                        f"Message: {error.get('message', 'N/A')}"
                    )
                    error_lines.append(line)
            else:
                error_lines.append("No detailed errors were provided in the exception.")

            # Join the list of lines into a single string with newlines.
            error_report_string = "\n".join(error_lines)
            logging.info(error_report_string)
            status_dict = {
                'res_id': ckan_conf.get('resource_id'),
                'state': 'failed',
                'message': error_report_string,
                'dag_run_id': ckan_conf.get('dag_run_id')
            }
            aircan_status_update(ckan_conf.get('site_url'), ckan_conf.get('api_key'), status_dict)
            raise AirflowCKANException('Data ingestion has failed.', str(e))
            #status_dict = {
            #    'res_id': ckan_conf.get('resource_id'),
            #    'state': 'progress',
            #    'message': 'Data ingestion using provided schema failed, trying to autodetect schema.',
            #    'dag_run_id': ckan_conf.get('dag_run_id')
            #}
            #aircan_status_update(ckan_conf.get('site_url'), ckan_conf.get('api_key'), status_dict)
            #job_config = bigquery.LoadJobConfig()
            #job_config.autodetect = True

            #job_config.skip_leading_rows = 1
            #job_config.source_format = bigquery.SourceFormat.CSV
            ## overwrite a Table
            #job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
            #load_job = client.load_table_from_uri(
            #    gcs_path, table_id, job_config=job_config
            #)
            #load_job.result()  # Waits for table load to complete.
            #destination_table = client.get_table(table_id)
        status_dict = {
            'res_id': ckan_conf.get('resource_id'),
            'state': 'progress',
            'message': 'Data ingestion is in progress.',
            'dag_run_id': ckan_conf.get('dag_run_id')
        }
        aircan_status_update(ckan_conf.get('site_url'), ckan_conf.get('api_key'), status_dict)
        if destination_table:
            status_dict = {
                'res_id': ckan_conf.get('resource_id'),
                'state': 'complete',
                'message': "Ingession Completed",
            'dag_run_id': ckan_conf.get('dag_run_id')
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
            'dag_run_id': ckan_conf.get('dag_run_id'),
            'state': 'failed',
            'message': str(e)
        }
        aircan_status_update(ckan_conf.get('site_url'), ckan_conf.get('api_key'), status_dict)
        raise AirflowCKANException('Data ingestion has failed.', str(e))


def bq_schema_from_table_schema(table_schema):
    mapping = {
        'string': 'STRING',
        'number': 'NUMERIC',
        'integer': 'NUMERIC',
        'boolean': 'BOOLEAN',
        'object': 'STRING',
        'array': 'STRING',
        'date': 'DATE',
        'time': 'TIME',
        'datetime': 'DATETIME',
        'year': 'NUMERIC',
        'yearmonth': 'STRING',
        'duration': 'DATETIME',
        'geopoint': 'GEOPOINT',
        'geojson': 'STRING',
        'any': 'STRING'
    }

    def _convert(field):
        # Â TODO: support for e.g. required
        return bigquery.SchemaField(field['name'],
                                    mapping.get(field['type'], field['type']),
                                    'NULLABLE'
                                    )
    return [_convert(field) for field in table_schema]
