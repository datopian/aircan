import os
from datetime import datetime, time, timedelta
import decimal
import json
import itertools
import logging
import requests
import tempfile
import hashlib
from urllib.parse import urljoin, urlparse
from requests.adapters import HTTPAdapter, Retry

from airflow.hooks.base_hook import BaseHook
from airflow.providers.sendgrid.utils import emailer
from airflow.exceptions import AirflowFailException
from airflow.models import Variable
from sqlalchemy import create_engine
from airflow.utils import timezone

DOWNLOAD_TIMEOUT = 30
DOWNLOAD_RETRIES = 3
CHUNK_SIZE = 16 * 1024 

def frictionless_to_ckan_schema(field_type):
    '''
    Convert frictionless schema to ckan schema
    '''
    mapper = {
        'integer': 'integer',
        'number': 'numeric',
        'datetime': 'timestamp', 
        'timestamptz':'timestamptz',
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
    return mapper.get(field_type, 'text')

def ckan_to_frictionless_schema(field_type):
    '''
    Convert ckan schema to frictionless schema
    '''
    mapper = {
        'integer': 'integer',
        'numeric': 'number',
        'timestamp': 'datetime', 
        'timestamptz':'timestamptz',
        'date': 'date',
        'time': 'time', 
        'text': 'string',
        'interval': 'duration',
        'boolean': 'boolean',
        'jsonb': 'object',
        'array': 'array',
        }

    return mapper.get(field_type, 'text')


def days_ago(n, hour=0, minute=0, second=0, microsecond=0):
    return datetime.combine(
        datetime.now(timezone.TIMEZONE) - timedelta(days=n),
        time(hour, minute, second, microsecond, tzinfo=timezone.TIMEZONE),
    )

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
            'last_updated': str(datetime.utcnow()),
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
        if isinstance(obj, datetime):
            return obj.isoformat()
        if isinstance(obj, decimal.Decimal):
            return str(obj)

        return json.JSONEncoder.default(self, obj)

def get_connection(url):
    engine = create_engine(url)
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


class AirflowCKANException(AirflowFailException):
    def __init__(self, value, err=None):
        super().__init__(value, err)
        self.value = value
        self.err = err

    def __str__(self):
        return self.value


def email_dispatcher(context, api_key, site_url):
    resource_dict = context['params'].get('resource', {})
    api_key = context['params'].get('ckan_config', {}).get('api_key')
    site_url = context['params'].get('ckan_config', {}).get('site_url')
    notification_subject = context['params'].get('ckan_config', {}).get('aircan_notification_subject')
    notificaton_from = context['params'].get('ckan_config', {}).get('aircan_notificaton_from')   
    notification_to = context['params'].get('ckan_config', {}).get('aircan_notification_to')   
    exception = context.get('exception')

    try:
        url = urljoin(site_url, '/api/3/action/package_show?id={0}'.format(
                                                        resource_dict['package_id']))
        response = requests.get(url,
                        headers={'Content-Type': 'application/json',
                                'Authorization': api_key})
        if response.status_code == 200:
            package_dict = response.json()

            if package_dict['result'] and notification_to:
                author_email = package_dict['result'].get('author_email', None)
                maintainer_email = package_dict['result'].get('maintainer_email', None)
                editor_email = resource_dict.get('editor_user_email', None)
                email_to = []
                
                for r in notification_to.split(","):
                    r = r.strip()
                    if r == 'author' and author_email:
                        email_to.append(author_email)
                    if r == 'maintainer' and maintainer_email:
                        email_to.append(maintainer_email)
                    if r == 'editor' and editor_email:
                        email_to.append(editor_email)
                    if r not in ['author', 'maintainer', 'editor']:
                        email_to.append(r)

                datastore_manage_url = urljoin(site_url,'/dataset/{0}/resource_data/{1}' ).format(
                    resource_dict['package_id'], resource_dict['ckan_resource_id'])
                if email_to:
                    emailer.send_email(
                        to = list(set(email_to)) , 
                        subject = notification_subject,
                        html_content = _compose_error_email_body(
                            site_url,
                            datastore_manage_url,
                            exception
                        ), 
                        from_email = notificaton_from,
                        sandbox_mode = False
                        )
                else:
                    logging.info('No email to send.')

    except Exception as e:
        logging.error(e)


def ckan_datstore_loader_failure(context):

    # Delete the temporary resource file
    ti = context['ti']
    xcom_result = ti.xcom_pull(task_ids='fetch_resource_data') or {}
    resource_tmp_file = xcom_result.get('resource_tmp_file', False)
    if resource_tmp_file:
        os.unlink(resource_tmp_file)

    exception = context.get('exception')
    resource_dict = context['params'].get('resource', {})
    api_key = context['params'].get('ckan_config', {}).get('api_key')
    site_url = context['params'].get('ckan_config', {}).get('site_url')    
    logging.error(exception.err or exception.value)
    status_dict = { 
            'res_id': resource_dict.get('ckan_resource_id'),
            'state': 'error',
            'message': exception.value,
            'error': exception.err or exception.value
        }
    aircan_status_update(site_url, api_key, status_dict)
    email_dispatcher(context, api_key, site_url)


def _compose_error_email_body(site_url, datastore_manage_url, exception):
    email_html = '''
        <!DOCTYPE HTML PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN" "http://www.w3.org/TR/html4/loose.dtd">
        <html>
        <body>
            <div>
            <h3>✖ Data ingestion has been failed.</h3>
            <div>
            <p>Data Ingestion has failed because of the following reason:</p>
            <p>
            <div style="padding:0px 6px;color:#a94442;background-color:#f2dede;border:2px solid #ebccd1;margin-bottom:20px;">
                <p><strong>Message:</strong> {error_msg}
                <p><strong>Upload Error:</strong> {error}
                <p> 
            </div>
            <a  style="padding:8px 10px;background-color:#f26522;border:1px solid #f26522;border-radius:12px;color: #fff;text-decoration:none;"
            href="{datastore_manage_url}">View error</a></p>

        </body>
        </html>
            '''
    return email_html.format(
        datastore_manage_url = datastore_manage_url,
        site_url = urlparse(site_url).netloc,
        error_msg = exception.value,
        error = exception.err
        )


def get_response(url, headers):
    def get_url():
        kwargs = {
            'headers': headers, 
            'timeout': DOWNLOAD_TIMEOUT,
            'stream': True
        }
        retry = Retry(total=3, backoff_factor=0.3, status_forcelist=[402, 408, 502, 503, 504 ])
        adapter = HTTPAdapter(max_retries=retry)
        with requests.Session() as session:
            session.mount('http://', adapter)
            session.mount('https://', adapter)
            return session.get(url, **kwargs)
    response = get_url()


    response.raise_for_status()
    return response

def download_resource_file(url, headers, delete=True):
    '''
    Download resource file from CKAN
    '''
    response = get_response(url, headers)
    filename = url.split('/')[-1].split('#')[0].split('?')[0]
    m = hashlib.md5()

    tmp_file = tempfile.NamedTemporaryFile(suffix=filename, delete=delete)
    for chunk in response.iter_content(chunk_size=CHUNK_SIZE):
        if chunk:
            tmp_file.write(chunk)
        m.update(chunk)
    response.close()
    tmp_file.seek(0)
    file_hash = m.hexdigest()
    return tmp_file, file_hash


def join_path(path, *paths):
    """
    Join path with multiple paths
    """
    for p in paths:
        path = os.path.join(path, p)
    return path