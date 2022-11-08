from datetime import datetime, time, timedelta
import decimal
import json
import itertools
import logging
import requests
from urllib.parse import urljoin, urlparse

from airflow.hooks.base_hook import BaseHook
from airflow.providers.sendgrid.utils import emailer
from airflow.exceptions import AirflowFailException
from airflow.models import Variable
from sqlalchemy import create_engine
from airflow.utils import timezone

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
    def __init__(self, value, err,):
        super().__init__(value, err)
        self.value = value
        self.err = err or ''

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
    exception = context.get('exception')
    resource_dict = context['params'].get('resource', {})
    api_key = context['params'].get('ckan_config', {}).get('api_key')
    site_url = context['params'].get('ckan_config', {}).get('site_url')    
    logging.error(exception.err)
    status_dict = { 
            'res_id': resource_dict.get('ckan_resource_id'),
            'state': 'error',
            'message': exception.value,
            'error': exception.err
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
