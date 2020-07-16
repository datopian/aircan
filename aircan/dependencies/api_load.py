# Standard library imports
from urllib.parse import urljoin
import hashlib
import datetime
import decimal
import logging as log

# Third-party library imports
import json
import requests


class DatastoreEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime.datetime):
            return obj.isoformat()
        if isinstance(obj, decimal.Decimal):
            return str(obj)

        return json.JSONEncoder.default(self, obj)


def load_resource_via_api(ckan_resource_id, blob, ckan_api_key, ckan_site_url):
    log.info("Loading resource via API lib")
    records = blob.download_as_string()
    records = records.decode('utf-8')
    try:
        request = {
           'resource_id': ckan_resource_id,
           'force': True,
           'records': records}

        url = urljoin(ckan_site_url, '/api/3/action/datastore_create')
        response = requests.post(url,
                      data=json.dumps(request, cls=DatastoreEncoder),
                      headers={'Content-Type': 'application/json',
                               'Authorization': ckan_api_key})
        log.info(response)
        if response.status_code == 200:
            resource_json = response.json()
            log.info('Table was created successfuly')
            return {'success': True, 'response_resource': resource_json}
        else:
            return response.json()

    except Exception as e:
        return {"success": False, "errors": [e]}
