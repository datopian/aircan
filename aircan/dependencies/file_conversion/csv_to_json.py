import pandas as pd
import io
import requests
import logging as log
from aircan import NotFound, RequestError

def convert(input, output, **kwargs):
    try:
        log.info('Starting file conversion')
        df = pd.read_csv(input)
        df.to_json(output, orient='records')
        return {"success": True}
    except FileNotFoundError as e:
        raise NotFound(f"File {input} doesn\'t exist.")

def convert_from_url(input_url, output, ckan_api_key, **kwargs):
    try:
        log.info('Starting file conversion from url: ' + ckan_api_key)
        header = {'Authorization': ckan_api_key}
        s = requests.get(input_url, headers=header).content
        if s.status_code == 200
            df = pd.read_csv(io.StringIO(s.decode('utf-8')))
            df.to_json(output, orient='records')
            return {"success": True}
        else:
            raise RequestError(response.json()['error'])
    except Exception as e:
        return str(e)
