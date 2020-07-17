import pandas as pd
import io
import requests
import logging as log
from aircan import NotFound, RequestError
from aircan.dependencies.google_cloud.file_handler import update_file_content_from_string

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
        log.info('Starting file conversion from url: ' + input_url)
        s = requests.get(input_url)
        if s.status_code == 200:
            log.info('Reading remote CSV')
            s = s.content
            df = pd.read_csv(io.StringIO(s.decode('utf-8')))
            data = df.to_json(orient='records')
            update_file_content_from_string(output, data)
            return {"success": True}
        else:
            raise RequestError(response.json()['error'])
    except Exception as e:
        return str(e)
