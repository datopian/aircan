import pandas as pd
import io
import requests
import logging as log


def convert(input, output, **kwargs):
    log.info('Starting file conversion')
    df = pd.read_csv(input)
    df.to_json(output, orient='records')
    return {"success": True}

def convert_from_url(input_url, output, ckan_api_key, **kwargs):
    log.info('Starting file conversion from url: ' + ckan_api_key)
    header = {'Authorization': ckan_api_key}
    s = requests.get(input_url, headers=header).content
    df = pd.read_csv(io.StringIO(s.decode('utf-8')))
    df.to_json(output, orient='records')
    return {"success": True}