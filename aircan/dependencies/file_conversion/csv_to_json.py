import pandas as pd
import logging
from aircan import NotFound
def convert(input, output, **kwargs):
    logging.info('Starting file conversion')
    try:
        df = pd.read_csv(input)
        df.to_json(output, orient='records')
        return {"success": True}
    except FileNotFoundError as e:
        raise NotFound(f"File {input} doesn\'t exist.")