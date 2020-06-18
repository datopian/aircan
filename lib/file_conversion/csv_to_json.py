import pandas as pd


def convert(input, output, **kwargs):
    print('Starting file conversion')
    df = pd.read_csv(input)
    df.to_json(output, orient='records')
    return {"success": True}