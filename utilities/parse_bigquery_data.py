import pandas as pd
import numpy as np
from pandas.compat import lzip


""" the following methods have been copied from Pandas
https://github.com/pydata/pandas/blob/1919e26ead5d156c2b505a0ad8d233b02eb1b573/pandas/io/gbq.py
"""

def parse_data(schema, rows):
    dtype_map = {'INTEGER': np.dtype(float),
                 'FLOAT': np.dtype(float),
                 'TIMESTAMP': 'M8[ns]',
                 'DATE': 'M8[D]', # added date dtype, which didn't exist in legacy BigQuery SQL
    }

    fields = schema['fields']
    col_types = [field['type'] for field in fields]
    col_names = [str(field['name']) for field in fields]
    col_dtypes = [dtype_map.get(field['type'], object) for field in fields]
    page_array = np.zeros((len(rows),),
                          dtype=lzip(col_names, col_dtypes))

    for row_num, raw_row in enumerate(rows):
        entries = raw_row.get('f', [])
        for col_num, field_type in enumerate(col_types):
            field_value = _parse_entry(entries[col_num].get('v', ''),
                                       field_type)
            page_array[row_num][col_num] = field_value

    return pd.DataFrame(page_array, columns=col_names)

def _parse_entry(field_value, field_type):
    if field_value is None or field_value == 'null':
        return None
    if field_type == 'INTEGER' or field_type == 'FLOAT':
        return float(field_value)
    elif field_type == 'TIMESTAMP':
        timestamp = datetime.utcfromtimestamp(float(field_value))
        return np.datetime64(timestamp)
    elif field_type == 'BOOLEAN':
        return field_value == 'true'
    return field_value
