import os
import datetime as dt
import dateutil.relativedelta
from googleapiclient import discovery
from oauth2client.client import GoogleCredentials
from utilities.parse_bigquery_data import parse_data
import pandas as pd

projectId = 'gc-data-warehouse'
credentials = GoogleCredentials.get_application_default()

today = dt.date.today()
this_month = dt.date(today.year, today.month, 1)
last_month = this_month - dateutil.relativedelta.relativedelta(months=1)

# [TODO] Instead of querying BigQuery directly, replace this logic (and that in parse_big_query_data.py)
# with a monkey-patched version of the read_gbq method in Pandas. The method is much fancier, but currently
# uses Legacy SQL, and we want to use standard SQL.

def revenue_summary():
    revenue_last_two_months = run_query('revenue_last_two_months.sql')
    revenue_this_month = revenue_last_two_months[revenue_last_two_months['month'] == this_month]
    revenue_this_month['revenue_running_sum'] = revenue_this_month.revenue.cumsum()
    revenue_last_month = revenue_last_two_months[revenue_last_two_months['month'] == last_month]
    revenue_last_month['revenue_running_sum'] = revenue_last_month.revenue.cumsum()
    return revenue_this_month, revenue_last_month

def volume_summary():
    volume_last_two_months = run_query('volume_last_two_months.sql')
    volume_this_month = volume_last_two_months[volume_last_two_months['month'] == this_month]
    volume_this_month['volume_running_sum'] = volume_this_month.volume.cumsum()
    volume_last_month = volume_last_two_months[volume_last_two_months['month'] == last_month]
    volume_last_month['volume_running_sum'] = volume_last_month.volume.cumsum()
    return volume_this_month, volume_last_month

def run_query(query_filename):
    service = discovery.build('bigquery', 'v2', credentials=credentials)

    query_request_body = {
        'query': _load_query(query_filename),
        'use_cached_results': True,
        'useLegacySql': False,
    }

    request = service.jobs().query(projectId=projectId, body=query_request_body)
    response = request.execute()
    return parse_data(response['schema'], response['rows'])

def _load_query(query_filename):
    with open(os.path.join('queries', query_filename)) as sql_file:
        return sql_file.read()

if __name__ == "__main__":
    x, y = revenue_summary()
    print(x, y)

