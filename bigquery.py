import os
import datetime as dt
import dateutil.relativedelta
from googleapiclient import discovery
from oauth2client.client import GoogleCredentials
from utilities.parse_bigquery_data import parse_data
import pandas as pd

# plotting libraries
import matplotlib
matplotlib.use('Agg') # use a non-interactive backend for writing to file
import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter
import seaborn as sns

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

def generate_revenue_comment(revenue_this_month, revenue_last_month):
    current_working_day = revenue_this_month.iloc[-1]['working_day']
    current_revenue = int(revenue_this_month.iloc[-1]['revenue_running_sum'])
    revenue_this_time_last_month = revenue_last_month.loc[revenue_last_month.working_day == current_working_day, 'revenue_running_sum']
    revenue_this_time_last_month = int(revenue_this_time_last_month.values[0]) # get cell value
    percent_mom_difference = round(100*(current_revenue - revenue_this_time_last_month)/revenue_this_time_last_month, 2)
    comment_template = "So far this month we've generated £{0:,} in revenue, compared to £{1:,} this time last month ({2:+}%)."
    return comment_template.format(current_revenue, revenue_this_time_last_month, percent_mom_difference)

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

def plot_mom_chart(revenue_this_month, revenue_last_month):
    fig, ax = plt.subplots()
    ax.yaxis.set_major_formatter(FuncFormatter(_thousands_formatter))
    plt.plot(revenue_this_month['working_day'], revenue_this_month['revenue_running_sum'], label='This Month')
    plt.plot(revenue_last_month['working_day'], revenue_last_month['revenue_running_sum'], label='Last Month')
    plt.xlabel('Working Day')
    plt.ylabel('Revenue')
    plt.legend(loc='lower right')
    fig.savefig('mom_revenue.png')

def _load_query(query_filename):
    with open(os.path.join('queries', query_filename)) as sql_file:
        return sql_file.read()

def _thousands_formatter(x, pos):
    'The args are the value and tick position'
    return '£{}k'.format(int(x*1e-3))

if __name__ == "__main__":
    this_month, last_month = revenue_summary()
    comment = generate_revenue_comment(this_month, last_month)
    print(comment)
    plot_mom_chart(this_month, last_month)

