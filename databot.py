import os
import time
from slackclient import SlackClient
from bigquery import revenue_summary
from raven import Client

BOT_ID = os.environ.get('DATABOT_ID')

slack_client = SlackClient(os.environ.get('DATABOT_API_TOKEN'))

databot_messages = []

def parse_slack_output(slack_rtm_output):
    for message in slack_rtm_output:
        if 'text' in message and '<@' + BOT_ID + '>' in message['text']:
            print(message)
            channel_id = message['channel']
            if 'revenue' in message['text']:
                revenue_this_month, revenue_last_month = revenue_summary()
                request = slack_client.api_call("chat.postMessage",
                                channel=channel_id,
                                text=round(revenue_this_month.iloc[-1]['revenue_running_sum']),
                                as_user=True)
            if 'volume' in message['text']:
                volume_this_month, volume_last_month = volume_summary()
                request = slack_client.api_call("chat.postMessage",
                                channel=channel_id,
                                text=round(volume_this_month.iloc[-1]['volume_running_sum']),
                                as_user=True)
            else:
                request = slack_client.api_call("chat.postMessage",
                                                channel=channel_id,
                                                text='you called?',
                                                as_user=True)
        else:
            print(message)


if __name__ == '__main__':

    try:
        if slack_client.rtm_connect():
            print("websocket open for business")
            while True:
                parse_slack_output(slack_client.rtm_read())
                time.sleep(1)
        else:
            # print(slack_client.rtm_connect())
            print('problem with connection')
    except:
        client = Client(os.environ.get('DATABOT_SNITCH_URL'))
        client.captureException()
