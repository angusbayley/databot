import os
import time
from slackclient import SlackClient
from bigquery import revenue_summary
from bigquery import plot_mom_chart
from bigquery import generate_revenue_comment
from raven import Client
from slacker import Slacker

BOT_ID = os.environ.get('DATABOT_ID')
slack_rtm_client = SlackClient(os.environ.get('DATABOT_API_TOKEN'))
slack = Slacker(os.environ.get('DATABOT_API_TOKEN'))


def parse_slack_output(slack_rtm_output):
    for message in slack_rtm_output:
        if 'text' in message and '<@' + BOT_ID + '>' in message['text']:
            print(message)
            channel_id = message['channel']
            if 'revenue' in message['text']:
                revenue_this_month, revenue_last_month = revenue_summary()
                comment = generate_revenue_comment(revenue_this_month, revenue_last_month)
                plot_mom_chart(revenue_this_month, revenue_last_month)
                slack.files.upload('mom_revenue.png',
                                   filename='Month-on-Month Revenue',
                                   channels=channel_id,
                                   initial_comment=comment)
            else:
                slack.chat.post_message(channel=channel_id,
                                        text='you called?',
                                        as_user=True)
        else:
            print(message)


if __name__ == '__main__':

    try:
        if slack_rtm_client.rtm_connect():
            print("websocket open for business")
            while True:
                parse_slack_output(slack_rtm_client.rtm_read())
                time.sleep(1)
        else:
            print('problem with connection')
    except:
        client = Client(os.environ.get('DATABOT_SNITCH_URL'))
        client.captureException()
