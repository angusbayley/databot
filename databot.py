import os
import time
from slackclient import SlackClient

BOT_ID = os.environ.get('DATABOT_ID')

slack_client = SlackClient(os.environ.get('DATABOT_API_TOKEN'))

databot_messages = []

def parse_slack_output(slack_rtm_output):
    for message in slack_rtm_output:
        if 'text' in message and '<@' + BOT_ID + '>' in message['text']:
            print(message)
            channel_id = message['channel']
            # do tableau stuff

            # post tableau stuff back to channel
            request = slack_client.api_call("chat.postMessage",
                                            channel=channel_id,
                                            text='you called?',
                                            as_user=True)
        else:
            print(message)


if __name__ == '__main__':

    if slack_client.rtm_connect():
        print("websocket open for business")
        while True:
            parse_slack_output(slack_client.rtm_read())
            time.sleep(1)
    else:
        # print(slack_client.rtm_connect())
        print('problem with connection')
