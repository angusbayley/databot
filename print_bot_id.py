import os
from slackclient import SlackClient

BOT_NAME = 'databot'

slack_client = SlackClient(os.environ.get('DATABOT_API_TOKEN'))

if __name__ == '__main__':
    api_call = slack_client.api_call("users.list")
    if api_call.get("ok"):
        users = api_call.get("members")
        for user in users:
            if user['name'] == 'databot':
                print(user['id'])
