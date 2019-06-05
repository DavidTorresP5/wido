import json
import requests

# Use your webhook url to post your msgs to a particular slack channel!
webhook_url = ""

def send_message(msg, webhook_url):

    """
    Send a message to slack using a webhook connector
    """
    
    message = {"text": msg}
    return requests.post(webhook_url, data = json.dumps(message))