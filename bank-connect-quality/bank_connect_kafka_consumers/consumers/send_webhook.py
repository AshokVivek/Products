import os

import django
django.setup()

from utils.general import put_response_to_webhook
from bank_connect.models import Entity

def send_webhook(task_details):
    webhook_url = task_details.get('webhook_url')
    session_id = task_details.get('session_id')
    bank = task_details.get('bank')
    notification_details = task_details.get('notification_details')
    event_type = task_details.get('event_type')
    mode = task_details.get('mode')

    error_code = notification_details.get('error_code')
    error_message = notification_details.get('error_message')
    
    webhook_payload = {
        "session_id": session_id,
        "event_name": event_type,
        "accounts":[
            {
                "bank_name": bank,
                "account_id" :"",
                "account_status" :"",
                "error_code" : error_code,
                "error_message": error_message
            }
        ]
    }

    entity_object = Entity.objects.get(entity_id=session_id)
    put_response_to_webhook(webhook_url, webhook_payload, event_type, None, entity = entity_object)