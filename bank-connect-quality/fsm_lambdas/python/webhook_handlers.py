import datetime
import json
import traceback

from python.configs import (
    LAMBDA_LOGGER,
    DJANGO_BASE_URL,
    API_KEY,
    SESSION_EXPIRY_SQS_QUEUE_URL,
    sqs_client
)


# from python.configs import *
from python.utils import get_datetime
from python.api_utils import call_api_with_session


def invoke_session_status_handler(event, context):
    session_expiry_notification_webhook = "Session_expiry_notification"
    initiate_webhook_url = f"{DJANGO_BASE_URL}/bank-connect/v1/internal_admin/admin_initiated_webhook/"

    records = event.get("Records", {})

    current_time = datetime.datetime.now()
    for record in records:
        body = json.loads(record.get("body", "{}"))
        if body.get("session_expiry_at"):
            log_data = {
                "session_id": body.get("session_id", ""),
                "session_expiry_at": body.get("session_expiry_at", ""),
                "url": initiate_webhook_url
            }
            try:
                session_expiry_time = get_datetime(body.get("session_expiry_at"))
                time_diff_in_seconds = (session_expiry_time - current_time).total_seconds()
            except Exception as e:
                LAMBDA_LOGGER.error(
                    f"Session_expiry_notification failed due to new format of session_expiry_time with error: {e}",
                    extra=log_data
                )
                continue

            if time_diff_in_seconds <= 0:
                LAMBDA_LOGGER.debug(
                    "Invoking webhook intimation",
                    extra=log_data
                )

                payload = json.dumps({"session_id": body.get("session_id", ""),
                                      "notification_type": session_expiry_notification_webhook})
                headers = {
                    'Content-Type': 'application/json',
                    'x-api-key': API_KEY
                }
                
                response = call_api_with_session(initiate_webhook_url,"POST", payload, headers)     
                log_data["response"] = response.text
                LAMBDA_LOGGER.info(
                    "Completed the initiate webhook api",
                    extra=log_data
                )
            else:
                update_message_to_sqs(body, int(time_diff_in_seconds), log_data)


def update_message_to_sqs(body, remaining_time_in_seconds, log_data):
    maximum_visibility_timeout_in_seconds = 14 * 60
    remaining_time_in_seconds = min(remaining_time_in_seconds, maximum_visibility_timeout_in_seconds)

    try:
        # if we are adding a message into the queue again, means
        # the status is still in PENDING state
        # sleeping for 2 seconds, before putting message again into the queue
        log_data.update({
            "body": body,
            "remaining_time_in_seconds": remaining_time_in_seconds
        })
        sqs_client.send_message(
            QueueUrl=SESSION_EXPIRY_SQS_QUEUE_URL,
            MessageBody=json.dumps(body),
            DelaySeconds=remaining_time_in_seconds
        )
        LAMBDA_LOGGER.info(
            "Updated the message back to sqs",
            extra=log_data
        )
    except Exception as e:
        log_data.update({
            "exception": str(e),
            "trace": traceback.format_exc()
        })
        print("Exception while updating the message to sqs = {}".format(log_data))
        LAMBDA_LOGGER.error(
            "Exception while updating the message to sqs",
            extra=log_data
        )
