import requests
import json
from python.aggregates import API_KEY
from python.identity_handlers import update_failed_pdf_status
from time import sleep
from sentry_sdk import capture_exception
from python.configs import *
from python.utils import call_django_to_insert_data_into_kafka
from python.configs import KAFKA_TOPIC_WEBHOOK_SEND

def poll_finsense_for_status(event, context):

    print("event: {}".format(event))

    # getting the records array
    records = event.get("Records", None)

    if records == None or len(records) == 0:
        print("No records were found")
    
    record = records[0]

    body = record.get("body", None)

    if body == None:
        print("body was None")
    
    sqs_message = json.loads(body)

    MAX_RETRIES = 75
    consent_id = sqs_message.get("consent_id", None)
    session_id = sqs_message.get("session_id", None)
    consent_handle_id = sqs_message.get("consent_handle_id", None)
    customer_id = sqs_message.get("customer_id", None)
    count = sqs_message.get("count", 0)
    polling_url = sqs_message.get("polling_url", None)
    finsense_token = sqs_message.get("finsense_token", None)
    organization_id = sqs_message.get("organization_id", None)
    webhook_url = sqs_message.get('webhook_url', None)
    bank_name = sqs_message.get('bank_name', None)


    failure_kafka_payload = {
        'webhook_url': webhook_url,
        'session_id': session_id,
        'bank': bank_name,
        'notification_details': {},
        'event_type': 'FAILURE_NOTIFICATION',
        'mode': 'aa'
    }

    if consent_id == None or session_id == None or consent_handle_id == None or customer_id == None or polling_url == None or finsense_token == None or organization_id == None:
        print("something was None:- consent_id: {}, session_id: {}, consent_handle_id: {}, customer_id: {}, polling_url: {}, finsense_token: {}, organization_id: {}".format(consent_id, session_id, consent_handle_id, customer_id, polling_url, finsense_token, organization_id))
        failure_kafka_payload['notification_details'] = {
            'error_code': 'INCORRECT_DATA_RECIEVED',
            'error_message': 'We recieved incorrect data from the account aggregator'
        }
        call_django_to_insert_data_into_kafka(KAFKA_TOPIC_WEBHOOK_SEND, failure_kafka_payload)
        return

    # pre-defining the next sqs message to avoid code repeatition
    next_sqs_message = {
        "consent_id": consent_id,
        "consent_handle_id": consent_handle_id,
        "session_id": session_id,
        "customer_id": customer_id,
        "count": count + 1,
        "polling_url": polling_url,
        "finsense_token": finsense_token,
        "organization_id": organization_id
    }

    finsense_polling_api_url = polling_url.format(consent_id, session_id, consent_handle_id, customer_id)
    print("url was: {}".format(finsense_polling_api_url))
    finsense_request_headers = {
        'Authorization': finsense_token, # TODO
        'Content-Type': 'application/json'
    }

    response = requests.get(url=finsense_polling_api_url, headers=finsense_request_headers)

    if response.status_code != 200:
        if count <= MAX_RETRIES:
            print(f"non 200 response recieved - max retries are not reached sending message to queue again")
            send_message_to_finvu_queue(next_sqs_message)

            failure_kafka_payload['notification_details'] = {
                'error_code': 'MAX_POLLING_INTERVAL_EXCEEDED',
                'error_message': 'Max polling interval exceeded for the session'
            }

            call_django_to_insert_data_into_kafka(KAFKA_TOPIC_WEBHOOK_SEND, failure_kafka_payload)
        return

    response_json = json.loads(response.text)

    status = response_json["body"].get("fiRequestStatus", None)

    print("status recieved in background for consent_handle_id: {} was: {}".format(consent_handle_id, status))

    portal_api_url = f"{DJANGO_BASE_URL}/bank-connect/v1/account_aggregator/update_consent_status_callback/"
    portal_request_headers = {
        "x-api-key": API_KEY,
        "Content-Type": "application/json"
    }

    any_error_message = ''
    if status == "READY" or status == "FAILED":
        if status == "FAILED":
            any_error_message = 'AA_DATA_RETRIEVAL_FAILED'
            update_failed_pdf_status(session_id, 'Data could not be retrieved from the Account Aggregator')

            failure_kafka_payload['notification_details'] = {
                'error_code': 'INCORRECT_DATA_RECIEVED',
                'error_message': 'We recieved incorrect data from the account aggregator'
            }
            call_django_to_insert_data_into_kafka(KAFKA_TOPIC_WEBHOOK_SEND, failure_kafka_payload)
        # TODO: call the portal api to update the status
        payload = json.dumps({
            "consent_handle_id": consent_handle_id,
            "session_id": session_id,
            "status": status,
            "any_error_message": any_error_message
        })

        try:
            response = requests.post(url=portal_api_url, headers=portal_request_headers, data=payload)
            print("Response status code: {}".format(response.status_code))
            print("Response text: {}".format(response.text))
        except Exception as e:
            capture_exception(e)
            print(e)

        return

    # if count is more than max retries we explcitly say the status is FAILED
    if count >= MAX_RETRIES:
        any_error_message = 'MAX_RETRIES_EXCEEDED'
        update_failed_pdf_status(session_id, 'Data could not be retrieved from the Account Aggregator')
        payload = json.dumps({
            "consent_handle_id": consent_handle_id,
            "session_id": session_id,
            "status": "FAILED",
            "any_error_message": any_error_message
        })

        try:
            response = requests.post(url=portal_api_url, headers=portal_request_headers, data=payload)
            print("Response status code: {}".format(response.status_code))
            print("Response text: {}".format(response.text))
        except Exception as e:
            print(e)

        failure_kafka_payload["notification_details"] = {
            "error_code": "MAX_RETRIES_EXCEEDED",
            "error_message": "Data could not be retrieved from the Account Aggregator"
        }
        call_django_to_insert_data_into_kafka(KAFKA_TOPIC_WEBHOOK_SEND, failure_kafka_payload)
        return

    # if the status was not READY / FAILED and count < MAX_RETRIES
    # enter a new message into sqs for retrying
    send_message_to_finvu_queue(next_sqs_message)
    

def send_message_to_finvu_queue(queue_message):
    try:
        # if we are adding a message into the queue again, means
        # the status is still in PENDING state
        # sleeping for 2 seconds, before putting message again into the queue
        sleep(2)

        queue_response = sqs_client.send_message(
            QueueUrl=FINVU_AA_REQUEST_STATUS_POLLING_JOBS_QUEUE_URL,
            MessageBody=json.dumps(queue_message)
        )

        print("sent message to queue again, message was: {}, queue_response was: {}".format(queue_message, queue_response))
    except Exception as e:
        capture_exception(e)
        print("Exception: {}".format(e))