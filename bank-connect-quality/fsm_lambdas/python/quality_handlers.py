import json
import time

from python.configs import INTERNAL_QUALITY_CHECK_URL, BANK_CONNECT_QUALITY_PRIVATE_IP, QUALITY_ACCESS_CODE
from python.api_utils import call_api_with_session
def initiate_internal_quality_check_handler(event, context):
    """
    This lambda recieves messages from a queue
    QUEUE: bank-connect-statement-quality-analysis-tasks
    based on the data it calls the internal quality check apis
    """

    print("event received: {}".format(event))

    records = event.get("Records")

    for record in records:
        
        try:
            record_body = json.loads(record.get("body", ""))
        except Exception as e:
            print("could not parse record body, record: {}, exception: {}".format(record, e))
            continue

        statement_id = record_body.get("statement_id")
        entity_id = record_body.get("entity_id")
        internal_secret = record_body.get("internal_secret")

        if statement_id is None or statement_id == "":
            return {
                "message": "statement_id is required"
            }

        if entity_id is None or entity_id == "":
            return {
                "message": "entity_id is requried"
            }

        if internal_secret is None or internal_secret == "":
            return {
                "message": "internal_secret is requried"
            }

        # now simply call the internal quality check api
        internal_quality_api_paylaod = {
            "statement_id": statement_id,
            "entity_id": entity_id
        }

        headers = {
            'Content-Type': "application/json",
            "internal-secret": internal_secret
        }

        retries = 3
        sleep_duration = 5  # in seconds
        while retries:
            response = call_api_with_session(INTERNAL_QUALITY_CHECK_URL,"POST", json.dumps(internal_quality_api_paylaod), headers)
            if response.status_code == 200:
                break
            retries -= 1
            time.sleep(sleep_duration)
        
        if retries == 0:
            print("could not call the internal quality check api, response text: {}".format(response.text))
        else:
            print("successfully called the internal quality check api")


def invoke_quality_handler(event, context):
    
    if event.get("Records",None) and isinstance(event, dict):
        records = event.get("Records", None)
        
        if records is None or len(records) == 0:
            return
        
        statements = []
        for record in records:
            body = record.get("body", None)
            if body is None:
                continue
            
            event=json.loads(body)

            statement_id = event.get("statement_id", None)
            is_credit_card = event.get('is_credit_card', False)
            identity = event.get('identity', None)
            statements.append({
                'statement_id':statement_id,
                'is_credit_card':is_credit_card,
                'identity': identity
            })

            
        url = BANK_CONNECT_QUALITY_PRIVATE_IP+ "/ingest/ingest"
        
        payload = json.dumps({
            "statements": statements,
            "access_code": QUALITY_ACCESS_CODE
        })
        
        headers = {
            'Content-Type': 'application/json'
        }

        try:
            call_api_with_session(url,"POST", payload, headers)
        except Exception as e:
            print("Exception in calling quality ", e)