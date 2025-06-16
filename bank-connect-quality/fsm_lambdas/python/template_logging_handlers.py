import json
import time
from python.configs import *
from python.api_utils import call_api_with_session

def log_fsmlib_template_handler(event, context):

    print("event received: {}".format(event))

    records = event.get("Records")

    for record in records:

        try:
            record_body = json.loads(record.get("body", ""))
        except Exception as e:
            print("could not parse body, record: {}, exception: {}".format(record, e))
            continue

        s3_file_key = record_body.get("s3_file_key", None)
        page_number = record_body.get("page_number", None)
        template_uuid = record_body.get("template_uuid", None)
        template = record_body.get("template", None)
        statement_id = record_body.get("statement_id", None)

        if not s3_file_key and not statement_id:
            print("s3_file_key or statement_id are incorrect - s3_file_key: {}, statement_id: {}".format(s3_file_key, statement_id))
            continue
        if page_number is None or page_number == "" or template_uuid is None or template_uuid == "":
            print("something was incorrect - page_number: {}, template_uuid: {}".format(page_number, template_uuid))
            continue

        print('calling dashboard API for template logging')
        
        '''
        Not logging template for dev environment
        '''
        if CURRENT_STAGE not in ['prod','uat']:
            return
        
        api_url = f'{DJANGO_BASE_URL}/bank-connect/v1/internal/log_fsmlib_template/'

        payload = {
            "s3_file_key": s3_file_key,
            "page_number": page_number,
            "template_uuid": template_uuid,
            "template": template,
            "statement_id": statement_id,
        }

        headers = {
            'x-api-key': API_KEY,
            'Content-Type': "application/json",
        }

        retries = 3
        sleep_duration = 5  # in seconds
        while retries:
            response = call_api_with_session(api_url,"POST", json.dumps(payload), headers)
            
            if response.status_code == 200:
                break
            retries -= 1
            time.sleep(sleep_duration)
        
        if retries == 0:
            print("could not call dashboard api for fsmlib template logging")
        else:
            print("successfully called dashboard api for fsmlib template logging")
