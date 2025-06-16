from app.database_utils import portal_db
from datetime import datetime, timedelta
from app.utils import trigger_update_state
import time
import os
import csv
from app.conf import s3_resource, FSM_ARBITER_BUCKET, s3
import traceback
from dateutil import tz
import json
from io import BytesIO
import random

UPDATE_STATE_CRON_LOCK_FILE = '/tmp/update_state.lock'
async def get_all_processing_statements_and_trigger_update_state(time_duration):

    done_statements = []

    sleep_time = random.randint(1,5)
    time.sleep(sleep_time)

    if os.path.exists(UPDATE_STATE_CRON_LOCK_FILE):
        print("Another instance is already running, skipping for this worker")
        return    

    with open(UPDATE_STATE_CRON_LOCK_FILE, "w") as f:
        pass
    current_time = datetime.now() - timedelta(seconds=time_duration + 2) - timedelta(minutes=20)
    formatted_time = current_time.strftime('%Y-%m-%d %H:%M:%S')

    to_time = datetime.now() - timedelta(minutes=20)
    formatted_to_time = to_time.strftime('%Y-%m-%d %H:%M:%S')

    portal_query = f"select s.statement_id as statement_id, e.entity_id as entity_id from bank_connect_statement s, bank_connect_entity e where e.id=s.entity_id and s.is_complete=false and s.is_external_xml_data=false and s.is_extracted_by_perfios=false and s.is_extracted_by_nanonets=false and s.statement_status not in (1,2,5,6,8,12,23,25) and s.bank_name!='NA' and s.created_at >= '{formatted_time}' and s.created_at <= '{formatted_to_time}' order by s.created_at desc"
    portal_data = await portal_db.fetch_all(portal_query)

    count = 0
    print(f'Total statements to retrigger : {len(portal_data)}')
    print(f'Total statements to retrigger : {len(portal_data)}')
    print(f'Total statements to retrigger : {len(portal_data)}')
    print(f'Total statements to retrigger : {len(portal_data)}')
    print(f'Total statements to retrigger : {len(portal_data)}')
    print(f'Total statements to retrigger : {len(portal_data)}')
    print(f'Total statements to retrigger : {len(portal_data)}')
    for statement_data in portal_data:
        statement_data = dict(statement_data)
        statement_id = statement_data.get('statement_id')
        entity_id = statement_data.get('entity_id')

        statement_data = {
            "statement_id":statement_id,
            "entity_id":entity_id,
            "failure_reason":"",
            "is_success":True
        }

        lambda_payload = {
            'statement_id':statement_id,
            'entity_id':entity_id
        }

        print(f'Triggering statement : {statement_id}, entity_id : {entity_id}, triggered : {count+1} number of statements',)
        count+=1
        time.sleep(20)
        try:
            _ = await trigger_update_state(lambda_payload, 'UPDATE_STATE_RETRIGGER_CRON', entity_id, statement_id)
        except Exception as e:
            print(e)
            print(traceback.format_exc())
            statement_data['failure_reason'] = e
            statement_data['is_success'] = False
        print(f'Triggered Statement : {statement_id}')

        done_statements.append(statement_data)

    ist_fomatted_time = datetime.now().replace(tzinfo=tz.gettz('UTC')).astimezone(tz.gettz('Asia/Kolkata')).strftime('%Y-%m-%d %H:%M:%S')
    stringified = json.dumps(done_statements)
    cache_bucket_key = f'retrigger_logs/{ist_fomatted_time}.json'
    bytesIO = BytesIO(bytes(stringified,encoding='utf8'))    
    with bytesIO as data:
        s3.upload_fileobj(data, FSM_ARBITER_BUCKET, cache_bucket_key)

    if os.path.exists(UPDATE_STATE_CRON_LOCK_FILE):
        os.remove(UPDATE_STATE_CRON_LOCK_FILE)