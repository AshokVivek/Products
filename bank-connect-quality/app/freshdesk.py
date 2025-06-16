import datetime
import random
import time
import requests
from app.database_utils import prepare_clickhouse_client
from app.conf import FRESHDESK_APIKEY,FRESHDESK_URL, AWS_REGION
from .conf import redis_cli
import pytz
import os

def get_freshdesk_tickets():
    if AWS_REGION != 'ap-south-1':
        return
    
    print('Entered get_freshdesk_tickets function')
    api_key = FRESHDESK_APIKEY
    api_url= FRESHDESK_URL

    start_time_utc = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(hours=24)
    ist_timezone = pytz.timezone('Asia/Kolkata')
    start_time_ist = start_time_utc.astimezone(ist_timezone)
    start_time_ist_str = start_time_ist.strftime('%Y-%m-%dT%H:%M:%SZ')
    print('Date and time :', start_time_ist_str)
    params = {
        'updated_since': start_time_ist_str,
        'per_page': 100
    }
    id_lists=[]
    try:
        all_data = []
        page = 1
        
        while True:
            params['page'] = page
            response = requests.get(api_url, auth=(api_key, 'X'), params=params)
            response.raise_for_status()
            data = response.json()
            
            if not data:
                break

            all_data.extend(data)
            page += 1

        if all_data:
            final_json_list = []
            for data in all_data:
                try:
                    custom_fields = data.pop('custom_fields', {})
                    final_json = {**data, **custom_fields}
                    final_json_list.append(final_json)
                except KeyError as e:
                    print("KeyError occurred with data:", data)
                    continue

            id_lists = [[
                item['id'],
                item['requester_id'],
                item['responder_id'],
                item['status'],
                item['source'],
                item['spam'],
                datetime.datetime.strptime(item['created_at'],"%Y-%m-%dT%H:%M:%SZ"),
                datetime.datetime.strptime(item['updated_at'],"%Y-%m-%dT%H:%M:%SZ"),
                item['subject'],
                item['group_id'],
                datetime.datetime.strptime(item['due_by'],"%Y-%m-%dT%H:%M:%SZ"),
                datetime.datetime.strptime(item['fr_due_by'],"%Y-%m-%dT%H:%M:%SZ"),
                item['is_escalated'],
                item['priority'],
                item['fr_escalated'],
                item.get('to_emails', []) or [],
                item['email_config_id'],
                item['association_type'],
                item['product_id'],
                item['company_id'],
                item['support_email'],
                item['type'],
                item['associated_tickets_count'],
                item['tags'],
                item['cc_emails'],
                item['fwd_emails'],
                item['reply_cc_emails'],
                item.get('ticket_cc_emails', []) or [],
                item['cf_user_stage'],
                item['cf_dependency'],
                item['cf_fsm_contact_name'],
                item['cf_state'],
                item['cf_fsm_phone_number'],
                item['cf_fsm_service_location'],
                item['cf_fsm_appointment_start_time'],
                item['cf_fsm_appointment_end_time'],
            ] for item in final_json_list]
            print('data fetched')
            return id_lists
    except requests.exceptions.RequestException as e:
        print('Failed to fetch data:', e)




FRESHDESK_DATA_LOCK_FILE = '/tmp/freshdesk_cron_job.lock'
async def insert_freshdesk_data_to_clickhouse():
    print('Entered insert_freshdesk_data_to_clickhouse function')
    if os.path.exists(FRESHDESK_DATA_LOCK_FILE):
        print("Another instance is already running, skipping for this worker")
        return
    
    with open(FRESHDESK_DATA_LOCK_FILE, "w") as f:
        pass
    time.sleep(random.randint(1, 30))
    job_executed = redis_cli.get("freshdesk")
    if job_executed == '1':
        print("Insert data into clickhouse already Scheduled")
        os.remove(FRESHDESK_DATA_LOCK_FILE)
        return
    print("-- Performing Insertion JOB --")
    redis_cli.set("freshdesk", 1, 85000)
    id_lists = get_freshdesk_tickets()
    try:
        clickhouse_client= prepare_clickhouse_client()
        if id_lists:
            print('Inserting data into clickhouse')
            clickhouse_client.insert('freshdeskTicket', id_lists, column_names=[
                            'id',            
                            'requester_id',
                            'responder_id',
                            'status',
                            'source',
                            'spam',
                            'created_at',
                            'updated_at',
                            'subject',
                            'group_id',
                            'due_by',
                            'fr_due_by',
                            'is_escalated',
                            'priority',
                            'fr_escalated',
                            'to_emails',
                            'email_config_id',
                            'association_type',
                            'product_id',
                            'company_id',
                            'support_email',
                            'type',
                            'associated_tickets_count',
                            'tags',
                            'cc_emails',
                            'fwd_emails',
                            'reply_cc_emails',
                            'ticket_cc_emails',
                            'cf_user_stage',
                            'cf_dependency',
                            'cf_fsm_contact_name' , 
                            'cf_state',
                            'cf_fsm_phone_number',  
                            'cf_fsm_service_location',  
                            'cf_fsm_appointment_start_time',  
                            'cf_fsm_appointment_end_time',
                        ])
            print("data insertion successfull")
        else:
            print("No data found to insert into clickhouse")
    except Exception as e:
        print('Insertion data into clickhouse failed', e)
    print("Insertion task completed")
    os.remove(FRESHDESK_DATA_LOCK_FILE)
        


