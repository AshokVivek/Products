import json
import requests
import time
import warnings
import pandas as pd
import re
from datetime import datetime
from sentry_sdk import capture_exception
from botocore.exceptions import ClientError

from library.transaction_channel import get_transaction_channel
from library.transaction_description import get_transaction_description
from library.utils import add_hash_to_transactions_df, get_date_format, convert_pandas_timestamp_to_date_string, get_amount_sign
from python.aggregates import get_scanned_webhook_callback_payload, update_progress, get_statement_ids_for_account_id
# from library.extract_txns_fitz import remove_opening_balance, remove_closing_balance
from library.statement_plumber import amount_to_float, get_transaction_type, get_amount
from python.identity_handlers import get_account, add_statement_to_account, create_new_account, update_failed_pdf_status
from python.enrichment_regexes import check_and_get_everything
from library.fraud import transaction_balance_check, optimise_transaction_type
from python.configs import *
from python.configs import CATEGORIZE_RS_PRIVATE_IP, bank_connect_transactions_table, s3, bank_connect_identity_table
from library.validations import back_fill_balance, front_fill_balance
from python.handlers import update_field_for_statement, update_bsa_extracted_count
from category import SingleCategory
from library.fitz_functions import read_pdf
from python.utils import update_page_cnt_and_page_done
from python.analyze_textract import TextractExtractor
from python.aggregates import process_and_optimize_transactions
from python.utils import send_message_to_slack
from python.api_utils import call_api_with_session

warnings.simplefilter(action = "ignore", category = FutureWarning)
pd.options.mode.chained_assignment = None


def get_transaction_per_page(cells,page_no):
    max_length = 0
    for cell in cells:
        max_length = max(max_length,cell['row'])
    trans = list()
    for i in range(0, max_length):
        trans.append(dict())

    for cell in cells:
        if cell['label'] == 'Transaction_date' and 'Transaction_date' in trans[cell['row']-1].keys():
            trans[cell['row']-1]['Transaction_date1']=cell['text']
        else:
            trans[cell['row']-1][cell['label']]=cell['text']
    return {
        "page_no":page_no,
        "transactions":trans
    }

def get_mapped_data_for_webhook(result_list):
    final_result = dict()
    identity = dict()
    transactions = list()
    labels_present = set()

    predictions = result_list.get('result', dict()).get('moderated_boxes', list())
    if len(predictions) == 0:
        predictions = result_list.get('result', dict()).get('prediction_boxes', list())
    if len(predictions) == 0:
        predictions = result_list.get('result', dict()).get('prediction', list())
    for prediction in predictions:
        label = prediction['label']
        labels_present.add(label)
        if label == 'table':
            transactions.append(get_transaction_per_page(prediction['cells'], prediction['page']))
        elif label == 'type_of_document':
            pass
        else:
            identity[label] = prediction['ocr_text']

    final_result['identity'] = identity
    final_result['transactions'] = transactions
    return final_result, labels_present

def get_mapped_data_for_sync_upload(result_list):
    final_result = dict()
    identity = dict()
    transactions = list()

    for page in result_list:
        prediction = page['prediction']
        for item in prediction:
            label = item['label']
            ocr_text = item['ocr_text']
            if label != 'table':
                identity[label] = ocr_text
            else:
                transactions.append(get_transaction_per_page(item['cells'],item['page_no']))
    final_result['identity'] = identity
    final_result['transactions'] = transactions
    return final_result

bc_nanonets_mapping = {
    "Account_address":"address",
    "Account_name":"name",
    "Account_number":"account_number",
    "Bank_name":"bank_name",
    "Transaction_date":"date",
    "Transaction_date1":"date1",
    "Date_posted":"value_date",
    "Debit":"debit",
    "Credit":"credit",
    "Balance":"balance",
    "Description":"transaction_note",
    "Amount":"amount",
    "Transaction_type": "transaction_type",
    "IFSC": "ifsc",
    "MICR": "micr"
}

nanonets_bc_mapping = { 
    'address': 'Account_address', 
    'name': 'Account_name', 
    'account_number': 'Account_number', 
    'bank_name': 'Bank_name', 
    'date': 'Transaction_date', 
    'date1': 'Transaction_date1',
    'value_date': 'Date_posted', 
    'debit': 'Debit', 
    'credit': 'Credit', 
    'balance': 'Balance', 
    'transaction_note': 'Description', 
    'amount': 'Amount',
    'transaction_type': 'Transaction_type', 
    'ifsc': 'IFSC', 
    'micr': 'MICR',
    'chq_num': 'Ref No./Cheque No.'
}

identity_fileds = ['account_number','address','account_id','name','micr','account_category','credit_limit','ifsc','bank_name','metadata_analysis']

def get_basic_words_remove_from_float(value):
    if value == None or isinstance(value, str) == False:
        return value
    value = value.replace('\n', '')
    for i in range(65,91):
        value = value.replace(chr(i), '')
        value = value.replace(chr(i+32), '')
    dot_count = value.count('.')
    if dot_count>1:
        new_value = ""
        for i in range(len(value)):
            if value[i] != '.':
                new_value += value[i]
            elif value[i] == '.' and  dot_count == 1:
                new_value += value[i]
            else:
                dot_count -= 1
        value = new_value
    return value
    

def map_bc_nanonets_transactions(transactions,bank_name):
    bc_transactions_list = list()
    final_transaction_fields = ['date','transaction_type','amount','transaction_note','balance']
    for transaction in transactions["transactions"]:
        bc_transaction = dict()
        for key in transaction.keys():
            mapped_key = bc_nanonets_mapping.get(key,key)    
            bc_transaction[mapped_key] = transaction[key]

        bc_transaction['transaction_type'] = get_transaction_type({'transaction_type':bc_transaction.get('transaction_type', None),'credit':bc_transaction.get('credit',None),'debit':bc_transaction.get('debit',None),'amount': bc_transaction.get('amount',None)}, "IN")
        amount = get_amount({'credit':bc_transaction.get('credit',None),'debit':bc_transaction.get('debit',None),'amount': bc_transaction.get('amount',None)},bank_name)

        amount = get_basic_words_remove_from_float(amount)
        amount = amount_to_float(amount)

        raw_balance = bc_transaction.get('balance')
        balance_sign = get_amount_sign(raw_balance, bank_name)
        balance = amount_to_float(get_basic_words_remove_from_float(raw_balance))
        
        if balance_sign is not None and balance is not None:
            balance = balance_sign * balance

        if amount == None:
            amount = -10101010.1
        if balance == None:
            balance = -10101010.1
        bc_transaction['amount'] = amount
        bc_transaction['balance'] = balance

        date_or_flag = get_date_format(bc_transaction.get('date', None))
        if date_or_flag == False:
            date_or_flag = get_date_format(bc_transaction.get('value_date', None))
        if date_or_flag == False:
            date_or_flag = get_date_format(bc_transaction.get('date1', None))
        if date_or_flag == False:
            bc_transaction['date'] = None
        else:
            bc_transaction['date'] = date_or_flag 

        #as of now marking as None, in future TODO we will remove such transaction with null date
        if bc_transaction['date'] == None:
            bc_transaction['date'] = datetime.strptime('10-04-2015', '%d-%m-%Y')
        if bc_transaction.get('transaction_note', None) == None:
            bc_transaction['transaction_note'] = ''
        
        bc_transaction_keys = list(bc_transaction.keys())
        for txn_field in bc_transaction_keys:
            if txn_field not in final_transaction_fields:
                bc_transaction.pop(txn_field, None)

        bc_transactions_list.append(bc_transaction)
    return {
        'page_no':transactions['page_no'],
        'transactions':bc_transactions_list
    }

def get_nanonets_data_from_s3_dump(entity_id, statement_id):
    key = "nanonets/{}.json".format(statement_id)
    dump_data = None
    try:
        response = s3.get_object(Bucket=BANK_CONNECT_DUMP_BUCKET, Key=key)
        print("Object exists in s3, serving from here.")
        
        # write a temporary file with content
        file_path = "/tmp/{}.json".format(entity_id)
        with open(file_path, 'wb') as file_obj:
            file_obj.write(response['Body'].read())
        dump_data = json.load(open(file_path))
    
    except ClientError as e:
        if e.response['Error']['Code'] == 'NoSuchKey':
            print("Object {} does not exist in {}".format(key, BANK_CONNECT_UPLOADS_BUCKET))
    return dump_data

def remove_opening_balance(page):
    headers=["Opening\s*Balance", "Balance\s*Brought\s*Forward", "B\s*/\s*F", "BROUGHT\s*FORWARD"]
    removed_date = None
    for element in headers:
        if len(page) > 0 and re.match(element, page[0]["transaction_note"].strip().replace(" ",""), re.IGNORECASE):
            removed_date = page[0]["date"]
            page=page[1:]
    return page, removed_date

def remove_closing_balance(page):
    headers=["Closing\s*Balance", "CARRIED\s*FORWARD", "Page\s*Total", "Total", "GRAND\s*TOTAL"]
    removed_date = None
    for element in headers:
        if len(page) > 0 and re.match(element, page[-1]["transaction_note"].strip().replace(" ",""), re.IGNORECASE):
            removed_date = page[-1]['date']
            page=page[:-1]
    return page, removed_date

def update_progress_on_dashboard_for_nanonets(statement_id, payload):
    print('calling nanonets dashboard API for statement_id: {}'.format(statement_id))
    url = '{}/bank-connect/v1/internal/{}/update_nanonets_status/'.format(DJANGO_BASE_URL, statement_id)

    headers = {
        'x-api-key': API_KEY,
        'Content-Type': "application/json",
    }

    retries = 3
    sleep_duration = 5  # in seconds
    while retries:
        response = call_api_with_session(url,"POST", json.dumps(payload), headers)
        print('retries {} and response status {} and response {} and payload {}'.format(retries, response.status_code, response.text, payload))
        if response.status_code == 200:
            break
        retries -= 1
        time.sleep(sleep_duration)
    return (retries != 0)

def call_nanonets_for_manual_check(nanonets_request_id, inference_id, statement_id, label, manual_check_message, labels_present):
    print("Calling nanonets for manual check request for statement_id", statement_id)
    get_inferences_url = 'https://app.nanonets.com/api/v2/Inferences/Model/'+ NANONETS_MODEL_ID + '/ImageLevelInferences/'+inference_id

    retries = 3
    sleep_duration = 5  # in seconds
    inference_data = None
    while retries:    
        response = requests.request('GET', get_inferences_url, auth=requests.auth.HTTPBasicAuth(NANONETS_API_KEY, ''))
        log_to_dashboard_nanonets(nanonets_request_id, statement_id, 'POLLING', json.dumps(response.json()))

        response_json = response.json()
        if response.status_code==200 and isinstance(response_json, dict) and 'result' in response_json.keys() and len(response_json['result']) > 0:
            inference_data = response_json['result'][0]
            break
        retries -= 1
        time.sleep(sleep_duration)
    if inference_data == None:
        return 'failed', 'Nanonets polling api failed', 'Scanned statement - extraction failed'

    update_inference_request_body = {}

    update_inference_request_body['moderated_boxes'] = inference_data['moderated_boxes']
    update_inference_request_body['id'] = inference_id
    update_inference_request_body['day_since_epoch'] = inference_data['day_since_epoch']
    update_inference_request_body['hour_of_day'] = inference_data['hour_of_day']

    # mark_any = False
    # label_to_mark = nanonets_bc_mapping.get(label, None)
    # if label_to_mark == None or label_to_mark not in labels_present:
    #     mark_any = True
    mark_any = True

    for predicted_box in inference_data['predicted_boxes']:
        # if predicted_box['label'] == label_to_mark or mark_any:
        if mark_any:
            moderated_box = predicted_box
            moderated_box['validation_status'] = 'failed'
            moderated_box['validation_message'] = manual_check_message
            update_inference_request_body['moderated_boxes'].append(moderated_box)
            break

    update_inferences_moderated_boxes_url = "https://app.nanonets.com/api/v2/Inferences/Model/" + NANONETS_MODEL_ID + "/ImageLevelInferenceModeratedBoxes"

    response = requests.patch(update_inferences_moderated_boxes_url, auth=requests.auth.HTTPBasicAuth(NANONETS_API_KEY, ''), json=update_inference_request_body)
    print("status={} for manual review request api for statement_id = {}".format(statement_id, response.status_code))
    log_to_dashboard_nanonets(nanonets_request_id, statement_id, 'MANUAL_REQUEST', json.dumps(response.json()))

    if response.status_code != 200:
        return 'failed', 'Nanonets manual request api failed', 'Scanned statement - extraction failed'
    return 'manual_check_stage2', manual_check_message, 'Scanned Statement - Raised for manual review'

def check_for_manual_stage2(identity, transactions):
    if identity.get('name', None) == None:
        return True, 'name', 'name is null'
    if identity.get('account_number', None) == None:
        return True, 'account_number', 'account_number is null'
    bank_name = identity.get("bank_name", "")
    predicted_bank = identity.get('predicted_bank', '')
    if isinstance(bank_name, str) and isinstance(predicted_bank, str) and  bank_name.lower() != predicted_bank.lower():
        return True, 'bank_name', 'bank_name_mismatch'
    
    is_any_balance_default = False
    is_any_amount_default = False
    is_any_transaction_type_empty = False
    is_any_date_default = False
    is_any_date_empty = False
    for transaction in transactions:
        if transaction['amount'] == -10101010.1:
            is_any_amount_default = True
        if transaction['balance'] == -10101010.1:
            is_any_balance_default = True
        if transaction['transaction_type'] == None or transaction['transaction_type'] == '':
            is_any_transaction_type_empty = True
        if transaction['date'] == None:
            is_any_date_default = True
        if transaction['date'] == datetime.strptime('10-04-2015', '%d-%m-%Y'):
            is_any_date_empty = True
        transaction["optimizations"] = []
    if is_any_balance_default:
        return True, 'balance', 'balance is set to default'
    if is_any_amount_default:
        return True, 'amount', 'amount is set to default'
    if is_any_date_default:
        return True, 'date', 'date is set to default'
    if is_any_transaction_type_empty:
        return True, 'transaction_type', 'transaction_type is empty'
    if is_any_date_empty:
        return True, 'date', 'date is empty'
    
    try:
        transactions, _pages_updated, _num_optimization , _= optimise_transaction_type(
            transactions_dict=transactions,
            bank=bank_name
        )
        print(f"Optimize transaction Type completed with {_num_optimization} optimizations.")
    except Exception as e:
        print(f"Exception arose in Scanned PDF - {e}")

    inconsistent_flag = transaction_balance_check(transactions, bank_name)
    if inconsistent_flag:
        print(f"Inconsistent Hash Found - {inconsistent_flag}")
        return True, 'amount', 'inconsistent_transaction'
    return False, None, None

def update_message_in_bsa_status(statement_id, message):
    bank_connect_statement_table.update_item(
        Key={
            'statement_id': statement_id},
        UpdateExpression="set message = :m",
        ExpressionAttributeValues={
            ':m': message
        }
    )

def log_to_dashboard_nanonets(nanonets_request_id, statement_id, event_type, dump):
    """
    This helper method calls the internal nanonets logging
    API with provided inputs.
    """
    dashboard_logging_url = "{}/bank-connect/v1/internal/{}/nanonets_logging/".format(DJANGO_BASE_URL, statement_id)
    
    request_headers = {
        'x-api-key': API_KEY,
        'Content-Type': "application/json",
    }

    request_payload = {
        "nanonets_request_id": nanonets_request_id,
        "statement_id": statement_id,
        "event_type": event_type,
        "dump": dump
    }

    # we will try to log only once
    try:
        response = requests.post(
            url=dashboard_logging_url,
            headers=request_headers,
            data=json.dumps(request_payload)
        )
        
        print("nanonets logging api response status code: {}, response data: {}".format(response.status_code, response.text))
    except Exception as e:
        print("some error occured while logging nanonets related data to dashboard, exception: {}".format(e))

def nanonets_integration(event, context):
    bucket = event['bucket']
    key = event['key']
    re_extraction = event.get('re_extraction', False)
    nanonets_request_id = event.get('nanonets_request_id', None)
    webhook_type = event.get('webhook_type', None)
    type_of_document = event.get('type_of_document', None)
    inference_id = event.get('inference_id', None)

    response = s3.get_object(Bucket=bucket, Key=key)

    pdf_metadata = response.get('Metadata')
    bank_name = pdf_metadata.get('bank_name')
    password = pdf_metadata.get('pdf_password')
    entity_id = pdf_metadata.get('entity_id')
    statement_id = pdf_metadata.get('statement_id')
    org_metadata = event.get('org_metadata', dict())

    path = f"/tmp/{statement_id}.pdf"
    with open(path, "wb+") as file_obj:
        file_obj.write(response['Body'].read())
    
    doc = read_pdf(path, password)
    number_of_pages = doc.page_count

    check_and_get_everything(bank_name)  

    print('updating processing status for st id {}'.format(statement_id))
    update_progress(statement_id, 'identity_status', 'processing', '')
    update_progress(statement_id, 'processing_status', 'processing', '')
    update_progress(statement_id, 'transactions_status', 'processing', '')

    #not calling dashboard api to update status because it is already updated before invoking this lambda
    if type_of_document == 'others':
        update_failed_pdf_status(statement_id, 'Scanned Statement - Not a valid bank statement')
        return
    if webhook_type == 'RANDOM_WEBHOOK' or webhook_type == 'REJECT_WEBHOOK':
        update_failed_pdf_status(statement_id, 'Scanned Statement - Extraction failed')
        return
    elif webhook_type == 'INFERENCE_WEBHOOK':
        update_message_in_bsa_status(statement_id, "Scanned Statement - Raised for manual review")
        return

    data = get_nanonets_data_from_s3_dump(entity_id, statement_id)
    if data is None:
        print("No data found in s3 dump")
        update_failed_pdf_status(statement_id, "Scanned Statement - Extraction failed")
        update_progress_on_dashboard_for_nanonets(statement_id, {'nanonets_request_id':nanonets_request_id, 'status_type':'failed', 'message': 'No data found in s3 dump'})
        return "failed"

    #after updating identity, account removing cache from s3
    folder_name ="entity_" + entity_id + "/"
    try:
        BANK_CONNECT_CACHEBOX_RESOURCE.objects.filter(Prefix=folder_name).delete()
    except:
        print("Could not delete cache from s3 for entity_id = {}".format(entity_id))

    #nanonets_data contains identity, transactions in nanonets format
    nanonets_data, labels_present = get_mapped_data_for_webhook(data)
    from_date = get_date_format(nanonets_data.get('identity').pop('Period_from', None))
    to_date = get_date_format(nanonets_data.get('identity').pop('Period_to', None))
    if isinstance(from_date, datetime) and isinstance(to_date, datetime):
        from_date = from_date.strftime('%Y-%m-%d')
        to_date = to_date.strftime('%Y-%m-%d')
    else:
        from_date = None
        to_date = None

    #starting data conversion into bank conenct format and then adding into ddb
    identity = dict()
    for key in nanonets_data['identity'].keys():
        mapped_key  = bc_nanonets_mapping.get(key, None)
        if mapped_key:
            identity[mapped_key]=nanonets_data['identity'][key]
    #adding defaults in identity 
    for key in identity_fileds:
        if key == 'metadata_analysis':
            identity[key] = {'name_matches':list()}
        else:
            identity[key] = identity.get(key, None)

    account_number = identity.get('account_number',None)
    account = get_account(entity_id, account_number) 
    account_id = account.get('account_id', None) if account is not None else None

    #creating new entry in ddb for account if account_id is null else adding statement into account
    if not account_id:
        account_id = create_new_account(
            entity_id, bank_name, account_number, statement_id, 
                identity.get('ifsc', None), 
                identity.get('micr', None), 
                identity.get('account_category', None),
                identity.get('is_od_account', None),
                identity.get('od_limit', None),
                identity.get('credit_limit', None),
                None,None)
    else:
        account_statement_ids = get_statement_ids_for_account_id(entity_id, account_id)
        # Don't add the statement_id to the account on `re_extraction` case or another webhook from nanonets
        if statement_id in account_statement_ids:
            re_extraction = True
        if not re_extraction:
            add_statement_to_account(entity_id, account_id, statement_id)

    identity['bank_name'] = bank_name
    identity['account_id'] = account_id
    predicted_bank = identity.pop('predicted_bank', None)
    final_identity = {'identity': identity}

    final_identity['date_range'] = {'from_date': None, 'to_date': None}
    final_identity['extracted_date_range'] = {'from_date': from_date, 'to_date':to_date}
    final_identity['page_count'] = number_of_pages
    #adding identity into ddb

    time_stamp_in_mlilliseconds = time.time_ns()
    dynamo_object = {
        'statement_id': statement_id,
        'item_data': final_identity,
        'created_at': time_stamp_in_mlilliseconds,
        'updated_at': time_stamp_in_mlilliseconds
    }
    bank_connect_identity_table.put_item(Item=dynamo_object)

    identity['predicted_bank'] = predicted_bank
    
    final_bc_transactions_list = list()
    no_of_pages = 0
    is_zero_indexed = False
    for trans in nanonets_data['transactions']:
        page_no = int(trans['page_no'])
        no_of_pages = max(no_of_pages, page_no)
        if page_no == 0:
            is_zero_indexed = True
        final_bc_transactions_list.append(map_bc_nanonets_transactions(trans,bank_name))
    if is_zero_indexed:
        no_of_pages += 1

    page_wise_transactions = dict()
    for transaction_page in final_bc_transactions_list:
        page_no = int(transaction_page['page_no'])
        if page_no not in page_wise_transactions.keys():
            page_wise_transactions[page_no] = list()
        page_wise_transactions[page_no].extend(transaction_page['transactions'])

    all_bc_transactions = list()
    for page_no in range(no_of_pages):
        transactions = page_wise_transactions.get(page_no, list())
        if bank_name in ['bcabnk']:
            balance_populated_transactions = back_fill_balance([transactions], -1)
            transactions = balance_populated_transactions[0]
        if bank_name in ['baroda']:
            opening_balance = identity.get('opening_bal')
            closing_balance = identity.get('closing_bal')
            balance_populated_transactions, _ = front_fill_balance([transactions], opening_balance=opening_balance, closing_balance=closing_balance, default_balance=-10101010.1, bank='baroda')
            transactions = balance_populated_transactions[0]
        transactions_df = pd.DataFrame(transactions)
        if len(transactions_df.columns) == 0:
            trans_dict = []
            time_stamp_in_mlilliseconds = time.time_ns()
            dynamo_object = {
                'statement_id': statement_id,
                'page_number': page_no,
                'item_data': json.dumps(trans_dict, default=str),
                'transaction_count': len(trans_dict),
                'created_at': time_stamp_in_mlilliseconds,
                'updated_at': time_stamp_in_mlilliseconds
            }
            bank_connect_transactions_table.put_item(Item=dynamo_object)
            continue
            
        transactions_df['transaction_note'] = transactions_df['transaction_note'].replace('\n', '', regex=True)
        trans_df = get_transaction_channel(transactions_df, bank_name)
        trans_df = get_transaction_description(trans_df,identity.get('name',None))
        trans_df = add_hash_to_transactions_df(trans_df)
        trans_dict, removed_opening_date = remove_opening_balance(trans_df.to_dict('records'))
        trans_dict, removed_closing_date = remove_closing_balance(trans_dict)

        if removed_opening_date is not None:
            removed_opening_date = convert_pandas_timestamp_to_date_string(removed_opening_date)
            update_field_for_statement(statement_id, f'removed_date_opening_balance_{page_no}', removed_opening_date)
        
        if removed_closing_date is not None:
            removed_closing_date = convert_pandas_timestamp_to_date_string(removed_closing_date)
            update_field_for_statement(statement_id, f'removed_date_closing_balance_{page_no}', removed_closing_date)
        
        categorizer = SingleCategory(bank_name=bank_name, transactions=trans_dict, categorize_server_ip=CATEGORIZE_RS_PRIVATE_IP)
        trans_dict = categorizer.categorize_from_forward_mapper()
        print("Number of transactions on page_no = {} is {}".format(page_no, len(trans_dict)))
        all_bc_transactions.extend(trans_dict)

        time_stamp_in_mlilliseconds = time.time_ns()
        dynamo_object = {
            'statement_id': statement_id,
            'page_number': page_no,
            'item_data': json.dumps(trans_dict, default=str),
            'transaction_count': len(trans_dict),
            'created_at': time_stamp_in_mlilliseconds,
            'updated_at': time_stamp_in_mlilliseconds
        }
        bank_connect_transactions_table.put_item(Item=dynamo_object)
    
    #updating bsa page count table
    update_page_cnt_and_page_done(statement_id, no_of_pages, no_of_pages)

    #TODO check for manual request from nanonetes and make an api call call to nanonets 
    # add in ddb also regarding maunal request and don't call second time for manual
    check_for_manual_check, lable_name, manual_check_message = check_for_manual_stage2(identity, all_bc_transactions)
    if check_for_manual_check:
        status_type, rds_message, ddb_message = call_nanonets_for_manual_check(nanonets_request_id,inference_id, statement_id, lable_name, manual_check_message, labels_present)
        data_to_send = {
            'nanonets_request_id':nanonets_request_id,
            'status_type': status_type,
            'message': rds_message
        }
        if status_type == 'failed':
            update_failed_pdf_status(statement_id, ddb_message)
        else:
            update_message_in_bsa_status(statement_id, ddb_message)
        statements_in_processing, scanned_callback_payload = get_scanned_webhook_callback_payload(entity_id)
        if not statements_in_processing:
            data_to_send['nanonets_webhook_config'] = {
                'send_webhook': True,
                'callback_payload': scanned_callback_payload
            }
        update_progress_on_dashboard_for_nanonets(statement_id, data_to_send)
        return
    ## manual request ends here

    #trigger update state fan out
    queue_payload = json.dumps({
                            "entity_id": entity_id,
                            "statement_id": statement_id,
                            "org_metadata": org_metadata
                        })
    
    if re_extraction:
        print("Re extration case invoking lambda directly")
        try:
            lambda_client.invoke(
                FunctionName = UPDATE_STATE_FAN_OUT_FUNCTION, 
                Payload = queue_payload, 
                InvocationType = 'Event'
            )
        except Exception as e:
            print(e) 
    else:
        print("Invoking update_state_fan_out lambda via events in SQS")
        try:
            sqs_push_response = sqs_client.send_message(
                QueueUrl = UPDATE_STATE_FAN_OUT_INVOCATION_QUEUE_URL,
                MessageBody = queue_payload,
                MessageDeduplicationId = '{}_{}'.format(entity_id, statement_id),
                MessageGroupId = 'update_state_invocation_{}'.format(statement_id)
            )
            print(sqs_push_response)
        except Exception as e:
            capture_exception(e)
            print("Failed to push into update-state-fan-out sqs")
    return "success"

def textract_event_handler(event, context):
    to_fail = event["to_fail"]
    item_data = event["item_data"]
    statement_id = event["statement_id"]
    entity_id = event["entity_id"]
    bank_name = event["bank_name"]
    page_count = event["page_count"]
    file_key = event["file_key"]
    bucket = event["bucket"]
    template = event["template"]
    organization_id = event["organization_id"]
    organization_name = event["organization_name"]

    to_fail_status = to_fail.get("to_fail_status")
    to_fail_message = to_fail.get("to_fail_message")
    statement_meta_data_for_warehousing = event.get('statement_meta_data_for_warehousing', dict())

    check_and_get_everything(bank_name=bank_name, country="IN")

    if to_fail_status:
        update_failed_pdf_status(statement_id, to_fail_message)
        return

    tex = TextractExtractor(
        entity_id=entity_id,
        statement_id=statement_id,
        bank_name=bank_name,
        bucket_name=bucket,
        destination_bucket_name=bucket,
        template=template,
        page_count=page_count,
        pdf_key=file_key
    )
    
    tex.identity_for_textract(
        item_data=item_data,
        statement_id=statement_id,
        entity_id=entity_id,
        bank_name=bank_name,
        page_count=page_count,
        file_key=file_key,
        bucket=bucket
    )
    
    textract_job_id_map = tex.textract_job_id_map
    
    xlsx_paths = []
    for page in range(page_count):
        job_id = textract_job_id_map[page]
        xlsx_path = tex.wait_for_textract_job(get_paths=True, page_job_id=job_id, page_num=page)
        xlsx_paths.append(xlsx_path[0])
    
    tex.get_all_transactions(xlsx_paths=xlsx_paths)
    update_field_for_statement(
            statement_id=statement_id,
            field_name="is_extracted_by_textract",
            field_value=True
        )

    # TODO: Pass country - 'ID' for Indonesia, Get country for Identity / accounts
    transction_hash = process_and_optimize_transactions(statement_id, bank_name, 'pdf', 'IN', item_data, entity_id, warehousing_meta_data=statement_meta_data_for_warehousing)

    if transction_hash:
        # mark under manual review
        print(f"Statement {statement_id} has inconsistency at {transction_hash}, Raised for manual review")
        update_field_for_statement(statement_id, 'message', 'Scanned Statement - Raised for manual review')
        
        # here send slack message with inconsistent message and hash
        send_message_to_slack(
            SLACK_TOKEN=EXTRACTION_ISSUE_SLACK_TOKEN,
            SLACK_CHANNEL=EXTRACTION_ISSUE_SLACK_CHANNEL,
            message=f"""
                    Region: *{REGION}*
                    Environment: *{CURRENT_STAGE}* 
                    Statement ID: *{statement_id}*
                    Bank: *{bank_name}*
                    Organization_Id: *{organization_id}*
                    Organization_Name: *{organization_name}*
                    Stage: `Scanned Statement has inconsistency. Marking status as Scanned Statement - Raised for manual review`
                    Inconsistent Hash: `{transction_hash}`
                """
        )
    else:
        print(f"Statement {statement_id} Does not have inconsistency, Marking pages_done as page_count - 1 {page_count-1}")
        update_field_for_statement(
            statement_id=statement_id,
            field_name="pages_done",
            field_value=page_count - 1
        )
        print(f"Invoking update state fan out for statement id {statement_id}")
        update_bsa_extracted_count(
            entity_id=entity_id,
            statement_id=statement_id,
            page_number=page_count-1,
            number_of_pages=page_count
        )