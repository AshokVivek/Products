import json
import requests
import xmltodict
import time
import re
import hashlib
from uuid import uuid4, UUID
from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError
from python.aggregates import update_progress, update_progress_on_dashboard, update_progress_fraud_status, get_fraud_progress
from python.aws_utils import collect_results
from sentry_sdk import capture_exception
from category import SingleCategory
from python.clickhouse.firehose import send_data_to_firehose
from python.constants import FRAUD_STATUS_TO_FRAUD_TYPE_MAPPING
from python.handlers import update_bsa_extracted_count
from python.identity_handlers import identity_handler
from python.aggregates import get_complete_identity_for_statement
from python.configs import CATEGORIZE_RS_PRIVATE_IP, bank_connect_identity_table, bank_connect_salary_table, bank_connect_statement_table, bank_connect_disparities_table, bank_connect_account_table, DJANGO_BASE_URL, API_KEY, PERFIOS_REPORT_FETCH_TASK_QUEUE_URL, \
    sqs_client, BANK_CONNECT_UPLOADS_REPLICA_BUCKET, s3, bank_connect_transactions_table, bank_connect_tmp_identity_table, TRANSACTIONS_STREAM_NAME, s3_resource, BANK_CONNECT_DDB_FAILOVER_BUCKET, bank_connect_recurring_table
from python.configs import PDF_PAGES_HASH_GENERATION_TASKS_QUEUE_URL, lambda_client, METADATA_FRAUDS_FUNCTION, BANK_CONNECT_CACHEBOX_RESOURCE
from python.utils import prepare_warehouse_data


def is_valid_and_format_uuid(uuid_str):
    try: 
        formatted_str = f'{uuid_str[:8]}-{uuid_str[8:12]}-{uuid_str[12:16]}-{uuid_str[16:20]}-{uuid_str[20:]}'
        uuid_str_converted = str(UUID(formatted_str, version=4))
        return True, uuid_str_converted
    except Exception:
        pass
    return False, None

# Helper methods
# Just to keep Perfios related code separated
def get_perfios_statement_progress(statement_id, status_type = "identity_status"):
    items = bank_connect_statement_table.query(KeyConditionExpression=Key('statement_id').eq(statement_id))

    if items.get('Count') == 0:
        return 'failed'

    entry = items.get('Items')[0]
    status = entry.get(status_type, 'processing')

    return status


def _get_perfios_xml_report_origin(perfios_xml_report_dict: dict) -> str:
    if not isinstance(perfios_xml_report_dict, dict):
        return None
    
    known_primary_keys = ['PIR:Data', 'IIFLXMLRoot', 'IIFLXML']
    for known_primary_key in known_primary_keys:
        perfios_xml_report_data = perfios_xml_report_dict.get(known_primary_key)
        if isinstance(perfios_xml_report_data, dict):
            return known_primary_key
    
    return None


def _get_origin_recalibrated_perfios_xml_report(perfios_xml_report_dict: dict) -> dict:
    if not isinstance(perfios_xml_report_dict, dict):
        return perfios_xml_report_dict
    
    perfios_xml_report_origin_key = _get_perfios_xml_report_origin(perfios_xml_report_dict)
    if isinstance(perfios_xml_report_origin_key, str):
        return perfios_xml_report_dict.get(perfios_xml_report_origin_key, dict())
    
    known_secondary_primary_keys = ["root"]
    for known_secondary_primary_key in known_secondary_primary_keys:
        perfios_xml_report_data = perfios_xml_report_dict.get(known_secondary_primary_key)
        if isinstance(perfios_xml_report_data, dict):
            return _get_origin_recalibrated_perfios_xml_report(perfios_xml_report_data)
    
    return perfios_xml_report_dict


def create_add_transaction_hash_perfios(transactions_list):
    # hack for arabic
    cid_regex = re.compile(r"(\(cid\:[0-9]+\))")
    only_char_digit_regex = re.compile(r"[^\w]+", re.UNICODE)
    total_transactions = len(transactions_list)
    for i in range(total_transactions):
        amount = transactions_list[i].get('amount')
        date = transactions_list[i].get('date')
        balance = transactions_list[i].get('balance')
        transaction_note = transactions_list[i].get('transaction_note')
        transaction_type = transactions_list[i].get('transaction_type')

        if i > 0:
            amount_1 = transactions_list[i-1].get('amount')
            date_1 = transactions_list[i-1].get('date')
            balance_1 = transactions_list[i-1].get('balance')
            transaction_note_1 = transactions_list[i-1].get('transaction_note')
            transaction_type_1 = transactions_list[i-1].get('transaction_type')
        else:
            amount_1 = ""
            date_1 = ""
            balance_1 = ""
            transaction_note_1 = ""
            transaction_type_1 = ""

        if i < (total_transactions - 1):
            amount_2 = transactions_list[i+1].get('amount')
            date_2 = transactions_list[i+1].get('date')
            balance_2 = transactions_list[i+1].get('balance')
            transaction_note_2 = transactions_list[i+1].get('transaction_note')
            transaction_type_2 = transactions_list[i+1].get('transaction_type')
        else:
            amount_2 = ""
            date_2 = ""
            balance_2 = ""
            transaction_note_2 = ""
            transaction_type_2 = ""

        to_be_hashed_list = [
            str(amount),
            str(date),
            str(balance),
            str(transaction_note),
            str(transaction_type),
            str(amount_1),
            str(date_1),
            str(balance_1),
            str(transaction_note_1),
            str(transaction_type_1),
            str(amount_2),
            str(date_2),
            str(balance_2),
            str(transaction_note_2),
            str(transaction_type_2)]
        to_be_hashed = "".join(to_be_hashed_list)

        to_be_hashed = to_be_hashed.replace(" ", "").replace("_", "")
        to_be_hashed = cid_regex.sub("", to_be_hashed)
        to_be_hashed = only_char_digit_regex.sub("", to_be_hashed)
        to_be_hashed = to_be_hashed.encode('utf-8')
        generated_hash = hashlib.md5(to_be_hashed)
        transactions_list[i]["hash"] = generated_hash.hexdigest()

    return transactions_list


def get_transactions_list_of_lists_perfios(transactions_list):
    transactions_list_of_lists = []

    if len(transactions_list) <= 30:
        transactions_list_of_lists.append(transactions_list)
    else:
        transactions_list_of_lists = [transactions_list[i:i+25] for i in range(0, len(transactions_list), 25)]
    
    return transactions_list_of_lists


def create_new_account_perfios(
        entity_id,
        bank,
        account_number,
        statement_id,
        ifsc=None,
        micr=None,
        account_category=None):
    account_id = str(uuid4())

    time_stamp_in_mlilliseconds = time.time_ns()
    dynamo_object = {
        'entity_id': entity_id,
        'account_id': account_id,
        'item_data': {
            'bank': bank,
            'account_number': account_number,
            'statements': [statement_id],
            'account_id': account_id,
            'ifsc': ifsc,
            'micr': micr,
            'account_category': account_category
        },
        'created_at': time_stamp_in_mlilliseconds,
        'updated_at': time_stamp_in_mlilliseconds
    }

    bank_connect_account_table.put_item(Item=dynamo_object)
    return account_id


def add_statement_to_account_perfios(entity_id, account_id, statement_id):
    try:
        bank_connect_account_table.update_item(
            Key={
                'entity_id': entity_id,
                'account_id': account_id
            },
            UpdateExpression="SET item_data.statements = list_append(item_data.statements, :i), updated_at = :u",
            ConditionExpression="NOT contains (item_data.statements, :ei)",
            ExpressionAttributeValues={
                ':i': [statement_id],
                ':ei': statement_id,
                ':u': time.time_ns()
            })
    except ClientError as e:
        if e.response["Error"]["Code"] == "ConditionalCheckFailedException":
            print("statement id: {} seems to be already present in the list".format(statement_id))
        else:
            raise e


def update_account_category_for_entity(entity_id, account_id, account_category):
    print("update account category ddb called...")
    bank_connect_account_table.update_item(
        Key={
            "entity_id": entity_id,
            "account_id": account_id,
        },
        UpdateExpression="SET item_data.account_category = :c, updated_at = :u",
        ExpressionAttributeValues={
            ":c": account_category,
            ":u": time.time_ns()
        }
    )


def get_account_id_perfios(entity_id, account_number, bank):
    if account_number is None:
        return None

    qp = {
        'KeyConditionExpression': Key('entity_id').eq(entity_id), 
        'ConsistentRead': True,
        'ProjectionExpression': 'entity_id,account_id,item_data'}
    accounts = collect_results(bank_connect_account_table.query, qp)

    for account in accounts:
        curr_account_number = account.get(
            'item_data', dict()).get('account_number', None)

        if curr_account_number is None:
            continue
        
        account_number_check = re.compile("[a-zA-Z]+", re.IGNORECASE)
        # Check if account numbers contains alphabets, then only check last 4 digits
        if account_number_check.findall(account_number) or account_number_check.findall(curr_account_number):
            if account_number[-4:] == curr_account_number[-4:]:
                account_id = account.get('account_id')
                return account_id
        else:    
            if account_number == curr_account_number:
                account_id = account.get('account_id')
                return account_id

    return None


def get_transaction_description_perfios(category):
    """
    This method takes in the perfios category string and 
    returns a description for the txn according to Bank Connect
    """
    lower_perfios_category = category.lower()

    if ("loan" in lower_perfios_category) or ("emi payment" in lower_perfios_category):
        return "lender_transaction"
    
    if "bounced i/w cheque charges" == lower_perfios_category:
        return "chq_bounce_charge"

    if "bounced i/w ecs charges" == lower_perfios_category:
        return "ach_bounce_charge"

    if ("transfer to" in lower_perfios_category or "transfer from" in lower_perfios_category) and isinstance(category, str):
        return category
    
    if "credit card payment" == lower_perfios_category:
        return "credit_card_bill"
    
    if ("investment expense" == lower_perfios_category) or ("investment income" == lower_perfios_category):
        return "trading/investments"

    if "insurance" == lower_perfios_category:
        return "insurance"
    
    return ""

def get_merchant_category(category):
    lower_perfios_category = category.lower()

    if ("loan" in lower_perfios_category) or ("emi payment" in lower_perfios_category):
        return "loans"
    
    if "credit card payment" == lower_perfios_category:
        return "bills"
    
    if "fuel" == lower_perfios_category:
        return "fuel"
    
    if "travel" == lower_perfios_category:
        return "travel"

    if ("investment expense" == lower_perfios_category) or ("investment income" == lower_perfios_category):
        return "trading/investments"

    if ("insurance" == lower_perfios_category) or ("mf purchase" == lower_perfios_category):
        return "insurance"
    
    if "food" == lower_perfios_category:
        return "food"

    return ""

def get_account_category_perfios(perfios_account_category):
    """
    This method takes in the perfios account category and returns
    "corporate" or "individual" or "" as per the basic string checks suggested
    by IIFL.
    """
    if perfios_account_category == "" or perfios_account_category is None:
        return ""

    # cleaning and converting to list of strings
    perfios_account_category_splitted = "".join(ch if ch.isalpha() else " " for ch in perfios_account_category).lower().split()

    print("cleaned perfios account category splitted: {}".format(perfios_account_category_splitted))
    
    current_account_category_keywords = ["current", "cc"]
    savings_account_category_keywords = ["saving", "savings"]
    overdraft_account_category_keywords = ["overdraft"]

    for keyword in current_account_category_keywords:
        if keyword in perfios_account_category_splitted:
            return "corporate"

    for keyword in savings_account_category_keywords:
        if keyword in perfios_account_category_splitted:
            return "individual"
        
    for keyword in overdraft_account_category_keywords:
        if keyword in perfios_account_category_splitted:
            return "overdraft"

    return ""


def get_transaction_channel_perfios(category):
    """
    This method takes in the perfios category string and 
    returns a transaction channel for the txn according to Bank Connect
    """
    lower_perfios_category = category.lower()

    if "bounced i/w cheque" == lower_perfios_category:
        return "inward_cheque_bounce"

    if "bounced i/w ecs" == lower_perfios_category:
        return "auto_debit_payment_bounce"

    if "bounced o/w cheque" == lower_perfios_category:
        return "outward_cheque_bounce"
    
    if (("bounced i/w cheque charges" == lower_perfios_category)
        or ("bounced i/w ecs charges" == lower_perfios_category)
        or ("bank charges" == lower_perfios_category)):
        return "bank_charge"
    
    if "bounced i/w ecs" == lower_perfios_category:
        return "inward_cheque_bounce"

    if "interest" == lower_perfios_category:
        return "bank_interest"

    if "reversal" == lower_perfios_category:
        return "refund"
        
    if "salary" == lower_perfios_category:
        return "salary"

    if "cash withdrawal" == lower_perfios_category:
        return "cash_withdrawl"
    
    if "cash deposit" == lower_perfios_category:
        return "cash_deposit"
    
    if "reversal" == lower_perfios_category:
        return "refund"
    
    if "credit card payment" == lower_perfios_category:
        return "bill_payment"
    
    if "fixed deposit" == lower_perfios_category:
        return "investment"
    
    if "purchase by card" == lower_perfios_category:
        return "debit_card"
    
    if "Online Shopping" == lower_perfios_category:
        return "payment_gateway_purchase"
    return ""


def log_to_dashboard_perfios(entity_id, statement_id, event_type, remarks, dump):
    """
    This helper method calls the internal perfios logging 
    API with provided inputs.
    """
    dashboard_logging_url = "{}/bank-connect/v1/perfios_integration/perfios_logging/".format(DJANGO_BASE_URL)
    
    request_headers = {
        'x-api-key': API_KEY,
        'Content-Type': "application/json",
    }

    request_payload = {
        "entity_id": entity_id,
        "statement_id": statement_id,
        "event_type": event_type,
        "remarks": remarks,
        "dump": dump
    }

    # we will try to log only once
    try:
        response = requests.post(
            url=dashboard_logging_url,
            headers=request_headers,
            data=json.dumps(request_payload)
        )
        
        print("perfios logging api response status code: {}, response data: {}".format(response.status_code, response.text))
    except Exception as e:
        print("some error occured while logging perfios related data to dashboard, exception: {}".format(e))


def perfios_report_fetch_and_transform(event, context):
    """
    This handler fetches the perfios report data and 
    transforms it into Bank Connect's data format.

    One report is tried to fetch 3 times max.
    """

    # print("event recieved: {}".format(event))

    records = event.get("Records", None)

    if records is None or len(records) == 0:
        print("no records were found")
        return

    record = records[0]

    body = record.get("body", None)

    if body is None:
        print("record body was none")
        return

    sqs_message = json.loads(body)

    # MAX_RETRIES = 8 # with a sleep of 8 seconds (8 x 8 = 64 seconds)
    retrieve_report_request_signature = sqs_message.get("retrieve_report_request_signature", None)
    x_perfios_date = sqs_message.get("x_perfios_date", None)
    retrieve_report_request_headers = sqs_message.get("retrieve_report_request_headers", None)
    retrieve_report_request_url = sqs_message.get("retrieve_report_request_url", None)
    statement_id = sqs_message.get("statement_id", None)
    related_statement_ids_data = sqs_message.get("related_statement_ids_data", dict())
    usable_account_number = sqs_message.get("usable_account_number", None)
    entity_id = sqs_message.get("entity_id", None)
    bank_name = sqs_message.get("bank_name", None)
    is_only_initiated = sqs_message.get("is_only_initiated", False)
    is_perfios_failed = sqs_message.get("is_perfios_failed", False)
    is_perfios_callback_received = sqs_message.get("is_perfios_callback_received", False)
    perfios_fetch_initiation_seconds_epoch = sqs_message.get("perfios_fetch_initiation_seconds_epoch", None)
    transaction_status_request_signature = sqs_message.get("transaction_status_request_signature", None)
    transaction_status_request_headers = sqs_message.get("transaction_status_request_headers", None)
    transaction_status_request_url = sqs_message.get("transaction_status_request_url", None)
    perfios_transaction_id = sqs_message.get("perfios_transaction_id", None)
    statement_meta_data_for_warehousing = sqs_message.get("statement_meta_data_for_warehousing", {})

    print("received perfios transaction id: {}".format(perfios_transaction_id))
    print("warehouse data: {}".format(statement_meta_data_for_warehousing))

    # null value check
    # this is an eager check, without this we can't mark status on dashboard or ddb
    if statement_id is None or statement_id == "" or len(related_statement_ids_data) == 0 or related_statement_ids_data is None:
        print("statement id or related statement data were not found, could not proceed")
        return
    
    if usable_account_number is None or usable_account_number == "" \
        or entity_id is None or entity_id == "" \
        or bank_name is None or bank_name == "":
        print("account number or entity id or bank name was null, could not proceed")
        # updating the status as failed
        for related_statement_id in related_statement_ids_data.keys():
            print("marking statement id {} as failed".format(related_statement_id))
            # logging
            log_to_dashboard_perfios(
                entity_id=entity_id,
                statement_id=statement_id,
                event_type="NULL_INITIAL_DATA_FROM_FROM_PERFIOS",
                remarks="Recieved incomplete initial data from Perfios",
                dump=json.dumps({
                    "related_statement_ids_data": related_statement_ids_data,
                    "account_number": usable_account_number,
                    "entity_id": entity_id,
                    "bank_name": bank_name
                })
            )
            update_progress_all_types(related_statement_id, "failed")

            dashboard_data_to_send = {
                "is_extracted": False,
                "is_complete": False,
                "account_id": None
            }
            update_progress_on_dashboard(related_statement_id, dashboard_data_to_send)
        return

    # creating an entry for all the related statement ids in bsa-count table for showing status as processing
    # only if 'is_only_initiated' is True
    if is_only_initiated:
        # logging
        log_to_dashboard_perfios(
            entity_id=entity_id,
            statement_id=statement_id,
            event_type="PERFIOS_LAMBDA_INITIATED_PROCESSING",
            remarks="Perfios Lambda report processing initiated",
            dump=json.dumps({
                "related_statement_ids_data": related_statement_ids_data
            })
        )

        print("inside is_only_initiated flow...")

        for related_statement_id in related_statement_ids_data.keys():
            print("making entry to bsa-count table for statement id {}".format(related_statement_id))
            bank_connect_statement_table.update_item(
                Key={"statement_id": related_statement_id},
                UpdateExpression="set {} = :s, created_at = :c".format('page_count'),
                ExpressionAttributeValues={':s': 0, ':c': str(int(time.time()))}
            )

            update_progress(related_statement_id, "identity_status", "processing")
            update_progress(related_statement_id, "transactions_status", "processing")
            update_progress(related_statement_id, "processing_status", "processing")

        bc_account_id = get_account_id_perfios(entity_id, usable_account_number, bank=bank_name)

        if not bc_account_id:
            # no single account mismatch check
            bc_account_id = create_new_account_perfios(
                entity_id,
                bank_name,
                usable_account_number,
                statement_id,
                "",
                "",
                ""
            )
        else:
            add_statement_to_account_perfios(entity_id, bc_account_id, statement_id)

        # also add other related statement ids to this same account id
        for related_statement_id in related_statement_ids_data.keys():
            # because the statement_id will be added in the else part already
            if related_statement_id != statement_id:
                add_statement_to_account_perfios(entity_id, bc_account_id, related_statement_id)

    # marking all the related statement ids as failed if 'is_perfios_failed' is True and return
    if is_perfios_failed:
        # logging
        log_to_dashboard_perfios(
            entity_id=entity_id,
            statement_id=statement_id,
            event_type="PERFIOS_LAMBDA_PROCESSING_FAILED_CALLBACK",
            remarks="Perfios Lambda report processing failed, recieved from callback",
            dump=json.dumps({
                "related_statement_ids_data": related_statement_ids_data
            })
        )

        print("inside is_perfios_failed flow...")
        for related_statement_id in related_statement_ids_data.keys():
            print("marking statement id {} as failed".format(related_statement_id))
            update_progress_all_types(related_statement_id, "failed")

            dashboard_data_to_send = {
                "is_extracted": False,
                "is_complete": False,
                "account_id": None
            }
            update_progress_on_dashboard(related_statement_id, dashboard_data_to_send)
        return

    # continue the flow only if 'is_perfios_failed' is not True and 'is_only_initiated' is not True
    if retrieve_report_request_signature is None or retrieve_report_request_signature == "" \
        or x_perfios_date is None or x_perfios_date == "" \
        or retrieve_report_request_headers is None or retrieve_report_request_headers == "" \
        or retrieve_report_request_url is None or retrieve_report_request_url == "" \
        or transaction_status_request_signature is None or transaction_status_request_signature == "" \
        or transaction_status_request_headers is None or transaction_status_request_headers == "" \
        or transaction_status_request_url is None or transaction_status_request_url == "":
        print("something was null, could not proceed, marking statement id as failed {}".format(statement_id))
        print("event received: {}".format(event))

        # logging
        log_to_dashboard_perfios(
            entity_id=entity_id,
            statement_id=statement_id,
            event_type="PERFIOS_LAMBDA_WRONG_INPUT_MARKING_FAILED",
            remarks="Perfios Lambda received some wrong input, marked the statements as failed",
            dump=json.dumps({
                "related_statement_ids_data": related_statement_ids_data,
                "sqs_message_recieved": sqs_message
            })
        )

        # marking this statement as failed
        # update statement status as failed extraction in ddb and dashboard both
        for related_statement_id in related_statement_ids_data.keys():
            print("marking statement id {} as failed".format(related_statement_id))
            update_progress_all_types(related_statement_id, "failed")

            dashboard_data_to_send = {
                "is_extracted": False,
                "is_complete": False,
                "account_id": None
            }
            update_progress_on_dashboard(related_statement_id, dashboard_data_to_send)

    current_statement_progress_ddb = get_perfios_statement_progress(statement_id=statement_id)

    if not is_perfios_callback_received and current_statement_progress_ddb == "processing":
        # NOTE: if here, means we haven't yet got the callback from Perfios and we will try to fetch the
        # status using Perfios Transaction Status API
        # if it is completed we will continue to fetch the report 
        # else we'll mark the status of all the related statement ids as failed
        if (time.time() - perfios_fetch_initiation_seconds_epoch) > 120:
            # NOTE: if already 120 seconds are passed
            print("120 seconds have passed, falling back to perfios status api")
            print("perfios transaction status api url: {}".format(transaction_status_request_url))
            print("perfios transaction status api headers: {}".format(transaction_status_request_headers))
            perfios_transaction_status_response = requests.get(
                url=transaction_status_request_url,
                headers=transaction_status_request_headers
            )

            # logging
            log_to_dashboard_perfios(
                entity_id=entity_id,
                statement_id=statement_id,
                event_type="PERFIOS_LAMBDA_FALLBACK_PROCESSING_INITIATED",
                remarks="Perfios report fallback mechanism initiated, as 120 seconds have passed",
                dump=json.dumps({
                    "related_statement_ids_data": related_statement_ids_data,
                    "perfios_transaction_status_api_response_code": perfios_transaction_status_response.status_code,
                    "perfios_transaction_status_api_response_payload": perfios_transaction_status_response.text
                })
            )

            is_perfios_transaction_status_ready = False

            if perfios_transaction_status_response.status_code == 200:
                perfios_transaction_status_response_json = json.loads(perfios_transaction_status_response.text)

                perfios_regular_debits_transactions_json_txn_list = perfios_transaction_status_response_json.get("transactions", dict()).get("transaction", [])
                if len(perfios_regular_debits_transactions_json_txn_list) > 0:                    
                    perfios_transaction_error_code = perfios_regular_debits_transactions_json_txn_list[0].get("errorCode", None)
                    perfios_transaction_status = perfios_regular_debits_transactions_json_txn_list[0].get("status", None)
                    if perfios_transaction_error_code == "E_NO_ERROR" or perfios_transaction_status == "COMPLETED":
                        is_perfios_transaction_status_ready = True

            
            # mark failed in any failure scenarios
            if not is_perfios_transaction_status_ready:
                # logging
                log_to_dashboard_perfios(
                    entity_id=entity_id,
                    statement_id=statement_id,
                    event_type="PERFIOS_LAMBDA_FALLBACK_PROCESSING_FAILED",
                    remarks="Perfios report fallback mechanism recieved failed status, making statements as failed",
                    dump=json.dumps({
                        "related_statement_ids_data": related_statement_ids_data
                    })
                )
                print("perfios transaction status api response status code: {}".format(perfios_transaction_status_response.status_code))
                print("error occured while getting the transaction status from perfios: {}".format(perfios_transaction_status_response.text))
                
                # simply mark all the statement ids as failed
                for related_statement_id in related_statement_ids_data.keys():
                    # do not mark karur bank statements as failed as requested by lending team
                    if bank_name.lower() not in ["karur"]:
                        print("marking statement id {} as failed".format(related_statement_id))
                        update_progress_all_types(related_statement_id, "failed")
                        dashboard_data_to_send = {
                            "is_extracted": False,
                            "is_complete": False,
                            "account_id": None
                        }
                        update_progress_on_dashboard(related_statement_id, dashboard_data_to_send)
                return
        else:
            # NOTE: push the data back to sqs queue for next trial
            print("120 seconds are not yet passed, will wait for perfios callback")
            sqs_message = {
                "retrieve_report_request_signature": retrieve_report_request_signature,
                "x_perfios_date": x_perfios_date,
                "retrieve_report_request_headers": retrieve_report_request_headers,
                "retrieve_report_request_url": retrieve_report_request_url,
                "statement_id": statement_id,
                "related_statement_ids_data": related_statement_ids_data,
                "usable_account_number": usable_account_number,
                "entity_id": entity_id,
                "bank_name": bank_name,
                "is_only_initiated": False,
                "is_perfios_failed": False,
                "is_perfios_callback_received": False,
                "perfios_fetch_initiation_seconds_epoch": perfios_fetch_initiation_seconds_epoch,
                "transaction_status_request_signature": transaction_status_request_signature,
                "transaction_status_request_headers": transaction_status_request_headers,
                "transaction_status_request_url": transaction_status_request_url,
                "perfios_transaction_id": perfios_transaction_id,
                "statement_meta_data_for_warehousing": statement_meta_data_for_warehousing
            }

            try:
                sqs_client.send_message(
                    QueueUrl=PERFIOS_REPORT_FETCH_TASK_QUEUE_URL,
                    MessageBody=json.dumps(sqs_message),
                    MessageDeduplicationId="{}_{}".format(statement_id, str(uuid4())),
                    MessageGroupId="perfios_report_fetch_{}".format(statement_id)
                )
                print("successfully sent message to queue for perfios report fetch for 120 seconds fallback")
            except Exception as e:
                print("unable to send message to queue for perfios report fetch: {}".format(e))
                # mark this statement id as failed
                for related_statement_id in related_statement_ids_data.keys():
                    print("marking statement id {} as failed".format(related_statement_id))
                    update_progress_all_types(related_statement_id, "failed")

                    dashboard_data_to_send = {
                        "is_extracted": False,
                        "is_complete": False,
                        "account_id": None
                    }
                    update_progress_on_dashboard(related_statement_id, dashboard_data_to_send)
            
            return
    
    # fetch the report from perfios
    perfios_report_response = requests.get(
        url=retrieve_report_request_url,
        headers=retrieve_report_request_headers
    )

    object_key = 'perfios_callback_xml/{}_{}.xml'.format(entity_id, statement_id)
    s3.put_object(Body=perfios_report_response.text, Bucket=BANK_CONNECT_UPLOADS_REPLICA_BUCKET, Key=object_key)

    # logging
    log_to_dashboard_perfios(
        entity_id=entity_id,
        statement_id=statement_id,
        event_type="PERFIOS_LAMBDA_REPORT_FETCH_API_RESPONSE",
        remarks="Perfios report fetch api response data",
        dump=json.dumps({
            "related_statement_ids_data": related_statement_ids_data,
            "perfios_report_fetch_api_response_status_code": perfios_report_response.status_code,
            "s3_bucket_name": BANK_CONNECT_UPLOADS_REPLICA_BUCKET,
            "perfios_report_fetch_api_response_s3_key": object_key
        })
    )

    if perfios_report_response.status_code != 200:
        # logging
        log_to_dashboard_perfios(
            entity_id=entity_id,
            statement_id=statement_id,
            event_type="PERFIOS_LAMBDA_REPORT_FETCH_FAILED",
            remarks="Perfios report fetch api failed, marking statements as failed",
            dump=json.dumps({
                "related_statement_ids_data": related_statement_ids_data,
                "perfios_report_fetch_api_response_status_code": perfios_report_response.status_code,
                "perfios_report_fetch_api_response_payload": perfios_report_response.text
            })
        )

        print("perfios report response status code: {}".format(perfios_report_response.status_code))
        print("error occured while fetch the report from perfios: {}".format(perfios_report_response.text))
        
        # simply mark all the statement ids as failed
        for related_statement_id in related_statement_ids_data.keys():
            print("marking statement id {} as failed".format(related_statement_id))
            update_progress_all_types(related_statement_id, "failed")

            dashboard_data_to_send = {
                "is_extracted": False,
                "is_complete": False,
                "account_id": None
            }
            update_progress_on_dashboard(related_statement_id, dashboard_data_to_send)

        return

    # ---- THE MAIN TRANSFORMATION CODE STARTS HERE ----
    # initializing bc_account_id before to use in exception block also
    bc_account_id = None

    try:
        # convert the report XML to JSON format    
        report_ordered_dict = xmltodict.parse("""{}""".format(perfios_report_response.text), attr_prefix="")

        perfios_report = json.loads(json.dumps(report_ordered_dict))
        # print(perfios_report)

        customer_info_perfios_report = perfios_report.get("PIR:Data", dict()).get("CustomerInfo", dict())
        perfios_statement_details = perfios_report.get("PIR:Data", dict()).get("Statementdetails", dict()).get("Statement", dict())
        perfios_summary_info = perfios_report.get("PIR:Data", dict()).get("SummaryInfo", dict())
        perfios_transactions_info = perfios_report.get("PIR:Data", dict()).get("Xns", dict()).get("Xn", [])
        perfios_possible_fraud_indicators = perfios_report.get("PIR:Data", dict()).get("FCUAnalysis", dict()).get("PossibleFraudIndicators", dict())
        perfios_regular_debits_transactions_info = perfios_report.get("PIR:Data", dict()).get("RegularDebits", dict()).get("RXn", [])

        # print("perfios report api response: {}".format(perfios_report))
        # print("perfios statement details: {}".format(perfios_statement_details))

        # to handle cases when PossibleFraudIndicators tag is present but empty
        if perfios_possible_fraud_indicators is None:
            perfios_possible_fraud_indicators = dict()

        # handling the case when perfios statement details is a dict object
        if isinstance(perfios_statement_details, dict):
            # put it in the list with only one object
            print("dict object was found in perifos statement data")
            perfios_statement_details = [perfios_statement_details]

        # IDENTITY - start
        from_date = perfios_transactions_info[0].get("date", "")
        to_date = perfios_transactions_info[-1].get("date", "")

        identity_with_extra_params = dict()
        identity_with_extra_params["is_image"] = False
        identity_with_extra_params["password_incorrect"] = False
        identity_with_extra_params["identity"] = {}
        identity_with_extra_params["identity"]["account_number"] = usable_account_number
        identity_with_extra_params["identity"]["name"] = customer_info_perfios_report.get("name", "")
        identity_with_extra_params["identity"]["address"] = customer_info_perfios_report.get("address", "")
        identity_with_extra_params["identity"]["ifsc"] = ""
        identity_with_extra_params["identity"]["micr"] = ""
        identity_with_extra_params["identity"]["perfios_account_category"] = perfios_summary_info.get("accType", "")
        identity_with_extra_params["identity"]["perfios_transaction_id"] = perfios_transaction_id
        identity_with_extra_params["identity"]["account_category"] = get_account_category_perfios(identity_with_extra_params["identity"]["perfios_account_category"]) # ""
        identity_with_extra_params["identity"]["credit_limit"] = ""
        identity_with_extra_params["keywords"] = {
            "amount_present": True,
            "balance_present": True,
            "date_present": True,
            "all_present": True
        }
        identity_with_extra_params["date_range"] = {'from_date': from_date, 'to_date': to_date}

        # print("identity with extra params: {}".format(identity_with_extra_params))
        print("name found in pdf: {}".format(identity_with_extra_params.get("identity", dict()).get("name", "")))
        # IDENTITY - end
 
        # no password incorrect check is required
        # no identity check is required
        bc_account_id = get_account_id_perfios(entity_id, usable_account_number, bank=bank_name)

        if not bc_account_id:
            # no single account mismatch check
            bc_account_id = create_new_account_perfios(
                entity_id,
                bank_name,
                usable_account_number,
                statement_id,
                identity_with_extra_params.get("identity", dict()).get("ifsc", ""),
                identity_with_extra_params.get("identity", dict()).get("micr", ""),
                identity_with_extra_params.get("identity", dict()).get("account_category", "")
            )
        else:
            add_statement_to_account_perfios(entity_id, bc_account_id, statement_id)

        # also add other related statement ids to this same account id
        for related_statement_id in related_statement_ids_data.keys():
            # because the statement_id will be added in the else part already
            if related_statement_id != statement_id:
                add_statement_to_account_perfios(entity_id, bc_account_id, related_statement_id)

        identity_with_extra_params.get("identity", dict()).update({
            "account_id": bc_account_id
        })

        # update the account category in the "account" type dynamo object
        update_account_category_for_entity(entity_id, bc_account_id, identity_with_extra_params.get("identity", dict()).get("account_category", ""))

        # Fraud given by perfios to statement_id
        statement_fraud_status_by_perfios = {}
        for perfios_statement in perfios_statement_details:
            perfios_statement_status = perfios_statement.get("statementStatus", "").upper()
            perfios_file_name = perfios_statement.get("fileName", '').replace('.pdf', '')
            is_valid_uuid4, statement_id_through_perfios_file_name = is_valid_and_format_uuid(perfios_file_name)
            if is_valid_uuid4 and statement_id_through_perfios_file_name in related_statement_ids_data.keys():
                statement_fraud_status_by_perfios[statement_id_through_perfios_file_name] = perfios_statement_status
                
        single_fraud_status_by_perfios = 'VERIFIED'
        for perfios_statement in perfios_statement_details:
            perfios_statement_status = perfios_statement.get("statementStatus", "").upper()
            if perfios_statement_status == "FRAUD":  # refer removed from author_fraud upon discussion with lending team for iifl
                single_fraud_status_by_perfios = "FRAUD"
                break
            elif perfios_statement_status == "REFER":
                single_fraud_status_by_perfios = "REFER"

        # Making statement_{statement_id}_account entry in fsm results table
        # Also Making statement_{statement_id}_identity in fsm results table
        # also update the progress in bsa_page_count_table
        # for all the statement ids
        is_fraud_by_perfios_possible_indicators = perfios_possible_fraud_indicators.get("SuspiciousBankEStatements", dict()).get("status", "false") == "true"
        for related_statement_id in related_statement_ids_data.keys():
            # Saving fraud per statement to identity
            fraud_status = statement_fraud_status_by_perfios.get(related_statement_id, single_fraud_status_by_perfios).upper()
            identity_with_extra_params["is_fraud"] = True if fraud_status in ["FRAUD", "REFER"] else False
            identity_with_extra_params["fraud_type"] = FRAUD_STATUS_TO_FRAUD_TYPE_MAPPING.get(fraud_status, None)
            identity_with_extra_params["identity"]["perfios_statement_status"] = fraud_status
            identity_with_extra_params["is_fraud_from_perfios_data"] = fraud_status
            
            if single_fraud_status_by_perfios not in ["FRAUD", "REFER"] and is_fraud_by_perfios_possible_indicators:
                identity_with_extra_params["fraud_type"] = 'font_and_encryption_fraud'
                
            identity_with_extra_params["is_fraud_from_excel"] = is_fraud_by_perfios_possible_indicators
            identity_with_extra_params["is_extracted_by_perfios"] = True
            
            # identity

            time_stamp_in_mlilliseconds = time.time_ns()
            ddb_object = {
                "statement_id": related_statement_id,
                "item_data": identity_with_extra_params,
                'created_at': time_stamp_in_mlilliseconds,
                'updated_at': time_stamp_in_mlilliseconds
            }
            bank_connect_identity_table.put_item(Item=ddb_object)

            # also update the progress in bsa results table for identity
            update_progress(related_statement_id, "identity_status", "completed")

        # TRANSACTIONS - start
        # converting all txns into Bank Connect's format
        transactions_list = []
        for sequence_number, txn_perfios in enumerate(perfios_transactions_info):
            transactions_list.append({
                "transaction_type": "credit" if float(txn_perfios.get("amount", "0.00")) > 0 else "debit",
                "transaction_note": txn_perfios.get("narration", ""),
                "amount": abs(float(txn_perfios.get("amount", "0.00"))),
                "balance": float(txn_perfios.get("balance", "0.00")),
                "date": txn_perfios.get("date") + " 00:00:00",
                "transaction_channel": get_transaction_channel_perfios(txn_perfios.get("category", "")),
                "unclean_merchant": "",
                "merchant_category": get_merchant_category(txn_perfios.get("category", "")),
                "perfios_txn_category": txn_perfios.get("category", ""),
                "description": get_transaction_description_perfios(txn_perfios.get("category", "")),
                "is_lender": False,
                "merchant": "",
                "hash": "created in next step",
                "sequence_number": sequence_number
            })
        categorizer = SingleCategory(bank_name=bank_name, transactions=transactions_list, categorize_server_ip=CATEGORIZE_RS_PRIVATE_IP)
        transactions_list = categorizer.categorize_from_forward_mapper()
        transactions_list = create_add_transaction_hash_perfios(transactions_list)
        transactions_list_of_lists = get_transactions_list_of_lists_perfios(transactions_list)

        number_of_pages = len(transactions_list_of_lists)
        print("total number of pages: {}".format(number_of_pages))

        # update bsa page count table with number of pages
        # for all the statement ids
        for related_statement_id in related_statement_ids_data.keys():
            bank_connect_statement_table.update_item(
                Key={"statement_id": related_statement_id},
                UpdateExpression="set {} = :s, created_at = :c, pages_done = :d".format('page_count'),
                ExpressionAttributeValues={':s': number_of_pages, ':c': str(int(time.time())), ':d': 0}
            )

        number_of_pages = len(transactions_list_of_lists)

        # update statement_meta_dat_for_warehousing

        statement_meta_data_for_warehousing['account_number'] = usable_account_number
        statement_meta_data_for_warehousing['statement_id'] = statement_id
        statement_meta_data_for_warehousing['entity_id'] = entity_id
        statement_meta_data_for_warehousing['bank_name'] = bank_name

        # writing all transactions into ddb page wise
        for page_number, transaction_list_page in enumerate(transactions_list_of_lists):
            print("found {} transactions".format(len(transaction_list_page)))
            # do this for all the statement ids
            for related_statement_id in related_statement_ids_data.keys():
                time_stamp_in_mlilliseconds = time.time_ns()
                ddb_object = {
                    "statement_id": related_statement_id,
                    "page_number": page_number,
                    "item_data": json.dumps(transaction_list_page, default=str),
                    "transaction_count": len(transaction_list_page),
                    'created_at': time_stamp_in_mlilliseconds,
                    'updated_at': time_stamp_in_mlilliseconds
                }

                bank_connect_transactions_table.put_item(Item=ddb_object)
                update_bsa_extracted_count(entity_id, related_statement_id, page_number, number_of_pages)

                # prepare warehouse data and send to firehose
                statement_meta_data_for_warehousing['page_number'] = page_number
                warehouse_data = prepare_warehouse_data(statement_meta_data_for_warehousing, transaction_list_page)
                send_data_to_firehose(warehouse_data, TRANSACTIONS_STREAM_NAME)
                print("sent perfios data to firehose successfully", warehouse_data)
        # TRANSACTIONS - end

        # SALARY TXNS - start
        salary_transactions = []
        for txn in transactions_list:
            if txn["transaction_channel"].lower() == "salary":
                salary_transactions.append(txn)
        
        time_stamp_in_mlilliseconds = time.time_ns()
        salary_transactions_ddb_object = {
            "account_id": bc_account_id,
            "item_data": json.dumps(salary_transactions, default=str),
            'created_at': time_stamp_in_mlilliseconds,
            'updated_at': time_stamp_in_mlilliseconds
        }

        # advance features are calculated at account level
        # we do not need to store the same for all the statement ids
        try:
            bank_connect_salary_table.put_item(Item=salary_transactions_ddb_object)
        except Exception:
            print("failed to write salary data to ddb - saving into s3 now")
            s3_object_key = "salary_transactions/entity_{}/account_{}".format(entity_id, bc_account_id)
            s3_object = s3_resource.Object(BANK_CONNECT_DDB_FAILOVER_BUCKET, s3_object_key)
            s3_object.put(Body=bytes(json.dumps(salary_transactions, default=str), encoding='utf-8'))
            time_stamp_in_mlilliseconds = time.time_ns()
            dynamo_object = {
                'account_id': bc_account_id,
                's3_object_key': s3_object_key,
                'created_at': time_stamp_in_mlilliseconds,
                'updated_at': time_stamp_in_mlilliseconds
            }
            bank_connect_salary_table.put_item(Item=dynamo_object)
        # SALARY TXNS - end

        # DISPARITIES - start
        disparities = []
        # TODO: get update from Perfios for behavioural frauds and make a mapping here
        time_stamp_in_mlilliseconds = time.time_ns()
        disparities_ddb_object = {
            "account_id": bc_account_id,
            "item_data": json.dumps(disparities, default=str),
            'created_at': time_stamp_in_mlilliseconds,
            'updated_at': time_stamp_in_mlilliseconds
        }
        bank_connect_disparities_table.put_item(Item=disparities_ddb_object)
        # DISPARITIES - end

        # ADVANCED FEATURES - start
        recurring_txns = {}

        # recurring debit txns
        group_wise_recurring_debit_txns = dict() # group -> [txns array]
        for reg_debit_txn_perfios in perfios_regular_debits_transactions_info:
            # initialize with an empty list if not present already
            if reg_debit_txn_perfios["group"] not in group_wise_recurring_debit_txns.keys():
                group_wise_recurring_debit_txns[reg_debit_txn_perfios["group"]] = []
            
            txn_to_append = {
                "transaction_type": "credit" if float(reg_debit_txn_perfios.get("amount", "0.00")) > 0 else "debit",
                "transaction_note": reg_debit_txn_perfios.get("narration", ""),
                "amount": abs(float(reg_debit_txn_perfios.get("amount", "0.00"))),
                "balance": float(reg_debit_txn_perfios.get("balance", "0.00")),
                "date": reg_debit_txn_perfios.get("date") + " 00:00:00",
                "transaction_channel": "",
                "merchant_category": "",
                "description": reg_debit_txn_perfios.get("category", ""),
                "hash": "searched in next step",
                "account_id": bc_account_id,
                "clean_transaction_note": ""
            }

            # find hash in transactions_list for this txn
            found_txn = next((txn for txn in transactions_list if txn["transaction_type"] == txn_to_append["transaction_type"] and txn["transaction_note"] == txn_to_append["transaction_note"] and txn["amount"] == txn_to_append["amount"] and txn["balance"] == txn_to_append["balance"] and txn["date"] == txn_to_append["date"] and txn["description"] == txn_to_append["description"]), {})
            txn_to_append["hash"] = found_txn.get("hash", "")

            group_wise_recurring_debit_txns[reg_debit_txn_perfios["group"]].append(txn_to_append)

        recurring_debit_transactions = []
        for i, (group_key, group_txns) in enumerate(group_wise_recurring_debit_txns.items(), start=1):
            recurring_debit_transactions.append({
                "source": "Source{}".format(i),
                "transactions": group_txns
            })

        recurring_txns["recurring_debit_transactions"] = recurring_debit_transactions

        time_stamp_in_mlilliseconds = time.time_ns()
        recurring_ddb_object = {
            "account_id": bc_account_id,
            "item_data": json.dumps(recurring_txns, default=str),
            'created_at': time_stamp_in_mlilliseconds,
            'updated_at': time_stamp_in_mlilliseconds
        }

        try:
            bank_connect_recurring_table.put_item(Item=recurring_ddb_object)
        except Exception:
            print("failed to write advanced features data to ddb - saving into s3 now")
            s3_object_key = "recurring_transactions/entity_{}/account_{}".format(entity_id, bc_account_id)
            s3_object = s3_resource.Object(BANK_CONNECT_DDB_FAILOVER_BUCKET, s3_object_key)
            s3_object.put(Body=bytes(json.dumps(recurring_txns, default=str), encoding='utf-8'))
            time_stamp_in_mlilliseconds = time.time_ns()
            dynamo_object = {
                'account_id': bc_account_id,
                's3_object_key': s3_object_key,
                'created_at': time_stamp_in_mlilliseconds,
                'updated_at': time_stamp_in_mlilliseconds
            }
            bank_connect_recurring_table.put_item(Item=dynamo_object)
        # recurring_txns end

        #only for iifl_fraud_flow, need to be removed in future
        try:
            if bank_name == 'sbi':
                calculate_bank_connect_frauds(list(related_statement_ids_data.keys()),entity_id)
        except Exception as e:
            print("Exception occured while calculating bc frauds for statement_id = {} as {}".format(entity_id, e))
        #--------------------------------------------------

        # update the transactions_status in bsa_page_count_table
        # for all the related statement ids
        for related_statement_id in related_statement_ids_data.keys():
            update_progress(related_statement_id, "transactions_status", "completed")
            update_progress(related_statement_id, "processing_status", "completed")

            #in case of sbi of sbi frauds are calculated by finbox and not marking as completed here, fraud status in sbi will be marked after calculation
            #this sbi condition is only for iifl_fraud_flow, need to be removed in future
            if bank_name != 'sbi':
                update_progress_fraud_status(related_statement_id, 'completed')

            data_to_send = {
                "is_extracted": True,
                "is_complete": True,
                "account_id": bc_account_id
            }
            update_progress_on_dashboard(related_statement_id, data_to_send)

    except Exception as e:
        print("some error ocurred while transaforming perfios data: exception: {}".format(e))

        # logging
        log_to_dashboard_perfios(
            entity_id=entity_id,
            statement_id=statement_id,
            event_type="PERFIOS_LAMBDA_EXCEPTION_OCCURED",
            remarks="Perfios report lambda encountered some exception",
            dump=json.dumps({
                "related_statement_ids_data": related_statement_ids_data,
                "exception": "{}".format(e)
            })
        )

        # updating the status as failed
        for related_statement_id in related_statement_ids_data.keys():
            print("marking statement id {} as failed".format(related_statement_id))
            update_progress_all_types(related_statement_id, "failed")

            dashboard_data_to_send = {
                "is_extracted": False,
                "is_complete": False,
                "account_id": bc_account_id
            }
            update_progress_on_dashboard(related_statement_id, dashboard_data_to_send)


def get_identity_with_params_from_perfios_report_dict(perfios_xml_report_dict):
    """
    Takes in the XML converted to DICT input of Perfios Report and return back BankConnect specific IDENTITY with extra params dict.
    """
    perfios_xml_report_dict = _get_origin_recalibrated_perfios_xml_report(perfios_xml_report_dict)
    perfios_statement_details = perfios_xml_report_dict.get("Statementdetails", dict()).get("Statement", dict())

    if isinstance(perfios_statement_details, dict):
        perfios_statement_details = [perfios_statement_details]

    customer_info_perfios_report = perfios_xml_report_dict.get("CustomerInfo", dict())
    perfios_transactions_info = perfios_xml_report_dict.get("Xns", dict()).get("Xn", [])
    perfios_summary_info = perfios_xml_report_dict.get("SummaryInfo", dict())
    perfios_possible_fraud_indicators = perfios_xml_report_dict.get("FCUAnalysis", dict()).get("PossibleFraudIndicators", dict())

    # to handle cases when PossibleFraudIndicators tag is present but empty
    if perfios_possible_fraud_indicators is None:
        perfios_possible_fraud_indicators = dict()

    from_date = None
    to_date = None
    if perfios_transactions_info:
        from_date = perfios_transactions_info[0].get("date", "")
        to_date = perfios_transactions_info[-1].get("date", "")

    identity_with_extra_params = dict()

    identity_with_extra_params["is_image"] = False
    identity_with_extra_params["password_incorrect"] = False

    identity_with_extra_params["identity"] = {}
    identity_with_extra_params["identity"]["account_number"] = perfios_summary_info.get("accNo", "")
    identity_with_extra_params["identity"]["name"] = customer_info_perfios_report.get("name", "")
    identity_with_extra_params["identity"]["address"] = customer_info_perfios_report.get("address", "")
    identity_with_extra_params["identity"]["ifsc"] = ""
    identity_with_extra_params["identity"]["micr"] = ""
    identity_with_extra_params["identity"]["perfios_account_category"] = perfios_summary_info.get("accType", "")
    # identity_with_extra_params["identity"]["perfios_statement_status"] = perfios_statement_details.get("statementStatus", "").upper() # always checking for the first object only
    identity_with_extra_params["identity"]["perfios_transaction_id"] = customer_info_perfios_report.get("perfiosTransactionId", "")
    identity_with_extra_params["identity"]["perfios_institution_id"] = customer_info_perfios_report.get("instId", "")
    identity_with_extra_params["identity"]["account_category"] = get_account_category_perfios(identity_with_extra_params["identity"]["perfios_account_category"]) # ""
    identity_with_extra_params["identity"]["credit_limit"] = ""

    identity_with_extra_params["keywords"] = {
        "amount_present": True,
        "balance_present": True,
        "date_present": True,
        "all_present": True
    }

    identity_with_extra_params["date_range"] = {'from_date': from_date, 'to_date': to_date} if from_date and to_date else None

    # Moving the statement status check in a loop to run over all the available statements
    final_fraud_status = "VERIFIED"
    for perfios_statement in perfios_statement_details:
        perfios_statement_status = perfios_statement.get("statementStatus", "").upper()

        if perfios_statement_status == "FRAUD":  # refer removed from author_fraud upon discussion with lending team for iifl
            final_fraud_status = "FRAUD"
            break
        elif perfios_statement_status == "REFER":
            final_fraud_status = "REFER"

    identity_with_extra_params["is_fraud"] = True if final_fraud_status in ["FRAUD", "REFER"] else False
    identity_with_extra_params["fraud_type"] = FRAUD_STATUS_TO_FRAUD_TYPE_MAPPING.get(final_fraud_status, None)
    identity_with_extra_params["identity"]["perfios_statement_status"] = final_fraud_status.upper()
    identity_with_extra_params["is_fraud_from_perfios_data"] = final_fraud_status

    is_fraud_by_perfios_possible_indicators = perfios_possible_fraud_indicators.get("SuspiciousBankEStatements", dict()).get("status", "false") == "true"
    if final_fraud_status not in ["FRAUD", "REFER"] and is_fraud_by_perfios_possible_indicators:
        identity_with_extra_params["fraud_type"] = 'font_and_encryption_fraud'

    identity_with_extra_params["is_fraud_from_excel"] = is_fraud_by_perfios_possible_indicators
    identity_with_extra_params["is_extracted_by_perfios"] = True

    print("identity with extra params: {}".format(identity_with_extra_params))
    return identity_with_extra_params


def get_transactions_list_from_perfios_report_dict(perfios_xml_report_dict):
    perfios_xml_report_dict = _get_origin_recalibrated_perfios_xml_report(perfios_xml_report_dict)
    perfios_transactions_info = perfios_xml_report_dict.get("Xns", dict()).get("Xn", [])

    # converting all txns into Bank Connect's format
    transactions_list = []
    for txn_perfios in perfios_transactions_info:
        transactions_list.append({
            "transaction_type": "credit" if float(txn_perfios.get("amount", "0.00")) > 0 else "debit",
            "transaction_note": txn_perfios.get("narration", ""),
            "amount": abs(float(txn_perfios.get("amount", "0.00"))),
            "balance": float(txn_perfios.get("balance", "0.00")),
            "date": txn_perfios.get("date") + " 00:00:00",
            "transaction_channel": get_transaction_channel_perfios(txn_perfios.get("category", "")),
            "unclean_merchant": "",
            "merchant_category": "",
            "perfios_txn_category": txn_perfios.get("category", ""),
            "description": get_transaction_description_perfios(txn_perfios.get("category", "")),
            "is_lender": False,
            "merchant": "",
            "hash": "created in next step"
        })

    transactions_list = create_add_transaction_hash_perfios(transactions_list)

    return transactions_list


def get_recurring_debit_transactions_from_perfios_report_dict(perfios_xml_report_dict, account_id, transactions_list):
    perfios_xml_report_dict = _get_origin_recalibrated_perfios_xml_report(perfios_xml_report_dict)
    perfios_regular_debits_transactions_info = perfios_xml_report_dict.get("RegularDebits", dict()).get("RXn", [])
    # recurring debit txns
    group_wise_recurring_debit_txns = dict() # group -> [txns array]
    for reg_debit_txn_perfios in perfios_regular_debits_transactions_info:
        # initialize with an empty list if not present already
        if reg_debit_txn_perfios["group"] not in group_wise_recurring_debit_txns.keys():
            group_wise_recurring_debit_txns[reg_debit_txn_perfios["group"]] = []
        
        txn_to_append = {
            "transaction_type": "credit" if float(reg_debit_txn_perfios.get("amount", "0.00")) > 0 else "debit",
            "transaction_note": reg_debit_txn_perfios.get("narration", ""),
            "amount": abs(float(reg_debit_txn_perfios.get("amount", "0.00"))),
            "balance": float(reg_debit_txn_perfios.get("balance", "0.00")),
            "date": reg_debit_txn_perfios.get("date") + " 00:00:00",
            "transaction_channel": "",
            "merchant_category": "",
            "description": reg_debit_txn_perfios.get("category", ""),
            "hash": "searched in next step",
            "account_id": account_id,
            "clean_transaction_note": ""
        }

        # find hash in transactions_list for this txn
        found_txn = next((txn for txn in transactions_list if txn["transaction_type"] == txn_to_append["transaction_type"] and txn["transaction_note"] == txn_to_append["transaction_note"] and txn["amount"] == txn_to_append["amount"] and txn["balance"] == txn_to_append["balance"] and txn["date"] == txn_to_append["date"] and txn["description"] == txn_to_append["description"]), {})
        txn_to_append["hash"] = found_txn.get("hash", "")

        group_wise_recurring_debit_txns[reg_debit_txn_perfios["group"]].append(txn_to_append)

    recurring_debit_transactions = []
    for i, (group_key, group_txns) in enumerate(group_wise_recurring_debit_txns.items(), start=1):
        recurring_debit_transactions.append({
            "source": "Source{}".format(i),
            "transactions": group_txns
        })
    
    return recurring_debit_transactions


def convert_perfios_report_xml_into_dict(report_xml_string):
    report_ordered_dict = xmltodict.parse("""{}""".format(report_xml_string), attr_prefix="")

    perfios_report_dict = json.loads(json.dumps(report_ordered_dict))

    return perfios_report_dict


def get_bank_name_from_perfios_institution_id(perfios_institution_id):
    """
    This helper method calls the internal perfios institution id mapping api to get BankConnect's bank idenitifier
    """
    dashboard_logging_url = "{}/bank-connect/v1/perfios_integration/get_bank_name_for_perfios_institution_id/?perfios_institution_id={}".format(DJANGO_BASE_URL, perfios_institution_id)
    
    request_headers = {
        'x-api-key': API_KEY,
        'Content-Type': "application/json",
    }

    # initializing
    bank_name = None

    # we will try to log only once
    try:
        response = requests.get(
            url=dashboard_logging_url,
            headers=request_headers
        )
        
        print("perfios institution id mapping to bank name api response status code: {}, response data: {}".format(response.status_code, response.text))

        if response.status_code != 200:
            return None

        json_response = json.loads(response.text)

        bank_name = json_response.get("data", dict()).get("bank_name", None)
    except Exception as e:
        capture_exception(e)
        print("some error occured while logging perfios related data to dashboard, exception: {}".format(e))

    return bank_name

#this function updates progress of all type as same to save few ddb writes
def update_progress_all_types(statement_id, progress_type, message = None):
    bank_connect_statement_table.update_item(
        Key={
            'statement_id': statement_id},
        UpdateExpression=""" set identity_status = :i,
                                transactions_status = :t,
                                processing_status = :p,
                                metadata_fraud_status = :m, 
                                page_identity_fraud_status = :pi,
                                message = :msg""",
        ExpressionAttributeValues={
            ':i': progress_type,
            ':t': progress_type,
            ':p': progress_type,
            ':m': progress_type,
            ':pi': progress_type,
            ':msg': message })

def parse_external_perfios_xml_report(event, context):

    # get required data from event
    statement_id = event.get("statement_id", None)
    entity_id = event.get("entity_id", None)
    s3_bucket_name = event.get("s3_bucket_name", None)
    s3_file_key = event.get("s3_file_key", None)

    s3_response = s3.get_object(Bucket=s3_bucket_name, Key=s3_file_key)
    
    s3_file_data_string = s3_response['Body'].read().decode()

    print("s3 file response body", s3_file_key)

    # create an entry in bsa-page-count table to refelect processing status in apis
    bank_connect_statement_table.update_item(
        Key={"statement_id": statement_id},
        UpdateExpression="set {} = :s, created_at = :c".format('page_count'),
        ExpressionAttributeValues={':s': 0, ':c': str(int(time.time()))}
    )

    update_progress_all_types(statement_id, "processing")

    # get dict from perfios report xml
    perfios_xml_report_dict = convert_perfios_report_xml_into_dict(s3_file_data_string)

    # get identity with extra params from report dict
    identity_with_params = get_identity_with_params_from_perfios_report_dict(perfios_xml_report_dict)

    # bank name we have to get calling an api to dashboard
    perfios_institution_id = identity_with_params.get("identity", dict()).get("perfios_institution_id", None)
    bank_name = get_bank_name_from_perfios_institution_id(perfios_institution_id)

    if bank_name is None:
        return {
            "data": None,
            "error": "Perfios Institution ID is not mapped to any Bank identifier"
        }

    # get account number
    account_number = identity_with_params.get("identity", dict()).get("account_number", None    )

    # get account id from    
    account_id = get_account_id_perfios(entity_id, account_number, bank_name)

    if not account_id:
        account_id = create_new_account_perfios(
            entity_id,
            bank_name,
            account_number,
            statement_id,
            identity_with_params.get("identity", dict()).get("ifsc", ""),
            identity_with_params.get("identity", dict()).get("micr", ""),
            identity_with_params.get("identity", dict()).get("account_category", "")
        )
    else:
        add_statement_to_account_perfios(entity_id, account_id, statement_id)

    # set account id in identity with params
    identity_with_params.get("identity", dict()).update({
        "account_id": account_id
    })

    # update the account category in the "account" type dynamo object
    update_account_category_for_entity(entity_id, account_id, identity_with_params.get("identity", dict()).get("account_category", ""))

    # adding statement_{statement_id}_identity entry in fsm results table
    time_stamp_in_mlilliseconds = time.time_ns()
    ddb_object = {
        "statement_id": statement_id,
        "item_data": identity_with_params,
        'created_at': time_stamp_in_mlilliseconds,
        'updated_at': time_stamp_in_mlilliseconds
    }
    bank_connect_identity_table.put_item(Item=ddb_object)

    # also update the progress in bsa results table for identity
    update_progress(statement_id, "identity_status", "completed")

    # TRANSACTIONS
    transactions_list = get_transactions_list_from_perfios_report_dict(perfios_xml_report_dict)
    transactions_list_of_lists = get_transactions_list_of_lists_perfios(transactions_list)
    number_of_pages = len(transactions_list_of_lists)
    print("total number of pages: {}".format(number_of_pages))

    # writing all the transactions page wise into ddb
    for page_number, transaction_list_page in enumerate(transactions_list_of_lists):
        print("found {} transactions".format(len(transaction_list_page)))

        time_stamp_in_mlilliseconds = time.time_ns()
        fsm_results_page_ddb_object = {
            "statement_id": statement_id,
            "page_number":  page_number,
            "item_data": json.dumps(transaction_list_page, default=str),
            "transaction_count": len(transaction_list_page),
            'created_at': time_stamp_in_mlilliseconds,
            'updated_at': time_stamp_in_mlilliseconds
        }
        bank_connect_transactions_table.put_item(Item=fsm_results_page_ddb_object)


    
    # SALARY TRANSACTIONS
    salary_transactions = []
    for txn in transactions_list:
        if txn["transaction_channel"].lower() == "salary":
            salary_transactions.append(txn)
    
    time_stamp_in_mlilliseconds = time.time_ns()
    salary_transactions_ddb_object = {
        "account_id": account_id,
        "item_data": json.dumps(salary_transactions, default=str),
        'created_at': time_stamp_in_mlilliseconds,
        'updated_at': time_stamp_in_mlilliseconds
    }

    # advance features are calculated at account level
    # we do not need to store the same for all the statement ids
    try:
        bank_connect_salary_table.put_item(Item=salary_transactions_ddb_object)
    except Exception:
        print("failed to write salary data to ddb - saving into s3 now")
        s3_object_key = "salary_transactions/entity_{}/account_{}".format(entity_id, account_id)
        s3_object = s3_resource.Object(BANK_CONNECT_DDB_FAILOVER_BUCKET, s3_object_key)
        s3_object.put(Body=bytes(json.dumps(salary_transactions, default=str), encoding='utf-8'))
        time_stamp_in_mlilliseconds = time.time_ns()
        dynamo_object = {
            'account_id': account_id,
            's3_object_key': s3_object_key,
            'created_at': time_stamp_in_mlilliseconds,
            'updated_at': time_stamp_in_mlilliseconds
        }
        bank_connect_salary_table.put_item(Item=dynamo_object)

    # DISPARITIES
    disparities = []
    # TODO: get update from Perfios for behavioural frauds and make a mapping here
    time_stamp_in_mlilliseconds = time.time_ns()
    disparities_ddb_object = {
        "account_id": account_id,
        "item_data": json.dumps(disparities, default=str),
        'created_at': time_stamp_in_mlilliseconds,
        'updated_at': time_stamp_in_mlilliseconds
    }
    bank_connect_disparities_table.put_item(Item=disparities_ddb_object)

    # recurring_txns
    recurring_txns = {}
    # recurring debit transactions
    recurring_debit_transactions = get_recurring_debit_transactions_from_perfios_report_dict(
        perfios_xml_report_dict, 
        account_id, 
        transactions_list
    )

    recurring_txns["recurring_debit_transactions"] = recurring_debit_transactions
    # save advanced features to ddb
    time_stamp_in_mlilliseconds = time.time_ns()
    recurring_ddb_object = {
        "account_id": account_id,
        "item_data": json.dumps(recurring_txns, default=str),
        'created_at': time_stamp_in_mlilliseconds,
        'updated_at': time_stamp_in_mlilliseconds
    }

    try:
        bank_connect_recurring_table.put_item(Item=recurring_ddb_object)
    except Exception:
        print("failed to write advanced features data to ddb - saving into s3 now")
        s3_object_key = "recurring_transactions/entity_{}/account_{}".format(entity_id, account_id)
        s3_object = s3_resource.Object(BANK_CONNECT_DDB_FAILOVER_BUCKET, s3_object_key)
        s3_object.put(Body=bytes(json.dumps(recurring_txns, default=str), encoding='utf-8'))
        time_stamp_in_mlilliseconds = time.time_ns()
        dynamo_object = {
            'account_id': account_id,
            's3_object_key': s3_object_key,
            'created_at': time_stamp_in_mlilliseconds,
            'updated_at': time_stamp_in_mlilliseconds
        }
        bank_connect_recurring_table.put_item(Item=dynamo_object)

    # update the status in ddb and dashboard db both
    update_progress(statement_id, "transactions_status", "completed")
    update_progress(statement_id, "processing_status", "completed")
    update_progress_fraud_status(statement_id, "completed")

    data_to_send = {
        "is_extracted": True,
        "is_complete": True,
        "account_id": account_id
    }
    update_progress_on_dashboard(statement_id, data_to_send)

    data_to_return = {
        "data": {
            "identity": identity_with_params.get("identity", dict()),
            "bank_name": bank_name,
            "date_range": identity_with_params.get("date_range", None)
        },
        "error": None
    }

    print("data to return: {}".format(data_to_return))

    return data_to_return

#only for iifl_fraud_flow, need to be removed in future
def mark_all_statements_as_fraud_or_refer_ddb(statement_ids,entity_id,final_bc_perfios_status):
    for statement_id in statement_ids:
        bank_connect_identity_table.update_item(
            Key={ 'statement_id' : statement_id},
            UpdateExpression = """set item_data.#id.perfios_statement_status = :s, updated_at = :u""",
            ExpressionAttributeNames={"#id": "identity"},
            ExpressionAttributeValues = {':s':final_bc_perfios_status, ':u': time.time_ns()}
        )

def get_bankconnect_fraud_status(statement_id):
    statement_rows = bank_connect_tmp_identity_table.query(
        KeyConditionExpression=Key('statement_id').eq(statement_id))

    statement_items = statement_rows.get('Items', list())

    if len(statement_items) == 0:
        return dict()

    return statement_items[0].get('item_data')

def calculate_bank_connect_frauds(statement_ids,entity_id):
    print("calculating bank connect frauds for iifl flow for statements_ids {}".format(statement_ids))
    is_any_fraud = False
    is_any_refer = False
    
    for statement_id in statement_ids:
        counter = 0
        while counter < 60:
            time.sleep(counter)
            counter += 2
            fraud_status, _ = get_fraud_progress(statement_id, None)
            if fraud_status == 'processing':
                continue
            item_data = get_complete_identity_for_statement(statement_id)
            bc_item_data = get_bankconnect_fraud_status(statement_id)
            is_bc_fraud = bc_item_data.get("is_fraud",False)
            perfios_statement_status = item_data.get("identity", dict()).get("perfios_statement_status",None)
            if perfios_statement_status == "FRAUD" or is_bc_fraud:
                is_any_fraud = True
            if perfios_statement_status == "REFER":
                is_any_refer = True
            break
    
    if is_any_fraud:
        print("Marking all statements ids, {} as FRAUD".format(statement_ids))
        mark_all_statements_as_fraud_or_refer_ddb(statement_ids,entity_id,"FRAUD")
    elif is_any_refer:
        print("Marking all statements ids, {} as REFER".format(statement_ids))
        mark_all_statements_as_fraud_or_refer_ddb(statement_ids,entity_id,"REFER")
    #after updating identity removing cache from s3
    folder_name ="entity_" + entity_id + "/"
    try:
        BANK_CONNECT_CACHEBOX_RESOURCE.objects.filter(Prefix=folder_name).delete()
    except Exception:
        print("Could not delete cache from s3 for entity_id = {}".format(entity_id))


def get_identity_for_iifl_fraud_flow(bucket, key, template):
    payload = {
        "bucket": bucket,
        "key": key,
        "is_iifl_fraud_flow":True,
        "template": template
    }
    try:
        response = identity_handler(payload, None)
        return response
    except Exception as e:
        print("Exception occured while invoking identity lambda as {}".format(e))
        return dict()

def iifl_fraud_flow(event, context):
    print("Calculation of BC frauds fro iifl pdfs for event = {}".format(event))
    bucket = event['bucket']
    key = event['key']
    statement_id = event.get('statement_id',None)
    entity_id = event.get('entity_id',None)
    attempt_type = event.get('attempt_type',None)
    is_retrigger = event.get('is_retrigger',False)
    template = event.get('template', None)
    stream_font_list = event.get('stream_font_list', [])
    encryption_algo_list = event.get('encryption_algo_list', [])
    good_font_list = event.get('good_font_list',[])
    strict_metadata_fraud_list = event.get('strict_metadata_fraud_list', [])
    
    print("iifl fraud flow for statement_id = {} and entity_id = {}".format(statement_id,entity_id))

    if statement_id is None or entity_id is None:
        print("statement_id or entity_id cannot be None")
        return 

    #inserting initial data
    initial_data = {
        'is_fraud':False,
        "fraud_type":None,
        "doc_metadata":dict()
    }
    time_stamp_in_mlilliseconds = time.time_ns()
    ddb_object = {
        "statement_id": statement_id,
        "item_data": initial_data,
        'created_at': time_stamp_in_mlilliseconds,
        'updated_at': time_stamp_in_mlilliseconds
    }
    bank_connect_tmp_identity_table.put_item(Item=ddb_object)

    identity_response = get_identity_for_iifl_fraud_flow(bucket, key, template)
    print("Response from identity handler",identity_response)
    name = identity_response.get('name',None)
    account_number = identity_response.get('account_number',None)
    try:
        params = {
            "bucket": bucket,
            "key": key,
            "is_retrigger": is_retrigger,
            "attempt_type":attempt_type,
            "is_iifl_fraud_flow":True,
            "stream_font_list": stream_font_list,
            "encryption_algo_list": encryption_algo_list,
            'good_font_list': good_font_list,
            'strict_metadata_fraud_list': strict_metadata_fraud_list
        }
        payload = json.dumps(params)
        lambda_client.invoke(FunctionName = METADATA_FRAUDS_FUNCTION, Payload=payload, InvocationType='Event')
    except Exception as e:
        print("Exception occured while invoking metadata frauds for key = {} as {}".format(key,e))

    if account_number is not None:
        try:
            queue_message = json.dumps({
                "account_number": account_number,
                "name": name,
                "s3_file_key": key,
                "s3_file_bucket": bucket,
            })

            queue_response = sqs_client.send_message(
                QueueUrl=PDF_PAGES_HASH_GENERATION_TASKS_QUEUE_URL,
                MessageBody=queue_message,
                MessageDeduplicationId=statement_id,
                MessageGroupId="hash_generation_group_id"
            )
            print("Send message to Queue, Response: {}".format(queue_response))
        except Exception as e:
            print("exception occured while sending message to page hash generation task queue: {}".format(e))
    else:
        print("account number was None, not checking for page hash fraud")