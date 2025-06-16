import logging
import os
import re
import traceback
import string
import json

from datetime import datetime, timedelta
from urllib3.exceptions import ReadTimeoutError
from python.configs import (
    lambda_client,
    XLSX_REPORT_LAMBDA,
    AGGREGATE_XLSX_REPORT_LAMBDA,
    XML_REPORT_LAMBDA,
    DMS_PUSH_LAMBDA,
    BANK_CONNECT_REPORTS_BUCKET,
    BANK_CONNECT_CLICKHOUSE_BUCKET,
    CURRENT_STAGE,
    s3,
    RECURRING_MICROSERVICE_URL,
    RECURRING_MICROSERVICE_TOKEN, TCAP_RECURRING_AA_PULLS_STREAM_NAME, TCAP_CUSTOMERS_STREAM_NAME,
    BANK_CONNECT_DMS_PUSH_LOGS_BUCKET
)
from python.configs import LAMBDA_LOGGER, sqs_client, RAMS_POST_PROCESSING_QUEUE_URL, API_KEY, UPDATE_STATE_FAN_OUT_INFO_URL, bank_connect_account_table, bank_connect_statement_table, DJANGO_BASE_URL
from sentry_sdk import capture_exception, push_scope
from python.context.logging import LoggingContext
import random
from library.fitz_functions import get_generic_text_from_bbox, get_address, get_name, get_account_num
from library.utils import check_date, get_bank_threshold_diff
import math
import uuid
from python.aws_utils import collect_results
from boto3.dynamodb.conditions import Key
from typing import Union, TypedDict, Optional, Any 
import time
from copy import deepcopy
from library.fraud import convert_str_date_to_datetime
from slack_sdk import WebClient
from python.api_utils import call_api_with_session


class AccountItemData(TypedDict):
    account_id: str
    account_category: str
    account_number: str
    account_opening_date: str
    bank: str
    credit_limit: int
    ifsc: str
    is_od_account: Optional[bool]
    linked_account_ref_number: str
    micr: int
    missing_data: object
    neg_txn_od: Optional[bool]
    od_limit: Optional[int]
    salary_confidence: str
    statements: list[str]
    od_limit_input_by_customer: str

class AccountDict(TypedDict):
    entity_id: str
    account_id: str
    created_at: int
    item_data: Optional[AccountItemData]
    item_status: Optional[dict]
    updated_at: int

class ODLimitConfigAfterIdentityExtraction(TypedDict):
    neg_txn_od: Optional[bool]
    od_limit: Optional[int]



def get_date_of_format(date_str: str, format: str) -> str:
    if not isinstance(date_str, str) or not isinstance(format, str):
        return None
    
    try:
        datetime_obj, _ = check_date(date_str)
        if isinstance(datetime_obj, datetime):
            return datetime_obj.strftime(format)
    except Exception as _:
        pass
    
    return None


def get_datetime(date_str: str, formats: list = []) -> datetime:
    if not isinstance(date_str, str):
        return None
    
    known_formats = formats
    if not known_formats:
        known_formats = ["%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S%z"]
    
    date_time_obj = None
    for known_format in known_formats:
        try:
            date_time_obj = datetime.strptime(date_str, known_format)
            break
        except Exception as _:
            pass
    
    return date_time_obj


def get_balance_from_aa_transaction(transaction_item: dict):
    if not isinstance(transaction_item, dict):
        return None
    
    balance_value = transaction_item.get('currentBalance')
    if balance_value is None:
        balance_value = transaction_item[0].get("balance")
    
    return balance_value


def is_raw_aa_transactions_inconsistent(transactions_list, bank_name):
    aa_transactions_list = deepcopy(transactions_list)
    total_trxn_count = len(aa_transactions_list)
    
    if total_trxn_count < 2:
        return False
    
    THRESHOLD_DIFF = get_bank_threshold_diff(bank_name)
    
    try:
        first_date = get_datetime(aa_transactions_list[0].get("transactionTimestamp"))
        last_date = get_datetime(aa_transactions_list[-1].get("transactionTimestamp"))
        if not isinstance(first_date, datetime) or not isinstance(last_date, datetime):
            return True
        
        if first_date > last_date:
            aa_transactions_list = aa_transactions_list[::-1]
        
        prev_balance = float(get_balance_from_aa_transaction(aa_transactions_list[0]))
    except Exception as _:
        return True
    
    for i in range(1, total_trxn_count):
        try:
            amount = float(aa_transactions_list[i].get('amount'))
            new_balance = float(get_balance_from_aa_transaction(aa_transactions_list[i]))
            transaction_type = str(aa_transactions_list[i].get("type"))
            
            amount = abs(amount)
            if transaction_type.upper() == "DEBIT":
                amount = -1 * amount
        except Exception as _:
            return True
        
        new_balance_calculated = round(prev_balance + amount, 2)
        if abs(new_balance_calculated - new_balance) > THRESHOLD_DIFF:
            return True
        
        prev_balance = new_balance
    return False


def get_transactions_list_of_lists_finvu_aa(transactions_list):
    transactions_list_of_lists = []

    if len(transactions_list) <= 30:
        transactions_list_of_lists.append(transactions_list)
    else:
        transactions_list_of_lists = [transactions_list[i:i+25] for i in range(0, len(transactions_list), 25)]

        last_transactions_list = transactions_list_of_lists[-1]

        # if last txn list has less than 10 txns and total batches are at least 2
        if len(last_transactions_list) < 10 and len(transactions_list_of_lists) >= 2:
            second_last_transactions_list = transactions_list_of_lists[-2]
            combined_last_and_second_last_transactions_list = second_last_transactions_list + last_transactions_list
            transactions_list_of_lists[-2] = combined_last_and_second_last_transactions_list
            transactions_list_of_lists = transactions_list_of_lists[:-1]

    return transactions_list_of_lists

def remove_local_file(file_path):
    if os.path.exists(file_path):
        os.remove(file_path)


def send_event_to_update_state_queue(entity_id, statement_id, statement_meta_data_for_warehousing=None, org_metadata:dict = dict()):
    if statement_meta_data_for_warehousing is None:
        statement_meta_data_for_warehousing = {}
    queue_payload = json.dumps({
                            "entity_id": entity_id,
                            "statement_id": statement_id,
                            "statement_meta_data_for_warehousing": statement_meta_data_for_warehousing,
                            "org_metadata":org_metadata
                        })
    
    random_number = random.randint(0, 100000)
    print(f"Invoking update_state_fan_out lambda via events in SQS for statement_id: {statement_id}, entity_id: {entity_id}")
    try:
        sqs_push_response = sqs_client.send_message(
            QueueUrl = RAMS_POST_PROCESSING_QUEUE_URL,
            MessageBody = queue_payload,
            MessageDeduplicationId = '{}_{}_{}'.format(entity_id, statement_id, random_number),
            MessageGroupId = 'update_state_invocation_{}_{}'.format(statement_id, random_number)
        )
        print(sqs_push_response)
    except Exception as e:
        capture_exception(e)
        print("Failed to push into update-state-fan-out sqs as ", str(e))


def get_dashboard_info_for_update_state_fan_out(
    entity_id, statement_id=None, local_logging_context: LoggingContext = None
):
    if not isinstance(local_logging_context, LoggingContext):
        local_logging_context: LoggingContext = LoggingContext(entity_id=entity_id, statement_id=statement_id)

    local_logging_context.upsert(source="get_dashboard_info_for_update_state_fan_out")
    LAMBDA_LOGGER.debug("Inside get_dashboard_info_for_update_state_fan_out", extra=local_logging_context.store)
    headers = {'x-api-key': API_KEY}
    params = {'entity_id': entity_id, 'statement_id': statement_id}

    response = {}
    response["recurring_salary_flag"] = False
    response["employer_names"] = []
    response["session_flow"] = False
    response["session_date_range"] = {}
    response["date_range_approval_criteria"] = 0
    response["is_missing_date_range_enabled"] = False
    response["acceptance_criteria"] = [] 
    response["salary_mode"] = 'HARD'
    response["statement_attempt_type"] = None
    response["attempt_type_data"] = {}
    response["is_sme"] = False
    response["adjusted_eod"] = False
    response["to_reject_account"] = False
    response["to_remap_predictors"] = False
    response["excel_report_version"] = "v1"
    response["aggregate_excel_report_version"] = "v1"
    response["xml_report_version"] = "v1"
    response["bank_mapping"] = None
    response["api_subscriptions"] = []
    response["accept_anything"] = False
    response["metadata"] = {}
    response['excel_filename_format'] = ''
    response['ignore_self_transfer'] = False
    response['allowed_frauds'] = []
    response['ignore_missing_dates_days'] = 4
    response['is_dms_push_enabled'] = False
    response['session_metadata'] = {}
    response['salary_configuration'] = {}
    response['to_reject_statement'] = False
    response["aa_journey_mode"] = ""
    response["aa_session_details"] = dict()
    response["month_over_month_aggregated"] = False
    response["aa_data_file_key"] = ""
    response["bucket_name"] = ""

    LAMBDA_LOGGER.debug(
        f"Fetch Fan Out information from Dashboard {UPDATE_STATE_FAN_OUT_INFO_URL}",
        extra=local_logging_context.store
    )
    try:
        info_response = call_api_with_session(UPDATE_STATE_FAN_OUT_INFO_URL, "GET", None, headers, params)
        print('response from finboxdashboard to get update_state_fan_out_data ', info_response.text)
        if info_response.status_code == 200:
            response_json = info_response.json()
            response["recurring_salary_flag"] = response_json.get('recurring_salary',False)
            response["employer_names"] = response_json.get('employer_names',[])
            response["salary_mode"] = response_json.get('salary_mode','HARD')
            response["session_flow"] = response_json.get('session_flow', False)
            response["session_date_range"] = response_json.get("session_date_range", {})
            response["date_range_approval_criteria"] = response_json.get("date_range_approval_criteria", 0)
            response["is_missing_date_range_enabled"] = response_json.get("is_missing_date_range_enabled", False)
            response["acceptance_criteria"] = response_json.get("acceptance_criteria", [])
            response["statement_attempt_type"] = response_json.get('attempt_type', None)
            response["attempt_type_data"] = response_json.get('attempt_type_data', {})
            response["is_sme"] = response_json.get('is_sme', False)
            response["adjusted_eod"] = response_json.get('adjusted_eod', False)
            response["to_reject_account"] = response_json.get('to_reject_account', False)
            response["to_remap_predictors"] = response_json.get('to_remap_predictors', False)
            response["excel_report_version"] = response_json.get('excel_report_version', 'v1')
            response["aggregate_excel_report_version"] = response_json.get('aggregate_excel_report_version', 'v1')
            response["xml_report_version"] = response_json.get('xml_report_version', 'v1')
            response["bank_mapping"] = response_json.get('bank_mapping', None)
            response["api_subscriptions"] = response_json.get('api_subscriptions', [])
            response["accept_anything"] = response_json.get('accept_anything', False)
            response["metadata"] = response_json.get('metadata', {})
            response["excel_filename_format"] = response_json.get('excel_filename_format', '')
            response["ignore_self_transfer"] = response_json.get('ignore_self_transfer', False)
            response["allowed_frauds"] = response_json.get('allowed_frauds', [])
            response['ignore_missing_dates_days'] = response_json.get('ignore_missing_dates_days', 4)
            response["is_dms_push_enabled"] = response_json.get('is_dms_push_enabled', False)
            response["session_metadata"] = response_json.get('session_metadata', {})
            response["salary_configuration"] = response_json.get('salary_configuration', {})
            response["to_reject_statement"] = response_json.get('to_reject_statement', False)
            response["aa_journey_mode"] = response_json.get('aa_journey_mode', "")
            response["aa_session_details"] = response_json.get('aa_session_details', dict())
            response["month_over_month_aggregated"] = response_json.get('month_over_month_aggregated', False)
            response["aa_data_file_key"] = response_json.get('aa_data_file_key', list())
            response["bucket_name"] = response_json.get('bucket_name', list())
        else:
            local_logging_context.upsert(
                response_status_code=info_response.status_code,
                response_reason=info_response.reason
            )
            LAMBDA_LOGGER.debug(
                f"Error calling {UPDATE_STATE_FAN_OUT_INFO_URL}",
                extra=local_logging_context.store
            )
            local_logging_context.remove_keys(['response_status_code', 'response_reason'])
    except Exception as e:
        print("Exception occured while calling UPDATE_STATE_FAN_OUT_INFO_URL as {}".format(e))
        local_logging_context.upsert(exception=str(e), traceback=traceback.format_exc())
        LAMBDA_LOGGER.error(
            f"Exception while calling dashboard {UPDATE_STATE_FAN_OUT_INFO_URL}",
            extra=local_logging_context.store
        )
        local_logging_context.remove_keys(['exception', 'traceback'])

    LAMBDA_LOGGER.debug(
        "Successfully fetched fan out information from Dashboard",
        extra=local_logging_context.store
    )
    return response


def deseralize_od_metadata(stream_data):
    if not isinstance(stream_data, dict):
        return None

    for key in stream_data.keys():
        value_type = list(stream_data.get(key).keys())[0]
        value = stream_data.get(key, dict()).get(value_type, None)
        if value_type=='NULL':
            stream_data[key] = None
        else:
            stream_data[key] = value
    return stream_data

def deseralize_preshared_names(preshared_names):
    if not isinstance(preshared_names, list):
        return None
    
    return_data = []
    
    for item in preshared_names:
        if isinstance(item, dict):
            name = item.get('S', None)
            if name:
                return_data.append(name)
    return return_data

def deseralize_metadata_analysis(data):
    
    transformed_json = transform_json(data)
    return_data = {}
    name_matches = transformed_json.get('name_matches', [])
    for match in name_matches:
        name = match.get('name', '')
        score = match.get('score', 0)
        if name not in [None, '']:
            return_data[name] = score
    
    return json.dumps(return_data)


def transform_json(data):

    def transform_list(lst):
        return [transform_dict(d["M"]) for d in lst]
    
    def transform_dict(dct):
        result = {}
        for key, value in dct.items():
            if isinstance(value, dict):
                if "S" in value:
                    result[key] = value["S"]
                elif "N" in value:
                    result[key] = int(value["N"])  # or float(value["N"]) depending on your data
                elif "BOOL" in value:
                    result[key] = value["BOOL"]
                elif "L" in value:
                    result[key] = transform_list(value["L"])
            else:
                result[key] = value
        return result
    
    return transform_dict(data)

def get_data_for_template_handler_util(template_type, doc, template_json, bank, file_path):
    template_data = {'data':[None], 'all_text':[None]}
    if template_type == 'card_number_bbox':
        template_data, _, _ = get_account_num(doc, template_json, bank, path=file_path, get_only_all_text=False)
        template_data_all_text, _, _ = get_account_num(doc, template_json, bank, path=file_path, get_only_all_text=True)
        template_data={'data':[template_data],'all_text':[template_data_all_text]}
    elif template_type in ['payment_due_date', 'statement_date']:
        template_data, _, _ = get_generic_text_from_bbox(doc, template_json, 'date', get_only_all_text=False, template_type=template_type)
        if template_data is not None:
            template_data = template_data.strftime("%Y-%m-%d")
        template_data_all_text, _, _ = get_generic_text_from_bbox(doc, template_json, '', get_only_all_text=True, template_type=template_type)
        template_data={'data':[template_data],'all_text':[template_data_all_text]}
    elif template_type in ['total_dues', 'min_amt_due', 'purchase/debits', 'credit_limit', 'avl_credit_limit', 'opening_balance', 'avl_cash_limit', 'payment/credits', 'card_type_bbox', 'rewards_opening_balance_bbox', 'rewards_closing_balance_bbox','rewards_points_expired_bbox','rewards_points_claimed_bbox','rewards_points_credited_bbox']:
        template_data, _, _ = get_generic_text_from_bbox(doc, template_json,template_type = template_type, get_only_all_text=False)
        template_data_all_text, _, _ = get_generic_text_from_bbox(doc, template_json,get_only_all_text=True, template_type=template_type)
        template_data={'data':[template_data],'all_text':[template_data_all_text]}
    elif template_type == 'cc_name_bbox':
        template_data, _, _ = get_name(doc, template_json, bank, file_path, get_only_all_text=False)
        template_data_all_text, _, _ = get_name(doc, template_json, bank, file_path, get_only_all_text=True)
        template_data={'data':[template_data], 'all_text':[template_data_all_text]}
    elif template_type == 'address_bbox':
        template_data, _, _ = get_address(doc, template_json, file_path, get_only_all_text=False)
        template_data_all_text, _, _ = get_address(doc, template_json, file_path, get_only_all_text=True)
        template_data={'data':[template_data], 'all_text':[template_data_all_text]}
    
    return template_data

def create_identity_object_for_quality(identity_obj, metadata_analysis = dict(), statement_id=None, org_metadata:dict = dict()):
    if metadata_analysis is None:
        metadata_analysis = dict()
    
    od_limit = identity_obj.get('od_limit', None)
    credit_limit = identity_obj.get('credit_limit', None)
    name_matches = metadata_analysis.get('name_matches', list())

    identity_object_for_quality = {
        'name': None,
        'ifsc': None,
        'micr': None,
        'is_od_account': None,
        'od_limit': None,
        'credit_limit': None,
        'account_category': None,
        'account_number': None,
        'address': None,
        'metadata_analysis': json.dumps([]),
        'org_metadata': org_metadata
    }

    try:
        modified_names = []
        for name_match in name_matches:
            name_match.pop('tokenized_matches', None)
            name_match['score'] = str(name_match['score'])
            modified_names.append(name_match)
        metadata_analysis['name_matches'] = modified_names

        identity_object_for_quality = {
            'name': identity_obj.get('name', None),
            'ifsc': identity_obj.get('ifsc', None),
            'micr': identity_obj.get('micr', None),
            'is_od_account': identity_obj.get('is_od_account', None),
            'od_limit': None if od_limit is None else str(od_limit),
            'credit_limit': None if credit_limit is None else str(credit_limit),
            'account_category': identity_obj.get('account_category', None),
            'account_number': identity_obj.get('account_number', None),
            'address': identity_obj.get('address', None),
            'metadata_analysis': json.dumps(metadata_analysis),
            'org_metadata': org_metadata
        }
    except Exception as _:
        with push_scope() as scope:
            scope.set_extra("statement_id", statement_id)
            capture_exception()

    return identity_object_for_quality

def create_identity_object_for_credit_card_quality(identity_obj):
    is_calculated_payment_due_date = identity_obj.get('is_calculated_payment_due_date', False)
    credit_card_identity_for_quality = {
        'credit_card_number': identity_obj.get('credit_card_number', None),
        'payment_due_date': None if is_calculated_payment_due_date else identity_obj.get('payment_due_date', None),
        'name': identity_obj.get('name', None),
        'address': identity_obj.get('address', None),
        'total_dues': identity_obj.get('total_dues', None),
        'min_amt_due': identity_obj.get('min_amt_due', None),
        'credit_limit': identity_obj.get('credit_limit', None),
        'avl_credit_limit': identity_obj.get('avl_credit_limit', None),
        'avl_cash_limit': identity_obj.get('avl_cash_limit', None),
        'opening_balance': identity_obj.get('opening_balance', None),
        'payment_or_credits': identity_obj.get('payment_or_credits', None),
        'purchase_or_debits': identity_obj.get('purchase_or_debits', None),
        'finance_charges': identity_obj.get('finance_charges', None),
        'statement_date': identity_obj.get('statement_date', None),
        'card_type': identity_obj.get('card_type', None),
        'rewards': identity_obj.get('rewards', None)
    }

    return credit_card_identity_for_quality

def prepare_disparities_warehouse_data(warehouse_data, disparities, allowed_frauds):
    """
    Prepares the warehouse for disparities
    Args:
        warehouse_data:
        disparities:

    Returns:
        updated list of disparities with additional data with additional data
    """

    required_keys = ['entity_id', 'account_id', 'bank_name', 'account_number', 'org_id', 'org_name', 'link_id']
    current_datetime = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S.%f')
    updated_warehouse_data = {
        'created_at': current_datetime,
        'updated_at': current_datetime
    }
    for key in warehouse_data.keys():
        if key in required_keys:
            updated_warehouse_data[key]=warehouse_data[key]

    for disparity in disparities:
        if disparity.get('fraud_type')=='inconsistent_transaction':
            if disparity.get('statement_id', None) is None:
                disparity['inconsistent_type'] = 'Account'
            else:
                disparity['inconsistent_type'] = 'Statement'
        disparity['usfo_statement_id'] = warehouse_data.get('statement_id')
        if disparity.get('fraud_type') in allowed_frauds:
            disparity['visible_to_client'] = True
        else:
            disparity['visible_to_client'] = False
        disparity.update(updated_warehouse_data)

    if len(disparities)==0:
        tmp_disparity = {
            'created_at': current_datetime,
            'updated_at': current_datetime
        }
        for key in warehouse_data.keys():
            if key in required_keys:
                tmp_disparity[key]=warehouse_data[key]
        tmp_disparity['fraud_type'] = 'None'
        tmp_disparity['usfo_statement_id'] = warehouse_data.get('statement_id')
        return [tmp_disparity]

    return disparities

def cc_prepare_warehouse_data(warehouse_data, statement_transactions):
    """
    Prepares the warehouse data at each transaction level
    Args:
        warehouse_data:
        statement_transactions:

    Returns:
        updated list of transactions with additional data

    """
    current_datetime = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
    for transaction in statement_transactions:
        transaction.update(warehouse_data)
        transaction['created_at'] = current_datetime
        transaction['updated_at'] = current_datetime
    return statement_transactions

def prepare_warehouse_data(warehouse_data, statement_transactions):
    """
    Prepares the warehouse data at each transaction level
    Args:
        warehouse_data:
        statement_transactions:

    Returns:
        updated list of transactions with additional data

    """
    current_datetime = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
    for transaction in statement_transactions:
        transaction.update(warehouse_data)
        transaction['created_at'] = current_datetime
        transaction['updated_at'] = current_datetime
        transaction['optimizations'] = json.dumps(transaction.get('optimizations', []))
        if 'description_regex' in transaction and str(transaction['description_regex'])=='nan':
            transaction['description_regex'] = None
        if 'last_account_number' in transaction and str(transaction['last_account_number'])=='nan':
            transaction['last_account_number'] = None
        if 'last_account_category' in transaction and str(transaction['last_account_category'])=='nan':
            transaction['last_account_category'] = None

    return statement_transactions

def prepare_tcap_customers_data(session_metadata):
    key_mappings = {
        'session_id': 'session_id',
        'account_id': 'account_id',
        'Tclstmtid': 'session_id',
        'Applicationno': 'applicationNo',
        'Applicantid': 'applicantId',
        'Applicantname': 'applicantName',
        'Applicanttype': 'applicantType',
        'Emailid': 'emailId',
        'Altemailid': 'altEmailId',
        'Loanamount': 'loanAmount',
        'Loanduration': 'loanDuration',
        'Loantype': 'loanType',
        'Applicantnature': 'applicantNature',
        'Form26Asdob': 'form26ASDOB',
        'Webtopno': 'webtopNo',
        'Channel': 'channel',
        'OppId': 'oppId',
        'Destination': 'destination',
        'Yearmonthfrom': 'yearMonthFrom',
        'Yearmonthto': 'yearMonthTo',
        'Companycategory': 'companyCategory',
        'Sistercompanyname': 'sisterCompanyName',
        'Sourcesystemurl': 'sourceSystemURL',
        'Pan': 'pan',
        'Gstnumber': 'gstNumber',
        'Gstusername': 'gstUsername',
        'Employmenttype': 'employmentType',
        'Entityname': 'entityName',
        'Employername': 'employerName',
        'Dateofincorporation': 'dateofincorporation',
        'Companyname': 'companyName',
        'Mailsendflag': 'mailSendFlag',
        'Apisource': 'apiSource',
        'Borrowertype': 'borrowerType'
    }

    final_session_metadata = {}
    current_datetime = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
    final_session_metadata['created_at'] = current_datetime
    final_session_metadata['updated_at'] = current_datetime

    for key, value in key_mappings.items():
        if value in session_metadata.keys() and session_metadata[value] not in [None, ""]:
            final_session_metadata[key] = session_metadata[value]

    final_session_metadata['Appproduct'] = final_session_metadata.get('Loantype', '')

    return final_session_metadata

def prepare_tcap_call_details(session_metadata, dms_response):
    final_data = {}
    final_data['session_id'] = session_metadata['session_id']
    final_data['account_id'] = session_metadata['account_id']
    final_data['TclStmtId'] = session_metadata.get('session_id')
    final_data['WebtopNo'] = session_metadata.get('webtopNo')
    final_data['OppId'] = session_metadata.get('oppId')
    final_data['Destination'] = session_metadata.get('destination')

    xlsx_dms_response = dms_response.get('xlsx', {})
    if xlsx_dms_response:
        current_datetime = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
        final_data['DmsUploadStatus'] = xlsx_dms_response.get('status', 'failed')
        final_data['DmsUploadDate'] = current_datetime
        
        if final_data['DmsUploadStatus'] == 'success':
            pattern = r'.*objectID\":?\s?\"([0-9a-zA-Z]*)\"'
            match = re.search(pattern, xlsx_dms_response.get('response', {}))
            if match:
                final_data['DmsObjId'] = match.group(1)
    else:
        final_data['DmsUploadStatus'] = 'failed'
    try:
        xml_file_key = f"xml_report/entity_report_{final_data['session_id']}_original.xml"
        xml_file_object = s3.get_object(Bucket=BANK_CONNECT_REPORTS_BUCKET, Key=xml_file_key)
        xml_object = xml_file_object["Body"].read().decode()

        final_data['PerfiosXML'] = xml_object

    except Exception as e:
        print(e)
        print(final_data['session_id'])
        # print("XMl doesn't exist for session_id={}".format(final_data.get('session_id')))
    current_datetime = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
    final_data['PerfiosTxnIdDate'] = current_datetime
    final_data['created_at'] = current_datetime
    final_data['updated_at'] = current_datetime
    return final_data

def check_and_distribute_transactions_in_pages(list_of_list_txns):

    page_count = len(list_of_list_txns)

    if page_count < 2 :
        return list_of_list_txns

    for i in range(1, page_count):
        if len(list_of_list_txns[i]) != 0:
            return list_of_list_txns

    all_txns_list = [item for sublist in list_of_list_txns for item in sublist]

    count_txn = math.ceil(len(all_txns_list)/page_count)
    
    if count_txn == 0:
        return list_of_list_txns
    
    all_txns = [all_txns_list[i:i+count_txn] for i in range(0, len(all_txns_list), count_txn)]

    new_page_count = len(all_txns)
    if new_page_count < page_count:
        all_txns + ([[]]*(page_count - new_page_count))

    return all_txns

def async_invoke_cache_subscribed_data(payload):
    from python.cache_subscribed_data_handler import cache_subscribed_data_handler
    cache_subscribed_data_handler(payload, None)
    # try:
    #     lambda_client.invoke(
    #         FunctionName=CACHE_SUBSCRIBED_DATA_LAMBDA, 
    #         Payload=json.dumps(payload), 
    #         InvocationType='Event'
    #     )
    # except ReadTimeoutError as e:
    #     print("read time out error: {}".format(e))
    #     capture_exception(e)
    # except Exception as e:
    #     print("some exception occurred while invoking cache subscribed data lambda: {}".format(e))
    #     capture_exception(e)

def async_invoke_dms_push_lambda(payload):
    try:
        lambda_client.invoke(
            FunctionName=DMS_PUSH_LAMBDA, 
            Payload=json.dumps(payload), 
            InvocationType='Event'
        )
    except ReadTimeoutError as e:
        capture_exception(e)
    except Exception as e:
        capture_exception(e)

def async_invoke_enrichment(payload, function_name):
    try:
        lambda_client.invoke(
            FunctionName=function_name, 
            Payload=json.dumps(payload), 
            InvocationType='Event'
        )
    except ReadTimeoutError as e:
        capture_exception(e)
    except Exception as e:
        capture_exception(e)

def sync_invoke_xml_report_handler(payload):
    try:
        lambda_client.invoke(
           FunctionName=XML_REPORT_LAMBDA, 
           Payload=json.dumps(payload), 
           InvocationType='RequestResponse'
        )
        return True
    except ReadTimeoutError as e:
        print("xml lambda read time out error: {}".format(e))
        capture_exception(e)
        return None
    except Exception as e:
        print("xml report lambda exception: {}".format(e))
        capture_exception(e)

    return None

def sync_invoke_xlsx_report_handler(payload):
    try:
        lambda_client.invoke(
            FunctionName=XLSX_REPORT_LAMBDA, 
            Payload=json.dumps(payload), 
            InvocationType='RequestResponse'
        )
        return True
    except ReadTimeoutError as e:
        print("xlsx lambda read time out error: {}".format(e))
        capture_exception(e)
        return None
    except Exception as e:
        print("xlsx report lambda exception: {}".format(e))
        capture_exception(e)

    return None

def sync_invoke_aggregate_xlsx_report_handler(payload):
    try:
        lambda_client.invoke(
            FunctionName=AGGREGATE_XLSX_REPORT_LAMBDA, 
            Payload=json.dumps(payload), 
            InvocationType='RequestResponse'
        )
        return True
    except ReadTimeoutError as e:
        print("aggregate xlsx lambda read time out error: {}".format(e))
        capture_exception(e)
        return None
    except Exception as e:
        print("aggregate xlsx report lambda exception: {}".format(e))
        capture_exception(e)

    return None

def get_account_warehouse_data(entity_id, warehouse_data):
    # import locally to avoid circular dependency
    from python.handlers import access_handler
    accounts_data = {}
    accounts_data = access_handler({
        "access_type": "ENTITY_ACCOUNTS",
        "entity_id": entity_id,
        "last_updated_date_required": True,
        "is_streaming": True
    }, None)
    current_datetime = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
    for account in accounts_data:
        account.update(**warehouse_data)
        if not account.get("session_id"):
            account["session_id"] = None
        if account.get("credit_limit", None) is not None:
            account['credit_limit'] = float(account['credit_limit'])
        if account.get("od_limit", None) is not None:
            account['od_limit'] = float(account['od_limit'])
        if account.get("account_opening_date", None) is not None:
            account['account_opening_date'] = get_date_of_format(account['account_opening_date'], "%Y-%m-%d")
        for field in ["session_from_date", "session_to_date"]:
            if account.get(field) is not None:
                try:
                    account[field] = datetime.strptime(account[field], "%d/%m/%Y").strftime('%Y-%m-%d')
                except Exception:
                    account[field] = None
                    pass
        if account.get("last_updated", None) is not None:
            account['last_updated'] = datetime.fromtimestamp(int(account['last_updated'])/1000000000).strftime('%Y-%m-%d %H:%M:%S')
        account['entity_id'] = entity_id
        account['created_at'] = current_datetime
    return accounts_data

def prepare_ware_house_identity_data_for_credit_card(warehousing_data):
    data_to_return = {}

    for key, value in warehousing_data.items():
        if key == 'identity':
            identity = warehousing_data.get('identity', dict())
            for identity_key, identity_value in identity.items():
                if identity_key in ['rewards', 'date_range']:
                    for rewards_key, rewards_value in identity_value.items():
                        data_to_return[rewards_key] = rewards_value
                else:
                    data_to_return[identity_key] = identity_value
        elif key == 'template_info':
            template_info = warehousing_data.get('template_info', dict())
            for template_info_key, template_info_value in template_info.items():
                for internal_key, internal_value in template_info_value.items():
                    data_to_return[f'{template_info_key}_{internal_key}'] = internal_value
        else:
            data_to_return[key] = value
        
    data_to_return['payment_due_date'] = data_to_return.get('payment_due_date') if data_to_return.get('payment_due_date') is None else datetime.strftime(data_to_return.get('payment_due_date'), '%Y-%m-%d')
    data_to_return['statement_date'] = data_to_return.get('statement_date') if data_to_return.get('statement_date') is None else datetime.strftime(data_to_return.get('statement_date'), '%Y-%m-%d')
    current_timestamp = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S.%f')
    data_to_return['created_at'] = current_timestamp
    data_to_return['updated_at'] = current_timestamp
    
    return data_to_return

def prepare_statement_metadata_warehouse(warehouse_data, statement_id, bank_name):
    
    warehouse_data['statement_id'] = statement_id
    warehouse_data['bank_name'] = bank_name
    current_timestamp = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S.%f')
    
    warehouse_data['created_at'] = current_timestamp
    warehouse_data['updated_at'] = current_timestamp
    # json_serial custom function converts any element in a json object of a particualar type to desired type to make json serializable
    # eg: Decimal type to string
    warehouse_data = json.loads(json.dumps(warehouse_data, default=json_serial))
    return warehouse_data

def json_serial(obj):
    from decimal import Decimal
    from datetime import date, datetime

    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
        
    if isinstance(obj, (Decimal)):
        return str(round(obj, 3))
        
    raise TypeError ("Type %s not serializable" % type(obj))

def generate_random_string(len):
    random_string = ''.join(random.choices(string.ascii_uppercase + string.ascii_lowercase + string.digits, k=len))
    return random_string

def send_data_to_clickhouse_s3_bucket(data):
    now = datetime.now()
    formatted_time = now.strftime("%Y-%m-%d-%H-%M-%S")
    key = "tcap_call_details/{}/bank-connect-clickhouse-tcap-call-details-{}-1-{}-{}".format(generate_random_string(11), CURRENT_STAGE, formatted_time, str(uuid.uuid4()))
    s3.put_object(Bucket=BANK_CONNECT_CLICKHOUSE_BUCKET, Key=key, Body=json.dumps(data).encode('utf-8'))
    return

def compare_account_numbers(account_number: str, curr_account_number: str) -> bool:
    """Compares two account numbers based on different matching criteria assuming they are from the same bank"""
    
    def remove_hyphens_and_spaces(number: str) -> str:
        """Removes special characters and spaces from the account number."""
        return re.sub("[- ]", "", number)
    
    def get_last_n_characters(number: str, n: int) -> str:
        """Returns last n characters safely"""
        return number[-n:] if len(number) >= n else number

    alphabets_check = re.compile("[a-zA-Z]+", re.IGNORECASE)
    special_characters_check = re.compile("[-*X ]")
    
    stripped_account_number = account_number.lstrip("0")
    stripped_curr_account_number = curr_account_number.lstrip("0")
    
    matches = [
        # Case 1: If either contains alphabets, check last 3 digits
        (alphabets_check.findall(stripped_account_number) or alphabets_check.findall(stripped_curr_account_number)) and
        get_last_n_characters(stripped_account_number, 3) == get_last_n_characters(stripped_curr_account_number, 3),
        
        # Case 2: Exact match ignoring spaces
        stripped_account_number.replace(' ', '') == stripped_curr_account_number.replace(' ', ''),
        
        # Case 3: Remove '-', spaces, and compare while ignoring 'X' or '*'
        any(special_characters_check.search(num) for num in (stripped_account_number, stripped_curr_account_number)) and
        len(remove_hyphens_and_spaces(stripped_account_number)) == len(remove_hyphens_and_spaces(stripped_curr_account_number)) and
        all(a == b or a in "X*" or b in "X*" for a, b in zip(remove_hyphens_and_spaces(stripped_account_number), remove_hyphens_and_spaces(stripped_curr_account_number))),
        
        # Case 4: If completely numeric, compare last 3 digits
        stripped_account_number.isdigit() and stripped_curr_account_number.isdigit() and
        get_last_n_characters(stripped_account_number, 3) == get_last_n_characters(stripped_curr_account_number, 3),
    ]
    
    return any(matches)

def get_account(entity_id, account_number, bank_name=None) -> Union[dict, None]:
    if account_number is None:
        return None

    qp = {
        'KeyConditionExpression': Key('entity_id').eq(entity_id),
        'ConsistentRead': True,
        'ProjectionExpression': 'entity_id,account_id,item_data'}
    accounts = collect_results(bank_connect_account_table.query, qp)

    for account in accounts:
        curr_account_number = account.get('item_data', {}).get('account_number', None)
        if curr_account_number is None:
            continue

        curr_bank_name = account.get('item_data', {}).get('bank', None)
        if bank_name and curr_bank_name and bank_name != curr_bank_name: 
            continue
            
        if compare_account_numbers(account_number, curr_account_number):
            return account

    return None

def create_or_update_account_details_for_pdf(
        entity_id: str, 
        statement_id: str, 
        bank: str, 
        account: Union[AccountDict, None], 
        identity_with_extra_params: dict, 
        identity_lambda_input: dict
    ) -> Union[str, None]:
    identity_from_identity_item_data = identity_with_extra_params.get('identity', dict())

    identity_od_limit = identity_from_identity_item_data.get('od_limit')
    identity_credit_limit = identity_from_identity_item_data.get('credit_limit')
    ifsc = identity_from_identity_item_data.get('ifsc')
    micr = identity_from_identity_item_data.get('micr')
    account_category = identity_from_identity_item_data.get('account_category')
    is_od_account = identity_from_identity_item_data.get('is_od_account')
    account_opening_date = identity_from_identity_item_data.get('account_opening_date')
    pan_number = identity_from_identity_item_data.get("pan_number", "") or ""
    phone_number = identity_from_identity_item_data.get("phone_number", "") or ""
    email = identity_from_identity_item_data.get("email", "") or ""
    dob = identity_from_identity_item_data.get("dob", "") or ""
    account_status = identity_from_identity_item_data.get("account_status", "") or "" # since this is an AA only field
    holder_type = identity_from_identity_item_data.get("account_status", "") or "" # since this is an AA only field
    joint_account_holders = identity_from_identity_item_data.get("joint_account_holders", []) or []

    # Fetch account number
    account_number = identity_from_identity_item_data.get('account_number')

    # Fetch user input data
    re_extraction = identity_lambda_input.get("re_extraction", False)
    user_inputs = identity_lambda_input.get("user_inputs", {})
    ask_od_limit_flag = identity_lambda_input.get("ask_od_limit_flag", False)
    input_account_category = user_inputs.get("input_account_category", None)
    input_is_od_account = user_inputs.get("input_is_od_account", None)

    if account:
        account_id = account.get('account_id', None)
        if not re_extraction:
            # NOTE: Don't add the statement_id to the account on `re_extraction` case
            add_statement_to_account(entity_id, account_id, statement_id)

        account_data_to_update: list[tuple[str, Any]] = []
        if input_account_category is not None:
            account_data_to_update.append(('input_account_category', input_account_category))
            account_data_to_update.append(('input_is_od_account', input_is_od_account))

        if identity_credit_limit:
            account_data_to_update.append(('credit_limit',identity_credit_limit))
        if re_extraction:
            account_data_to_update.append(('account_category',account_category))
            account_data_to_update.append(('micr',micr))
            account_data_to_update.append(('ifsc',ifsc))
        if is_od_account is not None:
            account_data_to_update.append(('is_od_account',is_od_account))
        if account_opening_date:
            account_data_to_update.append(('account_opening_date', account_opening_date))

        # Update account OD Limits
        account_item_data = account.get('item_data')
        updated_od_limit_details = configure_od_limit_after_identity_extraction(account_item_data, identity_from_identity_item_data, 'pdf', ask_od_limit_flag)
        if updated_od_limit_details:
            account_data_to_update.append(('neg_txn_od', updated_od_limit_details.get('neg_txn_od')))
            account_data_to_update.append(('od_limit', updated_od_limit_details.get('od_limit')))

        # Update DDB
        update_account_table_multiple_keys(entity_id, account_id, account_data_to_update)
        return account_id

    print("Creating account with account_number ", account_number)
    return create_new_account(
        entity_id, 
        bank, 
        account_number,
        statement_id, 
        ifsc, 
        micr, 
        account_category,
        is_od_account,
        identity_od_limit,
        identity_credit_limit,
        input_account_category,
        input_is_od_account,
        account_opening_date,
        pan_number,
        phone_number,
        email,
        dob,
        account_status,
        holder_type,
        joint_account_holders
    )

def add_statement_to_account(entity_id, account_id, statement_id):
    bank_connect_account_table.update_item(
        Key={
            'entity_id': entity_id,
            'account_id': account_id
        },
        UpdateExpression="SET item_data.statements = list_append(item_data.statements, :i), updated_at = :u",
        ExpressionAttributeValues={
            ':i': [statement_id],
            ':u': time.time_ns()
        })

def configure_od_limit_after_identity_extraction(account_item_data: Union[AccountItemData, None], identity: dict, extraction_mode: str, ask_od_limit_flag: bool) -> Union[ODLimitConfigAfterIdentityExtraction, None]:
    print("Account category configuration is under process after identity processing")
    identity_extracted_od_limit = identity.get("od_limit")
    if not account_item_data:
        return None
    
    updated_od_limit_details: ODLimitConfigAfterIdentityExtraction = {
        'neg_txn_od': account_item_data.get('neg_txn_od', False),
        'od_limit': account_item_data.get('od_limit', None),
    }

    if extraction_mode == 'pdf' and ask_od_limit_flag and identity.get('is_od_account', False):
        updated_od_limit_details['neg_txn_od'] = False
    
    if identity_extracted_od_limit:
        updated_od_limit_details.update({
            'neg_txn_od': False,
            'od_limit': identity_extracted_od_limit
        })
    print("Extracted identity limit", identity_extracted_od_limit)
    return updated_od_limit_details

def update_account_table_multiple_keys(entity_id, account_id, field_data) -> None:
    if not len(field_data):
        print("account update field data is empty")
        return
    
    update_expression = "SET " + ", ".join(f"item_data.{field[0]} = :i{i}" for i, field in enumerate(field_data)) + ", updated_at = :u"
    expression_attribute_values = {f":i{i}": field[1] for i, field in enumerate(field_data)}
    expression_attribute_values[":u"] = time.time_ns()

    bank_connect_account_table.update_item(
        Key={"entity_id": entity_id, "account_id": account_id},
        UpdateExpression=update_expression,
        ExpressionAttributeValues=expression_attribute_values,
    )

def create_new_account(
    entity_id,
    bank,
    account_number,
    statement_id,
    ifsc=None,
    micr=None,
    account_category=None,
    is_od_account=None,
    od_limit=None,
    credit_limit=None,
    input_account_category = None,
    input_is_od_account = None,
    account_opening_date=None,
    pan_number="",
    phone_number="",
    email="",
    dob="",
    account_status="",
    holder_type="",
    joint_account_holders=[]
):
    account_id = str(uuid.uuid4())

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
            'account_opening_date': account_opening_date,
            'account_category': account_category,
            'is_od_account': is_od_account,
            'od_limit': od_limit,
            'credit_limit': credit_limit,
            'input_account_category': input_account_category,
            'input_is_od_account': input_is_od_account,
            'pan_number': pan_number,
            'phone_number': phone_number,
            'email': email,
            'dob': dob,
            "account_status": account_status,
            "holder_type": holder_type,
            "joint_account_holders": joint_account_holders
        },
        'created_at': time_stamp_in_mlilliseconds,
        'updated_at': time_stamp_in_mlilliseconds
    }

    bank_connect_account_table.put_item(Item=dynamo_object)
    return account_id

def update_page_cnt_and_page_done(statement_id,page_cnt,pages_done):
    bank_connect_statement_table.update_item(
        Key={
            'statement_id': statement_id },
        UpdateExpression="set page_count = :s, pages_done = :m",
        ExpressionAttributeValues={
            ':s': page_cnt,
            ':m': pages_done
        }
    )

def are_future_dates_present_in_statement_transactions(statement_id: str, transactions: list) -> bool:
    
    statement_timestamp = get_statement_created_at_timestamp(statement_id)

    if not statement_timestamp:
        return False
    statement_created_at = None
    if isinstance(statement_timestamp, str):
        try:
            statement_created_at = datetime.fromtimestamp(convert_to_seconds(float(statement_timestamp)))
            statement_created_at += timedelta(hours=5, minutes=30)
        except Exception as _:
            return False
    
    if not statement_created_at:
        return False
    
    for transaction in transactions:
        transaction_date = deepcopy(transaction['date'])
        transaction_datetime, _ = convert_str_date_to_datetime(transaction_date)
        if transaction_datetime > statement_created_at:
            return True
    
    return False

def convert_to_seconds(timestamp: float) -> float:
    """
    Detect if a timestamp is in nanoseconds, milliseconds, or seconds, 
    and convert it to seconds.
    """
    if timestamp > 1e18:  # Likely nanoseconds (more than 1 billion seconds since epoch)
        timestamp_in_seconds = timestamp / 1e9
    elif timestamp > 1e12:  # Likely milliseconds (more than 1 million seconds since epoch)
        timestamp_in_seconds = timestamp / 1e3
    else:  # Already in seconds
        timestamp_in_seconds = timestamp

    return timestamp_in_seconds

def get_statement_created_at_timestamp(statement_id: str) -> Union[str, None]:
    
    statement_items = bank_connect_statement_table.query(KeyConditionExpression=Key('statement_id').eq(statement_id))
    
    if statement_items.get('Count') == 0:
        return None
    
    statement_item = statement_items.get('Items')[0]
    statement_timestamp = statement_item.get('created_at')
    
    return statement_timestamp

def update_transactions_on_session_date_range(session_date_range, transactions, statement_id, page_number = None):
    if isinstance(session_date_range, dict) and session_date_range.get('from_date') != None and session_date_range.get('to_date') != None:
        tmp_transactions = list()
        session_from_date = datetime.strptime(session_date_range.get('from_date'), '%d/%m/%Y')
        session_to_date = datetime.strptime(session_date_range.get('to_date'), '%d/%m/%Y') + timedelta(days=1)
        trxns_beyond_session_date_range_exists = False
        for tranaction in transactions:
            tmp_date = tranaction.get('date')
            if not isinstance(tmp_date, datetime):
                try:
                    tmp_date = datetime.strptime(tmp_date, "%Y-%m-%d %H:%M:%S")
                except Exception as e:
                    capture_exception(e)
                    return transactions
            if isinstance(tmp_date, datetime) and tmp_date >= session_from_date and tmp_date < session_to_date:
                tranaction.update({"is_in_session_date_range": True})
            else:
                tranaction.update({"is_in_session_date_range": False})
                trxns_beyond_session_date_range_exists = True
            tmp_transactions.append(tranaction)
        
        if trxns_beyond_session_date_range_exists and page_number != None:
            update_field_for_statement(statement_id, f'cut_transactions_page_{page_number}', True)

        transactions = tmp_transactions

    return transactions


def update_field_for_statement(statement_id, field_name, field_value):
    bank_connect_statement_table.update_item(
        Key={'statement_id': statement_id},
        UpdateExpression=f"set {field_name} = :c",
        ExpressionAttributeValues={
            ':c': field_value
        }
    )

def update_inconsistency_data_for_statement(statement_id, inconsistency_data):
    inconsistent_hashes = inconsistency_data.get('inconsistent_hashes', [])
    clean_date_chunks = inconsistency_data.get('clean_date_chunks', [])
    is_missing_data = inconsistency_data.get('is_missing_data', False)
    
    bank_connect_statement_table.update_item(
        Key={'statement_id': statement_id},
        UpdateExpression="set inconsistent_hashes = :i, clean_date_chunks = :c, is_missing_data = :m, updated_at = :u",
        ExpressionAttributeValues={
            ':i': json.dumps(inconsistent_hashes),
            ':c': json.dumps(clean_date_chunks),
            ':m': is_missing_data,
            ':u': time.time_ns()
        }
    )

def send_message_to_slack(SLACK_TOKEN, SLACK_CHANNEL, message):
    slack_client = WebClient(token=SLACK_TOKEN)
    slack_client.chat_postMessage(
        channel=SLACK_CHANNEL,
        text=message
    )

#call django server to insert data into kafka
def call_django_to_insert_data_into_kafka(topic_name, payload):
    data = {
        'topic_name': topic_name,
        'payload': payload
    }

    url = '{}/bank-connect/v1/internal/kafka_insert/'.format(DJANGO_BASE_URL)
    headers = {
        'x-api-key': API_KEY,
        'Content-Type': "application/json",
    }

    #calling django
    payload = json.dumps(data, default=str)
    call_api_with_session(url, "POST", payload, headers)
    return


def get_statement_type(fan_out_info_dashboard_resp: dict) -> str:
    """
        There might be xml type statements aswell.
    """
    statement_attempt_type = fan_out_info_dashboard_resp.get('statement_attempt_type', 'pdf')
    if statement_attempt_type == 'pdf':
        return 'statement'
    if statement_attempt_type == 'online':
        return 'netbankingFetch'
    if statement_attempt_type == 'aa':
        return 'accountAggregator'
    return statement_attempt_type


def get_dms_logs_data(entity_id, created_on):
    log_data = dict()
    if isinstance(created_on, datetime):
        formatted_date = datetime.today().strftime("%d-%m-%Y")
    else:
        try:
            formatted_date = datetime.strptime(created_on, "%Y-%m-%d %H:%M:%S").strftime("%d-%m-%Y")
        except:
            print("Error while converting date")
            return log_data

    for dms_status in ["success", "failed"]:
        try:
            object_key = f"{formatted_date}/{dms_status}/entity_{entity_id}.json"
            obj = s3.get_object(Bucket=BANK_CONNECT_DMS_PUSH_LOGS_BUCKET, Key=object_key)
            data = obj['Body'].read()
            log_data = json.loads(data)
        except Exception as e:
            print(f"Error while Fetching data from s3 for {entity_id} for {dms_status} == {e}")
            continue

    return log_data


def move_clickhouse_data(source_table, action, fanout_data, local_logging_context):
    print("Inside the function move_clickhouse_data before import ===================== ")
    from python.clickhouse.firehose import send_data_to_firehose
    from python.clickhouse_data_formatter import prepare_tcap_recurring_pulls_data

    total_success_action = 0
    response = {
        "source_table": source_table,
        "action": action,
        "total_entities": len(fanout_data),
        "total_success_action": total_success_action
    }

    LAMBDA_LOGGER.info(
        f"Inside move clickhouse data functionality with source = {source_table} and action = {action}",
        extra=local_logging_context.store)

    if action == "UPDATE":
        if not fanout_data:
            LAMBDA_LOGGER.info(f"No fanout data found. Exiting", extra=local_logging_context.store)
            return response

        if source_table == "tcap_call_details":
            for session_id in fanout_data:
                fan_out_info_dashboard_resp = fanout_data.get(session_id)
                created_on = fan_out_info_dashboard_resp.get("created_on")
                dms_log_data = get_dms_logs_data(session_id, created_on)
                print(f"This is the DMS log response ==== {dms_log_data}")

                if not dms_log_data:
                    LAMBDA_LOGGER.info(f"No dms logs found for {session_id} and hence skipping",
                                       extra=local_logging_context.store)
                    continue

                dms_response = dms_log_data.get("dms_response", {})

                session_metadata = dms_log_data.get("session_metadata")
                if not session_metadata:
                    continue

                tcap_customers_data = prepare_tcap_customers_data(session_metadata)
                send_data_to_firehose([tcap_customers_data], TCAP_CUSTOMERS_STREAM_NAME)
                print(f"send_data_to_firehose completed")

                tcap_call_details = prepare_tcap_call_details(session_metadata, dms_response)
                tcap_call_details['Destination'] = get_statement_type(fan_out_info_dashboard_resp)
                send_data_to_clickhouse_s3_bucket(tcap_call_details)
                total_success_action += 1

        elif source_table == "tcap_recurring_pulls":
            for session_id in fanout_data:
                fan_out_info_dashboard_resp = fanout_data.get(session_id)
                tcap_recurring_pulls_data = prepare_tcap_recurring_pulls_data(session_id, fan_out_info_dashboard_resp,
                                                                              local_logging_context)
                LAMBDA_LOGGER.info(f"Data to be pushed to recurring pulls table = {tcap_recurring_pulls_data}",
                                   extra=local_logging_context.store)
                send_data_to_firehose([tcap_recurring_pulls_data], TCAP_RECURRING_AA_PULLS_STREAM_NAME)
                total_success_action += 1
    response["total_success_action"] = total_success_action
    return response