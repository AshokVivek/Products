import json
import os
import time
import traceback
import datetime
import re
from typing import Any, Optional, Tuple, TypedDict, Union
import warnings
import pandas as pd
from fuzzywuzzy import fuzz
from boto3.dynamodb.conditions import Key
from boto3.dynamodb.types import TypeSerializer
from sentry_sdk import capture_exception, capture_message
from python.configs import  STATEMENT_TYPE_MAP, QUALITY_QUEUE_URL, sqs_client
from python.constants import TRXN_KEYS_DEFAULT_VALUES, FRAUD_TYPE_PRECEDENCE_MAPPING, FRAUD_TO_ERROR_MAPPING
import math
from library.date_utils import get_months_from_periods, get_missing_date_range_on_extraction, is_month_missing, is_missing_dates
from library.transaction_description import get_self_transfer_description
from library.extract_txns_finvu_aa import get_transaction_channel_description_hash
from library.fraud import get_correct_transaction_order, get_inconsistency_date_range,transaction_balance_check, \
    account_level_frauds, fraud_category, optimise_transaction_type, remove_duplicate_transactions, optimise_refund_transactions, \
        correct_transactions_date, process_merged_pdf_transactions, merge_partial_transaction_notes, get_transactions_with_updated_categorization
from library.finvu_aa_inconsistency_removal import remove_finvu_aa_inconsistency, fix_yesbnk_inc_transactions, swap_inconsistent_trxns
from library.fsm_excel_report import produce_advanced_features
from python.configs import LAMBDA_LOGGER, DISPARITIES_STREAM_NAME
from python.database_utils import prepare_identity_rds_warehouse_data
from python.kafka_producer import send_data_to_kafka, send_large_list_payload_to_kafka
from python.utils import  deseralize_metadata_analysis, deseralize_od_metadata, deseralize_preshared_names, get_date_of_format, get_transactions_list_of_lists_finvu_aa, async_invoke_cache_subscribed_data, \
    prepare_disparities_warehouse_data, get_account, AccountDict, \
    AccountItemData, update_account_table_multiple_keys, create_or_update_account_details_for_pdf, \
    update_page_cnt_and_page_done, send_event_to_update_state_queue, are_future_dates_present_in_statement_transactions

from library.salary import get_salary_transactions, separate_probable_salary_txn_grps, get_salary_transactions_v1
from library.txn_metrics import get_entity_metrics
from library.date_utils import convert_date_range_to_datetime, change_date_format
from python.aws_utils import collect_results
from copy import deepcopy
from dateutil.relativedelta import relativedelta
from python.enrichment_regexes import check_and_get_everything

from python.configs import (
    bank_connect_enrichments_table,
    bank_connect_statement_table
)
from python.context.logging import LoggingContext
from python.configs import bank_connect_statement_table, BANK_CONNECT_DDB_FAILOVER_BUCKET, bank_connect_transactions_table, bank_connect_recurring_table, bank_connect_account_table, s3, BANK_CONNECT_CACHEBOX_BUCKET, BANK_CONNECT_REPORTS_BUCKET, CURRENT_STAGE, \
    DJANGO_BASE_URL, API_KEY, s3_resource, bank_connect_salary_table, bank_connect_identity_table, bank_connect_disparities_table, KAFKA_TOPIC_INCONSISTENCY
from library.recurring_lender_transactions import get_recurring_lender_debit_transactions
from library.transaction_channel import update_bounce_transactions_for_account_transactions, mark_refund_on_basis_of_same_balance, \
        mark_reversal_on_basis_of_neg_balance, update_transaction_channel_after_all_transactions
from library.utils import single_transaction_hash, amount_to_float, process_hsbc_ocr_transactions, add_hash_to_transactions_df
from library.extract_txns_fitz import get_account_wise_transactions_dict
from library.validations import front_fill_balance
from library.transactions import get_account_category_from_transactions
import calendar
import random
from python.utils import update_transactions_on_session_date_range, update_inconsistency_data_for_statement, check_and_distribute_transactions_in_pages
from python.api_utils import call_api_with_session
warnings.simplefilter(action = "ignore", category = FutureWarning)
pd.options.mode.chained_assignment = None


redundant_keys = ['unclean_merchant', 'is_lender', 'merchant', 'page_number', 'optimizations', 'transaction_channel_regex', 'description_regex', 'sequence_number']
required_keys = ['transaction_type', 'transaction_note', 'chq_num', 'amount', 'balance', 'date', 'transaction_channel', 'hash', 'merchant_category', 'description', 'account_id', 'month_year', 'salary_month', 'category', 'perfios_txn_category']

def get_link_id_overview(entity_ids):
    progress_data = list()

    for entity_id in entity_ids:
        bank = None
        account_data = get_accounts_for_entity(entity_id)

        if len(account_data) > 0:
            bank = account_data[0].get('item_data').get('bank')

        months = get_all_months(entity_id)
        entity_item = {
            'entity_id': entity_id,
            'months': months,
            'bank': bank
        }

        progress_data.append(entity_item)
    return progress_data

def combine_transaction_lists(list_of_transaction_lists):
    combined_transactions = list()

    if len(list_of_transaction_lists) == 0:
        return list()

    while max([len(txn_list) for txn_list in list_of_transaction_lists]) > 0:
        dates = [txn_list[0]['date'] if len(
            txn_list) > 0 else None for txn_list in list_of_transaction_lists]
        min_date = min([date for date in dates if date is not None])
        min_date_index = dates.index(min_date)
        combined_transactions.append(
            list_of_transaction_lists[min_date_index].pop(0))

    return combined_transactions

def binary_search(list_trans, find_date):
    start  = 0
    end = len(list_trans)
    mid = start
    while(start<end):
        mid = (start+end)//2
        if datetime.datetime.strptime(list_trans[mid]["date"], '%Y-%m-%d %H:%M:%S').date()<find_date.date():
            start = mid +1
        else:
            end = mid
    return mid

def combine_and_dedup_transaction_lists(list_of_txn_list):
    # remove empty lists from input lists
    list_of_txn_list = list(filter(lambda x: len(x) > 0, list_of_txn_list))
    if len(list_of_txn_list) == 0:
        return list()
    
    temp = []
    for lists in list_of_txn_list:
        temp_list = []
        for transaction in lists:
            if check_valid_transaction(transaction):
                temp_list.append(transaction)
        temp.append(temp_list)
    list_of_txn_list = deepcopy(temp)
    # remove empty lists again after performing checks.
    list_of_txn_list = list(filter(lambda x: len(x) > 0, list_of_txn_list))
    if len(list_of_txn_list) == 0:
        return list()
    sorted_list = sorted(list_of_txn_list, key=lambda x: x[0]['date'])
    combined_txn = []
    combined_txn.extend(sorted_list[0])
    for i in range(1, len(sorted_list)):
        flag = 0
        new_txn_list = sorted_list[i]
        min_date_new_list = datetime.datetime.strptime(new_txn_list[0]['date'], '%Y-%m-%d %H:%M:%S')
        max_date_new_list = datetime.datetime.strptime(new_txn_list[-1]['date'], '%Y-%m-%d %H:%M:%S')
        max_date_combined_list = datetime.datetime.strptime(combined_txn[-1]['date'], '%Y-%m-%d %H:%M:%S')
        if (min_date_new_list <= max_date_combined_list) and (max_date_new_list <= max_date_combined_list):
            # new list is a subset of combined list
            continue
        elif (min_date_new_list <= max_date_combined_list):
            # new list and combined list have someoverlap
            start = binary_search(combined_txn, min_date_new_list)
            for k in range(start, len(combined_txn)):
                if(datetime.datetime.strptime(combined_txn[k]['date'], '%Y-%m-%d %H:%M:%S').date() == min_date_new_list.date()) and (combined_txn[k]['amount'] == new_txn_list[0]['amount']) and (combined_txn[k]['balance'] == new_txn_list[0]['balance']) and (combined_txn[k]['transaction_type'] == new_txn_list[0]['transaction_type']) and (re.sub('[^A-Za-z0-9]+', '',combined_txn[k]["transaction_note"].lower()) == re.sub('[^A-Za-z0-9]+', '',new_txn_list[0]["transaction_note"].lower()) or (fuzz.partial_ratio(re.sub('[^A-Za-z0-9]+', '',combined_txn[k]["transaction_note"].lower()), re.sub('[^A-Za-z0-9]+', '',new_txn_list[0]["transaction_note"].lower())) >= 50 and len(combined_txn[k]["transaction_note"]) >= 4 and len(new_txn_list[0]["transaction_note"]) >= 4)):
                    # found a similar looking transaction, check if next transaction also matches on both list
                    if (k < len(combined_txn)-1):
                        if (len(new_txn_list) > 1):
                            # both list have next transaction
                            if (datetime.datetime.strptime(combined_txn[k+1]['date'], '%Y-%m-%d %H:%M:%S').date() == datetime.datetime.strptime(new_txn_list[1]['date'], '%Y-%m-%d %H:%M:%S').date()) and (combined_txn[k+1]['amount'] == new_txn_list[1]['amount']) and (combined_txn[k+1]['balance'] == new_txn_list[1]['balance']) and (combined_txn[k+1]['transaction_type'] == new_txn_list[1]['transaction_type']) and (re.sub('[^A-Za-z0-9]+', '',combined_txn[k+1]["transaction_note"].lower()) == re.sub('[^A-Za-z0-9]+', '',new_txn_list[1]["transaction_note"].lower()) or (fuzz.partial_ratio(re.sub('[^A-Za-z0-9]+', '',combined_txn[k+1]["transaction_note"].lower()), re.sub('[^A-Za-z0-9]+', '',new_txn_list[1]["transaction_note"].lower())) >= 50 and len(combined_txn[k+1]["transaction_note"]) >= 4 and len(new_txn_list[1]["transaction_note"]) >= 4)):
                                # next transactions are also matching, search for next transactions
                                if (k < len(combined_txn) - 2):
                                    if (len(new_txn_list) > 2):
                                        if (datetime.datetime.strptime(combined_txn[k + 2]['date'], '%Y-%m-%d %H:%M:%S').date() == datetime.datetime.strptime(new_txn_list[2]['date'], '%Y-%m-%d %H:%M:%S').date()) and (combined_txn[k + 2]['amount'] == new_txn_list[2]['amount']) and (combined_txn[k + 2]['balance'] == new_txn_list[2]['balance']) and (combined_txn[k + 2]['transaction_type'] == new_txn_list[2]['transaction_type']) and (re.sub('[^A-Za-z0-9]+', '',combined_txn[k + 2]["transaction_note"].lower()) == re.sub('[^A-Za-z0-9]+', '',new_txn_list[2]["transaction_note"].lower()) or (fuzz.partial_ratio(re.sub('[^A-Za-z0-9]+', '',combined_txn[k + 2]["transaction_note"].lower()), re.sub('[^A-Za-z0-9]+', '',new_txn_list[2]["transaction_note"].lower())) >= 50 and len(combined_txn[k + 2]["transaction_note"]) >= 4 and len(new_txn_list[2]["transaction_note"]) >= 4)):
                                            combined_txn.extend(new_txn_list[len(combined_txn)-k:])
                                            flag = 1
                                            break
                                        else:
                                            # next transactions are not matching, so we need to keep looking
                                            continue
                                    else:
                                        # new list had only 2 transactions which was similar
                                        flag = 1
                                        break
                                else:
                                    # second last and last transaction of combined list is same as that of 1st and 2nd transaction of new list, merge
                                    combined_txn.extend(new_txn_list[2:])
                                    flag = 1
                                    break
                            else:
                                # next transactions are not matching, so we need to keep looking
                                continue
                        else:
                            # new list had only 1 transaction which was similar
                            flag = 1
                            break
                    else:
                        # last transaction of combined list is same as that of 1st transaction of new list, merge
                        flag = 1
                        combined_txn.extend(new_txn_list[1:])
                        break
            # tried searching for similar transactions but could not find, so append the whole new txn list
            if flag == 0:
                combined_txn.extend(new_txn_list)
        elif (min_date_new_list > max_date_combined_list):
            combined_txn.extend(new_txn_list)

    return combined_txn


def get_page_level_transactions_for_statement(entity_id, account_id, statement_id):
    page_level_dict = {}
    page_transactions_level_dict = {}

    qp = {
        'KeyConditionExpression': Key('statement_id').eq(statement_id),
        'ConsistentRead': True, 
        'ProjectionExpression': 'statement_id, page_number, item_data'
    }
    transaction_items = collect_results(bank_connect_transactions_table.query, qp)
    for transaction_item in transaction_items:
        page_number = transaction_item.get('page_number')
        transactions = json.loads(transaction_item.get('item_data', '[]'))
        payload = {
            "entity_id": entity_id,
            "account_id": account_id,
            "statement_id": statement_id,
            "transactions": transactions
        }
        key = f"entity_{entity_id}/account_{account_id}/statement_{statement_id}/page_{page_number}.json"
        s3.put_object(Bucket=BANK_CONNECT_CACHEBOX_BUCKET, Key=key, Body=json.dumps(payload).encode('utf-8'))
        page_level_dict[str(page_number)] = key
        page_transactions_level_dict[str(page_number)] = payload
    
    return page_level_dict, page_transactions_level_dict

def get_transactions_for_statement_page(statement_id, page_number):
    qp = {
        'KeyConditionExpression': Key('statement_id').eq(statement_id) & Key('page_number').eq(page_number),
        'ConsistentRead': True, 
        'ProjectionExpression': 'statement_id, page_number, item_data'
    }

    transaction_items = collect_results(bank_connect_transactions_table.query, qp)
    if len(transaction_items)>0:
        transactions = json.loads(transaction_items[0].get('item_data', []))
        for trans in transactions:
            trans['page_number'] = int(page_number)
        return transactions

    return []
    

def get_transactions_for_statement(statement_id, keep_same_order = False, send_hash_page_number_map=False, show_rejected_transactions=False, fetch_all_transactions=False):
    qp = {
        'KeyConditionExpression': Key('statement_id').eq(statement_id),
        'ConsistentRead': True, 
        'ProjectionExpression': 'statement_id, page_number, item_data, template_id'
    }

    transaction_items = collect_results(bank_connect_transactions_table.query, qp)
    transactions = list()
    hash_page_number_map = {}

    # print(transaction_items)
    items = bank_connect_statement_table.query(KeyConditionExpression=Key('statement_id').eq(statement_id))
    if items.get('Count') == 0 or (items.get('Items')[0].get('to_reject_statement', False) and not show_rejected_transactions):
        return transactions, hash_page_number_map

    entry = items.get('Items')[0]
    last_page = entry.get('last_page_index', -1)
    # last_page = len(transaction_items) if last_page == -1 else last_page
    print("Last page from bsa results page count table: ",last_page)
    
    for transaction_item in transaction_items:
        page_number = transaction_item.get('page_number')
        template_id = transaction_item.get('template_id')
        
        if last_page!=-1 and page_number>last_page:
            print("Exceeds last page, disregarding")
            continue
        try:
            page_transactions = json.loads(transaction_item.get('item_data', '[]'))
            for transaction_sequence, trans in enumerate(page_transactions):
                trans['page_number'] = int(page_number)
                trans['sequence_number'] = transaction_sequence
                trans['template_id'] = template_id
            transactions += page_transactions
            
            if send_hash_page_number_map:
                for txn in page_transactions:
                    hash = txn.get("hash")
                    if hash not in hash_page_number_map:
                        hash_page_number_map[hash] = [[statement_id, page_number]]
                    else:
                        hash_page_number_map[hash].append([statement_id, page_number])

        except Exception:
            print("Malformed transaction, not adding")

    if keep_same_order == False:
        transactions = get_correct_transaction_order(transactions)
    valid_transactions = []
    for transaction in transactions:
        if check_valid_transaction(transaction, fetch_all_transactions=fetch_all_transactions):
            valid_transactions.append(transaction)

    return valid_transactions, hash_page_number_map

def get_page_count_for_statement(statement_id):
    qp = {
        'KeyConditionExpression': Key('statement_id').eq(statement_id),
        'ConsistentRead': True, 
        'ProjectionExpression': 'transaction_count'
    }

    transaction_items = collect_results(bank_connect_transactions_table.query, qp)
    page_count = len(transaction_items)

    return page_count

def handle_more_than_one_year_cases(statement_id, transactions_list):
    done_MM = []
    year_increment = 0
    pages_to_be_updated = set()

    initial_transaction_list = deepcopy(transactions_list)
    for txn in transactions_list:
        current_datetime = datetime.datetime.strptime(txn.get('date'), '%Y-%m-%d %H:%M:%S')
        current_MM = current_datetime.strftime('%m')
        if len(done_MM)==0 or (len(done_MM)>0 and done_MM[-1]!=current_MM):
            done_MM.append(current_MM)

        if len(done_MM)==13:
            done_MM = [current_MM]
            year_increment+=1
        
        try:
            current_datetime = current_datetime.replace(year=current_datetime.year + year_increment)
        except Exception as e:

            #  This happens when we have multiaccount statement.
            #  For first account when 29th Feb occurs ( 29th Feb 2024 ), it is fine
            #  But for next account when 29th Feb accours it tries to make it next year and fails

            print(e, 'Reverting changes in the transactions for stanchar')
            transactions_list = deepcopy(initial_transaction_list)
            pages_to_be_updated = set()
            break

        txn['date'] = current_datetime.strftime('%Y-%m-%d %H:%M:%S')
        if year_increment!=0:
            pages_to_be_updated.add(txn.get('page_number'))

    if len(pages_to_be_updated) > 0:
        update_transactions_in_ddb(statement_id, pages_to_be_updated, transactions_list)
    return transactions_list


def detect_auto_debit_bounce_transactions(transaction_list):
    # a transaction is marked as `auto_debit_payment_bounce` when
    # it is a `lender transaction` and a `credit` and there exists a
    # transaction on same DATE which is a `debit`, `lender transaction`
    # and has same AMOUNT
    for index, transaction in enumerate(transaction_list):
        if transaction["transaction_type"] == "debit" and transaction["transaction_channel"] == "auto_debit_payment" and transaction["description"] == "lender_transaction" and index != len(transaction_list) - 1:
            for i in range(index + 1, len(transaction_list)):
                if transaction["date"] < transaction_list[i]["date"]:
                    # we don't need to need to on next date
                    break

                if transaction_list[i]["transaction_type"] == "credit" and transaction_list[i]["description"] == "lender_transaction" and transaction_list[i]["date"] == transaction["date"] and transaction_list[i]["amount"] == transaction["amount"]:
                    # mark this transaction as `auto_debit_payment_bounce`
                    transaction_list[i]["transaction_channel"] = "auto_debit_payment_bounce"
                    break

    return transaction_list

def remove_redundant_keys(transactions):
    for index in range(0, len(transactions)):
        for key in redundant_keys:
            transactions[index].pop(key, None)
    return transactions

def keep_specific_keys(transactions):
    final_transactions = []
    for transaction_obj in transactions:
        new_obj = deepcopy(transaction_obj)
        for k in transaction_obj.keys():
            if k not in required_keys:
                new_obj.pop(k)
        final_transactions.append(new_obj)
    return final_transactions


def fill_transactions_na_key(transaction_list: list, key_value_default_dict: dict = TRXN_KEYS_DEFAULT_VALUES):
    transaction_df = pd.DataFrame(transaction_list)
    transaction_df_filled = transaction_df.fillna(key_value_default_dict)
    
    final_transaction_list = transaction_df_filled.to_dict('records')
    return final_transaction_list


def get_transactions_for_account(entity_id, account_id, send_hash_page_number_map=False, show_rejected_transactions=False):
    account_statements = get_statement_ids_for_account_id(entity_id, account_id)
    list_of_txn_lists = list()
    
    hash_dict = {}

    for statement_id in account_statements:
        transactions, hash_page_number_map = get_transactions_for_statement(statement_id, False, send_hash_page_number_map, show_rejected_transactions)
        for i in range(len(transactions)):
            transactions[i].pop('optimizations', None)
            transactions[i].pop('optimizations_old', None)
        list_of_txn_lists.append(transactions)
        for hash in hash_page_number_map:
            if hash not in hash_dict:
                hash_dict[hash] = hash_page_number_map[hash]
            if hash_page_number_map[hash] not in hash_dict[hash]:
                hash_dict[hash] += hash_page_number_map[hash]

    all_transactions = combine_and_dedup_transaction_lists(list_of_txn_lists)

    # remove dedundant keys and add account id
    for index in range(0, len(all_transactions)):
        all_transactions[index]['account_id'] = account_id

    return all_transactions, hash_dict

def get_account_id_for_statement(statement_id):
    rows = bank_connect_identity_table.query(
        KeyConditionExpression=Key('statement_id').eq(statement_id),
        ConsistentRead=True)

    items = rows['Items']

    if len(items) > 0:
        return items[0].get('item_data', dict()).get('identity', dict()).get('account_id', None)
    return None

def get_bank_name_for_statement(statement_id):
    rows = bank_connect_identity_table.query(
        KeyConditionExpression=Key('statement_id').eq(statement_id),
        ConsistentRead=True)

    items = rows['Items']

    if len(items) > 0:
        return items[0].get('item_data', dict()).get('identity', dict()).get('bank_name', None)
    return None

def get_country_for_statement(statement_id):
    rows = bank_connect_identity_table.query(
        KeyConditionExpression=Key('statement_id').eq(statement_id),
        ConsistentRead=True)

    items = rows['Items']

    country_code=None
    if len(items) > 0:
        country_code = items[0].get('item_data', dict()).get('country_code', None)
    country_code = country_code or 'IN'
    return country_code

def get_currency_for_statement(statement_id):
    rows = bank_connect_identity_table.query(
        KeyConditionExpression=Key('statement_id').eq(statement_id),
        ConsistentRead=True)

    items = rows['Items']

    currency_code=None
    if len(items) > 0:
        currency_code = items[0].get('item_data', dict()).get('currency_code', None)
    currency_code = currency_code or 'INR'
    return currency_code

def get_statement_ids_for_account_id(entity_id, account_id):
    row = bank_connect_account_table.query(KeyConditionExpression=Key('entity_id').eq(entity_id) & Key('account_id').eq(account_id))

    if row.get('Count') == 0:
        return list()
    return row.get('Items')[0].get('item_data').get('statements')

def update_last_page(statement_id, page_number):
    items = bank_connect_statement_table.query(KeyConditionExpression=Key('statement_id').eq(statement_id))
    
    entry = items.get('Items')[0]

    last_page = entry.get('last_page_index', -1)

    should_update = True if (last_page==-1 or page_number<last_page) else False
    
    if should_update:
        print("Updating last page for {} with {}".format(statement_id, page_number))
        bank_connect_statement_table.update_item(
            Key={'statement_id': statement_id},
            UpdateExpression="set {} = :s".format('last_page_index'),
            ExpressionAttributeValues={':s': page_number}
        )      

def get_complete_progress(statement_id):
    items = bank_connect_statement_table.query(KeyConditionExpression=Key('statement_id').eq(statement_id))

    identity_status,identity_message = get_progress(statement_id,'identity_status',items)
    transaction_status, transaction_message = get_progress(statement_id,'transactions_status',items)
    processing_status, processing_message = get_progress(statement_id,'processing_status',items)
    fraud_status, _ = get_fraud_progress(statement_id, items)
    response = {
        'identity_status':identity_status,
        'identity_message':identity_message,
        'transaction_status':transaction_status,
        'transaction_message':transaction_message,
        'processing_status':processing_status,
        'processing_message':processing_message,
        'fraud_status': fraud_status
    }
    return response

def get_fraud_progress(statement_id, items=None):
    if items == None:
        items = bank_connect_statement_table.query(KeyConditionExpression=Key('statement_id').eq(statement_id))
    
    if items.get('Count') == 0:
        return 'failed', 'record not found'

    entry = items.get('Items')[0]
    metadata_fraud_status = entry.get('metadata_fraud_status', None)
    page_identity_fraud_status = entry.get('page_identity_fraud_status', None)

    if metadata_fraud_status == None or page_identity_fraud_status == None:   #this is for old cases where these fileds are not present
        return 'completed', None
    if metadata_fraud_status == 'failed' or page_identity_fraud_status == 'failed':
        return 'failed', None
    if metadata_fraud_status == 'processing' or page_identity_fraud_status == 'processing':
        return 'processing', None
    return 'completed', None

def get_statement_table_data(statement_id):
    items = bank_connect_statement_table.query(KeyConditionExpression=Key('statement_id').eq(statement_id))
    if items.get('Count') == 0:
        return dict()
    
    return items.get('Items')[0]

def get_progress(statement_id, status_type, items=None):
    if items == None:
        items = bank_connect_statement_table.query(KeyConditionExpression=Key('statement_id').eq(statement_id))

    if items.get('Count') == 0:
        return 'failed', 'record not found'

    entry = items.get('Items')[0]

    if entry.get('is_extracted_by_nanonets', False):
        return entry.get(status_type, 'processing'), entry.get('message', None)

    if entry.get('attempt_type') == 'aa':
        return entry.get(status_type, 'processing'), entry.get('message', None)
    
    status = entry.get(status_type, 'processing')

    if entry.get('identity_status') == 'completed' and entry.get(status_type) is None:
        return 'processing', 'processing document'

    # print("entry: {}".format(entry))
    entry_created_at = entry.get('created_at')

    if status == 'processing':

        if (
            is_empty(value=entry_created_at) or
            not is_castable_into_int(value=entry_created_at)
        ):
            # marking transaction and processing statuses as failed 
            # if created_at is not found or if it is not castable into an integer
            update_progress(statement_id, 'transactions_status', 'failed')
            update_progress(statement_id, 'processing_status', 'failed')
            
            entry["transactions_status"] = "failed"
            entry["processing_status"] = "failed"

        elif int(entry_created_at) < int(time.time()) - 90:
            is_complete, is_extracted, _ = check_extraction_progress(statement_id)
            is_identity_extracted = check_if_identity_complete(statement_id)

            if is_extracted and is_complete:
                update_progress(statement_id, 'transactions_status', 'completed')
                # update_progress_on_dashboard(statement_id,
                # {'is_extracted': is_extracted, 'is_complete': is_complete})

                if status_type == 'transactions_status':
                    return 'completed', None
            else:
                update_progress(statement_id, 'transactions_status', 'failed')
                update_progress_on_dashboard(statement_id, {
                        'is_extracted': is_extracted, 'is_complete': is_complete})

                if status_type == 'transactions_status':
                    return 'failed', 'taking too long'

            if is_identity_extracted:
                update_progress(statement_id, 'identity_status', 'completed')
                if status_type == 'identity_status':
                    return 'completed', None
            else:
                update_progress(statement_id, 'identity_status', 'failed')
                if status_type == 'identity_status':
                    return 'failed', 'taking too long'

            update_progress(statement_id, 'processing_status', 'failed')

    print("returning the default value")
    return entry.get(status_type, 'failed'), entry.get('message')

def is_castable_into_int(value: Any) -> bool:
    """
    Function which checks whether a value is castable into an integer or not.
    Returns True or False accordingly.
    """
    
    try:
        _ = int(value)
        return True

    except Exception as _:
        return False
    
def is_empty(value: Any) -> bool:
    """
    Function which checks whether a value is None type or not.
    Return True or False accordingly.
    """

    return value in [None, ""]

def get_transactions_status(statement_id):
    count_item = bank_connect_statement_table.query(KeyConditionExpression=Key('statement_id').eq(statement_id))
    all_items = count_item.get('Items')

    if len(all_items) == 0:
        return False
    else:
        status = all_items[0].get('transactions_status')
        return status

def check_extraction_progress(statement_id):
    count_item = bank_connect_statement_table.query(KeyConditionExpression=Key('statement_id').eq(statement_id))
    all_items = count_item.get('Items')

    if len(all_items) == 0:
        expected_page_count = 0
    else:
        # the next two lines check if it's already done with all stages of
        # processing
        status = all_items[0].get('processing_status')
        if status == 'completed':
            return True, True, True
        if status == 'failed':
            return True, False, True

        expected_page_count = all_items[0].get('page_count')

    qp = {
        'KeyConditionExpression': Key('statement_id').eq(statement_id),
        'ProjectionExpression': 'statement_id, transaction_count',
        'ConsistentRead': True
    }

    pages = collect_results(bank_connect_transactions_table.query, qp)

    transaction_count = int(sum([item['transaction_count'] for item in pages]))

    is_complete = (len(pages) == expected_page_count and expected_page_count != 0)
    is_extracted = (transaction_count > 0)

    return is_complete, is_extracted, False

def check_if_identity_complete(statement_id):
    qp = {
        'KeyConditionExpression': Key('statement_id').eq(statement_id),
        'ConsistentRead': True
    }
    entries = collect_results(bank_connect_identity_table.query, qp)

    if len(entries) > 0:
        return True

    return False

def get_account_for_entity(entity_id, account_id, to_reject_account=False) -> Union[AccountDict, None]:
    qp = {
        'KeyConditionExpression': Key('entity_id').eq(
        entity_id) & Key('account_id').eq(account_id)
    }

    account = collect_results(bank_connect_account_table.query, qp)
    if len(account)>0:
        if to_reject_account:
            item_status = account[0].get('item_status')
            if item_status and item_status.get('account_status') != 'completed':
                return None
        return account[0]
    return None

def get_accounts_for_entity(entity_id, to_reject_account=False):
    qp = {'KeyConditionExpression': Key('entity_id').eq(entity_id)}

    accounts = collect_results(bank_connect_account_table.query, qp)
    if to_reject_account:
        accounts_to_return = []
        for account in accounts:
            item_status = account.get('item_status')
            if item_status and item_status.get('account_status') != 'completed':
                continue
            accounts_to_return.append(account)
        accounts = accounts_to_return
    return accounts

def get_complete_identity_for_statement(statement_id):
    statement_rows = bank_connect_identity_table.query(KeyConditionExpression=Key('statement_id').eq(statement_id) )

    statement_items = statement_rows.get('Items', list())
    if len(statement_items) == 0:
        return dict()

    return statement_items[0].get('item_data', {})

def get_enrichment_for_entity(entity_id):
    qp = {
        'KeyConditionExpression': Key('entity_id').eq(
        entity_id)
    }

    entity_enrichment = collect_results(bank_connect_enrichments_table.query, qp)
    if len(entity_enrichment)==0:
        return dict()
    
    return entity_enrichment[0]


def update_enrichments_table_multiple_keys(entity_id, field_data) -> None:
    if not len(field_data):
        print("update field data is empty")
        return
    
    update_expression = "SET " + ", ".join(f"{field[0]} = :i{i}" for i, field in enumerate(field_data)) + ", updated_at = :u"
    expression_attribute_values = {f":i{i}": field[1] for i, field in enumerate(field_data)}
    expression_attribute_values[":u"] = time.time_ns()

    bank_connect_enrichments_table.update_item(
        Key={"entity_id": entity_id},
        UpdateExpression=update_expression,
        ExpressionAttributeValues=expression_attribute_values,
    )

def get_identity_for_statement(statement_id):
    statement_item = get_complete_identity_for_statement(statement_id)

    identity = statement_item.get('identity')
    if isinstance(identity, dict):
        identity["metadata_analysis"] = statement_item.get('metadata_analysis', None)

    identity = dict() if identity is None else identity
    # return statement_items[0].get('item_data', dict()).get('identity')
    return identity

def get_transaction_progress_for_entity(entity_id):
    accounts = get_accounts_for_entity(entity_id)

    failed_list = list()
    complete_list = list()
    processing_list = list()
    for account in accounts:
        account_id = account.get('account_id')
        account_statements = get_statement_ids_for_account_id(entity_id, account_id)

        for statement_id in account_statements:
            progress, _ = get_progress(statement_id, 'transactions_status')
            if progress == 'completed':
                complete_list.append(statement_id)
            elif progress == 'processing':
                processing_list.append(statement_id)
            else:
                failed_list.append(statement_id)

    return complete_list, processing_list, failed_list

def check_for_date_present(transactions, cur_transaction):
    low = 0
    high = len(transactions) - 1

    ans = -1
    while low<=high:
        mid = int( (low+high)/2 )
        if transactions[mid]['date'] >= cur_transaction['date']:
            ans = mid
            high = mid - 1
        else:
            low = mid + 1

    if ans == -1:
        return False

    #TODO consider charges in case of neft, etc.
    while ans < len(transactions) and transactions[ans]['date'] == cur_transaction['date']:
        if ( (cur_transaction['transaction_type']=='credit' and transactions[ans]['transaction_type'] == 'debit') or
            (cur_transaction['transaction_type']=='debit' and transactions[ans]['transaction_type'] == 'credit')):
            if cur_transaction['amount'] == transactions[ans]['amount']:
                return True
        ans += 1
    return False

def get_all_account_details(accounts):
    account_details = list()
    for account in accounts:
        statements = account.get('item_data').get('statements')
        identity = get_identity_for_statement(statements[0])
        account_details.append(identity)
    return account_details

def get_transactions_for_entity(entity_id, is_sme=False, to_reject_account=False, show_rejected_transactions=False):

    accounts = get_accounts_for_entity(entity_id, to_reject_account)
    list_of_transaction_lists = []    
    account_wise_transactions = dict()
    country = 'IN'

    for account in accounts:
        account_id = account.get('account_id')
        transactions, hash_dict = get_transactions_for_account(entity_id, account_id, show_rejected_transactions=show_rejected_transactions)
        list_of_transaction_lists.append(transactions)
        account_wise_transactions[account_id] = transactions
    
    if is_sme == True:
        start_time = time.time()
        account_details = get_all_account_details(accounts)
        list_of_transaction_lists = list()
        for account in accounts:
            cur_account_id = account.get('account_id')
            statement_ids = account.get('statements')
            if statement_ids:
                country = get_country_for_statement(statement_ids[0])
            for transaction in account_wise_transactions[cur_account_id]:
                name_or_account_present = False
                for account_detail in account_details:
                    name = account_detail.get('name', None)
                    account_number = account_detail.get('account_number', None)
                    if name == None or account_number == None:
                        continue
                    description = get_self_transfer_description(transaction['transaction_note'], transaction['unclean_merchant'], name, country)
                    if description == 'self_transfer' or account_number[-4:] in transaction['transaction_note']:
                        name_or_account_present = True
                        break
                if name_or_account_present:
                    for tmp_account in accounts:
                        tmp_account_id = tmp_account.get('account_id')
                        if cur_account_id != tmp_account_id:
                            if check_for_date_present(account_wise_transactions[tmp_account_id], transaction):
                                transaction['description'] = 'self_transfer'
            list_of_transaction_lists.append(account_wise_transactions[cur_account_id])
        print("self transfer calculation took {} for entity_id {}".format(time.time() - start_time, entity_id))

    return combine_transaction_lists(list_of_transaction_lists)

# this function updates fraud status for both metadata_fraud and page_identity_fraud together, which save 1 ddb write
def update_progress_fraud_status(statement_id, progress_status):
    bank_connect_statement_table.update_item(
        Key={'statement_id': statement_id},
        UpdateExpression="set metadata_fraud_status = :m, page_identity_fraud_status = :p",
        ExpressionAttributeValues={
            ':m': progress_status,
            ':p': progress_status 
        }
    )

def update_progress(statement_id, status_type, status, messages=None, to_reject_statement=False):
    time_ms = time.time_ns()
    # Base update expression and values
    update_expr = "set {} = :s, updated_at = :u".format(status_type)
    expression_attr_values = {
        ':s': status,
        ':u': time_ms
    }

    # Conditionally add message update if messages is not None
    if messages is not None:
        if messages=='':
            messages = None
        update_expr += ", message = :m"
        expression_attr_values[':m'] = messages
    if to_reject_statement:
        update_expr += ", to_reject_statement = :r"
        expression_attr_values[':r'] = to_reject_statement

    # Perform the update
    bank_connect_statement_table.update_item(
        Key={
            'statement_id': statement_id
        },
        UpdateExpression=update_expr,
        ExpressionAttributeValues=expression_attr_values
    )

def remove_transaction_on_page(statement_id, page_number, transaction_hash, transactions):
    
    page_transactions = [transaction_item for transaction_item in transactions
                               if transaction_item.get('page_number') == page_number 
                               and transaction_item.get('hash') != transaction_hash]

    update_transactions_on_page(statement_id, page_number, page_transactions)

    return None

def update_transactions_on_page(statement_id, page_number, transactions):
    bank_connect_transactions_table.update_item(
        Key = {
            'statement_id' : statement_id,
            'page_number' : page_number
        },
        UpdateExpression="set item_data = :t, transaction_count = :tc, updated_at = :u",
        ExpressionAttributeValues={
            ':t': json.dumps(transactions, default=str),
            ':tc': len(transactions),
            ':u': time.time_ns()
        }
    )
    return None

def update_transactions_in_ddb(statement_id, pages_updated, transactions):
    tmp_transactions = deepcopy(transactions)
    page_transaction_dict = {}
    for i in range(len(tmp_transactions)):
        tmp_transactions[i]['optimizations'].extend( tmp_transactions[i].pop('optimizations_old', []) )
        page_num = tmp_transactions[i].get('page_number')
        if page_num not in page_transaction_dict.keys():
            page_transaction_dict[page_num] = []
        page_transaction_dict[page_num].append(tmp_transactions[i])
    
    for page in pages_updated:
        if page in page_transaction_dict.keys():
            page_transactions = page_transaction_dict[page]
            update_transactions_on_page(statement_id, page, page_transactions)
        else:
            update_transactions_on_page(statement_id, page, [])

    return None


def process_and_optimize_transactions_aa(
    statement_id,
    bank_name,
    statement_attempt_type,
    country,
    identity,
    local_logging_context: LoggingContext = None
) -> None:
    if not isinstance(local_logging_context, LoggingContext):
        local_logging_context: LoggingContext = LoggingContext(
            statement_id=statement_id
        )
    local_logging_context.upsert(
        source="process_and_optimise_transactions_aa",
        statement_id=statement_id
    )
    LAMBDA_LOGGER.debug(
        "Fetch AA transactions for statements",
        extra=local_logging_context.store
    )
    transaction_list, _ = get_transactions_for_statement(
        statement_id,
        keep_same_order=True,
        send_hash_page_number_map=False
    )
    if not transaction_list:
        LAMBDA_LOGGER.debug(
            "AA transactions for statement is empty",
            extra=local_logging_context.store
        )
        return
    
    name = identity.get('identity', {}).get('name', '')
    account_category = identity.get('identity', {}).get('account_category', '')
    for i in range(len(transaction_list)):
        if 'optimizations' not in transaction_list[i].keys():
            transaction_list[i]['optimizations'] = []

    LAMBDA_LOGGER.debug(
        "Calling transaction_balance_check to fetch inconsistent_transaction_hash ",
        extra=local_logging_context.store
    )
    inconsistent_transaction_hash = transaction_balance_check(
        transaction_list,
        bank_name,
        statement_attempt_type
    )
    if not inconsistent_transaction_hash:
        LAMBDA_LOGGER.debug(
            "And the inconsistent_transaction_hash is invalid",
            extra=local_logging_context.store
        )
        return

    #storing optimizations made while extraction into a new key
    for statement_transaction in transaction_list:
        statement_transaction['optimizations_old']=statement_transaction.get('optimizations', [])
        statement_transaction['optimizations']=[]

    LAMBDA_LOGGER.debug(
        "Running optimisations and inconsistency removal function on the transactions",
        extra=local_logging_context.store
    )
    transaction_list, _, _, _ = optimise_transaction_type(
        transaction_list,
        bank_name,
        statement_attempt_type
    )
    transaction_list = remove_finvu_aa_inconsistency(transaction_list, bank_name)
    if inconsistent_transaction_hash and bank_name in ['canara']:
        transaction_list, _ = optimise_refund_transactions(transaction_list, bank_name)
    new_inconsistent_transaction_hash = transaction_balance_check(
        transaction_list,
        bank_name,
        statement_attempt_type
    )

    if inconsistent_transaction_hash == new_inconsistent_transaction_hash:
        local_logging_context.upsert(inconsistent_transaction_hash=inconsistent_transaction_hash)
        LAMBDA_LOGGER.debug(
            "Old and New inconsistent_transaction_hash is same, exiting the function",
            extra=local_logging_context.store
        )
        return

    LAMBDA_LOGGER.debug(
        "Calling get_transaction_channel_description_hash to fetch transaction_list",
        extra=local_logging_context.store
    )
    transaction_list = get_transaction_channel_description_hash(
        transaction_list,
        bank_name,
        name,
        country,
        account_category
    )

    #merging optimisations of both extraction and processing 
    for statement_transaction in transaction_list:
        statement_transaction['optimizations'].extend(statement_transaction.pop('optimizations_old', []))

    txns_list_of_lists = get_transactions_list_of_lists_finvu_aa(transaction_list)
    number_of_pages = len(txns_list_of_lists)
    local_logging_context.upsert(number_of_pages=number_of_pages)
    LAMBDA_LOGGER.debug(
        "Updating transactions on each page",
        extra=local_logging_context.store
    )
    for page_num in range(number_of_pages):
        update_transactions_on_page(statement_id, page_num, txns_list_of_lists[page_num])
    
    LAMBDA_LOGGER.debug(
        "Successfully processed and optimized AA transactions",
        extra=local_logging_context.store
    )
    return

def process_multi_account_transactions(statement_id, statement_transactions, bank_name, identity, entity_id, org_metadata = {}, local_logging_context: LoggingContext = None):

    main_account_number = identity.get('identity', {}).get('account_number')
    account_wise_transactions = get_account_wise_transactions_dict(statement_transactions, main_account_number, statement_level_call=True)
    child_statement_account_map = {}

    LAMBDA_LOGGER.info(f"Extracted accounts for statement_id {statement_id} are {account_wise_transactions.keys()} and identity_account_number {main_account_number}", extra=local_logging_context.store)
    
    if len(account_wise_transactions) <= 1:
        return statement_transactions
    else:
        accounts_list = [account for account in account_wise_transactions.keys() if account and account != main_account_number]
        child_statement_account_map, re_extraction = create_multi_account_statements(statement_id, bank_name, accounts_list, local_logging_context=local_logging_context)
        if len(child_statement_account_map) != len(accounts_list):
            capture_message(f"Error while creating child statements for statement_id{statement_id}")
            return statement_transactions
    
    LAMBDA_LOGGER.info(f"Child statement map created for statement_id {statement_id} are {child_statement_account_map}", extra=local_logging_context.store)

    page_count = int(get_field_for_statement(statement_id, 'page_count'))
    pages_updated = set(range(0, page_count))
    for account_number in account_wise_transactions.keys():
        if account_number != main_account_number:
            child_identity = deepcopy(identity)
            child_statement_id = child_statement_account_map[account_number]
            child_account = get_account(entity_id, account_number)
            child_account_category = account_wise_transactions[account_number][0].get('account_category') if len(account_wise_transactions[account_number]) > 0 else ""
            child_account_category = get_child_account_category(child_account_category)
            
            child_identity['account_category'] = child_account_category
            child_identity['identity']['account_number'] = account_number
            
            account_id = create_or_update_account_details_for_pdf(entity_id, child_statement_id, bank_name, child_account, identity_with_extra_params=child_identity, identity_lambda_input={'re_extraction': re_extraction})
            child_identity['identity']['account_id'] = account_id
            time_stamp_in_mlilliseconds = time.time_ns()
            child_identity_object = {
                'statement_id': child_statement_id,
                'item_data': child_identity,
                'created_at': time_stamp_in_mlilliseconds,
                'updated_at': time_stamp_in_mlilliseconds
            }
            
            bank_connect_identity_table.put_item(Item=child_identity_object)
            bank_connect_statement_table.update_item(Key={'statement_id': child_statement_id},
                UpdateExpression="set created_at = :m, identity_status = :i, updated_at = :u, transaction_status = :t, is_multi_account_statement = :v",
                ExpressionAttributeValues={
                    ':m': time_stamp_in_mlilliseconds, 
                    ':i': "completed",
                    ':t': "completed",
                    ':u': time_stamp_in_mlilliseconds,
                    ':v': True
                })
            update_child_statement_identity(child_statement_id, child_identity)
            update_transactions_in_ddb(child_statement_id, pages_updated, account_wise_transactions[account_number])
            update_page_cnt_and_page_done(child_statement_id, page_count, page_count)
            send_event_to_update_state_queue(entity_id, child_statement_id, org_metadata=org_metadata)
        else:
            update_transactions_in_ddb(statement_id, pages_updated, account_wise_transactions[account_number])
            bank_connect_statement_table.update_item(Key={'statement_id': statement_id},
                UpdateExpression="set updated_at = :u, is_multi_account_statement = :v",
                ExpressionAttributeValues={
                    ':u': time.time_ns(),
                    ':v': True
                })
            
    return account_wise_transactions[main_account_number]

def get_child_account_category(account_category_string):
    if not account_category_string:
        return ''
    if 'savings' in account_category_string.lower():
        return 'SAVINGS'
    if 'current' in account_category_string.lower():
        return 'CURRENT'
    if 'ppf' in account_category_string.lower():
        return 'INDIVIDUAL'
    return ''

def get_field_for_statement(statement_id, field):
    
    qp = {
        'KeyConditionExpression': Key('statement_id').eq(statement_id),
        'ConsistentRead': True, 
        'ProjectionExpression': field
    }
    
    statement_items = collect_results(bank_connect_statement_table.query, qp)
    
    if len(statement_items)>0:
        return statement_items[0].get(field, None)
    
    return None

def create_multi_account_statements(statement_id, bank_name, accounts_list, local_logging_context: LoggingContext = None):
    child_statement_account_map = get_child_statement_account_map(statement_id)
    if child_statement_account_map:
        return child_statement_account_map, True
    
    LAMBDA_LOGGER.info(f"calling dashboard API for creating child statement for multi_account_pdf: {statement_id}", extra=local_logging_context.store)
    
    url = '{}/bank-connect/v1/internal/create_multi_account_statements/'.format(
        DJANGO_BASE_URL)

    headers = {
        'x-api-key': API_KEY,
        'Content-Type': "application/json",
    }
    payload = {
       "parent_statement_id": statement_id,
       "bank_name": bank_name,
       "accounts_list": accounts_list
    }
    child_statement_account_map = {}
    
    payload = json.dumps(payload, default=str)
    response = call_api_with_session(url,"POST", payload, headers)
    response_json = response.json()
    child_statement_account_map = response_json.get('child_statement_account_map')
    
    bank_connect_statement_table.update_item(Key={'statement_id': statement_id},
        UpdateExpression="set updated_at = :u, child_statement_account_map = :v",
        ExpressionAttributeValues={
            ':u': time.time_ns(),
            ':v': child_statement_account_map
        })
    return child_statement_account_map, False

def get_child_statement_account_map(statement_id):
    statement_data = bank_connect_statement_table.query(KeyConditionExpression=Key('statement_id').eq(statement_id))
    if not statement_data:
        return None
    all_items = statement_data.get('Items')
    if len(all_items) == 0:
        return None
    
    child_statement_account_map = all_items[0].get('child_statement_account_map')
    return child_statement_account_map

def update_child_statement_identity(statement_id, identity_dict):
    print('calling dashboard API for updating child statement identity in case of multi account pdf: {}'.format(statement_id))
    
    url = '{}/bank-connect/v1/internal/update_child_statement_identity/'.format(
        DJANGO_BASE_URL)

    headers = {
        'x-api-key': API_KEY,
        'Content-Type': "application/json",
    }
    payload = {
       "statement_id": statement_id,
       "identity": identity_dict
    }
    
    payload = json.dumps(payload, default=str)

    #calling django
    call_api_with_session(url, "PUT",payload, headers)

def process_and_optimize_transactions(
    statement_id,
    bank_name,
    statement_attempt_type,
    country,
    identity,
    entity_id,
    local_logging_context: LoggingContext = None,
    org_metadata: dict = {},
    warehousing_meta_data={}
) -> None:
    if not isinstance(local_logging_context, LoggingContext):
        local_logging_context: LoggingContext = LoggingContext(
            statement_id=statement_id
        )
    local_logging_context.upsert(
        source="process_and_optimize_transactions",
        bank=bank_name
    )
    LAMBDA_LOGGER.debug(
        "Fetch transactions for statements",
        extra=local_logging_context.store
    )

    if bank_name in ['phonepe_bnk']:
        LAMBDA_LOGGER.info(f"Un-optimizable bank detected, exiting process_and_optimize_transactions function for statement_id {statement_id}", extra=local_logging_context.store)
        return None

    session_date_range = warehousing_meta_data.get('session_date_range', {})
    
    statement_transactions, hash_page_number_map = get_transactions_for_statement(
        statement_id,
        keep_same_order=True,
        send_hash_page_number_map=False,
        fetch_all_transactions = True
    )

    start_time = time.time()
    
    statement_transactions = process_multi_account_transactions(statement_id, statement_transactions, bank_name, identity, entity_id, org_metadata=org_metadata, local_logging_context=local_logging_context)
    
    LAMBDA_LOGGER.info(f"Time taken to process multi account transactions for statement_id {statement_id} is {time.time() - start_time}", extra=local_logging_context.store)

    name = identity.get('identity', {}).get('name', '')
    account_category = identity.get('identity', {}).get('account_category', '')
    opening_balance = identity.get('opening_bal', None)
    closing_balance = identity.get('closing_bal', None)
    
    try:
        opening_balance = float(opening_balance)
    except Exception as _:
        opening_balance = None
    
    try:
        closing_balance = float(closing_balance)
    except Exception as _:
        closing_balance = None

    #storing optimizations made while extraction into a new key
    for statement_transaction in statement_transactions:
        statement_transaction['optimizations_old']=statement_transaction.get('optimizations', [])
        statement_transaction['optimizations']=[]

    if bank_name == 'hsbc':
        statement_transactions, pages_updated = process_hsbc_ocr_transactions(statement_transactions)
        statement_transactions = get_transaction_channel_description_hash(
            statement_transactions,
            bank_name,
            name,
            country,
            account_category
        )
        statement_transactions = update_transactions_on_session_date_range(session_date_range, statement_transactions, statement_id)
        update_transactions_in_ddb(statement_id, pages_updated, statement_transactions)
    
    if bank_name in ["baroda", "kanaka_mahalakshmi_co_op", "permata", "dbsbnk", "citi", "bnibnk", "akhand_anand", "akola", "eenadu_urban", "punjab_sind", "karad_urban", "gscb", "bnk_of_georgia", "harij_nagarik_sahakari", "vysya_co_op", "padmavathi_co_op_bnk", "mangal_co_op_bnk", "primebnk"]:
        all_transactions, pages_updated = front_fill_balance([statement_transactions], opening_balance, closing_balance, -1, bank_name)
        statement_transactions = all_transactions[0]
        if len(pages_updated) > 0:
            # Updating hash for transactions
            statement_transactions = add_hash_to_transactions_df(pd.DataFrame(statement_transactions)).to_dict('records')

            statement_transactions = update_transactions_on_session_date_range(session_date_range, statement_transactions, statement_id)
            update_transactions_in_ddb(statement_id, pages_updated, statement_transactions)
    
    if bank_name in ['hdfc', 'mizoram']:
        LAMBDA_LOGGER.info("Going inside the transaction for the hdfc bank in process and optimise", extra=local_logging_context.store)
        
        statement_transactions, pages_updated = merge_partial_transaction_notes(statement_transactions, bank_name, name, country, account_category)
        
        statement_transactions = update_transactions_on_session_date_range(session_date_range, statement_transactions, statement_id)
        update_transactions_in_ddb(statement_id, pages_updated, statement_transactions)
    
    if not statement_transactions:
        LAMBDA_LOGGER.debug("Statement transactions are empty, exiting the function", extra=local_logging_context.store)
        return

    LAMBDA_LOGGER.debug(
        "Optimizing transactions type to fetch transaction_hash",
        extra=local_logging_context.store
    )
    statement_transactions, pages_updated, num_optimizations, _ = optimise_transaction_type(
        statement_transactions,
        bank_name,
        statement_attempt_type,
        send_optimized=True,
        update_flag=True
    )
    if len(pages_updated) > 0:
        start_time = time.time()
        
        statement_transactions = get_transactions_with_updated_categorization(statement_transactions, bank=bank_name, name=name, country=country, account_category=account_category)
        
        LAMBDA_LOGGER.info(f"Time taken to get transaction_channel_description hash for statement_id {statement_id} is {time.time() - start_time}", extra=local_logging_context.store)
        
        statement_transactions = update_transactions_on_session_date_range(session_date_range, statement_transactions, statement_id)
        update_transactions_in_ddb(statement_id, pages_updated, statement_transactions)

    transaction_hash = transaction_balance_check(statement_transactions, bank_name, statement_attempt_type)
    if transaction_hash and transaction_hash == statement_transactions[-1].get('hash'):
        page_number = statement_transactions[-1].get('page_number')
        remove_transaction_on_page(statement_id, page_number, transaction_hash, statement_transactions)
        transaction_hash = None

    if (transaction_hash
            and len(statement_transactions) > 1 and transaction_hash == statement_transactions[-2].get('hash')):
        test_transaction_hash = transaction_balance_check(
            statement_transactions[:-1],
            bank_name,
            statement_attempt_type
        )
        if test_transaction_hash is None:
            transaction_hash = statement_transactions[-1].get('hash')
            page_number = statement_transactions[-1].get('page_number')
            remove_transaction_on_page(statement_id, page_number, transaction_hash, statement_transactions)
            transaction_hash = None
        
    if bank_name in ['central', 'sbi']:
        transactions_without_duplicates, pages_updated = remove_duplicate_transactions(
            statement_transactions,
            bank_name
        )
        transaction_hash = transaction_balance_check(
            transactions_without_duplicates,
            bank_name,
            statement_attempt_type
        )
        if not transaction_hash:
            transactions_without_duplicates = update_transactions_on_session_date_range(session_date_range, transactions_without_duplicates, statement_id)
            update_transactions_in_ddb(statement_id, pages_updated, transactions_without_duplicates)

    if transaction_hash and bank_name in ['yesbnk'] and statement_attempt_type == 'pdf':
        transactions_without_splits_swaps, pages_updated = fix_yesbnk_inc_transactions(
            statement_transactions,
            bank_name,
            True
        )
        transaction_hash = transaction_balance_check(
            transactions_without_splits_swaps,
            bank_name,
            statement_attempt_type
        )
        if not transaction_hash:
            transactions_without_splits_swaps = update_transactions_on_session_date_range(session_date_range, transactions_without_splits_swaps, statement_id)
            update_transactions_in_ddb(statement_id, pages_updated, transactions_without_splits_swaps)

    if transaction_hash and bank_name in ['gp_parsik'] and statement_attempt_type == 'pdf':
        transactions_without_swaps, pages_updated = swap_inconsistent_trxns(
            statement_transactions,
            bank_name,
            transaction_hash,
            'pdf'
        )
        transaction_hash = transaction_balance_check(
            transactions_without_swaps,
            bank_name,
            statement_attempt_type
        )
        if not transaction_hash:
            transactions_without_swaps = update_transactions_on_session_date_range(session_date_range, transactions_without_swaps, statement_id)
            update_transactions_in_ddb(statement_id, pages_updated, transactions_without_swaps)

    if transaction_hash or bank_name in ['ubi']:
        statement_transactions, is_inconsistent, inconsistent_data, pages_updated = process_merged_pdf_transactions(statement_transactions, LAMBDA_LOGGER, local_logging_context)
        if not is_inconsistent:
            LAMBDA_LOGGER.info(f"Merged pdf data successfully solved for statement_id {statement_id}", extra=local_logging_context.store)
            transaction_hash = None
            if pages_updated or is_inconsistent is False:
                statement_transactions = update_transactions_on_session_date_range(session_date_range, statement_transactions, statement_id)
                # In case of merged statements containing multiple date ranges in unsorted order, the
                # DDB transactions table keeps them unsorted, causing inconsistencies due to date and balance mismatch

                # Fetch the total number of pages in the statement
                no_of_pages = get_page_count_for_statement(statement_id)

                # Distribute the sorted transactions into pages
                statement_pages_list = [[]] * no_of_pages
                statement_pages_list[0] = statement_transactions
                transactions_chunks_list = check_and_distribute_transactions_in_pages(statement_pages_list)

                # Update the sorted transactions into DDB
                for page_num, transaction_chunk in enumerate(transactions_chunks_list):
                    update_transactions_on_page(statement_id, page_num, transaction_chunk)

        update_inconsistency_data_for_statement(statement_id, inconsistent_data)

    if not transaction_hash:
        statement_transactions, pages_updated = correct_transactions_date(statement_transactions, bank_name)
        if pages_updated:
            statement_transactions = update_transactions_on_session_date_range(session_date_range, statement_transactions, statement_id)
            update_transactions_in_ddb(statement_id, pages_updated, statement_transactions)

    LAMBDA_LOGGER.debug(
        "Successfully processed and optimized transactions",
        extra=local_logging_context.store
    )
    
    return transaction_hash


def update_account_disparity(
    entity_id,
    account_id,
    event_statement_id,
    account_transactions,
    salary_transactions,
    account_category,
    bank_name,
    country,
    statement_attempt_type=None,
    warehouse_data={},
    fan_out_info_dashboard_resp={},
    local_logging_context: LoggingContext = None
) -> None:
    """
    Updates Inconsistent Disparity and Account Level Frauds in DDB
    :param: request: takes entity_id, account_id, statement_id, account_transactions, 
                            salary_transactions, account_category, bank_name, country
    """
    if not isinstance(local_logging_context, LoggingContext):
        local_logging_context: LoggingContext = LoggingContext(
            entity_id=entity_id,
            account_id=account_id,
            statement_id=event_statement_id,
        )
    local_logging_context.upsert(source="update_account_disparity", bank=bank_name)
    LAMBDA_LOGGER.info(
        f"Attempting to update the account disparity for account {account_id}",
        extra=local_logging_context.store
    )
    LAMBDA_LOGGER.debug(
        "Extract transactions from each statement and process them",
        extra=local_logging_context.store
    )

    disparities = []
    disparity_data = {}
    inconsistent_transactions = []
    statement_ids = get_statement_ids_for_account_id(entity_id, account_id)
    for statement_id in statement_ids:
        # Only update statement disparity when extraction of that particular statement is completed
        is_complete, _, _ = check_extraction_progress(statement_id)
        if not is_complete:
            continue
        statement_transactions, hash_page_number_map = get_transactions_for_statement(
            statement_id,
            keep_same_order=True,
            send_hash_page_number_map=False
        )
        if not statement_transactions:
            continue
        transaction_hash = transaction_balance_check(
            statement_transactions,
            bank_name,
            statement_attempt_type
        )
        if transaction_hash is not None:
            new_transaction_hash = transaction_balance_check(
                statement_transactions,
                bank_name,
                statement_attempt_type,
                update_month_order=True
            )
            if not new_transaction_hash:
                transaction_hash = None
        if transaction_hash is not None:
            prev_date, curr_date, inconsistent_transactions = get_inconsistency_date_range(statement_transactions, transaction_hash)
            disparity_data = {
                "fraud_type": "inconsistent_transaction",
                "transaction_hash": transaction_hash,
                "prev_date": prev_date,
                "curr_date": curr_date,
                "statement_id": statement_id,
                "account_id": account_id,
                "inconsistent_transaction": inconsistent_transactions,
                "bank_name": bank_name
            }
            disparities.append(disparity_data)

        # Saving inconsistency in DDB for every statement since now calling advanced features once for every account
        if transaction_hash:
            update_statement_inconsistency_ddb(statement_id, True, transaction_hash)
            # Invoke inconsistency solving function solved through kafka. Below code produces an event into Kafka
            if statement_attempt_type in ['pdf', 'online']:
                is_successful = send_data_to_kafka(topic_name=KAFKA_TOPIC_INCONSISTENCY, payload=disparity_data)

                local_logging_context.upsert(topic_name=KAFKA_TOPIC_INCONSISTENCY)
                LAMBDA_LOGGER.info(
                    "Failed to send data to kafka" if not is_successful else "Successfully sent data to kafka",
                    extra=local_logging_context.store
                )
        else:
            update_statement_inconsistency_ddb(statement_id, False, None)

    LAMBDA_LOGGER.debug(
        "Calling transaction_balance_check to fetch transaction_hash",
        extra=local_logging_context.store
    )
    transaction_hash = transaction_balance_check(account_transactions, bank_name)
    if transaction_hash is not None:
        LAMBDA_LOGGER.debug(
            "Generating new transaction hash",
            extra=local_logging_context.store
        )
        new_transaction_hash = transaction_balance_check(account_transactions, bank_name, update_month_order=True)
        if not new_transaction_hash:
            transaction_hash = None

    LAMBDA_LOGGER.debug(
        "Get inconsistency date range and update transaction hash in DDB",
        extra=local_logging_context.store
    )
    if transaction_hash:
        prev_date, curr_date, inconsistent_transactions = get_inconsistency_date_range(account_transactions, transaction_hash)
        disparities.append({
                "fraud_type": "inconsistent_transaction",
                "transaction_hash": transaction_hash,
                "prev_date": prev_date,
                "curr_date": curr_date,
                "account_id": account_id,
                "inconsistent_transaction": inconsistent_transactions,
                "bank_name": bank_name
            })
        update_account_inconsistency_ddb(entity_id, account_id, True, transaction_hash)
    else:
        update_account_inconsistency_ddb(entity_id, account_id, False, None)

    LAMBDA_LOGGER.debug(
        "Checking for account level frauds",
        extra=local_logging_context.store
    )
    # check for account level frauds
    disparities.extend(
        account_level_frauds(
            account_transactions,
            account_category,
            salary_transactions,
            country
        )
    )
    allowed_frauds = fan_out_info_dashboard_resp.get('allowed_frauds', [])
    final_client_disparities = []

    for disparity in disparities:
        if disparity.get('fraud_type', None) in allowed_frauds:
            final_client_disparities.append(disparity)

    item_data = json.dumps(final_client_disparities)

    LAMBDA_LOGGER.debug(
        "Prepare disparities for warehouse and send it to FireHose",
        extra=local_logging_context.store
    )
    try:
        # update in dynamodb
        time_stamp_in_milliseconds = time.time_ns()
        dynamo_object = {
            'account_id': account_id,
            'item_data': item_data,
            'created_at': time_stamp_in_milliseconds,
            'updated_at': time_stamp_in_milliseconds
        }
        bank_connect_disparities_table.put_item(Item=dynamo_object)
    except Exception as e:
        local_logging_context.upsert(
            exception=str(e),
            trace=traceback.format_exc()
        )
        LAMBDA_LOGGER.error(
            "Exception raised while updating DDB",
            extra=local_logging_context.store
        )
        local_logging_context.remove_keys(["exception", "trace"])

        # write in s3 if ddb write fails
        object_key = "disparities/entity_{}/account_{}".format(entity_id, account_id)
        s3_object = s3_resource.Object(BANK_CONNECT_DDB_FAILOVER_BUCKET, object_key)
        s3_object.put(Body=bytes(item_data, encoding='utf-8'))
        # write key in ddb
        time_stamp_in_milliseconds = time.time_ns()
        dynamo_object = {
            'account_id': account_id,
            's3_object_key': object_key,
            'created_at': time_stamp_in_milliseconds,
            'updated_at': time_stamp_in_milliseconds
        }
        bank_connect_disparities_table.put_item(Item=dynamo_object)

    for disparity in disparities:
        disparity.pop("inconsistent_transaction", None)
    warehouse_disparities = prepare_disparities_warehouse_data(warehouse_data, disparities, allowed_frauds)
    # send_data_to_firehose(warehouse_disparities, DISPARITIES_STREAM_NAME)
    send_large_list_payload_to_kafka(warehouse_disparities, DISPARITIES_STREAM_NAME)
    LAMBDA_LOGGER.debug(
        "Successfully updated account disparity",
        extra=local_logging_context.store
    )
    return


def add_salary_confidence_percentage(entity_id,account_id,salary_confidence_percentage):
    bank_connect_account_table.update_item(
        Key={
            'entity_id': entity_id,
            'account_id': account_id
        },
        UpdateExpression="SET item_data.salary_confidence = :i, updated_at = :u",
        ExpressionAttributeValues={
            ':i': salary_confidence_percentage,
            ':u': time.time_ns()
        }
    )

def update_date_range(statement_id, min_txn_date, max_txn_date):
    bank_connect_identity_table.update_item(
        Key={'statement_id' : statement_id},
        UpdateExpression="set item_data.date_range.from_date = :s, item_data.date_range.to_date = :e, updated_at = :u",
        ExpressionAttributeValues={':s': min_txn_date[:10], ':e': max_txn_date[:10], ':u': time.time_ns()}
    )

def update_statement_inconsistency_ddb(statement_id, is_inconsistent, transaction_hash):
    bank_connect_identity_table.update_item(
        Key={
            'statement_id' : statement_id
        },
        UpdateExpression="SET item_data.is_inconsistent = :b, item_data.inconsistent_hash = :s, updated_at = :u",
        ExpressionAttributeValues={
            ':b': is_inconsistent, 
            ':s': transaction_hash, 
            ':u': time.time_ns()
        }
    )

def update_account_inconsistency_ddb(entity_id, account_id, is_inconsistent, transaction_hash):
    bank_connect_account_table.update_item(
        Key={
            'entity_id': entity_id,
            'account_id': account_id
        },
        UpdateExpression="SET item_data.is_inconsistent = :b, item_data.inconsistent_hash = :s, updated_at = :u",
        ExpressionAttributeValues={
            ':b': is_inconsistent,
            ':s': transaction_hash,
            ':u': time.time_ns()
        }
    )

def update_statement_reject_reason(statement_id: str, reject_reason: str) -> None:
    if not reject_reason:
        return
    bank_connect_identity_table.update_item(
        Key={
            'statement_id' : statement_id
        },
        UpdateExpression="SET item_data.reject_reason = :b, updated_at = :u",
        ExpressionAttributeValues={
            ':b': reject_reason, 
            ':u': time.time_ns()
        }
    )

def update_account_category(entity_id, account_id, statement_id):
    category = "corporate"

    bank_connect_identity_table.update_item(
        Key={'statement_id': statement_id},
        UpdateExpression="set #data.#id.#ac = :ac, updated_at = :u",
        ExpressionAttributeValues={
            ':ac': category, ':u': time.time_ns()},
        ExpressionAttributeNames={
            '#data': 'item_data',
            '#id': 'identity',
            '#ac': 'account_category',
        }
    )
    bank_connect_account_table.update_item(
        Key={
            'entity_id': entity_id,
            'account_id': account_id
        },
        UpdateExpression="set #data.#ac = :ac, updated_at = :u",
        ExpressionAttributeValues={
            ':ac': category, ':u': time.time_ns()},
        ExpressionAttributeNames={
            '#data': 'item_data',
            '#ac': 'account_category',
        }
    )

def map_session_account_status(entity_id, accounts, session_date_range, acceptance_criteria, date_range_approval_criteria, is_missing_date_range_enabled, accept_anything):
    response = {}
    response['session_id'] = entity_id
    response['accounts'] = []

    session_date_range = convert_date_range_to_datetime(session_date_range, "%d/%m/%Y")
    all_account_months = get_account_wise_months(entity_id, None, is_missing_date_range_enabled, session_date_range)

    for account in accounts:
        acc = {}
        account_id = account.get('account_id', '')
        if account_id in [None, '']:
            continue

        account_months = all_account_months.get(account_id, {})
        months_on_txn = account_months.get('months_on_txn', [])
        missing_date_range_on_extraction = account_months.get('missing_date_range_on_extraction', {})
        missing_months_on_extraction = account_months.get('missing_months_on_extraction', [])
        missing_date_range_on_trxn = account_months.get('missing_date_range_on_trxn', [])
        missing_months_on_txn, session_months = is_month_missing(months_on_txn, deepcopy(session_date_range))

        first_and_last_months = []
        if len(session_months) > 0:
            if session_months[0] in missing_months_on_txn:
                first_and_last_months.append(session_months[0])
            if session_months[-1] in missing_months_on_txn:
                first_and_last_months.append(session_months[-1])

        created_at = account.get('created_at', '')
        created_at = str(datetime.datetime.utcfromtimestamp(int(created_at/1000000000)))
        updated_at = account.get('updated_at', '')
        updated_at = str(datetime.datetime.utcfromtimestamp(int(updated_at/1000000000)))
        
        acc_data = account.get('item_data', {})
        
        account_number = acc_data.get('account_number', None)
        statements = acc_data.get('statements', [])
        bank_name = acc_data.get('bank', '')
        acc['account_id'] = account_id
        acc['account_number'] = account_number
        acc['bank_name'] = bank_name
        acc['error_code'] = None
        acc['error_message'] = None
        acc['account_status'] = 'completed'
        acc['created_at'] = created_at
        acc['last_updated_at'] = updated_at
        acc['statements'] = statements

        # If in disparity table, hash is repeating itself it means this is statement inconsistency
        # Account_level inconsistency is stored with statement_id = None
        disparities = get_disparities_from_ddb(account_id)
        is_inconsistent = False
        does_only_account_level_inconsistency_exist = None
        inconsistent_transaction_hash = None
        for disparity in disparities:
            if disparity['fraud_type'] == 'inconsistent_transaction':
                is_inconsistent = True
                if does_only_account_level_inconsistency_exist is None:
                    does_only_account_level_inconsistency_exist = True
                if inconsistent_transaction_hash is None:
                    inconsistent_transaction_hash = disparity.get('transaction_hash')
                elif inconsistent_transaction_hash == disparity.get('transaction_hash'):
                    does_only_account_level_inconsistency_exist = False

        if account_number in [None, '']:
            acc['account_status'] = 'failed'
            acc['error_code'] = 'NULL_ACCOUNT_NUMBER'
            acc['error_message'] = 'Account number is unavailable or unidentified'
        elif not accept_anything and len(missing_months_on_extraction) > 0 and len(missing_months_on_txn) > 0 and 'missing_upload_months' in acceptance_criteria:
            #for incomplete upload from end-user side
            #len(missing_months_on_txn) > 0 is applied in case missing_months_on_extraction is not empty because we couldn't detect date range, but txn were present
            tmp_month_list = ', '.join(change_date_format(missing_months_on_extraction, "%Y-%m", "%b %Y"))
            acc['account_status'] = 'failed'
            acc['error_code'] = 'INCOMPLETE_MONTHS_UPLOAD'
            acc['error_message'] = f'Statement(s) uploaded contain incomplete months. Missing data present for {tmp_month_list}'
        elif not accept_anything and is_missing_date_range_enabled and is_missing_dates(missing_date_range_on_extraction, date_range_approval_criteria) and 'missing_upload_date_range' in acceptance_criteria:
            acc['account_status'] = 'failed'
            acc['error_code'] = 'INCOMPLETE_DATES_UPLOAD'
            acc['error_message'] = f'Statement(s) uploaded contain incomplete dates. Missing dates present for {json.dumps(missing_date_range_on_extraction)}'
        elif (
            does_only_account_level_inconsistency_exist
            and not accept_anything
            and is_missing_date_range_enabled
            and is_missing_dates(missing_date_range_on_trxn, date_range_approval_criteria)
            and "missing_upload_date_range" in acceptance_criteria
        ):
            acc["account_status"] = "failed"
            acc["error_code"] = "INCOMPLETE_DATES_UPLOAD"
            acc["error_message"] = (
                f"Statement(s) uploaded contain incomplete dates. Missing dates present for {json.dumps(missing_date_range_on_trxn)}"
            )
        elif not accept_anything and missing_months_on_txn not in [None, []] and 'atleast_one_transaction_permonth' in acceptance_criteria:
            tmp_month_list = ', '.join(change_date_format(missing_months_on_txn, "%Y-%m", "%b %Y"))
            acc['account_status'] = 'failed'
            acc['error_code'] = 'INCOMPLETE_MONTHS'
            acc['error_message'] = f'Insufficient data to generate report. There are no transactions for {tmp_month_list}'
        elif len(months_on_txn) == 0 and 'atleast_one_transaction' in acceptance_criteria:
            acc['account_status'] = 'failed'
            acc['error_code'] = 'NO_TRANSACTIONS'
            acc['error_message'] = 'No bank transactions in the expected date range'
        elif not accept_anything and 'atleast_one_transaction_in_start_and_end_months' in acceptance_criteria and len(first_and_last_months) > 0:
            tmp_month_list = ', '.join(change_date_format(first_and_last_months, "%Y-%m", "%b %Y"))
            acc['account_status'] = 'failed'
            acc['error_code'] = 'INCOMPLETE_MONTHS'
            acc['error_message'] = f'Insufficient data to generate report. There are no transactions for {tmp_month_list}'
        elif is_inconsistent:
            acc['account_status'] = 'failed'
            acc['error_code'] = 'UNPARSABLE'
            acc['error_message'] = 'Failed to process because of an unparsable statement'
        response['accounts'].append(acc)
        bank_connect_account_table.update_item(
            Key={
                'entity_id': entity_id,
                'account_id': account_id
            },
            UpdateExpression="SET item_status = :i, updated_at = :u",
            ExpressionAttributeValues={
                ':i': {
                    'error_code': acc['error_code'],
                    'error_message': acc['error_message'],
                    'account_status': acc['account_status']
                },
                ':u': time.time_ns()
            }
        )
    return response

def put_advance_features_in_ddb(account_data, account_id, entity_id):
    local_logging_context: LoggingContext = LoggingContext(
        source="put_advance_features_in_ddb", account_id=account_id, entity_id=entity_id
    )
    LAMBDA_LOGGER.debug("Adding advanced features to DDB", extra=local_logging_context.store)
    for data_key in account_data.keys():
        if data_key == 'recurring_transactions':
            ddb_table = bank_connect_recurring_table
        elif data_key == 'salary_transactions':
            ddb_table = bank_connect_salary_table
        else:
            continue
        
        item_data = json.dumps(account_data[data_key], default=str)
        time_stamp_in_mlilliseconds = time.time_ns()
        dynamo_object = {
            'account_id': account_id,
            'item_data': item_data,
            'created_at': time_stamp_in_mlilliseconds,
            'updated_at': time_stamp_in_mlilliseconds
        }
        try:
            ddb_table.put_item(Item=dynamo_object)
        except Exception:
            # write in s3 if ddb write fails
            object_key = "{}/entity_{}/account_{}".format(data_key, entity_id, account_id)
            s3_object = s3_resource.Object(BANK_CONNECT_DDB_FAILOVER_BUCKET, object_key)
            s3_object.put(Body=bytes(item_data, encoding='utf-8'))
            # write key in ddb
            time_stamp_in_mlilliseconds = time.time_ns()
            dynamo_object = {
                'account_id': account_id,
                's3_object_key': object_key,
                'created_at': time_stamp_in_mlilliseconds,
                'updated_at': time_stamp_in_mlilliseconds
            }
            ddb_table.put_item(Item=dynamo_object)

    LAMBDA_LOGGER.info("Advanced features to DDB insertion is completed", extra=local_logging_context.store)

def process_account_category_based_on_transactions(entity_id, account_id, account_transactions, initial_account_category, account_statement_ids):
    if initial_account_category is not None:
        return
    
    account_category_based_on_transactions = get_account_category_from_transactions(account_transactions)
    account_data_to_update: list[tuple[str, Any]] = []
    account_data_to_update.append(('account_category', account_category_based_on_transactions))

    for statement_id in account_statement_ids:
        identity_data_to_update: list[tuple[str, Any]] = []
        identity_data_to_update.append(('account_category', account_category_based_on_transactions))
        update_identity_table_item_data_multiple_keys(statement_id, identity_data_to_update)
    
    update_account_table_multiple_keys(entity_id, account_id, account_data_to_update)

def process_account_category(
        entity_id, 
        account_id, 
        account_transactions, 
        account_statement_ids
    ) -> Union[AccountDict, None]:
    print("Processing account category for entity_id: {} account_id: {}".format(entity_id, account_id))
    account = get_account_for_entity(entity_id, account_id)
    if not account:
        print("account details not found to process account category for entity_id: {} account_id: {}".format(entity_id, account_id))
        return None
    
    initial_account_item_data = account.get('item_data')
    if not initial_account_item_data: 
        print("account item data details not found to configure account od limit for entity_id: {} account_id: {}".format(entity_id, account_id))
        return None
    
    account_data_to_update: list[tuple[str, Any]] = []
    updated_od_limit_details = configure_od_limit_after_transactions_extraction(initial_account_item_data, account_transactions)
    updated_neg_txn_od_value = updated_od_limit_details.get('neg_txn_od', None)
    updated_is_od_account_value = updated_od_limit_details.get('is_od_account', None)
    updated_od_limit = updated_od_limit_details.get('od_limit', None)
    updated_od_paramters_by = updated_od_limit_details.get('updated_od_paramters_by', None)

    initial_account_category = initial_account_item_data.get('account_category')
    account_category_v2 = None
    if initial_account_category is None:
        account_category_v2 = get_account_category_from_transactions(account_transactions)
    else:
        account_category_v2 = initial_account_category

    # Update neg_txn_od flag
    account_data_to_update.append(('neg_txn_od', updated_neg_txn_od_value))
    initial_account_item_data['neg_txn_od'] = updated_neg_txn_od_value

    # Update is_od_account flag
    account_data_to_update.append(('is_od_account', updated_is_od_account_value))
    initial_account_item_data['is_od_account'] = updated_is_od_account_value

    # Update OD Limit
    account_data_to_update.append(('od_limit', updated_od_limit))
    initial_account_item_data['od_limit'] = updated_od_limit

    # Update transactions start_date and end_date
    transaction_date_range = {
        "from_date": None,
        "to_date": None
    }
    if len(account_transactions)>0:
        transaction_date_range['from_date'] = get_date_of_format(account_transactions[0]['date'], "%Y-%m-%d")
        transaction_date_range['to_date'] = get_date_of_format(account_transactions[-1]['date'], "%Y-%m-%d")
    account_data_to_update.append(('transaction_date_range', transaction_date_range))

    # Update DDB
    update_account_table_multiple_keys(entity_id, account_id, account_data_to_update)
    
    # Update for statement identity
    initial_neg_txn_od_value = initial_account_item_data.get("neg_txn_od")
    for statement_id in account_statement_ids:
        identity_data_to_update: list[tuple[str, Any]] = []
        
        identity_item_data = get_complete_identity_for_statement(statement_id)
        identity_od_metadata = identity_item_data.get('identity', {}).get('od_metadata', {})
        identity_od_metadata.update({'initial_neg_txn_od': initial_neg_txn_od_value })
        
        identity_data_to_update.append(('is_od_account', updated_is_od_account_value))
        
        identity_data_to_update.append(('od_limit', updated_od_limit))
        
        if updated_od_paramters_by is not None:
            identity_data_to_update.append(('updated_od_paramters_by', updated_od_paramters_by))

        identity_data_to_update.append(('od_metadata', identity_od_metadata))
        identity_data_to_update.append(('account_category_v2', account_category_v2))
        
        update_identity_table_item_data_multiple_keys(statement_id, identity_data_to_update)

    # Return account dict after mutating 'neg_txn_od', 'is_od_account', 'od_limit' inside 'item_data'
    if 'item_data' in account.keys():
        account['item_data']['is_od_account'] = updated_is_od_account_value
        account['item_data']['od_limit'] = updated_od_limit 
        account['item_data']['neg_txn_od'] = updated_neg_txn_od_value
    
    return account

class ODLimitConfigAfterTransactionsExtraction(TypedDict):
    updated_od_paramters_by: str
    is_od_account: bool
    neg_txn_od: Optional[bool]
    od_limit: Optional[int]

def configure_od_limit_after_transactions_extraction(
        account_item_data: AccountItemData, 
        account_transactions: list[dict]
    ) -> ODLimitConfigAfterTransactionsExtraction:
    
    total_transactions = len(account_transactions)
    negative_transactions, most_negative_balance = calculate_most_negative_balance(account_transactions)

    # Custom logic give by client, to check if more than 50% of transactions balance are negative
    more_than_half_negative_balance_flag = total_transactions > 20 and negative_transactions >= (total_transactions//2)

    # IIFL OD Limit Logic
    is_od_account, od_limit, neg_txn_od, updated_od_paramters_by = iifl_od_limit_logic(most_negative_balance, account_item_data, more_than_half_negative_balance_flag)

    updated_od_limit_details: ODLimitConfigAfterTransactionsExtraction = {
        'is_od_account': is_od_account,
        'updated_od_paramters_by': updated_od_paramters_by,
        'neg_txn_od': neg_txn_od,
        'od_limit': od_limit
    }
    return updated_od_limit_details

def iifl_od_limit_logic(
        most_negative_balance: int, 
        account_item_data: AccountItemData,
        more_than_half_negative_balance_flag: bool
    ) -> Tuple[bool, Union[int, None], Union[bool, None], str]:
    initial_od_limit_input_by_client = account_item_data.get('od_limit_input_by_client')
    initial_account_od_limit = account_item_data.get('od_limit', None)
    initial_neg_txn_od = account_item_data.get('neg_txn_od')

    od_limit = None
    is_od_account = False
    neg_txn_od = False
    updated_od_paramters_by = None

    if more_than_half_negative_balance_flag:
        is_od_account = True
        if initial_od_limit_input_by_client:
            updated_od_paramters_by = 'UPDATED_OD_CLIENT_TRUE'
            neg_txn_od = False
            od_limit = initial_account_od_limit
        elif initial_account_od_limit:
            updated_od_paramters_by = 'G50%_TXN_OD_TRUE'
            od_limit = initial_account_od_limit
            neg_txn_od = initial_neg_txn_od
            if initial_neg_txn_od:
                updated_od_paramters_by = 'G50%_TXN_OD_TRUE_LIMIT_UPDATED'
                od_limit = int(float(most_negative_balance)) + 1
        else:
            updated_od_paramters_by = 'G50%_TXN_DEFAULT_OD_LIMIT_NONE_OD_TRUE_LIMIT_UPDATED'
            neg_txn_od = True
            od_limit = int(float(most_negative_balance)) + 1
    else:
        updated_od_paramters_by = 'L50%_NEGATIVE_TXN_OD_FALSE'
        is_od_account = False
        neg_txn_od = False
        od_limit = 0
    
    return is_od_account, od_limit, neg_txn_od, updated_od_paramters_by

def calculate_most_negative_balance(account_transactions):
    negative_transactions, most_negative_balance = 0, 0
    for transaction in account_transactions:
        balance = transaction.get('balance')
        if balance and balance < 0:
            negative_transactions += 1
            most_negative_balance = max(most_negative_balance, abs(balance))
    return negative_transactions,most_negative_balance

def update_identity_table_item_data_multiple_keys(statement_id, field_data) -> None:
    if not len(field_data):
        print("identity update field data is empty")
        return
    
    update_expression = "SET " + ", ".join(f"item_data.#identity.{field[0]} = :i{i}" for i, field in enumerate(field_data)) + ", updated_at = :u"
    expression_attribute_values = {f":i{i}": field[1] for i, field in enumerate(field_data)}
    expression_attribute_values[":u"] = time.time_ns()

    bank_connect_identity_table.update_item(
        Key={"statement_id": statement_id},
        UpdateExpression=update_expression,
        ExpressionAttributeNames={"#identity": "identity"},
        ExpressionAttributeValues=expression_attribute_values,
    )

# this function return final account category and is_od_acount flag after considering input account category from user
def get_final_account_category(account_category, is_od_account, input_account_category, input_is_od_account):
    if account_category == None or account_category == '':
        if is_od_account == None:
            is_od_account = input_is_od_account
        account_category = input_account_category

    if is_od_account:
        return 'overdraft', is_od_account
    return account_category, is_od_account

def update_all_transactions_in_statement_pages(statement_id, transactions):
    count_item = bank_connect_statement_table.query(KeyConditionExpression=Key('statement_id').eq(statement_id))
    all_items = count_item.get('Items')
    if len(all_items) == 0:
        return
    page_count = int(all_items[0]["page_count"])
    cnt_transactions_per_page = math.ceil(len(transactions)/page_count)
    all_txns = [transactions[i:i+cnt_transactions_per_page] for i in range(0, len(transactions), cnt_transactions_per_page)]
    for index, items in enumerate(all_txns):
        for i, txn in enumerate(items):
            txn['sequence_number'] = i
            txn['page_number'] = index
        
        update_transactions_on_page(statement_id, index, items)
    if len(all_txns)<=page_count:
        for i in range(len(all_txns), page_count):
            update_transactions_on_page(statement_id, i, [])

def update_transactions_back_in_ddb(statement_id, transactions):
    page_dict = {}
    for txn in transactions:
        if txn["page_number"] not in page_dict:
            page_dict[txn["page_number"]] = [txn]
        else:
            page_dict[txn["page_number"]].append(txn)
    # print(page_dict)
    for page_number in page_dict.keys():
        page_dict[page_number].sort(key=lambda x: x["sequence_number"])
        update_transactions_on_page(statement_id, page_number, page_dict[page_number])


def map_statement_rejection_status(entity_id, statement_id, account_id, to_reject_statement, transactions, bank_name, attempt_type, local_logging_context: LoggingContext = None):
    if not isinstance(local_logging_context, LoggingContext):
        local_logging_context: LoggingContext = LoggingContext(entity_id=entity_id, account_id=account_id, statement_id=statement_id)
    local_logging_context.upsert(source="map_statement_rejection_status")
    
    if not to_reject_statement:
        return False

    transaction_hash = transaction_balance_check(transactions, bank_name, attempt_type)
    if transaction_hash:
        new_transaction_hash = transaction_balance_check(transactions, bank_name, attempt_type, update_month_order=True)
        if not new_transaction_hash:
            transaction_hash = None
    if transaction_hash:
        prev_date, curr_date, inconsistent_transactions = get_inconsistency_date_range(transactions, transaction_hash)
        disparity_data = {
            "fraud_type": "inconsistent_transaction",
            "transaction_hash": transaction_hash,
            "prev_date": prev_date,
            "curr_date": curr_date,
            "statement_id": statement_id,
            "account_id": account_id
        }
        update_statement_inconsistency_ddb(statement_id, True, transaction_hash)
        # Invoke inconsistency solving function solved through kafka. Below code produces an event into Kafka
        if attempt_type in ['pdf', 'online']:
            disparity_data['inconsistent_transaction'] = inconsistent_transactions
            disparity_data['bank_name'] = bank_name
            is_successful = send_data_to_kafka(topic_name=KAFKA_TOPIC_INCONSISTENCY, payload=disparity_data)

            local_logging_context.upsert(topic_name=KAFKA_TOPIC_INCONSISTENCY)
            LAMBDA_LOGGER.info("Failed to send data to kafka" if not is_successful else "Successfully sent data to kafka", extra=local_logging_context.store)
        LAMBDA_LOGGER.info("Statement failed due to inconsistent transactions", extra=local_logging_context.store)
        update_progress(statement_id, 'transactions_status', 'failed', 'Statement contains inconsistent transactions', to_reject_statement=True)
        return True
    
    threshold = 15
    total_transactions = len(transactions)
    null_note_transactions = 0
    for single_transaction in transactions:
        transaction_note = single_transaction.get('transaction_note', '')
        if (isinstance(transaction_note, str) and transaction_note.strip()=='') or not transaction_note:
            null_note_transactions += 1
    if (null_note_transactions/total_transactions)*100 > threshold:
        LAMBDA_LOGGER.info("Statement failed due to transactions with empty transaction note", extra=local_logging_context.store)
        update_progress(statement_id, 'transactions_status', 'failed', 'Statement contains transactions with empty transaction note', to_reject_statement=True)
        return True

    return False


def get_advanced_features_execution_info(entity_id, account_id):
    """
    Returns whether to execute advanced features and statement ids with transaction status.
    Factors:
        1. Transaction status of any statement should not be in processing state.
        2. Transaction status of any one statement should be in completed state.
    """
    time.sleep(random.random())
    account_statements = get_statement_ids_for_account_id(entity_id, account_id)
    statements_transactions_status = {}
    for statement_id in account_statements:
        transactions_status = get_transactions_status(statement_id)
        if transactions_status in (None, 'processing'):
            # dictionary is returned just to log in parent func which statement is in processing state
            return False, {statement_id: transactions_status}
        else:
            statements_transactions_status[statement_id] = transactions_status
    return bool(statements_transactions_status), statements_transactions_status


def extract_advanced_features(
    entity_id,
    statement_id,
    warehousing_meta_data=None,
    fan_out_info_dashboard_resp={},
    local_logging_context: LoggingContext = None,
    org_metadata: dict = {}
) -> bool:
    
    if not isinstance(local_logging_context, LoggingContext):
        local_logging_context: LoggingContext = LoggingContext(entity_id=entity_id, statement_id=statement_id)
    local_logging_context.upsert(source="extract_advanced_features")

    LAMBDA_LOGGER.info(f"Extracting advanced features for {statement_id}", extra=local_logging_context.store)

    LAMBDA_LOGGER.debug(f"Fetching Identity for {statement_id}", extra=local_logging_context.store)
    identity = get_complete_identity_for_statement(statement_id)
    if not identity:
        LAMBDA_LOGGER.warning(f"Returning, identity not available for {statement_id}", extra=local_logging_context.store)
        return False
    
    LAMBDA_LOGGER.debug("identity_status completed, updating in DDB", extra=local_logging_context.store)
    update_progress(statement_id, 'identity_status', 'completed')
    
    # we do not create the advanced features if a statement is extracted by perfios
    if identity.get("is_extracted_by_perfios", False):
        LAMBDA_LOGGER.warning("Returning, statement is extracted by Perfios", extra=local_logging_context.store)
        return False
    
    account_id = identity.get('identity', {}).get('account_id')
    
    is_complete, is_extracted, is_processed = False, False, False
    retries = 0
    while not is_complete:
        is_complete, is_extracted, is_processed = check_extraction_progress(statement_id)
        if not is_complete:
            
            LAMBDA_LOGGER.debug(f"Retrying, all pages aren't extracted for {statement_id}", extra=local_logging_context.store)
            
            retries += 1
            if retries > 30:
                capture_message(f"All pages weren't extracted for {statement_id}")
                
                LAMBDA_LOGGER.warning("transaction_status failed, updating in DDB", extra=local_logging_context.store)
                update_progress(statement_id, 'transactions_status', 'failed')
                
                to_execute_adv_features, statements_transactions_status = get_advanced_features_execution_info(entity_id, account_id)
                if to_execute_adv_features:
                    LAMBDA_LOGGER.info(f"Calling execute_advanced_features_calculation by {statement_id}, statements_transactions_status: {statements_transactions_status}", extra=local_logging_context.store)
                    execute_advanced_features_calculation(
                        entity_id,
                        account_id,
                        statement_id,
                        identity,
                        statements_transactions_status,
                        warehousing_meta_data,
                        fan_out_info_dashboard_resp,
                        local_logging_context,
                        org_metadata=org_metadata
                    )
                    local_logging_context.upsert(source="extract_advanced_features")
                
                LAMBDA_LOGGER.info(f"Returning, all pages weren't extracted for {statement_id}", extra=local_logging_context.store)
                return False
            
            time.sleep(2)

    local_logging_context.upsert(is_complete=is_complete, is_extracted=is_extracted, is_processed=is_processed)

    # if is_processed:
    #     LAMBDA_LOGGER.info(f"Returning, processing already completed for {statement_id}", extra=local_logging_context.store)
    #     return True
    
    if is_complete and not is_extracted:
        LAMBDA_LOGGER.warning("transaction_status failed, updating in DDB",extra=local_logging_context.store)
        update_progress(statement_id, 'transactions_status', 'failed')

        to_execute_adv_features, statements_transactions_status = get_advanced_features_execution_info(entity_id, account_id)
        if to_execute_adv_features:
            LAMBDA_LOGGER.info(f"Calling execute_advanced_features_calculation by {statement_id}, statements_transactions_status: {statements_transactions_status}", extra=local_logging_context.store)
            execute_advanced_features_calculation(
                entity_id,
                account_id,
                statement_id,
                identity,
                statements_transactions_status,
                warehousing_meta_data,
                fan_out_info_dashboard_resp,
                local_logging_context,
                org_metadata=org_metadata
            )
            local_logging_context.upsert(source="extract_advanced_features")
        
        LAMBDA_LOGGER.info(f"Returning, zero transactions for {statement_id}", extra=local_logging_context.store)
        return True
    
    if is_complete and is_extracted and not is_processed:
        LAMBDA_LOGGER.debug(f"Processing {statement_id}", extra=local_logging_context.store)
        
        bank_name = get_bank_name_for_statement(statement_id)
        statement_attempt_type = fan_out_info_dashboard_resp['statement_attempt_type']
        country = get_country_for_statement(statement_id)
        local_logging_context.upsert(bank=bank_name, statement_type=STATEMENT_TYPE_MAP.get(statement_attempt_type, statement_attempt_type))

        check_and_get_everything(bank_name, country)
        
        start_time = time.time()
        LAMBDA_LOGGER.debug(f"Optimising transactions for {statement_id}", extra=local_logging_context.store)
        if statement_attempt_type == 'aa':
            process_and_optimize_transactions_aa(
                statement_id,
                bank_name,
                statement_attempt_type,
                country,
                identity,
                local_logging_context=local_logging_context
            )
        else:
            process_and_optimize_transactions(
                statement_id,
                bank_name,
                statement_attempt_type,
                country,
                identity,
                entity_id,
                local_logging_context=local_logging_context,
                org_metadata=org_metadata,
                warehousing_meta_data=warehousing_meta_data
            )
        local_logging_context.upsert(source="extract_advanced_features")
        end_time = time.time() - start_time
        LAMBDA_LOGGER.info(f"Transactions optimization took {end_time} time for {statement_id}", extra=local_logging_context.store)
        
        st_transactions, hash_page_number_map = get_transactions_for_statement(statement_id, False, False)

        # TODO: The current implementation is a temporary workaround and needs refinement to ensure the flow functions correctly.
        # if bank_name in ['stanchar']:
        #     LAMBDA_LOGGER.debug(f"Fetch more than one year worth of transactions for {statement_id}", extra=local_logging_context.store)
        #     st_transactions = handle_more_than_one_year_cases(statement_id, st_transactions)

        if st_transactions:
            transaction_dates = [txn.get('date') for txn in st_transactions]
            min_txn_date, max_txn_date = min(transaction_dates), max(transaction_dates)
            LAMBDA_LOGGER.debug("Updating Min and Max date range", extra=local_logging_context.store)
            update_date_range(statement_id, min_txn_date, max_txn_date)
        else:
            local_logging_context.upsert(is_extracted=False, is_complete=True)
            
            LAMBDA_LOGGER.warning("transaction_status failed, updating in DDB",extra=local_logging_context.store)
            update_progress(statement_id, 'transactions_status', 'failed')
            
            to_execute_adv_features, statements_transactions_status = get_advanced_features_execution_info(entity_id, account_id)
            if to_execute_adv_features:
                LAMBDA_LOGGER.info(f"Calling execute_advanced_features_calculation by {statement_id}, statements_transactions_status: {statements_transactions_status}", extra=local_logging_context.store)
                execute_advanced_features_calculation(
                    entity_id,
                    account_id,
                    statement_id,
                    identity,
                    statements_transactions_status,
                    warehousing_meta_data,
                    fan_out_info_dashboard_resp,
                    local_logging_context,
                    org_metadata=org_metadata
                )
                local_logging_context.upsert(source="extract_advanced_features")
            
            LAMBDA_LOGGER.info(f"Returning, zero transactions and no min, max date range for {statement_id}",extra=local_logging_context.store)
            return True

        local_logging_context.upsert(is_extracted=is_extracted, is_complete=is_complete)
        
        data_to_send = {
            'is_complete': is_complete,
            'is_extracted': is_extracted,
            'transaction_count': len(st_transactions),
            'update_extraction_timestamp': True
        }
        LAMBDA_LOGGER.debug("transaction_status completed, updating in DDB and RDS",extra=local_logging_context.store)
        to_reject_statement = fan_out_info_dashboard_resp.get('to_reject_statement', False)
        is_updated = map_statement_rejection_status(entity_id, statement_id, account_id, to_reject_statement, st_transactions, bank_name, statement_attempt_type, local_logging_context)
        are_future_dates_present = are_future_dates_present_in_statement_transactions(statement_id, st_transactions)
        if are_future_dates_present:
            update_statement_reject_reason(statement_id, 'Statement contains future date transactions')
            update_progress(statement_id, 'transactions_status', 'failed', 'Statement contains future date transactions', to_reject_statement=True)
            is_updated = True
        local_logging_context.upsert(source="extract_advanced_features")
        if not is_updated:
            update_progress(statement_id, 'transactions_status', 'completed')
        update_progress_on_dashboard(statement_id, data_to_send)
        
        is_multi_account_statement = get_is_multi_account_statement(statement_id)
        to_execute_adv_features, statements_transactions_status = get_advanced_features_execution_info(entity_id, account_id)
        
        # TODO: need to properly fix the flow, this is a hack at the moment
        if is_multi_account_statement:
            update_progress(statement_id, 'processing_status', 'completed')
            to_execute_adv_features = True
        if to_execute_adv_features:
            LAMBDA_LOGGER.info(f"Calling execute_advanced_features_calculation by {statement_id}, statements_transactions_status: {statements_transactions_status}", extra=local_logging_context.store)
            execute_advanced_features_calculation(
                entity_id,
                account_id,
                statement_id,
                identity,
                statements_transactions_status,
                warehousing_meta_data,
                fan_out_info_dashboard_resp,
                local_logging_context,
                org_metadata=org_metadata
            )
            local_logging_context.upsert(source="extract_advanced_features")
        else:
            LAMBDA_LOGGER.info(f"Returning for {statement_id}, other statements are in processing state: {statements_transactions_status}", extra=local_logging_context.store)

    return True

def get_is_multi_account_statement(statement_id):
    count_item = bank_connect_statement_table.query(KeyConditionExpression=Key('statement_id').eq(statement_id))
    all_items = count_item.get('Items')
    if len(all_items) == 0:
        return False
    else:
        status = all_items[0].get('is_multi_account_statement')
        return status


def execute_advanced_features_calculation(
    entity_id,
    account_id,
    event_statement_id,
    event_identity,
    statements_processing_status,
    warehousing_meta_data,
    fan_out_info_dashboard_resp,
    local_logging_context: LoggingContext = None,
    org_metadata: dict = {}
):
    
    if not isinstance(local_logging_context, LoggingContext):
        local_logging_context: LoggingContext = LoggingContext(entity_id=entity_id, account_id=account_id, statement_id=event_statement_id)
    local_logging_context.upsert(source="execute_advanced_features_calculation")
    
    LAMBDA_LOGGER.info(f"Executing advanced features calculation by account: {account_id}, statement: {event_statement_id}", extra=local_logging_context.store)
    
    if warehousing_meta_data is None:
        warehousing_meta_data = {}
    
    bank_name = get_bank_name_for_statement(event_statement_id)
    country = get_country_for_statement(event_statement_id)
    statement_attempt_type = fan_out_info_dashboard_resp['statement_attempt_type']

    warehouse_data = {
        "entity_id": entity_id,
        "statement_id": event_statement_id,
        "account_id": account_id,
        "bank_name": bank_name,
        "account_number": event_identity.get('identity', {}).get("account_number", ""),
        **warehousing_meta_data
    }
    
    caching_payload = {
        'entity_id': entity_id,
        'event_statement_id': event_statement_id,
        'account_id': account_id,
        'statements_processing_status': {},
        'warehousing_meta_data': warehousing_meta_data,
        'fan_out_info_dashboard_resp': fan_out_info_dashboard_resp,
        'org_metadata': org_metadata
    }
    
    txn_completed_statements = []
    txn_failed_statemenets = []
    for statement_id, transaction_status in statements_processing_status.items():
        if transaction_status=='completed':
            txn_completed_statements.append(statement_id)
        elif transaction_status=='failed':
            txn_failed_statemenets.append(statement_id)
    
    if not bool(txn_completed_statements) and len(txn_failed_statemenets)==len(statements_processing_status):
        caching_payload['statements_processing_status'] = statements_processing_status
        async_invoke_cache_subscribed_data(caching_payload)
    else:
        # Executing advanced features calculation only if any one statement is completed.
        is_extracted, is_complete = True, True
        
        account_transactions, hash_dict = get_transactions_for_account(entity_id, account_id, send_hash_page_number_map=True)
        local_logging_context.upsert(total_account_transactions=len(account_transactions))

        LAMBDA_LOGGER.debug(f"Processing account category for {account_id}", extra=local_logging_context.store)
        account_details = process_account_category(entity_id, account_id, account_transactions, txn_completed_statements)
        account_category, _ = get_final_account_category(
            account_details.get("item_data", {}).get("account_category"),
            account_details.get("item_data", {}).get("is_od_account"),
            account_details.get("item_data", {}).get("input_account_category"),
            account_details.get("item_data", {}).get("input_is_od_account")
        )
        
        start_time = time.time()

        LAMBDA_LOGGER.debug(f"Processing account transactions for {account_id}", extra=local_logging_context.store)
        try:
            process_account_transactions(
                entity_id,
                account_id,
                account_transactions,
                hash_dict,
                bank_name,
                account_category,
                country,
                local_logging_context=local_logging_context
            )
            local_logging_context.upsert(source="execute_advanced_features_calculation")
        except Exception as e:
            local_logging_context.upsert(exception=str(e), trace=traceback.format_exc())
            LAMBDA_LOGGER.error(f"Exception observed while processing account transactions for {account_id}", extra=local_logging_context.store)
            local_logging_context.remove_keys(["exception", "trace"])

            capture_exception(e)
            
            for statement_id in txn_completed_statements:
                statements_processing_status[statement_id] = 'failed'

            caching_payload['statements_processing_status'] = statements_processing_status

            async_invoke_cache_subscribed_data(caching_payload)
            return False

        end_time = time.time() - start_time
        LAMBDA_LOGGER.info(f"Total time taken to process account transactions for {account_id}: {end_time}", extra=local_logging_context.store)

        LAMBDA_LOGGER.info(f"Calculating advanced features for {account_id} by {event_statement_id}", extra=local_logging_context.store)
        
        account_data = dict()

        LAMBDA_LOGGER.debug(f"Producing advance features for {account_id}", extra=local_logging_context.store)
        start_time_recurring = time.time()
        account_data['recurring_transactions'] = produce_advanced_features(account_transactions, use_workers=False)
        end_time_recurring = time.time() - start_time_recurring
        LAMBDA_LOGGER.info(f"Total time taken to produce advance features for {account_id}: {end_time_recurring}", extra=local_logging_context.store)
        
        # NOTE: account category is marked as individual or corporate only and not overdraft, as of now it is not useful for salary calculation
        temp_account_category = account_details.get("item_data", {}).get('account_category', None)
        if temp_account_category == '' or temp_account_category is None:
            temp_account_category = account_details.get("item_data", {}).get('input_account_category', '')
        
        if isinstance(temp_account_category, str) and temp_account_category.lower() in ['corporate', 'current']:
            LAMBDA_LOGGER.debug(f'Adding salary confidence percentage for {account_id}', extra=local_logging_context.store)
            account_data['salary_transactions'] = []
            add_salary_confidence_percentage(entity_id, account_id, None)
        else:
            employer_names = fan_out_info_dashboard_resp["employer_names"]
            recurring_salary_flag = fan_out_info_dashboard_resp["recurring_salary_flag"]
            salary_mode = fan_out_info_dashboard_resp["salary_mode"]
            salary_configuration = fan_out_info_dashboard_resp.get('salary_configuration', dict())
            salary_v3_enabled = salary_configuration.get("v3", dict()).get("enabled", False)

            LAMBDA_LOGGER.debug(f'Getting salary transactions for {account_id}', extra=local_logging_context.store)
            probable_salary_txns_grps = separate_probable_salary_txn_grps(account_data['recurring_transactions'], account_transactions)
            
            if salary_v3_enabled:
                salary_data = get_salary_transactions(account_transactions, employer_names, recurring_salary_flag, salary_mode, probable_salary_transaction_groups=probable_salary_txns_grps, salary_configuration = salary_configuration)
            else:
                salary_data = get_salary_transactions_v1(account_transactions, employer_names, recurring_salary_flag, salary_mode, probable_salary_transaction_groups=probable_salary_txns_grps, salary_configuration = salary_configuration)
            
            salary_confidence_percentage = salary_data.get('confidence_percentage',None)
            account_data['salary_transactions'] = salary_data.get('salary_transactions', [])
            
            # update the single category of the recurring salary transactions to Salary
            denormalised_statement_dict = {}
            salary_hashes = []
            hashes_to_update = {}
            salary_calc_method_dict = {}
            
            for txn in account_data["salary_transactions"]:
                salary_hashes.append(txn["hash"])
                txn["category"] = "Salary"
                salary_calc_method_dict[txn["hash"]] = txn.get('calculation_method')
            
            LAMBDA_LOGGER.debug('Denormalize hash statements, update transactions and add salary confidence percentage', extra=local_logging_context.store)
            help_denormalise_hash_statements(denormalised_statement_dict, salary_hashes, hash_dict)
            hashes_to_update_generalised(
                hashes_to_update,
                salary_hashes,
                {
                    "transaction_channel": "salary", # uncomment this line when we are sure that transaction_channel should be salary for all cases - keyword & recurring etc.
                    "category": "Salary",
                    "is_lender": False,
                    "description": "",
                    "merchant_category": "",
                    "salary_confidence_percentage": salary_confidence_percentage
                },
                account_transactions,
                salary_calc_method_dict
            )
            update_transactions_basis_statement_dict(denormalised_statement_dict, hashes_to_update)
            add_salary_confidence_percentage(entity_id,account_id, salary_confidence_percentage)
        
        process_account_category_based_on_transactions(entity_id, account_id, account_transactions, account_category, txn_completed_statements)

        LAMBDA_LOGGER.debug(f"Salary completed, now updating account disparity for {account_id}",extra=local_logging_context.store)
        start_time_disparity = time.time()
        update_account_disparity(
            entity_id,
            account_id,
            event_statement_id,
            account_transactions,
            account_data['salary_transactions'],
            account_category,
            bank_name,
            country,
            statement_attempt_type,
            warehouse_data,
            fan_out_info_dashboard_resp,
            local_logging_context=local_logging_context
        )
        local_logging_context.upsert(source="execute_advanced_features_calculation")
        end_time_disparity = time.time() - start_time_disparity
        LAMBDA_LOGGER.info(f"Total time taken to produce disparity for {account_id}: {end_time_disparity}", extra=local_logging_context.store)

        # this method only saves salary transactions AND recurring transactions in ddb!
        put_advance_features_in_ddb(account_data, account_id, entity_id)

        # checking for date discontinuity
        LAMBDA_LOGGER.debug("Finding dates discontinuity in the transactions", extra=local_logging_context.store)
        date_range_for_statements_from_transactions, date_range_for_statements_identity = date_range_calculation_for_statements(entity_id, account_id, local_logging_context)
        LAMBDA_LOGGER.debug(f"Date range statement identity list: {date_range_for_statements_identity}", extra=local_logging_context.store)
        current_account_date_range = extract_account_date_range(date_range_for_statements_identity)

        # Account table data to update tuple
        account_data_to_update: list[tuple[str, Any]] = []
        account_data_to_update.append(('account_date_range', current_account_date_range))

        date_discontinuity = []
        if not fan_out_info_dashboard_resp['accept_anything']:
            date_discontinuity = missing_dates(
                entity_id,
                account_id,
                fan_out_info_dashboard_resp,
                bank_name,
                date_range_for_statements_from_transactions, 
                date_range_for_statements_identity,
                local_logging_context=local_logging_context
            )
            account_data_to_update.append(('missing_data', date_discontinuity))

        # Update account table
        update_account_table_multiple_keys(entity_id, account_id, account_data_to_update)

        caching_payload['statements_processing_status'] = statements_processing_status
        caching_payload['missing_data'] = date_discontinuity
        
        async_invoke_cache_subscribed_data(caching_payload)

    LAMBDA_LOGGER.debug("Successfully extracted advanced features", extra=local_logging_context.store)
    return True


def get_date_range_from_statement_id(statement_id):
    """
        This function returns from_date, to_date (transactions overwritten if no data)
        and extracted_from_date, extracted_to_date (identity).
        If extracted_from_date and extracted_to_date is None, then it defaults
        to from_date and to_date.
    """
    identity = get_complete_identity_for_statement(statement_id)
    date_range = identity.get("date_range")
    extracted_date_range = identity.get("extracted_date_range")

    if not isinstance(date_range, dict):
        date_range = {}

    if not isinstance(extracted_date_range, dict):
        extracted_date_range = {}

    from_date = date_range.get("from_date")
    to_date = date_range.get("to_date")

    extracted_from_date = extracted_date_range.get("from_date")
    extracted_to_date = extracted_date_range.get("to_date")

    if from_date is not None and to_date is not None:
        from_date = datetime.datetime.strptime(from_date,"%Y-%m-%d")
        to_date = datetime.datetime.strptime(to_date,"%Y-%m-%d")
    
    if extracted_from_date and extracted_to_date:
        extracted_from_date = datetime.datetime.strptime(extracted_from_date, "%Y-%m-%d")
        extracted_to_date = datetime.datetime.strptime(extracted_to_date, "%Y-%m-%d")
    else:
        extracted_from_date = from_date
        extracted_to_date = to_date

    return {
        "from_date": from_date,
        "to_date": to_date,
        "extracted_from_date": extracted_from_date,
        "extracted_to_date": extracted_to_date
    }


def extract_account_date_range(date_range_for_statements_identity):
    account_from_date = None
    account_to_date = None

    if date_range_for_statements_identity:
        try:
            # Extract earliest identity from date 
            account_from_date = date_range_for_statements_identity[0][1].strftime('%d-%m-%Y')
            # Extract last identity to date
            account_to_date = date_range_for_statements_identity[-1][-1].strftime('%d-%m-%Y')
        except Exception as e:
            capture_exception(e)

    return {
        'from_date': account_from_date,
        'to_date': account_to_date
    }

def date_range_calculation_for_statements(entity_id, account_id, local_logging_context):
    local_logging_context.upsert(source="date_range_calculation_for_statements", entity_id=entity_id, account_id=account_id)
    LAMBDA_LOGGER.info(
        "Calculating date range for statements",
        extra=local_logging_context.store
    )

    statement_ids = get_statement_ids_for_account_id(entity_id, account_id)

    date_range_for_statements_from_transactions = []
    date_range_for_statements_identity = []
    identity_date_range_dict = {}

    for statement_id in statement_ids:
        statement_transaction_progress, _ = get_progress(statement_id, "transactions_status")
        LAMBDA_LOGGER.debug(
            f"Statement transaction progress for statement id: {statement_id} is {statement_transaction_progress}",
            extra=local_logging_context.store
        )
        rejected_statement = get_field_for_statement(statement_id, "to_reject_statement")
        if statement_transaction_progress != "completed" and not rejected_statement:
            continue

        date_range = get_date_range_from_statement_id(statement_id)
        from_date = date_range.get("from_date")
        to_date = date_range.get("to_date")
        identity_from_date = date_range.get("extracted_from_date")
        identity_to_date = date_range.get("extracted_to_date")

        # for now we are not taking into consideration the statements where from and to date is not extracted
        # future work : test for cases where from and to date is null

        if not (from_date and to_date and identity_from_date and identity_to_date):
            continue
        if isinstance(from_date, str) and isinstance(to_date, str):
            from_date = datetime.datetime.strptime(from_date, "%Y-%m-%d %H:%M:%S")
            to_date = datetime.datetime.strptime(to_date, "%Y-%m-%d %H:%M:%S")
        if isinstance(identity_from_date, str) and isinstance(identity_to_date, str):
            identity_from_date = datetime.datetime.strptime(identity_from_date, "%Y-%m-%d %H:%M:%S")
            identity_to_date = datetime.datetime.strptime(identity_to_date, "%Y-%m-%d %H:%M:%S")

        date_range_for_statements_from_transactions.append([statement_id, from_date, to_date])
        date_range_for_statements_identity.append([statement_id, identity_from_date, identity_to_date])
        identity_date_range_dict[statement_id] = {'from_date': identity_from_date, 'to_date': identity_to_date}

    date_range_for_statements_from_transactions.sort(key = lambda x : (x[1], x[2]))
    date_range_for_statements_identity.sort(key = lambda x : (x[1], x[2]))
    LAMBDA_LOGGER.debug(
        f"Date range for statements: {date_range_for_statements_from_transactions}",
        extra=local_logging_context.store
    )
    return date_range_for_statements_from_transactions,date_range_for_statements_identity

def missing_dates(
    entity_id,
    account_id,
    fan_out_info_dashboard_resp,
    bank_name,
    date_range_for_statements_from_transactions, 
    date_range_for_statements_identity, 
    local_logging_context: LoggingContext = None
):
    #####################################################################################
    # input (args): entity_id: str, account_id: str, session_date_range: dict, bank_name: str
    # output: list
    #####################################################################################
    session_date_range = fan_out_info_dashboard_resp["session_date_range"]
    if not isinstance(local_logging_context, LoggingContext):
        local_logging_context: LoggingContext = LoggingContext(entity_id=entity_id, account_id=account_id)
    local_logging_context.upsert(source="missing_dates", bank=bank_name)
    LAMBDA_LOGGER.info(
        f"Attempting to find missing dates for the account_id: {account_id}, session_date_range: {session_date_range}",
        extra=local_logging_context.store
    )
    missing_data = []
    session_from_date = session_date_range.get("from_date")
    session_to_date = session_date_range.get("to_date")
    print("session start date : ", session_from_date)
    print("session to date : ", session_to_date)
    session_from_date_dt = datetime.datetime.strptime(session_from_date, '%d/%m/%Y') if session_from_date else None
    session_to_date_dt = datetime.datetime.strptime(session_to_date, '%d/%m/%Y') if session_to_date else None
    # print("session start date datetime: ", session_from_date_dt)
    # print("session to date datetime: ", session_to_date_dt)
    GRACE_PERIOD = 4

    previous_statement_single_transaction_hashes = []
    current_statement_single_transaction_hashes = []
    for index in range(1, len(date_range_for_statements_from_transactions)):
        # ----------------------------------------------------------------
        # to find inconsistency around edges of statements in an account
        # this looks for missing data around closing balance of a statement and opening of other statement
        # ----------------------------------------------------------------
        # if (
        #     (date_range_for_statements[index][1]>= date_range_for_statements[index-1][1]) and 
        #     (date_range_for_statements[index][2] <= date_range_for_statements[index-1][2])
        # ):
        previous_statement_id = date_range_for_statements_from_transactions[index-1][0]
        current_statement_id = date_range_for_statements_from_transactions[index][0]
        previous_statement_id_transactions, _ = get_transactions_for_statement(previous_statement_id, show_rejected_transactions=True)
        current_statement_transactions, _ = get_transactions_for_statement(current_statement_id, show_rejected_transactions=True)

        if index == 1:
            for prev_statement_transaction in previous_statement_id_transactions:
                previous_statement_single_transaction_hashes.append(single_transaction_hash(prev_statement_transaction))
        
        is_overlapping_pdf = False
        current_statement_single_transaction_hashes = []
        for current_statement_transaction in current_statement_transactions:
            current_hash = single_transaction_hash(current_statement_transaction)
            current_statement_single_transaction_hashes.append(current_hash)
            if current_hash in previous_statement_single_transaction_hashes:
                is_overlapping_pdf = True
                break
        
        previous_statement_single_transaction_hashes = current_statement_single_transaction_hashes
        if is_overlapping_pdf:
            continue
        
        if not (date_range_for_statements_from_transactions[index][1]
                >= date_range_for_statements_from_transactions[index-1][2]):
            continue

        if previous_statement_id_transactions and current_statement_transactions:
            last_transaction_of_previous_statement_id = get_correct_transaction_order(previous_statement_id_transactions)[-1]
            first_transaction_of_current_statement_id = get_correct_transaction_order(current_statement_transactions)[0]
            transaction_set = [last_transaction_of_previous_statement_id, first_transaction_of_current_statement_id]
            inconsistent_hash = transaction_balance_check(transaction_set, bank_name)
            if not inconsistent_hash:
                continue
            print("inconsistency detected at hash : ", inconsistent_hash)
            missing_obj = {
                "from_date": date_range_for_statements_from_transactions[index-1][2].strftime("%Y-%m-%d"),
                "to_date": date_range_for_statements_from_transactions[index][1].strftime("%Y-%m-%d")
            }
            missing_data.append(missing_obj)
            
            print(
                f"missing data found between statements: {date_range_for_statements_from_transactions[index-1][0]}"
                f" and {date_range_for_statements_from_transactions[index][0]}, data injected is {missing_obj}\n\n"
            )

    from_date_to_check = None
    to_date_to_check = None
    identity_from_date_combined = date_range_for_statements_identity[0][1] if date_range_for_statements_identity else None
    identity_to_date_combined = date_range_for_statements_identity[-1][2] if date_range_for_statements_identity else None
    if session_from_date and session_to_date and date_range_for_statements_from_transactions:
        from_date_to_check = session_from_date_dt
        to_date_to_check = session_to_date_dt
    elif date_range_for_statements_from_transactions:
        from_date_to_check = date_range_for_statements_identity[0][1]
        to_date_to_check = date_range_for_statements_identity[-1][2]
    
    txn_from_date, txn_to_date = None, None
    if date_range_for_statements_from_transactions:
        txn_from_date = date_range_for_statements_from_transactions[0][1]
        txn_to_date = date_range_for_statements_from_transactions[-1][2]
    
    acceptance_criteria = fan_out_info_dashboard_resp['acceptance_criteria']
    ignore_missing_dates_days = fan_out_info_dashboard_resp.get('ignore_missing_dates_days')

    difference_first = None
    is_ignore_from_first_enabled = 'ignore_missing_txns_first' in acceptance_criteria
    has_ignore_days_parameter_set = ignore_missing_dates_days is not None
    is_extracted_transactions_grater_than_required_in_front = txn_from_date is not None and from_date_to_check is not None and (txn_from_date <= from_date_to_check)
    if txn_from_date and from_date_to_check and not is_extracted_transactions_grater_than_required_in_front:
        delta = txn_from_date-from_date_to_check
        difference_first = delta.days
    is_difference_from_first_calculated = difference_first is not None
    ingore_missing_first = is_ignore_from_first_enabled and has_ignore_days_parameter_set and (is_extracted_transactions_grater_than_required_in_front or (is_difference_from_first_calculated and not is_extracted_transactions_grater_than_required_in_front and difference_first <= ignore_missing_dates_days ))

    difference_last = None
    is_ignore_from_end_enabled = 'ignore_missing_txns_last' in acceptance_criteria
    is_extracted_transactions_grater_than_required_in_end = txn_to_date is not None and to_date_to_check is not None and (txn_to_date >= to_date_to_check)
    if txn_to_date and to_date_to_check and not is_extracted_transactions_grater_than_required_in_end:
        delta = to_date_to_check-txn_to_date
        difference_last = delta.days
    is_difference_from_last_calculated = difference_last is not None
    
    ignore_missing_last = False
    if to_date_to_check is not None:
        last_month = to_date_to_check.strftime('%m')
        last_day = to_date_to_check.strftime('%d')
        to_date_year = to_date_to_check.strftime('%Y')
        _,total_days = calendar.monthrange(int(to_date_year),int(last_month))

        has_month_completed = total_days == int(last_day)
        first_date_of_last_month = datetime.datetime.strptime(f"{to_date_year}-{last_month}-01",'%Y-%m-%d')
        is_there_atleast_one_transaction_in_last_month = any([True if stmt_txn_range[1]<=first_date_of_last_month and first_date_of_last_month<=stmt_txn_range[2] else False for stmt_txn_range in date_range_for_statements_from_transactions])
        ignore_missing_last = is_there_atleast_one_transaction_in_last_month and not has_month_completed and is_ignore_from_end_enabled and has_ignore_days_parameter_set and (is_extracted_transactions_grater_than_required_in_end or (is_difference_from_last_calculated and not is_extracted_transactions_grater_than_required_in_end and difference_last <= ignore_missing_dates_days ))
    
    if from_date_to_check and to_date_to_check and date_range_for_statements_from_transactions:
        print("entering into date range checks")
        first_statement_id = date_range_for_statements_from_transactions[0][0]
        last_statement_id = date_range_for_statements_from_transactions[-1][0]
        statement_data_first_statement = get_statement_table_data(first_statement_id)
        opening_date_removal_0 = statement_data_first_statement.get(f'removed_date_opening_balance_{0}')
        cut_transactions_page_0 = statement_data_first_statement.get(f'cut_transactions_page_{0}', False)

        # This is a check only for entity flow
        opening_bal_first_txn_check, closing_bal_last_txn_check = False, False
        if session_from_date and session_to_date:
            
            first_statement_identity = get_complete_identity_for_statement(first_statement_id)
            last_statement_identity = {}
            
            first_statement_transactions, _ = get_transactions_for_statement(first_statement_id, show_rejected_transactions=True)
            last_statement_transactions = []
            
            if first_statement_id!=last_statement_id:
                last_statement_identity = get_complete_identity_for_statement(last_statement_id)
                last_statement_transactions, _ = get_transactions_for_statement(last_statement_id, show_rejected_transactions=True)
            else:
                last_statement_identity = deepcopy(first_statement_identity)
                last_statement_transactions = deepcopy(first_statement_transactions)
            
            first_txn_of_first_statement, last_txn_of_last_statement = {}, {}
            if first_statement_transactions:
                first_txn_of_first_statement = deepcopy(first_statement_transactions[0])
                if first_txn_of_first_statement['transaction_type'] == 'debit':
                    first_txn_of_first_statement['amount'] = -first_txn_of_first_statement['amount']
            if last_statement_transactions:
                last_txn_of_last_statement = deepcopy(last_statement_transactions[-1])
            
            opening_balance = amount_to_float(first_statement_identity.get('opening_bal'))
            closing_balance = amount_to_float(last_statement_identity.get('closing_bal'))

            if opening_balance and closing_balance and first_txn_of_first_statement and last_txn_of_last_statement:
                opening_bal_first_txn_check = (
                    (opening_balance + first_txn_of_first_statement['amount'])
                    == first_txn_of_first_statement['balance']
                )
                closing_bal_last_txn_check = closing_balance == last_txn_of_last_statement['balance']
            
        
        date_to_check = date_range_for_statements_from_transactions[0][1]
        if opening_date_removal_0 is not None:
            date_to_check = datetime.datetime.strptime(opening_date_removal_0, '%Y-%m-%d %H:%M:%S')
        if opening_bal_first_txn_check and identity_from_date_combined is not None:
            date_to_check = identity_from_date_combined

        if (not cut_transactions_page_0 and (from_date_to_check + relativedelta(days=GRACE_PERIOD) < date_to_check) and not ingore_missing_first):
            missing_obj = {
                "from_date": from_date_to_check.strftime("%Y-%m-%d"),
                "to_date": date_to_check.strftime("%Y-%m-%d")
            }
            missing_data.append(missing_obj)
            print("missing data identified in beginning : ", missing_obj)

        statement_data_last_statement = get_statement_table_data(last_statement_id)
        last_statement_page_count = statement_data_last_statement.get('page_count')
        if last_statement_page_count is not None:
            closing_date_removal_last_page = statement_data_last_statement.get(
                f'removed_date_closing_balance_{last_statement_page_count-1}'
            )
            cut_transactions_page_last = statement_data_last_statement.get(
                f'cut_transactions_page_{last_statement_page_count-1}', False
            )

            date_to_check = date_range_for_statements_from_transactions[-1][2]
            if closing_date_removal_last_page is not None:
                date_to_check = datetime.datetime.strptime(closing_date_removal_last_page, '%Y-%m-%d %H:%M:%S')
            if closing_bal_last_txn_check and identity_to_date_combined is not None:
                date_to_check = identity_to_date_combined

            if (not cut_transactions_page_last and (date_to_check + relativedelta(days=GRACE_PERIOD) < to_date_to_check)) and not ignore_missing_last:
                missing_obj = {
                    "from_date": date_to_check.strftime('%Y-%m-%d'),
                    "to_date": to_date_to_check.strftime('%Y-%m-%d'),
                }
                missing_data.append(missing_obj)
                print("missing data identified in end : ", missing_obj)

    # sort missing data in ascending order
    missing_data.sort(key=lambda row: (row["from_date"], row["to_date"]))
    account_data = get_account_for_entity(entity_id, account_id)
    account_opening_date = account_data.get("item_data", dict()).get("account_opening_date")
    
    final_missing_data = []
    last_to_date = None
    for i in range(len(missing_data)):
        if isinstance(account_opening_date, str) and missing_data[i]["to_date"] < account_opening_date:
            continue
        
        if isinstance(account_opening_date, str) and missing_data[i]["from_date"] <= account_opening_date:
            new_from_date = max(account_opening_date, missing_data[i]["from_date"])
            if missing_data[i]["to_date"] == new_from_date:
                continue
            missing_data[i]["from_date"] = new_from_date
        
        if len(final_missing_data) == 0:
            final_missing_data.append(missing_data[i])
        else:
            if missing_data[i]["from_date"] <= last_to_date:
                final_missing_data[-1]["to_date"] = last_to_date
            else:
                final_missing_data.append(missing_data[i])
        last_to_date = missing_data[i]["to_date"]

    LAMBDA_LOGGER.debug(
        f"Missing dates identified, {final_missing_data}",
        extra=local_logging_context.store
    )
    return final_missing_data


def check_for_date_discontinuity(entity_id, account_id, present_statement_id):
    statement_ids = get_statement_ids_for_account_id(entity_id, account_id)
    bank_name = get_bank_name_for_statement(present_statement_id)
    
    TRANSACTION_CHECK_BANK_LIST = ["sbi"]
    GRACE_PERIOD = 1 # in days

    dates = []
    discontinuitues = []

    for statement_id in statement_ids:
        date_range = get_date_range_from_statement_id(statement_id)
        from_date = date_range.get("from_date")
        to_date = date_range.get("to_date")
        extracted_from_date = date_range.get("extracted_from_date")
        extracted_to_date = date_range.get("extracted_to_date")
        
        if from_date is None or to_date is None:
            continue

        if isinstance(bank_name, str) and bank_name.lower() in TRANSACTION_CHECK_BANK_LIST:
            # for these banks we need to compare the from and to available now with the
            # from and to that was extracted during identity. from and to is modified in
            # the update state fan out to min and max transaction dates. we need to get this from a mock
            # identity date range stored during identity.
            
            if extracted_from_date is None or extracted_to_date is None:
                continue

            if to_date + relativedelta(days = GRACE_PERIOD) < extracted_to_date:
                print("Transaction level discontinuity spotted")
                discontinuitues.append({
                    "from_date" : to_date.strftime('%Y-%m-%d'),
                    "to_date" : extracted_to_date.strftime('%Y-%m-%d')
                })
                continue
        dates.append([from_date, to_date])

    # sort the dates
    dates = sorted(dates, key=lambda x: x[0])

    to_date_temp = dates[0][1] if dates else None

    for i in range(1, len(dates)):
        local_from_date = dates[i][0]
        local_to_date = dates[i][1]
        
        if local_from_date > to_date_temp + relativedelta(days = GRACE_PERIOD):
            # give all discontinuities
            discontinuitues.append({
                "to_date": local_from_date.strftime('%Y-%m-%d'),
                "from_date": to_date_temp.strftime('%Y-%m-%d')
            })
            print(f"dates discontinuous between {to_date_temp.strftime('%Y-%m-%d')} and {local_from_date.strftime('%Y-%m-%d')}")
        to_date_temp = local_to_date
    
    # removing the discontinuity dicts that may be overlapping
    final_discontinuities = []
    for i in discontinuitues:
        if i in final_discontinuities:
            continue
        final_discontinuities.append(i)
    return final_discontinuities

def get_date_discontinuity(entity_id, account_id):
    statement_rows = bank_connect_account_table.query(
                        KeyConditionExpression = Key('entity_id').eq(entity_id) & Key('account_id').eq(account_id)
                    )

    items = statement_rows['Items']

    if len(items) > 0:
        if remark := items[0].get('item_data', {}).get('missing_data', []):
            return remark
    return []


def send_event_to_quality(statement_id, entity_id, identity, is_credit_card=False, local_logging_context=None):
    """
    Send event to Quality tool
    """
    if CURRENT_STAGE not in ["prod", "dev"]:
        return
    
    sqs_response = sqs_client.send_message(
        QueueUrl = QUALITY_QUEUE_URL,
        MessageBody = json.dumps({
            "statement_id": statement_id,
            "is_credit_card": is_credit_card,
            'identity': identity
        })
    )
    print("Event sent to quality --> ", sqs_response)

    # if not local_logging_context:
    #     local_logging_context: LoggingContext = LoggingContext(
    #         statement_id=statement_id,
    #         entity_id=entity_id
    #     )
    #     local_logging_context.upsert(source="send_event_to_quality")

    # data_to_send_to_kafka = {
    #         "statement_id": statement_id,
    #         "is_credit_card": is_credit_card,
    #         'identity': identity
    #     }
    # is_successful = send_data_to_kafka(topic_name=KAFKA_TOPIC_QUALITY_EVENTS,data_to_send=data_to_send_to_kafka)

    # local_logging_context.upsert(topic_name=KAFKA_TOPIC_QUALITY_EVENTS)
    # LAMBDA_LOGGER.info(
    #     "Failed to send data to kafka" if not is_successful else "Successfully sent data to kafka",
    #     extra=local_logging_context.store
    # )


def get_salary_transactions_from_ddb(account_id):
    items = collect_results(bank_connect_salary_table.query,
                            {'KeyConditionExpression': Key('account_id').eq(account_id) })

    if len(items) == 0:
        return list()

    else:
        s3_object_key = items[0].get('s3_object_key')
        if s3_object_key:
            obj = s3_resource.Object(BANK_CONNECT_DDB_FAILOVER_BUCKET, s3_object_key)
            item_data = obj.get()['Body'].read().decode('utf-8')
            return json.loads(item_data)
    to_return = json.loads(items[0].get('item_data', '[]'))

    # handle the case to_return is None
    if to_return:
        return to_return
    return []

def get_disparities_from_ddb(account_id):
    """
    Fetches and return disparity (frauds other than identity) for a given
    entity and account combination from DynamoDB
    :param: entity_id, account_id
    :return: list of disparity object containing fraud_type, transaction_hash,
            statement_id (if disparity present),
            otherwise returns a blank list
    """

    items = collect_results(bank_connect_disparities_table.query,
            {'KeyConditionExpression': Key('account_id').eq(account_id)})
    
    if items:
        item = []
        s3_object_key = items[0].get('s3_object_key')
        if s3_object_key:
            obj = s3_resource.Object(BANK_CONNECT_DDB_FAILOVER_BUCKET, s3_object_key)
            item_data = obj.get()['Body'].read().decode('utf-8')
            item = json.loads(item_data)
        else:
            item = json.loads(items[0].get('item_data', '[]'))

        if item:
            if isinstance(item, dict):
                # handle old data where 'transaction_hash' might be missing
                # and/or item will be a dictionary instead of an array of
                # dictionary
                inconsistent_hash = item.get('inconsistent_transaction_hash')
                if inconsistent_hash:
                    item['transaction_hash'] = inconsistent_hash
                else:
                    item['transaction_hash'] = None
                item = [item]  # make array from dictionary

            return item
    return []

def get_non_metadata_frauds(account_id, include_inconsistent_transactions = False):
    """
    Takes entity id and account id and returns all non metadata frauds in
    required format
    """
    frauds = get_disparities_from_ddb(account_id)
    for index, fraud in enumerate(frauds):
        frauds[index] = {
        'statement_id': fraud.get('statement_id'),
        'fraud_type': fraud['fraud_type'],
        'account_id': account_id,
        'transaction_hash': fraud.get('transaction_hash', None),
        'previous_date': fraud.get('prev_date', None),
        'current_date': fraud.get('curr_date', None),
        'fraud_category': fraud_category.get(fraud['fraud_type'], 'uncategorized')
        }
        
        if include_inconsistent_transactions:
            inconsistent_transactions = fraud.get('inconsistent_transaction')
            if not isinstance(inconsistent_transactions, list):
                continue
                
            inconsistent_transactions_to_send = []
            for inconsistent_transaction in inconsistent_transactions:
                inconsistent_transactions_to_send.append({
                    "transaction_type": inconsistent_transaction.get("transaction_type"),
                    "transaction_note": inconsistent_transaction.get("transaction_note"),
                    "chq_num": inconsistent_transaction.get("chq_num"),
                    "amount": inconsistent_transaction.get("amount"),
                    "balance": inconsistent_transaction.get("balance"),
                    "date": inconsistent_transaction.get("date"),
                    "page_number": inconsistent_transaction.get("page_number"),
                    "sequence_number": inconsistent_transaction.get("sequence_number"),
                })
            inconsistent_transactions_to_send.sort(key=lambda x: (x['page_number'], x['sequence_number']))
            frauds[index].update({
                'inconsistent_transactions': inconsistent_transactions_to_send
            })
            
    return frauds

def get_extracted_frauds_list(entity_id, account_id, statements):
    fraud_list = []

    is_extracted_by_perfios = False
    for statement_id in statements:
        st_identity = get_complete_identity_for_statement(statement_id)
    
        print("complete identity for statement id: {} is -> {}".format(statement_id, st_identity))
        
        # checking if this acount was extracted by perfios
        is_extracted_by_perfios = is_extracted_by_perfios or st_identity.get("is_extracted_by_perfios", False)
    
        if st_identity.get('is_fraud'):
            # metadata fraud present
            fraud_list.append({
                'statement_id': statement_id,
                'fraud_type': st_identity.get('fraud_type'),
                'account_id': account_id,
                'transaction_hash': None,
                'fraud_category': 'metadata'
            })
    fraud_list.extend(get_non_metadata_frauds(account_id))
    return fraud_list, is_extracted_by_perfios


def get_recurring_raw_from_ddb(account_id):
    items = collect_results(bank_connect_recurring_table.query,
                            {'KeyConditionExpression': Key('account_id').eq(account_id)})
    if len(items) == 0:
        return dict()
    item = items[0]
    txn_data = dict()
    s3_object_key = item.get('s3_object_key')
    if s3_object_key:
        obj = s3_resource.Object(BANK_CONNECT_DDB_FAILOVER_BUCKET, s3_object_key)
        item_data = obj.get()['Body'].read().decode('utf-8')
        txn_data = json.loads(item_data)
    else:
        txn_data = json.loads(item.get('item_data', '{}'))
    return txn_data

def get_recurring_transactions_list_from_ddb(account_id):
    txn_data = get_recurring_raw_from_ddb(account_id)
    if not txn_data:
        return [], []

    recurring_debit_txns = txn_data.get('recurring_debit_transactions', [])
    recurring_credit_txns = txn_data.get('recurring_credit_transactions', [])

    recurring_debit = list()
    for item in recurring_debit_txns:
        transactions = item.get('transactions')
        median = sorted([txn['amount'] for txn in transactions])[
            len(transactions) // 2]
        account_id = transactions[0].get('account_id')
        start_date = transactions[0].get('date')
        end_date = transactions[-1].get('date')

        recurring_debit.append(
            {'account_id': account_id,
             'transaction_channel': transactions[0].get(
                 'transaction_channel', '').upper(),
             'clean_transaction_note': transactions[0].get(
                 'clean_transaction_note', '').upper(),
             'median': median, 'start_date': start_date, 'end_date': end_date,
             'transactions': transactions})

    recurring_credit = list()
    for item in recurring_credit_txns:
        transactions = item.get('transactions')
        median = sorted([txn['amount'] for txn in transactions])[
            len(transactions) // 2]
        account_id = transactions[0].get('account_id')
        start_date = transactions[0].get('date')
        end_date = transactions[-1].get('date')

        recurring_credit.append(
            {'account_id': account_id,
             'transaction_channel': transactions[0].get(
                 'transaction_channel', '').upper(),
             'clean_transaction_note': transactions[0].get(
                 'clean_transaction_note', '').upper(),
             'median': median, 'start_date': start_date, 'end_date': end_date,
             'transactions': transactions})

    return recurring_debit, recurring_credit

# TODO to be depricated, not used anywhere
def get_dashboard_metrics(entity_id, account_id):
    account_transactions, hash_dict = get_transactions_for_account(entity_id, account_id)
    return get_entity_metrics(account_transactions)

def get_all_months(entity_id):
    date_ranges = list()

    for acc in get_accounts_for_entity(entity_id):
        st_ids = acc.get('item_data').get('statements')
        for st_id in st_ids:
            date_range = get_complete_identity_for_statement(st_id).get('date_range')
            if date_range:
                date_ranges.append(date_range)

    return get_months_from_periods(date_ranges)

# def get_missing_date_range_on_extraction(month_list):


def get_account_wise_months(entity_id, account_id, is_missing_date_range_enabled = False, session_date_range = None):
    account_wise_ranges = {}
    account_list = []
    if account_id:
        temp_account = get_account_for_entity(entity_id, account_id)
        if temp_account:
            account_list = [temp_account]
    else:
        account_list = get_accounts_for_entity(entity_id)

    if not session_date_range or session_date_range.get('from_date', None) is None or session_date_range.get('to_date', None) is None:
        session_date_range = None
        is_missing_date_range_enabled = False

    for acc in account_list:
        month_list = []
        extracted_date_range_list = []
        missing_date_range_on_extraction = {}
        months_on_extraction = []
        account_id = acc.get('account_id')
        st_ids = acc.get('item_data').get('statements')
        for st_id in st_ids:
            st_identity = get_complete_identity_for_statement(st_id)
            
            date_range = st_identity.get('date_range', None)
            extracted_date_range = st_identity.get('extracted_date_range', None)
            if date_range and date_range.get('from_date', None) and date_range.get('to_date', None):
                month_list.append(date_range)

            if extracted_date_range and extracted_date_range.get('from_date', None) and extracted_date_range.get('to_date', None):
                extracted_date_range_list.append(extracted_date_range)
            else:
                is_missing_date_range_enabled = False

        missing_date_range_on_trxn = get_missing_date_range_on_extraction(month_list, session_date_range)
        if is_missing_date_range_enabled:
            missing_date_range_on_extraction = get_missing_date_range_on_extraction(extracted_date_range_list, session_date_range)

        months_on_txn = get_months_from_periods(month_list)
        months_on_extraction = get_months_from_periods(extracted_date_range_list)
        missing_months_on_extraction, _ = is_month_missing(months_on_extraction, deepcopy(session_date_range))

        # in future we can save it, if required
        # update_missing_months_on_extraction(entity_id, account_id, missing_date_range_on_extraction, missing_months_on_extraction, months_on_extraction, months_on_txn)

        account_wise_ranges[account_id] = {
            "bank" : acc.get('item_data').get('bank'),
            "months_on_txn": months_on_txn,
            "missing_date_range_on_trxn": missing_date_range_on_trxn,
            "months_on_extraction" : months_on_extraction,
            "missing_date_range_on_extraction": missing_date_range_on_extraction,
            "missing_months_on_extraction": missing_months_on_extraction
        }
    return account_wise_ranges

def is_only_last_statement_in_processing_state(entity_id, current_statement_id):
    accounts = get_accounts_for_entity(entity_id)
    for account in accounts:
        statement_ids = account.get('item_data', {}).get('statements')
        for statement_id in statement_ids:
            statement_items = bank_connect_statement_table.query(KeyConditionExpression=Key('statement_id').eq(statement_id))
            if statement_items.get('Count') == 0:
                continue
            statement_item = statement_items.get('Items')[0]
            processing_status = statement_item.get('processing_status')
            if current_statement_id == statement_id:
                continue
            elif processing_status not in ['completed', 'failed']:
                return False
    return True

# Deciding factor to cache insights, only used for session_flow after is_processing is requested
def is_only_last_account_in_processing_state(entity_id, current_account_id):
    accounts = get_accounts_for_entity(entity_id)
    for account in accounts:
        account_id = account['account_id']
        if account_id==current_account_id:
            continue
        statement_ids = account.get('item_data', {}).get('statements')
        for statement_id in statement_ids:
            statement_items = bank_connect_statement_table.query(KeyConditionExpression=Key('statement_id').eq(statement_id))
            if statement_items.get('Count') == 0:
                continue
            statement_item = statement_items.get('Items')[0]
            processing_status = statement_item.get('processing_status')
            if processing_status not in ['completed', 'failed']:
                return False
    return True

def is_all_statement_processing_completed(entity_id=None):
    """
    This method returns:
        False: If any statement is in processing state
        True: No statement is in processing state
    """
    accounts = get_accounts_for_entity(entity_id)
    for account in accounts:
        statement_ids = account.get('item_data').get('statements')
        for statement_id in statement_ids:
            statement_items = bank_connect_statement_table.query(KeyConditionExpression=Key('statement_id').eq(statement_id))
            if statement_items.get('Count') == 0:
                continue
            statement_item = statement_items.get('Items')[0]
            different_statuses = [statement_item.get('identity_status'), statement_item.get('transactions_status'), statement_item.get('processing_status')]
            for single_status in different_statuses:
                if single_status not in ['completed', 'failed']:
                    return False
    return True


def update_progress_on_dashboard(statement_id, payload, entity_id = None, fan_info = {}, session_accounts_status_response={}):
    if entity_id and fan_info not in [None, {}]:
        session_flow = fan_info['session_flow']
        if session_flow:
            all_statement_processing_completed = is_all_statement_processing_completed(entity_id)
            if all_statement_processing_completed:
                if session_accounts_status_response not in [None, {}]:
                    payload.update({'event_name': fan_info['event_name']})
                    payload.update({'session_accounts_status': session_accounts_status_response})
            nanonets_statements_in_processing, scanned_callback_payload = get_scanned_webhook_callback_payload(entity_id)
            if not nanonets_statements_in_processing:
                payload.update({
                    'nanonets_webhook_config': {
                        'send_webhook': True, 
                        'callback_payload': scanned_callback_payload
                    }
                })

    local_logging_context: LoggingContext = LoggingContext(entity_id=entity_id)
    local_logging_context.upsert(
        source="update_progress_on_dashboard",
        payload=payload,
        statement_id=statement_id
    )
    LAMBDA_LOGGER.info("Calling dashboard API /update_progress", extra=local_logging_context.store)

    url = '{}/bank-connect/v1/internal/{}/update_progress/'.format(
        DJANGO_BASE_URL, statement_id)

    headers = {
        'x-api-key': API_KEY,
        'Content-Type': "application/json",
    }
    #calling django
    payload = json.dumps(payload, default=str)
    call_api_with_session(url,"POST" ,payload, headers)

def generate_xlsx_report(account_id, transaction_list, identity_dict, salary_transactions, recurring_transactions, fraud_list, predictors, monthly_analysis, enriched_eod_balances, excel_report_version, country='IN', aggregated_workbook=None, workbook_num='', metadata={}, file_name=None, unadjusted_eod_balances={}, account_statement_metadata={}):
    """
    Generates the new excel report, uploads to s3 and returns the URL

    account_id -- account id
    transaction_list -- list of dictionary having transaction data
    identity_dict -- identity information
    salary_transactions -- list of salary transactions
    recurring_transactions -- dictionary with debit and credit recurring
    fraud_list -- list of frauds found
    """
    from library.excel_report.report_generator import create_xlsx_report
    try:
        file_name = 'account_report_{}.xlsx'.format(account_id) if not file_name else file_name
        file_path = '/tmp/{}'.format(file_name)
        overview_dict = create_xlsx_report(transaction_list, identity_dict, file_path, salary_transactions, recurring_transactions, fraud_list, predictors, monthly_analysis, enriched_eod_balances, unadjusted_eod_balances, excel_report_version, country, aggregated_workbook, workbook_num, account_statement_metadata=account_statement_metadata)
        
        if aggregated_workbook is None:
            metadata['created_at'] = str(datetime.datetime.now())
            s3_resource.Bucket(BANK_CONNECT_REPORTS_BUCKET).upload_file(file_path, file_name, ExtraArgs={'Metadata': metadata})
            
            if os.path.exists(file_path):
                os.remove(file_path)
            
            s3_path = s3.generate_presigned_url(
                    'get_object', 
                    Params={
                        'Bucket': BANK_CONNECT_REPORTS_BUCKET, 
                        'Key': file_name
                    })
            
            return s3_path
        else:
            return overview_dict
    except Exception as e:
        capture_exception(e)
        print("Report failed due to -> ", e)
    return None

def check_valid_transaction(transaction, fetch_all_transactions=False):
    amount = transaction.get("amount", None)
    balance = transaction.get("balance",None)
    date = transaction.get("date",None)
    is_in_session_date_range = transaction.get("is_in_session_date_range", True)
    if fetch_all_transactions:
        is_in_session_date_range = True
    if isinstance(amount, float) and amount != float("inf") and amount != float("-inf") \
        and isinstance(balance, float) and balance != float("inf") and balance != float("-inf") \
        and isinstance(date, str) and date != float("inf") and date != float("-inf"):
            return is_in_session_date_range
    return False

def help_denormalise_hash_statements(denormalised_dict, hashes, hash_dict):
    for h in hashes:
        statement_id_and_page_number = hash_dict[h]
        for statement_id, page_number in statement_id_and_page_number:
            if statement_id not in denormalised_dict:
                denormalised_dict[statement_id] = {}
            if page_number not in denormalised_dict[statement_id]:
                denormalised_dict[statement_id][page_number] = []
            denormalised_dict[statement_id][page_number].append(h)

def hashes_to_update_generalised(hashes_to_update_dict, hashes, update_dict, account_transactions, salary_calc_method_dict={}):
    for hash in hashes:
        if hash not in hashes_to_update_dict:
            hashes_to_update_dict[hash] = deepcopy(update_dict)
        else:
            for key, value in update_dict.items():
                hashes_to_update_dict[hash][key] = value
    
    for txn in account_transactions:
        if txn.get("hash") in hashes:
            if "merchant_category" in update_dict:
                txn["merchant_category"] = update_dict["merchant_category"]
            if "description" in update_dict:
                if update_dict["description"] == "":
                    txn["description"] = txn["description"] if "transfer" in txn["description"].lower() else ""
                else:    
                    txn["description"] = update_dict["description"]
            
            if "transaction_channel" in update_dict:
                txn["transaction_channel"] = update_dict["transaction_channel"]
                if update_dict["transaction_channel"] == 'salary':
                    txn['salary_calculation_method'] = salary_calc_method_dict.get(txn.get("hash"))
                    hashes_to_update_dict[txn.get("hash")]['salary_calculation_method'] = salary_calc_method_dict.get(txn.get("hash"))
                    
            if "category" in update_dict:
                txn["category"] = update_dict["category"]
            if "salary_confidence_percentage" in update_dict:
                txn["salary_confidence_percentage"] = update_dict["salary_confidence_percentage"]
            
            if "is_lender" in update_dict:
                txn["is_lender"] = update_dict["is_lender"]


def process_account_transactions(
    entity_id,
    account_id,
    account_transactions,
    hash_dict,
    bank_name,
    account_category,
    country="IN",
    local_logging_context: LoggingContext = None
):
    """
    this function updates bank connect enrichments based on post processing
    currently supported features : recurring_lender_transactions
    
    input (params): (entity_id: str, account_id: str, account_transactions: list(dict), hash_dict: dict)
    output : None

    """
    if not isinstance(local_logging_context, LoggingContext):
        local_logging_context: LoggingContext = LoggingContext(entity_id=entity_id)
    local_logging_context.upsert(
        source="process_account_transactions",
        account_id=account_id
    )
    LAMBDA_LOGGER.debug("Update BankConnect enrichments based on post processing", extra=local_logging_context.store)
    denormalised_statement_dict = {}
    transactions_df = pd.DataFrame(account_transactions)

    # these hashes should be marked as following : merchant_category: loans, description: lender_transaction
    lender_transaction_hashes = get_recurring_lender_debit_transactions(transactions_df)
    LAMBDA_LOGGER.debug(
        f"recurring lender transaction detected for account_id : {account_id}, "
        f"for following hashes : {lender_transaction_hashes}\n\n",
        extra=local_logging_context.store
    )

    auto_debit_payment_hashes, auto_debit_payment_bounce_hashes = update_bounce_transactions_for_account_transactions(
        transactions_df
    )
    LAMBDA_LOGGER.debug(
        f"auto debit payment bounce detected for account_id : {account_id}, "
        f"for following hashes : {auto_debit_payment_bounce_hashes}\n\n",
        extra=local_logging_context.store
    )

    refund_hash_set = mark_refund_on_basis_of_same_balance(transactions_df, auto_debit_payment_bounce_hashes)
    LAMBDA_LOGGER.debug(
        f"Refund transactions detected for account_id : {account_id}, for following hashes : {refund_hash_set}\n\n",
        extra=local_logging_context.store
    )

    reversal_hash_set = mark_reversal_on_basis_of_neg_balance(transactions_df, auto_debit_payment_bounce_hashes)
    LAMBDA_LOGGER.debug(
        f"Reversal transactions detected for account_id : {account_id}, for following hashes : {refund_hash_set}\n\n",
        extra=local_logging_context.store
    )

    print("account category received in process account transactions: ", account_category)
    cc_interest_hash_set = []
    if account_category == "overdraft":
        cc_interest_hash_set = update_transaction_channel_after_all_transactions(
            transactions_df,
            bank_name,
            account_category,
            country
        )
    LAMBDA_LOGGER.debug(
        f"cc interest transactions detected for account_id : "
        f"{account_id}, for following hashes: {cc_interest_hash_set}\n\n",
        extra=local_logging_context.store
    )

    help_denormalise_hash_statements(denormalised_statement_dict, lender_transaction_hashes, hash_dict)
    help_denormalise_hash_statements(denormalised_statement_dict, auto_debit_payment_hashes, hash_dict)
    help_denormalise_hash_statements(denormalised_statement_dict, auto_debit_payment_bounce_hashes, hash_dict)
    help_denormalise_hash_statements(denormalised_statement_dict, refund_hash_set, hash_dict)
    help_denormalise_hash_statements(denormalised_statement_dict, reversal_hash_set, hash_dict)
    help_denormalise_hash_statements(denormalised_statement_dict, cc_interest_hash_set, hash_dict)

    print("denormalised statements dict : ", denormalised_statement_dict)

    hashes_to_update = {}
    hashes_to_update_generalised(
        hashes_to_update, 
        lender_transaction_hashes, {
            "merchant_category": "loans",
            "description": "lender_transaction",
            "category": "Loan"
        },
        account_transactions
    )
    hashes_to_update_generalised(
        hashes_to_update,
        auto_debit_payment_hashes, {
            "transaction_channel": "auto_debit_payment",
            "transaction_channel_regex": "auto_debit_payment_account_optimisation"
        },
        account_transactions
    )
    hashes_to_update_generalised(
        hashes_to_update,
        auto_debit_payment_bounce_hashes, {
            "transaction_channel": "auto_debit_payment_bounce",
            "category": "Bounced I/W ECS",
            "transaction_channel_regex": "auto_debit_payment_bounce_account_optimisation"
        },
        account_transactions
    )
    hashes_to_update_generalised(
        hashes_to_update,
        refund_hash_set, {
            "transaction_channel": "refund",
            "category": "Refund"
        },
        account_transactions
    )
    hashes_to_update_generalised(
        hashes_to_update,
        reversal_hash_set, {
            "transaction_channel": "reversal",
            "category": "Reversal"
        },
        account_transactions
    )
    hashes_to_update_generalised(
        hashes_to_update,
        cc_interest_hash_set, {
            "transaction_channel": "cc_interest",
            "category": "Interest Charges"
        },
        account_transactions
    )
    print("hashes to update : ", hashes_to_update)
    update_transactions_basis_statement_dict(denormalised_statement_dict, hashes_to_update)
    LAMBDA_LOGGER.info(
        "Post processing on BankConnect enrichments completed",
        extra=local_logging_context.store
    )


def update_transactions_basis_statement_dict(denormalised_statement_dict, hashes_to_update):
    t1 = time.time()
    for statements_id, page_level_hashes in denormalised_statement_dict.items():
        for page_number in page_level_hashes:
            txns = get_transactions_for_statement_page(statements_id, page_number)
            updated_key = False
            updated_transaction = []
            for txn in txns:
                if txn['hash'] in hashes_to_update:
                    updated_key = True
                    to_update_vals = hashes_to_update[txn['hash']]
                    if "merchant_category" in to_update_vals:
                        txn["merchant_category"] = to_update_vals["merchant_category"]
                    if "description" in to_update_vals:
                        txn["description"] = to_update_vals["description"]
                    if "transaction_channel" in to_update_vals:
                        txn["transaction_channel"] = to_update_vals["transaction_channel"]
                    if "transaction_channel_regex" in to_update_vals:
                        txn["transaction_channel_regex"] = to_update_vals["transaction_channel_regex"]
                    if "category" in to_update_vals:
                        txn["category"] = to_update_vals["category"]
                    if "salary_confidence_percentage" in to_update_vals:
                        txn["salary_confidence_percentage"] = to_update_vals["salary_confidence_percentage"]
                    if "salary_calculation_method" in to_update_vals:
                        txn["salary_calculation_method"] = to_update_vals["salary_calculation_method"]
                    if "is_lender" in to_update_vals:
                        txn["is_lender"] = to_update_vals["is_lender"]
                updated_transaction.append(txn)
            
            if updated_key:
                # updating transactions at this page for this account
                update_transactions_on_page(statements_id, int(page_number), updated_transaction)
                print(f"updated transactions for page number: {page_number} of statement_id: {statements_id}")
    t2 = time.time()
    print("updating block took : ", (t2-t1))


def get_upload_status(entity_id, is_missing_date_range_enabled=True, session_date_range=None, to_reject_account=False,
                      need_account_status=False, acceptance_criteria=[], date_range_approval_criteria=0,
                      accept_anything=False):
    response = {
        "accounts": []
    }

    accounts = get_accounts_for_entity(entity_id, to_reject_account)
    print("These are the accounts = {}".format(accounts))
    session_date_range = convert_date_range_to_datetime(session_date_range, "%d/%m/%Y")
    for account in accounts:
        month_list = []
        extracted_date_range_list = []
        account_status = "PARTIAL"
        account_id = account.get("account_id")
        account_number = account.get("item_data", {}).get("account_number", "")

        account_data = {
            "account_id": account_id,
            "account_number": account_number,
            "bank_name": account.get("item_data", {}).get("bank", ""),
            "created_at": str(datetime.datetime.utcfromtimestamp(int(account["created_at"]/1000000000))) if account.get("created_at", "") else "",
            "last_updated_at": str(datetime.datetime.utcfromtimestamp(int(account["updated_at"]/1000000000))) if account.get("updated_at", "") else "",
        }

        statement_ids = account.get('item_data').get('statements')
        print("These are the statement data = {}".format(statement_ids))
        account_data["statements"] = statement_ids
        for statement_id in statement_ids:
            statement_items = bank_connect_statement_table.query(
                KeyConditionExpression=Key('statement_id').eq(statement_id))
            print("This is the statement items : {}".format(statement_items))
            if statement_items.get('Count') == 0:
                continue
            statement_item = statement_items.get('Items')[0]

            if not statement_item.get("message"):
                statement_identity = bank_connect_identity_table.query(
                    KeyConditionExpression=Key('statement_id').eq(statement_id),
                    ConsistentRead=True
                )

                items = statement_identity['Items']

                if len(items) > 0:
                    st_identity_item = items[0].get('item_data', dict())
                    date_range = st_identity_item.get('date_range', None)
                    extracted_date_range = st_identity_item.get('extracted_date_range', None)

                    if date_range and date_range.get('from_date', None) and date_range.get('to_date', None):
                        month_list.append(date_range)

                    if extracted_date_range and extracted_date_range.get('from_date',
                                                                         None) and extracted_date_range.get(
                            'to_date', None):
                        extracted_date_range_list.append(extracted_date_range)
                    else:
                        is_missing_date_range_enabled = False
                        account_status = "PARTIAL"

        if is_missing_date_range_enabled:
            missing_date_range_on_extraction = get_missing_date_range_on_extraction(extracted_date_range_list,
                                                                                    session_date_range)
            account_status = "PARTIAL" if missing_date_range_on_extraction else "COMPLETED"

        if need_account_status:
            all_account_months = get_account_wise_months(entity_id, None, is_missing_date_range_enabled,
                                                         session_date_range)
            account_months = all_account_months.get(account_id, {})
            months_on_txn = account_months.get('months_on_txn', [])
            missing_date_range_on_extraction = account_months.get('missing_date_range_on_extraction', {})
            missing_months_on_extraction = account_months.get('missing_months_on_extraction', [])
            missing_months_on_txn, session_months = is_month_missing(months_on_txn, deepcopy(session_date_range))

            first_and_last_months = []
            if len(session_months) > 0:
                if session_months[0] in missing_months_on_txn:
                    first_and_last_months.append(session_months[0])
                if session_months[-1] in missing_months_on_txn:
                    first_and_last_months.append(session_months[-1])

            account_error_details = get_account_errors_from_account_details(entity_id, account_id, account_number,
                                                                            accept_anything, missing_months_on_extraction,
                                                                            missing_months_on_txn, acceptance_criteria,
                                                                            missing_date_range_on_extraction,
                                                                            date_range_approval_criteria, months_on_txn,
                                                                            first_and_last_months, is_missing_date_range_enabled)
            # print("This is the account_error_details = {}".format(account_error_details))
            account_status = account_error_details.get("account_status", account_status)
            account_data["error_message"] = account_error_details.get("error_message", "")
            account_data["error_code"] = account_error_details.get("error_code", "")

        account_data["account_status"] = account_status
        account_data["months"] = get_months_from_periods(extracted_date_range_list)
        account_data["missing_data"] = account.get("item_data", {}).get("missing_data", [])
        response["accounts"].append(account_data)

        # print("This is the final response = {}".format(response))

    return response


def get_account_errors_from_account_details(entity_id, account_id, account_number, accept_anything,
                                            missing_months_on_extraction, missing_months_on_txn, acceptance_criteria,
                                            missing_date_range_on_extraction, date_range_approval_criteria,
                                            months_on_txn, first_and_last_months, is_missing_date_range_enabled):

    resp = {
        "account_status": "FAILED",
        "error_code": "",
        "error_message": ""
    }
    existing_fraud_with_precedence = (None, 0)
    data_to_fed = ""

    if account_number in [None, '']:
        existing_fraud_with_precedence = ("account_number_missing", 0)

    _, frauds = get_fraud_for_account(entity_id, account_id)
    # print("This is the frauds data received = {}".format(frauds))
    for fraud in frauds:
        fraud_type = fraud.get('fraud_type', None)
        # print("This is the fraud type received = {}".format(fraud_type))
        if fraud_type:
            if existing_fraud_with_precedence[1] < FRAUD_TYPE_PRECEDENCE_MAPPING.get(fraud_type, 0):
                existing_fraud_with_precedence = (fraud_type, FRAUD_TYPE_PRECEDENCE_MAPPING[fraud_type])

    if not accept_anything and len(missing_months_on_extraction) > 0 and len(
            missing_months_on_txn) > 0 and 'missing_upload_months' in acceptance_criteria:
        data_to_fed = ', '.join(change_date_format(missing_months_on_extraction, "%Y-%m", "%b %Y"))
        existing_fraud_with_precedence = ("incomplete_months_upload", 0)

    elif not accept_anything and is_missing_date_range_enabled and is_missing_dates(missing_date_range_on_extraction,
                                                                                    date_range_approval_criteria) and 'missing_upload_date_range' in acceptance_criteria:
        data_to_fed = json.dumps(missing_date_range_on_extraction)
        existing_fraud_with_precedence = ("incomplete_dates_upload", 0)

    elif not accept_anything and missing_months_on_txn not in [None,
                                                               []] and 'atleast_one_transaction_permonth' in acceptance_criteria:
        data_to_fed = ', '.join(change_date_format(missing_months_on_txn, "%Y-%m", "%b %Y"))
        existing_fraud_with_precedence = ("incomplete_months", 0)

    elif len(months_on_txn) == 0 and 'atleast_one_transaction' in acceptance_criteria:
        existing_fraud_with_precedence = ("no_transactions", 0)

    elif not accept_anything and 'atleast_one_transaction_in_start_and_end_months' in acceptance_criteria and len(
            first_and_last_months) > 0:
        data_to_fed = ', '.join(change_date_format(first_and_last_months, "%Y-%m", "%b %Y"))
        existing_fraud_with_precedence = ("incomplete_months", 0)

    # print("This is the existing_fraud_with_precedence = {}".format(existing_fraud_with_precedence))
    if existing_fraud_with_precedence[0] is not None:
        error_details = FRAUD_TO_ERROR_MAPPING.get(existing_fraud_with_precedence[0], FRAUD_TO_ERROR_MAPPING["default"])
        resp["error_code"] = error_details["error_code"]
        resp["error_message"] = error_details["error_message"]

        if error_details.get("need_extra_data_for_message"):
            resp["error_message"] = resp["error_message"].format(data_to_fed)
    else:
        resp["account_status"] = "COMPLETE"

    # print("This is the response veing sent back = {}".format(resp))

    return resp


def get_fraud_for_account(entity_id, account_id, to_reject_account=False):
    accounts = list()
    if account_id:
        temp_account = get_account_for_entity(entity_id, account_id, to_reject_account)
        if temp_account:
            accounts = [temp_account]
    else:
        accounts = get_accounts_for_entity(entity_id, to_reject_account)

    fraud_statements = list()  # stores statement ids
    fraud_reasons = list()  # stores fraud dictionary

    for account in accounts:
        account_dict = account.get('item_data')
        statements = account_dict.get('statements')
        account_id = account_dict.get('account_id')
        is_od_account = account_dict.get('is_od_account', False)
        account_category = account_dict.get('account_category', False)
        input_is_od_account = account_dict.get('input_is_od_account', False)
        input_account_category = account_dict.get('input_account_category', False)
        account_category, _ = get_final_account_category(account_category, is_od_account, input_account_category, input_is_od_account)

        for stmt_id in statements:
            identity = get_complete_identity_for_statement(stmt_id)

            # masking every metadata fraud with author_fraud
            metadata_frauds = [_[0] for _ in fraud_category.items() if _[1]=='metadata']
            if identity.get("fraud_type", None) in metadata_frauds:
                identity["fraud_type"] = "author_fraud"

            if identity.get('is_fraud'):
                # metadata fraud present
                fraud_statements.append(stmt_id)
                fraud_reasons.append({
                    'statement_id': stmt_id,
                    'fraud_type': identity.get('fraud_type'),
                    'account_id': account_id,
                    'transaction_hash': None,
                    'fraud_category': 'metadata'
                })
        # get non metadata frauds in required format
        disparities = get_non_metadata_frauds(account_id)

        for disparity in disparities:
            fraud_type = disparity.get('fraud_type', None)
            if account_category in ["CURRENT", "corporate", "overdraft"] and fraud_type == 'negative_balance':
                continue
            
            fraud_statement_id = disparity.get('statement_id')
            if fraud_statement_id and (fraud_statement_id not in fraud_statements):
                fraud_statements.append(fraud_statement_id)

            fraud_reasons.append(disparity)
        
    return fraud_statements, fraud_reasons

def get_scanned_webhook_callback_payload(entity_id):
    '''
        Context: 
            If any statement is in processing_state with a message 'Scanned Statement - Raised for manual review'.
            It means, that statement requires manual intervention and is in under review.
            If any statement is in processing_state with a null message, it means statement is actually in processing
            and it can either goes into completed, failed, under review (processing_status in processing with manual review message)
        What we are trying to achieve:
            We have to check all the statements processing status and send the callback_payload for session accounts under review or failed or completed accordingly:
                1. Every statement should be in completed/failed/under review and not be in processing. If any one found in processing, return.
                2. For each account, iterate over statements and if none of the statement is in processing_state check for the following conditions:
                    a. If any one statement is in under review. Account is under review.
                    b. If none of the statements is in under review, but any one statement is completed. Account is completed.
                    c. If none of the statments is in under review or completed, but any one statement is failed. Account is failed.
    '''
    callback_payload = []
    accounts = get_accounts_for_entity(entity_id)
    for account in accounts:
        account_item_data = account.get('item_data', {})
        account_status = {
            "account_id" : account_item_data["account_id"],
            "bank_name": account_item_data["bank"],
            "error_code" : None,
            "error_message": None,
            "account_status" : None,
            "statements": []
        }
        statement_ids = account_item_data.get('statements', [])

        account_statement_statuses = {
            'under_review': 0,
            'completed': 0,
            'failed': 0
        }
        for statement_id in statement_ids:
            statement_item = get_statement_table_data(statement_id)

            # Ignore those statements which are not extracted by nanonets
            if not statement_item.get('is_extracted_by_nanonets'):
                continue

            if statement_item.get('processing_status')=='processing':
                if statement_item.get('message')=='Scanned Statement - Raised for manual review':
                    account_statement_statuses['under_review'] += 1
                else: # Return if any one statement found in procecssing state.
                    return True, []

            if statement_item.get('processing_status')=='completed':
                account_statement_statuses['completed'] += 1

            if statement_item.get('processing_status')=='failed':
                account_statement_statuses['failed'] += 1
            
            account_status['statements'].append(statement_id)

        if account_statement_statuses['under_review']:
            account_status['account_status'] = 'Raised for manual review'
        elif account_statement_statuses['completed']:
            account_status['account_status'] = 'completed'
        elif account_statement_statuses['failed']:
            account_status['account_status'] = 'failed'
        if account_status['account_status']:
            callback_payload.append(account_status)

    # Do not send webhook in non nanonets statements or when callback payload is an empty list
    if not callback_payload:
        return True, []

    return False, callback_payload

def transform_identity(item_data):
    # !!!  from_date and to_date gets overridden to transaction date range upon transaction extraction
    from_date = item_data.get("date_range", dict()).get("M", dict()).get("from_date", dict()).get("S", None)
    to_date = item_data.get("date_range", dict()).get("M", dict()).get("to_date", dict()).get("S", None)

    # !!!  extracted dates are from identity extraction and are never changed
    extracted_from_date = item_data.get("extracted_date_range", dict()).get("M", dict()).get("from_date", dict()).get("S", None)
    extracted_to_date = item_data.get("extracted_date_range", dict()).get("M", dict()).get("to_date", dict()).get("S", None)

    # identity data
    # TODO: Clean the dictionary references
    account_number = item_data.get("identity", dict()).get("M", dict()).get("account_number", dict()).get("S", None)
    address = item_data.get("identity", dict()).get("M", dict()).get("address", dict()).get("S", None)
    account_id = item_data.get("identity", dict()).get("M", dict()).get("account_id", dict()).get("S", None)
    name = item_data.get("identity", dict()).get("M", dict()).get("name", dict()).get("S", None)
    account_category = item_data.get("identity", dict()).get("M", dict()).get("account_category", dict()).get("S", None)
    account_category_v2 = item_data.get("identity", dict()).get("M", dict()).get("account_category_v2", dict()).get("S", None)
    raw_account_category = item_data.get("identity", dict()).get("M", dict()).get("raw_account_category", dict()).get("S", None)
    perfios_account_category = item_data.get("identity", dict()).get("M", dict()).get("perfios_account_category", dict()).get("S", None)
    input_account_category = item_data.get("identity", dict()).get("M", dict()).get("input_account_category", dict()).get("S", None)
    input_is_od_account = item_data.get("identity", dict()).get("M", dict()).get("input_is_od_account", dict()).get("BOOL", None)
    is_od_account = item_data.get("identity", dict()).get("M", dict()).get("is_od_account", dict()).get("BOOL", None)
    credit_limit = item_data.get("identity", dict()).get("M", dict()).get("credit_limit", dict()).get("N", None)
    od_limit = item_data.get("identity", dict()).get("M", dict()).get("od_limit", dict()).get("N", None)
    pan_number = item_data.get("identity", dict()).get("M", dict()).get("pan_number", dict()).get("S", None)
    dob = item_data.get("identity", dict()).get("M", dict()).get("dob", dict()).get("S", None)
    account_status = item_data.get("identity", dict()).get("M", dict()).get("account_status", dict()).get("S", None)
    holder_type = item_data.get("identity", dict()).get("M", dict()).get("holder_type", dict()).get("S", None)

    email = item_data.get("identity", dict()).get("M", dict()).get("email", dict()).get("S", None)
    phone_number = item_data.get("identity", dict()).get("M", dict()).get("phone_number", dict()).get("S", None)
    ifsc = item_data.get("identity", dict()).get("M", dict()).get("ifsc", dict()).get("S", None)
    micr = item_data.get("identity", dict()).get("M", dict()).get("micr", dict()).get("S", None)
    is_fraud = item_data.get("is_fraud", dict()).get("BOOL", False)
    fraud_type = item_data.get("fraud_type", dict()).get("S", None)
    page_count = item_data.get("page_count", dict()).get("N", 0)
    od_limit_input_by_customer = item_data.get("od_limit_input_by_customer",dict()).get("BOOL",None)
    updated_od_paramters_by = item_data.get('identity', dict()).get('M', dict()).get('updated_od_paramters_by', dict()).get("S", None)
    od_metadata = item_data.get('identity', dict()).get('M', dict()).get('od_metadata', dict()).get('M', dict())
    od_metadata = deseralize_od_metadata(od_metadata)

    bank_name = item_data.get("identity", dict()).get("M", dict()).get("bank_name", dict()).get("S", None)
    all_keywords_present = item_data.get("keywords", dict()).get("M", dict()).get("all_present", dict()).get("BOOL", False)
    amount_keyword_present = item_data.get("keywords", dict()).get("M", dict()).get("amount_present", dict()).get("BOOL", False)
    balance_keyword_present = item_data.get("keywords", dict()).get("M", dict()).get("balance_present", dict()).get("BOOL", False)
    date_keyword_present = item_data.get("keywords", dict()).get("M", dict()).get("date_present", dict()).get("BOOL", False)
    keywords_in_line = item_data.get("keywords_in_line", dict()).get("BOOL", False)
    opening_date = item_data.get("opening_date", dict()).get("S", None)
    opening_bal = item_data.get("opening_bal", dict()).get("S", None)
    closing_bal = item_data.get("closing_bal", dict()).get("S", None)
    country_code = item_data.get("country_code", dict()).get("S", None)
    currency_code = item_data.get("currency_code", dict()).get("S", None)
    preshared_names = item_data.get("preshared_names", dict()).get("L", None)
    preshared_names = deseralize_preshared_names(preshared_names)
    joint_account_holders = item_data.get("joint_account_holders", dict()).get("L", None)
    joint_account_holders = deseralize_preshared_names(joint_account_holders)
    is_inconsistent = item_data.get("is_inconsistent", dict()).get("BOOL", False)
    inconsistent_hash = item_data.get("inconsistent_hash", dict()).get("S", None)

    
    # getting template_data
    templates_used = item_data.get("templates_used", dict()).get("M", dict())
    account_number_template_uuid = templates_used.get("account_number_template_uuid", dict()).get("S", None)
    name_template_uuid = templates_used.get("name_template_uuid", dict()).get("S", None)
    address_template_uuid = templates_used.get("address_template_uuid", dict()).get("S", None)
    ifsc_template_uuid = templates_used.get("ifsc_template_uuid", dict()).get("S", None)
    micr_template_uuid = templates_used.get("micr_template_uuid", dict()).get("S", None)
    currency_template_uuid = templates_used.get("currency_template_uuid", dict()).get("S", None)
    account_category_template_uuid = templates_used.get("account_category_template_uuid", dict()).get("S", None)
    credit_limit_template_uuid = templates_used.get("credit_limit_template_uuid", dict()).get("S", None)
    od_limit_template_uuid = templates_used.get("od_limit_template_uuid", dict()).get("S", None)
    od_account_template_uuid = templates_used.get("od_account_template_uuid", dict()).get("S", None)
    date_range_template_uuid = templates_used.get("date_range_template_uuid", dict()).get("S", None)
    opening_date_template_uuid = templates_used.get("opening_date_template_uuid", dict()).get("S", None)
    opening_bal_template_uuid = templates_used.get("opening_bal_template_uuid", dict()).get("S", None)
    closing_bal_template_uuid = templates_used.get("closing_bal_template_uuid", dict()).get("S", None)

    is_ocr_extracted = item_data.get("is_ocr_extracted", dict()).get("BOOL", False)
    metadata_analysis = item_data.get("metadata_analysis", dict()).get("M", dict())
    name_match_analysis = deseralize_metadata_analysis(metadata_analysis)
    reject_reason = item_data.get("reject_reason", dict()).get("S", None)
    if credit_limit is not None:
        try:
            credit_limit = round(float(credit_limit))
        except Exception as _:
            pass
        
    transformed_identity = {
        "from_date": from_date if from_date else None,
        "to_date": to_date if to_date else None,
        "extracted_from_date": extracted_from_date if extracted_from_date else None,
        "extracted_to_date": extracted_to_date if extracted_to_date else None,
        "account_number": account_number,
        "address": address,
        "account_id": account_id,
        "name": name,
        "account_category": account_category if account_category else None,
        "input_account_category": input_account_category,
        "input_is_od_account": input_is_od_account,
        "is_od_account": is_od_account,
        "credit_limit": credit_limit,
        "od_limit": od_limit,
        "ifsc": ifsc,
        "micr": micr,
        "pan_number": pan_number,
        "dob": dob,
        "account_status": account_status,
        "holder_type": holder_type,
        "email": email,
        "phone_number": phone_number,
        "is_fraud": is_fraud,
        "fraud_type": fraud_type,
        "page_count": page_count,
        "od_limit_input_by_customer": od_limit_input_by_customer,
        "perfios_account_category": perfios_account_category,
        "raw_account_category": raw_account_category,
        "updated_od_paramters_by": updated_od_paramters_by,
        "od_metadata": od_metadata,
        "bank_name": bank_name,
        "country_code": country_code,
        "currency_code": currency_code,
        "opening_date": opening_date,
        "opening_bal": opening_bal,
        "closing_bal": closing_bal,
        "keywords_in_line": keywords_in_line,
        "all_keywords_present": all_keywords_present,
        "amount_keyword_present": amount_keyword_present,
        "balance_keyword_present": balance_keyword_present,
        "date_keyword_present": date_keyword_present,
        "account_number_template_uuid": account_number_template_uuid,
        "name_template_uuid": name_template_uuid,
        "address_template_uuid": address_template_uuid,
        "ifsc_template_uuid": ifsc_template_uuid,
        "micr_template_uuid": micr_template_uuid,
        "currency_template_uuid": currency_template_uuid,
        "account_category_template_uuid": account_category_template_uuid,
        "credit_limit_template_uuid": credit_limit_template_uuid,
        "od_limit_template_uuid": od_limit_template_uuid,
        "od_account_template_uuid": od_account_template_uuid,
        "date_range_template_uuid": date_range_template_uuid,
        "opening_date_template_uuid": opening_date_template_uuid,
        "opening_bal_template_uuid": opening_bal_template_uuid,
        "closing_bal_template_uuid": closing_bal_template_uuid,
        "preshared_names": preshared_names,
        "is_ocr_extracted": is_ocr_extracted,
        "name_match_analysis": name_match_analysis,
        "is_inconsistent": is_inconsistent,
        "inconsistent_hash": inconsistent_hash,
        "account_category_v2": account_category_v2,
        "joint_account_holders": joint_account_holders,
        "reject_reason": reject_reason
    }
    return transformed_identity

def prepare_identity_clickhouse_stream_data(identity_warehouse_data, identity):
    return_data = dict()

    serializer = TypeSerializer()
    identity = {key: serializer.serialize(value) for key, value in identity.items()}
    transformed_identity = transform_identity(identity)
    identity_rds_data = prepare_identity_rds_warehouse_data(identity_warehouse_data['entity_id'], identity_warehouse_data['statement_id'])

    data = {
        **identity_warehouse_data,
        **transformed_identity,
        **identity_rds_data
    }

    identity_stream_required_keys = [
        "org_id", "org_name", "link_id", "session_flow", "entity_id", "account_id", "statement_id", "bank_name", "name",
        "account_number", "address", "attempt_type", "is_processing_requested", "statement_status", "statement_created_at",
        "from_date", "to_date", "extracted_from_date", "extracted_to_date", "session_from_date", "session_to_date",
        "account_category", "input_account_category", "input_is_od_account", "is_od_account", "credit_limit", "od_limit",
        "ifsc", "micr", "page_count", "transaction_count", "is_ocr_extracted", "is_multi_account_statement", "child_statement_list",
        "parent_statement_id", "fraud_list", "preshared_names", "name_match_analysis", "od_limit_input_by_customer",
        "is_extracted_by_perfios", "perfios_account_category", "raw_account_category", "updated_od_paramters_by", "country_code",
        "currency_code", "od_metadata", "opening_date", "opening_bal", "closing_bal", "keywords_in_line", "all_keywords_present",
        "amount_keyword_present", "balance_keyword_present", "date_keyword_present", "pdf_hash", "logo_hash", "is_extracted", 
        "is_complete", "is_inconsistent", "inconsistent_hash", "account_number_template_uuid", "name_template_uuid", 
        "address_template_uuid", "ifsc_template_uuid", "micr_template_uuid", "currency_template_uuid","account_category_template_uuid", 
        "credit_limit_template_uuid", "od_limit_template_uuid", "od_account_template_uuid", "date_range_template_uuid", 
        "opening_date_template_uuid", "opening_bal_template_uuid", "closing_bal_template_uuid"
    ]

    for key in identity_stream_required_keys:
        if key=='account_category':
            return_data[key] = data[key] if data.get(key) else None
        elif key=='od_metadata':
            return_data[key] = json.dumps(data.get(key))
        elif key=='statement_created_at':
            return_data[key] = data[key].strftime("%Y-%m-%d %H:%M:%S.%f") if data.get(key) and isinstance(data[key], datetime.datetime) else None
        elif key in ['session_from_date', 'session_to_date']:
            try:
                return_data[key] = datetime.datetime.strptime(data[key], "%d/%m/%Y").strftime("%Y-%m-%d") if data.get(key) else None
            except Exception:
                return_data[key] = None     
        else:
            return_data[key] = data.get(key)

    timestamp = datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S.%f')
    return_data['created_at'] = timestamp
    return_data['updated_at'] = timestamp

    return return_data