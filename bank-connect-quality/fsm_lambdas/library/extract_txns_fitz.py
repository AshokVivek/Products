import time
import os
import json
import re
import warnings
import pandas as pd
import multiprocessing
from copy import deepcopy
from library.get_edges_test import get_df_graphical_lines, get_lines, all_text_check_for_pdf
from library.fitz_functions import read_pdf
from library.fraud import transaction_balance_check, optimise_transaction_type, remove_duplicate_transactions, correct_transactions_date, change_transaction_type
from library.statement_plumber import get_date_format, transaction_rows, map_correct_columns
from library.table import parse_table
from library.transaction_channel import get_transaction_channel
from library.transaction_description import get_transaction_description
from library.utils import remove_unicode, validate_amount, add_hash_to_transactions_df, update_transaction_channel_for_cheque_bounce, get_closing_balances, \
    get_opening_balances, convert_pandas_timestamp_to_date_string, match_compiled_regex, check_semi_date, date_regexes, print_on_condition, \
    format_hsbc_ocr_rows, sanitize_transaction_note, get_pages, remove_indusind_invalid_transactions, get_account_wise_transactions_dict, recombine_account_transctions, \
    log_data, process_mock_transaction, find_all_accounts_in_table
from library.complete_semi_dates import complete_semi_dates, complete_semi_dates_from_txn, populate_semi_date
from library.validations import complete_split_balance_from_next_line, format_stanchar_rows, populate_transaction_notes, populate_dates, mark_negative_balances, \
    populate_merge_flag, populate_debit_from_note, fix_bni_amount, format_ID_currency, sanitize_ubi_hidden_rows, fix_last_txn_type, \
    complete_transction, fix_karur_numericals, convert_date_to_date_format, fix_kotak_repeated_amount, fill_missing_balance, fix_canara_repeated_amount_in_first_txn, absolute_negative_debits, \
    get_balance_from_amount_dhanlaxmi, fix_balance_amount, fix_jpmorgan_amount_balance, fix_indusind_date, clean_id_amount_balance, convert_utc_time, fix_sahabat_sampoerna_amount_from_balance, \
    solve_ubi_null_balance_transaction_case, sinitize_trxn_columns_based_on_regex
from library.finvu_aa_inconsistency_removal import fix_yesbnk_inc_transactions, swap_inconsistent_trxns
from sentry_sdk import capture_exception
from fitz import PDF_REDACT_IMAGE_NONE, PDF_ENCRYPT_KEEP
from library.helpers.constants import HEADERS_OPENING_BALANCE, HEADERS_CLOSING_BALANCE, DEFAULT_BALANCE_FLOAT, DEFAULT_TIMESTAMP_UTC, CUT_FOOTER_REGEXES, CUT_HEADERS_PATTERNS, BANKS_WITH_TRANSACTIONS_SPLIT_ENABLED
from typing import Any, List, Tuple, Union
from datetime import datetime, timedelta
from library.utils import check_date, remove_next_page_data_after_footer, amount_to_float, get_first_transaction_type
from concurrent.futures import ProcessPoolExecutor, as_completed

from uuid import uuid4
import shutil

warnings.simplefilter(action = "ignore", category = FutureWarning)
pd.options.mode.chained_assignment = None


# regexes for amounts
amount_merge_regex1 = re.compile('(?s)(?i)([0-9\,\.\s]+.*[/(cr|dr/)]*).*')
amount_merge_regex2 = re.compile('([0-9\,\.\s]+).{,6}')
amount_merge_regex3 = re.compile('([0-9\,\.\s]+).{0,6}$')
remove_gibberish_reg = re.compile("[A-Za-z!@#$%^&*]")


def collect_results(table_f, qp):
    items = []
    while True:
        r = table_f(**qp)
        items.extend(r['Items'])
        lek = r.get('LastEvaluatedKey')
        if lek is None or lek == '':
            break
        qp['ExclusiveStartKey'] = lek
    return items

def get_tables_each_page_parallelised(parameter: dict):
    vertical_lines = parameter["vertical_lines"]
    horizontal_lines = parameter["horizontal_lines"]
    path = parameter["path"]
    password = parameter["password"]
    bank = parameter["bank"]
    page_number = parameter["page_number"]
    account_number = parameter["account_number"]
    opening_date = parameter["opening_date"]
    last_page_regex = parameter["last_page_regex"]
    page = parameter["page"]
    width = parameter["width"]
    height = parameter["height"]
    plumber_page_edges = parameter["plumber_page_edges"]
    account_delimiter_regex = parameter["account_delimiter_regex"]
    extract_multiple_accounts = parameter["extract_multiple_accounts"]
    image_flag = parameter["image_flag"]
    range_involved = parameter["range_involved"]
    each_parameter = parameter["each_parameter"]
    transaction_regex_flag = parameter["transaction_regex_flag"]
    originally_extracted_for_page = parameter["originally_extracted_for_page"]
    unused_raw_txn_rows_from_second_page = parameter["unused_raw_txn_rows_from_second_page"]
    template_date_format = parameter["template_date_format"]
    country = parameter["country"]
    note_after_date = parameter["note_after_date"]
    identity = parameter["identity"]
    transaction_note_regex_list = parameter["transaction_note_regex_list"]
    current_template_uuid = parameter["current_template_uuid"]
    debug_prints_enabled = parameter["debug_prints_enabled"]
    opening_closing_date_dict = parameter["opening_closing_date_dict"]
    session_date_range = parameter["session_date_range"]
    account_category = parameter["account_category"]
    round_off_coordinates = parameter.get("round_off_coordinates", False)
    LOGGER = parameter["LOGGER"]
    local_logging_context = parameter["local_logging_context"]

    # log_data(message="Get Tables Each Page parallelised Invoked", local_logging_context=local_logging_context, log_type="info", LOGGER=LOGGER)
    
    textract_table = parameter.get("textract_table")
    textract_extracted = False
    
    removed_opening_balance_date, removed_closing_balance_date = None, None

    opened_now = False
    if page is None and not textract_table:
        doc = read_pdf(path, password)
        page = doc[page_number]
        opened_now = True
    
    if textract_table:
        tables = textract_table
        textract_extracted = True
        last_page = None
    else:
        try:
            if vertical_lines == True:
                if horizontal_lines in [True, "Text"]:
                    tables, last_page = get_df_graphical_lines(path, password, page_number, horizontal=horizontal_lines, \
                                vertical=True, inconsistent_regexes=last_page_regex, match_field=account_number, bank=bank, \
                                page=page, plumber_page_edges=plumber_page_edges , account_delimiter_regex=account_delimiter_regex, \
                                extract_multiple_accounts=extract_multiple_accounts)
                else:
                    horizontal_lines, vertical_lines = get_lines(path, password, page_number, horizontal=False, vertical=True, page=page, plumber_page_edges=plumber_page_edges)
                    vertical_lines = vertical_lines[1:-1] if len(vertical_lines)>0 else vertical_lines
                    vertical_lines = [int(_) for _ in vertical_lines]
                    tables, list_of_y, last_page = parse_table(page, [0, 0, width, height], columns=vertical_lines, image_flag=image_flag, range_involved=range_involved, inconsistent_regexes = last_page_regex, match_field = account_number, account_delimiter_regex=account_delimiter_regex, extract_multiple_accounts=extract_multiple_accounts)
            elif horizontal_lines in [True, "Text"]:
                # print("Entering horizontal with vertical lines")
                tables, last_page = get_df_graphical_lines(path, password, page_number, horizontal=horizontal_lines, vertical=vertical_lines, inconsistent_regexes=last_page_regex, match_field=account_number, bank=bank, page=page, plumber_page_edges=plumber_page_edges, account_delimiter_regex=account_delimiter_regex, extract_multiple_accounts=extract_multiple_accounts)
            elif transaction_regex_flag:
                tables, last_page, opening_balance = get_transactions_using_regex(page, bank, each_parameter)
            else:
                # print("this template is extracting from parse table directly")
                tables, list_of_y, last_page = parse_table(page, [0, 0, width, height], columns=vertical_lines, image_flag=image_flag, range_involved=range_involved, inconsistent_regexes = last_page_regex, match_field = account_number, account_delimiter_regex=account_delimiter_regex, extract_multiple_accounts=extract_multiple_accounts, round_off_coordinates=round_off_coordinates)
        except Exception as e:
            log_data(message=f"Exception occured at Block 135 with error {e}", local_logging_context=local_logging_context, log_type="info", LOGGER=LOGGER)
            pass

    columns = each_parameter['column']
    needs_merging = each_parameter.get("merge", False)
    footer = each_parameter.get("footer", [False,None])
    special_symbol = each_parameter.get("special_symbol", False)
    merge_balance = each_parameter.get("merge_balance", False)
    merge_balance_note = each_parameter.get('merge_balance_note', False)
    actual_table = tables
    actual_table = cut_footer_from_table(actual_table, bank)

    txn_dataframe = pd.DataFrame(actual_table)

    all_extracted_accounts = {}
    num_columns = txn_dataframe.shape[1]
    if extract_multiple_accounts and actual_table and not transaction_regex_flag and num_columns > 2 and not textract_extracted:
        account_number_list = txn_dataframe.pop(num_columns - 2).to_list()
        account_category_list = txn_dataframe.pop(num_columns - 1).to_list()

    counter = -1
    template_transactions = {
        "template_uuid": current_template_uuid,
        "data": {}
    }
    for each_column_list in columns:
        counter += 1 
        txn_df = deepcopy(txn_dataframe)
        
        if textract_extracted and merge_balance:
            each_column_list.append('balance')
            txn_df[txn_df.shape[1]] = ""
        
        transaction_list = []
        if special_symbol:
            txn_df = txn_df.replace('\|', '', regex=True)
            txn_df = txn_df.replace('(?<![0-9])0.000', '', regex=True)
            txn_df = txn_df.replace('Continued\.\. page.*', '', regex=True)
            txn_df = txn_df.replace('\s*BANK LIMITED.*', '', regex=True)
        ######################
        # remove n arrays (footer) from actual_table
        if footer[0]:
            actual_table = actual_table[:len(actual_table) - footer[1]]
        #######################
        if needs_merging:
            tmp_each_column_list = deepcopy(each_column_list)
            if extract_multiple_accounts:
                tmp_each_column_list.append('account_number')
                tmp_each_column_list.append('account_category')
            try:
                clubbed_actual_table = club_possibly_same_txns(actual_table, tmp_each_column_list, list_of_y)
                txn_df = pd.DataFrame(clubbed_actual_table)
                num_columns = txn_df.shape[1]
                if extract_multiple_accounts and actual_table and not transaction_regex_flag and num_columns > 2:
                    account_number_list = txn_df.pop(num_columns - 2).to_list()
                    account_category_list = txn_df.pop(num_columns - 1).to_list()
            except Exception as _:
                log_data(message="Erroneous Template Clubbing, Avoiding", local_logging_context=local_logging_context, log_type="info", LOGGER=LOGGER)
                
        if txn_df.shape[1] == len(each_column_list):
            txn_df.columns = each_column_list
            if extract_multiple_accounts and not transaction_regex_flag and not textract_extracted:
                try:
                    txn_df['account_number'] = account_number_list
                    txn_df['account_category'] = account_category_list
                    all_extracted_accounts = find_all_accounts_in_table(txn_df)
                except Exception as _:
                    log_data(message="Could not integrate account number and account category", local_logging_context=local_logging_context, log_type="info", LOGGER=LOGGER)
                    pass
            txn_list = txn_df.to_dict("records")
            txn_list = cut_headers_from_table(txn_list,bank)
            next_page_meta = {
                'balance':'',
                'amount':'',
                'is_debit':False
            }
            if page_number == originally_extracted_for_page:
                previous_unused_txn_rows = unused_raw_txn_rows_from_second_page.get('transaction_rows',[])
                previous_raw_txns = unused_raw_txn_rows_from_second_page.get('raw_rows',[])
                length_of_previous_txn_rows = len(previous_unused_txn_rows)
                for i in range(length_of_previous_txn_rows):
                    next_txn = previous_unused_txn_rows[i]
                    balance = next_txn.get('balance','')
                    debit = next_txn.get('debit','')
                    credit = next_txn.get('credit','')
                    if validate_amount(balance) and not next_page_meta.get('balance'):
                        next_page_meta['balance'] = balance
                        next_txn['balance'] = ''
                        previous_raw_txns[i]['balance']=''
                    if validate_amount(debit) and not next_page_meta.get('amount'):
                        next_page_meta['amount'] = debit
                        next_txn['debit'] = ''
                        next_page_meta['is_debit'] = True
                        previous_raw_txns[i]['debit']=''
                    if validate_amount(credit) and not next_page_meta.get('amount'):
                        next_page_meta['amount'] = credit
                        next_page_meta['is_debit'] = False
                        next_txn['credit'] = ''
                        previous_raw_txns[i]['credit']=''
                    
                    if next_page_meta.get('balance') and  next_page_meta.get('amount'):
                        break
                
                for prev_txn in previous_raw_txns:
                    prev_txn['next_page_txn'] = True
                
                txn_list += previous_raw_txns

            for txn_obj in txn_list:
                if 'next_page_txn' not in txn_obj.keys():
                    txn_obj['next_page_txn'] = False
            
            txn_df = pd.DataFrame(txn_list)
            if template_date_format != None:
                txn_df = convert_date_to_date_format(txn_df, template_date_format)
            
            if country == 'ID':
                txn_df = txn_df.apply(lambda row: clean_id_amount_balance(row), axis=1)
            
            if bank == 'hsbc':
                txn_df = format_hsbc_ocr_rows(txn_df)
            
            if bank in ("yesbnk", "canara") and "debit" in txn_df.columns:
                if "Deposits" in txn_df.debit.values:
                    txn_df.rename(columns={'debit': 'credit', 'credit': 'debit'}, inplace=True)
            
            if bank in ['stanchar']:
                txn_df = txn_df.apply(lambda row: complete_semi_dates(row, opening_date, country), axis=1) 
                txn_df = format_stanchar_rows(txn_df)

            if bank in ['kotak', 'uco', 'canara', 'bandhan', 'crsucbnk', 'rajkot', 'ujjivan', 'rajkot_cobnk', 'mahabk']:
                txn_df = complete_semi_dates_from_txn(txn_df, country)
            
            if note_after_date and bank in ['saraswat', 'uco']:
                txn_df = populate_transaction_notes(txn_df)
            
            if bank in ['solapur_siddheshwar', 'shri_chhani_sahakari', 'megabnk', 'csbbnk', 'ambarnath', 'akhand_anand','jalna_people','gscb', 'harij_nagarik_sahakari', 'bombay_mercantile', 'padmavathi_co_op_bnk', 'mangal_co_op_bnk', 'primebnk']:
                txn_df = populate_dates(txn_df)

            if bank in ['idfc', 'masind', 'citi', 'stanchar']:
                txn_df = txn_df.apply(lambda row: mark_negative_balances(row), axis=1)
            
            if merge_balance_note and bank in ['jnkbnk']:
                txn_df = populate_merge_flag(txn_df)
            
            if bank in ['federal']:
                txn_df = populate_semi_date(txn_df)

            if bank in ['sahabat_sampoerna']:
                txn_df = txn_df.apply(lambda row: fix_sahabat_sampoerna_amount_from_balance(row), axis=1)
            
            if bank in ['rakayat', 'masind', 'permata', 'maybnk', 'danamon', 'sahabat_sampoerna']:
                txn_df = format_ID_currency(txn_df, country, bank)
            
            if bank in ['jpmorgan_chase_bnk']:
                txn_df = fix_jpmorgan_amount_balance(txn_df)
            
            if bank in ['bcabnk', 'bnibnk', 'mandiribnk', 'cimb', 'masind', 'danamon', 'federal', 'megabnk', 'permata', 'spcb', 'maybnk', 'jpmorgan_chase_bnk', 'rajkot', 'kokan', 'bhadradri_urban','kotak']:
                txn_df = txn_df.apply(lambda row: complete_semi_dates(row, opening_date, country), axis=1)
            
            if bank in ['bnibnk']:
                txn_df = populate_debit_from_note(txn_df)
                txn_df = fix_bni_amount(txn_df)
            
            if bank in ['ubi']:
                txn_df = sanitize_ubi_hidden_rows(txn_df, each_parameter)
            
            if bank in ['bcabnk']:
                txn_df = complete_transction(txn_df)
            
            if bank in ['kotak']:
                txn_df = fix_kotak_repeated_amount(txn_df)
            
            if bank in ['karur'] and identity.get('fix_numericals', False):
                txn_df = fix_karur_numericals(txn_df)
            
            if bank in ['dcbbnk', 'federal', 'mehsana_urban']:
                txn_df = fill_missing_balance(txn_df)
            
            if bank in ['canara']:
                txn_df = fix_canara_repeated_amount_in_first_txn(txn_df)
            
            if bank in ['dhanlaxmi']:
                txn_df = get_balance_from_amount_dhanlaxmi(txn_df)
            
            if bank in ['kurla_nagrik', 'rajarshi_shahu']:
                txn_df = fix_balance_amount(txn_df)
            
            if bank in ['indusind']:
                txn_df = fix_indusind_date(txn_df)
            
            if bank in ['equitas']:
                txn_df = convert_utc_time(txn_df)
            
            if bank in ['veershaiv_bnk', 'tjsb_sahakari']:
                txn_df = txn_df.apply(lambda row: sinitize_trxn_columns_based_on_regex(row, bank), axis=1)
            
            if bank in ['abhinav_sahakari']:
                txn_df = remove_next_page_data_after_footer(txn_df)
            
            if bank in ['yesbnk']:
                    txn_df = complete_split_balance_from_next_line(bank, txn_df)

            txn_df = txn_df.apply(lambda row: sanitize_transaction_note(row), axis=1)
            trxn_notes_list = []

            first_narrative_index = -1
            first_valid_date_index = -1
            last_account_number = ''
            last_account_category = ''
            if extract_multiple_accounts and len(txn_df) > 0:
                last_account_number = txn_df.iloc[-1].get('account_number')
                last_account_category = txn_df.iloc[-1].get('account_category')
            
            # transaction_note_regex_list is for hsbc only
            if transaction_note_regex_list not in [None, []]:
                vertical_lines = [vertical_lines[0], vertical_lines[-1]]
                extracted_rows, _ = get_df_graphical_lines(path, password, page_number, horizontal=True, vertical=vertical_lines, inconsistent_regexes=last_page_regex, match_field=account_number, bank=bank, plumber_page_edges=plumber_page_edges)
                for extracted_row in extracted_rows:
                    if isinstance(extracted_row, list):
                        for extracted_row_item in extracted_row:
                            if isinstance(extracted_row_item, str):
                                extracted_row_item = [extracted_row_item]
                            if isinstance(extracted_row_item, list):
                                for trxn_note_regex in transaction_note_regex_list:
                                    regex_match = re.match(trxn_note_regex, extracted_row_item[0])
                                    if regex_match is not None:
                                        if get_date_format(regex_match.group(1)):
                                            continue
                                        trxn_notes_list.append(regex_match.group(1))
                                        break
            
                for index in txn_df.index:
                    if first_valid_date_index == -1 and get_date_format(txn_df['date'][index]):
                        first_valid_date_index = index
                    if first_narrative_index == -1 and (any(keyword in txn_df.iloc[index, 0].lower() for keyword in ['narrative', 'transfer', 'atm'])):
                        first_narrative_index = index
                    if first_narrative_index > -1 and first_valid_date_index > -1:
                        break
            
            raw_transactions = txn_df.to_dict('records')
            balance_date_rows_page = balance_date_rows(txn_df, merge_balance=merge_balance, next_page_meta=next_page_meta, bank_name=bank)

            if bank in ['hdfc', 'mizoram']:
                balance_date_rows_page = process_mock_transaction(balance_date_rows_page, bank)
            
            if bank in ['ubi']:
                balance_date_rows_page = solve_ubi_null_balance_transaction_case(balance_date_rows_page)

            if not is_datetime_present(balance_date_rows_page) and len(all_extracted_accounts) < 1:
                print_on_condition(f"Skipping template {current_template_uuid} condition that no date is present in txn_df", debug_prints_enabled)
                log_data(message=f"Skipping template {current_template_uuid} condition that no date is present in txn_df", local_logging_context=local_logging_context, log_type="info", LOGGER=LOGGER)
                break
            
            balance_date_rows_page_list = balance_date_rows_page.to_dict('records')
            transaction_rows_page, first_unused_row_indexes = transaction_rows(balance_date_rows_page, bank)
            first_unused_rows = {'raw_rows':[],'transaction_rows':[]}
            for index in first_unused_row_indexes:
                first_unused_rows['raw_rows'].append(raw_transactions[index])
                first_unused_rows['transaction_rows'].append(balance_date_rows_page_list[index])
            
            transaction_list.extend(transaction_rows_page.apply(lambda row: map_correct_columns(row, bank, country), axis=1))
            transaction_df = pd.DataFrame(transaction_list)
            
            for col in transaction_df:
                if col in ['amount', 'balance']:
                    transaction_df[col] = transaction_df[col].fillna(0)
                else:
                    transaction_df[col] = transaction_df[col].fillna('')
            
            if transaction_df.shape[1] == 1:
                transaction_df = transaction_df.transpose()
                new_header = transaction_df.iloc[0]
                transaction_df= transaction_df[1:]
                transaction_df.columns = new_header 
            
            if transaction_note_regex_list and trxn_notes_list:
                new_prosthetic_row = {}
                if first_narrative_index < first_valid_date_index:
                    new_prosthetic_row = {
                        'transaction_type': 'credit',
                        'transaction_note': trxn_notes_list.pop(0),
                        'amount': DEFAULT_BALANCE_FLOAT,
                        'balance': DEFAULT_BALANCE_FLOAT,
                        'date': datetime.strptime(DEFAULT_TIMESTAMP_UTC, '%Y-%m-%d %H:%M:%S'),
                        'account_number':"",
                        'account_category':"",
                        'chq_num':""
                    }
                if transaction_df.shape[0] == len(trxn_notes_list):
                    transaction_df['transaction_note'] = trxn_notes_list
                elif transaction_df.shape[0] == len(trxn_notes_list) + 1:
                    transaction_df.loc[:len(trxn_notes_list)-1, 'transaction_note'] = trxn_notes_list
                elif transaction_df.shape[0] == len(trxn_notes_list) - 1:
                    transaction_df['transaction_note'] = trxn_notes_list[1:]
                
                if not transaction_df.empty and first_narrative_index < first_valid_date_index:
                    transaction_df.loc[-1] = new_prosthetic_row
                    transaction_df.index = transaction_df.index + 1
                    transaction_df = transaction_df.sort_index()
            
            if transaction_df.shape[0] > 0:
                is_opening_balance = transaction_df.apply(get_opening_balances, axis=1)
                opening_balance_df = transaction_df[is_opening_balance]
                if opening_balance_df.shape[0]>0:
                    removed_opening_balance_date = opening_balance_df['date'].iloc[0]
                    removed_opening_balance_date = convert_pandas_timestamp_to_date_string(removed_opening_balance_date)
                
            if transaction_df.shape[0] > 0:
                is_closing_balance = transaction_df.apply(get_closing_balances, axis=1)
                closing_balance_df = transaction_df[is_closing_balance]
                if closing_balance_df.shape[0]>0:
                    removed_closing_balance_date = closing_balance_df['date'].iloc[-1]
                    removed_closing_balance_date = convert_pandas_timestamp_to_date_string(removed_closing_balance_date)
            
            opening_closing_date_dict[current_template_uuid] = {
                'opening_date':removed_opening_balance_date,
                'closing_date':removed_closing_balance_date
            }

            if transaction_df.shape[0] > 0:
                transaction_df = transaction_df[
                    ((transaction_df['transaction_type'] == 'credit') | (transaction_df['transaction_type'] == 'debit')) & 
                    (abs(transaction_df['amount']) > 0) &
                    (not (any (transaction_df['date']==False)))
                ]
            
            if no_valid_transaction_note_present(transaction_df, bank):
                print_on_condition(f"Skipping template {current_template_uuid} condition that no valid transaction note present in txn_df", debug_prints_enabled)
                log_data(message=f"Skipping template {current_template_uuid} condition that no valid transaction note present in txn_df", local_logging_context=local_logging_context, log_type="info", LOGGER=LOGGER)
                break
            
            current_transactions_in_session_date_range = are_current_transactions_in_session_date_range(transaction_df, session_date_range)
            account_transactions_dict = get_account_wise_transactions_dict(transaction_df, account_number, all_extracted_accounts=all_extracted_accounts)

            current_fraud = None
            for extracted_account_number, txns in account_transactions_dict.items():
                txns = pd.DataFrame(txns)
                if bank in ['bcabnk']:
                    txns = fix_last_txn_type(txns)
                transaction_df = txns.to_dict('records')
                for i in range(len(transaction_df)):
                    transaction_df[i]['optimizations'] = []
                if bank in ['kotak']:
                    transaction_df = absolute_negative_debits(transaction_df)
                transaction_df, pages_updated, current_optimizations_count, _ = optimise_transaction_type(transaction_df, bank)
                transaction_df = pd.DataFrame(transaction_df)
                transaction_df = get_transaction_channel(transaction_df, bank, country, account_category, False)
                if transaction_df.shape[0] > 0:
                    current_null_note_count = transaction_df[transaction_df['transaction_note'] == ''].shape[0]
                    current_null_channel_count = transaction_df[transaction_df['transaction_channel'] == 'Other'].shape[0]
                else:
                    current_null_note_count = 1000000
                    current_null_channel_count = 1000000
                
                transaction_df = add_hash_to_transactions_df(transaction_df)
                transaction_list = transaction_df.to_dict('records')
                if bank in ['yesbnk']:
                    transaction_list, _ = fix_yesbnk_inc_transactions(transaction_list, bank)
                if bank in ['gp_parsik', 'dhanlaxmi']:
                    inconsistent_hash = transaction_balance_check(transaction_list, bank)
                    if inconsistent_hash:
                        transaction_list, _ = swap_inconsistent_trxns(transaction_list, bank, inconsistent_hash, 'pdf')
                if bank == "hdfc" and len(transaction_list) > 0 and transaction_list[0].get('transaction_merge_flag'):
                    log_data("Going inside the if condition for the transaction_merge_flag", local_logging_context=local_logging_context, log_type="info", LOGGER=LOGGER)
                    current_fraud = transaction_balance_check(transaction_list[1:], bank)
                elif transaction_balance_check(transaction_list, bank):
                    current_fraud = 'random_hash'
                transaction_df = pd.DataFrame(transaction_list)
                
                account_transactions_dict[extracted_account_number] = transaction_df

            transaction_df = recombine_account_transctions(account_transactions_dict)
            transaction_df = pd.DataFrame(transaction_df)
            
            if not current_fraud and 'amount' in transaction_df.columns.to_list() and (transaction_df['amount'] < 0).any():
                transaction_list, _, _, _ , _= change_transaction_type(transaction_df.to_dict('records'), bank)
                if not transaction_balance_check(transaction_list, bank):
                    transaction_df = pd.DataFrame(transaction_list)
            
            if transaction_regex_flag and opening_balance and not current_fraud:
                get_first_transaction_type(transaction_df, opening_balance)

            if not transaction_df.shape[0]:
                continue
            
            template_transactions["data"][counter] = {
                "current_transactions_in_session_date_range": current_transactions_in_session_date_range,
                "current_template_txn_count": transaction_df.shape[0],
                "max_transaction_note_length": transaction_df['transaction_note'].apply(len).max(),
                "current_fraud": current_fraud,
                "current_template_uuid": current_template_uuid,
                "current_null_channel_count": current_null_channel_count,
                "current_optimizations_count": current_optimizations_count,
                "current_null_note_count": current_null_note_count,
                "return_data_page": transaction_df.to_dict("records"),
                "last_account_number": last_account_number,
                "last_account_category": last_account_category,
                "first_unused_rows": first_unused_rows,
                "last_page": last_page
            }
        else:
            import math
            template_transactions["data"][counter] = {
                "current_transactions_in_session_date_range": 0,
                "current_template_txn_count": 0,
                "max_transaction_note_length": math.inf,
                "current_fraud": False,
                "current_template_uuid": current_template_uuid,
                "current_null_channel_count": math.inf,
                "current_optimizations_count": math.inf,
                "current_null_note_count": math.inf,
                "return_data_page": [],
                "last_account_number": "",
                "last_account_category": "",
                "first_unused_rows": {},
                "last_page": last_page
            }
    
    if opened_now:
        doc.close()
        
    return template_transactions



def choose_best_template(all_template_transactions, bank, debug_prints_enabled, MAXIMUM_PERMISSIBLE_NOTE_LENGTH, template_list, local_logging_context, LOGGER):
    best_template_txn_count = 0
    return_data_page = []
    best_fraud: bool = False
    extraction_template_uuid = None
    best_null_note_count: int = 10000
    best_null_channel_count: int = 10000
    best_optimizations_count: int = 100000
    unused_raw_txn_rows_from_starting = {'raw_rows':[],'transaction_rows':[]}
    global_last_account_number = ''
    global_last_account_category = ''
    best_transactions_in_session_date_range : bool = False
    global_last_page = False

    for template in template_list:
        template_data = all_template_transactions.get(template)
        if not template_data:
            log_data(message=f"No data found for the template {template}", local_logging_context=local_logging_context, log_type="debug", LOGGER=LOGGER)
            continue

        current_last_page = template_data["last_page"]
        if len(template_data) < 2:
            global_last_page = global_last_page or current_last_page
            log_data(message="Template containing only 1 item, just updating global_last_page", local_logging_context=local_logging_context, log_type="debug", LOGGER=LOGGER)
            continue

        current_transactions_in_session_date_range = template_data["current_transactions_in_session_date_range"]
        current_template_txn_count = template_data["current_template_txn_count"]
        max_transaction_note_length = template_data["max_transaction_note_length"]
        current_fraud = template_data["current_fraud"]
        current_template_uuid = template_data["current_template_uuid"]
        current_null_channel_count = template_data["current_null_channel_count"]
        current_optimizations_count = template_data["current_optimizations_count"]
        current_null_note_count = template_data["current_null_note_count"]
        current_return_data_page = template_data["return_data_page"]
        last_account_category = template_data["last_account_category"]
        last_account_number = template_data["last_account_number"]
        first_unused_rows = template_data["first_unused_rows"]

        global_last_page = global_last_page or current_last_page

        best_template_txn_count = len(return_data_page)

        log_data(message=f"{current_template_uuid} : {current_template_txn_count}: {max_transaction_note_length}: {current_fraud}", local_logging_context=local_logging_context, log_type="debug", LOGGER=LOGGER)
        if current_template_txn_count == 0:
            print_on_condition(f"Skipping template {current_template_uuid} condition 0.1", debug_prints_enabled)
            log_data(message=f"Skipping template {current_template_uuid} condition 0.1", local_logging_context=local_logging_context, log_type="debug", LOGGER=LOGGER)
            continue
            
        if max_transaction_note_length > MAXIMUM_PERMISSIBLE_NOTE_LENGTH:
            print_on_condition(f"Skipping template {current_template_uuid} condition 0.2 with note length {max_transaction_note_length}", debug_prints_enabled)
            log_data(message=f"Skipping template {current_template_uuid} condition 0.2 with note length {max_transaction_note_length}", local_logging_context=local_logging_context, log_type="debug", LOGGER=LOGGER)
            continue
        
        if current_template_txn_count > best_template_txn_count:
            if (best_template_txn_count == 1 and current_fraud and not best_fraud):
                print_on_condition(f"Promoting template {current_template_uuid} condition 1.1", debug_prints_enabled)
                extraction_template_uuid = current_template_uuid
                return_data_page = current_return_data_page
                best_fraud = current_fraud
                best_null_note_count = current_null_note_count
                best_null_channel_count = current_null_channel_count
                best_optimizations_count = current_optimizations_count
                unused_raw_txn_rows_from_starting = first_unused_rows
                global_last_account_number = last_account_number
                global_last_account_category = last_account_category
                best_transactions_in_session_date_range = current_transactions_in_session_date_range
            elif (best_template_txn_count > 0 and current_template_txn_count - best_template_txn_count == 1 and \
                            current_fraud and not best_fraud):
                print_on_condition(f"Skipping template {current_template_uuid} condition 1.2", debug_prints_enabled)
                log_data(message=f"Skipping template {current_template_uuid} condition 1.2", local_logging_context=local_logging_context, log_type="debug", LOGGER=LOGGER)
            elif (best_template_txn_count > 0 and current_template_txn_count - best_template_txn_count <= 3 and current_fraud \
                            and not best_fraud and bank == 'icici'):
                print_on_condition(f"Skipping template {current_template_uuid} condition 1.3", debug_prints_enabled)
                log_data(message=f"Skipping template {current_template_uuid} condition 1.3", local_logging_context=local_logging_context, log_type="debug", LOGGER=LOGGER)
            else:
                print_on_condition(f"Promoting template {current_template_uuid} condition 1.4", debug_prints_enabled)
                log_data(message=f"Promoting template {current_template_uuid} condition 1.4", local_logging_context=local_logging_context, log_type="debug", LOGGER=LOGGER)
                extraction_template_uuid = current_template_uuid
                return_data_page = current_return_data_page
                best_fraud = current_fraud
                best_null_note_count = current_null_note_count
                best_null_channel_count = current_null_channel_count
                best_optimizations_count = current_optimizations_count
                unused_raw_txn_rows_from_starting = first_unused_rows
                global_last_account_number = last_account_number
                global_last_account_category = last_account_category
                best_transactions_in_session_date_range = current_transactions_in_session_date_range
        elif current_template_txn_count == best_template_txn_count:
            if not best_fraud and current_fraud:
                print_on_condition(f"Skipping template {current_template_uuid} condition 2.1", debug_prints_enabled)
                log_data(message=f"Skipping template {current_template_uuid} condition 2.1", local_logging_context=local_logging_context, log_type="debug", LOGGER=LOGGER)
                continue
            elif best_fraud and not current_fraud:
                print_on_condition(f"Promoting template {current_template_uuid} condition 2.2", debug_prints_enabled)
                log_data(message=f"Promoting template {current_template_uuid} condition 2.2", local_logging_context=local_logging_context, log_type="debug", LOGGER=LOGGER)
                extraction_template_uuid = current_template_uuid
                return_data_page = current_return_data_page
                best_fraud = current_fraud
                best_null_note_count = current_null_note_count
                best_null_channel_count = current_null_channel_count
                best_optimizations_count = current_optimizations_count
                unused_raw_txn_rows_from_starting = first_unused_rows
                global_last_account_number = last_account_number
                global_last_account_category = last_account_category
                best_transactions_in_session_date_range = current_transactions_in_session_date_range
            elif current_null_channel_count < best_null_channel_count:
                print_on_condition(f"Promoting template {current_template_uuid} condition 2.3", debug_prints_enabled)
                log_data(message=f"Promoting template {current_template_uuid} condition 2.3", local_logging_context=local_logging_context, log_type="debug", LOGGER=LOGGER)
                extraction_template_uuid = current_template_uuid
                return_data_page = current_return_data_page
                best_fraud = current_fraud
                best_null_note_count = current_null_note_count
                best_null_channel_count = current_null_channel_count
                best_optimizations_count = current_optimizations_count
                unused_raw_txn_rows_from_starting = first_unused_rows
                global_last_account_number = last_account_number
                global_last_account_category = last_account_category
                best_transactions_in_session_date_range = current_transactions_in_session_date_range
            elif (current_null_channel_count == best_null_channel_count and current_null_note_count == best_null_note_count and \
                        current_optimizations_count < best_optimizations_count):
                print_on_condition(f"Promoting template {current_template_uuid} condition 2.4", debug_prints_enabled)
                log_data(message=f"Promoting template {current_template_uuid} condition 2.4", local_logging_context=local_logging_context, log_type="debug", LOGGER=LOGGER)
                extraction_template_uuid = current_template_uuid
                return_data_page = current_return_data_page
                best_fraud = current_fraud
                best_null_note_count = current_null_note_count
                best_null_channel_count = current_null_channel_count
                best_optimizations_count = current_optimizations_count
                unused_raw_txn_rows_from_starting = first_unused_rows
                global_last_account_number = last_account_number
                global_last_account_category = last_account_category
                best_transactions_in_session_date_range = current_transactions_in_session_date_range
            elif current_transactions_in_session_date_range and not best_transactions_in_session_date_range:
                print_on_condition(f"Promoting template {current_template_uuid} condition 2.5", debug_prints_enabled)
                log_data(message=f"Promoting template {current_template_uuid} condition 2.5", local_logging_context=local_logging_context, log_type="debug", LOGGER=LOGGER)
                extraction_template_uuid = current_template_uuid
                return_data_page = current_return_data_page
                best_fraud = current_fraud
                best_null_note_count = current_null_note_count
                best_null_channel_count = current_null_channel_count
                best_optimizations_count = current_optimizations_count
                unused_raw_txn_rows_from_starting = first_unused_rows
                global_last_account_number = last_account_number
                global_last_account_category = last_account_category
                best_transactions_in_session_date_range = current_transactions_in_session_date_range
            elif current_template_txn_count == best_template_txn_count == 1 and \
                (
                    (not current_return_data_page[0].get('zero_transaction_page_account') and return_data_page[0].get('zero_transaction_page_account')) or \
                    (not current_return_data_page[0].get('transaction_merge_flag') and return_data_page[0].get('transaction_merge_flag'))
                ):
                print_on_condition(f"Promoting template {current_template_uuid} condition 2.6", debug_prints_enabled)
                log_data(message=f"Promoting template {current_template_uuid} condition 2.6", local_logging_context=local_logging_context, log_type="debug", LOGGER=LOGGER)
                extraction_template_uuid = current_template_uuid
                return_data_page = current_return_data_page
                best_fraud = current_fraud
                best_null_note_count = current_null_note_count
                best_null_channel_count = current_null_channel_count
                best_optimizations_count = current_optimizations_count
                unused_raw_txn_rows_from_starting = first_unused_rows
                global_last_account_number = last_account_number
                global_last_account_category = last_account_category
                best_transactions_in_session_date_range = current_transactions_in_session_date_range
            elif current_template_txn_count == best_template_txn_count == 1 and \
                    current_return_data_page[0].get('transaction_merge_flag') and bank == 'hsbc' and\
                    return_data_page[0].get('transaction_merge_flag') and \
                    (current_return_data_page[0].get('balance') != -1.0 and return_data_page[0].get('balance') == -1.0):
                print_on_condition(f"Promoting template {current_template_uuid} condition 2.7", debug_prints_enabled)
                log_data(message=f"Promoting template {current_template_uuid} condition 2.7", local_logging_context=local_logging_context, log_type="debug", LOGGER=LOGGER)
                extraction_template_uuid = current_template_uuid
                return_data_page = current_return_data_page
                best_fraud = current_fraud
                best_null_note_count = current_null_note_count
                best_null_channel_count = current_null_channel_count
                best_optimizations_count = current_optimizations_count
                unused_raw_txn_rows_from_starting = first_unused_rows
                global_last_account_number = last_account_number
                global_last_account_category = last_account_category
                best_transactions_in_session_date_range = current_transactions_in_session_date_range
            else:
                print_on_condition(f"Skipping template {current_template_uuid} condition 2.8", debug_prints_enabled)
                log_data(message=f"Skipping template {current_template_uuid} condition 2.8", local_logging_context=local_logging_context, log_type="debug", LOGGER=LOGGER)
        elif current_template_txn_count < best_template_txn_count:
            if (current_template_txn_count > 0 and best_template_txn_count - current_template_txn_count == 1 and not current_fraud and best_fraud):
                print_on_condition(f"Promoting template {current_template_uuid} condition 3.1", debug_prints_enabled)
                log_data(message=f"Promoting template {current_template_uuid} condition 3.1", local_logging_context=local_logging_context, log_type="debug", LOGGER=LOGGER)
                extraction_template_uuid = current_template_uuid
                return_data_page = current_return_data_page
                best_fraud = current_fraud
                best_null_note_count = current_null_note_count
                best_null_channel_count = current_null_channel_count
                best_optimizations_count = current_optimizations_count
                unused_raw_txn_rows_from_starting = first_unused_rows
                global_last_account_number = last_account_number
                global_last_account_category = last_account_category
                best_transactions_in_session_date_range = current_transactions_in_session_date_range
            elif (current_template_txn_count > 0 and best_template_txn_count - current_template_txn_count <= 3 and not current_fraud and best_fraud and  bank == 'icici'):
                print_on_condition(f"Promoting template {current_template_uuid} condition 3.2", debug_prints_enabled)
                log_data(message=f"Promoting template {current_template_uuid} condition 3.2", local_logging_context=local_logging_context, log_type="debug", LOGGER=LOGGER)
                extraction_template_uuid = current_template_uuid
                return_data_page = current_return_data_page
                best_fraud = current_fraud
                best_null_note_count = current_null_note_count
                best_null_channel_count = current_null_channel_count
                best_optimizations_count = current_optimizations_count
                unused_raw_txn_rows_from_starting = first_unused_rows
                global_last_account_number = last_account_number
                global_last_account_category = last_account_category
                best_transactions_in_session_date_range = current_transactions_in_session_date_range
            else:
                print_on_condition(f"Skipping template {current_template_uuid} condition 3.3", debug_prints_enabled)
                log_data(message=f"Skipping template {current_template_uuid} condition 3.3", local_logging_context=local_logging_context, log_type="debug", LOGGER=LOGGER)
    
    return {
        "extraction_template_uuid": extraction_template_uuid,
        "return_data_page": return_data_page,
        "best_fraud": best_fraud,
        "best_null_note_count": best_null_note_count,
        "best_null_channel_count": best_null_channel_count,
        "best_optimizations_count": best_optimizations_count,
        "unused_raw_txn_rows_from_starting": unused_raw_txn_rows_from_starting,
        "global_last_account_number": global_last_account_number,
        "global_last_account_category": global_last_account_category,
        "best_transactions_in_session_date_range": best_transactions_in_session_date_range,
        "global_last_page": global_last_page
    }



def process_parameter(new_parameter):
    try:
        template_data = get_tables_each_page_parallelised(parameter=new_parameter)
        template_id = template_data["template_uuid"]
        data = template_data["data"]
        return {f"{template_id}_{keys}": value for keys, value in data.items()}
    except Exception:
        return {}

def multiprocess_in_server(
        extraction_parameters,
        path,
        password,
        bank,
        page_number,
        account_number,
        opening_date,
        last_page_regex,
        page,
        width,
        height,
        account_delimiter_regex,
        extract_multiple_accounts,
        originally_extracted_for_page,
        unused_raw_txn_rows_from_second_page,
        country,
        identity,
        debug_prints_enabled,
        opening_closing_date_dict,
        session_date_range,
        account_category,
        textract_table,
        local_logging_context,
        LOGGER,
        number_of_pages,
        plumber_page_edges
    ):
    log_data(message="Multiprocessing in Server", local_logging_context=local_logging_context, log_type="info", LOGGER=LOGGER)
    
    # dialing current workers down to 3 from 4, to enable
    current_workers = min(3, multiprocessing.cpu_count())
    
    log_data(message=f"Max workers - {current_workers}", local_logging_context=local_logging_context, log_type="info", LOGGER=LOGGER)
    all_template_transactions = {}
    
    with ProcessPoolExecutor(max_workers=current_workers) as executor:
        # Submit tasks to the executor
        futures = [
            executor.submit(process_parameter, {
                "vertical_lines": each_parameter.get('vertical_lines'),
                "horizontal_lines": each_parameter.get('horizontal_lines'),
                "path": path,
                "password": password,
                "bank": bank,
                "page_number": page_number,
                "account_number": account_number,
                "opening_date": opening_date,
                "last_page_regex": last_page_regex,
                "page": page,
                "width": width,
                "height": height,
                "account_delimiter_regex": account_delimiter_regex,
                "extract_multiple_accounts": extract_multiple_accounts,
                "image_flag": each_parameter.get('image_flag', False),
                "range_involved": each_parameter.get('range', False),
                "each_parameter": each_parameter,
                "transaction_regex_flag": each_parameter.get('transaction_regex_flag'),
                "originally_extracted_for_page": originally_extracted_for_page,
                "unused_raw_txn_rows_from_second_page": unused_raw_txn_rows_from_second_page,
                "template_date_format": each_parameter.get('date_format'),
                "country": country,
                "note_after_date": each_parameter.get('note_after_date', False),
                "identity": identity,
                "transaction_note_regex_list": each_parameter.get('transaction_note_regex_list', []),
                "current_template_uuid": each_parameter.get('uuid'),
                "debug_prints_enabled": debug_prints_enabled,
                "opening_closing_date_dict": opening_closing_date_dict,
                "session_date_range": session_date_range,
                "account_category": account_category,
                "textract_table": textract_table,
                "round_off_coordinates": each_parameter.get('round_off_coordinates', False),
                "local_logging_context": local_logging_context,
                "LOGGER": LOGGER,
                "plumber_page_edges": plumber_page_edges
            })
            for each_parameter in extraction_parameters
        ]

        # Collect results
        for future in as_completed(futures):
            result = future.result()
            all_template_transactions.update(result)

    return all_template_transactions


def get_tables_each_page(transaction_input_payload, local_logging_context, LOGGER) -> dict:
    """
    Based on extraction_parameter (list of template configurations for columns), get column wise values from given page
    :param: path (pdf file path), password (for pdf file), bank (bank name), page (fitz document page object),
            extraction_parameter (list of template configurations for columns), page_number (page number)
    :return: list of dict storing values column wise for each transaction row extracted
    """
    
    path = transaction_input_payload.get('path')
    password = transaction_input_payload.get('password')
    extraction_parameter = transaction_input_payload.get('trans_bbox')
    page_number = transaction_input_payload.get('page_number')
    page = transaction_input_payload.get('page')
    bank = transaction_input_payload.get('bank')
    country = transaction_input_payload.get('country', 'IN')
    account_category = transaction_input_payload.get('account_category')
    identity = transaction_input_payload.get('identity', {})
    account_number = transaction_input_payload.get('account_number', '')
    key = transaction_input_payload.get('key')
    last_page_regex = transaction_input_payload.get('last_page_regex', [])
    account_delimiter_regex = transaction_input_payload.get('account_delimiter_regex', [])
    number_of_pages = transaction_input_payload.get('number_of_pages')
    extract_multiple_accounts = transaction_input_payload.get('extract_multiple_accounts')
    unused_raw_txn_rows_from_second_page = transaction_input_payload.get('unused_raw_txn_rows_from_second_page')
    originally_extracted_for_page = transaction_input_payload.get('original_page_num')
    session_date_range = transaction_input_payload.get('session_date_range')
    textract_table = transaction_input_payload.get('textract_table')
    new_file_path = None
    
    return_data_page = []
    
    width = None
    height = None
    if page:
        page_coordinates = page.mediabox
        width = page_coordinates.width
        height = page_coordinates.height
    
    best_fraud: bool = False
    best_null_note_count: int = 10000
    best_null_channel_count: int = 10000
    best_optimizations_count: int = 100000
    global_last_account_number = ''
    global_last_account_category = ''
    # list_of_y = None
    # best_transactions_in_session_date_range : bool = False

    extraction_template_uuid = ""
    current_template_uuid: Union[str, None] = None
    
    global_last_page = False
    unused_raw_txn_rows_from_starting = {'raw_rows':[],'transaction_rows':[]}

    debug_prints_enabled: bool = False
    MAXIMUM_PERMISSIBLE_NOTE_LENGTH: int = 300

    plumber_page_time_taken = None
    if textract_table:
        plumber_page = None
        plumber_page_edges = None
    else:
        plumber_page_start = time.time()
        try:
            plumber_page=get_pages(path, password)[page_number]
            plumber_page_edges = plumber_page.edges
        except Exception as _:
            plumber_page=None
            plumber_page_edges=None
    

        plumber_page_end = time.time()
        plumber_page_time_taken = plumber_page_end - plumber_page_start

    opening_date = None

    log_data(message=f"Time taken by plumber page is {plumber_page_time_taken}",local_logging_context=local_logging_context,log_type="info",LOGGER=LOGGER)
    
    if identity is not None and identity != {}:
        opening_date = identity.get('opening_date', None)
        from_date = identity.get('date_range', {}).get('from_date', None)
        if opening_date is None and from_date is not None:
            opening_date = from_date

    try:
        if bank in ['tjsb_sahakari','pnbbnk', 'boi', 'karnataka_gramin','gp_parsik', 'mehsana_urban' ] and not textract_table:
            new_file_path = f"/tmp/{str(uuid4())}_{bank}.pdf"
            shutil.copyfile(path, new_file_path)
            path = new_file_path

            doc = read_pdf(path, password)
            page = doc[page_number]
            draft = page.search_for("|")
            for rect in draft:
                annot = page.add_redact_annot(rect)
            page.apply_redactions()
            page.apply_redactions(images=PDF_REDACT_IMAGE_NONE)
            doc.save(path, incremental=True, encryption=PDF_ENCRYPT_KEEP)
    except Exception as e:
        log_data(message=f"Exception occured in for Block 906 with exception {e}",local_logging_context=local_logging_context,log_type="info",LOGGER=LOGGER)

    log_data(message=f"Account Number Received {account_number}",local_logging_context=local_logging_context,log_type="info",LOGGER=LOGGER)

    removed_opening_balance_date, removed_closing_balance_date = None, None
    opening_closing_date_dict = {}
    
    if bank in ['abhinav_sahakari', 'kurla_nagrik', 'agrasen_urban', 'rajarshi_shahu'] and not textract_table:
        if page_number != 0 and all_text_check_for_pdf(path, password, page_number):
            return {}
    
    all_template_transactions = {}
    
    # here we try to get the template data for all templates
    # this for loop gets all the data in sync
    PIPELINE = os.environ.get("PIPELINE", "LAMBDA")
    
    # if plumber_page_time_taken and plumber_page_time_taken > 1:
    #     PIPELINE = 'LAMBDA'
    # Disabling the above code section as we have implemented a fix to extract page edges,
    # which are necessary for the next step of transaction extraction. Moreover,
    # since the page edges are serializable, we no longer need to extract the page in all threads.
    log_data(message=f"PIPELINE received from environment is {PIPELINE}", local_logging_context=local_logging_context, log_type="info", LOGGER=LOGGER)
    
    if PIPELINE != "SERVER":
        log_data(message=f"PIPELINE {PIPELINE}, Extraction in sync", local_logging_context=local_logging_context, log_type="info", LOGGER=LOGGER)
        for _index, each_parameter in enumerate(extraction_parameter):
            vertical_lines = each_parameter.get('vertical_lines')
            horizontal = each_parameter.get('horizontal_lines')
            image_flag = each_parameter.get('image_flag', False)
            range_involved = each_parameter.get('range', False)
            transaction_regex_flag = each_parameter.get('transaction_regex_flag')
            template_date_format = each_parameter.get('date_format')
            note_after_date = each_parameter.get('note_after_date', False)
            transaction_note_regex_list = each_parameter.get('transaction_note_regex_list', [])
            current_template_uuid = each_parameter.get('uuid')
            round_off_coordinates = each_parameter.get('round_off_coordinates', False)

            new_parameter = {
                "vertical_lines": vertical_lines,
                "horizontal_lines": horizontal,
                "path": path,
                "password": password,
                "bank": bank,
                "page_number": page_number,
                "account_number": account_number,
                "opening_date": opening_date,
                "last_page_regex": last_page_regex,
                "page": page,
                "width": width,
                "height": height,
                "plumber_page": plumber_page,
                "plumber_page_edges": plumber_page_edges,
                "account_delimiter_regex": account_delimiter_regex,
                "extract_multiple_accounts": extract_multiple_accounts,
                "image_flag": image_flag,
                "range_involved": range_involved,
                "each_parameter": each_parameter,
                "transaction_regex_flag": transaction_regex_flag,
                "originally_extracted_for_page": originally_extracted_for_page,
                "unused_raw_txn_rows_from_second_page": unused_raw_txn_rows_from_second_page,
                "template_date_format": template_date_format,
                "country": country,
                "note_after_date": note_after_date,
                "identity": identity,
                "transaction_note_regex_list": transaction_note_regex_list,
                "current_template_uuid": current_template_uuid,
                "debug_prints_enabled": debug_prints_enabled,
                "opening_closing_date_dict": opening_closing_date_dict,
                "session_date_range": session_date_range,
                "account_category": account_category,
                "textract_table": textract_table,
                "round_off_coordinates": round_off_coordinates,
                "LOGGER": LOGGER,
                "local_logging_context": local_logging_context
            }
            try:
                template_data = get_tables_each_page_parallelised(parameter=new_parameter)
                template_id = template_data["template_uuid"]
                data = template_data["data"]
                for keys, value in data.items():
                    all_template_transactions[f"{template_id}_{keys}"] = value
            except Exception as e:
                log_data(message=f"Exception occured in for Block 1007 with exception {e}",local_logging_context=local_logging_context,log_type="info",LOGGER=LOGGER)   
                continue
            
    else:
        all_template_transactions = multiprocess_in_server(
            extraction_parameters=extraction_parameter,
            path=path,
            password=password,
            bank=bank,
            page=None,
            page_number=page_number,
            account_number=account_number,
            opening_date=opening_date,
            last_page_regex=last_page_regex,
            width=width,
            height=height,
            account_delimiter_regex=account_delimiter_regex,
            extract_multiple_accounts=extract_multiple_accounts,
            originally_extracted_for_page=originally_extracted_for_page,
            unused_raw_txn_rows_from_second_page=unused_raw_txn_rows_from_second_page,
            country=country,
            identity=identity,
            debug_prints_enabled=debug_prints_enabled,
            opening_closing_date_dict=opening_closing_date_dict,
            session_date_range=session_date_range,
            account_category=account_category,
            textract_table=textract_table,
            local_logging_context=local_logging_context,
            LOGGER=LOGGER,
            number_of_pages=number_of_pages,
            plumber_page_edges=plumber_page_edges
        )

    log_data(message="Extracted from all templates", local_logging_context=local_logging_context, log_type="info", LOGGER=LOGGER)

    templates_list = []
    for template in extraction_parameter:
        template_uuid = template.get('uuid')
        count_column_list = 0
        if template_uuid:
            count_column_list = len(template.get('column', []))
        for i in range(count_column_list):
            templates_list.append(f'{template_uuid}_{i}')

    # now we have to run the logic to pick the best template among all present templates
    best_template_meta = choose_best_template(all_template_transactions, bank, debug_prints_enabled, MAXIMUM_PERMISSIBLE_NOTE_LENGTH, templates_list, local_logging_context, LOGGER)
    extraction_template_uuid = best_template_meta["extraction_template_uuid"]
    return_data_page = best_template_meta["return_data_page"]
    best_fraud = best_template_meta["best_fraud"]
    best_null_note_count = best_template_meta["best_null_note_count"]
    best_null_channel_count = best_template_meta["best_null_channel_count"]
    best_optimizations_count = best_template_meta["best_optimizations_count"]
    unused_raw_txn_rows_from_starting = best_template_meta["unused_raw_txn_rows_from_starting"]
    global_last_account_number = best_template_meta["global_last_account_number"]
    global_last_account_category = best_template_meta["global_last_account_category"]
    global_last_page = best_template_meta["global_last_page"]
    # best_transactions_in_session_date_range = best_template_meta["best_transactions_in_session_date_range"]
    
    best_parameter_list: list = [
        str(True if best_fraud else False),
        str(extraction_template_uuid),
        f"t: {str(len(return_data_page))}",
        f"c: {str(best_null_channel_count)}",
        f"o: {str(best_optimizations_count)}",
        f"n: {str(best_null_note_count)}"
    ]

    log_data(message=f'BEST: {", ".join(best_parameter_list)}', local_logging_context=local_logging_context, log_type="info", LOGGER=LOGGER)
    print_on_condition(f'BEST: {", ".join(best_parameter_list)}', True)

    final_opening_closing_dates = opening_closing_date_dict.get(extraction_template_uuid, dict())
    removed_opening_balance_date = final_opening_closing_dates.get('opening_date')
    removed_closing_balance_date = final_opening_closing_dates.get('closing_date')

    return_data_page, removed_date_opening_balance = remove_opening_balance(return_data_page, bank)
    return_data_page, removed_date_closing_balance = remove_closing_balance(return_data_page)
    if removed_date_opening_balance is not None:
        removed_opening_balance_date = convert_pandas_timestamp_to_date_string(removed_date_opening_balance)
    if removed_date_closing_balance is not None:
        removed_closing_balance_date = convert_pandas_timestamp_to_date_string(removed_date_closing_balance)
    
    if not best_fraud:
        return_data_page, _ = correct_transactions_date(return_data_page, bank)
    
    # if len(return_data_page) > 0 and signed_amount:
    #     for transaction in return_data_page:
    #         if transaction['transaction_type'] == 'debit':
    #             transaction['amount'] = abs(transaction['amount'])

    date_bool = any(transaction['date'] == False for transaction in return_data_page)
    
    if path and bank=='karur' and page_number==0 and len(path.rsplit("-", maxsplit=1))>1:
        original_page_num = path.rsplit("-", maxsplit=1)[-1].replace('.pdf', '')
        if original_page_num.isdigit():
            page_number = int(original_page_num)-1
    
    if bank == 'indusind':
        return_data_page = remove_indusind_invalid_transactions(number_of_pages, page_number, return_data_page)
    
    log_data(message=f"template: {extraction_template_uuid}, bank: {bank}, key: {key}\n\n", local_logging_context=local_logging_context, log_type="info", LOGGER=LOGGER)
    
    if date_bool:
        return {}
    
    log_data(message=f'Global Last Page - {global_last_page}', local_logging_context=local_logging_context, log_type="info", LOGGER=LOGGER)

    if extract_multiple_accounts and len(return_data_page) > 0 and return_data_page[-1]['account_number'] != global_last_account_number:
        return_data_page[-1]['last_account_number'] = global_last_account_number
        return_data_page[-1]['last_account_category'] = global_last_account_category
    
    # Final Output Dict
    output_payload = {
        'transactions': return_data_page,
        'last_page_flag': global_last_page,
        'extraction_template_uuid': extraction_template_uuid,
        'removed_opening_balance_date': removed_opening_balance_date,
        'removed_closing_balance_date': removed_closing_balance_date,
        'unused_raw_txn_rows_from_starting': unused_raw_txn_rows_from_starting
    }

    if new_file_path and os.path.exists(new_file_path):
        os.remove(new_file_path)

    return output_payload

def remove_opening_balance(page, bank=None):
    headers=HEADERS_OPENING_BALANCE
    removed_opening_balance = None
    for element in headers:
        if len(page) > 0:
            element = element.lower()
            transaction_note = page[0]["transaction_note"].strip().lower()
            if len(page) > 0 and re.match(element, transaction_note, re.IGNORECASE):
                removed_opening_balance = page[0]['date']
                page=page[1:]
        
            if len(page) > 0 and bank in ['bnk_of_georgia', 'harij_nagarik_sahakari'] and \
                (transaction_note.startswith(element) or transaction_note.endswith(element)):
                page=page[1:]

    return page, removed_opening_balance

def remove_closing_balance(page):
    headers=HEADERS_CLOSING_BALANCE
    removed_closing_balance = None
    for element in headers:
        if len(page) > 0 and re.match(element, page[-1]["transaction_note"].strip(), re.IGNORECASE):
            removed_closing_balance = page[-1]['date']
            page=page[:-1]
    return page, removed_closing_balance

def is_datetime_present(txn_df):
    return txn_df['date'].apply(lambda x: isinstance(x, datetime)).any()

def no_valid_transaction_note_present(txn_df: Any, bank: str) -> bool:
    if bank not in ['baroda']:
        return False
    with_alphabets = txn_df['transaction_note'].str.contains(r'[a-zA-Z]', regex=True)
    without_alphabets = ~with_alphabets
    return without_alphabets.sum() > with_alphabets.sum()

def are_current_transactions_in_session_date_range(txn_df: Any, session_date_range: dict) -> bool:
    if isinstance(session_date_range, dict) and session_date_range.get('from_date') != None and session_date_range.get('to_date') != None:
        try:
            session_from_date = datetime.strptime(session_date_range.get('from_date'), '%d/%m/%Y')
            session_to_date = datetime.strptime(session_date_range.get('to_date'), '%d/%m/%Y') + timedelta(days=1)
            return txn_df['date'].between(session_from_date, session_to_date).all()
        except Exception as _:
            return False
    return False

def get_transactions_using_regex(page: Any, bank: str, template: dict) -> Tuple[List, bool, Union[float, None]]:
    """
        This function if for extracting transactions from the page using regexes.
        It returns a list of transactions and last_page flag.
        It uses regexes and keys in the template and assign the transaction elements.
        Sample transction regex template:
        {
            "transaction_regex_flag": true,
            "regex_list": [
                {
                    "regex": "(?i)([0-9]{2}\\-[0-9]{2}-[0-9]{2})\\s+([a-z0-9\\-\\/\\:\\@\\.\\-]+)\\s+([0-9]+)\\s+([0-9\\,\\.]+)\\s+([0-9\\,\\.]+)",
                    "group_count": 5,
                    "keys": ["date", "transaction_note", "chq_num", "amount", "balance"]
                }
            ],
            "column": [
                [
                    "date",
                    "transaction_note",
                    "chq_num",
                    "amount",
                    "balance"
                ]
            ]
        }
    """
    
    transactions = []
    regex_list = template.get('regex_list', [])
    columns = template.get('column', [])
    consec_lines_combine_cnt = template.get('consec_lines_combine_cnt', None)
    opening_balance = None
    opening_balance_regex = template.get('opening_balance_regex')
    if regex_list == [] or columns == []:
        return transactions, False, opening_balance

    all_txt = ''
    if page:
        all_txt = page.get_text()
    
    all_txt = all_txt.split('\n')
    num_lines = len(all_txt)
    
    i = 0
    while i < num_lines:
        txn = {key: '' for key in columns[0]}
        if bank == 'saurashtra_co_bnk' and i < num_lines - 7:
            text_slice = all_txt[i:i+7]
            line = ('$$$').join(text_slice)
        elif consec_lines_combine_cnt is not None and bank in ['baroda', 'ubi', 'indbnk', 'nainital']:
            right_limit = min(i+consec_lines_combine_cnt, num_lines)
            text_slice = all_txt[i:right_limit]
            line = ('$$$'.join(text_slice))
        else:
            line = all_txt[i]
            line = line.strip()
        for item in regex_list:
            regex = item.get('regex', '')
            match = re.match(regex, line)
            if match:
                group_list = match.groups()
                groups = [group for group in group_list if group is not None]
                keys = item.get('keys')
                group_count = item.get('group_count', 0)
                if group_count != len(groups):   # the regex should capture group count number of groups and any group should not be None
                    continue
                index = 0
                for key in keys:
                    txn[key] += groups[index].strip()
                    index += 1
                if bank in ['saurashtra_co_bnk']:
                    i = i + 5
                if bank in ['indbnk', 'nainital']:
                    i = i + 4
                break
        if not opening_balance and opening_balance_regex:
            opening_balance_match = re.match(opening_balance_regex, line)
            if opening_balance_match:
                opening_balance = opening_balance_match.groups()[0]
                opening_balance = amount_to_float(opening_balance)

        transactions.append(txn)
        i += 1

    return transactions, False, opening_balance
def club_possibly_same_txns(actual_table, each_column_list, list_of_y):

    # print("Merging required...")
    if list_of_y == None or list_of_y == []:
        return actual_table

    list_of_y_copy = list(list_of_y)

    if actual_table != [] and actual_table != None:
        if len(each_column_list) == len(actual_table[0]):
            # means table has same number of columns as template we are checking
            clubbed_actual_table = list(actual_table)
            # get the index of balance column from this template
            index_of_balance = each_column_list.index("balance")
            # print("Index of balance -> ", index_of_balance)

            # print(" ----------------------------- ")
            # for row in clubbed_actual_table:
            #     print(row)
            # print(" ----------------------------- ")

            index_of_transaction_note = each_column_list.index("transaction_note")
            i = 0
            while i < len(clubbed_actual_table):
                # print("i = ", i)
                if clubbed_actual_table[i][index_of_transaction_note].strip() != "" and clubbed_actual_table[i][index_of_balance].strip() == "":
                    # means this row needs to be merged into some other row (either up of down)
                    # going up
                    j = i - 1
                    distance_up = 5000
                    while j >= 0:
                        if clubbed_actual_table[j][index_of_balance].strip() != "":
                            distance_up = abs(list_of_y_copy[i] - list_of_y_copy[j])
                            break
                        j = j - 1
                    # going down
                    j = i + 1
                    distance_down = 5000
                    while j < len(clubbed_actual_table):
                        if clubbed_actual_table[j][index_of_balance].strip() != "": 
                            check_if_amount = False
                            try:
                                _ = validate_amount(clubbed_actual_table[j][index_of_balance])
                                # _ = validate_amount(clubbed_actual_table[i][index_of_balance]) # Extra string handling for kotak and axis
                                check_if_amount = True
                            except:
                                # print("Balance column does not contain a float object")
                                pass
                            
                            if check_if_amount:
                                distance_down = abs(list_of_y_copy[i] - list_of_y_copy[j])
                            break
                        j = j + 1
                    # print((distance_up, distance_down))
                    # we merge with the row having minimum distance
                    if distance_up < distance_down:
                        # merge with upper row (i - 1) and save it at the position of upper row 
                        if i - 1 >= 0:
                            new_upper_row = ["{} {}".format(a, b) for a, b in zip(clubbed_actual_table[i - 1], clubbed_actual_table[i])]
                            if 'account_number' in each_column_list and 'account_category' in each_column_list:
                                new_upper_row[-1] = clubbed_actual_table[i-1][-1]
                                new_upper_row[-2] = clubbed_actual_table[i-1][-2]
                            clubbed_actual_table[i - 1] = new_upper_row
                            # taking average of both the `y` at i-1th and ith position
                            list_of_y_copy[i - 1] = (list_of_y_copy[i - 1] + list_of_y_copy[i]) / 2
                            clubbed_actual_table.pop(i)
                            list_of_y_copy.pop(i)
                            i = i - 1
                        else:
                            i = i + 1
                    else:
                        # merge with lower row (i + 1) and save it at the position of lower row
                        if i + 1 < len(clubbed_actual_table):
                            new_lower_row = ["{} {}".format(a, b) for a, b in
                                             zip(clubbed_actual_table[i], clubbed_actual_table[i + 1])]
                            if 'account_number' in each_column_list and 'account_category' in each_column_list:
                                new_lower_row[-1] = clubbed_actual_table[i+1][-1]
                                new_lower_row[-2] = clubbed_actual_table[i+1][-2]
                            clubbed_actual_table[i + 1] = new_lower_row
                            # taking average of both the `y` at i+1th and ith position
                            list_of_y_copy[i + 1] = (list_of_y_copy[i + 1] + list_of_y_copy[i]) / 2
                            clubbed_actual_table.pop(i)
                            list_of_y_copy.pop(i)
                            # print(clubbed_actual_table[i])
                        else:
                            i = i + 1
                else:
                    # print(clubbed_actual_table[i])
                    i = i + 1

            # print(" ----------------------------- ")
            # for row in clubbed_actual_table:
            #     print(row)
            # print(" ----------------------------- ")
            return clubbed_actual_table

    return actual_table


def get_transactions_using_fitz(transaction_input_payload, local_logging_context, LOGGER):
    """
    Get transactions from a given page of a given statement pdf
    :param request: takes path (pdf file path), bank, password (of pdf file)
                    and page_number (page number starts from 0)
    :return: list (list of transactions extracted)
    """

    path = transaction_input_payload.get('path')
    password = transaction_input_payload.get('password')
    trans_bbox = transaction_input_payload.get('trans_bbox')
    page_number = transaction_input_payload.get('page_number')
    bank = transaction_input_payload.get('bank')
    country = transaction_input_payload.get('country', 'IN')
    name = transaction_input_payload.get('name')
    account_category = transaction_input_payload.get('account_category')
    number_of_pages = transaction_input_payload.get('number_of_pages')
    
    doc = read_pdf(path, password)  # gets fitz document object
    
    if isinstance(doc, int):
        # password incorrect or file doesn't exist or file is not a pdf
        log_data(message="Unable to open pdf", local_logging_context=local_logging_context, log_type="error", LOGGER=LOGGER)
        return {}
    
    
    if page_number < number_of_pages:  # check whether page_number provided doesn't exceed num_pages
        page = doc[page_number]
        transaction_input_payload['page'] = page
    else:
        return {}

    if not trans_bbox:
        try:
            log_data(message=f"Did not receive trans bbox for bank {bank}", local_logging_context=local_logging_context, log_type="warning", LOGGER=LOGGER)
            trans_bbox, last_page_regex = get_local_templates(bank)
            transaction_input_payload['trans_bbox'] = trans_bbox
            transaction_input_payload['last_page_regex'] = last_page_regex
        except Exception as e:
            log_data(message=f"Exception Occured while reading local templates : {e}", local_logging_context=local_logging_context, log_type="error", LOGGER=LOGGER)
    else:
        log_data(message="Templates retrieved from server", local_logging_context=local_logging_context, log_type="info", LOGGER=LOGGER)
            
    transaction_output_payload = get_tables_each_page(transaction_input_payload, local_logging_context, LOGGER)

    all_transactions = transaction_output_payload.get('transactions', [])

    if all_transactions:
        transaction_df = pd.DataFrame(all_transactions)
        
        transaction_df = get_transaction_channel(transaction_df, bank, country, account_category)
        transaction_df = get_transaction_description(transaction_df, name, country)
        transaction_df = add_hash_to_transactions_df(transaction_df)
        
        if transaction_input_payload.get('extract_multiple_accounts') and 'last_account_number' in transaction_df.columns:
            transaction_df['last_account_number'] = transaction_df['last_account_number'].fillna("")
            transaction_df['last_account_category'] = transaction_df['last_account_category'].fillna("")
        
        if transaction_input_payload.get('extract_multiple_accounts') and 'zero_transaction_page_account' in transaction_df.columns:
            transaction_df['zero_transaction_page_account'] = transaction_df['zero_transaction_page_account'].fillna(False)
        
        transaction_list = transaction_df.to_dict('records')

        transaction_list = update_transaction_channel_for_cheque_bounce(transaction_list)
        
        if bank in ['central', 'sbi']:
            transaction_list, _ = remove_duplicate_transactions(transaction_list, bank)
        
        transaction_output_payload['transactions'] = transaction_list

    # close the doc before responding
    doc.close()
    
    return transaction_output_payload


def get_local_templates(bank):

    transaction_templates = []
    last_page_regex = []

    if bank in ("federal", "india_post"):
        bank = str(bank) + '1'
    
    file_path = 'library/bank_data/' + bank + '.json'
    
    if os.path.exists(file_path):
        with open(file_path, 'r') as data_file:
            try:
                data = json.load(data_file)
                transaction_templates = data.get('trans_bbox', [])
                last_page_regex = data.get('last_page_regex',[])
            except ValueError:
                print("Invalid JSON file\nPlease check")
                transaction_templates = []
                last_page_regex = []
            except Exception as e:
                print(e)
                transaction_templates = []
                last_page_regex = []
            finally:
                data_file.close()
    else:
        print("Incorrect bank name")
    
    return transaction_templates, last_page_regex


def balance_date_rows(df, merge_balance = None, key=None, next_page_meta={}, bank_name = ''):
    
    if merge_balance:   
        df = df.apply(lambda row: merge_balance_row(row, key=key), axis=1)
    df = df.apply(lambda row: check_balance_row(row), axis=1)
    df = get_date_merged(df, key)
    df = df.apply(lambda row: check_transaction_row(row, key=key), axis=1)
    if next_page_meta and bank_name in BANKS_WITH_TRANSACTIONS_SPLIT_ENABLED:
        df = add_unused_balance(df, next_page_meta = next_page_meta)
    df = amount_merge(df)
    return df

def add_unused_balance(df, next_page_meta = {}):
    txn_list = df.to_dict('records')
    last_transaction_row_index = -1
    len_of_records = len(txn_list)

    for i in range(len_of_records):
        is_txn_row = txn_list[i].get('is_transaction_row', True)
        if is_txn_row:
            last_transaction_row_index = i

    date_string, formatted_date = '', None
    for i in range(last_transaction_row_index+1, len_of_records):
        if isinstance(txn_list[i].get('date'),str):
            date_string += txn_list[i].get('date', '')
            date_check,_ = check_date(date_string)
            if date_check:
                formatted_date = date_check
                break
        else:
            break
    
    for i in range(last_transaction_row_index+1, len_of_records):
        date_check,_ = check_date(txn_list[i].get('date'))
        if date_check and not txn_list[i].get('is_balance'):
            txn_list[i]['is_balance'] = True
            txn_list[i]['balance'] = next_page_meta.get('balance','')
            if (not txn_list[i].get('debit') and not txn_list[i].get('credit')) and next_page_meta.get('amount', ''):
                if next_page_meta.get('is_debit'):
                    txn_list[i]['debit'] = next_page_meta.get('amount')
                else:
                    txn_list[i]['credit'] = next_page_meta.get('amount')
            txn_list[i]['is_transaction_row'] = True
            txn_list[i]['date'] = date_check
            break
        elif formatted_date and txn_list[i].get('is_balance'):
            txn_list[i]['is_transaction_row'] = True
            txn_list[i]['date'] = formatted_date
            break

    df = pd.DataFrame(txn_list)

    return df

def merge_balance_row(row, key=None):
    date_format = False
    debit = False
    credit = False
    amount = False
    is_balance = False
    default_balance = '-1'

    # Check date
    if ((isinstance(row['date'], str))) and len(str(row['date'])) > 5:
        date_format = get_date_format(row['date'], key=key)
    elif isinstance(row['date'],datetime):
        date_format = row['date']

    # Check Credit, Debit and Balance
    if row.get('debit') != None:
        debit = validate_amount(row['debit'])
    if row.get('credit') != None:
        credit = validate_amount(row['credit'])
    if row.get('amount') != None:
        amount = validate_amount(row['amount'])
    if row.get('balance') != None:
        is_balance = validate_amount(row['balance'])
    
    # Update balance to default
    if date_format and (debit or credit or amount):
        if not is_balance:
            row['balance'] = default_balance
    return row

def check_balance_row(row):
    row['is_balance'] = validate_amount(row.get('balance', ''))
    return row

def check_transaction_row(row, key=None):

    if ((isinstance(row['date'], str))) and len(str(row['date'])) > 5:
        # and len(str(row['date'])) < 15):
        date_format = get_date_format(row['date'], key=key)
    elif isinstance(row['date'], datetime):
        date_format = row['date']
    else:
        date_format = False

    if date_format and (validate_amount(row['balance']) == True):
        row['is_transaction_row'] = True
        row['date_formatted'] = date_format
    else:
        row['is_transaction_row'] = False
        row['date_formatted'] = False
    end_words = ["Carried Forward", "Closing Balance"]
    if row['transaction_note'] in end_words:
        row['is_transaction_row'] = False
    return row


def get_date_merged(df, key=None):

    df['is_date_used'] = False
    row_dicts = df.to_dict('records')
    total_num_transaction_row = df[df['is_balance'] == True].shape[0]
    num_transaction_row_done = 0
    prev_i = -1
    for i in range(0, len(row_dicts)):
        if isinstance(row_dicts[i]['date'], datetime):
            continue
        if row_dicts[i]['date'].startswith(':'):
            row_dicts[i]['date'] = row_dicts[i]['date'][1:]
        row_dicts[i]['date'] = remove_unicode(row_dicts[i]['date'])

        for date_regex in date_regexes:        
            date_matched = re.match(date_regex, row_dicts[i]['date'])
            final_date = None
            if date_matched:
                final_date = get_date_format(date_matched.group(1), key=key)
            if date_matched and final_date:
                row_dicts[i]['date'] = final_date
                break

        if row_dicts[i]['is_balance']:
            num_transaction_row_done = num_transaction_row_done + 1
            prev_i = i
            is_valid_date = get_date_format(row_dicts[i]['date'], key=key)
            
            if not is_valid_date:
                # this means that the valid date was not extracted
                # remove all the alphabet characters and try again
                temp_date_text = row_dicts[i]['date']
                temp_date_text = remove_gibberish_reg.sub("", temp_date_text)
                final_date = get_date_format(temp_date_text, key=key)
                if final_date:
                    row_dicts[i]['is_date_used'] = True
                    row_dicts[i]['date'] = final_date
                    is_valid_date = final_date
            if is_valid_date:
                row_dicts[i]['date'] = is_valid_date
                row_dicts[i]['is_date_used'] = True
            
            elif (row_dicts[i]['date'] == '') and (i > 0):
                possible_date = get_date_format(row_dicts[i - 1]['date'], key=key)
                is_pre_valid_semi_date = check_semi_date(
                    row_dicts[i - 1]['date'])
                if (possible_date != False) and (row_dicts[i - 1]['is_balance'] == False) and (
                        row_dicts[i - 1]['is_date_used'] == False):
                    row_dicts[i]['date'] = possible_date
                    row_dicts[i - 1]['is_date_used'] = True
                elif (is_pre_valid_semi_date != False) and (row_dicts[i - 1]['is_balance'] == False):
                    row_dicts[i]['date'] = row_dicts[i - 1]['date']
            #below is the case for pnb , when date is splitted into multiple rows
            elif 0 < i < len(row_dicts)-1:
                if not row_dicts[i-1]['is_balance'] and not row_dicts[i+1]['is_balance'] and row_dicts[i]['date'] != '':
                    prev_date = ''
                    next_date = ''
                    if isinstance(row_dicts[i-1]['date'], str):
                        prev_date = row_dicts[i-1]['date']
                    if isinstance(row_dicts[i+1]['date'], str):
                        next_date = row_dicts[i+1]['date']
                    possible_date1 = get_date_format(prev_date + row_dicts[i]['date'] + next_date, key=key)
                    if possible_date1 != False and (row_dicts[i - 1]['is_date_used'] == False):
                        row_dicts[i]['date'] = possible_date1
                        row_dicts[i]['is_date_used'] = True
        elif (
                (
                    (
                        len(str(row_dicts[i]['date'])) > 5 
                        and len(str(row_dicts[i]['date'])) < 15
                    ) 
                    or isinstance(row_dicts[i]['date'], datetime)
                ) 
                and prev_i > -1 
                and num_transaction_row_done <= total_num_transaction_row
            ):
            if get_date_format(row_dicts[prev_i]['date'], key=key) == False:
                if isinstance(row_dicts[i]['date'], datetime) and row_dicts[prev_i]['date'] in ['', None]:
                    row_dicts[prev_i]['date'] = row_dicts[i]['date']
                elif isinstance(row_dicts[i]['date'], str):
                    row_dicts[prev_i]['date'] = (row_dicts[prev_i]['date'] if row_dicts[prev_i]['date'] else '') + ' ' + (
                                                row_dicts[i]['date'] if row_dicts[i]['date'] else '')
                row_dicts[prev_i]['date'] = get_date_format(row_dicts[prev_i]['date'], key=key)
                row_dicts[i]['is_date_used'] = True
        # below is the case for sbi, whenit does not detect the lines
        elif (
                (
                    len(str(row_dicts[i]['date'])) == 4 
                    or isinstance(row_dicts[i]['date'], datetime)
                ) 
                and prev_i > -1
                and num_transaction_row_done <= total_num_transaction_row
            ):
            if get_date_format(row_dicts[prev_i]['date'], key=key) == False:
                if isinstance(row_dicts[i]['date'], datetime) and row_dicts[prev_i]['date'] in ['', None]:
                    row_dicts[prev_i]['date'] = row_dicts[i]['date']
                elif isinstance(row_dicts[i]['date'], str):
                    row_dicts[prev_i]['date'] = (row_dicts[prev_i]['date'] if row_dicts[prev_i]['date'] else '') + ' ' + (
                                                row_dicts[i]['date'] if row_dicts[i]['date'] else '')
                row_dicts[prev_i]['date'] = get_date_format(row_dicts[prev_i]['date'], key=key)
                row_dicts[i]['is_date_used'] = True
    date_modified_df = pd.DataFrame(row_dicts)

    return date_modified_df


# def get_date_merged_2(df):
#     row_dicts = df.to_dict('records')
#     prev_i = -1
#     joint_date = None
#     for i in range(0, len(row_dicts)):
#         row_dicts[i]['date'] = remove_unicode(row_dicts[i]['date'])
#         is_valid_date = get_date_format(row_dicts[i]['date'])
#         is_valid_semi_date = check_semi_date(row_dicts[i]['date'])
#         if row_dicts[i]['is_balance']:
#             prev_i = i
#             if is_valid_date:
#                 continue
#             elif joint_date is not None and (get_date_format(joint_date + ' ' + row_dicts[i]['date'])):
#                 row_dicts[i]['date'] = joint_date + ' ' + row_dicts[i]['date']

#         elif (is_valid_date != False) or (is_valid_semi_date != False):
#             if joint_date is None:
#                 joint_date = row_dicts[i]['date']
#             else:
#                 joint_date = joint_date + ' ' + row_dicts[i]['date']
#             if get_date_format(joint_date) and (prev_i > -1) and (get_date_format(row_dicts[prev_i]['date']) == False):
#                 row_dicts[prev_i]['date'] = joint_date
#                 joint_date = None
#         elif len(row_dicts[i]['date']) == 0:
#             continue
#         else:
#             joint_date = None


def amount_merge(df):
    df['is_amount_used'] = False
    row_dicts = df.to_dict('records')
    for i in range(0, len(row_dicts)):
        if row_dicts[i]['is_transaction_row']:
            amount = row_dicts[i].get('amount')
            credit = row_dicts[i].get('credit')
            debit = row_dicts[i].get('debit')
            if (amount is not None and len(amount) == 0) or (
                    (credit is not None and len(credit) == 0) and (debit is not None and len(debit) == 0)):
                if (i > 0) and (row_dicts[i - 1]['is_transaction_row'] == False) and (
                        row_dicts[i - 1]['is_amount_used'] == False):
                    if (amount is not None):
                        new_amount = row_dicts[i - 1].get('amount')
                        match_amount = match_compiled_regex(new_amount, amount_merge_regex1, 1)
                        if match_amount is not None:
                            row_dicts[i]['amount'] = (row_dicts[i]['amount'] if row_dicts[i][
                                'amount'] else '') + '' + str(match_amount)

                    if (credit is not None):
                        new_credit = row_dicts[i - 1].get('credit')
                        # The below amount regex ensures a big random word does not come
                        match_credit = match_compiled_regex(new_credit, amount_merge_regex2, 1)
                        if match_credit is not None and len(str(match_credit).split('.')[0]) < 12:
                            row_dicts[i]['credit'] = (row_dicts[i]['credit'] if row_dicts[i][
                                'credit'] else '') + '' + str(match_credit)

                    if (debit is not None):
                        new_debit = row_dicts[i - 1].get('debit')
                        match_debit = match_compiled_regex(new_debit, amount_merge_regex2, 1)
                        if match_debit is not None and len(str(match_debit).split('.')[0]) < 12:
                            row_dicts[i]['debit'] = (row_dicts[i]['debit'] if row_dicts[i]['debit'] else '') + '' + str(
                                match_debit)
                    row_dicts[i - 1]['is_amount_used'] = True

                if i + 1 < len(row_dicts):
                    for j in range(i + 1, len(row_dicts)):
                        amount = row_dicts[i].get('amount')
                        credit = row_dicts[i].get('credit')
                        debit = row_dicts[i].get('debit')
                        if (amount is not None and len(amount) == 0) or (
                                (credit is not None and len(credit) == 0) and (debit is not None and len(debit) == 0)):
                            if (row_dicts[j]['is_transaction_row'] == False) and (
                                    row_dicts[j]['is_amount_used'] == False):
                                if (amount is not None):
                                    new_amount = row_dicts[j].get('amount')
                                    match_amount = match_compiled_regex(new_amount, amount_merge_regex1, 1)
                                    if match_amount is not None:
                                        row_dicts[i]['amount'] = (row_dicts[i]['amount'] if row_dicts[i][
                                            'amount'] else '') + '' + str(match_amount)

                                if (credit is not None):
                                    new_credit = row_dicts[j].get('credit')
                                    match_credit = match_compiled_regex(new_credit, amount_merge_regex3, 1)
                                    if match_credit is not None:
                                        row_dicts[i]['credit'] = (row_dicts[i]['credit'] if row_dicts[i][
                                            'credit'] else '') + '' + str(match_credit)

                                if (debit is not None):
                                    new_debit = row_dicts[j].get('debit')
                                    match_debit = match_compiled_regex(new_debit, amount_merge_regex3, 1)
                                    if match_debit is not None:
                                        row_dicts[i]['debit'] = (row_dicts[i]['debit'] if row_dicts[i][
                                            'debit'] else '') + '' + str(match_debit)

                                row_dicts[j]['is_amount_used'] = True

                            else:
                                break
                        else:
                            break

    return pd.DataFrame(row_dicts)


def get_blobs(page):
    final_data = []
    text = page.get_text("dict").get('blocks')
    for each_line in text:
        if each_line.get('lines') is not None:
            overall_blob = each_line['bbox']
            overall_blob_top = overall_blob[1]
            overall_blob_bottom = overall_blob[3]
            line_word_array = each_line['lines']
            for line in line_word_array:
                blob_dict = {}
                word = line['spans'][0]['text']
                bbox = line['bbox']
                blob_dict['bbox_left'] = bbox[0]
                blob_dict['bbox_right'] = bbox[2]
                blob_dict['bbox_top'] = overall_blob_top
                blob_dict['bbox_bottom'] = overall_blob_bottom
                blob_dict['text'] = word
                final_data.append(blob_dict)
    return pd.DataFrame(final_data)


def get_transaction_note_filter(each_parameter):
    columns = each_parameter['column']
    vertical_lines = each_parameter['vertical_lines']
    transaction_note_index = columns.index('transaction_note')
    if transaction_note_index == 0:
        left_filter = 0
    else:
        left_filter = vertical_lines[transaction_note_index - 1]

    if transaction_note_index == len(columns) - 1:
        right_filter = 2000
    else:
        right_filter = vertical_lines[transaction_note_index]
    return left_filter, right_filter


def get_possible_transaction_note_merged(page, parameter):
    df_blob = get_blobs(page)
    left_filter, right_filter = get_transaction_note_filter(parameter)
    df_transaction_note = df_blob[(df_blob['bbox_left'] > left_filter) and (
            df_blob['bbox_right'] < right_filter)]
    df_group = df_transaction_note.groupby(['bbox_top', 'bbox_bottom'])
    list_transaction_notes = []
    for group, group_df in df_group:
        word_list = list(group_df['text'])
        transaction_note = ' '.join(word_list)
        list_transaction_notes.append(transaction_note)
    return list_transaction_notes


def get_notes_for_blanks(df):
    flag = 0
    row_dicts = df.to_dict('records')
    for i in range(0, len(row_dicts)):
        row_dicts[i]['transaction_note'] = remove_unicode(
            row_dicts[i]['transaction_note'])
        if (row_dicts[i]['is_transaction_row']) and (i < len(row_dicts)):
            row_dicts[i]['transaction_note'] = row_dicts[i + 1]['transaction_note'] if row_dicts[i + 1][
                'transaction_note'] else ''
            if row_dicts[i]['transaction_note'] != '':
                flag = 1
        if flag == 1:
            break
    transaction_note_modified_df = pd.DataFrame(row_dicts)
    return transaction_note_modified_df[transaction_note_modified_df['is_transaction_row'] == True]


def merge_transaction_note(df, possible_transaction_notes):
    transaction_df = df[df['is_transaction_row'] == True]
    if transaction_df[transaction_df['transaction_note'] != ''].shape[0] == 0:
        transaction_df = get_notes_for_blanks(df)
    transactions = transaction_df.to_dict('records')
    num_transactions = len(transactions)
    num_notes = len(possible_transaction_notes)
    flag = 0
    i = 0
    j = 0
    for each_transaction in transactions:
        transaction_note = each_transaction['transaction_note']
        if isinstance(transaction_note, str):
            transaction_note_regex = re.compile(('.*({}).*'.format(transaction_note)).upper())
            j = 0
            for word in possible_transaction_notes:
                match = match_compiled_regex(word.upper(), transaction_note_regex, 1)
                if match is not None and len(match) > 1:
                    flag = 1
                    break
                j = j + 1
        # TODO the logic would break if there are 2 tables of transactions because of some reason
        if (flag == 1) and (num_notes > j - i + num_transactions - 1):
            if j >= i:
                for k in range(0, num_transactions):
                    # print(j, i, num_transactions, num_notes)
                    # print(possible_transaction_notes[j - i + k])
                    transactions[k]['transaction_note'] = possible_transaction_notes[j - i + k]
            break
        # TODO write the else logic
        i = i + 1
    return pd.DataFrame(transactions)

def cut_headers_from_table(
    txn_list: list, 
    bank_name: str
) -> list:
    if bank_name in CUT_HEADERS_PATTERNS.keys():
        index = -1
        bank_patterns = CUT_HEADERS_PATTERNS.get(bank_name,[])
        length_of_list = len(txn_list)
        for pattern in bank_patterns:
            for i in range(length_of_list):
                pattern_valid = True
                for key, val in txn_list[i].items():
                    if key in ['account_category', 'account_number']:
                        continue

                    if key not in pattern.keys():
                        pattern_valid = False
                        break
                    elif pattern[key] != val:
                        pattern_valid = False
                        break
                
                if pattern_valid:
                    index = i
                    break 
            
            if index != -1:
                break
        
        if index != -1:
            txn_list = txn_list[index + 1:]
    
    return txn_list

def cut_footer_from_table(
    actual_table: list, 
    bank_name: str
) -> list:
    if bank_name in CUT_FOOTER_REGEXES.keys():
        index = -1
        current_regex_list = CUT_FOOTER_REGEXES.get(bank_name,[])
        length_of_table = len(actual_table)
        
        for regex in current_regex_list:
            for i in range(length_of_table):
                for item in actual_table[i]:
                    if re.match(regex, item) is not None:
                        index = i
                        break
                
                if index != -1:
                    break
            
            if index != -1:
                break
        
        if index != -1:
            actual_table = actual_table[:index]
    
    return actual_table

# def get_txns_from_rotated_pdf(path, page_num,bank):
#     try:
#         pdf_tables_key = get_api_key_pdf_tables()
#
#         if pdf_tables_key == '':
#             return []
#
#         pdf_tables = pdftables_api.Client(pdf_tables_key)
#         csv_table_file_name = "/tmp/{}.csv".format(str(uuid.uuid4()))
#         pdf_tables.csv(path, csv_table_file_name)
#
#         try:
#             csv_data = []
#             with open(csv_table_file_name, "r") as csv_file:
#                 reader = csv.reader(csv_file)
#                 csv_data = list(reader)
#                 # print(csv_data)
#                 header_index = -1
#                 needs_combine_col = False
#                 df = []
#                 for i, row in enumerate(csv_data):
#                     if header_index > page_num:
#                         break
#                     if if_header_row_predicted_header(row, bank):
#                         header_index += 1
#                         if len(row) > 8 and bank == 'mahabk':
#                             # print(row)
#                             needs_combine_col = True
#                             needs_combine_col_index_one = list(row).index('')
#                             needs_combine_col_index_two = list(row).index('Particulars')
#                         else:
#                             needs_combine_col = False
#
#                     if page_num == header_index:
#                         # print(row)
#                         if len(row) > 6 and bank == 'andhra':
#                             row = row[:6]
#                         if bank == 'mahabk' and len(row) > 8 and needs_combine_col:
#                             row[needs_combine_col_index_one] += row[needs_combine_col_index_two]
#                             row = row[:needs_combine_col_index_two] + row[needs_combine_col_index_two + 1:]
#                             if row[-1] == "":
#                                 row = row[:-1]
#                             else:
#                                 continue
#                         elif bank == 'mahabk' and (len(row) > 8 or len(row) < 2):
#                             continue
#                         df.append(row)
#                 # print(df)
#                 return df
#         except Exception as e:
#             print(e)
#     except Exception as e:
#         print(e)
#         return []
#
# def if_header_row_predicted_header(row, bank):
#     if 'TRAN DATE' in row and bank == 'andhra':
#         print("Header Row found")
#         return True
#     elif 'Particulars' in row and bank == 'mahabk':
#         print("Header Row found")
#         return True
#     else:
#         # print("Header row not found")
#         return False
#
# def get_api_key_pdf_tables():
#     # only used for rotated pdf
#     # Check and give api_key for pdf_tables
#
#     items = collect_results(api_keys_table.scan, {})
#     api_keys = [item['api_key'] for item in items]
#     api_keys.sort()
#
#     try:
#         key_1 = api_keys[0]
#         key_2 = api_keys[1]
#         URL = "https://pdftables.com/api/remaining"
#         PARAMS = {'key': key_1}
#         print(PARAMS)
#         res = requests.get(url=URL, params=PARAMS)
#         print('page_limit_left', res.text)
#         if int(res.text) > 15:
#             print('key_to_send', key_1)
#             return key_1
#         else:
#             # Delete key_1
#             api_keys_table.delete_item(Key={'api_key': key_1})
#
#             print('key_to_send', key_2)
#             return key_2
#     except Exception as e:
#         print(e)
#         return ''

# def get_date_merged_2(df):
#     row_dicts = df.to_dict('records')
#     # total_num_transaction_row = df[df['is_balance'] == True].shape[0]
#     # num_transaction_row_done = 0
#     prev_i = -1
#     joint_date = None
#     for i in range(0, len(row_dicts)):
#         row_dicts[i]['date'] = remove_unicode(row_dicts[i]['date'])
#         is_valid_date = get_date_format(row_dicts[i]['date'])
#         is_valid_semi_date = check_semi_date(row_dicts[i]['date'])
#         if (row_dicts[i]['is_balance'] == True):
#             prev_i = i
#             if is_valid_date == True:
#                 continue
#             elif joint_date is not None and (get_date_format(joint_date + ' ' + row_dicts[i]['date']) != False):
#                 row_dicts[i]['date'] = joint_date + ' ' + row_dicts[i]['date']

#         elif (is_valid_date != False) or (is_valid_semi_date != False):
#             if joint_date is None:
#                 joint_date = row_dicts[i]['date']
#             else:
#                 joint_date = joint_date + ' ' + row_dicts[i]['date']
#             print(joint_date)
#             if (get_date_format(joint_date) != False) and (prev_i > -1) and (get_date_format(row_dicts[prev_i]['date']) == False):
#                 row_dicts[prev_i]['date'] = joint_date
#                 joint_date = None
#         elif len(row_dicts[i]['date']) == 0:
#             continue
#         else:
#             joint_date = None

#     date_modified_df = pd.DataFrame(row_dicts)
#     return date_modified_df
