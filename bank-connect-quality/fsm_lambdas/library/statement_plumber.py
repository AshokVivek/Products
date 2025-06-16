import json
import os
import warnings
import pandas as pd
import pdfplumber
from library.custom_exceptions import NonParsablePDF
from library.transaction_channel import get_transaction_channel
from library.transaction_description import get_transaction_description

from library.utils import remove_unicode, validate_amount, \
    match_regex, add_hash_to_transactions_df, get_date_format, check_transaction_beginning, add_notes, fix_decimals, \
    update_transaction_channel_for_cheque_bounce, amount_to_float, get_pages, get_amount_sign, check_date
from pdfminer.pdfdocument import PDFPasswordIncorrect
from library.fraud import transaction_balance_check
from datetime import datetime
from library.helpers.constants import UJJIVAN_IGNORE_TRANSACTIONS_NOTE, SKIP_UNICODE_REMOVAL_LIST

warnings.simplefilter(action = "ignore", category = FutureWarning)
pd.options.mode.chained_assignment = None


def get_pdf(path, password):
    try:
        pdf = pdfplumber.open(path, password=password)
    except ValueError:
        raise NonParsablePDF

    return pdf


def get_transactions_from_page(page, bank):
    transaction_list = []
    all_pages = [page]
    all_transactions, extraction_template_uuid, removed_opening_balance_date, removed_closing_balance_date = get_tables_each_page_2(all_pages, bank)
    all_transactions_df = pd.DataFrame(all_transactions)
    transaction_list.extend(all_transactions_df.apply(
        lambda row: map_correct_columns(row, bank, "IN"), axis=1))
    return transaction_list


def get_number_of_pages(path, password):
    return len(get_pages(path, password))

def get_transactions_using_plumber(transaction_input_payload):
    
    path = transaction_input_payload.get('path')
    password = transaction_input_payload.get('password')
    trans_bbox = transaction_input_payload.get('trans_bbox')
    page_number = transaction_input_payload.get('page_number')
    bank = transaction_input_payload.get('bank')
    country = transaction_input_payload.get('country', 'IN')
    account_category = transaction_input_payload.get('account_category')
    name = transaction_input_payload.get('name', '')
    account_number = transaction_input_payload.get('account_number', '')
    
    extraction_template_uuid = ''
    removed_date_opening_balance, removed_date_closing_balance = None, None
    
    output_payload = {
        'transactions': [],
        'last_page_flag': False,
        'extraction_template_uuid': extraction_template_uuid,
        'removed_opening_balance_date': removed_date_opening_balance,
        'removed_closing_balance_date': removed_date_closing_balance
    }
    
    try:
        all_pages = get_pages(path, password)
    except (NonParsablePDF, PDFPasswordIncorrect):
        return output_payload
    
    if not all_pages:
        return output_payload
    
    transaction_list = []
    relevant_pages = [all_pages[page_number]]
    all_transactions, extraction_template_uuid, removed_opening_balance_date, removed_closing_balance_date = get_tables_each_page_2(relevant_pages, bank, trans_bbox)
    removed_date_opening_balance, removed_date_closing_balance = removed_opening_balance_date, removed_closing_balance_date

    if len(all_transactions) == 0:
        return output_payload

    all_transactions_df = pd.DataFrame(all_transactions)
    all_transactions_df['account_number'] = account_number
    all_transactions_df['account_category'] = ''

    # transaction_list.extend(all_transactions_df.apply(
    #     lambda row: map_correct_columns(row), axis=1))
    transaction_df = all_transactions_df
    transaction_channel_df = get_transaction_channel(transaction_df, bank, country, account_category)
    transaction_channel_df = get_transaction_description(transaction_channel_df, name)
    transaction_channel_df = add_hash_to_transactions_df(transaction_channel_df)

    transaction_list = transaction_channel_df.to_dict('records')
    transaction_list = update_transaction_channel_for_cheque_bounce(transaction_list)
   
    # Final Output Dict
    output_payload['transactions'] = transaction_list
    output_payload['extraction_template_uuid'] = extraction_template_uuid
    output_payload['removed_opening_balance_date'] = removed_date_opening_balance
    output_payload['removed_closing_balance_date'] = removed_date_closing_balance
    
    return output_payload


def clean_transaction_note(transaction_note):
    if transaction_note is not None:
        transaction_note = transaction_note.replace('\n', '')
        return transaction_note


def get_tables_each_page_2(pages, bank, trans_bbox):
    extraction_template_uuid = ''
    removed_date_opening_balance, removed_date_closing_balance = None, None
    if trans_bbox:
        extraction_parameter = trans_bbox
        print("Using extraction parameter retrieved from server.")
    else:
        try:
            # capture_message("Did not receive bbox for bank {}".format(bank))
            print("Did not receive trans bbox for bank {}".format(bank))
        except Exception as e:
            print(e)
        file_path = 'library/bank_data/'+bank+'.json'
        if os.path.exists(file_path):
            with open(file_path, 'r') as data_file:
                try:
                    extraction_parameter = json.load(data_file).get('trans_bbox', [])
                except ValueError:
                    print("Invalid JSON file\nPlease check")
                    extraction_parameter = []
                except Exception as e:
                    print(e)
                    extraction_parameter = []
                finally:
                    data_file.close()
        else:
            print("Incorrect bank name")
            extraction_parameter = []
    return_data = []
    for page in pages:
        return_data_page, extraction_template_uuid = get_transaction_data_per_page(extraction_parameter, page, bank)
        return_data.extend(return_data_page)
    return return_data, extraction_template_uuid, removed_date_opening_balance, removed_date_closing_balance


def get_transaction_data_per_page(extraction_parameter, page, bank):
    extraction_template_uuid = ''
    return_data_page = []
    fraud_flag = False
    for index, each_parameter in enumerate(extraction_parameter):
        try:
            tables = page.extract_tables(each_parameter['table_setting'])
        except Exception as e:
            print("Error in extracting tables: ", e)
            continue
        columns = each_parameter['column']
        actual_table = tables
        for each_table in tables:
            if len(each_table[0]) == len(columns):
                actual_table = each_table
                break
        txn_df = pd.DataFrame(actual_table)
        # print(actual_table)
        # print(txn_df)
        try:
            if txn_df.shape[1] == len(columns):
                transaction_list = []
                txn_df.columns = columns
                transaction_rows_page,_ = transaction_rows(txn_df, bank)
                transaction_list.extend(transaction_rows_page.apply(lambda row: map_correct_columns(row, bank, "IN"), axis=1))
                is_fraud = transaction_balance_check(transaction_list, bank)
                transactions_template = pd.DataFrame(transaction_list)
                
                for col in transactions_template:
                    if col in ['amount', 'balance']:
                        transactions_template[col] = transactions_template[col].fillna(0)
                    else:
                        transactions_template[col] = transactions_template[col].fillna('')
                if transactions_template.shape[0] > 0:
                    transactions_template = transactions_template[((transactions_template['transaction_type'] == 'credit') | (
                        transactions_template['transaction_type'] == 'debit')) & (
                        abs(transactions_template['amount']) > 0)]
                
                if transactions_template.shape[0] > len(return_data_page):
                    return_data_page = transactions_template.to_dict('records')
                    extraction_template_uuid = each_parameter.get('uuid')
                    fraud_flag = is_fraud

                elif (transactions_template.shape[0] == len(return_data_page)) and (is_fraud is None):
                    if fraud_flag:
                        return_data_page = transactions_template.to_dict('records')
                        extraction_template_uuid = each_parameter.get('uuid')
                        fraud_flag = is_fraud
        except:
            continue
    return return_data_page, extraction_template_uuid


def transaction_rows(df, bank, key=None):
    df = df.apply(lambda row: check_transaction_row(row, key=key), axis=1)
    df.fillna('', inplace=True)
    row_dicts = df.to_dict('records')
    tota_num_transaction_row = df[df['is_transaction_row'] == True].shape[0]
    num_transaction_row_done = 0
    prev_i = -1
    yet = False # True if row has null fields except transaction_note
    end_flag = False
    ignore_transaction_note = False

    '''
    Gracefully handle HSBC dataframes that have transactions split across multiple lines and multiple pages
    '''

    has_used_any_row = False
    first_unused_row_indexes = []
    if bank in ["hsbc"]:
        for i in range(0, len(row_dicts)):
            hsbc_ignore_transaction_note_list = ['Werefer', 'We refer']
            
            for word in hsbc_ignore_transaction_note_list:
                if isinstance(row_dicts[i]['date'], str) and word in row_dicts[i]['date']:
                    ignore_transaction_note = True
                
            row_dicts[i]['transaction_note'] = " ".join(row_dicts[i]['transaction_note'].split())
            
            if prev_i > -1:
                row_dicts[i]['transaction_note'] = " ".join(row_dicts[i]['transaction_note'].split())
            
            if isinstance(row_dicts[i]['date'], str) and (check_transaction_beginning(row_dicts[i]["date"], False) or check_transaction_beginning(row_dicts[i]["transaction_note"], False)):
                break
            elif row_dicts[i]['is_transaction_row']:
                num_transaction_row_done = num_transaction_row_done + 1
                prev_i = i
                ignore_transaction_note = False
            elif (len(str(row_dicts[i]['transaction_note'])) > 0) & (prev_i > -1) & (num_transaction_row_done <= tota_num_transaction_row) and not ignore_transaction_note:
                row_dicts[prev_i]['transaction_note'] = (row_dicts[prev_i]['transaction_note'] if row_dicts[prev_i]['transaction_note'] else '') + ' ' + (row_dicts[i]['transaction_note'] if row_dicts[i]['transaction_note'] else '')
    else:
        for i in range(0, len(row_dicts)):

            if bank not in SKIP_UNICODE_REMOVAL_LIST:
                row_dicts[i]['transaction_note'] = remove_unicode(row_dicts[i]['transaction_note'])
                row_dicts[i]['transaction_note'] = row_dicts[i]['transaction_note'].strip().replace('\t', ' ').replace('  ', ' ').replace('    ', ' ')
            else:
                row_dicts[i]['transaction_note'] = " ".join(row_dicts[i]['transaction_note'].split())

            if (
                row_dicts[i] and
                row_dicts[i].get('chq_num', '') and
                'Page Total' in row_dicts[i].get('chq_num', '')
            ):
                ignore_transaction_note=True

            if isinstance(row_dicts[i]['date'], str) and ('----------' in row_dicts[i]['date'] or 'Statement' in row_dicts[i]['date'] or 'PAGE TOTAL :' in row_dicts[i]['date']):
                ignore_transaction_note=True
            
            if '-'*10 in row_dicts[i]['transaction_note']:
                ignore_transaction_note=True
            
            if isinstance(row_dicts[i]['transaction_note'], str) and row_dicts[i]['transaction_note'].startswith('Closing Balance'):
                ignore_transaction_note = True
                
            if isinstance(row_dicts[i]['transaction_note'], str) and ('CrDr.Count:' in row_dicts[i]['transaction_note'].replace(' ', '')):
                ignore_transaction_note = True
            
            if 'random' in row_dicts[i] and 'page' in row_dicts[i]['random'].lower():
                ignore_transaction_note = True  

            # for repeated account pdf
            if i > 0 and row_dicts[i]['transaction_note'] == 'TOTAL' and row_dicts[i]['balance'] == row_dicts[i-1]['balance']:
                ignore_transaction_note = True
            
            if "STATEMENT SUMMARY" in row_dicts[i]['transaction_note']:
                ignore_transaction_note = True
            
            if bank == 'ujjivan':
                for ignore_text in UJJIVAN_IGNORE_TRANSACTIONS_NOTE:
                    if ignore_text.upper() in row_dicts[i]['transaction_note'].upper():
                        ignore_transaction_note = True
                        break

            if prev_i > -1:
                if bank not in SKIP_UNICODE_REMOVAL_LIST:
                    row_dicts[i]['transaction_note'] = remove_unicode(row_dicts[i]['transaction_note'])
                else:
                    row_dicts[i]['transaction_note'] = " ".join(row_dicts[i]['transaction_note'].split())
            if row_dicts[i]['is_transaction_row']:
                num_transaction_row_done = num_transaction_row_done + 1
                prev_i = i
                ignore_transaction_note = False
                has_used_any_row = True
            elif (len(str(row_dicts[i]['transaction_note'])) > 0) & (prev_i > -1) & (
                    num_transaction_row_done < tota_num_transaction_row) & (not ignore_transaction_note):
                row_dicts[prev_i]['transaction_note'] = (row_dicts[prev_i]['transaction_note'] if row_dicts[prev_i][
                    'transaction_note'] else '') + ' ' + (row_dicts[i]['transaction_note'] if row_dicts[i][
                        'transaction_note'] else '')
                has_used_any_row = True
            elif (len(str(row_dicts[i]['transaction_note'])) > 0) & (prev_i > -1) & (
                    num_transaction_row_done == tota_num_transaction_row) and not yet and not ignore_transaction_note:
                if add_notes(row_dicts[i], is_from_next_page=row_dicts[i].get('next_page_txn', False), bank_name = bank):
                    row_dicts[prev_i]['transaction_note'] = (row_dicts[prev_i]['transaction_note'] if row_dicts[prev_i][
                        'transaction_note'] else '') + ' ' + (row_dicts[i]['transaction_note'] if row_dicts[i][
                            'transaction_note'] else '')

                    if row_dicts[i].get('next_page_txn') and 'chq_num' in row_dicts[prev_i].keys() and not row_dicts[prev_i].get('chq_num'):
                        row_dicts[prev_i]['chq_num'] = row_dicts[i].get('chq_num','')
                    has_used_any_row = True
                else:
                    yet = True
            

            if not has_used_any_row:
                first_unused_row_indexes.append(i)
    transaction_note_modified_df = pd.DataFrame(row_dicts)
    return transaction_note_modified_df[transaction_note_modified_df['is_transaction_row'] == True], first_unused_row_indexes


def check_transaction_row(row, key=None):
    if isinstance(row['date'], datetime):
        date_format = row['date']
    elif isinstance(row['date'], str) and (len(str(row['date'])) > 5 and len(str(row['date'])) < 20):
        date_format = get_date_format(row['date'], key=key)
    elif isinstance(row['date'], str) and (len(str(row['date'])) > 19):
        date_format = get_date_format(row['date'], key=key)
    else:
        date_format = False

    if date_format is not False and validate_amount(row['balance']):
        row['is_transaction_row'] = True
        row['date_formatted'] = date_format
    else:
        row['is_transaction_row'] = False
        row['date_formatted'] = False
    end_words = ["Carried Forward", "Closing Balance"]
    if row['transaction_note'] in end_words:
        row['is_transaction_row'] = False
    return row


# TODO if amount is none then do not return the dict
def map_correct_columns(row, bank, country):
    data_to_return = dict()
    data_to_return['transaction_type'] = get_transaction_type(row, country)
    transaction_note = row.get('transaction_note')
    if transaction_note is not None:
        transaction_note = transaction_note.replace('\n', ' ')
    data_to_return['transaction_note'] = transaction_note
    chq_num = row.get('chq_num')
    if chq_num is not None:
        chq_num = chq_num.replace('\n', ' ')
    data_to_return['chq_num'] = chq_num
    data_to_return['account_number'] = row.get('account_number').strip() if isinstance(row.get('account_number'), str) else row.get('account_number')
    data_to_return['account_category'] = row.get('account_category').strip() if isinstance(row.get('account_category'), str) else row.get('account_category')
    
    amount = get_amount(row, bank)
    data_to_return['amount'] = amount
    
    if bank in ["hsbc"]:
        row['balance'] = fix_decimals(row.get('balance'))
    if bank in ["hsbc", "jnkbnk", "hdfc", "mizoram"]:
        data_to_return['transaction_merge_flag'] = row.get('transaction_merge_flag', False)
    data_to_return['balance'] = amount_to_float(row.get('balance'))
    balance_sign = get_amount_sign(row.get('balance'), bank)

    if balance_sign is not None:
        try:
            data_to_return['balance'] = data_to_return['balance'] * balance_sign
        except:
            data_to_return['balance'] = data_to_return['balance'] * balance_sign if data_to_return['balance'] != None else None

    data_to_return['date'] = row['date']
    if isinstance(row['date'], str):
        data_to_return['date'], _ = check_date(row.get('date'))

    return data_to_return


def get_transaction_type(row, country):
    transaction_type = ''
    original_transaction_type = row.get('transaction_type')

    original_credit = row.get('credit')
    if original_credit is not None:
        original_credit = original_credit.replace(",", "").replace('\n', '')

    try:
        original_credit = amount_to_float(original_credit)
    except ValueError:
        pass
    except Exception as e:
        print(e)

    original_debit = row.get('debit')
    if original_debit is not None:
        original_debit = original_debit.replace(",", "").replace('\n', '')

    try:
        original_debit = amount_to_float(original_debit)
    except ValueError:
        pass
    except Exception as e:
        print(e)

    original_autosweep = row.get('autosweep')
    if original_autosweep is not None:
        original_autosweep = original_autosweep.replace(",","").replace('\n', '')
    
    try:
        original_autosweep = amount_to_float(original_autosweep)
    except ValueError:
        pass
    except Exception as e:
        print(e)
    
    original_reverse_sweep = row.get('reverse_sweep')
    if original_reverse_sweep is not None:
        original_reverse_sweep = original_reverse_sweep.replace(",","").replace('\n', '')
    
    try:
        original_reverse_sweep = amount_to_float(original_reverse_sweep)
    except ValueError:
        pass
    except Exception as e:
        print(e)

    original_amount = row.get('amount')

    if original_transaction_type is not None:
        if (original_transaction_type.upper().find('CR') > -1):
            transaction_type = 'credit'
        elif (original_transaction_type.upper().find('C') > -1):
            transaction_type = 'credit'
        elif (original_transaction_type.upper().find('DR') > -1):
            transaction_type = 'debit'
        elif (original_transaction_type.upper().find('D') > -1):
            transaction_type = 'debit'
        elif (original_transaction_type.upper().find('K') > -1) and country in ['ID']:
            transaction_type = 'credit'
        elif country == 'ID' and (original_transaction_type.upper().find('-') > -1):
            transaction_type = 'debit'
        elif country == 'ID' and (original_transaction_type.upper().find('+') > -1):
            transaction_type = 'credit'
    elif isinstance(original_debit, float) & isinstance(original_credit, float):
        if abs(original_debit) > abs(original_credit):
            transaction_type = 'debit'
        elif abs(original_debit) < abs(original_credit):
            transaction_type = 'credit'
    elif isinstance(original_debit, float):
        transaction_type = 'debit'
    elif isinstance(original_credit, float):
        transaction_type = 'credit'
    elif isinstance(original_autosweep, float):
        transaction_type='debit'
    elif isinstance(original_reverse_sweep, float):
        transaction_type='credit'
    elif original_amount is not None:
        if (original_amount.upper().find('DR') > 0):
            transaction_type = 'debit'
        elif country == 'ID' and (original_amount.upper().find('D') > 0):
            transaction_type = 'debit'
        elif (original_amount.upper().find('CR') > 0):
            transaction_type = 'credit'
        else:
            direct_amount = amount_to_float(original_amount)
            if direct_amount is not None:
                if direct_amount > 0:
                    transaction_type = 'credit'
                    row['amount'] = abs(direct_amount)
                elif direct_amount < 0:
                    transaction_type = 'debit'
                    row['amount'] = abs(direct_amount)
    return transaction_type


def get_amount(row, bank):
    original_credit = row.get('credit')
    original_debit = row.get('debit')
    original_amount = row.get('amount')
    original_autosweep = row.get('autosweep')
    original_reverse_sweep = row.get('reverse_sweep')

    ocr_banks = ['hsbc']
    if bank in ocr_banks:
        original_credit = fix_decimals(original_credit)
        original_debit =  fix_decimals(original_debit)
        original_amount = fix_decimals(original_amount)

    credit_amount = amount_to_float(original_credit)
    debit_amount = amount_to_float(original_debit)
    direct_amount = amount_to_float(original_amount)
    autosweep_amount = amount_to_float(original_autosweep)
    reverse_sweep_amount = amount_to_float(original_reverse_sweep)

    skip_sign_banks_list = ["alrajhi", "ncb"]
    if bank in skip_sign_banks_list:
        credit_amount = abs(credit_amount) if credit_amount is not None else None
        debit_amount = abs(debit_amount) if debit_amount is not None else None
        direct_amount = abs(direct_amount) if direct_amount is not None else None
    if reverse_sweep_amount!=None:
        credit_amount = reverse_sweep_amount
    if autosweep_amount!=None:
        debit_amount = autosweep_amount

    if direct_amount is not None:
        return direct_amount
    elif credit_amount is not None and debit_amount is not None:
        if abs(credit_amount) >= abs(debit_amount):
            return credit_amount
        elif abs(debit_amount) > abs(credit_amount):
            return debit_amount
    elif credit_amount is not None:
        return credit_amount
    elif debit_amount is not None:
        return debit_amount
    else:
        return None

def get_amount_sign_cc(amount_string):
    amount_sign = match_regex(amount_string, '(?i).*(cr|dr|d|c).*', 1)
    if amount_sign is not None:
        if amount_sign.upper() in ['CR','C']:
            return -1
        elif amount_sign.upper() in ['DR','D']:
            return 1
    return None

def get_image(path, password):
    table_setting_temp = {
        'explicit_vertical_lines': [
            50, 210, 280, 335, 400, 460, 540
        ],
        'horizontal_strategy': 'lines',
        'snap_tolerance': 5,

    }

    pages = get_pages(path, password)
    page = pages[0]
    im = page.to_image()
    k = im.reset().debug_tablefinder(table_setting_temp)
    k.save('/Users/nikhil/Downloads/test', format="PNG")


def get_image_cropped(path, password):
    pages = get_pages(path, password)
    page = pages[0]
    bbox = (10, 50, 500, 350)
    cropped_page = page.within_bbox(bbox)
    im = cropped_page.to_image()
    k = im.reset()
    k.save('/Users/nikhil/Downloads/test', format="PNG")


def get_table(path, password):
    table_setting_temp = {
        'explicit_vertical_lines': [50, 210, 280, 335, 400, 460, 540],
        'horizontal_strategy': 'lines',
        'snap_tolerance': 2,
        'vertical_strategy': 'explicit'
    }
    pages = get_pages(path, password)
    page = pages[0]
    table_data = page.extract_tables(table_setting_temp)
    print(table_data)
