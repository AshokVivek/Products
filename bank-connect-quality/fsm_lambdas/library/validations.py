from library.utils import check_format_row, check_date, get_bank_threshold_diff, validate_amount, check_transaction_beginning, get_date_format
import warnings
import pandas as pd
import re
from library.utils import EPOCH_DATE, remove_unicode, date_regexes, amount_to_float, get_amount_sign
from library.helpers.constants import (
    DEFAULT_BALANCE_FLOAT,
    DEFAULT_TIMESTAMP_UTC,
    DEFAULT_DATE,
    DEFAULT_BALANCE_STRING,
    BCABNK_COMPLETE_TRSNSACTION_TOLERANCE,
    REGEXES_TO_SANITIZE_TRXN_COLUMN,
)
from datetime import datetime
from copy import deepcopy
from typing import Union


warnings.simplefilter(action = "ignore", category = FutureWarning)
pd.options.mode.chained_assignment = None



def format_stanchar_rows(df):
    '''
        Populate Dates to transactions that do not have it's own date but follow the date of parent transaction.
    '''
    df = df.apply(lambda row: check_format_row(row), axis=1)
    total_num_transaction_row = df[df['is_balance'] == True].shape[0]
    num_transaction_row_done = 0
    row_dicts = df.to_dict('records')
    transaction_started = False
    for i in range(0, len(row_dicts)):
        if row_dicts[i]["transaction_note"].lower() == "total":
            # signifies end of the stanchar statement
            break
        elif row_dicts[i]["is_date_used"] and row_dicts[i]["is_balance"] and num_transaction_row_done <= total_num_transaction_row:
            transaction_started = True
            num_transaction_row_done += 1
            prev_date = row_dicts[i]["date"]
        elif transaction_started and num_transaction_row_done <= total_num_transaction_row:
            if row_dicts[i]["is_balance"] and not row_dicts[i]["is_date_used"]:
                row_dicts[i]["date"] = prev_date
                num_transaction_row_done += 1                
    df = pd.DataFrame(row_dicts)
    df.drop(['is_balance', 'is_date_used'], axis=1, inplace=True)
    return df

def back_fill_balance(transactions, default_balance):
    '''
        Pick the amount and balance of last transaction, and back fill the balance to transactions by traversing in reverse.
    '''
    #update default balance
    calculated = None
    for i in range(len(transactions)-1, -1, -1):
        for j in range(len(transactions[i])-1, -1, -1):
            type = transactions[i][j]['transaction_type']
            amount = transactions[i][j]['amount']
            balance = transactions[i][j]['balance']
            if type == 'credit' and balance != default_balance:
                calculated = balance - amount
            elif type == 'debit' and balance != default_balance:
                calculated = balance + amount
            elif type == 'credit' and balance == default_balance and calculated is not None:
                balance = round(calculated, 2)
                transactions[i][j]['balance'] = balance
                calculated = balance - amount
            elif type == 'debit' and balance == default_balance and calculated is not None:
                balance = round(calculated, 2)
                transactions[i][j]['balance'] = balance
                calculated = balance + amount
    return transactions

def check_default_balance(transactions, default_balance, bank):
    for i in range(0, len(transactions), 1):
        for j in range(0, len(transactions[i]), 1):
            
            if bank == 'hsbc' and "transaction_merge_flag" in transactions[i][j].keys():
                return False
            
            if transactions[i][j]['balance'] == default_balance:
                return True
    return False

def front_fill_balance(transactions, opening_balance, closing_balance, default_balance, bank):
    '''
        Pick the opening balance, and fill the balance to transactions.
    '''
    if not check_default_balance(transactions, default_balance, bank):
        return transactions, set()
    
    if not isinstance(opening_balance, float) and bank == 'bcabnk':
        if len(transactions) > 0 and len(transactions[0]) > 0:
            opening_balance = transactions[0][0]['balance']

    if opening_balance in [None, ''] and closing_balance in [None, ''] :
        return [[]]*len(transactions), set()
    
    #TODO solve for case where all transactions are of same date but the order of transactions is reversed
    order = check_date_order(transactions)
    if order is None:
        return [[]]*len(transactions), set()

    calculated = opening_balance
    if order == "reverse" and len(transactions) > 0 and len(transactions[0]) > 0:
        if closing_balance not in [None, '']:
            transactions[0][0]['balance'] = closing_balance
            calculated = closing_balance
        else:
            return [[]]*len(transactions), set()
    
    total_txns = 0
    count_unpdated_txn = 0
    pages_updated = set()

    for i in range(0, len(transactions), 1):
        total_txns += len(transactions[i])
        for j in range(0, len(transactions[i]), 1):

            type = transactions[i][j]['transaction_type']
            amount = transactions[i][j]['amount']
            balance = transactions[i][j]['balance']
            page_num = transactions[i][j].get('page_number', None)
            if type == 'credit' and balance != default_balance:
                if order == "reverse":
                    calculated = balance - amount
                    continue
                calculated = balance 
            elif type == 'debit' and balance != default_balance:
                if order == "reverse":
                    calculated = balance + amount
                    continue
                calculated = balance 
            elif type == 'credit' and balance == default_balance and calculated is not None:
                balance = round(calculated, 2)
                if order == "reverse":
                    transactions[i][j]['balance'] = round(calculated, 2)
                    calculated = balance - amount
                    pages_updated.add(page_num)
                    continue
                calculated = balance + amount
                transactions[i][j]['balance'] = round(calculated, 2)
                pages_updated.add(page_num)
            elif type == 'debit' and balance == default_balance and calculated is not None:
                balance = round(calculated, 2)
                if order == "reverse":
                    transactions[i][j]['balance'] = round(calculated, 2)
                    pages_updated.add(page_num)
                    calculated = balance + amount
                    continue
                calculated = balance - amount
                transactions[i][j]['balance'] = round(calculated, 2)
                pages_updated.add(page_num)
            
            if transactions[i][j]['balance'] == default_balance:
                count_unpdated_txn += 1

    if total_txns > 0  and count_unpdated_txn/total_txns > 0.7:
        return [[]]*len(transactions), set()
    
    return transactions, pages_updated

def check_date_order(transactions, bank=''):
    start_date = None
    end_date = None
    THRESHOLD_DIFF = get_bank_threshold_diff(bank)

    for i in range(len(transactions)):
        if len(transactions[i]) > 0:
            start_date = transactions[i][0].get('date')
            break

    for i in range(len(transactions)-1, -1, -1):
        if len(transactions[i]) > 0:
            end_date = transactions[i][-1].get('date')
            break

    if start_date is not None and end_date is not None:
        if start_date < end_date:
            return "correct"
        if start_date > end_date:
            return "reverse"
        if start_date==end_date:
            is_reverse_list = []
            is_correct_list = []
            for i in range(len(transactions)):
                for j in range(len(transactions[i])):
                    if j==len(transactions[i])-1:
                        break
                    first_transaction = deepcopy(transactions[i][j])
                    second_transaction = deepcopy(transactions[i][j+1])
                    
                    first_transaction['amount'] = first_transaction['amount'] if first_transaction['transaction_type']=='credit' else -first_transaction['amount']
                    second_transaction['amount'] = second_transaction['amount'] if second_transaction['transaction_type']=='credit' else -second_transaction['amount']
                    
                    is_correct = abs((first_transaction['balance']+second_transaction['amount']) - second_transaction['balance']) <= THRESHOLD_DIFF
                    is_reverse = abs((second_transaction['balance']+first_transaction['amount']) - first_transaction['balance']) <= THRESHOLD_DIFF
                    
                    is_correct_list.append(is_correct)
                    is_reverse_list.append(is_reverse)

            if all(is_reverse_list) and all(is_correct_list):
                return 'correct'
            elif all(is_reverse_list):
                return 'reverse'
            else:
                return 'correct'

    return None

def populate_transaction_notes(df):
    '''
        For the cases where transaction note just comes after the date,
        Pick the transaction note from date column, and fill to transaction column.
    '''
    row_dicts = df.to_dict('records')
    for transaction in row_dicts:
        index = row_dicts.index(transaction)
        current_transaction = row_dicts[index]
        date, _ = check_date(current_transaction['date'])
        if date and index+1 < len(row_dicts):
            next_transaction = row_dicts[index+1]
            if not validate_amount(next_transaction['amount']) and not validate_amount(next_transaction['balance']):
                current_transaction['transaction_note'] = next_transaction.get('date', '')
                del row_dicts[index+1]
    df = pd.DataFrame(row_dicts)
    return df

def populate_dates(df):
    '''
        Populate a default date to transactions missing date, specifically for the cases where date has to be fetched from previous page.
    '''
    row_dicts = df.to_dict('records')
    prev_date = ''
    
    for index in range(len(row_dicts)):
        if check_transaction_beginning(row_dicts[index].get('amount', ''), True):
            is_next_date, _ = check_date(row_dicts[index+1]['date'])
            if not is_next_date:
                row_dicts[index+1]['date'] = EPOCH_DATE
        if check_transaction_beginning(row_dicts[index]['transaction_note']):
            break
        datetime_date, _ = check_date(row_dicts[index]['date'])
        if datetime_date:
            prev_date = datetime_date
        if isinstance(row_dicts[index]['date'], str) and row_dicts[index]['date'].strip() == '' and \
            (validate_amount(row_dicts[index].get('debit', '')) or validate_amount(row_dicts[index].get('credit', '')) or validate_amount(row_dicts[index].get('amount', ''))):
            
            row_dicts[index]['date'] = prev_date
    df = pd.DataFrame(row_dicts)
    return df

def mark_negative_balances(row):
    if isinstance(row['balance'], str):
        if re.match(r'^\([\d\.\, ]+\)*$', row['balance'].strip()) is not None :
            # substituting bracket with a negative sign appended so that the normal flow captures negative balance automatically
            row['balance'] = re.sub(r'^\(', "(-", row['balance'])
        elif re.match(r'(?i)^(D)\s*[0-9\,\. ]+', row['balance'].strip()) is not None:
            # substituting D with a negative sign appended so that the normal flow captures negative balance automatically
            row['balance'] = re.sub(r'D', "-", row['balance'])
        elif re.match(r'(?i)^\s*[0-9\,\.]+\s*\-', row['balance'].strip()) is not None:
            row['balance'] = '-' + row['balance']
    return row

def populate_merge_flag(df):
    '''
        Populate a new key flag to transactions that need fixes and have to be post processed. 
    '''
    df = df.apply(lambda row: check_format_row(row), axis=1)
    df["transaction_merge_flag"] = False
    total_num_transaction_row = df[df['is_balance'] == True].shape[0]
    num_transaction_row_done = 0
    row_dicts = df.to_dict('records')
    transaction_started = False
    str_epoch_date = EPOCH_DATE.strftime("%d-%m-%Y")
    prev_date = str_epoch_date
    default_amount = "-1.0"
    transaction_i = -1
    next_transaction_i = -1
    for i in range(0, len(row_dicts)):
        if check_transaction_beginning(row_dicts[i]["transaction_note"], True) or check_transaction_beginning(row_dicts[i]["date"], True):
            next_transaction_i = i+1
        elif (check_transaction_beginning(row_dicts[i]["transaction_note"], False) or check_transaction_beginning(row_dicts[i]["date"],False)) and next_transaction_i > -1 and not row_dicts[next_transaction_i]["is_balance"]:
            if row_dicts[next_transaction_i]["transaction_note"].strip() != "":
                row_dicts[next_transaction_i]["transaction_merge_flag"] = True
                row_dicts[next_transaction_i]["date"] = prev_date
                row_dicts[next_transaction_i]["credit"] = default_amount
                row_dicts[next_transaction_i]["balance"] = default_amount
                row_dicts[i]["balance"] = ""
            break
        elif row_dicts[i]["is_date_used"] and num_transaction_row_done <= total_num_transaction_row:
            transaction_started = True
            if row_dicts[i]["is_balance"]:
                transaction_started = False
                num_transaction_row_done += 1
                next_transaction_i = i+1
            else:
                transaction_i = i
                prev_date = row_dicts[i]["date"]
        elif transaction_started and num_transaction_row_done <= total_num_transaction_row:
            if row_dicts[i]["is_balance"]:
                row_dicts[transaction_i]["credit"] = row_dicts[i]["credit"]
                row_dicts[transaction_i]["debit"] = row_dicts[i]["debit"]
                row_dicts[transaction_i]["balance"] = row_dicts[i]["balance"]
                row_dicts[transaction_i]["is_balance"] = row_dicts[i]["is_balance"]
                if i!=transaction_i:
                    row_dicts[i]["credit"] = ""
                    row_dicts[i]["debit"] = ""
                    row_dicts[i]["balance"] = ""
                    row_dicts[i]["is_balance"] = row_dicts[i]["is_balance"]
                transaction_started = False
                num_transaction_row_done += 1
                next_transaction_i = i+1
        elif row_dicts[i]["is_balance"] and num_transaction_row_done <= total_num_transaction_row and next_transaction_i>-1:
            if prev_date == str_epoch_date:
                row_dicts[next_transaction_i]["transaction_merge_flag"] = True
            row_dicts[next_transaction_i]["date"] = prev_date
            row_dicts[next_transaction_i]["credit"] = row_dicts[i]["credit"]
            row_dicts[next_transaction_i]["debit"] = row_dicts[i]["debit"]
            row_dicts[next_transaction_i]["balance"] = row_dicts[i]["balance"]
            row_dicts[next_transaction_i]["is_balance"] = row_dicts[i]["is_balance"]
            if i!=next_transaction_i or check_transaction_beginning(row_dicts[i]["transaction_note"], False):
                row_dicts[i]["date"] = ""
                row_dicts[i]["credit"] = ""
                row_dicts[i]["debit"] = ""
                row_dicts[i]["balance"] = ""
                row_dicts[i]["is_balance"] = row_dicts[i]["is_balance"]
            transaction_started = False
            num_transaction_row_done += 1
            next_transaction_i = i+1
    df = pd.DataFrame(row_dicts)
    df.drop(['is_balance', 'is_date_used'], axis=1, inplace=True)
    df.replace(r'^s*$', float('NaN'), regex = True, inplace=True)
    df.dropna(how='all', inplace=True)
    df.replace(float('NaN'), '', inplace=True)
    return df

def populate_debit_from_note(df):
    '''
        For cases, where transaction_note and debit columns are merged.
    '''
    # Added a check for BNI Bank, if amount is present in place of debit and credit then return the original dataframe.
    if 'debit' not in list(df.columns) and 'credit' not in list(df.columns):
        return df
    row_dicts = df.to_dict('records')
    for i in range(0, len(row_dicts)):
        if row_dicts[i]['debit'].strip() == '' and row_dicts[i]['credit'].strip() == '':
            debit_match = re.match(r'.*?\s*([0-9\,]+\.[0-9]{2})$', row_dicts[i]['transaction_note'].strip())
            if debit_match is not None:
                debit_str = debit_match.group(1)
                row_dicts[i]['debit'] = debit_str
                row_dicts[i]['transaction_note'] = row_dicts[i]['transaction_note'].replace(debit_str, '').strip()
    df = pd.DataFrame(row_dicts)
    return df

def update_epoch_date(transactions, epoch_date):
    '''
        Update the epoch date of transaction to it's desired date.
    '''
    prev_date = ''
    for i in range(len(transactions)):
        for j in range(len(transactions[i])):
            if transactions[i][j]['date'] != epoch_date:
                prev_date = transactions[i][j]['date']
            elif prev_date and transactions[i][j]['date'] == epoch_date:
                transactions[i][j]['date'] = prev_date
    return transactions

def format_ID_currency(df, country='ID', bank=''):
    '''
        Convert the indonesian currency format to general currency format
        Example - 12.000,10 -> 12,000.10
        Converts amount with DR at end to - balance in maybnk
        Example > 7.219.835.529,50DR to -7.219.835.529,50
    '''
    if country not in ['ID']:
        return df
    row_dicts = df.to_dict('records')
    for i in range(0, len(row_dicts)):
        for _type in ['debit', 'credit', 'amount', 'balance']:
            if bank == 'maybnk' and _type == 'balance' and _type in row_dicts[i].keys():
                if len(row_dicts[i]['balance'].split()) == 1 and row_dicts[i]['balance'].endswith('DR'):
                    row_dicts[i]['balance'] = '-' + row_dicts[i]['balance'].replace('DR', '')
            if _type in row_dicts[i].keys():
                    if len(row_dicts[i][_type]) > 2 and row_dicts[i][_type][-3] == ',':
                        row_dicts[i][_type] = row_dicts[i][_type].replace('.', '')
                        row_dicts[i][_type] = row_dicts[i][_type].replace(',', '.')
    df = pd.DataFrame(row_dicts)
    return df

def fix_bni_amount(df):
    # TODO: Remove coordinate duplicates of words in `parse_table`.
    # Multiple cases here like 0.00, .00 , digit
    # Hence using the amount with maximum length here
    if 'amount' not in list(df.columns):
        return df
    row_dicts = df.to_dict('records')
    for i in range(0, len(row_dicts)):
        amt_list = row_dicts[i]['amount'].split()
        final_amt = ''
        max_len = -1
        for amt in amt_list:
            if len(amt) >= max_len:
                max_len = len(amt)
                final_amt = amt
        row_dicts[i]['amount'] = final_amt
    df = pd.DataFrame(row_dicts)
    return df

def complete_transction(df):
    row_dicts = df.to_dict('records')
    buffer_size = BCABNK_COMPLETE_TRSNSACTION_TOLERANCE
    for i in range(len(row_dicts)):
        if row_dicts[i]['date'] in ['',None] and row_dicts[i]['balance'] in ['',None] and row_dicts[i]['transaction_note'] not in ['',None] and row_dicts[i]['amount'] not in ['',None]:
            row_dicts[i]['date'] = DEFAULT_DATE
            row_dicts[i]['balance'] = DEFAULT_BALANCE_STRING
        elif row_dicts[i]['date'] not in ['',None] and row_dicts[i]['amount'] in ['CR','DR','DB','',None] and row_dicts[i]['balance'] not in ['',None]:
            row_dicts[i]['amount'] = DEFAULT_BALANCE_STRING
        else:
            buffer_size = buffer_size-1
        
        if buffer_size<0:
            break

    
    buffer_size = BCABNK_COMPLETE_TRSNSACTION_TOLERANCE
    for i in range(len(row_dicts)-1,-1,-1):
        if row_dicts[i]['date'] in ['',None] and row_dicts[i]['balance'] in ['',None] and row_dicts[i]['transaction_note'] not in ['',None] and row_dicts[i]['amount'] not in ['',None]:
            row_dicts[i]['date'] = DEFAULT_DATE
            row_dicts[i]['balance'] = DEFAULT_BALANCE_STRING
        elif row_dicts[i]['date'] not in ['',None] and row_dicts[i]['amount'] in ['CR','DR','DB','',None] and row_dicts[i]['balance'] not in ['',None]:
            row_dicts[i]['amount'] = DEFAULT_BALANCE_STRING
        else:
            buffer_size = buffer_size-1
        
        if buffer_size<0:
            break

    df = pd.DataFrame(row_dicts)
    return df

def connect_transaction(all_transactions):
    for i in range(len(all_transactions)-1):
        len1 = len(all_transactions[i])
        len2 = len(all_transactions[i+1])
        if len2>0 and len1>0 and all_transactions[i][len1-1]['balance']==all_transactions[i+1][0]['amount'] and all_transactions[i+1][0]['amount']==DEFAULT_BALANCE_FLOAT and all_transactions[i][len1-1]['date'] == DEFAULT_TIMESTAMP_UTC:
            all_transactions[i][len1-1]['balance'] = all_transactions[i+1][0]['balance']
            all_transactions[i][len1-1]['date'] = all_transactions[i+1][0]['date']
            all_transactions[i][len1-1]['transaction_note'] = all_transactions[i][len1-1]['transaction_note'] + " " + all_transactions[i+1][0]['transaction_note']
            if len1-2>=0:
                if all_transactions[i][len1-2]['balance']-all_transactions[i][len1-1]['amount']==all_transactions[i][len1-1]['balance']:
                    all_transactions[i][len1-1]['transaction_type'] = 'debit'
                else:
                    all_transactions[i][len1-1]['transaction_type'] = 'credit'
            all_transactions[i+1].pop(0)
    
    for i in range(len(all_transactions)):
        index_from_starting = -1

        for j in range(len(all_transactions[i])//2):
            if ( all_transactions[i][j]['balance']==DEFAULT_BALANCE_FLOAT or all_transactions[i][j]['amount']==DEFAULT_BALANCE_FLOAT or all_transactions[i][j]['date']==DEFAULT_TIMESTAMP_UTC ):
                index_from_starting = j
        
        if index_from_starting!=-1:
            all_transactions[i] = all_transactions[i][index_from_starting+1:]
        
        index_from_ending = len(all_transactions[i])
        for j in range(len(all_transactions[i])-1,-1,-1):
            if ( all_transactions[i][j]['balance']==DEFAULT_BALANCE_FLOAT or all_transactions[i][j]['amount']==DEFAULT_BALANCE_FLOAT or all_transactions[i][j]['date']==DEFAULT_TIMESTAMP_UTC ):
                index_from_ending = j
        
        if index_from_ending!=len(all_transactions[i]):
            all_transactions[i] = all_transactions[i][:index_from_ending]
            
    return all_transactions


def correct_2row_transaction_notes(all_transactions):
    for i in range(1, len(all_transactions)):
        len1 = len(all_transactions[i - 1])
        len2 = len(all_transactions[i])
        if (
            len1 > 0
            and len2 > 0
            and all_transactions[i][0]["amount"] == DEFAULT_BALANCE_FLOAT
            and all_transactions[i][0]["balance"] == DEFAULT_BALANCE_FLOAT
            and all_transactions[i][0]["date"] == DEFAULT_TIMESTAMP_UTC
        ):
            prosthetic_transaction = all_transactions[i].pop(0)
            all_transactions[i - 1][len1 - 1]["transaction_note"] = prosthetic_transaction["transaction_note"]
        if (
            len1 > 0
            and all_transactions[i - 1][0]["amount"] == DEFAULT_BALANCE_FLOAT
            and all_transactions[i - 1][0]["balance"] == DEFAULT_BALANCE_FLOAT
            and all_transactions[i - 1][0]["date"] == DEFAULT_TIMESTAMP_UTC
        ):
            all_transactions[i - 1].pop(0)
    return all_transactions


def fix_last_txn_type(df):
    """
        this function transforms the transaction type of last transaction
        depending on the balance of second last transaction.
    """
    row_dicts = df.to_dict('records')
    calculated_balance = 0

    if len(row_dicts) > 1:
        calculated_balance = row_dicts[-2]['balance']
    else:
        return df
    
    extracted_balance = row_dicts[-1]['balance']
    amount = row_dicts[-1]['amount']
    txn_type = row_dicts[-1]['transaction_type']
    if txn_type == 'credit':
        calculated_balance = calculated_balance + amount
    else:
        calculated_balance = calculated_balance - amount
    if abs(calculated_balance - extracted_balance) == 2*abs(amount):
        if txn_type == 'credit':
            row_dicts[-1]['transaction_type'] = 'debit'
        else:
            row_dicts[-1]['transaction_type'] = 'credit'
    df = pd.DataFrame(row_dicts)
    return df
    

def sanitize_ubi_hidden_rows(df, each_parameter):
    """
    This method is used to sanitize extracted values of hidden transactions of UBI bank.
    This hidden row might contain the values like:
    random          : 14(any integral value)
    date            : 12/10/2023 NEFT : NEFT :
    random2         : S10403866 National Electronic Fund Transfer National Electronic Fund Transfer
    transaction_note: UPIAR/328517708097/DR/M PREMKR/PYTM /paytmqrb95025n | UPI : Unified Payment Interface This is system generated statement and https://www.unionbankofindia.co.in | UPI : Unified Payment Interface This is system generated statement and https://www.unionbankofindia.co.in
    amount          : 285.00 (Dr) does not require signature does not require signature
    balance         : 8887.07 (Cr)

    :return : This method would sanitize the date, transaction_note, and amount if being extracted from a particular template and
                return the sanitized df.
    """
    row_dicts = df.to_dict('records')
    transaction_table_columns = df.columns
    for i in range(len(row_dicts)):
        if 'date' in transaction_table_columns:
            date_before_sanitization = row_dicts[i]['date'].strip()
            if date_before_sanitization:
                date_match = re.match(r'(?i).*([0-9\/]{10})\s+NEFT.*', date_before_sanitization)
                if date_match:
                    row_dicts[i]['date'] = date_match.group(1)
        if 'transaction_note' in transaction_table_columns:
            note_before_sanitization = row_dicts[i]['transaction_note'].strip()
            if note_before_sanitization:
                note_match = re.match(r'(?i)(.*)\s+\|\s+UPI\s+:.*\s+\|\s+UPI\s+:.*', note_before_sanitization)
                if note_match:
                    row_dicts[i]['transaction_note'] = note_match.group(1)
        if 'amount' in transaction_table_columns:
            amnt_before_sanitization = row_dicts[i]['amount'].strip()
            if amnt_before_sanitization:
                amnt_match = re.match(r'(?i)^(\d+\.\d+\s+\(Dr\))\s+does\s+.*', amnt_before_sanitization)
                if amnt_match:
                    row_dicts[i]['amount'] = amnt_match.group(1)
    df = pd.DataFrame(row_dicts)
    return df

def correct_transaction_type(transactions, opening_balance):

    if not isinstance(opening_balance, float):
        return [[]]*len(transactions)
    
    prev_balance = opening_balance
    if len(transactions) > 0 and len(transactions[0]) > 0:
        balance = transactions[0][0]['balance']
        amount = transactions[0][0]['amount']
        if opening_balance + amount <= balance:
            transactions[0][0]['transaction_type'] = 'credit'
        else:
            transactions[0][0]['transaction_type'] = 'debit'
        prev_balance = balance
    
    for i in range(len(transactions)):
        for j in range(len(transactions[i])):
            if i == 0 and j == 0:
                continue
            balance = transactions[i][j]['balance']
            if balance > prev_balance:
                transactions[i][j]['transaction_type'] = 'credit'
            else:
                transactions[i][j]['transaction_type'] = 'debit'
            prev_balance = balance
    
    return transactions

def fix_karur_numericals(df):
    """
        Checks for credit, debit and balance amount and fix if its wrongly detected by OCR.
        Ignores the amount in Indian Numeral System (##,###.00).
        Remove special charracter ocr notations and decimal it to 100.
    """
    df_columns = list(df.columns)
    if 'credit' not in df_columns or 'debit' not in df_columns or 'balance' not in df_columns:
        return df
    
    def floating(transaction, type, amount):
        replace = ',.:;<'
        if not re.match(r'(^[0-9]{1,3}(?:,[0-9]{2,3})*\.[0-9]{2}$)', amount):
            for ch in replace:
                amount = amount.replace(ch, '')
            if amount.isdigit() and len(amount)>2:
                transaction[type] = amount[:-2]+'.'+amount[-2:]
    
    row_dicts = df.to_dict('records')
    for transaction in row_dicts:
        fix_credit = transaction['credit']
        fix_debit = transaction['debit']
        fix_balance = transaction['balance']
        
        floating(transaction, 'credit', fix_credit)
        floating(transaction, 'debit', fix_debit)
        floating(transaction, 'balance', fix_balance)
    df = pd.DataFrame(row_dicts)
    return df

def fill_missing_balance(df, key=None):
    """
    Fill missing balance values with 0.0 if the current transaction is debit and the debit amount
    is equal to the previous transaction's balance, or if there are no transactions, it should be equal
    to the opening balance. Ignore otherwise. 
    """

    if 'balance' not in list(df.columns) or 'debit' not in list(df.columns):
        return df
    
    row_dicts = df.to_dict('records')
    for i in range(len(row_dicts)):
        if(
            get_date_format(row_dicts[i]['date'], key) 
            and row_dicts[i]['balance'].strip()=='' 
            and row_dicts[i]['transaction_note'].strip() 
            and (
                    validate_amount(row_dicts[i]['debit'].strip()) 
                    or validate_amount(row_dicts[i]['credit'].strip())
                )
        ): # check for valid transaction row
            balance_to_check = None
            if i-1>=0:
                balance = amount_to_float(row_dicts[i-1]['balance'])
                debit = amount_to_float(row_dicts[i]['debit'])
                credit = amount_to_float(row_dicts[i]['credit'])
                amount = -debit if debit else credit
                balance_to_check = balance+amount if amount and balance else None
            if balance_to_check==None and i+1<len(row_dicts):
                debit = amount_to_float(row_dicts[i+1]['debit'])
                credit = amount_to_float(row_dicts[i+1]['credit'])
                balance = amount_to_float(row_dicts[i+1]['balance'])
                amount = -debit if debit else credit
                balance_to_check = balance-amount if amount and balance else None
            if balance_to_check==0:
                row_dicts[i]['balance'] = '0'
    df = pd.DataFrame(row_dicts)
    return df

def convert_date_to_date_format(df, date_format):
    # Convert transaction date to a specific date format extracted from transaction template
    transactions = df.to_dict('records')
    for i in range(len(transactions)):
        try:
            transactions[i]['date'] = remove_unicode(transactions[i]['date'])
            for date_regex in date_regexes:        
                date_matched = re.match(date_regex, transactions[i]['date'])
                if date_matched is not None:
                    transactions[i]['date'] = date_matched.group(1)
                    break
            transactions[i]['date'] = datetime.strptime(transactions[i]['date'], date_format).strftime('%d %B %Y')
        except (ValueError, TypeError):
            continue
    df = pd.DataFrame(transactions)
    return df

def fix_kotak_repeated_amount(df):
    """
    remove repeated amount value +2,000.00 +2,000.00 to +2,000.00
    """
    df_columns = list(df.columns)
    if 'transaction_type' in df_columns or 'amount' not in df_columns:
        return df
    
    transactions = df.to_dict('records')
    for i in range(len(transactions)):
        try:
            if 'amount' in transactions[i]:
                amounts = transactions[i]['amount'].split()
                if len(amounts) == 2:
                    transactions[i]['amount'] = amounts[0]
        except (ValueError, TypeError):
            continue
    df = pd.DataFrame(transactions)
    return df

def absolute_negative_debits(transactions_list, key=''):
    """
    Absolute negative amount in debits.
    """
    debit_transactions = 0
    negative_debit_transactions = 0
    signed_transactions = deepcopy(transactions_list)
    for i in range(len(signed_transactions)):
        if signed_transactions[i]['transaction_type']=='debit':
            debit_transactions += 1
            if signed_transactions[i]['amount']<0:
                negative_debit_transactions += 1
                signed_transactions[i]['amount'] = abs(signed_transactions[i]['amount'])
    if debit_transactions>0 and negative_debit_transactions>0:
        if negative_debit_transactions/debit_transactions>0.7:
            return signed_transactions
        else:
            print(f"Key - {key}, Negative Debit Transactions - {negative_debit_transactions}, Debit Transactions - {debit_transactions}")
    return transactions_list

def fix_canara_repeated_amount_in_first_txn(df):
    """
    remove double value in balance like  1,686.80 31,611.80 to 31,611.80
    remove extra value in credit like  Opening Balance 29,925.00 to 29,925.00
    """
    df = df.apply(lambda row: check_format_row(row), axis=1)
    df_columns = list(df.columns)
    if 'amount' in df_columns or 'credit' not in df_columns or 'debit' not in df_columns:
        return df
    
    transactions = df.to_dict('records')
    for i in range(len(transactions)):
        if transactions[i]["is_date_used"]:
            credit = transactions[i]['credit'].split()
            if len(credit) == 3:
                transactions[i]['credit'] = credit[2]
            debit = transactions[i]['debit'].split()
            if len(debit) == 2:
                transactions[i]['debit'] = debit[1]
            balance = transactions[i]['balance'].split()
            if len(balance) == 2:
                transactions[i]['balance'] = balance[1]
            break
    df = pd.DataFrame(transactions)
    return df


def get_balance_from_amount_dhanlaxmi(df):
    """
    amount eg: Balance : 58,737.40 50,000.00 Cr
               Balance : 58,347.40 390.00 Dr
           eg: 360000 - Dr Balance : 16,39,935.46
                20000 - Dr Balance : 1,04,857.46
    extracting balance and amount using regex and filling balance and amount column with proper values
    """
    df_columns = list(df.columns)
    if "credit" in df_columns or "debit" in df_columns or "amount" not in df_columns:
        return df

    transactions = df.to_dict("records")

    regexes = [
        {
            "balance": {
                "re": r"(?i)^balance\s*\:?\s*([0-9\-\,\. ]+)\s+[0-9\,\.]+\s+(?:cr|dr)$",
                "groups": 1,
            },
            "amount": {
                "re": r"(?i)^balance\s*\:?\s*\-?\s*[0-9\,\.]+\s+([0-9\,\.\-\s]+\s+(?:cr|dr))$",
                "groups": 1,
            },
        },
        {
            "balance": {"re": r".*Balance\s*:\s*([\d,]+\.\d+)", "groups": 1},
            "amount": {"re": r"^(\d+(?:\.\d+)?)\s*-\s*(Cr|Dr)", "groups": 2},
        },
    ]

    for ind in range(len(transactions)):
        raw_amount = transactions[ind]["amount"]
        if not raw_amount:
            continue
        for rx in regexes:
            balance = re.match(rx["balance"]["re"], raw_amount)
            amount = re.match(rx["amount"]["re"], raw_amount)
            amount_groups_len = rx["amount"]["groups"]
            balance_groups_len = rx["balance"]["groups"]
            # Only if the groups count match, proceed to append the amount and balance to transactions
            if (
                (amount and balance)
                and amount_groups_len == len(amount.groups())
                and balance_groups_len == len(balance.groups())
            ):
                transactions[ind]["amount"] = "".join(
                    [amount.group(i) for i in range(1, amount_groups_len + 1)]
                )
                transactions[ind]["balance"] = "".join(
                    [balance.group(i) for i in range(1, balance_groups_len + 1)]
                )
                break

    return pd.DataFrame(transactions).fillna("")


def fix_jpmorgan_amount_balance(df):
    df_columns = list(df.columns)
    if 'credit' not in df_columns or 'debit' not in df_columns or 'balance' not in df_columns:
        return df
    
    transactions = df.to_dict('records')
    for i in range(len(transactions)):
        debit = transactions[i]['debit']
        credit = transactions[i]['credit']
        balance = transactions[i]['balance']
        if (len(debit.split()) == 2 or len(credit.split()) == 2) and len(balance.split()) == 2:
            balance = balance.split()[1]
            if len(debit.split()) == 2:
                debit = debit.split()[0]
            else:
                credit = credit.split()[0]
        if balance == credit or balance == debit:
            balance = ''
            credit = ''
            debit = ''
        if debit.startswith('-'):
            debit = debit[1:]
        transactions[i]['debit'] = debit
        transactions[i]['credit'] = credit
        transactions[i]['balance'] = balance
    
    df = pd.DataFrame(transactions)
    return df

def fix_balance_amount(df):
    df_columns = list(df.columns)
    
    if 'balance' not in df_columns:
        return df
    
    transactions = df.to_dict('records')
    regex_list = [
        "(?i)([0-9\\,\\.]+\\s*(cr|dr))\\s*page\\s*[0-9]+\\s*(\\/|of)\\s*[0-9]+\\s*"
    ]
    for i in range(len(transactions)):
        for regex in regex_list:
            match = re.match(regex, transactions[i]['balance'])
            if match:
                transactions[i]['balance'] = match.group(1)
    
    df = pd.DataFrame(transactions)
    return df

def complete_split_balance_from_next_line(bank, df):
    df_columns = list(df.columns)
    if 'balance' not in df_columns:
        return df
    
    transactions = df.to_dict('records')
    regex_list = {
        "yesbnk": [
                   {
                    "prefix_balance_regex": r"(?i)\s*inr\s+\-*[0-9\,]+\.$",
                    "suffix_balance_regex": r"^\d{2}$"
                   },
                   {
                    "prefix_balance_regex": r"(?i)\s*inr\s+\-*[0-9\,]+$",
                    "suffix_balance_regex": r"^.\d{2}$"
                   },
                   {
                    "prefix_balance_regex": r"(?i)^[0-9\,]+$",
                    "suffix_balance_regex": r"^.\d{2}$"
                   },
                   {
                    "prefix_balance_regex": r"(?i)^[0-9\,]+\.$",
                    "suffix_balance_regex": r"^\d{2}$"
                   },
                  ]
    }
    
    regex_list_for_bank = regex_list.get(bank, None)
    if regex_list_for_bank is None:
        return df
    transactions_length = len(transactions)
    for i in range(transactions_length):
        for regex_item in regex_list_for_bank:
            prefix_match = re.match(regex_item['prefix_balance_regex'], transactions[i]['balance'])
            if prefix_match and i+1 < transactions_length:
                suffix_match = re.match(regex_item['suffix_balance_regex'], transactions[i+1]['balance'])
                if suffix_match:
                    transactions[i]['balance'] = transactions[i]['balance'] + transactions[i+1]['balance']
                    break
    
    df = pd.DataFrame(transactions)
    return df

def fix_indusind_date(df):
    """
        supports date of the fromat:
        11
        Apr
        2024

        and semi dates for last transactions
        11
        Apr

        and 
        11
    """

    df_columns = list(df.columns)
    df = df.apply(lambda row: check_format_row(row), axis=1)

    if 'date' not in df_columns:
        return df
    
    transactions = df.to_dict('records')
    t = len(transactions)
    
    prev_date = None

    for i in range(len(transactions) - 2):
        if transactions[i]['is_balance'] and transactions[i]['date'] !='':
            temp_date = transactions[i]['date'] + ' ' + transactions[i+1]['date'] + ' ' + transactions[i+2]['date']
            try:
                prev_date = datetime.strptime(temp_date, '%d %b %Y')
                transactions[i]['date'] = prev_date.strftime('%d %B %Y')
                transactions[i+1]['date'] = ''
                transactions[i+2]['date'] = ''
            except Exception as _:
                pass
    
    if transactions[t-2]['is_balance'] and transactions[t-2]['date'] != '' and transactions[t-1]['date'] != '' and prev_date:
        temp_date = transactions[t-2]['date'] + ' ' + transactions[t-1]['date']
        try:
            date = datetime.strptime(temp_date, '%d %b')
            temp_prev_date = datetime.strptime(prev_date.strftime('%b %d'), '%b %d')
            if date <= temp_prev_date:
                year_to_add =  str(prev_date.year)
            else:
                year_to_add =  str(prev_date.year - 1)
            temp_date = temp_date + ' ' + year_to_add
            prev_date = datetime.strptime(temp_date, '%d %b %Y')
            transactions[t-2]['date'] = prev_date.strftime('%d %B %Y')
            transactions[t-1]['date'] = ''
        except Exception as _:
            pass
    
    if transactions[t-1]['is_balance'] and transactions[t-1]['date'] != '' and len(transactions[t-1]['date']) ==2 and prev_date:
        temp_date = transactions[t-1]['date']
        try:
            temp_day = int(temp_date)
            if temp_day <= prev_date.day:
                temp_date = prev_date.replace(day=temp_day)
            else:
                temp_date = prev_date.replace(day=temp_day)
                temp_date = temp_date.replace(month=temp_date.month - 1)
            
            transactions[t-1]['date'] = temp_date.strftime('%d %B %Y')
        except Exception as _:
            pass


    df = pd.DataFrame(transactions)
    return df

def clean_id_amount_balance(row):
    """
        This function removes 'IDR' from currency values in case of indonesia
        eg: "IDR 325346263.00" to "325346263.00"
    """
    
    req_keys = ['balance', 'amount', 'debit', 'credit']
    
    for item in req_keys:
        if item in row.keys():
            row[item] = row[item].replace('IDR', '')

    return row

def convert_utc_time(df):
    transactions = df.to_dict('records')
    prev_year_detected, prev_month, prev_date = '','',''
    for transaction in transactions:
        date = transaction.get('date', None)
        if date is None:
            continue

        joined_date = date.replace(' ','')
        if len(joined_date)>=12:
            try:
                date_string = joined_date[3:12]
                datetime_obj = datetime.strptime(date_string,'%b%d%Y')
                date_string = datetime_obj.strftime('%Y-%m-%d')
                transaction['date'] = date_string

                year = datetime_obj.strftime('%Y')
                prev_month = datetime_obj.strftime('%m')
                prev_date = datetime_obj.strftime('%d')
                prev_year_detected = year
            except Exception:
                continue
        elif len(joined_date)>=8:
            try:
                date_string = joined_date[:8]
                date_obj = datetime.strptime(date_string,'%a%b%d')
                current_date = date_obj.strftime('%d')
                current_month = date_obj.strftime('%m')

                year_to_be_populated = prev_year_detected
                if prev_date == '31' and prev_month == '12' and current_month=='01' and current_date=='01':
                    integer_year = int(year_to_be_populated)
                    year_to_be_populated = f'{integer_year+1}'

                date_string+=year_to_be_populated
                datetime_obj = datetime.strptime(date_string,'%a%b%d%Y')
                date_string = datetime_obj.strftime('%Y-%m-%d')
                transaction['date'] = date_string

                prev_month = datetime_obj.strftime('%m')
                prev_date = datetime_obj.strftime('%d')
                year = datetime_obj.strftime('%Y')
                prev_year_detected = year
            except Exception:
                continue
    
    df = pd.DataFrame(transactions)
    return df

def fix_sahabat_sampoerna_amount_from_balance(row):
    row_keys = row.keys()
    if 'credit' not in row_keys or 'debit' not in row_keys or 'balance' not in row_keys:
        return row
    
    balance = row.get('balance')
    if balance and len(balance.strip().split()) == 2:
        amount_list =  balance.strip().split()
        row['balance'] = amount_list[0]
        row['credit'] = amount_list[1]
    
    return row

def solve_ubi_null_balance_transaction_case(txn_df: pd.DataFrame) -> pd.DataFrame:
    """
        handling ubi case :
             random               date                                                  random                                   transaction_note                                          Amount                              Balance
        10   217                 17/01/2024                                             A19634                                       NITYA SPORTS                                       8260.00 (Cr)                        1415513.87 (Cr)
        11   218                 17/01/2024                                             A22295            PRIYA TRADING COMPANY PROP KAPIL SHARMA                                      15576.00 (Cr)                                   (Cr)
        12   219                       null                                                                                                                                                     (Cr)                                   (Cr)
        13   220                       null                                                                                                                                                     (Cr)                                   (Cr)
        14   221                 15/07/2024                                            A674701                                    AGARWAL TRADING                                      17700.00 (Cr)                        2715295.50 (Cr)

        Adding balance in row 11 in accordance with date order.
        Currently only valid in case where date order is reverse.

        Refence statement id: '827ddc0b-83e6-4995-8ed7-d9413bd76850'
    """
    columns = list(txn_df.columns)
    dates_list = list(txn_df['date'])
    date_order = check_ubi_date_order(dates_list)
    
    if not date_order or (date_order and date_order != 'reverse'):
        return txn_df
    
    row_dicts = txn_df.to_dict('records')
    num_rows = len(row_dicts)

    for i in range(num_rows):

        if ('balance' in columns and 'amount' in columns) and \
            i > 0 and i < num_rows - 1 and row_dicts[i]['balance'].lower().strip() == '(cr)' and \
            isinstance(row_dicts[i+1]['date'], str) and row_dicts[i+1]['date'].lower().strip() == 'null' and \
            amount_to_float(row_dicts[i]['amount']) and amount_to_float(row_dicts[i-1]['amount']):
            
            amount = amount_to_float(row_dicts[i-1]['amount'])
            amount_sign = get_amount_sign(row_dicts[i-1]['amount'])
            amount = amount * amount_sign if amount and amount_sign else None
            
            balance = amount_to_float(row_dicts[i-1]['balance'])
            balance_sign = get_amount_sign(row_dicts[i-1]['balance'])
            balance = balance * balance_sign if balance and balance_sign else None

            if (amount and balance):

                sign = -1 if date_order == 'reverse' else 1
                row_dicts[i]['balance'] = str(round(balance + sign * amount, 2))

    return pd.DataFrame(row_dicts)


def check_ubi_date_order(dates_list: list) -> Union[str, None]:
    first_datetime = None
    last_datetime = None
    null_start_index = None
    null_end_index = None

    num_rows = len(dates_list)

    # Iterate through the list to find the first and last datetime object
    for i in range(num_rows):
        if isinstance(dates_list[i], datetime):
            if first_datetime is None:
                first_datetime = i  # Set the first datetime
            last_datetime = i
        elif isinstance(dates_list[i], str) and dates_list[i] == 'null':
            if null_start_index is None:
                null_start_index = i
            null_end_index = i
    
    if not (first_datetime and last_datetime and null_start_index and null_end_index):
        return None
    # TODO handle cases of multiple island scanerios
    date_list1 = dates_list[first_datetime:null_start_index]
    date_list2 = dates_list[null_end_index+1:last_datetime + 1]
    order1 = date_order(date_list1)
    order2 = date_order(date_list2)

    if order1 and order2:
        if order1 == order2:
            return order1
        else:
            return 'reverse'
    
    if order1:
        return order1
    
    if order2:
        return order2

    return None

def date_order(date_list: list) -> Union[str, None]:
    if not date_list:
        return None
    start_date = date_list[0]
    end_date = date_list[-1]
    
    if not isinstance(start_date, datetime) or not isinstance(end_date, datetime):
        return None
    
    if start_date > end_date:
        return 'reverse'
    elif start_date < end_date:
        return 'correct'
    
    return None


def sanitize_str_based_on_regex(original_str: str, regex_list: list) -> str:
    for regex_item in regex_list:
        regex_match = re.match(regex_item, original_str)
        if not regex_match:
            continue
        
        group_list = regex_match.groups()
        if len(group_list) != 1:
            continue
        
        return group_list[0]
    
    return original_str


def sinitize_trxn_columns_based_on_regex(transaction_row: dict, bank_name: str) -> dict:
    transaction_key_list = ["date", "amount", "date", "credit", "debit", "balance"]
    for trxn_key in transaction_key_list:
        if trxn_key not in transaction_row.keys():
            continue
        transaction_row[trxn_key] = sanitize_str_based_on_regex(transaction_row[trxn_key], REGEXES_TO_SANITIZE_TRXN_COLUMN[bank_name].get(trxn_key, []))

    return transaction_row
