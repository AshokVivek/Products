from datetime import datetime
from collections import defaultdict
import holidays, hashlib
from library.utils import get_bank_threshold_diff, single_transaction_hash, remove_special_chars, log_data
from library.validations import check_date_order
from library.utils import check_date, match_regex
from copy import deepcopy
from Levenshtein import distance
from library.helpers.constants import (
    DATE_DELTA_DAYS_THRESHOLD_FOR_DATE_CORRECTION,
    OPTIMIZATION_THRESHOLDS,
    MAXIMUM_UPI_DAILY_LIMIT,
    MAXIMUM_UPI_PER_TRANSACTION_LIMIT,
    MAXIMUM_IMPS_DAILY_LIMIT,
    MAXIMUM_IMPS_PER_TRANSACTION_LIMIT,
    IMPS_RECOGNISATION_REGEX_LIST,
    DEFAULT_BALANCE_FLOAT,
    DEFAULT_TIMESTAMP_UTC
)

from library.extract_txns_finvu_aa import get_transaction_channel_description_hash
import pandas as pd

def get_signed_amount(transaction_row, bank='', statement_attempt_type = None):
    '''
    Return a copied transaction row with amount as signed on basis of transaction type
    - original transaction_row is not modified, a copy of transaction_row is returned after required modification 
    '''
    transaction_row = transaction_row.copy()
    amount = transaction_row['amount']
    transaction_type = transaction_row['transaction_type']

    # Bypassing signed amount modification for statements of attempt_type = aa
    # if statement_attempt_type and statement_attempt_type == 'aa':
    #     if transaction_type == 'debit':
    #         transaction_row['amount'] = -1 * amount
    #     return transaction_row
    
    if transaction_type == 'credit':
        if amount < 0:
            amount = -1 * amount
    
    if transaction_type == 'debit':
        if amount > 0:
            amount = -1 * amount
    
    transaction_row['amount'] = amount
    return transaction_row


def check_transaction_tally_increasing_date(transaction_dict, bank, index=0):
    '''
    Detects any balance mismatch in the transaction_dict provided
    - transaction_dict is not modified
    - index default value is zero, index is used to if balance mismatch checks starting is in between txn's
    '''
    prev_balance = None

    THRESHOLD_DIFF = get_bank_threshold_diff(bank)

    if len(transaction_dict) > index:
        prev_balance = transaction_dict[index]['balance']

    if len(transaction_dict) > index + 1:
        for i in range(index + 1, len(transaction_dict)):
            
            if transaction_dict[i]['amount'] is None:
                return transaction_dict[i].get('hash', 'random_hash')
            
            new_balance_calculated = round(prev_balance + transaction_dict[i]['amount'],2)
            new_balance = transaction_dict[i]['balance']

            if abs(new_balance_calculated - new_balance) > THRESHOLD_DIFF:
                inconsistent_hash = transaction_dict[i].get('hash', 'random_hash')
                return inconsistent_hash
            
            prev_balance = new_balance
    return None


def get_correct_transaction_order(transaction_list):
    if len(transaction_list) > 0:

        first_transaction = transaction_list[0]

        # Crash fixes
        # Handled first_transaction - data type issue
        # checking first_transaction is type of dict/json
        # Sentry link - https://finbox.sentry.io/issues/3865435584/
        if type(first_transaction) == type({}) and isinstance(first_transaction['date'], datetime):
            first_date = first_transaction['date']
        elif type(first_transaction) == type({}) and isinstance(first_transaction['date'], str):
            first_date = datetime.strptime(
                first_transaction['date'], '%Y-%m-%d %H:%M:%S')
        else:
            first_date_str = '1970-01-01 00:00:00'
            first_date = datetime.strptime(first_date_str, '%Y-%m-%d %H:%M:%S')

        last_transaction = transaction_list[-1]

        if isinstance(last_transaction['date'], datetime):
            last_date = last_transaction['date']
        elif isinstance(last_transaction['date'], str):
            last_date = datetime.strptime(
                last_transaction['date'], '%Y-%m-%d %H:%M:%S')
        else:
            last_date_str = '1970-01-01 00:00:00'
            last_date = datetime.strptime(last_date_str, '%Y-%m-%d %H:%M:%S')

        if last_date < first_date:
            return transaction_list[::-1]
        else:
            return transaction_list
    else:
        return transaction_list

def get_inconsistent_hash(transaction_list, bank, statement_attempt_type):
    signed_txn_list = []

    for each_transaction_row in transaction_list:
        each_transaction_row = get_signed_amount(each_transaction_row,bank, statement_attempt_type)
        signed_txn_list.append(each_transaction_row)

    return check_transaction_tally_increasing_date(signed_txn_list, bank)

def check_jumbled_dates(transaction_list):
    if len(transaction_list) <= 2:
        return False
    increasing_order = decreasing_order = True
    for i in range(len(transaction_list) - 1):
        date =  datetime.strptime(transaction_list[i]["date"], '%Y-%m-%d %H:%M:%S') if isinstance(transaction_list[i]['date'], str) else transaction_list[i]['date']
        next_date = datetime.strptime(transaction_list[i + 1]["date"], '%Y-%m-%d %H:%M:%S') if isinstance(transaction_list[i + 1]['date'], str) else transaction_list[i + 1]['date']
        if date > next_date:
            increasing_order = False
        elif next_date > date:
            decreasing_order = False
        if not increasing_order and not decreasing_order:
            return True

    return False

def sort_transactions_based_on_month(transaction_list):
    are_dates_jumbled = check_jumbled_dates(transaction_list)
    if not are_dates_jumbled:
        return transaction_list
    months_based_transctions = {}
    sorted_transaction_list = []
    months_list=[]
    for i in range(len(transaction_list)):
        if isinstance(transaction_list[i]["date"], str):
            obj_date = datetime.strptime(transaction_list[i]["date"], '%Y-%m-%d %H:%M:%S')
        else:
            obj_date = transaction_list[i]["date"]
        month = obj_date.strftime("%b-%y")
        if month not in months_based_transctions.keys():
            months_list.append(month)
            months_based_transctions[month] = []
        months_based_transctions[month].append(transaction_list[i])
    sorted_months = sorted(months_list, key=months_sort)
    for month in sorted_months:
        months_based_transctions[month] = get_correct_transaction_order(months_based_transctions[month])
        sorted_transaction_list.extend(months_based_transctions[month])
    return sorted_transaction_list

def months_sort(month):
    return datetime.strptime(month, '%b-%y')

def transaction_balance_check(transaction_list, bank='', statement_attempt_type = None, update_month_order = False, ignore_repetitive_transactions=True):
    if update_month_order:
        transaction_list = sort_transactions_based_on_month(transaction_list)
    #NOTE: in case of reverse order increasing_date_transaction_list will be a copied version else original
    increasing_date_transaction_list = get_correct_transaction_order(transaction_list)

    if len(increasing_date_transaction_list) <= 0:
        return None

    final_transactions, date_amount_map, _ = get_deduped_transactions(increasing_date_transaction_list, ignore_repetitive_transactions)
    
    single_date = len(date_amount_map.keys()) == 1
    
    inconsistent_hash = get_inconsistent_hash(final_transactions, bank, statement_attempt_type)

    if single_date and inconsistent_hash:
        inconsistent_hash_2 = get_inconsistent_hash(final_transactions[::-1], bank, statement_attempt_type)
        inconsistent_hash = inconsistent_hash if inconsistent_hash_2 else inconsistent_hash_2
    
    return inconsistent_hash

def get_deduped_transactions(transactions_list, ignore_repetitive_transactions=True):
    
    date_amount_map = defaultdict(list)
    memory_list = []
    memory_set = set()
    to_discover = []
    discovered = []
    discovered_items = []
    refined_list = []
    hash_to_index_mapping = {}
    current_index = 0
    pages_removed = set()
    if ignore_repetitive_transactions:
        for items in transactions_list:
            
            date_amount_map[items['date']].append(items['balance'])
            
            single_hash = single_transaction_hash(items)
            
            if (single_hash not in memory_set) or (len(items['transaction_note'])<3):
                if discovered_items:
                    # print(f"some unadded discovered items, {discovered_items}")
                    refined_list += discovered_items
                memory_set.add(single_hash)
                memory_list.append(single_hash)
                refined_list.append(items)
                to_discover = []
                discovered = []
                discovered_items = []
                hash_to_index_mapping[single_hash] = current_index
            else:
                index = hash_to_index_mapping[single_hash]
                to_discover = to_discover or memory_list[index:]
                
                discovered.append(single_hash)
                discovered_items.append(items)

                if discovered == to_discover:
                    print(f"Ignored repetitive transactions --> {len(discovered_items)}")
                    for txn in discovered_items:
                        pages_removed.add(txn.get('page_number'))
                    to_discover = []
                    discovered = []
                    discovered_items = []

                same_els = all(x in to_discover for x in discovered) and len(discovered)>0
                if (not same_els) or (len(discovered) == len(to_discover)):
                    memory_list += discovered
                    for hash in discovered:
                        memory_set.add(hash)
                    refined_list += discovered_items
                    to_discover = []
                    discovered = []
                    discovered_items = []

            current_index+=1
    else:
        for items in transactions_list:
            date_amount_map[items['date']].append(items['balance'])
        # Don't need to ignore repitive transactions for those banks who are covered in remove_duplicate_transactions()
        refined_list = transactions_list
    
    final_transactions = refined_list

    return final_transactions, date_amount_map, pages_removed

def optimise_transaction_type(transactions_dict, bank='', statement_attempt_type=None, update_month_order=False, send_optimized=False, update_flag=False):
    """
        This function takes transactions as an input and tries to fix inconsistency by changing transaction type.
        If it is able to fix inconsistency by changing transaction type then, it return the transactions with changed transaction type,
        otherwise it returns the original transactions.
    """
    if len(transactions_dict) < 2:
        return transactions_dict, set(), 0, None
    
    is_fraud = transaction_balance_check(transactions_dict, bank, statement_attempt_type, update_month_order)
    
    if not is_fraud:
        return transactions_dict, set(), 0, None
    original_txns = deepcopy(transactions_dict)

    order = check_date_order([transactions_dict])
    
    if order == 'reverse':
        transactions_dict = transactions_dict[::-1]
    
    transactions_dict, num_type_changed, pages_updated, num_optimizations, inconsistent_hashes = change_transaction_type(transactions_dict, bank, update_flag)

    if num_type_changed == len(transactions_dict) - 1:
        transactions_dict[0]['transaction_type'] = 'debit' if transactions_dict[0]['transaction_type'] == 'credit' else 'credit'
        transactions_dict[0]['optimizations'].append('TRANSACTION_TYPE_CHANGED')

    is_fraud = transaction_balance_check(transactions_dict, bank, statement_attempt_type, update_month_order)
    
    if order == 'reverse':
        transactions_dict = transactions_dict[::-1]
    
    if not is_fraud or send_optimized:
        return transactions_dict, pages_updated, num_optimizations, is_fraud
    
    return original_txns, set(), 0, is_fraud

def remove_duplicate_transactions(transactions_list, bank=''):
    ignore_repetitive_transactions = False
    inconsistent_hash = transaction_balance_check(transactions_list, bank, ignore_repetitive_transactions=ignore_repetitive_transactions)
    if not inconsistent_hash:
        transactions_list, _, pages_updated = get_deduped_transactions(transactions_list, ignore_repetitive_transactions=False)
        return transactions_list, pages_updated
    
    temp_transactions = deepcopy(transactions_list)
    pages_updated = set()
    discovered = []

    order = check_date_order([temp_transactions], bank)
    if order == 'reverse':
        temp_transactions = temp_transactions[::-1]

    for i in range(len(temp_transactions)):
        temp_transactions[i] = get_signed_amount(temp_transactions[i], bank)
    
    index = 0    
    while index<len(temp_transactions):
        amount = temp_transactions[index].get('amount')
        date = temp_transactions[index].get('date')
        balance = temp_transactions[index].get('balance')
        transaction_type = temp_transactions[index].get('transaction_type')
        transaction_hash = temp_transactions[index].get('hash')
        
        to_be_hashed_list = [str(amount), str(date), str(balance), str(transaction_type)]
        if bank in ['sbi']:
            to_be_hashed_list.append(str(temp_transactions[index].get('transaction_note')))
        to_be_hashed = remove_special_chars("".join(to_be_hashed_list)).encode("utf-8")
        hash_to_check = hashlib.md5(to_be_hashed).hexdigest()

        if hash_to_check not in discovered:
            discovered.append(hash_to_check)
        elif transaction_hash==inconsistent_hash and hash_to_check in discovered:
            pages_updated.add(temp_transactions[index].get('page_number', None))
            del temp_transactions[index]
            index = index - 1
            inconsistent_hash = check_transaction_tally_increasing_date(temp_transactions, bank, index)
            if not inconsistent_hash:
                if order=='reverse':
                    return temp_transactions[::-1], pages_updated
                return temp_transactions, pages_updated
        index = index + 1
    
    return transactions_list, set()

def change_transaction_type(transactions_dict, bank='', update_flag=False):
    
    if len(transactions_dict) == 0:
        return transactions_dict, 0, set(), 0, [] 
    
    original_txn_dict = deepcopy(transactions_dict)
    pages_updated = set()
    prev_balance = transactions_dict[0]['balance']
    num_type_changed = 0
    num_balance_sign_changed = 0
    num_optimizations = 0
    inconsistent_hashes = []

    THRESHOLD_DIFF = get_bank_threshold_diff(bank)

    if transactions_dict[0]['amount'] < 0:
        transactions_dict[0]['amount'] = abs(transactions_dict[0]['amount'])
        pages_updated.add(transactions_dict[0].get('page_number', None))
        transactions_dict[0]['optimizations'].append('AMOUNT_MADE_POSITIVE')
        if update_flag:
            transactions_dict[0]['to_update'] = True
        num_optimizations += 1

    for i in range(1, len(transactions_dict)):
        txn = transactions_dict[i]
        amount = abs(txn['amount'])
        if txn['amount'] < 0:
            pages_updated.add(txn.get('page_number', None))
            transactions_dict[i]['amount'] = amount
            transactions_dict[i]['optimizations'].append('AMOUNT_MADE_POSITIVE')
            if update_flag:
                transactions_dict[i]['to_update'] = True
            num_optimizations += 1
        balance = txn['balance']
        if abs(round(prev_balance + amount,2) - round(balance,2)) <= THRESHOLD_DIFF:
            if transactions_dict[i]['transaction_type'] == 'debit':
                pages_updated.add(txn.get('page_number', None))
                num_type_changed +=1
                transactions_dict[i]['optimizations'].append('TRANSACTION_TYPE_CHANGED')
                if update_flag:
                    transactions_dict[i]['to_update'] = True
            transactions_dict[i]['transaction_type'] = 'credit'
        elif abs(round(prev_balance - amount,2) - round(balance,2)) <= THRESHOLD_DIFF:
            if transactions_dict[i]['transaction_type'] == 'credit':
                pages_updated.add(txn.get('page_number', None))
                num_type_changed +=1
                transactions_dict[i]['optimizations'].append('TRANSACTION_TYPE_CHANGED')
                if update_flag:
                    transactions_dict[i]['to_update'] = True
            transactions_dict[i]['transaction_type'] = 'debit'
        elif abs(round(prev_balance - amount,2) - -1*round(balance,2)) <= THRESHOLD_DIFF:
            if transactions_dict[i]['transaction_type'] == 'credit':
                pages_updated.add(txn.get('page_number', None))
                num_type_changed +=1
                transactions_dict[i]['optimizations'].append('TRANSACTION_TYPE_CHANGED')
                if update_flag:
                    transactions_dict[i]['to_update'] = True
            balance = -1* balance
            transactions_dict[i]['transaction_type'] = 'debit'
            transactions_dict[i]['balance'] = balance
            transactions_dict[i]['optimizations'].append('BALANCE_SIGN_CHANGED')
            if update_flag:
                transactions_dict[i]['to_update'] = True
            num_balance_sign_changed += 1
        elif abs(round(prev_balance + amount,2) - -1*round(balance,2)) <= THRESHOLD_DIFF:
            if transactions_dict[i]['transaction_type'] == 'debit':
                pages_updated.add(txn.get('page_number', None))
                num_type_changed +=1
                transactions_dict[i]['optimizations'].append('TRANSACTION_TYPE_CHANGED')
            balance = -1* balance
            transactions_dict[i]['transaction_type'] = 'credit'
            transactions_dict[i]['balance'] = balance
            transactions_dict[i]['optimizations'].append('BALANCE_SIGN_CHANGED')
            if update_flag:
                transactions_dict[i]['to_update'] = True
            num_balance_sign_changed += 1
        elif transactions_dict[i].get('hash'):
            inconsistent_hashes.append((transactions_dict[i]['hash'], i))
        prev_balance = balance
    
    num_optimizations = num_optimizations + num_type_changed + num_balance_sign_changed

    # returned original transactions when we are changing balane sign in many transactions
    # setting a limit to number of changes optimization algorithms can make

    optimization_ratio = 0.2
    optimization_count = 2
    
    if bank in OPTIMIZATION_THRESHOLDS.keys():
        optimization_ratio = OPTIMIZATION_THRESHOLDS[bank].get("ratio", 0.2)
        optimization_count = OPTIMIZATION_THRESHOLDS[bank].get("count", 2)

    if (
        (
            (num_balance_sign_changed/len(transactions_dict) > optimization_ratio and len(transactions_dict) > 10) or 
            (num_balance_sign_changed > optimization_count and len(transactions_dict) <= 10)
        ) and 
        bank not in ['abhyudaya', 'adarsh_co_bnk', 'tjsb_sahakari']
    ):
        return original_txn_dict, 0, set(), 0, inconsistent_hashes 
    
    return transactions_dict, num_type_changed, pages_updated, num_optimizations, inconsistent_hashes

def optimise_refund_transactions(transactions_dict, bank_name=''):
    """
        This function changes the transaction type from debit to credit in case of transaction where transaction_channel is refund.
        Also makes amount positive if required.
    """
    pages_updated = set()

    for i in range(1, len(transactions_dict)): 
        txn_type = transactions_dict[i].get('transaction_type')
        amount = transactions_dict[i].get('amount')
        txn_channel = transactions_dict[i].get('transaction_channel')
        if txn_channel == 'refund' and txn_type == 'debit' and i == 0:
            transactions_dict[i]['transaction_type'] = 'credit'
            transactions_dict[i]['amount'] = abs(amount)
            transactions_dict[i]['optimizations'].append('TRANSACTION_TYPE_CHANGED')
            pages_updated.add(transactions_dict[i].get('page_number', None))
        elif txn_channel == 'refund' and txn_type == 'debit':
            prev_balance = transactions_dict[i - 1]['balance']
            if abs(amount) + prev_balance == transactions_dict[i]['balance']:
                transactions_dict[i]['transaction_type'] = 'credit'
                transactions_dict[i]['amount'] = abs(amount)
                transactions_dict[i]['optimizations'].append('TRANSACTION_TYPE_CHANGED')
                pages_updated.add(transactions_dict[i].get('page_number', None))
    
    return transactions_dict, pages_updated

def solve_merged_or_jumbled_transactions(transactions_list: list, Logger=None, local_logging_context=None) -> dict:
    """
    Process and clean a potentially jumbled or merged list of transactions.

    Args:
        transactions_list (list): List of transaction dictionaries to process.

    Returns:
        dict: Processed transaction data with key information about consistency and date ranges.
    """
    pages_updated = set()
    if len(transactions_list) <= 2:
        log_data("Statement has less than 3 transactions", Logger, local_logging_context, "info")
        return {
            "transactions": transactions_list,
            "inconsistent_hashes": [],
            "clean_date_chunks": [],
            "is_missing_data": False,
            "pages_updated": pages_updated
        }
    
    # Initial type conversion and hash consistency check
    transactions_list, _, page_updated, _, inconsistent_hashes = change_transaction_type(transactions_list)
    pages_updated.update(page_updated)

    # Check if transactions are excessively inconsistent
    if len(inconsistent_hashes) > 0.15 * len(transactions_list) and len(transactions_list) > 20:
        log_data("Very high inconsistency in statement", Logger, local_logging_context, "info")
        return {
            "transactions": transactions_list,
            "inconsistent_hashes": inconsistent_hashes,
            "clean_date_chunks": [],
            "is_missing_data": False
        }

    # Generate chunks and date ranges
    all_chunks_list, date_range_list = get_clear_chunks_list_and_date_range_list(inconsistent_hashes, transactions_list)

    # Deduplicate and sort transactions
    sorted_transactions_list, page_updated = dedup_and_get_sorted_transactions_list(date_range_list, all_chunks_list, Logger, local_logging_context)
    pages_updated.update(page_updated)

    # Reapply type conversion
    sorted_transactions_list, _, page_updated, _, inconsistent_hashes = change_transaction_type(sorted_transactions_list)
    pages_updated.update(page_updated)

    if len(inconsistent_hashes) > 0:
        # Regenerate chunks and date ranges
        all_chunks_list, date_range_list = get_clear_chunks_list_and_date_range_list(inconsistent_hashes, sorted_transactions_list)

        sorted_transactions_list, page_updated = dedup_and_get_sorted_transactions_list(date_range_list, all_chunks_list)
        pages_updated.update(page_updated)

        # Reapply type conversion
        sorted_transactions_list, _, page_updated, _, inconsistent_hashes = change_transaction_type(sorted_transactions_list)
        pages_updated.update(page_updated)

    # Regenerate chunks and date ranges
    all_chunks_list, date_range_list = get_clear_chunks_list_and_date_range_list(inconsistent_hashes, sorted_transactions_list)


    # Check for missing data
    is_missing_data = False
    if len(inconsistent_hashes) > 0:
        is_missing_data = get_missing_data(date_range_list)
    else:
        deduped_transactions_list, _, page_updated = get_deduped_transactions(sorted_transactions_list)
        is_inconsistent = transaction_balance_check(deduped_transactions_list)
        if not is_inconsistent:
            sorted_transactions_list = deduped_transactions_list
            pages_updated.update(page_updated)
    
    # Format date ranges for output
    for date_range in date_range_list:
        date_range[0] = date_range[0].strftime("%d-%m-%Y")
        date_range[1] = date_range[1].strftime("%d-%m-%Y")
        date_range.pop(-1)

    # Construct and return output response
    return {
        "transactions": sorted_transactions_list,
        "inconsistent_hashes": inconsistent_hashes,
        "clean_date_chunks": date_range_list,
        "is_missing_data": is_missing_data,
        "pages_updated": pages_updated
    }

def get_datetime(date_str: str, formats: list = []) -> datetime:
    """
    Convert a date string to a datetime object using multiple possible formats.

    Args:
        date_str (str): Input date string to parse.
        formats (list, optional): Custom date formats to try. Defaults to standard formats.

    Returns:
        datetime: Parsed datetime object or original input if not a string.
    """
    # If input is not a string, return as-is
    if not isinstance(date_str, str):
        return date_str
    
    # Use provided formats or default to standard formats
    known_formats = formats or ["%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S"]
    
    # Attempt parsing with each format
    for known_format in known_formats:
        try:
            return datetime.strptime(date_str, known_format)
        except ValueError:
            continue
    
    # Return None if no format matches
    return None

def get_clear_chunks_list_and_date_range_list(inconsistent_hashes: list, transactions_list: list) -> tuple[list, list]:
    """
    Divide transactions into chunks based on inconsistent hash indices.

    Args:
        inconsistent_hashes (list): List of tuples containing (hash, index).
        transactions_list (list): Complete list of transactions.

    Returns:
        tuple: 
            - List of transaction chunks
            - List of date ranges for each chunk
    """
    # Count number of inconsistencies
    count_inconsistencies = len(inconsistent_hashes)
    all_chunks_list = []
    date_range_list = []
    prev_index = 0
    
    # Process chunks up to the last inconsistent hash
    for index in range(count_inconsistencies):
        _hash, curr_index = inconsistent_hashes[index]
        
        # Extract chunk between previous and current index
        clear_chunk = transactions_list[prev_index:curr_index]
        
        # Add date range for current chunk
        date_range_list.append([
            get_datetime(clear_chunk[0]['date']), 
            get_datetime(clear_chunk[-1]['date']), 
            index
        ])
        
        all_chunks_list.append(clear_chunk)
        prev_index = curr_index
    
    # Process final chunk after last inconsistent hash
    clear_chunk = transactions_list[prev_index:]
    if clear_chunk:
        date_range_list.append([
            get_datetime(clear_chunk[0]['date']), 
            get_datetime(clear_chunk[-1]['date']), 
            count_inconsistencies
        ])
        all_chunks_list.append(clear_chunk)
    
    return all_chunks_list, date_range_list


def dedup_and_get_sorted_transactions_list(date_range_list: list, all_chunks_list: list, Logger=None, local_logging_context=None):
    """
    Deduplicate and sort transactions by removing overlapping or duplicate date ranges.

    Args:
        date_range_list (list): List of tuples containing (start_date, end_date, chunk_index).
        all_chunks_list (list): List of transaction chunks corresponding to date ranges.

    Returns:
        list: Sorted and deduplicated list of transactions.
    """
    pages_updated = set()
    
    # Sort date ranges to ensure proper processing
    date_range_list.sort()
    sorted_transactions_list = []
    if not date_range_list:
        return sorted_transactions_list, pages_updated

    # Initialize with the first date range
    final_date_range_list = [date_range_list[0]]
    prev_start, prev_end, prev_index = date_range_list[0]

    # Process subsequent date ranges
    for i in range(1, len(date_range_list)):
        curr_start, curr_end, curr_index = date_range_list[i]

        # Skip completely contained chunks with smaller or equal length
        if curr_start > prev_start and curr_end < prev_end and len(all_chunks_list[curr_index]) < len(all_chunks_list[prev_index]):
            pages_updated.update(get_pages_for_updated_transactions(all_chunks_list[curr_index]))
            log_data(f"{len(all_chunks_list[curr_index])} duplicate chunk found inside bigger chunk, deleting it, {curr_index}", Logger, local_logging_context, "info")
            continue
        if curr_start == prev_start and curr_end == prev_end and len(all_chunks_list[curr_index]) == len(all_chunks_list[prev_index]) and compare_transactions(all_chunks_list[prev_index], all_chunks_list[curr_index]):
            pages_updated.update(get_pages_for_updated_transactions(all_chunks_list[curr_index]))
            log_data(f"{len(all_chunks_list[curr_index])} duplicate chunk found, deleting it, {curr_index}", Logger, local_logging_context, "info")
            continue
        # Handle overlapping date ranges
        if prev_end >= curr_start:
            all_chunks_list[prev_index], all_chunks_list[curr_index], page_updated = fix_overlap_transactions(all_chunks_list[prev_index], all_chunks_list[curr_index], Logger, local_logging_context)
            pages_updated.update(page_updated)

        # Skip empty chunks
        if len(all_chunks_list[curr_index]) == 0:
            continue

        # Update previous range and add to final list
        prev_start, prev_end, prev_index = curr_start, curr_end, curr_index
        final_date_range_list.append(date_range_list[i])
    
    # Construct final sorted transactions list
    for _, _, index in final_date_range_list:
        sorted_transactions_list.extend(all_chunks_list[index])
    
    return sorted_transactions_list, pages_updated

def get_missing_data(date_range_list: list) -> bool:
    """
    Check if there are gaps between consecutive date ranges.

    Args:
        date_range_list (list): A list of tuples containing (start_date, end_date, additional_data).
                                Assumes the list is sorted by start date.

    Returns:
        bool: True if there are missing dates between ranges, False otherwise.
    """
    # Initialize flag and previous end date
    is_missing_dates = False
    prev_end = None

    # Iterate through the date ranges
    for curr_start, curr_end, _ in date_range_list:
        # Set initial previous end date for the first iteration
        if prev_end is None:
            prev_end = curr_start
        
        # Calculate days between previous range's end and current range's start
        diff = (curr_start - prev_end).days
        
        # Update previous end date for next iteration
        prev_end = curr_end
        
        # Check if there's a gap larger than one day
        if diff >= 1:
            is_missing_dates = True
            break
    
    return is_missing_dates

def fix_overlap_transactions(transactions1: list, transactions2: list, Logger=None, local_logging_context=None) -> list:
    
    pages_updated = set()
    last_transaction = transactions1[-1]
    found_index = -1
    
    for index, transaction in enumerate(transactions2):
        if last_transaction['date'] == transaction['date'] and \
            last_transaction['amount'] == transaction['amount'] and \
            last_transaction['balance'] == transaction['balance'] and \
            are_strings_similar(last_transaction['transaction_note'], transaction['transaction_note']) and \
            last_transaction['chq_num'] == transaction['chq_num']:
            found_index = index
            break
    
    if found_index != -1:
        n = len(transactions1)
        chunk1 = transactions1[n - found_index - 1:]
        chunk2 = transactions2[:found_index + 1]
        if compare_transactions(chunk1, chunk2, Logger, local_logging_context):
            log_data(f"{found_index + 1} duplicate transactions found, removing them", Logger, local_logging_context, "info")
            pages_updated.update(get_pages_for_updated_transactions(chunk2))
            transactions2 = transactions2[found_index + 1:]
    
    return transactions1, transactions2, pages_updated

def compare_transactions(list1: list, list2: list, Logger=None, local_logging_context=None) -> bool:
    if len(list1) != len(list2):
        log_data("both chunks are not of equal length", Logger, local_logging_context, "info")
        return False
    
    num_transactions = len(list1)
    
    for index in range(num_transactions):
        transaction1 = list1[index]
        transaction2 = list2[index]
        if transaction1['date'] != transaction2['date'] or \
            transaction1['amount'] != transaction2['amount'] or \
            transaction1['balance'] != transaction2['balance'] or \
            not are_strings_similar(transaction1['transaction_note'], transaction2['transaction_note']) or \
            transaction1['chq_num'] != transaction2['chq_num']:
            log_data(f"Transactions did not match, hence not deduplocating at index {index}", Logger, local_logging_context, "info")
            return False
    
    return True

def are_strings_similar(str1, str2):
    """
    Calculate the percentage similarity between two strings based on Levenshtein distance.

    Args:
        str1 (str): The first string.
        str2 (str): The second string.

    Returns:
        float: Similarity percentage (0 to 100).
    """
    if not str1 and not str2:
        return 100.0  # Both strings are empty, 100% match.
    
    # Calculate the Levenshtein distance
    edit_distance = distance(str1, str2)
    
    # Use the longer string length to calculate the percentage
    max_length = max(len(str1), len(str2))
    
    # Calculate similarity percentage
    similarity = ((max_length - edit_distance) / max_length) * 100
    return round(similarity, 2) > 75

def get_pages_for_updated_transactions(transactions):
    pages_updated = set()
    for txn in transactions:
        pages_updated.add(txn.get('page_number'))
    return pages_updated


def process_merged_pdf_transactions(transactions_list: list, Logger=None, local_logging_context=None) -> list:
    
    original_transactions = deepcopy(transactions_list)
    inconsistent_hashes = []
    inconsistent_data = {}
    pages_updated = set()
    try:
        response = solve_merged_or_jumbled_transactions(deepcopy(transactions_list), Logger, local_logging_context)
        transactions = response['transactions']
        inconsistent_hashes = response['inconsistent_hashes']
        
        if len(inconsistent_hashes) > 0:
            log_data("Trying to solve by reversing transactions list", Logger, local_logging_context, "info")
            reverse_response = solve_merged_or_jumbled_transactions(deepcopy(transactions_list[::-1]), Logger, local_logging_context)
            reverse_transactions = reverse_response['transactions']
            reverse_inconsistent_hashes = reverse_response['inconsistent_hashes']
        
            if len(reverse_inconsistent_hashes) < len(inconsistent_hashes):
                inconsistent_hashes = reverse_inconsistent_hashes
                transactions = reverse_transactions[::-1]
                response = reverse_response
        
        inconsistent_data['inconsistent_hashes'] = response.get('inconsistent_hashes')
        inconsistent_data['clean_date_chunks'] = response.get('clean_date_chunks')
        inconsistent_data['is_missing_data'] = response.get('is_missing_data')
        pages_updated = response.get('pages_updated')
        if len(inconsistent_hashes) > 0.15 * len(transactions) and len(transactions) > 20:
            log_data("probably faulty extraction due to very high inconsistencies", Logger, local_logging_context, "info")
    
    except Exception as e:
        log_data(e, Logger, local_logging_context, "info")
    
    is_fraud = len(inconsistent_hashes) > 0

    if not is_fraud:
        return transactions, is_fraud, inconsistent_data, pages_updated
    
    return original_transactions, is_fraud, inconsistent_data, set()

def correct_transactions_date(transactions, bank):
    """
        Takes transactions list as an input and returns updated transactions list
    """
    pages_updated = set()
    
    if len(transactions) < 1:
        return transactions, pages_updated
    
    order = check_date_order([transactions])
    
    if order == 'reverse':
        transactions = transactions[::-1]
    
    prev_date, _= convert_str_date_to_datetime(transactions[0]['date'])
    
    for i in range(len(transactions)):
        curr_date, is_current_date_converted = convert_str_date_to_datetime(transactions[i]['date'])
        if isinstance(curr_date, datetime) and isinstance(prev_date, datetime) \
            and curr_date < prev_date and (prev_date - curr_date).days < DATE_DELTA_DAYS_THRESHOLD_FOR_DATE_CORRECTION:
            if is_current_date_converted:
                transactions[i]['date'] = prev_date.strftime('%Y-%m-%d %H:%M:%S')
            else:
                transactions[i]['date'] = prev_date
            pages_updated.add(transactions[i].get('page_number'))
        
        prev_date, _ = convert_str_date_to_datetime(transactions[i]['date'])
    
    if order == 'reverse':
        transactions = transactions[::-1]
    
    return transactions, pages_updated

def convert_str_date_to_datetime(date_str):
    """
        Returns a datetime object if the input string if of format: %Y-%m-%d %H:%M:%S
    """

    if isinstance(date_str, datetime):
        return date_str, False

    date = None
    is_converted = False
    
    try:
        date = datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S")
        is_converted = True
    except Exception as _:
        pass
    
    return date, is_converted

def get_inconsistency_date_range(transactions, hash) -> tuple[datetime, datetime, list]:
    prev_date = None
    curr_date = None
    inconsistent_transactions = []
    if len(transactions) < 2:
        return prev_date, curr_date, inconsistent_transactions

    order = check_date_order([transactions])

    if order == 'reverse':
        transactions = transactions[::-1]

    for i in range(len(transactions)):
        if i != 0 and hash == transactions[i]['hash']:
            curr_date = transactions[i]['date']
            prev_date = transactions[i - 1]['date']
            inconsistent_transactions = [transactions[i], transactions[i - 1]]

    return prev_date, curr_date, inconsistent_transactions

"""
Sample Transaction object for functions below:
{
   merchant : ""
   transaction_note : "BY TRANSFER- UPI/CR/XXXXXXX/XXXXXX 91/UTIB/someuser/NA-",
   hash : "d5b9a7bb05aee74e94642a633992853b",
   description : "",
   unclean_merchant : "xxxx91",
   is_lender : False,
   transaction_type : "credit",
   amount : 900,
   date : "2019-02-01 00:00:00",
   balance : 1053.01,
   transaction_channel : "upi"
}
"""


def rtgs_min_amount_check(transaction):
    """
    Takes a single transaction object and returns True if RTGS transfer with less than 2 lakhs amount, otherwise False
    :param: transaction: transaction object
    :return: bool
    """
    if transaction and transaction['transaction_channel'] == "net_banking_transfer" and ("RTGS" in transaction['transaction_note'] and 'NEFT' not in transaction['transaction_note']  and transaction["amount"] < 200000):
        return True
    return False


def bank_holiday_check(transaction, country='IN'):
    """
    Takes a single transaction object and returns True if RTGS/Cheque happens on bank holiday, otherwise False
    :param: transaction: transaction object
    :return: bool
    """
    # TODO: cover other bank holidays than Sunday
    if transaction:
        channels_to_check = {"chq", "outward_cheque_bounce", "inward_cheque_bounce"}
        transaction_channel = transaction['transaction_channel']
        to_check = False

        if transaction_channel in channels_to_check:
            to_check = True
        # elif transaction_channel == "net_banking_transfer" and "RTGS" in transaction['transaction_note']:
        #     to_check = True
        elif transaction_channel == "cash_deposit" and transaction.get('description') != 'deposit_by_machine':
            to_check = True

        if to_check:
            date_obj = datetime.strptime(
                transaction['date'], '%Y-%m-%d %H:%M:%S'
            )
            if date_obj.weekday() == 6:  # Sunday 
                return True

            if country == 'UAE': country = 'AE'
            if country in holidays.list_supported_countries():
                country_holidays = sorted(holidays.country_holidays(country=country, years=datetime.now().year))
                if date_obj.date() in country_holidays: # Country Holidays
                    return True

    return False


def tax_fraud_check(transaction):
    """
    Takes a single transaction object and returns True if Bank Charge (Tax) is a multiple of 100, otherwise False
    :param: transaction: transaction object
    :return: bool
    """
    if transaction and transaction['transaction_channel'] == "bank_charge" and transaction['description'] not in ["ach_bounce_charge", "chq_bounce_charge", "card_issue_charge", "service_charge"] and transaction['amount'] % 100 == 0:
        return True
    return False


# def check_emi_fraud(transaction, loan_debit_transactions, fraud_list):
#     if transaction['merchant'] not in loan_debit_transactions:
#         loan_debit_transactions[transaction['merchant']] = []
#     else:
#         last_date = loan_debit_transactions[transaction['merchant']][-1]['date']
#         current_date = transaction['date']
#         last_date = datetime.strptime(last_date, '%Y-%m-%d %H:%M:%S')
#         current_date = datetime.strptime(current_date, '%Y-%m-%d %H:%M:%S')
#         days_in_month = calendar.monthrange(last_date.year, last_date.month)[1]

#         if last_date + timedelta(days=days_in_month-3) <= current_date <= last_date + timedelta(days=days_in_month+3):
#             fraud_list.append({"fraud_type": "irregular_emi_transaction", "transaction_hash": transaction['hash']})
#     loan_debit_transactions[transaction['merchant']].append(transaction)
#     return loan_debit_transactions, fraud_list



def month_from_date(transaction):
    try:
        return datetime.strftime(datetime.strptime(transaction['date'], '%Y-%m-%d %H:%M:%S'), '%Y-%m')
    except TypeError:
        return datetime.strftime(transaction['date'], '%Y-%m')


def account_level_frauds(transaction_list, account_category, salary_transactions, country='IN'):
    """
    Takes list of transactions, account_category
    and list of salary transactions, check for frauds and then
    returns a list of frauds found
    """
    month_dict = defaultdict(float)
    fraud_list = []
    cash_deposits = 0.0
    cnt_cash_transactions = 0
    total_credit = 0.0
    total_debit = 0.0
    last_month = None
    # if salary_transactions:
        # last_month = month_from_date(salary_transactions[0])
        # for index, salary_transact in enumerate(salary_transactions):
            # if salary_transact['amount'] % 1000 == 0 and country not in ['ID']: # Indonesian currency is higly inflated, therefore everything is in multiple of 1000 
                # fraud_list.append({"fraud_type": "salary_1000_multiple", "transaction_hash": salary_transact['hash']})
            # weekday = salary_transact['date'].weekday()
            # if weekday == 6:  # Sunday
            #     fraud_list.append({"fraud_type": "salary_credited_bank_holiday", "transaction_hash": salary_transact['hash']})
            # month = month_from_date(salary_transact)
            # month_dict[month] += salary_transact['amount']
            # mon = int(month[month.index("-")+1:])
            # if (mon == 2 or mon == 3) and (index < len(salary_transactions) - 1) and (salary_transact['amount'] == salary_transactions[index+1]['amount']) and (salary_transact['amount'] > 45000) and country not in ['ID']:
                # fraud_list.append({"fraud_type": "salary_remains_unchanged", "transaction_hash": salary_transact['hash']})
            # if index > 2 and salary_transactions[index-3]['amount'] == salary_transactions[index-2]['amount'] == salary_transactions[index-1]['amount'] == salary_transact['amount']:
            #     fraud_list.append({"fraud_type": "salary_remains_unchanged_4_months", "transaction_hash": salary_transact['hash']})

    if transaction_list:
        last_credit_trx = None
        upi_trxn_amount_per_day = dict()
        imps_trxn_amount_per_day = dict()
        max_upi_limit_per_day_fraud_exists = False
        max_imps_limit_per_day_fraud_exists = False
        for index, transaction in enumerate(transaction_list):

            #### Logic moved from Statement Level Frauds Check ####
            if rtgs_min_amount_check(transaction) and country not in ['ID']:
                fraud_list.append({"fraud_type": "min_rtgs_amount", "transaction_hash": transaction['hash']})
            # elif bank_holiday_check(transaction, country):
            #     # if transaction['transaction_channel'] == "net_banking_transfer":
            #     #     fraud_list.append({"fraud_type": "rtgs_bank_holiday_transaction", "transaction_hash": transaction['hash']})
            #     fraud_list.append({"fraud_type": "{}_bank_holiday_transaction".format(transaction['transaction_channel']), "transaction_hash": transaction['hash']})
            # elif tax_fraud_check(transaction) and country not in ['ID']: # Indonesian currency is higly inflated, therefore everything is in multiple of 100
                # fraud_list.append({"fraud_type": "tax_100_multiple", "transaction_hash": transaction['hash']})
            elif transaction["balance"] < 0 and account_category not in ["CURRENT", "corporate", "overdraft"] and country not in ['ID']:
                if index < len(transaction_list)-1 and transaction["date"] != transaction_list[index+1]["date"]:
                    fraud_list.append({"fraud_type": "negative_balance", "transaction_hash": transaction['hash']})
            #### --------------------------------------------- ####

            if transaction['transaction_channel'] == "cash_withdrawl" or transaction['transaction_channel'] == "cash_deposit":
                cnt_cash_transactions += 1
            if transaction['transaction_note'] == "credit":
                total_credit += transaction['amount']
                if last_credit_trx is not None and account_category in ['CURRENT', 'corporate']:
                    last_date = last_credit_trx['date']
                    current_date = transaction['date']
                    last_date = datetime.strptime(last_date, '%Y-%m-%d %H:%M:%S')
                    current_date = datetime.strptime(current_date, '%Y-%m-%d %H:%M:%S')
                    if (current_date-last_date).days > 15:
                        fraud_list.append({"fraud_type": "more_than_15_days_credit", "transaction_hash": transaction['hash']})
                else:
                    last_credit_trx = transaction
            else:
                total_debit += transaction['amount']

            if last_month == month_from_date(transaction):
                if transaction['transaction_channel'] == 'cash_deposit':
                    cash_deposits += transaction['amount']
            else:
                if last_month in month_dict and month_dict[last_month] <= cash_deposits and country not in ['ID']:
                    fraud_list.append({"fraud_type": "more_cash_deposits_than_salary"})
                cash_deposits = 0.0
                last_month = month_from_date(transaction)
            
            ### Max UPI limit fraud logic
            if transaction["transaction_channel"] == "upi" and transaction["transaction_type"] == "debit":
                if transaction["amount"] > MAXIMUM_UPI_PER_TRANSACTION_LIMIT:
                    fraud_list.append({"fraud_type": "max_upi_limit_per_transaction", "transaction_hash": transaction["hash"]})
                if not max_upi_limit_per_day_fraud_exists:
                    trxn_datetime, _ = check_date(transaction["date"])
                    if trxn_datetime:
                        trxn_date = trxn_datetime.strftime("%Y-%m-%d")
                        if trxn_date not in upi_trxn_amount_per_day:
                            upi_trxn_amount_per_day[trxn_date] = 0
                        
                        upi_trxn_amount_per_day[trxn_date] += abs(transaction["amount"])
                        if upi_trxn_amount_per_day[trxn_date] > MAXIMUM_UPI_DAILY_LIMIT:
                            max_upi_limit_per_day_fraud_exists = True
            
            ### Max IMPS limit fraud logic
            is_imps_transaction = False
            for regex_item in IMPS_RECOGNISATION_REGEX_LIST:
                if match_regex(transaction["transaction_note"], regex_item, 0):
                    is_imps_transaction = True
                    break
            if (
                transaction["transaction_channel"] == "net_banking_transfer"
                and transaction["transaction_type"] == "debit"
                and is_imps_transaction
            ):
                if transaction["amount"] > MAXIMUM_IMPS_PER_TRANSACTION_LIMIT:
                    fraud_list.append({"fraud_type": "max_imps_limit_per_transaction", "transaction_hash": transaction["hash"]})
                if not max_imps_limit_per_day_fraud_exists:
                    trxn_datetime, _ = check_date(transaction["date"])
                    if trxn_datetime:
                        trxn_date = trxn_datetime.strftime("%Y-%m-%d")
                        if trxn_date not in imps_trxn_amount_per_day:
                            imps_trxn_amount_per_day[trxn_date] = 0
                        
                        imps_trxn_amount_per_day[trxn_date] += abs(transaction["amount"])
                        if imps_trxn_amount_per_day[trxn_date] > MAXIMUM_IMPS_DAILY_LIMIT:
                            max_imps_limit_per_day_fraud_exists = True

        if total_credit == total_debit:
            fraud_list.append({"fraud_type": "equal_credit_debit"})
        if cnt_cash_transactions >= len(transaction_list)/2:
            fraud_list.append({"fraud_type": "mostly_cash_transactions"})
        if max_upi_limit_per_day_fraud_exists:
            fraud_list.append({"fraud_type": "max_upi_limit_per_day"})
        if max_imps_limit_per_day_fraud_exists:
            fraud_list.append({"fraud_type": "max_imps_limit_per_day"})
    return fraud_list


frauds_priority_list = [
    {"description": "The author of the PDF document is a non trusted author", "fraud_type": "author_fraud"},
    # {"description": "PDF modification date not equal to the PDF creation date", "fraud_type": "date_fraud"},
    {"description": "Balance after the transaction is not consistent with the previous balance or overall balance.", "fraud_type": "inconsistent_transaction"},
    {"description": "More cash deposits than the salary amount in a month", "fraud_type": "more_cash_deposits_than_salary"},
    # {"description": "Salary credits are the same for four consecutive months", "fraud_type": "salary_remains_unchanged_4_months"},
    # {"description": "Salary credited at the end of Financial year (FEB-MAR or MAR-APRIL) is unchanged", "fraud_type": "salary_remains_unchanged"},
    # {"description": "Salary credited on bank holidays", "fraud_type": "salary_credited_bank_holiday"},
    # {"description": "Salary credit is a multiple of 1000", "fraud_type": "salary_1000_multiple"},
    {"description": "Number of cash transactions is more than 50%", "fraud_type": "mostly_cash_transactions"},
    {"description": "Total credits and total debits are equal", "fraud_type": "equal_credit_debit"},
    {"description": "Business account with no credit transaction in 15 days", "fraud_type": "more_than_15_days_credit"},
    {"description": "EOD balance is negative for a non business account", "fraud_type": "negative_balance"},
    # {"description": "Tax paid in multiple of 100", "fraud_type": "tax_100_multiple"},
    {"description": "RTGS amount is less than allowed limit", "fraud_type": "min_rtgs_amount"},
    # {"description": "RTGS on bank holidays", "fraud_type": "rtgs_bank_holiday_transaction"},
    # {"description": "Cash Deposit on bank holidays", "fraud_type": "cash_deposit_bank_holiday_transaction"},
    # {"description": "Cheque Transfer on bank holidays", "fraud_type": "chq_bank_holiday_transaction"},
    # {"description": "Outward Cheque Bounce on bank holidays", "fraud_type": "outward_cheque_bounce_bank_holiday_transaction"},
    # {"description": "Inward Cheque Bounce on bank holidays", "fraud_type": "inward_cheque_bounce_bank_holiday_transaction"},
    {"description": "UPI transaction above 5 Lakhs", "fraud_type": "max_upi_limit_per_transaction"},
    {"description": "Total debit UPI transaction amount above 5 Lakhs", "fraud_type": "max_upi_limit_per_day"},
    {"description": "IMPS transaction above 5 Lakhs", "fraud_type": "max_imps_limit_per_transaction"},
    {"description": "Total debit IMPS transaction amount above 5 Lakhs", "fraud_type": "max_imps_limit_per_day"},
]

fraud_category = {
    "author_fraud": "metadata",
    # "date_fraud": "metadata",
    "font_and_encryption_fraud":"metadata",
    "page_hash_fraud":"metadata",
    "identity_name_fraud":"metadata",
    "rgb_fraud":"metadata",
    "good_author_fraud":"metadata",
    "tag_hex_fraud":"metadata",
    "flag_000rg_50_fraud":"metadata",
    "tag_hex_on_page_cnt_fraud":"metadata",
    "TD_cnt_fraud":"metadata",
    "TJ_cnt_fraud":"metadata",
    "touchup_textedit_fraud":"metadata",
    "cnt_of_pagefonts_not_equal_fraud":"metadata",
    "good_font_type_size_fraud":"metadata",
    "Tj_null_cnt_fraud":"metadata",
    "Non_hex_fraud":"metadata",
    "pikepdf_exception":"metadata",
    "inconsistent_transaction": "accounting",
    "more_cash_deposits_than_salary": "behavioural",
    # "salary_remains_unchanged_4_months": "behavioural",
    # "salary_remains_unchanged": "behavioural",
    # "salary_credited_bank_holiday": "transaction",
    # "salary_1000_multiple": "behavioural",
    "mostly_cash_transactions": "behavioural",
    "equal_credit_debit": "behavioural",
    "more_than_15_days_credit": "behavioural",
    "negative_balance": "transaction",
    # "tax_100_multiple": "transaction",
    "min_rtgs_amount": "transaction",
    # "rtgs_bank_holiday_transaction": "transaction",
    # "cash_deposit_bank_holiday_transaction": "transaction",
    # "chq_bank_holiday_transaction": "transaction",
    # "outward_cheque_bounce_bank_holiday_transaction": "transaction",
    # "inward_cheque_bounce_bank_holiday_transaction": "transaction",
    "max_upi_limit_per_transaction": "transaction",
    "max_upi_limit_per_day": "transaction",
    "max_imps_limit_per_transaction": "transaction",
    "max_imps_limit_per_day": "transaction",
}


def merge_partial_transaction_notes(all_transactions: list = [], bank: str="", name: str="", country: str="IN", account_category: str=""):
    new_transactions_list = []
    pages_updated = set()

    for index, transaction in enumerate(all_transactions):
        try:
            if transaction.get("transaction_merge_flag", False) and index == 0:
                pages_updated.add(all_transactions[index].get('page_number', None))
                continue

            if transaction.get("transaction_merge_flag", False) and \
                transaction.get("amount", 0) == DEFAULT_BALANCE_FLOAT and \
                transaction.get("balance", 0) == DEFAULT_BALANCE_FLOAT and \
                transaction.get("date") == DEFAULT_TIMESTAMP_UTC:
                if new_transactions_list:  # Check if list is not empty before accessing [-1]
                    new_transactions_list[-1]['transaction_note'] = new_transactions_list[-1]['transaction_note'] + " " + transaction.get(
                        "transaction_note", "")
                    new_transactions_list[-1]['optimizations'].append('TRANSACTION_NOTES_MERGED')
                    new_transactions_list[-1]['to_update'] = True
                    pages_updated.add(all_transactions[index - 1].get('page_number', None))
                    pages_updated.add(all_transactions[index].get('page_number', None))
            elif transaction.get("transaction_merge_flag", False) in [False, "", None] and \
                transaction.get("amount", 0) != DEFAULT_BALANCE_FLOAT and \
                transaction.get("balance", 0) != DEFAULT_BALANCE_FLOAT and \
                transaction.get("date") != DEFAULT_TIMESTAMP_UTC:
                transaction.pop('transaction_merge_flag', None)
                pages_updated.add(transaction.get('page_number', None))
                new_transactions_list.append(transaction)
        except Exception:
            pass

    print(f"length of new_transactions_list = {len(new_transactions_list)}")

    new_transactions_list = get_transactions_with_updated_categorization(new_transactions_list, bank=bank, name=name, country=country, account_category=account_category)

    return new_transactions_list, pages_updated # Return the processed list


def get_transactions_with_updated_categorization(transactions_list, bank: str="", name: str="", country: str="IN", account_category: str=""):
    
    transactions_df = pd.DataFrame(transactions_list)
    original_df = deepcopy(transactions_df)
    to_update = False

    if 'to_update' in transactions_df.columns:
        filter_condition = original_df['to_update'] == True
        indexes_to_update = original_df[filter_condition].index
        df_to_update = original_df.loc[indexes_to_update]
        transactions_df =  df_to_update
        to_update =  True
    
    transactions_df = get_transaction_channel_description_hash(transactions_df, bank=bank, name=name, country=country, account_category=account_category)
    
    if to_update:
        original_df.loc[indexes_to_update] = transactions_df
        transactions_df = original_df
        transactions_df.drop(columns=['to_update'], inplace=True)
    
    final_transaction_list = transactions_df.to_dict('records')

    return final_transaction_list