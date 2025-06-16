from library.fraud import transaction_balance_check, get_signed_amount
from library.validations import check_date_order
from copy import deepcopy
from itertools import groupby
import json

BANKS_HAVING_SWAP_TRANSACTIONS_INCONSISTENCY = ['uco', 'pnbbnk', 'hdfc', 'federal', 'indusind', 'icici']
BANKS_HAVING_SPLIT_TRANSACTIONS_INCONSISTENCY = ['kotak', 'pnbbnk', 'yesbnk']
BANKS_HAVING_TRANSACTION_JUMP_INCONSISTENCY = ['federal', 'karur', 'icici', 'canara', 'uco', 'pnbbnk', 'kotak', 'indusind', 'hdfc', 'idfc', 'iob']
BANKS_HAVING_JUMBLED_TRANSACTIONS_INCONSISTENCY = ['karnataka', 'icici', 'federal']
BANKS_HAVING_ONE_JUMP_INCORRECT_BALANCE = ['iob']
BANKS_HAVING_N_SPLIT_TRANSACTIONS = ['iob']

def remove_finvu_aa_inconsistency(transaction_list, bank_name):
    """
        Remove unwanted inconsistency from FINVU AA transaction data
        :param transaction_list: List of transactions (each transaction is a dictionary)
        :return: Updated Transaction list
    """
    if not transaction_list or len(transaction_list) <= 1:
        return transaction_list
    
    allowed_banks = set()
    allowed_banks.update(BANKS_HAVING_SWAP_TRANSACTIONS_INCONSISTENCY)
    allowed_banks.update(BANKS_HAVING_SPLIT_TRANSACTIONS_INCONSISTENCY)
    allowed_banks.update(BANKS_HAVING_TRANSACTION_JUMP_INCONSISTENCY)
    allowed_banks.update(BANKS_HAVING_JUMBLED_TRANSACTIONS_INCONSISTENCY)
    allowed_banks.update(BANKS_HAVING_ONE_JUMP_INCORRECT_BALANCE)
    allowed_banks.update(BANKS_HAVING_N_SPLIT_TRANSACTIONS)
    if bank_name not in allowed_banks:
        return transaction_list
    
    is_inconsistent = transaction_balance_check(transaction_list, bank_name)
    # 1. Swapeed Transactions: Two transactions having same transactionTimestamp, 
    #    if swapping these two trxns removes inconsistency then improve the order.
    if is_inconsistent and bank_name in BANKS_HAVING_SWAP_TRANSACTIONS_INCONSISTENCY:
        transaction_list, _ = swap_inconsistent_trxns(transaction_list, bank_name, is_inconsistent)
        is_inconsistent = transaction_balance_check(transaction_list, bank_name)
    
    # 2. Split Transactions: Two transactions having same transactionTimestamp and balance but different amount
    #    Merging these transactions eradicate inconsistency, but trxns aren't merged we would changing 1st trxn's balance to preserve trxn count.
    if is_inconsistent and bank_name in BANKS_HAVING_SPLIT_TRANSACTIONS_INCONSISTENCY:
        transaction_list = finvu_aa_split_trxns_inconsistency_improvement(transaction_list, bank_name, is_inconsistent)
        is_inconsistent = transaction_balance_check(transaction_list, bank_name)
    
    banks_to_solve_index_lvl_incon = set()
    banks_to_solve_index_lvl_incon.update(BANKS_HAVING_TRANSACTION_JUMP_INCONSISTENCY)
    banks_to_solve_index_lvl_incon.update(BANKS_HAVING_JUMBLED_TRANSACTIONS_INCONSISTENCY)
    banks_to_solve_index_lvl_incon.update(BANKS_HAVING_ONE_JUMP_INCORRECT_BALANCE)
    banks_to_solve_index_lvl_incon.update(BANKS_HAVING_N_SPLIT_TRANSACTIONS)
    # 3. Transaction Jump: A transaction is registered in the sequence before/late it should have been. Ex: (1 4 2 3 5 6) or (1 3 4 2 5 6)
    #    Moving jumped transaction to it's expected position, removes inconsistency.
    if is_inconsistent and (bank_name in banks_to_solve_index_lvl_incon):
        transaction_list = finvu_aa_inconsistency_improvement(transaction_list, bank_name, is_inconsistent)

    return transaction_list


def swap_inconsistent_trxns(transaction_list, bank_name, inconsistent_trxn_hash, attempt_type='aa'):
    """
        Swap two transactions if it removes inconsistency
        Swap Transactions: Two transactions having same TransactionTimestamp and swapped position is correct order
        
        :param 
            transaction_list: List of transactions (each transaction is a dictionary)
            bank_name
            inconsistent_trxn_hash: existing inconsistent transaction hash
        :return: Updated Transaction list
    """
    
    order = 'correct'
    if attempt_type=='pdf':
        order = check_date_order([transaction_list], bank_name)
        if order=='reverse':
            transaction_list = transaction_list[::-1]
    
    transactions_count = len(transaction_list)
    pages_updated = set()
    
    # Fixing inconsistency by swapping first two transactions given second inconsistent transaction
    if attempt_type != 'aa' and len(transaction_list)>1 and inconsistent_trxn_hash==transaction_list[1]['hash']:
        transaction_list[0], transaction_list[1] = transaction_list[1], transaction_list[0]
        is_inconsistent_after_swap = transaction_balance_check(transaction_list, bank_name)
        if is_inconsistent_after_swap in [transaction_list[0]['hash'], transaction_list[1]['hash']]:
            transaction_list[0], transaction_list[1] = transaction_list[1], transaction_list[0]
        else:
            if transaction_list[0].get('page_number') != transaction_list[1].get('page_number'):
                transaction_list[0]['page_number'], transaction_list[1]['page_number'] = transaction_list[1]['page_number'], transaction_list[0]['page_number']
            pages_updated_currently = update_optimizations_and_changed_pages(transaction_list, [0, 1], 'SWAPPED_CONSECUTIVE_TRANSACTIONS')
            pages_updated.update(pages_updated_currently)
            inconsistent_trxn_hash = is_inconsistent_after_swap
    
    trxn_index = 0
    while trxn_index < transactions_count - 1:
        if inconsistent_trxn_hash != transaction_list[trxn_index]['hash']:
            trxn_index += 1
            continue
        if transaction_list[trxn_index]['date'] != transaction_list[trxn_index + 1]['date']:
            break
        
        transaction_list[trxn_index], transaction_list[trxn_index+1] = transaction_list[trxn_index+1], transaction_list[trxn_index]
        is_inconsistent_after_swap = transaction_balance_check(transaction_list, bank_name)
        if is_inconsistent_after_swap in [transaction_list[trxn_index]['hash'], transaction_list[trxn_index + 1]['hash']]:
            transaction_list[trxn_index], transaction_list[trxn_index+1] = transaction_list[trxn_index+1], transaction_list[trxn_index]
            break
        
        if transaction_list[trxn_index].get('page_number') != transaction_list[trxn_index+1].get('page_number'):
            transaction_list[trxn_index]['page_number'], transaction_list[trxn_index+1]['page_number'] = transaction_list[trxn_index+1]['page_number'], transaction_list[trxn_index]['page_number']
        pages_updated_currently = update_optimizations_and_changed_pages(transaction_list, [trxn_index, trxn_index+1], 'SWAPPED_CONSECUTIVE_TRANSACTIONS')
        pages_updated.update(pages_updated_currently)
        if not is_inconsistent_after_swap:
            break
        inconsistent_trxn_hash = is_inconsistent_after_swap
        trxn_index += 2      ## Skipping next transaction check if swapped
    if attempt_type=='pdf' and order=='reverse':
        transaction_list = transaction_list[::-1]
    return transaction_list, pages_updated

def finvu_aa_split_trxns_inconsistency_improvement(transaction_list, bank_name, inconsistent_trxn_hash):
    """
        Update first's transactions balance of split transaction
        Split Transactions: Two transactions having same TransactionTimestamp and balance but probably different amount
        
        :param 
            transaction_list: List of transactions (each transaction is a dictionary)
            bank_name
            inconsistent_trxn_hash: existing inconsistent transaction hash
        :return: Updated Transaction list
    """
    
    transactions_count = len(transaction_list)
    trxn_index = 0
    while trxn_index < transactions_count-1:
        does_current_trxn_pair_has_inconsistency = (transaction_list[trxn_index]['hash'] == inconsistent_trxn_hash) or (transaction_list[trxn_index+1]['hash'] == inconsistent_trxn_hash) 
        inconsistent_three_split_trxn_condition = (
            trxn_index < transactions_count - 2 and
            does_current_trxn_pair_has_inconsistency and
            transaction_list[trxn_index]['date'] == transaction_list[trxn_index + 1]['date'] == transaction_list[trxn_index + 2]['date'] and 
            transaction_list[trxn_index]['balance'] == transaction_list[trxn_index + 1]['balance'] == transaction_list[trxn_index + 2]['balance']
        )
        inconsistent_two_split_trxn_condition = (
            does_current_trxn_pair_has_inconsistency and
            transaction_list[trxn_index]['date'] == transaction_list[trxn_index + 1]['date'] and 
            transaction_list[trxn_index]['balance'] == transaction_list[trxn_index + 1]['balance']
        )
        
        if not inconsistent_three_split_trxn_condition and not inconsistent_two_split_trxn_condition:
            trxn_index += 1
            continue
        
        optimization_type = None
        if inconsistent_three_split_trxn_condition:
            current_signed_amount = float(get_signed_amount(transaction_list[trxn_index])['amount'])
            next_signed_amount = float(get_signed_amount(transaction_list[trxn_index + 1])['amount'])
            second_next_signed_amount = float(get_signed_amount(transaction_list[trxn_index + 2])['amount'])
            if trxn_index > 0:
                previous_balance = float(transaction_list[trxn_index - 1]['balance'])
                if round(previous_balance + current_signed_amount + next_signed_amount + second_next_signed_amount, 2) == round(float(transaction_list[trxn_index]['balance']), 2):
                    transaction_list[trxn_index + 1]['balance'] -= second_next_signed_amount
                    transaction_list[trxn_index]['balance'] -= next_signed_amount + second_next_signed_amount
                    optimization_type = '3_SPLIT_TRANSACTIONS_BALANCE_UPDATE'
            else:
                transaction_list[trxn_index + 1]['balance'] -= second_next_signed_amount
                transaction_list[trxn_index]['balance'] -= next_signed_amount + second_next_signed_amount
                optimization_type = '3_SPLIT_TRANSACTIONS_BALANCE_UPDATE'
        elif inconsistent_two_split_trxn_condition:
            current_signed_amount = float(get_signed_amount(transaction_list[trxn_index])['amount'])
            next_signed_amount = float(get_signed_amount(transaction_list[trxn_index + 1])['amount'])
            if trxn_index > 0:
                previous_balance = float(transaction_list[trxn_index - 1]['balance'])
                if round(previous_balance + current_signed_amount + next_signed_amount, 2) == round(float(transaction_list[trxn_index]['balance']), 2):
                    transaction_list[trxn_index]['balance'] -= next_signed_amount
                    optimization_type = '2_SPLIT_TRANSACTIONS_BALANCE_UPDATE'
            else:
                transaction_list[trxn_index]['balance'] -= next_signed_amount
                optimization_type = '2_SPLIT_TRANSACTIONS_BALANCE_UPDATE'
        index_updated = []
        if optimization_type == '3_SPLIT_TRANSACTIONS_BALANCE_UPDATE':
            index_updated = [trxn_index, trxn_index+1]
            trxn_index += 2
        elif optimization_type == '2_SPLIT_TRANSACTIONS_BALANCE_UPDATE':
            index_updated = [trxn_index]
            trxn_index += 1
        update_optimizations_and_changed_pages(transaction_list, index_updated, optimization_type)
        inconsistent_trxn_hash = transaction_balance_check(transaction_list, bank_name)
        trxn_index += 1
    return transaction_list


def finvu_aa_inconsistency_improvement(transaction_list, bank_name, inconsistent_trxn_hash):
    """
        Move jumped transaction to it's expected position to remove inconsistency.
        Transaction Jump: A transaction is registered in the sequence before/late it should have been. Ex: (1 4 2 3 5 6) or (1 3 4 2 5 6)
        
        :param 
            transaction_list: List of transactions (each transaction is a dictionary)
            bank_name
            inconsistent_trxn_hash: existing inconsistent transaction hash
        :return: Updated Transaction list
    """
    if not transaction_list or len(transaction_list)<=1:
        return transaction_list

    transactions_count = len(transaction_list)
    hash_to_index_map = {None: transactions_count+15}
    for index in range(transactions_count):
        hash_to_index_map[transaction_list[index]['hash']] = index
    trxn_index = hash_to_index_map[inconsistent_trxn_hash]
    while trxn_index < transactions_count:
        if inconsistent_trxn_hash != transaction_list[trxn_index]['hash']:
            trxn_index += 1
            continue
        transaction_list, new_inconsistent_trxn_hash = early_trxn_jump_inconsistency_improvement(transaction_list, bank_name, inconsistent_trxn_hash, hash_to_index_map)
        if new_inconsistent_trxn_hash != inconsistent_trxn_hash:
            trxn_index = hash_to_index_map[new_inconsistent_trxn_hash]
            inconsistent_trxn_hash = new_inconsistent_trxn_hash
            continue
        transaction_list, new_inconsistent_trxn_hash = late_trxn_jump_inconsistency_improvement(transaction_list, bank_name, new_inconsistent_trxn_hash, hash_to_index_map)
        if new_inconsistent_trxn_hash != inconsistent_trxn_hash:
            trxn_index = hash_to_index_map[new_inconsistent_trxn_hash]
            inconsistent_trxn_hash = new_inconsistent_trxn_hash
            continue
        transaction_list, new_inconsistent_trxn_hash = early_trxn_jump_to_top_inconsistency_improvement(transaction_list, bank_name, new_inconsistent_trxn_hash, hash_to_index_map)
        if new_inconsistent_trxn_hash != inconsistent_trxn_hash:
            trxn_index = hash_to_index_map[new_inconsistent_trxn_hash]
            inconsistent_trxn_hash = new_inconsistent_trxn_hash
            continue
        transaction_list, new_inconsistent_trxn_hash = null_pair_trxn_jump_inconsistency_improvement(transaction_list, bank_name, new_inconsistent_trxn_hash, hash_to_index_map)
        if new_inconsistent_trxn_hash != inconsistent_trxn_hash:
            trxn_index = hash_to_index_map[new_inconsistent_trxn_hash]
            inconsistent_trxn_hash = new_inconsistent_trxn_hash
            continue
        transaction_list, new_inconsistent_trxn_hash = permutation_inconsistency_improvement(transaction_list, bank_name, new_inconsistent_trxn_hash, hash_to_index_map)
        if new_inconsistent_trxn_hash != inconsistent_trxn_hash:
            trxn_index = hash_to_index_map[new_inconsistent_trxn_hash]
            inconsistent_trxn_hash = new_inconsistent_trxn_hash
            continue
        transaction_list, new_inconsistent_trxn_hash = n_split_transactions_improvement(transaction_list, bank_name, new_inconsistent_trxn_hash, hash_to_index_map)
        if new_inconsistent_trxn_hash != inconsistent_trxn_hash:
            trxn_index = hash_to_index_map[new_inconsistent_trxn_hash]
            inconsistent_trxn_hash = new_inconsistent_trxn_hash
            continue
        transaction_list, new_inconsistent_trxn_hash = one_jump_incorrect_balance_inconsistency_improvement(transaction_list, bank_name, new_inconsistent_trxn_hash, hash_to_index_map)
        if new_inconsistent_trxn_hash != inconsistent_trxn_hash:
            trxn_index = hash_to_index_map[new_inconsistent_trxn_hash]
            inconsistent_trxn_hash = new_inconsistent_trxn_hash
            continue
        break
    return transaction_list

def n_split_transactions_improvement(transaction_list, bank_name, inconsistent_trxn_hash, hash_to_index_map):
    if not transaction_list or len(transaction_list)<=1 or bank_name not in BANKS_HAVING_N_SPLIT_TRANSACTIONS:
        return transaction_list, inconsistent_trxn_hash
    
    total_transactions = len(transaction_list)
    incon_index = hash_to_index_map[inconsistent_trxn_hash]
    final_balance = transaction_list[incon_index].get('balance')
    prev_balance = transaction_list[incon_index-1].get('balance')
    amounts_sum = 0
    last_same_index = incon_index
    for index in range(incon_index, total_transactions):
        if transaction_list[index].get('balance') != final_balance:
            break
        transaction_type = transaction_list[index].get('transaction_type')
        current_amount = transaction_list[index].get('amount')
        amounts_sum += ( current_amount*-1 if transaction_type=='debit' else current_amount)
        last_same_index = index
    
    if last_same_index!=incon_index:
        if prev_balance+amounts_sum == final_balance:
            copied_transaction_list = json.loads(json.dumps(transaction_list))
            for index in range(incon_index, last_same_index+1):
                current_amount = copied_transaction_list[index].get('amount')
                transaction_type = copied_transaction_list[index].get('transaction_type')
                new_balance = prev_balance + ( current_amount*-1 if transaction_type=='debit' else current_amount)
                update_optimizations_and_changed_pages(copied_transaction_list, [index], 'N_SPLIT_IMPROVEMENT')
                copied_transaction_list[index]['balance'] = new_balance
                prev_balance = new_balance
            
            inconsistent_trxn_hash_after_solving = transaction_balance_check(copied_transaction_list, bank_name)
            if hash_to_index_map[inconsistent_trxn_hash_after_solving] <= incon_index:
                return transaction_list, inconsistent_trxn_hash
            
            return copied_transaction_list, inconsistent_trxn_hash_after_solving
        else:
            return transaction_list, inconsistent_trxn_hash
    else:
        return transaction_list, inconsistent_trxn_hash
    


def one_jump_incorrect_balance_inconsistency_improvement(transaction_list, bank_name, inconsistent_trxn_hash, hash_to_index_map):
    if not transaction_list or len(transaction_list)<=1 or bank_name not in BANKS_HAVING_ONE_JUMP_INCORRECT_BALANCE:
        return transaction_list, inconsistent_trxn_hash
    
    trxn_index = hash_to_index_map[inconsistent_trxn_hash]
    copied_transaction_list = json.loads(json.dumps(transaction_list))
    amount_of_incon_txn = float(get_signed_amount(copied_transaction_list[trxn_index])['amount'])
    
    copied_transaction_list[trxn_index], copied_transaction_list[trxn_index-1] = copied_transaction_list[trxn_index-1], copied_transaction_list[trxn_index]
    updated_balance = copied_transaction_list[trxn_index]['balance'] + amount_of_incon_txn
    copied_transaction_list[trxn_index]['balance'] = round(updated_balance, 2)
    inconsistent_trxn_hash_after_swap = transaction_balance_check(copied_transaction_list, bank_name)
    if hash_to_index_map[inconsistent_trxn_hash_after_swap] <= trxn_index:
        return transaction_list, inconsistent_trxn_hash
    
    update_optimizations_and_changed_pages(copied_transaction_list, [trxn_index-1, trxn_index], f'ONE_JUMP_AND_IMPROVED_BAL_BY_{amount_of_incon_txn}')
    return copied_transaction_list, inconsistent_trxn_hash_after_swap

def early_trxn_jump_inconsistency_improvement(transaction_list, bank_name, inconsistent_trxn_hash, hash_to_index_map):
    """
        Early Transaction Jump: 
            A transaction (or consecutive trxns) is/are registered in the sequence before it/they should have been. 
            Ex: (1 2 6 3 4 5 7 8 9) or (1 2 7 8 3 4 5 6 9)
    """
    if not transaction_list or len(transaction_list)<=1 or bank_name not in BANKS_HAVING_TRANSACTION_JUMP_INCONSISTENCY:
        return transaction_list, inconsistent_trxn_hash
    
    trxn_index = hash_to_index_map[inconsistent_trxn_hash]
    MAX_CONSECUTIVE_TRXN_JUMP = 5
    copied_transaction_list = json.loads(json.dumps(transaction_list))
    consecutive_transactions_list = []
    for consecutive_trxns_count in range(1, MAX_CONSECUTIVE_TRXN_JUMP):
        if trxn_index + consecutive_trxns_count >= len(transaction_list):
            break
        consecutive_transactions_list.append(copied_transaction_list.pop(trxn_index))
        inconsistent_trxn_hash_after_removal = transaction_balance_check(copied_transaction_list, bank_name)
        inconsistent_trxn_index_after_removal = hash_to_index_map[inconsistent_trxn_hash_after_removal]
        if inconsistent_trxn_index_after_removal - consecutive_trxns_count > trxn_index:
            for _ in range(len(consecutive_transactions_list)):
                copied_transaction_list.insert(inconsistent_trxn_index_after_removal-consecutive_trxns_count, consecutive_transactions_list.pop())
            inconsistent_trxn_hash_after_addition = transaction_balance_check(copied_transaction_list, bank_name)
            inconsistent_trxn_index_after_addition = hash_to_index_map[inconsistent_trxn_hash_after_addition]
            if not inconsistent_trxn_hash_after_addition or inconsistent_trxn_index_after_addition > inconsistent_trxn_index_after_removal:
                optimized_transaction_index = []
                for index_item in range(inconsistent_trxn_index_after_removal-consecutive_trxns_count, inconsistent_trxn_index_after_removal):
                    optimized_transaction_index.append(index_item)
                transaction_list = copied_transaction_list
                update_optimizations_and_changed_pages(transaction_list, optimized_transaction_index, 'EARLY_TRANSACTION_JUMP_MOVED')
                inconsistent_trxn_hash = inconsistent_trxn_hash_after_addition
                break
    return transaction_list, inconsistent_trxn_hash


def late_trxn_jump_inconsistency_improvement(transaction_list, bank_name, inconsistent_trxn_hash, hash_to_index_map):
    """
        Late Transaction Jump: 
            A transaction (or consecutive trxns) is/are registered late in the sequence it/they should have been. 
            Ex: (1 2 4 5 6 3 7 8 9) or (1 2 5 6 7 8 9 3 4 10)
    """
    if not transaction_list or len(transaction_list)<=1 or bank_name not in BANKS_HAVING_TRANSACTION_JUMP_INCONSISTENCY:
        return transaction_list, inconsistent_trxn_hash
    
    trxn_index = hash_to_index_map[inconsistent_trxn_hash]
    copied_transaction_list = json.loads(json.dumps(transaction_list))
    next_inconsistent_trxn_hash = transaction_balance_check(copied_transaction_list[trxn_index:], bank_name)
    next_inconsistent_trxn_index = hash_to_index_map[next_inconsistent_trxn_hash]
    if not next_inconsistent_trxn_hash:
        # There wouldn't be any late trxn jump inconsistency.
        return transaction_list, inconsistent_trxn_hash
    
    MAX_CONSECUTIVE_TRXN_JUMP = 5
    consecutive_transactions_list = []
    for consecutive_trxns_count in range(1, MAX_CONSECUTIVE_TRXN_JUMP):
        if next_inconsistent_trxn_index + consecutive_trxns_count >= len(transaction_list):
            break
        consecutive_transactions_list.append(copied_transaction_list.pop(next_inconsistent_trxn_index))
        next_inconsistent_trxn_hash_after_removal = transaction_balance_check(copied_transaction_list[trxn_index:], bank_name)
        next_inconsistent_trxn_index_after_removal = hash_to_index_map[next_inconsistent_trxn_hash_after_removal]
        if not next_inconsistent_trxn_hash_after_removal or next_inconsistent_trxn_index_after_removal-consecutive_trxns_count > next_inconsistent_trxn_index:
            for _ in range(len(consecutive_transactions_list)):
                copied_transaction_list.insert(trxn_index, consecutive_transactions_list.pop())
            new_inconsistent_trxn_hash = transaction_balance_check(copied_transaction_list, bank_name)
            new_inconsistent_trxn_index = hash_to_index_map[new_inconsistent_trxn_hash]
            if not new_inconsistent_trxn_hash or new_inconsistent_trxn_index-consecutive_trxns_count > next_inconsistent_trxn_index:
                optimized_transaction_index = []
                for index_item in range(trxn_index, trxn_index + consecutive_trxns_count):
                    optimized_transaction_index.append(index_item)
                transaction_list = copied_transaction_list
                inconsistent_trxn_hash = new_inconsistent_trxn_hash
                update_optimizations_and_changed_pages(transaction_list, optimized_transaction_index, 'LATE_TRANSACTION_JUMP_MOVED')
                break
    return transaction_list, inconsistent_trxn_hash


def early_trxn_jump_to_top_inconsistency_improvement(transaction_list, bank_name, inconsistent_trxn_hash, hash_to_index_map):
    """
        Early Transaction Jump to Top: 
            A transaction (or consecutive trxns) is/are registered late in the sequence it/they should have been. 
            Ex: (4 1 2 3 5 6 7 8 9) or (6 7 1 2 3 4 5 8 9 10)
    """
    if not transaction_list or len(transaction_list)<=1:
        return transaction_list
    
    trxn_index = hash_to_index_map[inconsistent_trxn_hash]
    MAX_TOP_CONSECUTIVE_TRXN_JUMP = 15
    if trxn_index >= MAX_TOP_CONSECUTIVE_TRXN_JUMP:
        return transaction_list, inconsistent_trxn_hash
    copied_transaction_list = json.loads(json.dumps(transaction_list))
    consecutive_transactions_list = []
    for _ in range(trxn_index):
        consecutive_transactions_list.append(copied_transaction_list.pop(0))
    inconsistent_trxn_hash_after_removal = transaction_balance_check(copied_transaction_list, bank_name)
    inconsistent_trxn_index_after_removal = hash_to_index_map[inconsistent_trxn_hash_after_removal]
    for trxn_item in consecutive_transactions_list[::-1]:
        copied_transaction_list.insert(inconsistent_trxn_index_after_removal-trxn_index, trxn_item)
    inconsistent_trxn_hash_after_addition = transaction_balance_check(copied_transaction_list, bank_name)
    inconsistent_trxn_index_after_addition = hash_to_index_map[inconsistent_trxn_hash_after_addition]
    if not inconsistent_trxn_hash_after_addition or inconsistent_trxn_index_after_addition > inconsistent_trxn_index_after_removal:
        optimized_transaction_index = []
        for index_item in range(inconsistent_trxn_index_after_removal-trxn_index, inconsistent_trxn_index_after_removal):
            optimized_transaction_index.append(index_item)
        transaction_list = copied_transaction_list
        update_optimizations_and_changed_pages(transaction_list, optimized_transaction_index, 'EARLY_TRANSACTION_JUMP_MOVED')
        inconsistent_trxn_hash = inconsistent_trxn_hash_after_addition
    return transaction_list, inconsistent_trxn_hash


def null_pair_trxn_jump_inconsistency_improvement(transaction_list: list, bank_name: str, inconsistent_trxn_hash: str, hash_to_index_map: dict):
    """
        Example:
        1. {"transaction_type": "debit",   "amount": 300.0,    "balance": 15.11    },
        2. {"transaction_type": "credit",  "amount": 200.0,    "balance": 215.11   },
        3. {"transaction_type": "credit",  "amount": 3600.0,   "balance": 3615.11  },
        4. {"transaction_type": "debit",   "amount": 3600.0,   "balance": 15.11    },
        5. {"transaction_type": "credit",  "amount": 624.0,    "balance": 839.11   },
        In this example of 5 transactions, 1-3-4-2-5 is the correct order here. 
        Here (3-4) is 15.11:NULL-pair means this trxn-pair can be attached just below any transaction having balance 15.11, and it wouldn't introduce new inconsistency.
    """
    should_inconsistency_not_be_improved = (
        not transaction_list or 
        len(transaction_list)<=1 or
        not hash_to_index_map or
        bank_name not in BANKS_HAVING_TRANSACTION_JUMP_INCONSISTENCY
    )
    if should_inconsistency_not_be_improved:
        return transaction_list, inconsistent_trxn_hash
    
    transaction_count = len(transaction_list)
    trxn_index = hash_to_index_map.get(inconsistent_trxn_hash, None)
    copied_transaction_list = json.loads(json.dumps(transaction_list))
    if not trxn_index or trxn_index+1 >= transaction_count:
        return transaction_list, inconsistent_trxn_hash
    
    ## Validation 1. :: Check if removing NULL-pair improves inconsistency (atleast solves current one). From ex current_trxn_list: (1-2-5)
    first_inconsistent_transaction = copied_transaction_list.pop(trxn_index)
    second_inconsistent_transaction = copied_transaction_list.pop(trxn_index)
    inconsistent_trxn_hash_after_removal = transaction_balance_check(copied_transaction_list, bank_name)
    inconsistent_trxn_index_after_removal = hash_to_index_map[inconsistent_trxn_hash_after_removal]
    if inconsistent_trxn_index_after_removal - trxn_index <= 2:
        return transaction_list, inconsistent_trxn_hash
    
    ## Validation 2. :: Check if putting NULL-pair just after NULL-pair-balance. In given example NULL-pair-balance = 15.11
    transactions_moved_to_index = -1
    null_pair_balance = second_inconsistent_transaction.get('balance', None)
    if null_pair_balance == None:
        return transaction_list, inconsistent_trxn_hash
    for index in range(transaction_count-2):
        if copied_transaction_list[index]['balance'] == null_pair_balance:
            transactions_moved_to_index = index+1
            copied_transaction_list.insert(transactions_moved_to_index, second_inconsistent_transaction)
            copied_transaction_list.insert(transactions_moved_to_index, first_inconsistent_transaction)
            break
    inconsistent_trxn_hash_after_insertion = transaction_balance_check(copied_transaction_list, bank_name)
    inconsistent_trxn_index_after_insertion = hash_to_index_map[inconsistent_trxn_hash_after_insertion]
    if transactions_moved_to_index == -1 or inconsistent_trxn_index_after_insertion - trxn_index <= 2:
        return transaction_list, inconsistent_trxn_hash
    
    ## Reorder :: update original transaction list and mark the moved transactions
    transaction_list = copied_transaction_list
    optimization_type = 'EARLY_TRANSACTION_JUMP_MOVED'
    if transactions_moved_to_index < trxn_index:
        optimization_type = 'LATE_TRANSACTION_JUMP_MOVED'
    update_optimizations_and_changed_pages(transaction_list, [transactions_moved_to_index, transactions_moved_to_index+1], optimization_type)
    return transaction_list, inconsistent_trxn_hash_after_insertion


def permutation_inconsistency_improvement(transaction_list, bank_name, inconsistent_trxn_hash, hash_to_index_map):
    """
        If local inconsistency can be removed by reordering some N number of transactions.
    """
    if not transaction_list or len(transaction_list)<=1 or bank_name not in BANKS_HAVING_JUMBLED_TRANSACTIONS_INCONSISTENCY:
        return transaction_list, inconsistent_trxn_hash
    
    transactions_count = len(transaction_list)
    trxn_index = hash_to_index_map[inconsistent_trxn_hash]
    copied_transaction_list = json.loads(json.dumps(transaction_list))
    
    correct_trxn_index = trxn_index
    while correct_trxn_index < transactions_count:
        further_inconsistent_trxn_hash = transaction_balance_check(copied_transaction_list[correct_trxn_index:])
        further_inconsistent_trxn_index = hash_to_index_map[further_inconsistent_trxn_hash]
        if further_inconsistent_trxn_index > correct_trxn_index + 5:
            break
        correct_trxn_index += 1
    correct_trxn_index = min(transactions_count - 1, correct_trxn_index + 5)
    if correct_trxn_index == transactions_count - 1:
        tmp_index = trxn_index
        expected_final_balance = copied_transaction_list[trxn_index-1]['balance']
        while tmp_index < transactions_count:
            expected_final_balance = round(expected_final_balance + float(get_signed_amount(copied_transaction_list[tmp_index])['amount']), 2)
            tmp_index += 1
        tmp_index = trxn_index
        swap_with_index = trxn_index
        while tmp_index < transactions_count:
            if expected_final_balance == copied_transaction_list[tmp_index]['balance']:
                swap_with_index = tmp_index
            tmp_index += 1
        copied_transaction_list[correct_trxn_index], copied_transaction_list[swap_with_index] = copied_transaction_list[swap_with_index], copied_transaction_list[correct_trxn_index]

    balance_to_trxn_map = {}
    tmp_index = correct_trxn_index-1
    while tmp_index >= trxn_index-1:
        curr_balance = copied_transaction_list[tmp_index]['balance']
        if curr_balance in balance_to_trxn_map:
            balance_to_trxn_map[curr_balance].append(copied_transaction_list[tmp_index])
        else:
            balance_to_trxn_map[curr_balance] = [copied_transaction_list[tmp_index]]
        tmp_index -= 1
    
    tmp_index = correct_trxn_index
    while tmp_index >= trxn_index:
        curr_transaction = copied_transaction_list[tmp_index]
        prev_trxn_bal = round(float(curr_transaction['balance']) - float(get_signed_amount(curr_transaction)['amount']), 2)
        if prev_trxn_bal not in list(balance_to_trxn_map.keys()):
            return transaction_list, inconsistent_trxn_hash
        
        copied_transaction_list[tmp_index - 1] = balance_to_trxn_map[prev_trxn_bal][0]
        balance_to_trxn_map[prev_trxn_bal].pop(0)
        if len(balance_to_trxn_map[prev_trxn_bal]) == 0:
            balance_to_trxn_map.pop(prev_trxn_bal)
        tmp_index -= 1
    
    new_inconsistent_trxn_hash = transaction_balance_check(copied_transaction_list, bank_name)
    new_inconsistent_trxn_index = hash_to_index_map[new_inconsistent_trxn_hash]
    if new_inconsistent_trxn_index <= correct_trxn_index:
        return transaction_list, inconsistent_trxn_hash
    
    transaction_list = copied_transaction_list
    inconsistent_trxn_hash = new_inconsistent_trxn_hash
    update_optimizations_and_changed_pages(transaction_list, [_ for _ in range(trxn_index, correct_trxn_index+1)], 'PERMUTATION_TRANSACTION_MOVEMENT')
    return transaction_list, inconsistent_trxn_hash


def update_optimizations_and_changed_pages(transaction_list, index_list, optimization_type):
    if index_list in [None, []]:
        return set()
    pages_updated = set()
    for index in index_list:
        if index < len(transaction_list):
            transaction_list[index]['optimizations'].append(optimization_type)
            pages_updated.add(transaction_list[index].get('page_number', None))
    return pages_updated

def fix_yesbnk_inc_transactions(transactions_list, bank='', from_usfo=False):
    inconsistent_hash = transaction_balance_check(transactions_list, bank)
    if not inconsistent_hash:
        return transactions_list, set()
    
    temp_transactions = deepcopy(transactions_list)
    pages_updated = set()
    
    order = check_date_order([temp_transactions], bank)
    
    if order=='reverse':
        temp_transactions = temp_transactions[::-1]
    
    transactions_hash_list = [_['hash'] for _ in temp_transactions]
    index = 0
    SWAP_DISTANCE = 15
    SPLIT_LIMIT = 600
    while index<len(temp_transactions):
        transaction_hash = temp_transactions[index].get('hash')
        if transaction_hash==inconsistent_hash:
            # print("Inconsistent Transaction --> ", temp_transactions[index])
            if temp_transactions[index]['amount']<0:
                temp_transactions[index]['amount'] = abs(temp_transactions[index]['amount'])
                temp_transactions[index]['transaction_type'] = 'credit' if temp_transactions[index]['transaction_type']=='debit' else 'debit'
                temp_transactions[index]['optimizations'].append('TRANSACTION_TYPE_CHANGED')
                pages_updated.add(temp_transactions[index].get('page_number', None))
                inconsistent_hash = transaction_balance_check(temp_transactions, bank)
            if index+1<len(temp_transactions) and index-1>=0 and \
                temp_transactions[index]['amount']==temp_transactions[index+1]['amount'] and \
                    ('insufficient' in [temp_transactions[index]['transaction_note'].replace(" ", "").lower(), temp_transactions[index+1]['transaction_note'].replace(" ", "").lower()]):
                        # print("---------------------- SOLVING INSUFFICIENT COMPLEMENTARY TRANSACTIONS ----------------------")
                        transaction_types = [temp_transactions[index]['transaction_type'], temp_transactions[index+1]['transaction_type']]
                        if transaction_types == ['credit', 'debit']:
                            temp_transactions[index], temp_transactions[index+1] = temp_transactions[index+1], temp_transactions[index]
                            if temp_transactions[index].get('page_number') != temp_transactions[index+1].get('page_number'):
                                    temp_transactions[index]['page_number'], temp_transactions[index+1]['page_number'] = temp_transactions[index+1]['page_number'], temp_transactions[index]['page_number']
                            transaction_types = [temp_transactions[index]['transaction_type'], temp_transactions[index+1]['transaction_type']]
                            pages_updated_temp = update_optimizations_and_changed_pages(temp_transactions, [index, index+1], 'SWAPPED_TRANSACTIONS')
                            pages_updated.update(pages_updated_temp)
                        if transaction_types == ['debit', 'credit']:
                            temp_transactions[index]['balance'] = temp_transactions[index-1]['balance'] - temp_transactions[index]['amount']
                            temp_transactions[index]['optimizations'].append('INSUFFICIENT_BALANCE_CHANGED')
            elif transaction_hash==inconsistent_hash:
                start_index = index-1 if index-1>=0 else None
                end_index = [] if start_index!=None else None
                check = 0
                while start_index!=None and check<2:
                    for _, group in groupby(temp_transactions[start_index+1:start_index+SPLIT_LIMIT], key=lambda x: x['balance']):
                        g = list(group)
                        end_index.append(len(g))
                        if len(g)==1:
                            break
                    end_index = None if len(end_index)==0 or (len(end_index)==1 and end_index[0]==1) else start_index + sum(end_index)
                    if start_index==0 and not end_index and from_usfo:
                        # Handling an edge case where inconsistency is on the second transaction and is splitted with first transaction
                        start_index, end_index = -1, []
                    else:
                        break
                    check += 1
                solving_splits = True
                if start_index!=None and end_index!=None:
                    end_transaction = deepcopy(temp_transactions[end_index])
                    split_signed_amounts = [_['amount'] if _['transaction_type']=='credit' else -_['amount']  for _ in temp_transactions[start_index+1:end_index+1]]
                    start_transaction = deepcopy(temp_transactions[start_index]) if start_index!=-1 else {"balance": round(end_transaction['balance']-sum(split_signed_amounts), 2)}
                    if round(start_transaction['balance']+sum(split_signed_amounts), 2)==end_transaction['balance']:
                        # print("---------------------- SOLVING SPLITS ----------------------")
                        for split_index in range(start_index+1, end_index):
                            signed_amount = temp_transactions[split_index]['amount'] if temp_transactions[split_index]['transaction_type']=='credit' else -temp_transactions[split_index]['amount']
                            balance = temp_transactions[split_index-1]['balance'] if split_index-1>=0 else start_transaction['balance']
                            temp_transactions[split_index]['balance'] = round(balance + signed_amount, 2) 
                            temp_transactions[split_index]['optimizations'].append('SPLIT_BALANCE_CHANGED')
                            pages_updated.add(temp_transactions[split_index].get('page_number', None))
                    else:
                        solving_splits = False
                else:
                    solving_splits = False
                if not solving_splits:
                    # print("---------------------- SOLVING SWAPS ----------------------")
                    i = 1
                    swap_index = index
                    while i<SWAP_DISTANCE:
                        if swap_index+i<len(temp_transactions):
                            temp_transactions[swap_index], temp_transactions[swap_index+i] = temp_transactions[swap_index+i], temp_transactions[swap_index]
                            if temp_transactions[swap_index].get('page_number') != temp_transactions[swap_index+i].get('page_number'):
                                temp_transactions[swap_index]['page_number'], temp_transactions[swap_index+i]['page_number'] = temp_transactions[swap_index+i]['page_number'], temp_transactions[swap_index]['page_number']
                            inconsistent_hash = transaction_balance_check(temp_transactions, bank)
                            if not inconsistent_hash:
                                pages_updated_temp = update_optimizations_and_changed_pages(temp_transactions, [swap_index, swap_index+i], 'SWAPPED_TRANSACTIONS')
                                pages_updated.update(pages_updated_temp)
                                break
                            elif swap_index==transactions_hash_list.index(inconsistent_hash)-i:
                                inconsistent_hash = transaction_hash               
                                temp_transactions[swap_index], temp_transactions[swap_index+i] = temp_transactions[swap_index+i], temp_transactions[swap_index]
                                if temp_transactions[swap_index].get('page_number') != temp_transactions[swap_index+i].get('page_number'):
                                    temp_transactions[swap_index]['page_number'], temp_transactions[swap_index+i]['page_number'] = temp_transactions[swap_index+i]['page_number'], temp_transactions[swap_index]['page_number']
                            else:
                                pages_updated_temp = update_optimizations_and_changed_pages(temp_transactions, [swap_index, swap_index+i], 'SWAPPED_TRANSACTIONS')
                                pages_updated.update(pages_updated_temp)
                                break
                        i += 1
            inconsistent_hash = transaction_balance_check(temp_transactions, bank)
            if not inconsistent_hash:
                if order=='reverse':
                    temp_transactions = temp_transactions[::-1]
                return temp_transactions, pages_updated
        index += 1
    return transactions_list, set()
