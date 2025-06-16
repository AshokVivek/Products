from boto3.dynamodb.conditions import Key
from app.conf import TRANSACTIONS_TABLE, STATEMENT_TABLE
import json
from fsm_lambdas.library.fraud import get_correct_transaction_order

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


def check_valid_transaction(transaction):
    amount = transaction.get("amount", None)
    balance = transaction.get("balance",None)
    date = transaction.get("date",None)
    is_in_session_date_range = transaction.get("is_in_session_date_range", True)
    if isinstance(amount, float) and amount != float("inf") and amount != float("-inf") \
        and isinstance(balance, float) and balance != float("inf") and balance != float("-inf") \
        and isinstance(date, str) and date != float("inf") and date != float("-inf"):
            return is_in_session_date_range
    return False

def get_transactions_for_statement(statement_id, keep_same_order = False, send_hash_page_number_map=False, show_rejected_transactions=False):
    qp = {
        'KeyConditionExpression': Key('statement_id').eq(statement_id),
        'ConsistentRead': True, 
        'ProjectionExpression': 'statement_id, page_number, item_data, template_id'
    }

    transaction_items = collect_results(TRANSACTIONS_TABLE.query, qp)
    transactions = list()
    hash_page_number_map = {}

    # print(transaction_items)
    items = STATEMENT_TABLE.query(KeyConditionExpression=Key('statement_id').eq(statement_id))
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
        if check_valid_transaction(transaction):
            valid_transactions.append(transaction)

    return valid_transactions, hash_page_number_map