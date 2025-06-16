import json
import time
from app.ddb_utils import collect_results
from boto3.dynamodb.conditions import Key

from app.conf import TRANSACTIONS_TABLE, STATEMENT_TABLE

def update_transactions_on_page(statement_id, page_number, transactions):
    print(f"updating transactions for page {page_number} for statement_id {statement_id}")
    print(f"transactions: {transactions}")
    TRANSACTIONS_TABLE.update_item(
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

def get_page_count(statement_id):
    print(f"getting page count for statement_id {statement_id}")
    count_item = STATEMENT_TABLE.query(KeyConditionExpression=Key('statement_id').eq(statement_id))
    all_items = count_item.get('Items')
    if len(all_items) == 0:
        return None

    page_count = int(all_items[0]["page_count"])
    print(f"page count for statement_id {statement_id} is {page_count}")

    return page_count

def get_transactions_for_statement_page(statement_id, page_number):
    qp = {
        'KeyConditionExpression': Key('statement_id').eq(statement_id) & Key('page_number').eq(page_number),
        'ConsistentRead': True, 
        'ProjectionExpression': 'statement_id, page_number, item_data'
    }

    print(f"getting transactions for statement_id {statement_id} and page_number {page_number}")
    transaction_items = collect_results(TRANSACTIONS_TABLE.query, qp)
    if len(transaction_items)>0:
        transactions = json.loads(transaction_items[0].get('item_data', []))
        for trans in transactions:
            trans['page_number'] = int(page_number)
        return transactions
    
    print(f"no transactions found for statement_id {statement_id} and page_number {page_number}")
    print(f"transactions: {transaction_items}")

    return []

def copy_transactions_between_statements(from_statement_id, to_statement_id):   
     
    print(f"getting page count for {to_statement_id}")
    page_count = get_page_count(to_statement_id)
    print(f"page count for {to_statement_id} is {page_count}")
    for page in range(page_count):
        print(f"copying transactions for page {page} for statment_id {to_statement_id}")
        txns = get_transactions_for_statement_page(from_statement_id, page)
        for t in txns:
            print(f"transaction: {t}")
            t['account_number'] = None

        update_transactions_on_page(to_statement_id, page, txns)

        print(f"transactions for page {page} for statment_id {to_statement_id} copied")
        print(f"updated transactions for page {page} for statment_id {to_statement_id}")