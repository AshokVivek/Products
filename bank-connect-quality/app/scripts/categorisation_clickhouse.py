import pandas as pd 
import json
import time
import requests
import os
import concurrent.futures
from app.conf import *
import random
from app.database_utils import clickhouse_client

cwd = os.getcwd()

def multithread_helper(obj):
    # print(obj)
    # print("\n\n")
    entity_id = obj.get('entity_id')
    link_id = obj.get('link_id')
    api_key = obj.get('api_key')
    server_hash = obj.get('server_hash')
    index = obj.get('index')

    headers = {
        'x-api-key': api_key,
        'server-hash': server_hash
    }

    time.sleep(random.randint(1,5))
    IDENTITY_URL = f"https://apis.bankconnect.finbox.in/bank-connect/v1/entity/{entity_id}/identity/"
    response = requests.request("GET", IDENTITY_URL, headers=headers, data={})

    # print(response)
    print(f'got response for identity for {index}')

    response_json_identity = response.json()

    accounts = response_json_identity.get('accounts',[])
    for account in accounts:
        account_id = account.get('account_id', None)
        bank_name = account.get('bank', None)
        account_category = account.get('account_category', None)

        TXN_URL = f"https://apis.bankconnect.finbox.in/bank-connect/v1/entity/{entity_id}/transactions?account_id={account_id}"
        response_txn = requests.request("GET", TXN_URL, headers=headers, data={})
        response_json_txn = response_txn.json()
        transactions = response_json_txn.get('transactions', [])

        if index<10:
            print(f'got transaction response for account_id : {account_id}, at index : {index}')

        params = {
            'transactions':transactions,
            'bank_name':bank_name,
            'account_category':account_category
        }

        response_lamnda = lambda_client.invoke(
            FunctionName = CATEGORISATION_LAMBDA_FUNCTION_NAME, 
            Payload = json.dumps(params), 
            InvocationType = 'RequestResponse'
        )

        payload = json.loads(response_lamnda['Payload'].read().decode('utf-8'))
        # print(payload)
        for x in payload:
            x["account_id"] = account_id
            x["entity_id"] = entity_id
            x["link_id"] = link_id
            if "transaction_channel_regex" in x.keys():
                del x["transaction_channel_regex"]
            if "description_regex" in x.keys():
                del x["description_regex"]

        new_transaction_df = pd.DataFrame(payload)
        # print("Length of df", len(new_transaction_df))
        
        try:
            clickhouse_client.insert_df(
                "bank_connect.transactions_new", new_transaction_df
            )

            # if index<10:
            print(f'inserted data into clickhouse at index : {index}, for account_id : {account_id}')

            clickhouse_client.close()
        except Exception as e:
            print(e)

    if index<10 or (index%10)==0:
        print(f'^^^^^ Done for entity_id : {entity_id}, reached index : {index} ^^^^^^^')
    

def run():
    csv_file_path = cwd+'/app/scripts/combined_2.csv'
    df = pd.read_csv(csv_file_path)
    process_list = []
    for i in range(len(df)):
        entity_id = df['entity_id'][i]
        link_id = df['link_id'][i]
        api_key = df['api_key'][i]
        server_hash = df['server_hash'][i]

        process_list.append({
            'entity_id':entity_id,
            'link_id':link_id,
            'api_key':api_key,
            'server_hash':server_hash,
            'index':i
        })

    # process_list = process_list[:5]
    num_threads = 5  # Adjust as needed
    with concurrent.futures.ThreadPoolExecutor(max_workers=num_threads) as executor:
        executor.map(multithread_helper, process_list)
