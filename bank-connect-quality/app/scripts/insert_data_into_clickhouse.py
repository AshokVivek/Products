import json, pandas as pd, requests
from app.conf import *
from datetime import datetime
from app.database_utils import clickhouse_client
import threading

def invoke_lambda_and_get_data(payload, lambda_name):
    lambda_response = lambda_client.invoke(
        FunctionName=lambda_name,
        Payload=json.dumps(payload),
        InvocationType="RequestResponse",
    )
    result = json.loads(lambda_response["Payload"].read().decode("utf-8"))
    if type(result) == dict and result.get("s3_object_key", False):
        s3_object_key = result.get("s3_object_key")
        result = s3.get_object(Bucket=BANK_CONNECT_DDB_FAILOVER_BUCKET, Key=s3_object_key)['Body'].read()
        result = json.loads(result)
    return result

def get_transactions_data_from_access_lambda(entity_id, is_sme):
    print("ingesting for entity_id : ", entity_id)
    entity_id = entity_id
    is_sme = is_sme

    accounts_payload = {
        "entity_id": entity_id,
        'access_type': 'ENTITY_ACCOUNTS'
    }
    accounts = invoke_lambda_and_get_data(accounts_payload, ACCESS_LAMBDA_FUNCTION_NAME)

    payload = {
        'entity_id': entity_id,
        'access_type': 'ENTITY_TRANSACTIONS',
        'is_sme' : is_sme
    }
    
    transactions = invoke_lambda_and_get_data(payload, ACCESS_LAMBDA_FUNCTION_NAME)
    if not transactions:
        print(f"transactions for entity_id : {entity_id} not present")
    unique_account_ids = []
    
    for txn in transactions:
        account_id = txn["account_id"]
        txn["created_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        txn["entity_id"] = entity_id
        if account_id not in unique_account_ids:
            unique_account_ids.append(account_id)
    
    print(f"accounts in {entity_id} are {unique_account_ids}")
    for account_id in unique_account_ids:
        account_transactions = [_ for _ in transactions if _.get("account_id")==account_id]
        if not account_transactions:
            continue
        # print(account_transactions)
        filtered_account = [_ for _ in accounts if _.get("account_id")==account_id]
        lambda_payload = {"bank_name": filtered_account[0]["bank"], "transactions": transactions}
        lambda_response = lambda_client.invoke(
            FunctionName = CATEGORISATION_LAMBDA_FUNCTION_NAME,
            Payload = json.dumps(lambda_payload),
            InvocationType = "RequestResponse",
        )
        data = json.loads(lambda_response["Payload"].read().decode("utf-8"))
        new_transaction_df = pd.DataFrame(data)
        clickhouse_client.insert_df(
            "bank_connect.transactions_quality_run", new_transaction_df
        )
    
    clickhouse_client.close()

def orchestrate_ingestion(presigned_s3_url):
    file_name = f"transactions.xlsx"
    result = requests.get(presigned_s3_url)
    with open(file_name, mode="wb") as file:
        file.write(result.content)
    df = pd.read_excel(file_name)
    df.fillna('', inplace=True)
    total_count = len(df)
    print("total count of entities to ingest: ", total_count)
    THREAD_COUNT = 10
    for i in range(0, total_count, THREAD_COUNT):
        print("\n\n\n\n")
        threads_list = []
        for j in range(0, THREAD_COUNT):
            item_index = (i * THREAD_COUNT) + j
            df_row = df.loc[item_index].to_list()
            entity_id = df_row[1]
            is_sme = df_row[3]
            t = threading.Thread(target=get_transactions_data_from_access_lambda, args=(entity_id, bool(is_sme)))
            t.start()
            threads_list.append(t)
        for t in threads_list:
            t.join()
    os.remove(file_name)

def orchestrate_ingestion_serial(presigned_s3_url):
    file_name = f"transactions.xlsx"
    result = requests.get(presigned_s3_url)
    with open(file_name, mode="wb") as file:
        file.write(result.content)
    df = pd.read_excel(file_name)
    df.fillna('', inplace=True)
    total_count = len(df)
    print("total count of entities to ingest: ", total_count)
    for index, rows in df.iterrows():
        try:
            entity_id = rows['entity_id']
            is_sme = rows['is_sme']
            check_query = f"""
                        select count(*) as cnt from bank_connect.transactions_quality_run
                        where entity_id = '{entity_id}'
                    """
            data = clickhouse_client.query_df(check_query)
            cnt = data['cnt'][0]
            if cnt:
                continue
            print("serial invocation for ", entity_id)
            get_transactions_data_from_access_lambda(entity_id, is_sme)
        except Exception as e:
            print(e)
    os.remove(file_name)

def refresh_single_category():
    entity_id_query = """
                        select distinct entity_id from bank_connect.transactions
                    """
    entity_ids_data = clickhouse_client.query_df(entity_id_query)
    entity_ids_data.fillna('', inplace=True)
    total_count = len(entity_ids_data)
    print("total count of entities to ingest: ", total_count)
    for index, rows in entity_ids_data.iterrows():
        try:
            entity_id = rows['entity_id']
            is_sme = False
            check_query = f"""
                        select count(*) as cnt from bank_connect.transactions_quality_run
                        where entity_id = '{entity_id}'
                    """
            data = clickhouse_client.query_df(check_query)
            cnt = data['cnt'][0]
            if cnt:
                continue
            print("serial invocation for ", entity_id)
            get_transactions_data_from_access_lambda(entity_id, is_sme)
        except Exception as e:
            print(e)