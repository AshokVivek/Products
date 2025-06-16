from app.constants import QUALITY_DATABASE_NAME
from app.database_utils import quality_database, DBConnection
import json
from app.conf import *
import os
from datetime import datetime, timedelta
from app.template_dashboard.utils import create_viewable_presigned_url

def invoke_cache_access_lambda(entity_id, items_needed):
    payload = {
        "entity_id": entity_id,
        "items_needed": items_needed,
    }

    response = lambda_client.invoke(
            FunctionName = CACHE_ACCESS_LAMBDA_FUNCTION_NAME,
            Payload = json.dumps(payload), 
            InvocationType = 'RequestResponse'
        )
    
    http_status = response.get('ResponseMetadata', {}).get('HTTPStatusCode')

    if http_status != 200:
        return {}
    
    response_payload = response['Payload']._raw_stream.data.decode("utf-8")
    return json.loads(response_payload)

def get_account_transactions(entity_id, account_id):
    cache_access_data = invoke_cache_access_lambda(entity_id, ['account_transactions'])
    account_transactions_s3_path = cache_access_data.get(account_id, {}).get("account_transactions")
    if not account_transactions_s3_path:
        return []
    account_transactions_response = s3.get_object(Bucket=FSM_ARBITER_BUCKET, Key=account_transactions_s3_path)
    
    # write a temporary file with content
    file_path = f"/tmp/account_{account_id}_transactions.json"
    with open(file_path, 'wb') as file_obj:
        file_obj.write(account_transactions_response['Body'].read())
    
    data = json.load(open(file_path))
    account_transactions = data.get("transactions")

    if os.path.exists(file_path):
        os.remove(file_path)

    return account_transactions

def get_entity_transactions(entity_id):
    cache_access_data = invoke_cache_access_lambda(entity_id, ['entity_transactions'])
    entity_transactions_s3_path = cache_access_data.get('entity_transactions')
    entity_transactions_response = s3.get_object(Bucket=FSM_ARBITER_BUCKET, Key=entity_transactions_s3_path)
    
    # write a temporary file with content
    file_path = f"/tmp/entity_{entity_id}_transactions.json"
    with open(file_path, 'wb') as file_obj:
        file_obj.write(entity_transactions_response['Body'].read())
    
    data = json.load(open(file_path))
    entity_transactions = data.get("transactions")

    if os.path.exists(file_path):
        os.remove(file_path)

    return entity_transactions

def get_entity_fraud(entity_id):
    cache_access_data = invoke_cache_access_lambda(entity_id, ['entity_fraud'])
    entity_fraud_s3_path = cache_access_data.get('entity_fraud')
    entity_fraud_response = s3.get_object(Bucket=FSM_ARBITER_BUCKET, Key=entity_fraud_s3_path)
    
    # write a temporary file with content
    file_path = f"/tmp/entity_{entity_id}_fraud.json"
    with open(file_path, 'wb') as file_obj:
        file_obj.write(entity_fraud_response['Body'].read())
    
    data = json.load(open(file_path))
    entity_fraud = data.get("entity_fraud", {}).get("fraud_type", [])

    if os.path.exists(file_path):
        os.remove(file_path)
    
    return entity_fraud

def get_statement_transactions_of_account(entity_id, account_id, statement_id_list):
    invoke_cache_access_lambda(entity_id, ['entity_transactions'])

    statement_wise_transactions = {}
    
    for statement_id in statement_id_list:
        supposed_s3_path = f"entity_{entity_id}/account_{account_id}/statement_{statement_id}_transactions.json"
        response = s3.get_object(Bucket=FSM_ARBITER_BUCKET, Key=supposed_s3_path)
        # write a temporary file with content
        file_path = f"/tmp/{statement_id}_transactions.json"
        with open(file_path, 'wb') as file_obj:
            file_obj.write(response['Body'].read())
        data = json.load(open(file_path))
        statement_wise_transactions[statement_id] = data.get("transactions")

        if os.path.exists(file_path):
            os.remove(file_path)
    return statement_wise_transactions

def get_page_level_transactions(entity_id, account_id):
    page_level_paths = invoke_cache_access_lambda(entity_id, ['page_level_transactions'])

    page_level_response = {}
    account_filter = page_level_paths.get(account_id, {}).get("page_level_transactions")
    
    for statement_id in account_filter:
        page_level_response[statement_id] = {}

        for page in account_filter[statement_id]:
            s3_path = account_filter[statement_id][page]
            response = s3.get_object(Bucket=FSM_ARBITER_BUCKET, Key=s3_path)
            file_path = f"/tmp/{statement_id}_{page}_transactions.json"
            with open(file_path, 'wb') as file_obj:
                file_obj.write(response['Body'].read())
            data = json.load(open(file_path))

            page_level_response[statement_id][page] = data.get("transactions")

            if os.path.exists(file_path):
                os.remove(file_path)
    
    return page_level_response

def get_inconsistency_information(entity_id, account_id):
    entity_fraud = get_entity_fraud(entity_id)
    # check if there are any inconsistent items in the entity_fraud for this account_id
    for items in entity_fraud:
        if items.get("account_id") == account_id and items.get("fraud_type") == "inconsistent_transaction":
            return items.get("transaction_hash")
    return None


async def account_ingestion(entity_id, account_id, statement_id, bank_name):
    # check if any ingestion with these records exist, this is will decide whether it is going to be an ingestion or an updation to an existing account
    check_query = """
                        SELECT * from account_quality where entity_id = %(entity_id)s and account_id = %(account_id)s
                    """
    
    check_query_data = DBConnection(QUALITY_DATABASE_NAME).execute_query(
                                    query=check_query,
                                    values={
                                        "entity_id": entity_id,
                                        "account_id": account_id
                                    }
                                )

    entry_present = bool(check_query_data)
    inconsistent_info = None
    if entry_present:
        check_query_data = dict(check_query_data[0])
        statement_list = check_query_data.get("statement_list")
        statement_list.append(statement_id)
        inconsistent_info = get_inconsistency_information(entity_id, account_id)
        # just update the set of statement_list
        update_query = """
                            UPDATE account_quality set 
                            statement_list = %(statement_list)s, 
                            is_inconsistent = %(inconsistent_status)s,
                            inconsistent_context_data = %(context_data)s
                            where 
                            account_id = %(account_id)s and 
                            entity_id = %(entity_id)s
                        """
        DBConnection(QUALITY_DATABASE_NAME).execute_query(
            query=update_query,
            values={
                "entity_id": entity_id,
                "account_id": account_id,
                "inconsistent_status": bool(inconsistent_info),
                "statement_list": json.dumps(statement_list),
                "context_data": json.dumps({"hash": inconsistent_info}) if inconsistent_info else None
            }
        )
    else:
        statement_list = [statement_id]
        inconsistent_info = get_inconsistency_information(entity_id, account_id)
        insert_query = """
                            INSERT INTO account_quality 
                            (entity_id, account_id, bank_name, is_inconsistent, statement_list, inconsistent_context_data)
                            VALUES
                            (%(entity_id)s, %(account_id)s, %(bank_name)s, %(inconsistent_status)s, %(statement_list)s, %(context_data)s)
                        """
        DBConnection(QUALITY_DATABASE_NAME).execute_query(
            query=insert_query,
            values={
                "entity_id": entity_id,
                "account_id": account_id,
                "inconsistent_status": bool(inconsistent_info),
                "statement_list": json.dumps(statement_list),
                "context_data": json.dumps({"hash": inconsistent_info}) if inconsistent_info else None,
                "bank_name": bank_name
            }
        )
    
    return {"message": True}


async def get_inconsistency_cases(from_date, to_date):
    inconsistent_cases_query = """
                                    SELECT entity_id, account_id, statement_list, inconsistent_context_data, bank_name
                                    from account_quality where is_inconsistent=true and inconsistent_ignore_case=false
                                    and created_at >= :from_date and created_at <= :to_date
                                """
    values = {
        "from_date": None,
        "to_date": None
    }
    if from_date and to_date:
        # from date and to date should be in YYYY-MM-DD format
        from_date += ' 00:00:00'
        to_date += ' 11:59:59'
        try:
            from_date = datetime.strptime(from_date, "%Y-%m-%d %H:%M:%S")
            to_date = datetime.strptime(to_date, "%Y-%m-%d %H:%M:%S")
        except Exception as e:
            print(e)
            return {"message": "from and to date must be in `YYYY-MM-DD` format"}
        values["from_date"] = from_date
        values["to_date"] = to_date
    else:
        values["to_date"] = datetime.now()
        values["from_date"] = datetime.now() - timedelta(days=90)
    
    inconsistent_cases_data = await quality_database.fetch_all(inconsistent_cases_query, values)

    result = []
    for items in inconsistent_cases_data:
        data = dict(items)
        data["statement_list"] = json.loads(data["statement_list"])
        data["inconsistent_context_data"] = json.loads(data["inconsistent_context_data"])
        result.append(data)
    
    return {"inconsistent_data": result}


async def get_inconsistent_details(entity_id, account_id):
    check_if_exists = """
                            SELECT * from account_quality where entity_id = :entity_id and account_id = :account_id
                        """
    exists_data = await quality_database.fetch_one(check_if_exists, {
        "entity_id": entity_id,
        "account_id": account_id
    })

    if not exists_data:
        return {"message": "does not exist"}
    
    response = {
        "is_inconsistent": False,
        "inconsistent_dict": {
            "index": None,
            "inconsistent_hash": None,
            "statement_id": "",
            "page_num": 0,
            "statement_presigned_url": "",
            "transaction_list": []
        }
    }
    item_data = dict(exists_data)

    if not item_data.get("is_inconsistent"):
        return response
    
    response["is_inconsistent"] = True

    item_data["statement_list"] = json.loads(item_data["statement_list"])
    item_data["inconsistent_context_data"] = json.loads(item_data["inconsistent_context_data"]) if item_data["inconsistent_context_data"] else None
    inconsistent_hash = item_data["inconsistent_context_data"]["hash"]

    account_transactions = get_account_transactions(entity_id, account_id)
    inconsistent_hash_index = None

    for index, items in enumerate(account_transactions):
        if items.get("hash") == inconsistent_hash:
            inconsistent_hash_index = index
            break
    
    response["inconsistent_dict"]["inconsistent_hash"] = inconsistent_hash
    response["inconsistent_dict"]["transaction_list"] = account_transactions[max(0, (inconsistent_hash_index or 0) - 3): min((inconsistent_hash_index or 0) + 4, len(account_transactions))]
    
    # item index in the transaction_list
    for index, items in enumerate(response["inconsistent_dict"]["transaction_list"]):
        if items.get("hash") == inconsistent_hash:
            response["inconsistent_dict"]["index"] = index
            break

    page_level_transactions = get_page_level_transactions(entity_id, account_id)
    
    for statement_ids in page_level_transactions:
        for pages in page_level_transactions[statement_ids]:
            transactions = page_level_transactions[statement_ids][pages]
            if [_ for _ in transactions if _.get("hash")==inconsistent_hash]:
                response["inconsistent_dict"]["page_num"] = int(pages)
                response["inconsistent_dict"]["statement_id"] = statement_ids
                break
    
    response["inconsistent_dict"]["statement_presigned_url"] = create_viewable_presigned_url(response["inconsistent_dict"]["statement_id"], item_data.get('bank_name'))
    return response, item_data["statement_list"]