import json, time
from score.score_v1 import score_helper
from score.score_v2 import score_helper_v2
from python.configs import *


def get_data_from_access_lambda(entity_id, access_type, is_sme=False, to_reject_account=False):
    payload = {
        'entity_id': entity_id,
        'access_type': access_type,
        'is_sme': is_sme,
        'to_reject_account': to_reject_account
    }
    response = lambda_client.invoke(
                    FunctionName = ACCESS_FUNCTION, 
                    Payload=json.dumps(payload)
                )

    http_status = response.get('ResponseMetadata', {}).get('HTTPStatusCode')
    headers = response.get('ResponseMetadata', {}).get('HTTPHeaders', {})

    if http_status != 200 or headers.get('x-amz-function-error') != None:
        return {}

    for i in range(3):
        time.sleep(i)
        try:
            response_payload = response['Payload']._raw_stream.data.decode("utf-8")
            break
        except Exception as e:
            if i >= 2:
                raise e
            print(f"Excetion in reading Payload from lambda as {e}, retrying again")

    return json.loads(response_payload)

def handler(event, context):
    entity_id = event.get('entity_id')
    version = event.get('version', 'v2')
    is_sme = event.get('is_sme', False)
    to_reject_account = event.get('to_reject_account', False)
    
    # if version=='v1':
    #     predictors_data = get_data_from_access_lambda(entity_id, 'ENTITY_ACCOUNT_PREDICTORS')
    #     monthly_analysis_data = get_data_from_access_lambda(entity_id, 'ENTITY_MONTHLY_ANALYSIS', is_sme)

    #     payload = {}
    #     payload["payload"] = []
    #     for items in predictors_data:
    #         account_id = items.get("account_id")
    #         predictors = items.get("predictors")

    #         payload["payload"].append({
    #             "account_id": account_id,
    #             "predictors": predictors,
    #             "monthly_analysis": monthly_analysis_data
    #         })

    #     result = score_helper(payload["payload"])
    if version=='v2':
        print("V2 score request achieved for entity_id : {}".format(entity_id))
        transactions = get_data_from_access_lambda(entity_id, 'ENTITY_TRANSACTIONS', is_sme, to_reject_account)

        # check if s3 link is receieved in the response
        if type(transactions) == dict and transactions.get("s3_object_key", False):
            s3_object_key = transactions.get("s3_object_key")
            result = s3.get_object(Bucket=BANK_CONNECT_DDB_FAILOVER_BUCKET, Key=s3_object_key)['Body'].read()
            transactions = json.loads(result)

        # group the transactions in account level
        account_level_transactions = {}
        
        for transaction in transactions:
            account_id = transaction.get('account_id')
            if account_id not in account_level_transactions:
                account_level_transactions[account_id]=[transaction]
            else:
                account_level_transactions[account_id].append(transaction)
        
        result = score_helper_v2({
            "account_wise_transactions" : account_level_transactions
        })

    else:
        print("Invalid version is requested.")
        return None
    return result