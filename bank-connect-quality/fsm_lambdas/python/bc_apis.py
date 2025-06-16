import requests
import os
import json
import time

from python.configs import *

# def get_bank_connect_score(entity_id, scores=None):

#     if not entity_id:
#         return None


#     url = f"{DJANGO_BASE_URL}/bank-connect/v1/entity/{entity_id}/score/"

#     payload={}
#     headers = {
#         'x-api-key': API_KEY,
#         'Content-Type': "application/json",
#     }

#     try:
#         response = requests.request("GET", url, headers=headers, data=payload, timeout=3)
#         response_json = json.loads(response.text)
#         if 'score' in response_json:
#             score = response_json['score']
#             if scores is not None and isinstance(scores, dict):
#                 for items in score:
#                     scores[items.get('account_id')] = items.get("score")
#             return score
#     except Exception as e:
#         print("get_bank_connect_score FAILED --- ", e)

#     return None

def get_bank_connect_score_from_lambda(entity_id, scores = None, is_sme=False, to_reject_account=False):
    payload = {
        'entity_id': entity_id, 
        'version': 'v2',
        'is_sme': is_sme,
        'to_reject_account': to_reject_account
    }
    response = lambda_client.invoke(
        FunctionName = SCORE_LAMBDA_FUNCTION, 
        Payload = json.dumps(payload)
    )
    http_status = response.get('ResponseMetadata',{}).get('HTTPStatusCode')
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
    
    response =  json.loads(response_payload)
    
    if scores is not None and isinstance(scores, dict):
        for object in response['scores']:
            account_id = object.get("account_id")
            scores[account_id] = object.get("score")
    
    return scores

def get_bank_connect_predictors(entity_id, predictors_global=None, adjusted_eod=False, account_id=None, to_remap_predictors=False, ignore_self_transfer=False, to_reject_account=False, caching_enabled=False):
    # invoke predictors lambda to get the predictors data
    payload = {
        'entity_id': entity_id,
        'account_id': account_id,
        'to_reject_account': to_reject_account,
        'custom_flags': {
            'adjusted_eod': adjusted_eod,
            'to_remap_predictors': to_remap_predictors,
            'ignore_self_transfer': ignore_self_transfer
        },
        'caching_enabled': caching_enabled
    }
    response = lambda_client.invoke(
        FunctionName = ENRICHMENT_PREDICTORS_FUNCTION, 
        Payload=json.dumps(payload)
    )

    http_status = response.get('ResponseMetadata', dict()).get('HTTPStatusCode')
    headers = response.get('ResponseMetadata', dict()).get('HTTPHeaders', dict())

    if http_status != 200 or headers.get('x-amz-function-error') != None:
        return dict()

    for i in range(3):
        time.sleep(i)
        try:
            response_payload = response['Payload']._raw_stream.data.decode("utf-8")
            break
        except Exception as e:
            if i >= 2:
                raise e
            print("Excetion in reading Payload from lambda as {}, retrying again".format(e))
    
    if predictors_global is not None and isinstance(predictors_global, dict):
        for items in json.loads(response_payload):
            predictors_global[items.get("account_id")] = items.get("predictors")
    return json.loads(response_payload)

def get_bank_connect_monthly_analysis(entity_id, monthly_analysis_global=None, adjusted_eod=False, is_sme=False, ignore_self_transfer=False, to_reject_account=False, caching_enabled=False):
    payload = {
        'entity_id': entity_id,
        'is_updated_requested': True,
        'to_reject_account': to_reject_account,
        'custom_flags': {
            'adjusted_eod': adjusted_eod,
            'ignore_self_transfer': ignore_self_transfer
        },
        'is_sme': is_sme,
        'caching_enabled': caching_enabled
    }
    print(f"\n\nTriggering enrichments monthly analysis updated Lambda for entity_id: {entity_id}\n\n")
    response = lambda_client.invoke(
            FunctionName =  ENRICHMENT_MONTHLY_ANALYSIS_FUNCTION, 
            Payload = json.dumps(payload),
        )

    http_status = response.get('ResponseMetadata', dict()).get('HTTPStatusCode')
    headers = response.get('ResponseMetadata', dict()).get('HTTPHeaders', dict())

    if http_status != 200 or headers.get('x-amz-function-error') != None:
        return dict()
    response_payload = response['Payload']._raw_stream.data.decode("utf-8")
    response_list = json.loads(response_payload)
    if monthly_analysis_global is not None and isinstance(monthly_analysis_global, dict):
        for account_data in response_list:
            try:
                account_id = list(account_data.keys())[0]
                monthly_analysis_global[account_id] = account_data[account_id].get('monthly_analysis', dict())
            except Exception as e:
                print("Exception Occurred -> ", e)
                pass
    return response_list

def get_bank_connect_aggregate_monthly_analysis(entity_id, aggregate_monthly_analysis_global=None, adjusted_eod=False, is_sme=False, ignore_self_transfer=False, to_reject_account=False, caching_enabled=False, month_over_month_aggregated=False):
    payload = {
        'entity_id': entity_id,
        'is_sme': is_sme,
        'is_updated_requested': False,
        'to_reject_account': to_reject_account,
        'custom_flags': {
            'adjusted_eod': adjusted_eod,
            'ignore_self_transfer': ignore_self_transfer,
            'month_over_month_aggregated': month_over_month_aggregated, # For DMI: To be only send in API and not in Excel Report
        },
        'caching_enabled': caching_enabled
    }
    print(f"\n\nTriggering enrichments aggregate monthly analysis updated Lambda for entity_id: {entity_id}\n\n")
    response = lambda_client.invoke(
            FunctionName =  ENRICHMENT_MONTHLY_ANALYSIS_FUNCTION, 
            Payload = json.dumps(payload),
        )

    http_status = response.get('ResponseMetadata', dict()).get('HTTPStatusCode')
    headers = response.get('ResponseMetadata', dict()).get('HTTPHeaders', dict())

    if http_status != 200 or headers.get('x-amz-function-error') != None:
        return dict()
    response_payload = response['Payload']._raw_stream.data.decode("utf-8")
    response_dict = json.loads(response_payload)
    if aggregate_monthly_analysis_global is not None and isinstance(aggregate_monthly_analysis_global, dict) and isinstance(response_dict, dict):
        for key in response_dict.keys():
            aggregate_monthly_analysis_global[key] = response_dict[key]
    return response_dict

def get_bank_connect_eod_balances(entity_id, eod_balances_global=None, adjusted_eod=False, is_sme=False, to_reject_account=False, caching_enabled=False, session_dict={}):
    payload = {
        'entity_id': entity_id,
        'is_updated_requested': True,
        'to_reject_account': to_reject_account,
        'custom_flags': {
            'adjusted_eod': adjusted_eod
        },
        'is_sme': is_sme,
        'caching_enabled': caching_enabled,
        'session_dict': session_dict
    }
    # Sentry: https://finbox.sentry.io/issues/4708751406/?environment=prod&query=is%3Aunresolved&referrer=issue-stream&statsPeriod=24h&stream_index=4
    for _ in range(2):
        # Retrying to handle empty dict in EOD from enrichments.
        print(f"\n\nTriggering enrichments EOD Balances Lambda for entity_id: {entity_id}\n\n")
        response = lambda_client.invoke(
                FunctionName =  ENRICHMENT_EOD_FUNCTION, 
                Payload = json.dumps(payload),
            )

        http_status = response.get('ResponseMetadata', dict()).get('HTTPStatusCode')
        headers = response.get('ResponseMetadata', dict()).get('HTTPHeaders', dict())

        if http_status != 200 or headers.get('x-amz-function-error') != None:
            print(f"Some error occurred in getting EOD BALANCES {entity_id} -----> ", headers)
            return dict()
        response_payload = response['Payload']._raw_stream.data.decode("utf-8")
        response_list = json.loads(response_payload)
        print(f"Got the EOD BALANCES {entity_id} from Enrichments -----> ", response_list)
        
        if eod_balances_global is not None and isinstance(eod_balances_global, dict) and isinstance(response_list, list):
            for account_data in response_list:
                account_id = account_data.pop('account_id', None)
                eod_balances_global[account_id] = account_data
        if eod_balances_global:
            print(f"Got the EOD BALANCES {entity_id} -----> ", eod_balances_global)
            return response_list
        time.sleep(1)
    print(f"Still didn't recieve EOD BALANCES {entity_id} -----> ", eod_balances_global)
