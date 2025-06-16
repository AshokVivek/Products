from python.aggregates import (
    get_accounts_for_entity,
    get_enrichment_for_entity,
    is_all_statement_processing_completed,
    is_only_last_account_in_processing_state,
    prepare_identity_clickhouse_stream_data,
    send_event_to_quality,
    update_enrichments_table_multiple_keys,
    update_progress,
    update_progress_on_dashboard,
    get_complete_identity_for_statement,
    map_session_account_status
)
from typing import Any
from sentry_sdk import set_tag, capture_exception
from python.configs import ACCOUNTS_STREAM_NAME, ENRICHMENT_PREDICTORS_FUNCTION, IDENTITY_STREAM_NAME, LAMBDA_LOGGER, s3_resource, BANK_CONNECT_ENRICHMENTS_BUCKET,\
    bank_connect_statement_table, TRANSACTIONS_STREAM_NAME
from python.api_subscriptions.handler import get_subscriptions_handler
from python.context.logging import LoggingContext
from python.kafka_producer import send_large_list_payload_to_kafka, send_data_to_kafka
from python.utils import get_account_warehouse_data, get_date_of_format, prepare_warehouse_data, create_identity_object_for_quality
from python.aggregates import get_transactions_for_statement

from python.bc_apis import get_bank_connect_aggregate_monthly_analysis, get_bank_connect_monthly_analysis, get_bank_connect_predictors, get_bank_connect_eod_balances, get_bank_connect_score_from_lambda
from threading import Thread
import json
import time
import hashlib
from boto3.dynamodb.conditions import Key

from python.utils import async_invoke_enrichment, sync_invoke_xlsx_report_handler, \
    sync_invoke_aggregate_xlsx_report_handler, sync_invoke_xml_report_handler, async_invoke_dms_push_lambda

SUBSCRIPTION_TYPES = ["account_details", "fraud", "transactions", "salary_transactions", "recurring_transactions", "lender_transactions", "top_credits_debits", "statement_stats", "expense_categories"]

ENRICHMENT_TO_FUNCTION_MAPPINGS = {
    "monthly_analysis": {
        "function": "get_bank_connect_monthly_analysis",
        "params": ["entity_id", "monthly_analysis_global", "adjusted_eod", "is_sme", "ignore_self_transfer", "to_reject_account", "caching_enabled"],
    },
    "aggregate_monthly_analysis": {
        "function": "get_bank_connect_aggregate_monthly_analysis",
        "params": ["entity_id", "aggregate_monthly_analysis_global", "adjusted_eod", "is_sme", "ignore_self_transfer", "to_reject_account", "caching_enabled", "month_over_month_aggregated"],
    },
    "predictors": {
        "function": "get_bank_connect_predictors",
        "params": [
            "entity_id",
            "predictors_global",
            "adjusted_eod",
            "account_id",
            "to_remap_predictors",
            "ignore_self_transfer",
            "to_reject_account",
            "caching_enabled"
        ],
    },
    "eod_balances": {
        "function": "get_bank_connect_eod_balances",
        "params": ["entity_id", "eod_balances_global", "adjusted_eod", "is_sme", "to_reject_account", "caching_enabled", "session_dict"],
    },
    "score": {
        "function": "get_bank_connect_score_from_lambda",
        "params": ["entity_id", "is_sme", "to_reject_account"],
    },
}

REPORT_TO_FUNCTION_MAPPINGS = {
    "aggregate_xlsx_report_url": {
        "function": "aggregate_xlsx_report_handler",
        "params": [
            "entity_id",
            "attempt_type_data",
            "aggregate_excel_report_version",
            "is_sme",
            "adjusted_eod",
            "to_remap_predictors",
            "ignore_self_transfer",
            "to_reject_account",
            "caching_enabled",
            "session_dict",
            "metadata"
        ],
    },
    "xlsx_report_url": {
        "function": "xlsx_report_handler",
        "params": [
            "entity_id",
            "attempt_type_data",
            "excel_report_version",
            "account_id",
            "is_sme",
            "adjusted_eod",
            "to_remap_predictors",
            "ignore_self_transfer",
            "to_reject_account",
            "caching_enabled",
            "session_dict",
            "metadata",
            "excel_filename_format"
        ],
    },
    "xml_report_url": {
        "function": "xml_report_handler",
        "params": [
            "entity_id",
            "session_flow",
            "bank_mapping",
            "is_sme",
            "adjusted_eod",
            "caching_enabled",
            "session_date_range",
            "to_reject_account",
            "xml_report_version",
            "session_metadata",
            "aa_data_file_key",
            "bucket_name",
            "event_statement_id"
        ],
    },
}


def cache_subscribed_data_handler(event, context):
    start_time = time.time()
    local_logging_context: LoggingContext = LoggingContext(
        source="cache_subscribed_data_handler"
    )
    
    entity_id = event.get("entity_id")
    event_statement_id = event.get("event_statement_id")
    current_account_id = event.get('account_id')
    fan_out_info_dashboard_resp = event.get("fan_out_info_dashboard_resp")
    statements_processing_status = event.get("statements_processing_status", {})
    warehousing_meta_data = event.get("warehousing_meta_data", {})
    org_metadata = event.get('org_metadata', dict())

    local_logging_context.upsert(entity_id=entity_id, statement_id=event_statement_id)
    
    LAMBDA_LOGGER.info(
       f"Event received in cache_subscribed_data_handler: {event}",
        extra=local_logging_context.store
    )
    
    set_tag("entity_id", entity_id)
    set_tag("statement_id", event_statement_id)
    
    # Getting session account status of all accounts as failed or successful
    session_flow = fan_out_info_dashboard_resp.get("session_flow", False)
    session_accounts_status_response = {}
    if session_flow:
        session_accounts_status_response = map_session_account_status(
            entity_id,
            get_accounts_for_entity(entity_id),
            fan_out_info_dashboard_resp.get('session_date_range'),
            fan_out_info_dashboard_resp.get('acceptance_criteria'),
            fan_out_info_dashboard_resp.get('date_range_approval_criteria'),
            fan_out_info_dashboard_resp.get('is_missing_date_range_enabled'),
            fan_out_info_dashboard_resp.get('accept_anything'),
        )

    # Only cache and stream enrichments when only last account is in processing state
    last_account_in_processing_state = is_only_last_account_in_processing_state(entity_id, current_account_id)
    
    # Streaming transactions to firehose
    one_completed = False
    statement_identity_objects_for_quality = {}
    for statement_id, processing_status in statements_processing_status.items():
        if processing_status=='completed':
            one_completed = True
            statement_identity = get_complete_identity_for_statement(statement_id)
            
            identity_object_for_quality = create_identity_object_for_quality(
                statement_identity.get('identity', dict()),
                metadata_analysis=statement_identity.get('metadata_analysis', dict()),
                statement_id=statement_id,
                org_metadata=org_metadata
            )
            statement_identity_objects_for_quality[statement_id] = identity_object_for_quality
            
            warehouse_data = {
                "entity_id": entity_id,
                "statement_id": statement_id,
                "is_extracted_by_perfios": statement_identity.get("is_extracted_by_perfios", False),
                **warehousing_meta_data
            }
            
            warehouse_data.update({
                "bank_name": statement_identity.get('identity', {}).get("bank_name", ""),
                "account_id": statement_identity.get('identity', {}).get("account_id", ""),
                "account_number": statement_identity.get('identity', {}).get("account_number", ""),
                "txn_from_date": statement_identity.get('date_range', {}).get("from_date", None),
                "txn_to_date":  statement_identity.get('date_range', {}).get("to_date", None),
                "from_date": statement_identity.get('extracted_date_range', {}).get("from_date", None),
                "to_date": statement_identity.get('extracted_date_range', {}).get("to_date", None),
                "account_category": statement_identity.get('identity', {}).get("account_category", ""),
                "is_od_account": statement_identity.get('identity', {}).get("is_od_account", ""),
                "name": statement_identity.get('identity', {}).get("name", "")
            })

            st_transactions, _ = get_transactions_for_statement(statement_id)
            txn_warehouse_data = prepare_warehouse_data(warehouse_data, st_transactions)
            kafka_start_time = time.time()
            send_large_list_payload_to_kafka(payload=txn_warehouse_data, topic_name=TRANSACTIONS_STREAM_NAME, depth=0)
            LAMBDA_LOGGER.info(f"Time for txn streaming to kafka for statement_id {statement_id} is {time.time()-kafka_start_time}", extra=local_logging_context.store)
    
            identity_warehouse_data = {
                "org_id": warehousing_meta_data.get("org_id"),
                "org_name": warehousing_meta_data.get("org_name"),
                "link_id": warehousing_meta_data.get("link_id"),
                "session_flow": fan_out_info_dashboard_resp.get("session_flow"),
                "entity_id": entity_id,
                "account_id": current_account_id,
                "statement_id": statement_id
            }
            identity_warehouse_data = prepare_identity_clickhouse_stream_data(identity_warehouse_data, statement_identity)
            send_data_to_kafka(topic_name=IDENTITY_STREAM_NAME, payload=identity_warehouse_data)
    
    # Streaming enrichments and accounts data to firehose
    LAMBDA_LOGGER.debug(f"Streaming enrichments and accounts data to firehose for {current_account_id}", extra=local_logging_context.store)
    to_reject_account = fan_out_info_dashboard_resp.get("to_reject_account", False)
    if last_account_in_processing_state:
        if one_completed:
            stream_enrichments_to_firehose(event)
        if statements_processing_status and None not in statements_processing_status.values():
            warehousing_meta_data['to_reject_account_enabled'] = False if not to_reject_account else to_reject_account
            accounts_warehouse_data = get_account_warehouse_data(entity_id, warehousing_meta_data)
            send_data_to_kafka(topic_name=ACCOUNTS_STREAM_NAME, payload=accounts_warehouse_data)
    
    api_subscriptions = fan_out_info_dashboard_resp.get("api_subscriptions", [])
    is_dms_push_enabled = fan_out_info_dashboard_resp.get("is_dms_push_enabled", False)
    session_date_range = fan_out_info_dashboard_resp.get("session_date_range", {})
    session_metadata_dict = fan_out_info_dashboard_resp.get('session_metadata', dict())
    session_applicant_id = session_metadata_dict.get('applicantId') if isinstance(session_metadata_dict, dict) else ''

    LAMBDA_LOGGER.info(f"Total time taken to checks before caching start {time.time() - start_time}", extra=local_logging_context.store)

    # Caching not required for entity flow and no api subscriptions
    if not session_flow or len(api_subscriptions)==0:
        execute_downstream_services(event, statement_identity_objects_for_quality, session_accounts_status_response)
        LAMBDA_LOGGER.info(
            "Session flow and api_subscription is not enabled, exiting after downstream activities",
            extra=local_logging_context.store
        )
        return

    field_data: list[tuple[str, Any]] = []
    payload = {
        "entity_id": entity_id,
        "event_statement_id": event_statement_id,
        "session_dict": {
            "is_session_flow": session_flow,
            "from_date": session_date_range.get('from_date'),
            "to_date": session_date_range.get('to_date'),
            "session_applicant_id": session_applicant_id
        }
    }
    payload.update(fan_out_info_dashboard_resp)
    
    entity_enrichment = get_enrichment_for_entity(entity_id)
    
    # Case1: Processing not requested
    is_processing_requested = entity_enrichment.get('is_processing_requested', False)
    if not is_processing_requested:
        field_data.append(('message', 'Is processing is not requested.'))
        update_enrichments_table_multiple_keys(entity_id, field_data)
        execute_downstream_services(event, statement_identity_objects_for_quality, session_accounts_status_response)
        LAMBDA_LOGGER.info(
            "Processing not requested, exiting after downstream activities",
            extra=local_logging_context.store
        )
        return
    
    # processing_status is None for dummy_statement_id when INITIATE_PROCESSING event is called in update_state_fan_out
    if statements_processing_status and None not in statements_processing_status.values():
        if not last_account_in_processing_state:
            field_data.append(('message', 'Last account is not encountered.'))
            update_enrichments_table_multiple_keys(entity_id, field_data)
            execute_downstream_services(event, statement_identity_objects_for_quality, session_accounts_status_response)
            LAMBDA_LOGGER.info(
                "Multiple accounts are still in processing state, exiting after downstream activities",
                extra=local_logging_context.store
            )
            return
    else:
        # Case2B: All statement processing status
        all_statement_processing_completed = is_all_statement_processing_completed(entity_id)
        if not all_statement_processing_completed:
            field_data.append(('message', 'All statements are not processed.'))
            update_enrichments_table_multiple_keys(entity_id, field_data)
            execute_downstream_services(event, statement_identity_objects_for_quality, session_accounts_status_response)
            LAMBDA_LOGGER.info(
                "All statements are not processed, exiting after downstream activities",
                extra=local_logging_context.store
            )
            return

    accounts_to_cache = get_accounts_to_cache(entity_id, current_account_id, to_reject_account)

    # Case3: When all the statements are failed or account is failed in acceptance_criteria
    if len(accounts_to_cache) == 0:
        field_data.append(('caching_status', 'failed'))
        field_data.append(('message', 'All accounts are failed.'))
        update_enrichments_table_multiple_keys(entity_id, field_data)
        execute_downstream_services(event, statement_identity_objects_for_quality, session_accounts_status_response)
        LAMBDA_LOGGER.info(
            "All statements or account is failed, exiting after downstream activities",
            extra=local_logging_context.store
        )
        return

    created_hash = generate_hash(event, accounts_to_cache)
    
    # Case4: When the hash is same and there is no need to cache
    if entity_enrichment.get('entity_hash')==created_hash:
        field_data.append(('message', 'Caching is retried.'))
        update_enrichments_table_multiple_keys(entity_id, field_data)
        execute_downstream_services(event, statement_identity_objects_for_quality, session_accounts_status_response)
        LAMBDA_LOGGER.info(
            "Caching hash is not changed, exiting after downstream activities",
            extra=local_logging_context.store
        )
        return

    # re-compute enrichments and cache
    single_response = {"session_id": entity_id, "accounts": {}}
    cache_all_account_metadata(entity_id, api_subscriptions, to_reject_account, accounts_to_cache, single_response, fan_out_info_dashboard_resp)

    # prep lambda set to execute in parallel
    enrichment_lambdas, reports_lambdas = prep_lambda_set(api_subscriptions)
    execute_lambdas_parallel(payload, enrichment_lambdas, single_response)
    execute_lambdas_parallel(payload, reports_lambdas, single_response)
    
    # Formatting single response
    session_from_date = get_date_of_format(session_date_range.get('from_date'), "%Y-%m-%d")
    session_to_date = get_date_of_format(session_date_range.get('to_date'), "%Y-%m-%d")
    insights = {
        "session_id": entity_id, 
        "session_date_range": {
            "from_date": session_from_date,
            "to_date": session_to_date
        }, 
        "accounts": []
    }
    for account_id, account_data in single_response['accounts'].items():
        account_response = {
            "account_id": account_id,
            "data": {}
        }
        for api_sub in SUBSCRIPTION_TYPES+list(ENRICHMENT_TO_FUNCTION_MAPPINGS.keys()):
            if api_sub in api_subscriptions and api_sub!="aggregate_monthly_analysis":
                account_response["data"][api_sub] = account_data[api_sub]
        insights["accounts"].append(account_response)
    
    if "aggregate_monthly_analysis" in api_subscriptions:
        insights["aggregate_monthly_analysis"] = single_response.get("aggregate_monthly_analysis", {})
    
    # cache everything as a single json
    insights_s3_url = update_single_response_in_s3(entity_id, insights)

    # Update Enrichment DDB Table
    field_data.append(('entity_hash', created_hash))
    field_data.append(('insights_s3_url', insights_s3_url))
    field_data.append(('caching_status', 'completed'))
    field_data.append(('message', 'Success'))
    update_enrichments_table_multiple_keys(entity_id, field_data)

    # Execute downstream services after caching
    execute_downstream_services(event, statement_identity_objects_for_quality, session_accounts_status_response)

    # DMS Push Documents Zip
    if session_flow and is_dms_push_enabled:
        session_metadata = event.get("fan_out_info_dashboard_resp").get("session_metadata", {})
        session_metadata['session_id'] = entity_id
        session_metadata['account_id'] = account_id
        event_payload = {
            "entity_id": entity_id,
            "fan_out_info_dashboard_resp": fan_out_info_dashboard_resp,
            "documents_to_push": ["pdf", "aa", "xlsx"],
            "session_metadata": session_metadata
        }
        async_invoke_dms_push_lambda(event_payload)
        
    LAMBDA_LOGGER.info(
        "Caching and downstream activities execution completed",
        extra=local_logging_context.store
    )
    LAMBDA_LOGGER.info(f"Total time for lambda {time.time() - start_time}", extra=local_logging_context.store)
    return


#######################################################################################
# START: Util functions for update state fan out downstream
#######################################################################################

def cache_all_account_metadata(entity_id, api_subscriptions, to_reject_account, accounts, single_response, fan_out_info_dashboard_resp):
    # FIXME: need to handle metadata with better data structure
    if "transactions_metadata" in api_subscriptions and "transactions" not in api_subscriptions:
        capture_exception(Exception("Found transactions_metadata in the api_subscriptions list but transactions is absent."))
        api_subscriptions.append("transactions")

    if "transactions_metadata" in api_subscriptions:
        api_subscriptions.append("salary_transactions_metadata")
        api_subscriptions.append("top_credits_debits_metadata")
    
    for account in accounts:
        account_id = account["account_id"]
        single_response["accounts"][account_id] = {}
        for api_sub in SUBSCRIPTION_TYPES:
            if api_sub in api_subscriptions:
                access_payload = {
                    "entity_id": entity_id,
                    "account_id": account_id,
                    "subscription_type": f"ACCOUNT_{api_sub.upper()}" if api_sub!="account_details" else api_sub.upper(),
                    "to_reject_account": to_reject_account,
                    "enable_metadata": True if f"{api_sub}_metadata" in api_subscriptions else False,
                    "fan_out_info_dashboard_resp": fan_out_info_dashboard_resp
                }
                single_response["accounts"][account_id][api_sub] = get_subscriptions_handler(access_payload, None)


def update_single_response_in_s3(entity_id, insights):
    object_key = f"cache/entity_{entity_id}/insights.json"
    s3_object = s3_resource.Object(BANK_CONNECT_ENRICHMENTS_BUCKET, object_key) # type: ignore
    s3_object.put(Body=bytes(json.dumps(insights, default=str), encoding="utf-8"))
    return f"s3://{BANK_CONNECT_ENRICHMENTS_BUCKET}/{object_key}"


def generate_hash(event, accounts):
    account_statements = []
    account_updated_at = []
    for account in accounts:
        account_statements += account.get("item_data", {}).get("statements", [])
        account_updated_at.append(str(account["updated_at"]))

    account_statements.sort()
    account_updated_at.sort()

    items_to_be_hashed = ", ".join(account_statements + account_updated_at) + json.dumps(event)
    items_to_be_hashed = items_to_be_hashed.encode("utf-8")
    created_hash = hashlib.md5(items_to_be_hashed).hexdigest()
    return created_hash


def prep_lambda_set(api_subscriptions):
    enrichment_set_to_cache = {}
    report_set_to_cache = {}
    
    if "predictors" in api_subscriptions:
        enrichment_set_to_cache["predictors"] = ENRICHMENT_TO_FUNCTION_MAPPINGS["predictors"]

    if "monthly_analysis" in api_subscriptions:
        enrichment_set_to_cache["monthly_analysis"] = ENRICHMENT_TO_FUNCTION_MAPPINGS["monthly_analysis"]

    if "aggregate_monthly_analysis" in api_subscriptions:
        enrichment_set_to_cache["aggregate_monthly_analysis"] = ENRICHMENT_TO_FUNCTION_MAPPINGS["aggregate_monthly_analysis"]

    if "eod_balances" in api_subscriptions:
        enrichment_set_to_cache["eod_balances"] = ENRICHMENT_TO_FUNCTION_MAPPINGS["eod_balances"]

    if "xlsx_report_url" in api_subscriptions:
        enrichment_set_to_cache["predictors"] = ENRICHMENT_TO_FUNCTION_MAPPINGS["predictors"]
        enrichment_set_to_cache["monthly_analysis"] = ENRICHMENT_TO_FUNCTION_MAPPINGS["monthly_analysis"]
        enrichment_set_to_cache["eod_balances"] = ENRICHMENT_TO_FUNCTION_MAPPINGS["eod_balances"]
        report_set_to_cache["xlsx_report_url"] = REPORT_TO_FUNCTION_MAPPINGS["xlsx_report_url"]

    if "aggregate_xlsx_report_url" in api_subscriptions:
        enrichment_set_to_cache["predictors"] = ENRICHMENT_TO_FUNCTION_MAPPINGS["predictors"]
        enrichment_set_to_cache["monthly_analysis"] = ENRICHMENT_TO_FUNCTION_MAPPINGS["monthly_analysis"]
        enrichment_set_to_cache["eod_balances"] = ENRICHMENT_TO_FUNCTION_MAPPINGS["eod_balances"]
        report_set_to_cache["aggregate_xlsx_report_url"] = REPORT_TO_FUNCTION_MAPPINGS["aggregate_xlsx_report_url"]

    if "xml_report_url" in api_subscriptions:
        enrichment_set_to_cache["predictors"] = ENRICHMENT_TO_FUNCTION_MAPPINGS["predictors"]
        enrichment_set_to_cache["monthly_analysis"] = ENRICHMENT_TO_FUNCTION_MAPPINGS["monthly_analysis"]
        report_set_to_cache["xml_report_url"] = REPORT_TO_FUNCTION_MAPPINGS["xml_report_url"]

    if "score" in api_subscriptions:
        enrichment_set_to_cache["score"] = ENRICHMENT_TO_FUNCTION_MAPPINGS["score"]

    return enrichment_set_to_cache, report_set_to_cache


def execute_lambdas_parallel(
    org_configs,
    items_dict,
    single_response
):
    threads = []
    for _, helper in items_dict.items():
        function_name = helper["function"]
        payload = {}
        for param in helper["params"]:
            if param in org_configs.keys():
                payload[param] = org_configs[param]
        t = Thread(target=lambda_and_response_map, args=(function_name, payload, single_response))
        t.start()
        threads.append(t)
    for t in threads:
        t.join()


def lambda_and_response_map(function_name, payload, single_response):
    if function_name == "get_bank_connect_monthly_analysis":
        payload["monthly_analysis_global"] = {}
        payload["caching_enabled"] = True
        get_bank_connect_monthly_analysis(**payload)
        for account_id in single_response['accounts'].keys():
            single_response['accounts'][account_id]['monthly_analysis'] = payload["monthly_analysis_global"].get(account_id, {})

    if function_name == "get_bank_connect_aggregate_monthly_analysis":
        payload["aggregate_monthly_analysis_global"] = {}
        payload["caching_enabled"] = True
        get_bank_connect_aggregate_monthly_analysis(**payload)
        single_response['aggregate_monthly_analysis'] = payload["aggregate_monthly_analysis_global"]

    if function_name == "get_bank_connect_predictors":
        payload["predictors_global"] = {}
        payload["caching_enabled"] = True
        get_bank_connect_predictors(**payload)
        for account_id in single_response['accounts'].keys():
            single_response['accounts'][account_id]['predictors'] = payload["predictors_global"].get(account_id, {})

    if function_name == "get_bank_connect_eod_balances":
        payload["eod_balances_global"] = {}
        payload["caching_enabled"] = True
        get_bank_connect_eod_balances(**payload)
        for account_id in single_response['accounts'].keys():
            single_response['accounts'][account_id]['eod_balances'] = payload["eod_balances_global"].get(account_id, {})

    if function_name == "get_bank_connect_score_from_lambda":
        payload["scores"] = {}
        get_bank_connect_score_from_lambda(**payload)
        for account_id in single_response['accounts'].keys():
            single_response['accounts'][account_id]['score'] = payload["scores"].get(account_id, {})

    if function_name == "xlsx_report_handler":
        payload["caching_enabled"] = True
        sync_invoke_xlsx_report_handler(payload)

    if function_name == "aggregate_xlsx_report_handler":
        payload["caching_enabled"] = True
        sync_invoke_aggregate_xlsx_report_handler(payload)

    if function_name == "xml_report_handler":
        payload["caching_enabled"] = True
        print(f"This is the payload for xml_report_handler : {payload}")
        sync_invoke_xml_report_handler(payload)

    return None

def get_accounts_to_cache(entity_id, current_account_id, to_reject_account):
    accounts_to_cache = []
    all_accounts = get_accounts_for_entity(entity_id, to_reject_account)
    for account in all_accounts:
        account_statements = account.get('item_data', {}).get('statements', [])
        account_id = account['account_id']
        one_completed = False
        for statement_id in account_statements:
            statement_items = bank_connect_statement_table.query(KeyConditionExpression=Key('statement_id').eq(statement_id))
            if statement_items.get('Count') == 0:
                continue
            statement_item = statement_items.get('Items')[0]
            if account_id==current_account_id:
                processing_status = statement_item.get('transactions_status', 'processing')
            else:
                processing_status = statement_item.get('processing_status', 'processing')
            if processing_status=='completed':
                one_completed = True
                break
        if one_completed:
            accounts_to_cache.append(account)
    return accounts_to_cache

def execute_downstream_services(event, statement_identity_objects_for_quality, session_accounts_status_response):
    
    entity_id = event.get("entity_id")
    account_id = event.get("account_id")
    statements_processing_status = event.get("statements_processing_status", {})
    missing_data = event.get("missing_data", [])
    fan_out_info_dashboard_resp = event.get("fan_out_info_dashboard_resp")
    call_via_initiate_processing = event.get('call_via_initiate_processing', False)
    
    for statement_id, processing_status in statements_processing_status.items():
        data_to_send = {
            'account_id': account_id,
            'is_complete': True,
            'call_via_initiate_processing': call_via_initiate_processing
        }
        if processing_status=='completed':
            data_to_send['is_extracted'] = True
            data_to_send['missing_data'] = missing_data
        elif processing_status=='failed':
            data_to_send['is_extracted'] = False
        
        if processing_status is not None:
            # Excluding INITIATE_PROCESSING event is called in update_state_fan_out
            update_progress(statement_id, 'processing_status', processing_status)
        
        update_progress_on_dashboard(
            statement_id, 
            data_to_send,
            entity_id,
            fan_out_info_dashboard_resp,
            session_accounts_status_response
        )
        if statement_identity_objects_for_quality.get(statement_id) is not None:
            send_event_to_quality(statement_id, entity_id, statement_identity_objects_for_quality[statement_id])

def stream_enrichments_to_firehose(event):
    
    entity_id = event.get("entity_id")
    fan_out_info_dashboard_resp = event.get("fan_out_info_dashboard_resp", {})
    warehouse_data = event.get("warehousing_meta_data", {})
    
    # Streaming of predictors
    predictors_payload = {
        'entity_id': entity_id,
        'stream_enrichment': True,
        'warehouse_data': warehouse_data,
        'custom_flags': {
            'adjusted_eod': fan_out_info_dashboard_resp.get('adjusted_eod', False),
            'to_remap_predictors': fan_out_info_dashboard_resp.get('to_remap_predictors', False),
            'ignore_self_transfer': fan_out_info_dashboard_resp.get('ignore_self_transfer', False)
        }
    }
    
    async_invoke_enrichment(predictors_payload, ENRICHMENT_PREDICTORS_FUNCTION)

#######################################################################################
# END: Util functions for update state fan out downstream
#######################################################################################
