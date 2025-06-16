from sentry_sdk import set_context, set_tag
from python.aggregates import (
    extract_advanced_features,
    get_accounts_for_entity,
    async_invoke_cache_subscribed_data
)
from python.utils import get_dashboard_info_for_update_state_fan_out
from python.context.logging import LoggingContext
from python.configs import LAMBDA_LOGGER, RECURRING_MICROSERVICE_TOKEN, RECURRING_MICROSERVICE_URL
import json
import time
import requests
from python.api_utils import call_api_with_session

def update_state_fan_out_handler(event, context):
    local_logging_context: LoggingContext = LoggingContext(
        source="update_state_fan_out_handler"
    )

    # When triggered from SQS, the event variable data structure is different from the default lambda event.
    if isinstance(event, dict) and "Records" in event:
        print("Getting Event Data from SQS")
        LAMBDA_LOGGER.info(
            f"Event received from SQS : {event}",
            extra=local_logging_context.store
        )
        records = event.get("Records")
        if not (isinstance(records, list) and len(records)):
            print("no records were found")
            return
        record = records[0]
        # Body should be a stringified JSON
        body = record.get("body")
        if body is None:
            print("record body was none")
            return
        event = json.loads(body)
    else:
        LAMBDA_LOGGER.info(
            f"Raw event received in fan out lambda : {event}",
            extra=local_logging_context.store,
        )

    entity_id = event.get("entity_id")
    statement_id = event.get("statement_id")
    event_name = event.get("event_name")

    local_logging_context.upsert(
        entity_id=entity_id,
        statement_id=statement_id,
        event_name=event_name
    )

    LAMBDA_LOGGER.info(f"Calling RAMS, for entity_id : {entity_id}", extra=local_logging_context.store)
    call_rams_server(event, local_logging_context)
    return

def call_rams_server(event, local_logging_context):
    url = RECURRING_MICROSERVICE_URL+'recurring_transactions'
    LAMBDA_LOGGER.info(f"Using RAMS URL - {url}", extra=local_logging_context.store)
    try:
        # this is a hack! - TODO: find a better way to do this!
        payload = json.dumps(event, default=str)
        response = call_api_with_session(
            url = url,
            method = "POST",
            headers = {
                "token": RECURRING_MICROSERVICE_TOKEN
            },
            timeout = 10,
            payload = payload,
            params = None
        )
        if response.status_code != 200:
            local_logging_context.upsert(
                ram_response_status_code=response.status_code, 
                ram_response_reason=response.reason,
                request_payload=event,
                response_payload=response.text
            )
            LAMBDA_LOGGER.error(f"RAMS failed with status code: {response.status_code}, response: {response.reason}", extra=local_logging_context.store)
            local_logging_context.remove_keys(['request_payload', 'response_payload'])

    except requests.Timeout as e:
        # this is accepted for now
        LAMBDA_LOGGER.debug(f"RAMS Timeout Exception {e}", extra=local_logging_context.store)

    return True

def usfo_in_rams(event, local_logging_context):
    entity_id = event.get("entity_id")
    statement_id = event.get("statement_id")
    event_name = event.get("event_name")
    org_metadata = event.get("org_metadata")

    statement_meta_data_for_warehousing = event.get(
        "statement_meta_data_for_warehousing", {}
    )

    local_logging_context.upsert(
        entity_id=entity_id,
        statement_id=statement_id,
        event_name=event_name
    )
    set_tag("entity_id", entity_id)
    set_tag("statement_id", statement_id)
    set_context("update_state_fan_out_event_payload", event)

    start_time = time.time()
    if event_name == "INITIATE_PROCESSING":
        LAMBDA_LOGGER.debug(
            "Getting all accounts for entity",
            extra=local_logging_context.store
        )
        accounts = get_accounts_for_entity(entity_id)
        if accounts:
            dummy_statement_id = None
            dummy_account_id = None
            for account in accounts:
                statement_ids = account.get("item_data").get("statements", [])
                if statement_ids and len(statement_ids):
                    dummy_account_id = account.get("account_id")
                    dummy_statement_id = statement_ids[0]
                    break

            if dummy_statement_id:
                LAMBDA_LOGGER.debug(
                    "Fetching dashboard info",
                    extra=local_logging_context.store
                )
                fan_out_info_dashboard_resp = (
                    get_dashboard_info_for_update_state_fan_out(
                        entity_id, dummy_statement_id, local_logging_context=local_logging_context
                    )
                )
                local_logging_context.upsert(source="update_state_fan_out_handler")
                fan_out_info_dashboard_resp["event_name"] = "ENRICHMENT_NOTIFICATION"

                caching_payload = {
                    'entity_id': entity_id,
                    'event_statement_id': dummy_statement_id,
                    'account_id': dummy_account_id,
                    'statements_processing_status': {dummy_statement_id: None},
                    'warehousing_meta_data': statement_meta_data_for_warehousing,
                    'fan_out_info_dashboard_resp': fan_out_info_dashboard_resp,
                    'call_via_initiate_processing': True,
                    'org_metadata': org_metadata
                }
                LAMBDA_LOGGER.debug(
                    "Post processing is triggered",
                    extra=local_logging_context.store
                )
                async_invoke_cache_subscribed_data(caching_payload)
    else:
        LAMBDA_LOGGER.debug(
            "Fetching dashboard info",
            extra=local_logging_context.store
        )
        fan_out_info_dashboard_resp = get_dashboard_info_for_update_state_fan_out(
            entity_id, statement_id, local_logging_context=local_logging_context
        )
        local_logging_context.upsert(source="update_state_fan_out_handler")
        fan_out_info_dashboard_resp["event_name"] = "ENRICHMENT_NOTIFICATION"
        LAMBDA_LOGGER.debug(
            "Calling extract_advanced_features",
            extra=local_logging_context.store
        )
        extract_advanced_features(
            entity_id,
            statement_id,
            statement_meta_data_for_warehousing,
            fan_out_info_dashboard_resp,
            local_logging_context=local_logging_context,
            org_metadata = org_metadata
        )
        local_logging_context.upsert(source="update_state_fan_out_handler")

    LAMBDA_LOGGER.info(
        "Successfully completed update state fan out event, total time taken {}".format(time.time()-start_time),
        extra=local_logging_context.store,
    )
    return {"status": 200}