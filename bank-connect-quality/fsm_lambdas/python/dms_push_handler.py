import os
import shutil
import traceback
import json

from enum import Enum
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor

from sentry_sdk import capture_exception

from python.clickhouse_data_formatter import prepare_tcap_recurring_pulls_data
from python.configs import s3_resource, LAMBDA_LOGGER, BANK_CONNECT_DMS_PUSH_LOGS_BUCKET, TCAP_CUSTOMERS_STREAM_NAME, \
    DJANGO_BASE_URL, TCAP_RECURRING_AA_PULLS_STREAM_NAME
from python.constants import DMSDocumentType
from python.context.logging import LoggingContext
from python.dms_push.s3_fetch_pdf_and_aa import s3_fetch_pdf_and_aa, fetch_document_from_s3
from python.dms_push.dms_push_documents_zip import dms_push_documents_zip
from python.dms_push.s3_fetch_xlsx import s3_fetch_xlsx
from python.utils import prepare_tcap_customers_data, prepare_tcap_call_details, send_data_to_clickhouse_s3_bucket, \
    generate_random_string, get_statement_type
from python.clickhouse.firehose import send_data_to_firehose


def dms_push_handler(event, context):
    local_logging_context: LoggingContext = LoggingContext(source="dms_push_handler")

    LAMBDA_LOGGER.info(
        f"Event received to dms_push_handler lambda: {event}",
        extra=local_logging_context.store,
    )

    entity_id = event.get("entity_id")
    documents_to_push = event.get("documents_to_push", [])

    local_logging_context.upsert(
        entity_id=entity_id,
    )
    LAMBDA_LOGGER.info("Inside DMS Push Handler", extra=local_logging_context.store)

    if not documents_to_push:
        LAMBDA_LOGGER.warning("documents_to_push list is empty, exiting dms_push_handler",
                              extra=local_logging_context.store)
        return

    dms_handler(event, context)





def check_if_dir_has_files(aa_folder):
    return len(os.listdir(aa_folder)) != 0


def zip_folder(entity_id, xlsx_folder, zip_file_name):
    shutil.make_archive(base_name=f"/tmp/{entity_id}/" + zip_file_name, format="zip", root_dir=xlsx_folder)


def check_if_file_exists(entity_id, xlsx_folder):
    return os.path.isfile(f"{xlsx_folder}/xlsx_report_{entity_id}.xlsx")


def get_folder_address(path):
    os.makedirs(path, exist_ok=True)
    return path


def multithreading_helper(params):
    log_data = params["local_logging_context"]
    document_type = params["document_type"]
    entity_id = params["entity_id"]
    fan_out_info_dashboard_resp = params["fan_out_info_dashboard_resp"]
    doc_folder_path = params["doc_folder_path"]
    is_retriggered = params["is_retriggered"]
    conversation_id = params["conversation_id"]
    return dms_push_documents_zip(entity_id, document_type, doc_folder_path, fan_out_info_dashboard_resp, log_data,
                                  is_retriggered, conversation_id)


def push_statement_docs_to_dms(entity_id, fan_out_info_dashboard_resp, is_retriggered=False,
                               local_logging_context=None):
    dms_responses = dict()
    if not local_logging_context:
        local_logging_context: LoggingContext = LoggingContext(source="push_statement_docs_to_dms")

    LAMBDA_LOGGER.info(
        f"Request received to push statement docs to DMS: {entity_id}",
        extra=local_logging_context.store,
    )

    documents_to_push = {
        DMSDocumentType.PDF.value: get_folder_address(f"/tmp/{entity_id}/pdf/"),
        DMSDocumentType.AA.value: get_folder_address(f"/tmp/{entity_id}/aa/")
    }

    # download pdf and aa data from s3 buckets to local tmp/ folder
    fetch_document_from_s3(entity_id, documents_to_push, local_logging_context)

    for document, path in documents_to_push.items():
        if check_if_dir_has_files(path):
            zip_folder(entity_id, path, f"{document}-zip")
            doc_folder_path = f"/tmp/{entity_id}/{document}-zip.zip"
            conversation_id = generate_random_string(15)
            response = dms_push_documents_zip(entity_id, document.lower(), doc_folder_path, fan_out_info_dashboard_resp,
                                   local_logging_context, is_retriggered, conversation_id)

            dms_responses[document] = response

    return dms_responses


def push_analytical_docs_to_dms(entity_id, fan_out_info_dashboard_resp, is_retriggered=False,
                               local_logging_context=None):
    dms_responses = dict()
    if not local_logging_context:
        local_logging_context: LoggingContext = LoggingContext(source="push_analytical_docs_to_dms")

    LAMBDA_LOGGER.info(
        f"Request received to push analytical docs to DMS: {entity_id}",
        extra=local_logging_context.store,
    )

    documents_to_push = {
        DMSDocumentType.XLSX.value: get_folder_address(f"/tmp/{entity_id}/xlsx/")
    }

    for document in documents_to_push:
        # Get xlsx
        s3_fetch_xlsx(entity_id, fan_out_info_dashboard_resp, documents_to_push[document], local_logging_context)

        # DMS Push the xlsx
        if check_if_file_exists(entity_id, documents_to_push[document]):
            doc_folder_path = f"{documents_to_push[document]}/xlsx_report_{entity_id}.xlsx"
            conversation_id = generate_random_string(15)
            response = dms_push_documents_zip(entity_id, document.lower(), doc_folder_path, fan_out_info_dashboard_resp,
                                   local_logging_context, is_retriggered, conversation_id)
            dms_responses[document] = response

    return dms_responses

# TODO: This function will go in future. Not required in current release
# def create_entry_in_dms_table(dms_response, local_logging_context):
#     create_dms_entry_url = f"{DJANGO_BASE_URL}/bank-connect/v1/dms/"
#
#     if not local_logging_context:
#         local_logging_context: LoggingContext = LoggingContext(source="create_entry_in_dms_table")
#
#     LAMBDA_LOGGER.info(
#         f"Invoking dms entry creation: {entity_id}",
#         extra=local_logging_context.store,
#     )
#
#     LAMBDA_LOGGER.debug(
#         "Invoking dms entry creation",
#         extra=local_logging_context.store
#     )
#
#     payload = json.dumps({"session_id": body.get("session_id", ""),
#                           "notification_type": session_expiry_notification_webhook})
#     headers = {
#         'Content-Type': 'application/json',
#         'x-api-key': API_KEY
#     }
#
#     response = requests.request("POST", initiate_webhook_url, headers=headers, data=payload)
#     log_data["response"] = response.text
#     LAMBDA_LOGGER.info(
#         "Completed the initiate webhook api",
#         extra=log_data
#     )



def dms_handler(event, context):
    local_logging_context: LoggingContext = LoggingContext(source="dms_handler")

    LAMBDA_LOGGER.info(
        f"Event received to dms_handler function: {event}",
        extra=local_logging_context.store,
    )

    entity_id = event.get("entity_id")
    is_retriggered = event.get("is_retriggered", False)
    fan_out_info_dashboard_resp = event.get("fan_out_info_dashboard_resp")
    documents_to_push = event.get("documents_to_push", [])
    api_subscriptions = fan_out_info_dashboard_resp.get("api_subscriptions", [])
    dms_response = event.get("dms_response", {})
    dms_bucket_date = event.get("dms_bucket_date", None)
    session_metadata = event.get("session_metadata", {})
    handler_type = event.get("handler_type", "RE_TRIGGER_PUSH")


    local_logging_context.upsert(
        entity_id=entity_id,
    )
    LAMBDA_LOGGER.info("Inside DMS Handler", extra=local_logging_context.store)

    if not documents_to_push:
        LAMBDA_LOGGER.warning("documents_to_push list is empty, exiting dms_handler",
                              extra=local_logging_context.store)
        return

    os.makedirs(f"/tmp/{entity_id}", exist_ok=True)

    ##########################################################################################################

    ##########################################################################################################

    if handler_type == 'PUSH_STATEMENT':
        try:
            dms_response = push_statement_docs_to_dms(entity_id, fan_out_info_dashboard_resp, is_retriggered, local_logging_context)
        except Exception as e:
            local_logging_context.upsert(exception=str(e), trace=traceback.format_exc())
            LAMBDA_LOGGER.warning("DMS Push Failed", extra=local_logging_context.store)
            local_logging_context.remove_keys(["exception", "trace"])

    if handler_type == 'RE_TRIGGER_PUSH':
        try:
            dms_response = push_statement_docs_to_dms(entity_id, fan_out_info_dashboard_resp, is_retriggered, local_logging_context)
            if "xlsx" in documents_to_push:
                analytical_dms_responses = push_analytical_docs_to_dms(entity_id, fan_out_info_dashboard_resp, is_retriggered, local_logging_context)
                dms_response = {**dms_response, **analytical_dms_responses}
        except Exception as e:
            local_logging_context.upsert(exception=str(e), trace=traceback.format_exc())
            LAMBDA_LOGGER.warning("DMS Push Failed", extra=local_logging_context.store)
            local_logging_context.remove_keys(["exception", "trace"])

    formatted_date = datetime.today().strftime("%d-%m-%Y") if not dms_bucket_date else dms_bucket_date
    dms_status = "failed" if any(value.get("status") == "failed" for value in dms_response.values()) else "success"

    event.update({"dms_response": dms_response})

    push_to_clickhouse(entity_id, session_metadata, dms_response, fan_out_info_dashboard_resp, local_logging_context)

    try:
        object_key = f"{formatted_date}/{dms_status}/entity_{entity_id}.json"
        s3_object = s3_resource.Object(BANK_CONNECT_DMS_PUSH_LOGS_BUCKET, object_key)  # type: ignore
        s3_object.put(Body=bytes(json.dumps(event, default=str), encoding="utf-8"))
    except Exception as e:
        capture_exception(e)

    LAMBDA_LOGGER.info("End of DMS Push Handler", extra=local_logging_context.store)
    return


# pushing data to clickhouse
def push_to_clickhouse(entity_id, session_metadata, dms_response, fan_out_info_dashboard_resp, local_logging_context):
    LAMBDA_LOGGER.info(f"Inside push_to_clickhouse which fan_out_info_dashboard_resp = {fan_out_info_dashboard_resp}", extra=local_logging_context.store)
    aa_journey_mode = fan_out_info_dashboard_resp.get("aa_journey_mode", fan_out_info_dashboard_resp.get("session_metadata", {}).get("aa_journey_mode", ""))
    aa_session_details = fan_out_info_dashboard_resp.get("aa_session_details", dict())

    if aa_journey_mode in ["once_with_recurring", "only_recurring"] and aa_session_details:
        LAMBDA_LOGGER.info("Pushing data to recurring pulls table", extra=local_logging_context.store)
        tcap_recurring_pulls_data = prepare_tcap_recurring_pulls_data(entity_id, fan_out_info_dashboard_resp, local_logging_context)
        LAMBDA_LOGGER.info(f"Data to be pushed to recurring pulls table = {tcap_recurring_pulls_data}", extra=local_logging_context.store)
        send_data_to_firehose([tcap_recurring_pulls_data], TCAP_RECURRING_AA_PULLS_STREAM_NAME)
    else:
        LAMBDA_LOGGER.info(f"Pushing data to customer table with fan_out_info_dashboard_resp = {fan_out_info_dashboard_resp}", extra=local_logging_context.store)
        tcap_customers_data = prepare_tcap_customers_data(session_metadata)
        send_data_to_firehose([tcap_customers_data], TCAP_CUSTOMERS_STREAM_NAME)

        tcap_call_details = prepare_tcap_call_details(session_metadata, dms_response)
        tcap_call_details['Destination'] = get_statement_type(fan_out_info_dashboard_resp)
        send_data_to_clickhouse_s3_bucket(tcap_call_details)
