import requests
import traceback

from sentry_sdk import capture_exception

import base64
import json
import time

from python.configs import LAMBDA_LOGGER, TCAP_DMS_ENDPOINT, TCAP_DMS_AUTH_KEY, KAFKA_TOPIC_DMS_FAILURE_EMAIL
from python.utils import call_django_to_insert_data_into_kafka


def encode_zip_to_base64(zip_path):
    with open(zip_path, "rb") as zip_file:
        zip_data = zip_file.read()
        base64_encoded = base64.b64encode(zip_data)
        return base64_encoded.decode("utf-8")


def dms_push_documents_zip(
    entity_id,
    document_type,
    doc_folder_path,
    fan_out_info_dashboard_resp,
    log_data,
    is_retriggered,
    conversation_id,
    attempts=1,
    backoff_in_seconds=2,
):
    log_data.upsert(
        entity_id=entity_id,
    )
    LAMBDA_LOGGER.info(f"Attempting(attempt count - {attempts}) to upload {document_type} documents zip file", extra=log_data.store)

    session_metadata = fan_out_info_dashboard_resp.get("session_metadata", {})
    LAMBDA_LOGGER.info(f"Session metadata {session_metadata}", extra=log_data.store)

    DMS_PUSH_MAX_RETRY = 2 if not is_retriggered else 0
    if not TCAP_DMS_ENDPOINT:
        capture_exception(Exception("Tcap DMS endpoint not found"))
        return {"status": "failed", "document_type": document_type}

    base64_string = None
    docUploadName = ""
    response_text = ""
    try:
        webtopNo = session_metadata.get("webtopNo", None)
        applicantType = session_metadata.get("applicantType", None)
        docExtension = "xlsx" if document_type == "xlsx" else "zip"
        docUploadName = f"{session_metadata.get('applicantName')}_{entity_id}.{docExtension}"
        # conversation_id = generate_random_string(15)
        log_data.upsert(ConversationID=conversation_id)

        request_headers = {
            "Accept-Encoding": "gzip,deflate",
            "Authorization": TCAP_DMS_AUTH_KEY,
            "ConversationID": conversation_id,
            "SourceName": "Finbox_EL",
            "Content-Type": "application/json",
        }

        LAMBDA_LOGGER.debug(f"Base64 encoding {document_type} zip file", extra=log_data.store)
        base64_string = encode_zip_to_base64(doc_folder_path)
        if not base64_string:
            LAMBDA_LOGGER.error("could not convert zip to base64", extra=log_data.store)
            return {"status": "failed", "document_type": document_type}

        request_json = {
            "webtopNo": webtopNo,
            "applicantType": applicantType,
            "docUploadName": docUploadName
        }

        LAMBDA_LOGGER.info(f"Request being sent to DMS for {document_type}: {request_json}", extra=log_data.store)

        request_json["base64"] = base64_string
        request_payload = json.dumps(request_json)

        response = requests.post(TCAP_DMS_ENDPOINT, data=request_payload, headers=request_headers)
        response_text = response.text
        LAMBDA_LOGGER.info(f"Response got from DMS for {document_type}: Status code: {response.status_code}, Response text: {response_text} and Response headers = {response.headers}", extra=log_data.store)

        if response.status_code == 500:
            raise Exception("DMS server error.")

        response_json = response.json()
        if response.status_code == 200 and isinstance(response_json, dict) and response_json.get("objectID", None) is not None:
            LAMBDA_LOGGER.info(f"DMS Success Response: Status - {response.status_code}, Response - {response_text}", extra=log_data.store)
            return {"status": "success", "response": response_text, "document_type": document_type, "is_retriggered": is_retriggered}

        LAMBDA_LOGGER.warning(f"Failed to send document: {response.status_code} - {response_text}", extra=log_data.store)
        trigger_dms_failure_email(document_type, docUploadName, conversation_id, response_text)
        return {"status": "failed", "response": response_text, "document_type": document_type}
    except Exception as e:
        log_data.upsert(exception=str(e), trace=traceback.format_exc())
        LAMBDA_LOGGER.warning("Error uploading document", extra=log_data.store)
        log_data.remove_keys(["exception", "trace"])

        if attempts > int(DMS_PUSH_MAX_RETRY):
            LAMBDA_LOGGER.warning(
                f"Failed to send document, exhausted max attempts of {int(DMS_PUSH_MAX_RETRY)} against current of {attempts}", extra=log_data.store
            )
            return {"status": "failed", "document_type": document_type}

        sleep = backoff_in_seconds * 2 ** (attempts - 1)
        LAMBDA_LOGGER.warning(f"Retrying request (attempt #{attempts}) in {sleep} seconds...", extra=log_data.store)
        time.sleep(sleep)
        attempts += 1
        dms_push_documents_zip(entity_id, document_type, doc_folder_path, fan_out_info_dashboard_resp, log_data, is_retriggered, conversation_id, attempts)

    trigger_dms_failure_email(document_type, docUploadName, conversation_id, response_text)
    return {"status": "failed", "document_type": document_type}


def trigger_dms_failure_email(document_type, document_name, conversation_id, response_text):
    failure_email_payload = {
        "document_type": document_type,
        "document_name": document_name,
        "conversation_id": conversation_id,
        "response_text": response_text
    }
    try:
        call_django_to_insert_data_into_kafka(KAFKA_TOPIC_DMS_FAILURE_EMAIL, failure_email_payload)
    except Exception as e:
        print(f"Error while sending data to kafka = {e}")
    return