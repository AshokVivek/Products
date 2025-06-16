import json
import traceback
from datetime import datetime
from logging import exception

import requests

from python.aws_utils import get_json_from_s3_file
from python.configs import LAMBDA_LOGGER, s3, BANK_CONNECT_UPLOADS_BUCKET
from python.context.logging import LoggingContext


def get_data_from_s3_bucket(statement_id, bank_name, bucket, local_logging_context):
    file_object = None
    if not (bank_name and statement_id):
        LAMBDA_LOGGER.warning("No bank_name or statement id found", extra=local_logging_context.store)
        return file_object

    try:
        file_path = f"aa/{statement_id}_{bank_name}.json"
        file_data = s3.get_object(Bucket=bucket, Key=file_path)
        file_object = file_data["Body"].read().decode()
    except Exception as e:
        local_logging_context.upsert(exception=str(e), trace=traceback.format_exc())
        LAMBDA_LOGGER.warning("Failed to fetch file", extra=local_logging_context.store)
        local_logging_context.remove_keys(["exception", "trace"])

    return file_object


def generate_required_mappings(json_object, session_id):
    fi_objects = list()
    required_json_mapping = dict()
    body = json_object.get("body", [])
    header = json_object.get("header", {})
    if body:
        body = body[0]
        fi_objects = body.get("fiObjects")

    if fi_objects:
        fi_object = fi_objects[0]

        if fi_object:
            balance_datetime = fi_object.get("Summary", {}).get("balanceDateTime", "")
            if balance_datetime:
                balance_datetime = datetime.fromisoformat(balance_datetime.replace("Z", "+00:00")).strftime(
                    "%d-%m-%Y %H:%M:%S")
            required_json_mapping = {
                "unique_customer_identifier": body.get("custId", ""),
                "client_txn_id": session_id,
                "consent_id": body.get("consentId", ""),
                "session_id": body.get("sessionId", ""),
                "fi_type": fi_object.get("type", ""),
                "fiu_name": body.get("fipName", ""),
                "profile_id": "",
                "acc_data_fetched": f'{fi_object.get("maskedAccNumber", "")}:{body.get("fipId", "")}',
                "accounts_approved": fi_object.get("maskedAccNumber", ""),
                "fip_names": body.get("fipName", ""),
                "account_type": fi_object.get("Summary", {}).get("type", ""),
                "current_balance": fi_object.get("Summary", {}).get("currentBalance", ""),
                "facility": "",
                "sanction_limit": "",
                "drawing_limit": fi_object.get("Summary", {}).get("drawingLimit", ""),
                "user_aa_handle": body.get("custId", ""),
                "name": fi_object.get("Profile", {}).get("Holders", {}).get("Holder", {}).get("name", ""),
                "address": fi_object.get("Profile", {}).get("Holders", {}).get("Holder", {}).get("address", ""),
                "pan_no": fi_object.get("Profile", {}).get("Holders", {}).get("Holder", {}).get("pan", ""),
                "email_id": fi_object.get("Profile", {}).get("Holders", {}).get("Holder", {}).get("email", ""),
                "user_mobile_no": fi_object.get("Profile", {}).get("Holders", {}).get("Holder", {}).get("mobile"),
                "fetch_request_timestamp": header.get("ts", ""),
                "data_fetch_completion_timestamp": header.get("ts", ""),
                "data_fetch_status": "COMPLETED",
                "balance_date_time": balance_datetime,
                "error": body.get("error", None),
                "acc_consent_approved": f'{fi_object.get("maskedAccNumber", "")}:{body.get("fipId", "")}'
            }

    return required_json_mapping


def generate_balance_xml_mappings(event, local_logging_context: LoggingContext):
    """
    Description:
    This function serves as a handler for fetching the data from S3 and creating the balance XML json
    Parameters:
        - event: dict
            - 'entity_id': str
            - 'bank_mapping': dict
            - 'account_id': str
            - 'is_sme': bool
            - 'adjusted_eod': bool
    Returns:
        Formatted JSON
    """

    local_logging_context.upsert(event=event)
    LAMBDA_LOGGER.info("Balance XML generator called", extra=local_logging_context.store)

    entity_id = event.get("entity_id", "")
    bank_mapping = event.get("bank_mapping", {})
    event_statement_id = event.get("event_statement_id")
    session_metadata = event.get("session_metadata", {})
    bank_name = session_metadata.get("bank_name")

    if not bank_name:
        for account_id in bank_mapping:
            bank_name = bank_mapping.get(account_id, {}).get("bank_name", "")

    file_object = get_data_from_s3_bucket(event_statement_id, bank_name.lower(), BANK_CONNECT_UPLOADS_BUCKET,
                                          local_logging_context)

    if file_object:
        try:
            file_object = json.loads(file_object)
        except exception as e:
            print(f"Error converting file object to json because = {e}")
        json_mapping = generate_required_mappings(file_object, entity_id)
    else:
        json_mapping = dict()

    return json_mapping


def generate_raw_xml_mappings(aa_data_file_key, aa_bucket, local_logging_context: LoggingContext):
    json_mappings = dict()
    local_logging_context.upsert(aa_data_file_key=aa_data_file_key)
    LAMBDA_LOGGER.info("Raw XML generator called", extra=local_logging_context.store)

    if not aa_data_file_key:
        LAMBDA_LOGGER.info("Ignored, aa data file key not found in event", extra=local_logging_context.store)
        return json_mappings

    # getting aa data from s3 bucket
    json_mappings = get_json_from_s3_file(aa_bucket, aa_data_file_key)

    return json_mappings
