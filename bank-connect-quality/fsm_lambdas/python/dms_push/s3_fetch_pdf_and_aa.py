import traceback

from botocore.exceptions import ClientError

from python.configs import (
    s3,
    LAMBDA_LOGGER,
    BANK_CONNECT_UPLOADS_BUCKET,
)
from python.aggregates import get_accounts_for_entity, get_bank_name_for_statement
from python.constants import DOCUMENTS_TO_EXTENSION_MAP


def s3_fetch_pdf_and_aa(entity_id, pdf_folder, aa_folder, documents_to_push, log_data):
    all_accounts = get_accounts_for_entity(entity_id)
    for account in all_accounts:
        account_statements = account.get("item_data", {}).get("statements", [])
        for statement_id in account_statements:
            bank_name = get_bank_name_for_statement(statement_id)
            if not bank_name:
                LAMBDA_LOGGER.warning("bank_name not available", extra=log_data.store)
                continue

            if "pdf" in documents_to_push:
                try:
                    s3.download_file(BANK_CONNECT_UPLOADS_BUCKET, f"pdf/{statement_id}_{bank_name}.pdf", f"{pdf_folder}/{statement_id}_{bank_name}.pdf")
                except ClientError as e:
                    if e.response and e.response.get("Error", {}).get("Code", "") == "404":
                        LAMBDA_LOGGER.warning("PDF not available", extra=log_data.store)
                        continue
                    else:
                        log_data.upsert(exception=str(e), trace=traceback.format_exc())
                        LAMBDA_LOGGER.warning("s3 download failed for pdf", extra=log_data.store)
                        log_data.remove_keys(["exception", "trace"])

            if "aa" in documents_to_push:
                try:
                    s3.download_file(BANK_CONNECT_UPLOADS_BUCKET, f"aa/{statement_id}_{bank_name}.json", f"{aa_folder}/{statement_id}_{bank_name}.json")
                except ClientError as e:
                    if e.response and e.response.get("Error", {}).get("Code", "") == "404":
                        LAMBDA_LOGGER.warning("AA json not available", extra=log_data.store)
                        continue
                    else:
                        log_data.upsert(exception=str(e), trace=traceback.format_exc())
                        LAMBDA_LOGGER.warning("s3 download failed for aa json", extra=log_data.store)
                        log_data.remove_keys(["exception", "trace"])


def fetch_document_from_s3(entity_id, document_to_push, log_data):
    all_accounts = get_accounts_for_entity(entity_id)
    for account in all_accounts:
        account_statements = account.get("item_data", {}).get("statements", [])
        for statement_id in account_statements:
            bank_name = get_bank_name_for_statement(statement_id)

            if not bank_name:
                LAMBDA_LOGGER.warning("bank_name not available", extra=log_data.store)
                continue

            for document in document_to_push:
                extension = DOCUMENTS_TO_EXTENSION_MAP.get(document)
                if not extension:
                    LAMBDA_LOGGER.warning("extension not available", extra=log_data.store)
                    continue

                folder = document_to_push.get(document)
                if not folder:
                    LAMBDA_LOGGER.warning("folder not available", extra=log_data.store)
                    continue

                try:
                    s3.download_file(BANK_CONNECT_UPLOADS_BUCKET, f"{document}/{statement_id}_{bank_name}.{extension}", f"{folder}/{statement_id}_{bank_name}.{extension}")
                except ClientError as e:
                    if e.response and e.response.get("Error", {}).get("Code", "") == "404":
                        LAMBDA_LOGGER.warning(f"{document} not available", extra=log_data.store)
                        continue
                    else:
                        log_data.upsert(exception=str(e), trace=traceback.format_exc())
                        LAMBDA_LOGGER.warning(f"s3 download failed for {document}", extra=log_data.store)
                        log_data.remove_keys(["exception", "trace"])

