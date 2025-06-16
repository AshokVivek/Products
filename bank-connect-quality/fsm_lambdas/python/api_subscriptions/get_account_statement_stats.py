from python.aggregates import (
    get_bank_name_for_statement,
    get_complete_identity_for_statement,
    get_field_for_statement,
    get_page_count_for_statement,
    get_statement_ids_for_account_id,
    get_transactions_for_statement,
)
from datetime import datetime


def get_account_statement_stats(event):
    entity_id = event.get("entity_id")
    account_id = event.get("account_id")
    fan_out_info_dashboard_resp = event.get("fan_out_info_dashboard_resp", {})

    response = []
    statements = get_statement_ids_for_account_id(entity_id, account_id)
    for statement_id in statements:
        to_insert_obj = {
            "session_id": entity_id,
            "account_id": account_id,
            "statement_id": statement_id,
            "first_transaction_timestamp": None,
            "last_transaction_timestamp": None,
            "bank": None,
        }
        statement_transactions, _ = get_transactions_for_statement(statement_id, False, False)
        if len(statement_transactions) > 0:
            to_insert_obj["first_transaction_timestamp"] = statement_transactions[0].get("date")
            to_insert_obj["last_transaction_timestamp"] = statement_transactions[-1].get("date")

        to_insert_obj["bank"] = get_bank_name_for_statement(statement_id)
        to_insert_obj["source"] = fan_out_info_dashboard_resp.get("metadata", {}).get(account_id, {}).get(statement_id, {}).get("attempt_type")

        page_count = get_page_count_for_statement(statement_id)
        to_insert_obj["page_count"] = page_count
        to_insert_obj["transaction_count"] = len(statement_transactions)

        identity = get_complete_identity_for_statement(statement_id)
        extracted_from_date = identity.get("extracted_date_range", {}).get("from_date")
        extracted_to_date = identity.get("extracted_date_range", {}).get("to_date")
        to_insert_obj['from_date'] = None
        to_insert_obj['to_date'] = None
        if extracted_from_date:
            to_insert_obj["from_date"] = datetime.strptime(extracted_from_date, "%Y-%m-%d").strftime("%Y-%m-%d %H:%M:%S")
        if extracted_to_date:
            to_insert_obj["to_date"] = datetime.strptime(extracted_to_date, "%Y-%m-%d").strftime("%Y-%m-%d %H:%M:%S")

        is_extracted_by_nanonets = get_field_for_statement(statement_id, "is_extracted_by_nanonets")
        is_extracted_by_textract = get_field_for_statement(statement_id, "is_extracted_by_textract")
        is_scanned_pdf = is_extracted_by_nanonets or is_extracted_by_textract
        to_insert_obj['is_scanned_pdf'] = is_scanned_pdf if is_scanned_pdf else False

        response.append(to_insert_obj)

    return response
