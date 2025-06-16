from typing import Any, List

from python.aggregates import (
    get_account_for_entity,
    get_complete_identity_for_statement,
    get_final_account_category,
    get_non_metadata_frauds,
)
from library.fraud import fraud_category
from python.dtos.account.base import AccountItemDataDTO
from pydantic import TypeAdapter

from python.dtos.subscriptions.account_fraud_dto import SubscriptionAPIAccountFraudDTO


def get_account_fraud(event):
    entity_id = event.get("entity_id")
    account_id = event.get("account_id")
    to_reject_account = event.get("to_reject_account", False)

    account = get_account_for_entity(entity_id, account_id, to_reject_account)
    if not account:
        raise Exception("Account data not found")

    account_item_data: Any = account.get("item_data", {})
    if not account_item_data:
        raise Exception("Account item data not found")

    input_account = AccountItemDataDTO(**account_item_data)

    fraud_reasons_model: List[SubscriptionAPIAccountFraudDTO] = list()
    statements = input_account.statements
    account_id = input_account.account_id
    is_od_account = input_account.is_od_account
    account_category = input_account.account_category
    input_is_od_account = input_account.input_is_od_account
    input_account_category = input_account.input_account_category
    account_category, _ = get_final_account_category(account_category, is_od_account, input_account_category, input_is_od_account)

    for statement_id in statements:
        identity = get_complete_identity_for_statement(statement_id)

        # masking every metadata fraud with author_fraud
        metadata_frauds = [_[0] for _ in fraud_category.items() if _[1] == "metadata"]
        if identity.get("fraud_type", None) in metadata_frauds:
            identity["fraud_type"] = "author_fraud"

        if identity.get("is_fraud"):
            # metadata fraud present
            fraud_reason = SubscriptionAPIAccountFraudDTO(**{
                    "statement_id": statement_id, 
                    "fraud_type": identity.get("fraud_type"), 
                    "transaction_hash": None, 
                    "fraud_category": "metadata"
                }
            )
            fraud_reasons_model.append(fraud_reason)

    # get non metadata frauds in required format
    disparities = get_non_metadata_frauds(account_id)

    for disparity in disparities:
        fraud_type = disparity.get("fraud_type", None)
        if account_category in ["CURRENT", "corporate", "overdraft"] and fraud_type == "negative_balance":
            continue
        disparity.pop("account_id", None)
        fraud_reasons_model.append(SubscriptionAPIAccountFraudDTO(**disparity))

    fraud_responses = TypeAdapter(List[SubscriptionAPIAccountFraudDTO]).validate_python(fraud_reasons_model)
    fraud_responses_list = [item.model_dump() for item in fraud_responses]

    return fraud_responses_list

