from typing import List

from python.aggregates import (
    get_account_for_entity,
    get_salary_transactions_from_ddb,
)
from python.dtos.subscriptions.account_salary_transaction_dto import SubscriptionAPIAccountSalaryTransactionDTO
from python.handlers import transactions_sanity_checker
from pydantic import TypeAdapter
from sentry_sdk import capture_exception


def get_account_salary_transactions(event):
    entity_id = event.get("entity_id")
    account_id = event.get("account_id")
    to_reject_account = event.get("to_reject_account", False)
    enable_metadata = event.get("enable_metadata", False)

    account = get_account_for_entity(entity_id, account_id, to_reject_account)
    if not account:
        raise Exception("Account data not found")

    salary_transactions = get_salary_transactions_from_ddb(account_id)

    if enable_metadata is True:
        try:
            for transaction in salary_transactions:
                transaction["metadata"] = {
                    "transaction_channel": transaction.get("transaction_channel"),
                    "unclean_merchant": transaction.get("unclean_merchant"),
                    "description": transaction.get("description"),
                }
        except Exception as e:
            capture_exception(e)

    # validate transactions
    salary_transactions = TypeAdapter(List[SubscriptionAPIAccountSalaryTransactionDTO]).validate_python(salary_transactions)
    salary_transactions = [item.model_dump(exclude_unset=True) for item in salary_transactions]

    # transaction sanity cleanup
    salary_transactions = transactions_sanity_checker(salary_transactions)
    return salary_transactions
