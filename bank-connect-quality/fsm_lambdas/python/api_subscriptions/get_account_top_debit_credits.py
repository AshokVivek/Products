from typing import List

from library.excel_report.constants import DEFAULT_REQUIRED_TRANSACTIONS_COUNT
from python.aggregates import (
    get_account_for_entity,
    get_transactions_for_account,
)
from python.dtos.subscriptions.account_top_debit_credits_dto import SubscriptionAPIAccountTopDebitCreditDTO
from python.dtos.subscriptions.account_transaction_dto import SubscriptionAPIAccountTransactionDTO
from python.handlers import transactions_sanity_checker
from library.excel_report.metrics_helper import top_debit_credit_corrected
from pydantic import TypeAdapter
from sentry_sdk import capture_exception


def get_account_top_debit_credits(event):
    entity_id = event.get("entity_id")
    account_id = event.get("account_id")
    to_reject_account = event.get("to_reject_account", False)
    req_transactions_count = event.get("req_transactions_count", DEFAULT_REQUIRED_TRANSACTIONS_COUNT)
    enable_metadata = event.get("enable_metadata", False)

    account = get_account_for_entity(entity_id, account_id, to_reject_account)
    if not account:
        raise Exception("Account data not found")

    transactions, _ = get_transactions_for_account(entity_id, account_id)

    if enable_metadata is True:
        try:
            for transaction in transactions:
                transaction["metadata"] = {
                    "transaction_channel": transaction.get("transaction_channel"),
                    "unclean_merchant": transaction.get("unclean_merchant"),
                    "description": transaction.get("description"),
                }
        except Exception as e:
            capture_exception(e)

    # validate transactions
    transactions = TypeAdapter(List[SubscriptionAPIAccountTransactionDTO]).validate_python(transactions)
    transactions = [item.model_dump(exclude_unset=True) for item in transactions]

    transactions = transactions_sanity_checker(transactions)
    top_credits_debits = top_debit_credit_corrected(transactions, req_transactions_count)

    # validate top debit credits
    top_credits_debits = TypeAdapter(List[SubscriptionAPIAccountTopDebitCreditDTO]).validate_python(top_credits_debits)
    top_credits_debits = [item.model_dump(exclude_unset=True) for item in top_credits_debits]

    return top_credits_debits
