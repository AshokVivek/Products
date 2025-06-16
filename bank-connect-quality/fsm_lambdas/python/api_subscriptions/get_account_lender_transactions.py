from typing import List

from python.aggregates import (
    get_account_for_entity,
    get_transactions_for_account,
)
from python.dtos.subscriptions.account_lender_transactions import SubscriptionAPIAccountLenderTransactionDTO
from python.handlers import transactions_sanity_checker
from pydantic import TypeAdapter


def get_account_lender_transactions(event):
    entity_id = event.get("entity_id")
    account_id = event.get("account_id")
    to_reject_account = event.get("to_reject_account", False)

    account = get_account_for_entity(entity_id, account_id, to_reject_account)
    if not account:
        raise Exception("Account data not found")

    transactions, _ = get_transactions_for_account(entity_id, account_id)

    # validate transactions
    transactions = TypeAdapter(List[SubscriptionAPIAccountLenderTransactionDTO]).validate_python(transactions)
    transactions = [item.model_dump(exclude_unset=True) for item in transactions]

    # transaction sanity cleanup
    transactions = transactions_sanity_checker(transactions)
    
    transactions = [txn for txn in transactions if (txn['description'] == 'lender_transaction' and txn['transaction_channel'] != 'salary')]

    return transactions
