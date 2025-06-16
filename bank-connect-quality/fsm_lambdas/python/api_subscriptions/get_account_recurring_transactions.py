from typing import List
from pydantic import TypeAdapter
from python.aggregates import get_account_for_entity, get_recurring_transactions_list_from_ddb
from python.dtos.subscriptions.account_recurring_transactions import SubscriptionAPIAccountRecurringTransactionDTO


def get_account_recurring_transactions(event):
    entity_id = event.get("entity_id")
    account_id = event.get("account_id", None)
    to_reject_account = event.get("to_reject_account", None)

    account = get_account_for_entity(entity_id, account_id, to_reject_account)
    if not account:
        raise Exception("Account data not found")

    recurring_debit_transactions_list = list()
    recurring_credit_transactions_list = list()

    debit_transactions, credit_transactions = get_recurring_transactions_list_from_ddb(account_id)
    for i in range(0, len(debit_transactions)):
        # validate transactions
        transactions = TypeAdapter(List[SubscriptionAPIAccountRecurringTransactionDTO]).validate_python(debit_transactions[i]["transactions"])
        transactions = [item.model_dump(exclude_unset=True) for item in transactions]
        debit_transactions[i]["transactions"] = transactions

    for i in range(0, len(credit_transactions)):
        # validate transactions
        transactions = TypeAdapter(List[SubscriptionAPIAccountRecurringTransactionDTO]).validate_python(credit_transactions[i]["transactions"])
        transactions = [item.model_dump(exclude_unset=True) for item in transactions]
        credit_transactions[i]["transactions"] = transactions

    recurring_debit_transactions_list += debit_transactions
    recurring_credit_transactions_list += credit_transactions

    # before returning the lists, also sort them on the basis of median amount in a descending order
    recurring_debit_transactions_list = sorted(recurring_debit_transactions_list, key=lambda x: x["median"], reverse=True)
    recurring_credit_transactions_list = sorted(recurring_credit_transactions_list, key=lambda x: x["median"], reverse=True)

    resultant_recurring_transactions = {
        "debit_transactions": recurring_debit_transactions_list,
        "credit_transactions": recurring_credit_transactions_list,
    }

    return resultant_recurring_transactions
