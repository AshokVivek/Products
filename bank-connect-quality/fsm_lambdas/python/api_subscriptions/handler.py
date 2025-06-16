from sentry_sdk import capture_exception
from sentry_sdk import set_context, set_tag
from python.api_subscriptions.get_account_expense_categories import get_account_expense_categories
from python.api_subscriptions.get_account_lender_transactions import get_account_lender_transactions
from python.api_subscriptions.get_account_recurring_transactions import get_account_recurring_transactions
from python.api_subscriptions.get_account_details import get_account_details
from python.api_subscriptions.get_account_fraud import get_account_fraud
from python.api_subscriptions.get_account_salary_transactions import get_account_salary_transactions
from python.api_subscriptions.get_account_top_debit_credits import get_account_top_debit_credits
from python.api_subscriptions.get_account_transactions import get_account_transactions
from python.api_subscriptions.get_account_statement_stats import get_account_statement_stats


def get_subscriptions_handler(event, context):
    set_tag("entity_id", event.get("entity_id", None))
    set_context("subscriptions_handler_event_payload", event)

    subscription_type = event.get("subscription_type")

    if subscription_type == "ACCOUNT_DETAILS":
        return get_account_details(event)

    elif subscription_type == "ACCOUNT_FRAUD":
        return get_account_fraud(event)

    elif subscription_type == "ACCOUNT_TRANSACTIONS":
        return get_account_transactions(event)

    elif subscription_type == "ACCOUNT_SALARY_TRANSACTIONS":
        return get_account_salary_transactions(event)

    elif subscription_type == "ACCOUNT_RECURRING_TRANSACTIONS":
        return get_account_recurring_transactions(event)

    elif subscription_type == "ACCOUNT_LENDER_TRANSACTIONS":
        return get_account_lender_transactions(event)

    elif subscription_type == "ACCOUNT_TOP_CREDITS_DEBITS":
        return get_account_top_debit_credits(event)

    elif subscription_type == "ACCOUNT_STATEMENT_STATS":
        return get_account_statement_stats(event)

    elif subscription_type == "ACCOUNT_EXPENSE_CATEGORIES":
        return get_account_expense_categories(event)

    capture_exception(Exception("Invalid subscription_type"))
    return None
