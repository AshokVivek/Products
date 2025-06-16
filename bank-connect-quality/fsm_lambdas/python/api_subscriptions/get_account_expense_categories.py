from python.aggregates import (
    get_account_for_entity,
    get_transactions_for_account,
)
from collections import defaultdict


def round_to_100_percent(number_list, digit_after_decimal=2):
    """
    This function take a list of number and return a list of percentage,
    which represents the portion of each number in sum of all numbers
    """
    non_round_numbers = [x / float(sum(number_list)) * 100 * 10**digit_after_decimal for x in number_list]
    decimal_part_with_index = sorted(
        [(index, non_round_numbers[index] % 1) for index in range(len(non_round_numbers))], key=lambda y: y[1], reverse=True
    )
    remainder = 100 * 10**digit_after_decimal - sum([int(x) for x in non_round_numbers])
    index = 0
    while remainder > 0:
        non_round_numbers[decimal_part_with_index[index][0]] += 1
        remainder -= 1
        index = (index + 1) % len(number_list)
    return [int(x) / float(10**digit_after_decimal) for x in non_round_numbers]


def get_account_expense_categories(event):
    entity_id = event.get("entity_id")
    account_id = event.get("account_id")
    to_reject_account = event.get("to_reject_account", False)

    account = get_account_for_entity(entity_id, account_id, to_reject_account)
    if not account:
        raise Exception("Account data not found")

    transactions, _ = get_transactions_for_account(entity_id, account_id)
    categories = []
    category_counts = defaultdict(float)
    for transaction in transactions:
        if transaction.get("transaction_type") == "debit":
            # consider only debit transactions
            merchant_category = transaction.get("merchant_category")
            transaction_channel = transaction.get("transaction_channel", "Others")
            # transform transaction_channel
            if transaction_channel == "investment":
                # club investments
                transaction_channel = "investments"
            elif transaction_channel == "bill_payment":
                # club bills
                transaction_channel = "bills"
            elif transaction_channel == "cash_withdrawl":
                # mark cash
                transaction_channel = "cash"
            elif transaction_channel in ["upi", "net_banking_transfer", "chq"]:
                # mark transfers
                transaction_channel = "transfers"
            else:
                # mark rest as others
                transaction_channel = "Others"
            if merchant_category:
                category_counts[merchant_category] += transaction["amount"]
            else:
                category_counts[transaction_channel] += transaction["amount"]
    category_list = []
    number_list = []
    for category, count in category_counts.items():
        category = category.replace("_", " ").title()
        category_list.append(category)
        number_list.append(count)
    if any(number_list):
        # if at least one debit category exists
        percentage_list = round_to_100_percent(number_list, 0)
        others_obj = None
        for index in range(0, len(category_list)):
            if percentage_list[index] > 0:
                if category_list[index] == "Others":
                    others_obj = {"category": "Others", "percentage": int(percentage_list[index])}
                else:
                    categories.append({"category": category_list[index], "percentage": int(percentage_list[index])})
        categories.sort(key=lambda item: item["percentage"], reverse=True)
        # append others in end if present
        if others_obj:
            categories.append(others_obj)

    return categories
