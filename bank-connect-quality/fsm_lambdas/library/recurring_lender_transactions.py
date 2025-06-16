import re
import numpy as np
import warnings
import pandas as pd
from rapidfuzz import fuzz as rfuzz
from library.recurring_transaction import clean_transaction_note


warnings.simplefilter(action = "ignore", category = FutureWarning)
pd.options.mode.chained_assignment = None


def get_day_allowed_list(transaction_date):
    day_1 = transaction_date.day
    if day_1 > 3 & day_1 <= 28:
        allowed_days = list(range(day_1 - 2, day_1 + 4))
    elif day_1 == 29:
        allowed_days = [26, 27, 28, 29, 30, 31, 1]
    elif day_1 == 30:
        allowed_days = [27, 28, 29, 30, 31, 1, 2]
    elif day_1 == 31:
        allowed_days = [28, 29, 30, 31, 1, 2, 3]
    elif day_1 == 1:
        allowed_days = [29, 30, 31, 1, 2, 3, 4]
    elif day_1 == 2:
        allowed_days = [30, 31, 1, 2, 3, 4, 5]
    elif day_1 == 3:
        allowed_days = [31, 1, 2, 3, 4, 5, 6]

    return allowed_days


## Recurring logic
def get_recurring_lenders_transaction_hash(df):
    transactions_list = df.to_dict("records")

    already_matched_hash = set()
    lender_hash = set()

    for each_transaction in transactions_list:
        if each_transaction["hash"] not in lender_hash:
            date1 = each_transaction["date_obj"].date()

            allowed_days = get_day_allowed_list(date1)

            already_matched_hash.add(each_transaction["hash"])
            amount1 = each_transaction["amount"]
            hash1 = each_transaction["hash"]

            for each_transaction2 in transactions_list:
                amount2 = each_transaction2["amount"]
                hash2 = each_transaction2["hash"]
                if (
                    (hash2 not in already_matched_hash)
                    and (hash2 not in lender_hash)
                    and (amount1 == amount2)
                ):
                    transaction_channel1 = each_transaction["transaction_channel"]
                    transaction_channel2 = each_transaction2["transaction_channel"]

                    txn_note1 = each_transaction["transaction_note"].lower()
                    txn_note2 = each_transaction2["transaction_note"].lower()

                    date2 = each_transaction2["date_obj"].date()

                    condition1 = transaction_channel1 == transaction_channel2

                    condition2 = abs(
                        len(each_transaction["clean_transaction_note"])
                        - len(each_transaction2["clean_transaction_note"])
                        < 10
                    )

                    txn_note_match_ratio = rfuzz.WRatio(
                        each_transaction["clean_transaction_note"],
                        each_transaction2["clean_transaction_note"],
                    )

                    condition3 = txn_note_match_ratio > 92

                    condition4 = date1 != date2

                    condition5 = date2.day in allowed_days

                    condition6 = abs(len(txn_note1) - len(txn_note2)) < 3
                    condition7 = len(txn_note1) > 10
                    condition8 = len(txn_note2) > 10

                    if (condition1 and condition2 and condition4 and condition5) and (
                        condition3 or (condition6 and condition7 and condition8)
                    ):
                        lender_hash.update([hash1, hash2])

    return lender_hash


def get_recurring_lender_debit_transactions(df):
    recurring_lenders_txns_hash = set()

    if df.empty:
        return list(recurring_lenders_txns_hash)

    df["date_obj"] = pd.to_datetime(df["date"], format="%Y-%m-%d %H:%M:%S")

    not_allowed_description = ["insurance", "investments", "lender_transaction", "trading/investments", "mutual_funds", "donation"]
    allowed_transaction_type = ["debit"]
    allowed_channel = ["auto_debit_payment", "net_banking_transfer", "Other", "upi"]
    allowed_patterns = [
        ".*[^a-z](nach|ecs|ach|achdr)[^a-z]+.*",
        "^(ach|nach|ecs|achdr)[^a-z].*",
        "^Ins\\s*Debit.*(CSG|CA)\\s*[0-9]{3,}\\s*dt.*",
        "^ACHDR(\\/)?.*\\/[0-9]{4,}\\/[0-9]{4,}.*",
        "^(Loan)?\\s*Reco.*For\\s*[0-9]{3,}\\s*",
        "^INTERCITY\\s*ECS\\s*PAID\\s*NACH\\s*:\\s*Paid\\s*to.*",
        "^DEBIT-CMP\\s*MANDATE\\s*DEBIT.*",
        "^MAND\\s*DR\\s*-\\s*[a-zA-Z0-9]{4,}-[a-zA-Z0-9]{4,}$",
        "^CMS\\/[a-zA-Z0-9]{4,}\\/[0-9]{4,}$",
        ".*drawdown\\s*from\\s*casa.*"
    ]
    finall_pattern = ""
    for p in allowed_patterns:
        finall_pattern += p + "|"
    finall_pattern = finall_pattern[:-1]
    allowed_amount_threshold = 400

    allowed_df = df[
        (~df["description"].isin(not_allowed_description))
        & (df["transaction_type"].isin(allowed_transaction_type))
        & (df["transaction_channel"].isin(allowed_channel))
        & (df["amount"] > allowed_amount_threshold)
        & (df["transaction_note"].str.contains(finall_pattern, flags=re.IGNORECASE))
    ]

    if not allowed_df.empty:
        allowed_df.reset_index(drop=True, inplace=True)
        allowed_df["clean_transaction_note"] = allowed_df.apply(
            lambda x: clean_transaction_note(x, "lender"), axis=1
        )
        recurring_lenders_txns_hash = get_recurring_lenders_transaction_hash(allowed_df)

    # df['description'] = np.where(df['hash'].isin(recurring_lenders_txns_hash), 'lender_recurring', df['description'])
    return list(recurring_lenders_txns_hash)
