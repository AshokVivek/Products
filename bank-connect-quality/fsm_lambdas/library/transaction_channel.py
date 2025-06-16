import re
import warnings

import pandas as pd
from fuzzywuzzy import fuzz
from rapidfuzz import fuzz as rfuzz

from library.enrichment_regexes import (get_lender_list,
                                        get_transaction_channel_lists,
                                        get_unclean_merchant)
from library.helpers.constants import (
    BANK_CHARGE_AMOUNTS, BOUNCE_TRANSACTION_CHANNELS,
    MIN_SALARY_AMOUNT_FOR_KEYWORD_CLASSIFICATION, TRANSACTION_CHANNELS)
from library.merchant_category import get_merchant_category_dict
from library.utils import (CREDIT_ONLY_MERCHANT_CATEGORY,
                           DEBIT_ONLY_MERCHANT_CATEGORY,
                           DEBIT_ONLY_MERCHANT_CATEGORY_REGEX,
                           NON_LENDER_MERCHANT_KEYWORDS, match_compiled_regex,
                           remove_re_compile, remove_unicode)

warnings.simplefilter(action = "ignore", category = FutureWarning)
pd.options.mode.chained_assignment = None


SALARY_KEYWORDS_TO_IGNORE_CREDIT = ["REVERSAL", "EARLYSAL", "ERLYSAL", "FLEXSAL", "SALADV", "ADVSAL", "SALARYADVANCE", "ADVANCESALARY", "SALARYADV", "SALARYNOW", "SALARY.NOW", "ONDEMANDSALARY"]
SALARY_KEYWORDS_TO_IGNORE_DEBIT = ["REVERSAL", "EARLYSAL", "ERLYSAL", "FLEXSAL", "ONDEMANDSALARY", "SALADV", "ADVSAL", "SALARYADVANCE", "ADVANCESALARY", "SALARYADV", "SALARYNOW", "SALARY.NOW"]


def get_df(path):
    df = pd.read_csv(path)
    return df

def get_transaction_channel(df, bank, country="IN", account_category='', categorise_merchant_category=True):
    if country in [None, ""]:
        # default country is IN
        country = "IN"

    channel_data = get_transaction_channel_lists(bank, country)
    debit_channel_dict = channel_data.get("debit_channel_dict", {})
    debit_priority_order = channel_data.get("debit_priority_order", [])
    credit_channel_dict = channel_data.get("credit_channel_dict", {})
    credit_priority_order = channel_data.get("credit_priority_order", [])

    unclean_merchant_data = get_unclean_merchant(bank, country)
    merchant_debit_regex_list = unclean_merchant_data.get('merchant_debit_regex_list', [])
    merchant_credit_regex_list = unclean_merchant_data.get('merchant_credit_regex_list', [])

    merchant_category_dict = get_merchant_category_dict(country)

    merchant_categories = merchant_category_dict.keys()
    tag_merchant_categories = [_ for _ in merchant_categories if "_regex" not in _]
    regex_merchant_categories = [_ for _ in merchant_categories if "_regex" in _]

    if df.shape[0] > 0:
        if 'transaction_channel' not in df.columns:
            df['transaction_channel'] = ''
        if 'transaction_channel_regex' not in df.columns:
            df['transaction_channel_regex'] = ''

        for index, row in df.iterrows():
            result = transactionchannel(row, debit_channel_dict, debit_priority_order, credit_channel_dict, credit_priority_order, country, account_category, bank)
            df['transaction_channel'][index] = result[0]
            df['transaction_channel_regex'][index] = result[1]
        
        if categorise_merchant_category:
            df['unclean_merchant'] = df.apply(lambda x: get_merchant(x, merchant_debit_regex_list, merchant_credit_regex_list, bank, country), axis=1)
            df = df.apply(lambda x: get_merchant_category(x, tag_merchant_categories, regex_merchant_categories, merchant_category_dict,  False), axis=1)

    else:
        df['transaction_channel'], df['transaction_channel_regex'] = '', ''
        df['unclean_merchant'] = ''
    return df

def string_found(string1, string2):
    string1 = re.sub(r'[^\w]', ' ', string1).strip()
    if re.search(r"\b" + re.escape(string1) + r"\b", string2):
        return True
    return False

def transactionchannel(x, debit_channel_dict, debit_priority_order, credit_channel_dict, credit_priority_order, country='IN', account_category='', bank=''):
    if x['transaction_type'] == 'debit':
        transaction_note = remove_unicode(x['transaction_note'])
        if isinstance(transaction_note, str):
            transaction_note = str(transaction_note).upper().strip()
            for channel in debit_priority_order:
                channel_key_regexes = debit_channel_dict[channel]

                if channel != 'salary_paid' or x['amount'] < 5000:
                    for single_regex in channel_key_regexes:
                        if str(single_regex) == "re.compile('.*(ACH).*')":
                            if string_found(".*(ACH).*", transaction_note):
                                match = match_compiled_regex(transaction_note, single_regex, 1)
                            else:
                                continue
                        else:
                            match = match_compiled_regex(transaction_note, single_regex, 1)
                        if match is not None:
                            # print(match)
                            if (channel == 'bank_charge') and (x['amount'] > 50000) and country not in ['ID']: # Indonesian currency is higly inflated.
                                continue
                            if (channel == 'cc_interest') and account_category != 'overdraft':
                                continue
                            if channel in ["cash_withdrawl", "net_banking_transfer"]  and x['amount'] in BANK_CHARGE_AMOUNTS:
                                channel = "bank_charge"
                            return channel, remove_re_compile(single_regex)
                elif account_category!='individual':
                    salary_paid_classification, salary_regex = salary_transaction_note_classification(transaction_note, x['transaction_type'])
                    if salary_paid_classification == "salary":
                        return channel, remove_re_compile(salary_regex)
                    for single_regex in channel_key_regexes:
                        match = match_compiled_regex(transaction_note, single_regex, 1)

                        if match is not None:
                            if not any([keyword in transaction_note.replace(' ', '') for keyword in SALARY_KEYWORDS_TO_IGNORE_DEBIT]):
                                return channel, ''
        return 'Other', ''
    elif x['transaction_type'] == 'credit':
        transaction_note = remove_unicode(x['transaction_note'])
        # Do not classify channel to salary incase of reversal, early, advance salary
        if isinstance(transaction_note, str):
            transaction_note = str(transaction_note).upper().strip()
            #transaction_note = re.sub("[^A-Z]","",transaction_note)
            for channel in credit_priority_order:
                #print("Checking for channel :",channel)
                if channel!='salary':
                    channel_key_regexes = credit_channel_dict[channel]
                    for single_regex in channel_key_regexes:
                        match = match_compiled_regex(transaction_note, single_regex, 1)
                        if match is not None:
                            # print(match)
                            return channel, remove_re_compile(single_regex)
                else:
                    # Checking if the transaction is UPI
                    # upi_transaction = False
                    # upi_patterns = [
                    #     re.compile('(?i)(.*[^A-Za-z]+UPI[^A-Za-z]+.*)'),
                    #     re.compile('(?i)(^UPI[^A-Za-z]+.*)')
                    # ]
                    # for upi_patt in upi_patterns:
                    #     upi_patt_list = upi_patt.findall(transaction_note)
                    #     if len(upi_patt_list)>0:
                    #         upi_transaction = True
                    #         break
                    
                    # if not upi_transaction:
                    salary_classified, salary_regex = salary_transaction_note_classification(transaction_note, x['transaction_type'])
                    if salary_classified and x['amount'] > MIN_SALARY_AMOUNT_FOR_KEYWORD_CLASSIFICATION:
                        return channel, remove_re_compile(salary_regex)

                    channel_key_regexes = credit_channel_dict[channel]
                    for single_regex in channel_key_regexes:
                        match = match_compiled_regex(transaction_note, single_regex, 1)

                        if match is not None:
                            if not any([keyword in transaction_note.replace(' ', '') for keyword in SALARY_KEYWORDS_TO_IGNORE_CREDIT]) and x['amount'] > MIN_SALARY_AMOUNT_FOR_KEYWORD_CLASSIFICATION:
                                return channel, remove_re_compile(single_regex)
            return 'Other', ''
    else:
        return '', ''


def get_merchant(x, merchant_debit_regex_list, merchant_credit_regex_list, bank_name, country = 'IN'):
    if x['transaction_type'] == 'debit':
        transaction_note = remove_unicode(x['transaction_note'])
        if isinstance(transaction_note, str):
            if not(country == 'ID' and bank_name in ['bcabnk', 'mandiribnk']):
                transaction_note = transaction_note.upper()
            for regex in merchant_debit_regex_list:
                try:
                    match = match_compiled_regex(transaction_note, regex, 1)
                    if match is not None:
                        return match
                except Exception as e:
                    print(e)
    elif x['transaction_type'] == 'credit':
        transaction_note = remove_unicode(x['transaction_note'])
        if isinstance(transaction_note, str):
            if not(country == 'ID' and bank_name in ['bcabnk', 'mandiribnk']):
                transaction_note = transaction_note.upper()
            for regex in merchant_credit_regex_list:
                try:
                    match = match_compiled_regex(transaction_note, regex, 1)
                    if match is not None:
                        return match
                except Exception as e:
                    print(e)
    return ""

def classify_merchant_category_from_regexes(row, regex_merchant_categories, category_dict, return_merchant):
    # print("Trying to categorize merchant category from regexes")
    transaction_note = row["transaction_note"]
    for cat in regex_merchant_categories:
        if cat in DEBIT_ONLY_MERCHANT_CATEGORY_REGEX and row['transaction_type'] == 'credit':
            continue
        # print("Category -> ", cat)
        for regex in category_dict[cat]:
            try:
                regex = re.compile(regex, flags=re.IGNORECASE)
                matched_data = regex.findall(transaction_note)
                if len(matched_data)>0:
                    row["merchant_category"] = cat.replace("_regex", "")
                    # print("found merchant category match from regex for ", transaction_note)
                    if return_merchant:
                        row["merchant"] = matched_data[0]
                    break
            except Exception:
                continue
    return row

def get_merchant_category(row, tag_merchant_categories, regex_merchant_categories, category_dict, return_merchant=False):
    row['merchant_category'] = ''
    if return_merchant:
        row['merchant']=''
    
    transaction_channel = row.get("transaction_channel", "")

    # transaction channel skip categories
    if transaction_channel in [TRANSACTION_CHANNELS.BANK_CHARGE, TRANSACTION_CHANNELS.SALARY, TRANSACTION_CHANNELS.REFUND, TRANSACTION_CHANNELS.REVERSAL, TRANSACTION_CHANNELS.CASH_WITHDRAWL, TRANSACTION_CHANNELS.CASH_DEPOSIT] + BOUNCE_TRANSACTION_CHANNELS:
        return row

    row = classify_merchant_category_from_regexes(row, regex_merchant_categories, category_dict, return_merchant)
    if row["merchant_category"]:
        return row

    transaction_note = remove_unicode(row['transaction_note'])
    
    if isinstance(transaction_note, str):
        transaction_note = transaction_note.replace(' ', '').lower()
        for category in tag_merchant_categories:
            if category in  DEBIT_ONLY_MERCHANT_CATEGORY and row['transaction_type'] == 'credit':
                continue
            if category in  CREDIT_ONLY_MERCHANT_CATEGORY and row['transaction_type'] == 'debit':
                continue

            if category in ["bnpl"]:
                non_bnpl_keyword_present = False
                for word in NON_LENDER_MERCHANT_KEYWORDS:
                    x = fuzz.partial_token_sort_ratio(word, transaction_note)
                    if x == 100 and len(transaction_note) > len(word):
                        non_bnpl_keyword_present = True
                        break
                if non_bnpl_keyword_present:
                    continue
            if category == "bills" and row['transaction_type'] == 'credit':
                # ignore "bills" category if transaction type is credit
                continue
            if row.get('transaction_channel') == TRANSACTION_CHANNELS.CASH_WITHDRAWL:
                continue
            for each_word in category_dict[category]:
                x = rfuzz.partial_token_sort_ratio(each_word, transaction_note)
                if x > 90:
                    if each_word == 'adityabirlafi' and category == 'investments':
                        if check_non_investment_keywords(transaction_note):
                            continue
                    # print("Matching word: ",each_word)
                    row['merchant_category'] = category
                    if return_merchant:
                        row['merchant']=each_word.upper()
                    return row
    if row.get('transaction_channel') == "bill_payment":
        row['merchant_category'] = "bills"
    # "loans" category is set after lender detection
    return row

def check_non_investment_keywords(transaction_note):
    non_investment_keywords = ['NACH', 'ACH', 'ECS', 'AUTOMATIC PAYMENT']
    for each_word in non_investment_keywords:
        if each_word.lower() in transaction_note:
            return True
    return False

def salary_transaction_note_classification(transaction_note, transaction_type):
    keywords_to_ignore = SALARY_KEYWORDS_TO_IGNORE_CREDIT if transaction_type == "credit" else SALARY_KEYWORDS_TO_IGNORE_DEBIT
    # sal_patt=re.compile('(?i)(.*[^A-Za-z]+SAL[^A-Za-z]+.*)')
    # t=sal_patt.findall(transaction_note)
    # if t:
    #     for i in t:
    #         i=re.sub(" ","",i)
    #         if any([keyword in i for keyword in keywords_to_ignore]):
    #             continue
    #         else:
    #             return "salary", sal_patt
    # else:
    i=re.sub(" ","",transaction_note)
    salary_keywords = ["SALARY", "SALARIE"]
    salary_keyword = next((keyword for keyword in salary_keywords if keyword in transaction_note), None)
    if salary_keyword:
        if any([keyword in i for keyword in keywords_to_ignore]):
            return None, ''

        return "salary", salary_keyword

    sal_start_patt = re.compile('(?i)(^SAL[^A-Za-z]+.*)')
    sal_end_patt = re.compile('(?i).*[^a-zA-Z]{1}SAL$')
    t=sal_start_patt.findall(transaction_note)
    t2=sal_end_patt.findall(transaction_note)
    if t:
        return "salary", sal_start_patt
    if t2:
        return "salary", sal_end_patt

    return None, ''

def mark_bounce_on_basis_of_negative_balance(group_name, group_df, group_length, lender_list, transaction_hash_set_debit, transaction_hash_set_credit):
    # lender_present = False
    # for lenders in lender_list:
    #     if group_name[0].lower().startswith(lenders.lower()):
    #         lender_present = True
    #         break

    # if not lender_present:
    #     return transaction_hash_set_debit, transaction_hash_set_credit

    # marking debit transactions as auto_debit_payment and credit transactions as auto_debit_payment_bounce transactions
    dict_txns = group_df.to_dict("records")
    for i in range(0, group_length - 1, 2):
        current_transaction = dict_txns[i]
        next_transaction = dict_txns[i+1]

        if next_transaction["transaction_channel"] in ('auto_debit_payment_bounce', 'inward_cheque_bounce', 'inward_payment_bounce', 'outward_cheque_bounce', TRANSACTION_CHANNELS.REFUND, TRANSACTION_CHANNELS.REVERSAL):
            # the next transaction is already tagged as one of the variety of bounces or refund, no need to mark anymore
            continue

        if current_transaction["transaction_channel"] not in ("Other", "auto_debit_payment"):
            continue

        if (current_transaction["transaction_type"] == "debit") and \
            (next_transaction["transaction_type"] == "credit") and \
            (current_transaction["balance"] < 0):
            transaction_hash_set_debit.add(current_transaction["hash"])
            transaction_hash_set_credit.add(next_transaction["hash"])
        elif (current_transaction["transaction_type"] == "credit") and \
            (next_transaction["transaction_type"] == "debit") and \
            (next_transaction["balance"] < 0):
            transaction_hash_set_debit.add(next_transaction["hash"])
            transaction_hash_set_credit.add(current_transaction["hash"])

    return transaction_hash_set_debit, transaction_hash_set_credit

def mark_refund_on_basis_of_same_balance(df, ignore_hashes=[]):
    refund_hash_set = set()
    if df.empty:
        return list(refund_hash_set)
    dict_txns = df.to_dict("records")
    for i in range(1, len(dict_txns) - 1):
        if (
            (dict_txns[i+1]["balance"]==dict_txns[i-1]["balance"]) and
            (dict_txns[i+1]["date"]==dict_txns[i]["date"]) and
            (dict_txns[i+1]["transaction_note"]==dict_txns[i]["transaction_note"]) and
            (dict_txns[i+1]["transaction_type"]=="credit" and dict_txns[i]["transaction_type"]=="debit") and
            (dict_txns[i+1]["transaction_channel"] not in ["refund"] + BOUNCE_TRANSACTION_CHANNELS) and
            (dict_txns[i+1]["hash"] not in ignore_hashes)
        ):
            refund_hash_set.add(dict_txns[i+1]["hash"])
    return list(refund_hash_set)

def mark_reversal_on_basis_of_neg_balance(df, ignore_hashes=[]):
    refund_hash_set = set()
    if df.empty:
        return list(refund_hash_set)
    dict_txns = df.to_dict("records")
    for i in range(1, len(dict_txns) - 1):
        if (
            (dict_txns[i-1]["balance"] > 0) and
            (dict_txns[i]["balance"] < 0) and
            (dict_txns[i+1]["date"]==dict_txns[i]["date"]) and
            (dict_txns[i+1]["transaction_type"]=="credit" and dict_txns[i]["transaction_type"]=="debit") and
            (dict_txns[i+1]["transaction_channel"] in [TRANSACTION_CHANNELS.REFUND, TRANSACTION_CHANNELS.INWARD_PAYMENT_BOUNCE]) and
            (dict_txns[i+1]["hash"] not in ignore_hashes)
        ):
            refund_hash_set.add(dict_txns[i+1]["hash"])
    return list(refund_hash_set)

def update_bounce_transactions_for_account_transactions(df, country="IN"):
    transaction_hash_set_debit, transaction_hash_set_credit= set(), set()
    if df.empty:
        return list(transaction_hash_set_debit), list(transaction_hash_set_credit)

    lender_list = get_lender_list(country)
    for group_name, group_df in df.groupby(["transaction_note", "amount"]):
        group_length = len(group_df)
        if group_length < 2:
            continue

        transaction_hash_set_debit, transaction_hash_set_credit = mark_bounce_on_basis_of_negative_balance(group_name, group_df, group_length, lender_list, transaction_hash_set_debit, transaction_hash_set_credit)



    return list(transaction_hash_set_debit), list(transaction_hash_set_credit)

def update_transaction_channel_after_all_transactions(df, bank_name, account_category, country="IN"):
    tc_hash_dict = {
        "cc_interest": set()
    }
    if df.empty:
        return list(tc_hash_dict["cc_interest"])

    channel_data = get_transaction_channel_lists(bank_name, country)
    debit_channel_dict = channel_data.get("debit_channel_dict", {})
    credit_channel_dict = channel_data.get("credit_channel_dict", {})
    debit_priority_order = []
    credit_priority_order = []

    if "cc_interest" in debit_channel_dict.keys():
        debit_priority_order.append("cc_interest")

    if "transaction_channel" not in df.columns:
        df["transaction_channel"] = ""
    if "transaction_channel_regex" not in df.columns:
        df["transaction_channel_regex"] = ""

    for index, row in df.iterrows():
        result = transactionchannel(row, debit_channel_dict, debit_priority_order,
                        credit_channel_dict, credit_priority_order, country, account_category, bank_name)
        tc = result[0]
        if tc!=row["transaction_channel"]:
            if tc=="cc_interest":
                tc_hash_dict["cc_interest"].add(row["hash"])

    return list(tc_hash_dict["cc_interest"])
