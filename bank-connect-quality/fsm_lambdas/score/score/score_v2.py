"""
This script calculates BankConnect Score version 2.
"""
# Ignoring Pylint warnings.
#pylint: disable=import-error, disable=implicit-str-concat, disable=line-too-long

#importing libraries and dependency
import warnings
import pandas as pd
from datetime import datetime
from dateutil.relativedelta import relativedelta
from score.BankConnect_score_v2_scale import score_function


warnings.simplefilter(action = "ignore", category = FutureWarning)
pd.options.mode.chained_assignment = None


def median(list_val):
    '''
    This function is used to calculate median of the list
    Parameters:
        list_val(list): A list containing numbers
    Returns:
        result(float): A float number representing median
    '''
    list_val.sort()
    mid = len(list_val) // 2
    result = (list_val[mid] + list_val[~mid]) / 2
    return result

def mean(list_val):
    '''
    This function is used to calculate mean of the list
    Parameters:
        list_val(list): A list containing numbers
    Returns:
        result(float): A float number representing mean
    '''
    res = sum(list_val) / len(list_val)
    return res

def return_none_if_zero(val):
    '''
    This function is used to return None value if zero is present
    '''
    final_val = None if val == 0 else val
    return final_val

def get_predictors_for_scoring(txns):
    '''
    This function is used to calculate all the predictors which will
    be used in the calculation of BankConnect Score Version 2.
    Parameters:
        txns(list of dict): A list of dictionaries containing transaction deatils in sorted order
    Returns:
        final_features(dict): A dictionary containing all the features
    '''
    # declaring variables
    cnt_credit_txns = 0
    cnt_debit_txns = 0
    cnt_lender_debits = 0
    cnt_nonlender_debits = 0

    amt_bank_charge_txns_c30 = 0

    balance_list_c60 = []
    sum_amount_nonlender_debits_c60 = 0
    amt_upi_txns_c60 = 0
    cnt_refund_txns_c60 = 0

    cnt_txns_balance_less_than_100_c90 = 0
    cnt_txns_balance_more_than_5000_c90 = 0
    balance_median_c90 = []
    credit_max_c90 = []
    sum_amt_lender_debits_c90 = 0
    sum_amt_nonlender_debits_c90 = 0
    cnt_net_banking_transfer_txns_c90 = 0

    balance_mean_c180 = []
    debit_max_c180 = []
    amt_net_banking_transfer_txns_c180 = 0
    amt_bank_interest_txns_c180 = 0
    amt_salary_txns_c180 = 0

    balance_list_m0 = []
    credit_list_m0 = []
    debit_list_m0 = []
    credit_list_m0 = []
    cnt_txns_balance_more_than_5000_m0 = 0
    amt_bank_interest_txns_m0 = 0
    cnt_debit_card_txns_m0 = 0
    sum_luxury_spends_amt_m0 = 0

    debit_list_m1 = []
    cnt_upi_txns_m1 = 0
    balance_list_m2 = []
    amt_refund_txns_m2 = 0
    amt_bills_txns_m2 = 0

    credit_list_m3 = []
    sum_amount_lender_credits_m3 = 0
    sum_amount_lender_debits_m3 = 0
    debit_list_m3 = []
    amt_basicneed_debit_txns_m3 = 0

    balance_list_m4 = []
    debit_list_m4 = []
    cnt_ewallet_txns_m4 = 0
    amt_fuel_txns_m4 = 0

    balance_list_m5 = []

    final_features = {}

    # get the latest date of transaction
    final_date = txns[-1]['date']
    final_date_datetime = datetime.strptime(final_date, '%Y-%m-%d %H:%M:%S')

    #calculating _c dates
    c30_date = datetime.strftime(final_date_datetime - relativedelta(days = 30), '%Y-%m-%d %H:%M:%S')
    c60_date = datetime.strftime(final_date_datetime - relativedelta(days = 60), '%Y-%m-%d %H:%M:%S')
    c90_date = datetime.strftime(final_date_datetime - relativedelta(days = 90), '%Y-%m-%d %H:%M:%S')
    c180_date = datetime.strftime(final_date_datetime - relativedelta(days = 180), '%Y-%m-%d %H:%M:%S')

    #calculating _m dates
    min_m0_date = datetime.strftime(final_date_datetime - relativedelta(day = 1), '%Y-%m-%d %H:%M:%S')
    min_m1_date = datetime.strftime(final_date_datetime - relativedelta(months = 1, day = 1), '%Y-%m-%d %H:%M:%S')
    min_m2_date = datetime.strftime(final_date_datetime - relativedelta(months = 2, day = 1), '%Y-%m-%d %H:%M:%S')
    min_m3_date = datetime.strftime(final_date_datetime - relativedelta(months = 3, day = 1), '%Y-%m-%d %H:%M:%S')
    min_m4_date = datetime.strftime(final_date_datetime - relativedelta(months = 4, day = 1), '%Y-%m-%d %H:%M:%S')
    min_m5_date = datetime.strftime(final_date_datetime - relativedelta(months = 5, day = 1), '%Y-%m-%d %H:%M:%S')

    # calculating Features
    for item in reversed(txns):
        # calculating all features
        if item['transaction_type'] == 'credit':
            cnt_credit_txns += 1
        if item['transaction_type'] == 'debit':
            cnt_debit_txns += 1
        if item['merchant_category'] == 'loans' and item['transaction_type'] == 'debit':
            cnt_lender_debits += 1
        if item['merchant_category'] != 'loans' and item['transaction_type'] == 'debit':
            cnt_nonlender_debits += 1

        # calculating c30 features
        if item['date'] <= final_date and item['date'] > c30_date:
            if item['transaction_channel'] == 'bank_charge':
                amt_bank_charge_txns_c30 += item['amount']

        # calculating c60 features
        if item['date'] <= final_date and item['date'] > c60_date:
            balance_list_c60.append(item['balance'])
            if item['merchant_category'] != 'loans' and item['transaction_type'] == 'debit':
                sum_amount_nonlender_debits_c60 += item['amount']
            if item['transaction_channel'] == 'upi':
                amt_upi_txns_c60 += item['amount']
            if item['transaction_channel'] == 'refund':
                cnt_refund_txns_c60 += 1

        # calculating c90 features
        if item['date'] <= final_date and item['date'] > c90_date:
            balance_median_c90.append(item['balance'])
            if item['balance'] < 100:
                cnt_txns_balance_less_than_100_c90 += 1
            if item['balance'] > 5000:
                cnt_txns_balance_more_than_5000_c90 += 1
            if item['transaction_type'] == 'credit':
                credit_max_c90.append(item['amount'])
            if item['merchant_category'] == 'loans' and item['transaction_type'] == 'debit':
                sum_amt_lender_debits_c90 += item['amount']
            if item['merchant_category'] != 'loans' and item['transaction_type'] == 'debit':
                sum_amt_nonlender_debits_c90 += item['amount']
            if item['transaction_channel'] == 'net_banking_transfer':
                cnt_net_banking_transfer_txns_c90 +=1

        # calculating c180 features
        if item['date'] <= final_date and item['date'] > c180_date:
            balance_mean_c180.append(item['balance'])
            if item['transaction_type'] == 'debit':
                debit_max_c180.append(item['amount'])
            if item['transaction_channel'] == 'net_banking_transfer':
                amt_net_banking_transfer_txns_c180 += item['amount']
            if item['transaction_channel'] == 'bank_interest':
                amt_bank_interest_txns_c180 += item['amount']
            if item['transaction_channel'] == 'salary':
                amt_salary_txns_c180 += item['amount']

        # calculating m0 features
        if item['date'] <= final_date and item['date'] >= min_m0_date:
            balance_list_m0.append(item['balance'])
            if item['transaction_type'] == 'debit':
                debit_list_m0.append(item['amount'])
            if item['transaction_type'] == 'credit':
                credit_list_m0.append(item['amount'])
            if item['balance'] > 5000:
                cnt_txns_balance_more_than_5000_m0 += 1
            if item['transaction_channel'] == 'bank_interest':
                amt_bank_interest_txns_m0 += item['amount'] 
            if item['transaction_channel'] == 'debit_card':
                cnt_debit_card_txns_m0 += 1
            if item['merchant_category'] in ['food', 'entertainment', 'shopping', 'travel'] and item['transaction_type'] == 'debit':
                sum_luxury_spends_amt_m0 = item['amount']

        # calculating m1 features
        if item['date'] < min_m0_date and item['date'] >= min_m1_date:
            if item['transaction_type'] == 'debit':
                debit_list_m1.append(item['amount'])
            if item['transaction_channel'] == 'upi':
                cnt_upi_txns_m1 += 1

        # calculating m2 features
        if item['date'] < min_m1_date and item['date'] >= min_m2_date:
            balance_list_m2.append(item['balance'])
            if item['transaction_channel'] == 'refund':
                amt_refund_txns_m2 += item['amount']
            if item['merchant_category'] == 'bills':
                amt_bills_txns_m2 += item['amount']

        # calculating m3 features
        if item['date'] < min_m2_date and item['date'] >= min_m3_date:
            if item['transaction_type'] == 'credit':
                credit_list_m3.append(item['amount'])
            if item['transaction_type'] == 'debit':
                debit_list_m3.append(item['amount'])
            if item['merchant_category'] == 'loans' and item['transaction_type'] == 'debit':
                sum_amount_lender_debits_m3 += item['amount']
            if item['merchant_category'] == 'loans' and item['transaction_type'] == 'credit':
                sum_amount_lender_credits_m3 += item['amount']        
            if item['merchant_category'] in ['groceries', 'fuel'] and item['transaction_type'] == 'debit':
                amt_basicneed_debit_txns_m3 += item['amount']

        # calculating m4 features
        if item['date'] < min_m3_date and item['date'] >= min_m4_date:
            balance_list_m4.append(item['balance'])
            if item['transaction_type'] == 'debit':
                debit_list_m4.append(item['amount'])
            if item['transaction_type'] == 'debit' and item['merchant_category']=='ewallet':
                cnt_ewallet_txns_m4 += 1
            if item['transaction_type'] == 'debit' and item['merchant_category']=='fuel':
                amt_fuel_txns_m4 += item['amount']

        # calculating m5 features
        if item['date'] < min_m4_date and item['date'] >= min_m5_date:
            balance_list_m5.append(item['balance'])

    # calculating aggregated features
    ratio_cnt_nonlender_to_lender_debits = None if cnt_lender_debits == 0 else cnt_nonlender_debits/cnt_lender_debits 
    credit_to_debit_txns = None if cnt_debit_txns == 0 else cnt_credit_txns/cnt_debit_txns   
    ratio_amt_nonlender_to_lender_debits_c90 = None if sum_amt_lender_debits_c90 == 0 else sum_amt_nonlender_debits_c90/sum_amt_lender_debits_c90
    perc_luxury_spends_amt_m0 = None if sum(debit_list_m0) == 0 else (sum_luxury_spends_amt_m0/sum(debit_list_m0))*100
    ratio_lender_credits_to_debits_m3 = None if sum_amount_lender_debits_m3 == 0 else sum_amount_lender_credits_m3/sum_amount_lender_debits_m3
    perc_basicneed_spends_amt_m3 = None if sum(debit_list_m3) == 0 else (amt_basicneed_debit_txns_m3/sum(debit_list_m3))*100

    # adding value into dict. and treating zero with None
    final_features["cnt_credit_txns"] = return_none_if_zero(cnt_credit_txns)
    final_features["credit_to_debit_txns"] = return_none_if_zero(credit_to_debit_txns)
    final_features["ratio_cnt_nonlender_to_lender_debits"] = ratio_cnt_nonlender_to_lender_debits
    # c30 features
    final_features["amt_bank_charge_txns_c30"] = return_none_if_zero(amt_bank_charge_txns_c30)
    # c60 features
    final_features["balance_min_c60"] = None if len(balance_list_c60) == 0 else min(balance_list_c60)
    final_features["amt_upi_txns_c60"] = return_none_if_zero(amt_upi_txns_c60)
    final_features["cnt_refund_txns_c60"] = return_none_if_zero(cnt_refund_txns_c60)
    final_features["sum_amount_nonlender_debits_c60"] = return_none_if_zero(sum_amount_nonlender_debits_c60)
    # c90 features
    final_features["cnt_txns_balance_less_than_100_c90"] = return_none_if_zero(cnt_txns_balance_less_than_100_c90)
    final_features["cnt_txns_balance_more_than_5000_c90"] = return_none_if_zero(cnt_txns_balance_more_than_5000_c90)
    final_features["balance_median_c90"] = None if len(balance_median_c90) == 0 else median(balance_median_c90)
    final_features["credit_max_c90"] = None if len(credit_max_c90) == 0 else max(credit_max_c90)
    final_features["ratio_amt_nonlender_to_lender_debits_c90"] = return_none_if_zero(ratio_amt_nonlender_to_lender_debits_c90)
    final_features["cnt_net_banking_transfer_txns_c90"] = return_none_if_zero(cnt_net_banking_transfer_txns_c90)
    # c180 features
    final_features["balance_mean_c180"] = None if len(balance_mean_c180) == 0 else mean(balance_mean_c180)
    final_features["debit_max_c180"] = None if len(debit_max_c180) == 0 else max(debit_max_c180)
    final_features["amt_net_banking_transfer_txns_c180"] = return_none_if_zero(amt_net_banking_transfer_txns_c180)
    final_features["amt_bank_interest_txns_c180"] = return_none_if_zero(amt_bank_interest_txns_c180)
    final_features["amt_salary_txns_c180"] = return_none_if_zero(amt_salary_txns_c180)
    # m0 features
    final_features["balance_median_m0"] = None if len(balance_list_m0) == 0 else median(balance_list_m0)
    final_features["balance_min_m0"] = None if len(balance_list_m0) == 0 else min(balance_list_m0)
    final_features["balance_max_m0"] = None if len(balance_list_m0) == 0 else max(balance_list_m0)
    final_features["credit_mean_m0"] = None if len(credit_list_m0) == 0 else mean(credit_list_m0)
    final_features["credit_max_m0"] = None if len(credit_list_m0) == 0 else max(credit_list_m0)
    final_features["debit_mean_m0"] = None if len(debit_list_m0) == 0 else mean(debit_list_m0)
    final_features["cnt_txns_balance_more_than_5000_m0"] = return_none_if_zero(cnt_txns_balance_more_than_5000_m0)
    final_features["cnt_debit_card_txns_m0"] = return_none_if_zero(cnt_debit_card_txns_m0)
    final_features["amt_bank_interest_txns_m0"] = return_none_if_zero(amt_bank_interest_txns_m0)
    final_features["perc_luxury_spends_amt_m0"] = return_none_if_zero(perc_luxury_spends_amt_m0)
    # m1 features
    final_features["debit_max_m1"] = None if len(debit_list_m1) == 0 else max(debit_list_m1)
    final_features["cnt_upi_txns_m1"] = return_none_if_zero(cnt_upi_txns_m1)
    # m2 features
    final_features["balance_min_m2"] = None if len(balance_list_m2) == 0 else min(balance_list_m2)
    final_features["amt_refund_txns_m2"] = return_none_if_zero(amt_refund_txns_m2)
    final_features["amt_bills_txns_m2"] = return_none_if_zero(amt_bills_txns_m2)
    # m3 features
    final_features["credit_max_m3"] = None if len(credit_list_m3) == 0 else max(credit_list_m3)
    final_features["ratio_lender_credits_to_debits_m3"] = return_none_if_zero(ratio_lender_credits_to_debits_m3)
    final_features["perc_basicneed_spends_amt_m3"] = return_none_if_zero(perc_basicneed_spends_amt_m3)
    # m4 features
    final_features["balance_mean_m4"] = None if len(balance_list_m4) == 0 else mean(balance_list_m4)
    final_features["balance_max_m4"] = None if len(balance_list_m4) == 0 else max(balance_list_m4)
    final_features["debit_max_m4"] = None if len(debit_list_m4) == 0 else max(debit_list_m4)
    final_features["cnt_ewallet_txns_m4"] = return_none_if_zero(cnt_ewallet_txns_m4)
    final_features["amt_fuel_txns_m4"] = return_none_if_zero(amt_fuel_txns_m4)
    # m5 features
    final_features["balance_median_m5"] = None if len(balance_list_m5) == 0 else median(balance_list_m5)

    return final_features

def score_helper_v2(payload: list) -> list:
    '''
    This function is the main calling function to calculate score
    Parameter:
        payload(list): The payload contains a list of all the transactions belonging to the account_id
    Returns:
        res_obj(dict): A dictionary containing account_id, score
    '''
    res_obj = []
    try:
        for account_id in payload.get('account_wise_transactions'):
            transactions = payload.get('account_wise_transactions').get(account_id,[])
            features = get_predictors_for_scoring(transactions)
            columns = list(features.keys())
            # creating a blank df using predictor columns
            df = pd.DataFrame(columns=columns)
            df.loc[len(df)] = features
            score = score_function(df)
            print("Score for {} is {}".format(account_id, score))
            res_obj.append({
                "account_id": account_id,
                "score": score,
                "score_version": "uis2.0",
                "params": features
            })
        return {
            "status": 200,
            "scores": res_obj
        }

    except Exception as exceptions:
        print("Some Exception occured ", exceptions)
        return {
            "status": 500,
            "scores": None,
            "message": "some error occured"
        }
