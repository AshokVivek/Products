from datetime import datetime, timedelta, date
from calendar import monthrange
from collections import Counter
from library.lender_list import check_loan
from library.merchant_category import get_merchant_category_dict
from copy import deepcopy
import re

def EOD_balances_func(last_bal, EOD_balances, monthwise_dict = {}):
    avg_month_bal = {}
    for month in EOD_balances["months_order"]:
        avg_month_bal[month] = 0
        count = 0
        for row in range(1,len(EOD_balances[month])+1):
            count += 1
            if EOD_balances[month][row-1] == -1:
                EOD_balances[month][row-1] = last_bal
                if last_bal < 0 and monthwise_dict != {}:
                    # on this day there was no transaction but balance is negative so 
                    # increase count of negative_balance_days by one
                    monthwise_dict[month]["negative_balance_days"]["count"] += 1
            else:
                last_bal = EOD_balances[month][row-1]
            if monthwise_dict != {}:
                monthwise_dict[month]["avg_bal"] = insert_data(monthwise_dict, month, "avg_bal", {"amount": EOD_balances[month][row-1]},True, True, False)
            avg_month_bal[month] = EOD_balances[month][row-1]
        if count>0:
            avg_month_bal[month] /= count
    if monthwise_dict:
        return EOD_balances, monthwise_dict
    else:
        return EOD_balances, avg_month_bal


def info_type_func():
    return {
        "sum": 0,
        "avg": 0,	
        "max": None,	
        "min": None,	
        "count": 0,	
        "max_date": None,	
        "min_date": None,
        "amt": [],
        "transaction_type": None
    }


def create_dict(monthwise_dict, month):
    monthwise_dict[month] = {
        "dates": info_type_func(),
        "absolute_dates": info_type_func(),
        "opening_balance": info_type_func(),
        "credit": info_type_func(),
        "debit": info_type_func(),
        "closing_balance": info_type_func(),
        "net_cash_inflow": info_type_func(),
        "cash_deposit_credit": info_type_func(),
        "net_banking_transfer_credit": info_type_func(),
        "upi_credit": info_type_func(),
        "salary": info_type_func(),
        "chq_credit": info_type_func(),
        "international_transaction_arbitrage_credit": info_type_func(),
        "investment_cashin_credit": info_type_func(),
        "refund_credit": info_type_func(),
        "bank_interest_credit": info_type_func(),
        "debit_card_debit": info_type_func(),
        "cash_withdrawl_debit": info_type_func(),
        "auto_debit_payment_debit": info_type_func(),
        "bill_payment_debit": info_type_func(),
        "bank_charge_debit": info_type_func(),
        "chq_debit": info_type_func(),
        "auto_debit_payment_bounce_credit": info_type_func(),
        "upi_debit": info_type_func(),
        "net_banking_transfer_debit": info_type_func(),
        "international_transaction_arbitrage_debit": info_type_func(),
        "outward_cheque_bounce_debit": info_type_func(),
        "inward_cheque_bounce_credit": info_type_func(),
        "outward_cheque_bounce_insuff_fund_debit": info_type_func(),
        "inward_cheque_bounce_insuff_fund_credit": info_type_func(),
        "payment_gateway_purchase_debit": info_type_func(),
        "emi_debit": info_type_func(),
        "emi_bounce": info_type_func(),
        "credit_card_bill_debit": info_type_func(),
        "investments": info_type_func(),
        "loan_credits": info_type_func(),
        "income": info_type_func(),
        "negative_balance": info_type_func(),
        "balance_on_1st": info_type_func(),
        "balance_on_5th": info_type_func(),
        "balance_on_10th": info_type_func(),
        "balance_on_15th": info_type_func(),
        "balance_on_20th": info_type_func(),
        "balance_on_25th": info_type_func(),
        "balance_on_30th": info_type_func(),
        "food": info_type_func(),
        "travel": info_type_func(),
        "fuel": info_type_func(),
        "shopping": info_type_func(),
        "digital_payments": info_type_func(),                #UPI, NEFT, RTGS, IMPS
        # "digital_banking_debit": info_type_func(),           #MOBILE BANKING, NET BANKING
        # "digital_banking_credit": info_type_func(),           #MOBILE BANKING, NET BANKING
        "discretionary_spends": info_type_func(),            #POS, PAYMENT GATEWAY
        "negative_balance_days": info_type_func(),  # same day multiple transactions count as 1, negative balance with no transaction count as 1
        "bounce_penal": info_type_func(),
        "avg_bal": info_type_func(),
        "abb_isto_emi": info_type_func(),
        "tax": info_type_func(),
        "bills": info_type_func(),
        "self_transfer": info_type_func(),
        "min_bal_charge": info_type_func(),
        }

    return monthwise_dict

def populate_transaction_type(monthwise_dict, month_name, key):
    debit_keys = ['emi_debit', 'credit_card_bill_debit', 'debit', 'debit_card_debit', 'cash_withdrawl_debit', 'auto_debit_payment_debit', 'bill_payment_debit', 'bank_charge_debit', \
                    'chq_debit', 'upi_debit', 'net_banking_transfer_debit', 'international_transaction_arbitrage_debit', 'outward_cheque_bounce_debit', 'outward_cheque_bounce_insuff_fund_debit', 'payment_gateway_purchase_debit', 'shopping', 'travel', 'discretionary_spends', 'food', 'min_bal_charge', 'tax', 'bills']
    credit_keys = ['loan_credits', 'cash_deposit_credit', 'net_banking_transfer_credit', 'upi_credit', 'chq_credit', 'international_transaction_arbitrage_credit', 'investment_cashin_credit', 'refund_credit', \
                    'bank_interest_credit', 'auto_debit_payment_bounce_credit', 'inward_cheque_bounce_credit', 'inward_cheque_bounce_insuff_fund_credit', 'credit']
    if key in debit_keys:
        monthwise_dict[month_name][key]["transaction_type"]="debit"
    elif key in credit_keys:
        monthwise_dict[month_name][key]["transaction_type"]="credit"
    elif monthwise_dict[month_name][key]["count"]!=0:
        if monthwise_dict[month_name][key]["avg"]>=0:
            monthwise_dict[month_name][key]["transaction_type"]="credit"
        else:
            monthwise_dict[month_name][key]["transaction_type"]="debit"
    return monthwise_dict

#clubbing of data for 30,60,90....days
def club_data(dict_list):
    result = info_type_func()
    for dic in dict_list:
        result["amt"].extend(dic.get("amt",[]))
        result["sum"] = round(dic.get("sum",0)+result["sum"],2)
        result["count"] = round(dic.get("count",0)+result["count"],2)
        if dic.get("min", None):
            if result["min"] is not None:
                result["min"] = round(min(result["min"], dic["min"]),2)
            else:
                result["min"] = dic["min"]
        if dic.get("max", None):
            if result["max"] is not None:
                result["max"] = round(max(result["max"], dic["max"]),2)
            else:
                result["max"] = dic["max"]
        if dic.get("max_date", None):
            if result["max_date"] is not None:
                result["max_date"] = max(result["max_date"], dic["max_date"])
            else:
                result["max_date"] = dic["max_date"]
        if dic.get("min_date", None):
            if result["min_date"] is not None:
                result["min_date"] = min(result["min_date"], dic["min_date"])
            else:
                result["min_date"] = dic["min_date"]
    try:
        result["avg"] = round(result["sum"]/result["count"],2)
    except ZeroDivisionError:
        pass
    return result

def club_data_bank_bal(dict_list):
    result = info_type_func()
    for dic in dict_list:
        result["amt"].extend(dic.get("amt",[]))
        result["sum"] = round(dic.get("sum",0)+result["sum"],2)
        result["count"] = round(dic.get("count",0)+result["count"],2)
        if dic.get("min", None):
            if result["min"] is not None:
                if dic["min"]<result["min"]:
                    result["min_date"]=dic["min_date"]
                    result["min"] = round(dic["min"],2)
                # result["min"] = round(min(result["min"], dic["min"]),2)
            else:
                result["min"] = dic["min"]
                result["min_date"]=dic["min_date"]

        if dic.get("max", None):
            if result["max"] is not None:
                if dic["max"]>result["max"]:
                    result["max_date"]=dic["max_date"]
                    result["max"] = round(dic["max"],2)
                # result["max"] = round(max(result["max"], dic["max"]),2)
            else:
                result["max"] = dic["max"]
                result["max_date"]=dic["max_date"]

    try:
        result["avg"] = round(result["sum"]/result["count"],2)
    except ZeroDivisionError:
        pass
    return result


def insert_data(monthwise_dict, month, key, transaction, add=False, max_min=False, max_min_date=False):
    month_dict_var = monthwise_dict[month][key]
    month_dict_var["count"]+=1
    month_dict_var["amt"].append(transaction["amount"])
    if add:
        month_dict_var["sum"] += transaction["amount"]
    if max_min:
        if month_dict_var["max"] is None or month_dict_var["min"] is None:
            # assign the default value and assign the date if max_min_date
            month_dict_var["max"] = month_dict_var["min"] = transaction["amount"]
            if max_min_date:
                month_dict_var["max_date"] = month_dict_var["min_date"] = transaction["date"]
        else:
            # here check if the date needs to be updated
            if month_dict_var["max"]<transaction["amount"]:
                month_dict_var["max"]=transaction["amount"]
                month_dict_var["max_date"] = transaction["date"] if max_min_date else None
            if month_dict_var["min"]>transaction["amount"]:
                month_dict_var["min"]=transaction["amount"]
                month_dict_var["min_date"] = transaction["date"] if max_min_date else None
    populate_transaction_type(monthwise_dict, month, key)
    return month_dict_var


debit_tags = {
    "international_transaction_arbitrage": "international_transaction_arbitrage_debit",
    "bill_payment": "bill_payment_debit",
    "cash_withdrawl": "cash_withdrawl_debit",
    "bank_charge": "bank_charge_debit",
    "debit_card": "debit_card_debit",
    "outward_cheque_bounce": "outward_cheque_bounce_debit",
    "chq": "chq_debit",
    "upi": "upi_debit",
    "auto_debit_payment": "auto_debit_payment_debit",
    "net_banking_transfer": "net_banking_transfer_debit",
    "payment_gateway_purchase": "payment_gateway_purchase_debit",
    }

credit_tags = {
    "international_transaction_arbitrage": "international_transaction_arbitrage_credit",
    "bank_interest": "bank_interest_credit",
    "refund": "refund_credit",
    "cash_deposit": "cash_deposit_credit",
    "upi": "upi_credit",
    "net_banking_transfer": "net_banking_transfer_credit",
    "auto_debit_payment_bounce": "auto_debit_payment_bounce_credit", 
    "chq": "chq_credit",
    "investment_cashin": "investment_cashin_credit",
    "inward_cheque_bounce": "inward_cheque_bounce_credit",
    }

global categories
categories = {}

def monthwise_calculator(transaction_data, monthwise_dict, salary_transactions):
    categories = get_merchant_category_dict("IN") # passing default as IN because this is used only by DMI, which is based in India
    salary_hash_set = set()    
    if salary_transactions:
        for salary_tranasact in salary_transactions:
            salary_hash_set.add(salary_tranasact["hash"])
    
    EOD_balances_calendar = {}
    EOD_balances_calendar["months_order"] = []
    EOD_balances = {}
    EOD_balances["months_order"] = []
    hash_dict = {}
    monthwise_dict["months_order"] = []
    end_date = None
    last_date = transaction_data[0]["date"]
    closing_balance = transaction_data[0]["balance"]
    current_month = None
    month = None

    # for NET OFF balances
    net_off_balances = {}
    net_off_balances["months_order"] = []
    net_off_balances["start_date"] = []
    sum_lender_transactions = 0

    latest_date = transaction_data[-1]["date"].split()[0]
    latest_date = datetime.strptime(latest_date, "%Y-%m-%d") + timedelta(days=1)
    curr_date = latest_date
    oldest_date = transaction_data[0]["date"].split()[0]
    oldest_date = datetime.strptime(oldest_date, "%Y-%m-%d")

    # monthwise_dict["start_date"] is array of Rolling monthly dates 
    # from first transaction date to (last transaction date + 1 day)
    monthwise_dict["start_date"] = [latest_date]
    while(curr_date > oldest_date):
        # month_days -- will be in no. of days in one month before curr_date's months
        # if curr_date's is May 2022 then month_days will be 30 
        # if curr_date's is Jan 2022 then month_days will be 31 (Dec 2019)
        # if curr_date's is Mar 2020 then month_days will be 29 (Feb 2020)
        if int(curr_date.strftime("%m"))>1:
            month_days = (monthrange(int(curr_date.strftime("%Y")), int(curr_date.strftime("%m"))-1)[1])
        else:
            month_days = (monthrange(int(curr_date.strftime("%Y"))-1, 12)[1])
            
        curr_date = curr_date - timedelta(days = month_days)
        if curr_date > oldest_date:
            monthwise_dict["start_date"].insert(0, curr_date)

    monthwise_dict["start_date"].insert(0, oldest_date)
    current_month_count = len(monthwise_dict["start_date"]) - 1

    # print("Monthwise Dict Start Date --> ", monthwise_dict["start_date"])

    # next_month 
    # One month lesser than start date
    # eg.: 28-Mar-2022, then next_month will be 27-Mar-2022
    next_month = monthwise_dict["start_date"][0] - timedelta(days=1)
    next_month_index = 0
    transact = 0
    
    while transact < len(transaction_data):
        try:
            strDate = transaction_data[transact]["date"].split()[0]
        except AttributeError:
            pass
        objDate = datetime.strptime(strDate, "%Y-%m-%d")
        transaction_data[transact]["date"] = objDate
        calender_month = objDate.strftime('%b-%y')
        # calender month
        # [Mar-2022, Apr-2022, May-2022]
        # if array empty or the month is not  equal to last element 
        # then add it.
        if EOD_balances_calendar["months_order"] == [] or calender_month != EOD_balances_calendar["months_order"][-1]:
            EOD_balances_calendar["months_order"].append(calender_month)
            EOD_balances_calendar[calender_month] = [-1]*(monthrange(int(objDate.strftime("%Y")), int(objDate.strftime("%m")))[1])
            # for NET OFF balances
            net_off_balances["months_order"].append(calender_month)
            net_off_balances[calender_month] = [-1]*(monthrange(int(objDate.strftime("%Y")), int(objDate.strftime("%m")))[1])
    
        curr_date = objDate

        if next_month <= objDate and current_month_count > 0:
            current_month_count -= 1
            if next_month_index == 0:
                current_month = objDate
            else:
                current_month = next_month
            month = "month_" + str(current_month_count)
            monthwise_dict = create_dict(monthwise_dict, month)
            monthwise_dict["months_order"].append(month)
            EOD_balances["months_order"].append(month)
            if next_month_index == 0:
                monthwise_dict[month]["from_date"] = transaction_data[transact]["date"]
            else:
                monthwise_dict[month]["from_date"] = next_month
            next_month_index += 1
            next_month = monthwise_dict["start_date"][next_month_index]
            monthwise_dict[month]["to_date"] = next_month-timedelta(days=1)
            # print("future next month", next_month)
            
            EOD_balances[month] = [-1]*((monthwise_dict[month]["to_date"] - monthwise_dict[month]["from_date"]).days+1)
            # if few months are missing in the account statement, 
            # then we skip the month till next transaction date month doesn't matches the next month
            if next_month <= objDate:
                continue
            # print(month, monthwise_dict[month]["to_date"], monthwise_dict[month]["from_date"], (monthwise_dict[month]["to_date"] - monthwise_dict[month]["from_date"]).days+1, next_month)
            # print((monthwise_dict[month]["to_date"] - monthwise_dict[month]["from_date"]).days+1)
            monthwise_dict[month]["opening_balance"] = insert_data(monthwise_dict, month, "opening_balance", transaction_data[transact],True, True, True)
            monthwise_dict[month]["opening_balance"]["count"] = 1
            monthwise_dict[month]["opening_balance"]["min"] = transaction_data[transact]["balance"]
            monthwise_dict[month]["opening_balance"]["max"] = transaction_data[transact]["balance"]
            monthwise_dict[month]["opening_balance"]["avg"] = transaction_data[transact]["balance"]
            monthwise_dict[month]["opening_balance"]["sum"] = transaction_data[transact]["balance"]
            monthwise_dict[month]["dates"]["min_date"] = monthwise_dict[month]["opening_balance"]["min_date"]
        if transaction_data[transact]["description"] == "lender_transaction" and transaction_data[transact]["transaction_channel"] != "salary":
            # for NET OFF balances
            if transaction_data[transact]["transaction_type"] == "credit":
                # if here -> means the the transaction is actually a lender_transaction
                sum_lender_transactions += transaction_data[transact]["amount"]
            transaction_data[transact] = check_loan(transaction_data[transact])

        
        if transaction_data[transact]["hash"] in salary_hash_set:
            monthwise_dict[month]["salary"] = insert_data(monthwise_dict, month, "salary", transaction_data[transact],True, True, True)
        if transaction_data[transact]["transaction_type"] == "credit":
            if transaction_data[transact]["transaction_channel"] in credit_tags:
                monthwise_dict[month][credit_tags[transaction_data[transact]["transaction_channel"]]] = insert_data(monthwise_dict, month, credit_tags[transaction_data[transact]["transaction_channel"]], transaction_data[transact],True, True, True)
            if transaction_data[transact].get("is_lender", False):
                monthwise_dict[month]["loan_credits"] = insert_data(monthwise_dict, month, "loan_credits", transaction_data[transact],True, True, True)
                if transaction_data[transact]["transaction_channel"] in {"inward_cheque_bounce", "auto_debit_payment_bounce"}:
                    monthwise_dict[month]["emi_bounce"] = insert_data(monthwise_dict, month, "emi_bounce", transaction_data[transact],True, True, True)
            if transaction_data[transact]["transaction_channel"] == "inward_cheque_bounce" and transaction_data[transact]["description"] == "chq_bounce_insuff_funds":
                monthwise_dict[month]["inward_cheque_bounce_insuff_fund_credit"] = insert_data(monthwise_dict, month, "inward_cheque_bounce_insuff_fund_credit", transaction_data[transact],True, True, True)
            elif (transaction_data[transact]["transaction_channel"] not in {"international_transaction_arbitrage", "auto_debit_payment_bounce", "inward_cheque_bounce"}):
                monthwise_dict[month]["credit"] = insert_data(monthwise_dict, month, "credit", transaction_data[transact],True, True, True)            
        else:
            if transaction_data[transact].get("is_lender", False):
                monthwise_dict[month]["emi_debit"] = insert_data(monthwise_dict, month, "emi_debit",transaction_data[transact],True, True, True)
            if transaction_data[transact]["transaction_channel"] in debit_tags:
                monthwise_dict[month][debit_tags[transaction_data[transact]["transaction_channel"]]] = insert_data(monthwise_dict, month, debit_tags[transaction_data[transact]["transaction_channel"]], transaction_data[transact],True, True, True)
                if transaction_data[transact]["description"] == "credit_card_bill":
                    monthwise_dict[month]["credit_card_bill_debit"] = insert_data(monthwise_dict, month, "credit_card_bill_debit", transaction_data[transact], True, True, True)
            if transaction_data[transact]["transaction_channel"] == "investments":
                monthwise_dict[month]["investments"] = insert_data(monthwise_dict, month, "investments", transaction_data[transact], True, True, True)
            if transaction_data[transact]["transaction_channel"] != "outward_cheque_bounce":
                monthwise_dict[month]["debit"] = insert_data(monthwise_dict, month, "debit", transaction_data[transact],True, True, True)
                if transaction_data[transact]['description'] == 'chq_bounce_insuff_funds':
                    monthwise_dict[month]["outward_cheque_bounce_insuff_fund_debit"] = insert_data(monthwise_dict, month, "outward_cheque_bounce_insuff_fund_debit", transaction_data[transact],True, True, True)
            if transaction_data[transact]["transaction_channel"] in {"net_banking_transfer", "upi"}:
                monthwise_dict[month]["digital_payments"] = insert_data(monthwise_dict, month, "digital_payments", transaction_data[transact],True, True, True)
            elif transaction_data[transact]["transaction_channel"] in {"debit_card", "payment_gateway_purchase"}:
                monthwise_dict[month]["discretionary_spends"] = insert_data(monthwise_dict, month, "discretionary_spends", transaction_data[transact],True, True, True)
            if transaction_data[transact]["merchant_category"] in monthwise_dict[month] and transaction_data[transact]["merchant_category"] in categories:
                monthwise_dict[month][transaction_data[transact]["merchant_category"]] = insert_data(monthwise_dict, month, transaction_data[transact]["merchant_category"], transaction_data[transact],True, True, True)
            if transaction_data[transact]["description"] in {"min_bal_charge", "minimum_balance_charge"}:
                monthwise_dict[month]["min_bal_charge"] = insert_data(monthwise_dict, month, "min_bal_charge", transaction_data[transact], True, True, True)
        if transaction_data[transact]["description"] == "self_transfer":
            monthwise_dict[month]["self_transfer"] = insert_data(monthwise_dict, month, "self_transfer", transaction_data[transact], True, True, True)
        bad_flags = ['outward_cheque_bounce', 'auto_debit_payment_bounce', 'Outward Cheque Bounce', 'Auto Debit Payment Bounce']
        if transaction_data[transact]['transaction_channel'] in bad_flags or (transaction_data[transact]['transaction_channel'] == 'bank_charge' and (transaction_data[transact]['description'] == 'ach_bounce_charge' or transaction_data[transact]['description'] == 'chq_bounce_charge')):
            monthwise_dict[month]["bounce_penal"] = insert_data(monthwise_dict, month, "bounce_penal", transaction_data[transact],True, True, True) 

        delta = (objDate - current_month)
        # print("from date=",monthwise_dict[month]["from_date"] , "end date=", monthwise_dict[month]["to_date"], "obj date",monthwise_dict[month]["dates"]["min_date"])
        # print(current_month, objDate, delta.days, next_month)
        try:
            EOD_balances[month][delta.days] = round(transaction_data[transact]["balance"], 2)
        except IndexError:
            pass
        monthwise_dict[month]["closing_balance"]["amt"] = [transaction_data[transact]["balance"]]
        monthwise_dict[month]["closing_balance"]["avg"] = transaction_data[transact]["balance"]
        monthwise_dict[month]["closing_balance"]["min"] = transaction_data[transact]["balance"]
        monthwise_dict[month]["closing_balance"]["max"] = transaction_data[transact]["balance"]
        monthwise_dict[month]["closing_balance"]["sum"] = transaction_data[transact]["balance"]
        monthwise_dict[month]["closing_balance"]["max_date"] = monthwise_dict[month]["closing_balance"]["min_date"] = transaction_data[transact]["date"]
        monthwise_dict[month]["closing_balance"]["count"] = 1
        populate_transaction_type(monthwise_dict, month, "closing_balance")

        monthwise_dict[month]["dates"]["max_date"] = transaction_data[transact]["date"]
        if transaction_data[transact]["balance"] < 0:
            monthwise_dict[month]["negative_balance"] = insert_data(monthwise_dict, month, "negative_balance", transaction_data[transact],True, True, True)
            # In one day count once for negative_balance_days
            if last_date != transaction_data[transact]["date"]:
                monthwise_dict[month]["negative_balance_days"] = insert_data(monthwise_dict, month, "negative_balance_days", transaction_data[transact],True, True, True)

        EOD_balances_calendar[calender_month][int(objDate.strftime("%d"))-1] = transaction_data[transact]['balance']                
        # for NET OFF balances
        net_off_balances[calender_month][int(objDate.strftime("%d"))-1] = transaction_data[transact]['balance'] - sum_lender_transactions
        transact+=1
    last_date = transaction_data[transact-1]["date"]
    monthwise_dict["start_date"].pop()
    return monthwise_dict, EOD_balances, EOD_balances_calendar, net_off_balances

monthly_data_dict = {
        "balance_on_1st": 0,
        "balance_on_5th": 4,
        "balance_on_10th": 9,
        "balance_on_15th": 14,
        "balance_on_20th": 19,
        "balance_on_25th": 24,
        "balance_on_30th": 29,
}


not_to_club = {
    "opening_balance",
    "closing_balance",
    "bank_balance",
    "to_date",
    "from_date",
    "absolute_from_date",
    "absolute_to_date",
    "balance_on_1st",
    "balance_on_5th",
    "balance_on_10th",
    "balance_on_15th",
    "balance_on_20th",
    "balance_on_25th",
    "balance_on_30th",
    "balance_net_off_on_1st",
    "balance_net_off_on_5th",
    "balance_net_off_on_10th",
    "balance_net_off_on_15th",
    "balance_net_off_on_20th",
    "balance_net_off_on_25th",
    "balance_net_off_on_30th"
}

balances_to_club = {
    "bank_balance",
    "balance_on_1st",
    "balance_on_5th",
    "balance_on_10th",
    "balance_on_15th",
    "balance_on_20th",
    "balance_on_25th",
    "balance_on_30th",
    "balance_net_off_on_1st",
    "balance_net_off_on_5th",
    "balance_net_off_on_10th",
    "balance_net_off_on_15th",
    "balance_net_off_on_20th",
    "balance_net_off_on_25th",
    "balance_net_off_on_30th"
}

remove_var_dict = {
    "dates":{"sum","avg", "max", "min", "count", "amt", "transaction_type"},
    "absolute_dates":{"sum","avg", "max", "min", "count", "amt", "transaction_type", "min_date", "max_date"},
    "net_cash_inflow": {"count","max_date", "min_date"},
    "income": {"max_date", "min_date"},
    "emi_debit": {"avg"},
    "loan_credits": {"avg"},
    "balance_on_1st": {"sum"},
    "balance_on_5th": {"sum"},
    "balance_on_10th": {"sum"},
    "balance_on_15th": {"sum"},
    "balance_on_20th": {"sum"},
    "balance_on_25th": {"sum"},
    "balance_on_30th": {"sum"},
    "balance_net_off_on_1st": {"sum"},
    "balance_net_off_on_5th": {"sum"},
    "balance_net_off_on_10th": {"sum"},
    "balance_net_off_on_15th": {"sum"},
    "balance_net_off_on_20th": {"sum"},
    "balance_net_off_on_25th": {"sum"},
    "balance_net_off_on_30th": {"sum"}, 
    "abb_isto_emi": {"amt","sum","max_date","min_date","count"}, 
    "avg_bal": {"sum"},  
}


def format_data(data_dict):
    data_dict["sum"] = round(data_dict["sum"],2)
    if data_dict["count"]>=1:
        data_dict["avg"] = data_dict["sum"]/data_dict["count"]
    data_dict["avg"] = round(data_dict["avg"],2)

    if data_dict["max"] is not None:
        data_dict["max"] = round(data_dict["max"],2)

    if data_dict["min"] is not None:
        data_dict["min"] = round(data_dict["min"],2)
        
    if data_dict["max_date"] is not None:
        data_dict["max_date"] = datetime.strftime(data_dict["max_date"], "%d-%b-%y")
    if data_dict["min_date"] is not None:
        data_dict["min_date"] = datetime.strftime(data_dict["min_date"], "%d-%b-%y")
    return data_dict
    

def rolling_month_analysis_func(transaction_data, salary_transactions):
    # sort the transaction & salary data
    transaction_data = sorted(transaction_data, key=lambda x: x["date"])
    salary_transactions = sorted(salary_transactions, key= lambda x: x["date"])
    
    monthwise_dict = {}
    grouped_data = {}
    monthwise_dict, EOD_balances, EOD_balances_calendar, net_off_balances = monthwise_calculator(transaction_data, monthwise_dict, salary_transactions)
    EOD_balances, monthwise_dict = EOD_balances_func(monthwise_dict[EOD_balances["months_order"][0]]["opening_balance"]["sum"], EOD_balances, monthwise_dict)
    EOD_balances_calendar, avg_month_bal = EOD_balances_func(monthwise_dict[EOD_balances["months_order"][0]]["opening_balance"]["sum"], EOD_balances_calendar) 
    net_off_balances, not_useful = EOD_balances_func(monthwise_dict[EOD_balances["months_order"][0]]["opening_balance"]["sum"], net_off_balances)    
    months_order = monthwise_dict["months_order"]
    monthwise_dict["calendar_months_order"] = EOD_balances_calendar["months_order"]
    n=len(months_order)
    
    calendar_months_list = EOD_balances_calendar["months_order"]
    
    for month_order_key in months_order:
        start_date = monthwise_dict[month_order_key]["from_date"]
        start_day = int(start_date.strftime("%d"))
        end_date = monthwise_dict[month_order_key]["to_date"]
        start_month = start_date.strftime("%b-%y")
        end_month = end_date.strftime("%b-%y")
        if start_month == end_month:
            month_list = [start_month]
        else:
            month_list = [start_month, end_month]
        bal_on_date_key_check = []
        monthwise_dict[month_order_key]["bank_balance"]=info_type_func()
        monthwise_dict[month_order_key]["bank_balance"]["max"]=max(EOD_balances[month_order_key])
        monthwise_dict[month_order_key]["bank_balance"]["min"]=min(EOD_balances[month_order_key])
        monthwise_dict[month_order_key]["bank_balance"]["sum"]=sum(EOD_balances[month_order_key])
        monthwise_dict[month_order_key]["bank_balance"]["count"]=len(EOD_balances[month_order_key])
        monthwise_dict[month_order_key]["bank_balance"]["avg"]=(monthwise_dict[month_order_key]["bank_balance"]["sum"]/monthwise_dict[month_order_key]["bank_balance"]["count"])
        monthwise_dict[month_order_key]["bank_balance"]["amt"]=EOD_balances[month_order_key]
        monthwise_dict[month_order_key]["bank_balance"]["max_date"]=start_date+timedelta(days=EOD_balances[month_order_key].index(max(EOD_balances[month_order_key])))
        monthwise_dict[month_order_key]["bank_balance"]["min_date"]=start_date+timedelta(days=EOD_balances[month_order_key].index(min(EOD_balances[month_order_key])))
        populate_transaction_type(monthwise_dict, month_order_key, "bank_balance")
        
        for month_name in month_list:
            if month_name in EOD_balances_calendar['months_order']:
                monthwise_dict[month_name] = {}
            for bal_on_date, val in monthly_data_dict.items():
                net_off_bal_date = bal_on_date.replace("balance", "balance_net_off")
                tmp_date_str = str(val+1) +'-'+ month_name
                try:
                    tmp_date_obj = datetime.strptime(tmp_date_str, "%d-%b-%y")
                except ValueError:
                    tmp_date_obj = None
                    print('not a proper date')
                    pass
                
                if bal_on_date not in bal_on_date_key_check:
                    monthwise_dict[month_order_key][bal_on_date] = info_type_func()
                    monthwise_dict[month_order_key][net_off_bal_date] = info_type_func()
                
                if tmp_date_obj and tmp_date_obj >= start_date and tmp_date_obj <= end_date:
                    bal_on_date_key_check.append(bal_on_date)
                    monthwise_dict[month_order_key][bal_on_date]["amt"] = [EOD_balances[month_order_key][(tmp_date_obj - start_date).days]]
                    monthwise_dict[month_order_key][bal_on_date]["max_date"] = monthwise_dict[month_order_key][bal_on_date]["min_date"] = tmp_date_obj
                    monthwise_dict[month_order_key][bal_on_date]["sum"] = monthwise_dict[month_order_key][bal_on_date]["max"] = monthwise_dict[month_order_key][bal_on_date]["min"] = monthwise_dict[month_order_key][bal_on_date]["avg"] = EOD_balances[month_order_key][(tmp_date_obj - start_date).days]
                    monthwise_dict[month_order_key][bal_on_date]["count"] = 1
                    
                    if month_name in calendar_months_list:
                        monthwise_dict[month_order_key][net_off_bal_date]["amt"] = [net_off_balances[month_name][val]]
                        monthwise_dict[month_order_key][net_off_bal_date]["max_date"] = monthwise_dict[month_order_key][net_off_bal_date]["min_date"] = tmp_date_obj
                        monthwise_dict[month_order_key][net_off_bal_date]["sum"] = monthwise_dict[month_order_key][net_off_bal_date]["max"] =  monthwise_dict[month_order_key][net_off_bal_date]["min"] = monthwise_dict[month_order_key][net_off_bal_date]["avg"] = net_off_balances[month_name][val]
                        monthwise_dict[month_order_key][net_off_bal_date]["count"] = 1
                    
        monthwise_dict[month_order_key]['net_cash_inflow'] = insert_data(monthwise_dict, month_order_key, "net_cash_inflow", {"amount": round(monthwise_dict[month_order_key]["credit"]["sum"]-monthwise_dict[month_order_key]["debit"]["sum"], 2)}, True, True, False)
        monthwise_dict[month_order_key]['income'] = insert_data(monthwise_dict, month_order_key, "income", {"amount": round(monthwise_dict[month_order_key]["credit"]["sum"] - monthwise_dict[month_order_key]["loan_credits"]["sum"]- monthwise_dict[month_order_key]["refund_credit"]["sum"])}, True, True, False)
    
    prev_group = {}
    for i in range(len(months_order)//3):
        num = 3*(i+1)
        month = str(num)+"_months_group"
        monthwise_dict["months_order"].append(month)
        grouped_data[month] = {}
        for key, value in monthwise_dict["month_0"].items():
            if key not in not_to_club:
                grouped_data[month][key] = club_data([monthwise_dict[months_order[n-num+2]].get(key, {}),monthwise_dict[months_order[n-num+1]].get(key, {}),monthwise_dict[months_order[n-num]].get(key, {}), prev_group.get(key, {})])
            elif key in balances_to_club:
                grouped_data[month][key] = club_data_bank_bal([monthwise_dict[months_order[n-num+2]].get(key, {}),monthwise_dict[months_order[n-num+1]].get(key, {}),monthwise_dict[months_order[n-num]].get(key, {}), prev_group.get(key, {})])
        grouped_data[month]["opening_balance"]=info_type_func()
        grouped_data[month]["opening_balance"]["count"]=1
        grouped_data[month]["opening_balance"]["max"] = grouped_data[month]['opening_balance']['min'] = grouped_data[month]['opening_balance']['amt'] =  grouped_data[month]['opening_balance']['avg'] = EOD_balances['month_' + str(num-1)][0]
        grouped_data[month]["opening_balance"]["amt"] = [grouped_data[month]['opening_balance']['max']]
        grouped_data[month]["opening_balance"]["sum"] = grouped_data[month]['opening_balance']['max']
        grouped_data[month]["opening_balance"]["max_date"]=grouped_data[month]["opening_balance"]["min_date"] = monthwise_dict['month_' + str(num-1)]['dates']['min_date']
        grouped_data[month]["closing_balance"]=info_type_func()
        grouped_data[month]["closing_balance"]["count"]=1
        grouped_data[month]["closing_balance"]["max"] = grouped_data[month]['closing_balance']['min'] = grouped_data[month]['closing_balance']['amt'] =  grouped_data[month]['closing_balance']['avg'] = EOD_balances['month_0'][-1]
        grouped_data[month]["closing_balance"]["amt"] = [grouped_data[month]['closing_balance']['max']]
        grouped_data[month]["closing_balance"]["sum"] = grouped_data[month]['closing_balance']['max']
        grouped_data[month]["closing_balance"]["max_date"]=grouped_data[month]["closing_balance"]["min_date"] = monthwise_dict['month_0']['dates']['max_date']
        prev_group =  grouped_data[month]
    monthwise_dict.update(grouped_data)

    for month in monthwise_dict["months_order"]:
        # print(month, monthwise_dict[month].get("from_date", ""), monthwise_dict[month].get("to_date", ""))        
        for keys, values in monthwise_dict[month].items():
            if type(values) is dict:
                monthwise_dict[month][keys] = format_data(values)
            
            # before returning check if all transaction types are populated,
            # if not populate them explicitly
            if isinstance(monthwise_dict[month][keys], dict) and ('transaction_type' in monthwise_dict[month][keys]) and monthwise_dict[month][keys].get('transaction_type') is None:
                populate_transaction_type(monthwise_dict, month, keys)
        if monthwise_dict[month]["emi_debit"]["sum"] != 0:
            monthwise_dict[month]["abb_isto_emi"]["avg"] =  monthwise_dict[month]["avg_bal"]["avg"]/monthwise_dict[month]["emi_debit"]["sum"]
            monthwise_dict[month]["abb_isto_emi"]["sum"] = monthwise_dict[month]["abb_isto_emi"]["avg"]
            monthwise_dict[month]["abb_isto_emi"]["max"] = monthwise_dict[month]["abb_isto_emi"]["min"]=  monthwise_dict[month]["abb_isto_emi"]["avg"]
            monthwise_dict[month]["abb_isto_emi"]["count"] = 1
        else:
            monthwise_dict[month]["abb_isto_emi"]["avg"] = 9
        monthwise_dict[month]["abb_isto_emi"] = format_data(monthwise_dict[month]["abb_isto_emi"]) 
        monthwise_dict[month]["dates"]["start_date"] = monthwise_dict[month]["dates"].pop("min_date")
        monthwise_dict[month]["dates"]["end_date"] = monthwise_dict[month]["dates"].pop("max_date")

        if "from_date" in monthwise_dict[month]:
            monthwise_dict[month]["from_date"] = datetime.strftime(monthwise_dict[month]["from_date"], "%d-%b-%y")
            monthwise_dict[month]["avg_bal"]["max_date"] = monthwise_dict[month]["from_date"]
        if "to_date" in monthwise_dict[month]:
            monthwise_dict[month]["to_date"] = datetime.strftime(monthwise_dict[month]["to_date"], "%d-%b-%y")
            monthwise_dict[month]["avg_bal"]["min_date"] = monthwise_dict[month]["to_date"]
        for key in remove_var_dict.keys():
            if key in monthwise_dict[month]:
                for value in remove_var_dict[key]:
                    del monthwise_dict[month][key][value] 

    regex_for_month_key = re.compile('month_([0-9]+)')
    for keys in monthwise_dict:
        found_month = regex_for_month_key.findall(keys)
        if found_month:
            from_date = datetime.strftime(monthwise_dict['start_date'][len(monthwise_dict['start_date']) - int(found_month[0]) - 1], "%d-%b-%y")
            try:
                to_date = datetime.strftime(monthwise_dict['start_date'][len(monthwise_dict['start_date']) - int(found_month[0])] - timedelta(days=1), "%d-%b-%y")
            except Exception as e:
                # this means the list reached out of bounds, or beyond the length of the start_date
                # this usually means that the last month has been reached
                to_date = monthwise_dict[month]["dates"]["end_date"]
            monthwise_dict[keys]['absolute_dates']={
                "from_date": from_date,
                "to_date": to_date
            }
    
    regex_for_month_group_key = re.compile('([0-9]+)_months_group')
    for keys in monthwise_dict:
        found_month = regex_for_month_group_key.findall(keys)
        if found_month:
            oldest_ref_month = int(found_month[0])-1
            from_date = monthwise_dict[f"month_{str(oldest_ref_month)}"]["absolute_dates"]["from_date"]
            to_date = monthwise_dict["month_0"]["absolute_dates"]["to_date"]
            monthwise_dict[keys]['absolute_dates']={
                "from_date": from_date,
                "to_date": to_date
            }
    del monthwise_dict['start_date']

    # for avg_bal, since the data is made from Eod_balances list and not the transactions, itself, the data is wrong, so the date needs to be corrected here.
    # simple way is datetime + timedelta(day=index at the list)
    for items in monthwise_dict:
        if isinstance(monthwise_dict[items], dict) and "avg_bal" in monthwise_dict[items]:
            # get the index of the max_bal
            max_bal = monthwise_dict[items]['avg_bal']['max']
            min_bal = monthwise_dict[items]['avg_bal']['min']
            max_bal_index = monthwise_dict[items]['avg_bal']['amt'].index(max_bal) if max_bal!=None else None
            min_bal_index = monthwise_dict[items]['avg_bal']['amt'].index(min_bal) if min_bal!=None else None
            start_date = monthwise_dict[items]['absolute_dates']['from_date']

            if start_date is None:
                continue
            start_date_obj = datetime.strptime(start_date, "%d-%b-%y")
            if max_bal_index!=None:
                max_bal_date = start_date_obj + timedelta(days=max_bal_index)
                monthwise_dict[items]['avg_bal']['max_date'] = max_bal_date.strftime("%d-%b-%y")
            else:
                monthwise_dict[items]['avg_bal']['max_date'] = None
            if min_bal_index!=None:
                min_bal_date = start_date_obj + timedelta(days=min_bal_index)
                monthwise_dict[items]['avg_bal']['min_date'] = min_bal_date.strftime("%d-%b-%y")
            else:
                monthwise_dict[items]['avg_bal']['min_date'] = None
    

    return monthwise_dict