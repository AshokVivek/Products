from typing import List, Tuple

from python.handlers import access_handler
from python.aggregates import check_and_get_everything, get_statement_ids_for_account_id, get_country_for_statement
from datetime import datetime, timedelta
import warnings
import pandas as pd
from python.configs import *
from python.bc_apis import get_bank_connect_eod_balances
import json

warnings.simplefilter(action = "ignore", category = FutureWarning)
pd.options.mode.chained_assignment = None

account_category_mapping = {
    'individual' : 'SAVINGS',
    'corporate': 'CURRENT',
    'overdraft': 'OVERDRAFT',
    'SAVINGS': 'SAVINGS',
    'CURRENT': 'CURRENT'
}


def fill_missing_months(date_list: List[str]) -> Tuple:
    # Convert string dates to datetime objects for proper sorting
    dates = [datetime.strptime(date, '%d-%b-%y') for date in date_list]

    # Get min and max dates to establish range
    min_date = min(dates)
    max_date = max(dates)

    # Create date range for all months
    all_dates = pd.date_range(
        start=min_date.replace(day=1),
        end=max_date.replace(day=1),
        freq='MS'
    )

    original_dates_dict = {date.strftime('%m-%Y'): date for date in dates}

    result = []
    missing_months = []
    for date in all_dates:
        # Check if this month/year combination exists in original dates
        key = date.strftime('%m-%Y')
        if key in original_dates_dict:
            # Use the original date
            result.append(original_dates_dict[key].strftime('%d-%b-%y'))
        else:
            # Use the first of the month
            first_date = date.strftime('01-%b-%y')
            result.append(first_date)
            missing_months.append(first_date)

    return result, missing_months


def bc_p_mapping_handler(event, context):
    print("Event details: {}".format(event))

    entity_id = event.get('entity_id')
    link_id = event.get('link_id')
    bank_mapping = event.get('bank_mapping')
    is_sme = event.get('is_sme', False)
    adjusted_eod = event.get('adjusted_eod', False)
    to_reject_account = event.get('to_reject_account', False)

    PIR_Data = dict()

    account_list = access_handler({"entity_id": entity_id, "access_type": "ACCOUNT_IDENTITY", "to_reject_account": to_reject_account}, None)
    print(f"Account data of all the successful accounts - {account_list}")

    for account_data in account_list:
        account_id = account_data.get('account_id')
        statement_ids = get_statement_ids_for_account_id(entity_id, account_id)
        country = get_country_for_statement(statement_ids[0])

        check_and_get_everything(bank_mapping[account_id]['bank_name'], country)
        
        PIR_Data[account_id] = dict()
        PIR_Data[account_id]['CustomerInfo'] = dict()
        PIR_Data[account_id]['CustomerInfo']['instId'] = bank_mapping[account_id]['perfios_institution_id']
        PIR_Data[account_id]['CustomerInfo']['bankId'] = bank_mapping[account_id]['bank_name']
        PIR_Data[account_id]['CustomerInfo']['bank'] = bank_mapping[account_id]['full_bank_name']
        PIR_Data[account_id]['CustomerInfo']['address'] = account_data.get('address')
        PIR_Data[account_id]['CustomerInfo']['mobile'] = ""
        PIR_Data[account_id]['CustomerInfo']['name'] = account_data.get('name')
        PIR_Data[account_id]['CustomerInfo']['linkId'] = link_id
        PIR_Data[account_id]['CustomerInfo']['landline'] = ""
        PIR_Data[account_id]['CustomerInfo']['pan'] = ""
        PIR_Data[account_id]['CustomerInfo']['email'] = ""
        PIR_Data[account_id]['CustomerInfo']['accountId'] = account_id
        PIR_Data[account_id]['CustomerInfo']['entityId'] = entity_id
        PIR_Data[account_id]['CustomerInfo']['ifsc'] = account_data.get('ifsc')
        PIR_Data[account_id]['CustomerInfo']['micr'] = account_data.get('micr')

        PIR_Data[account_id]['SummaryInfo'] = dict()
        PIR_Data[account_id]['SummaryInfo']['accNo'] = account_data.get('account_number')
        PIR_Data[account_id]['SummaryInfo']['Total'] = dict()
        PIR_Data[account_id]['SummaryInfo']['Average'] = dict()
        
        PIR_Data[account_id]['SummaryInfo']['instName'] = bank_mapping[account_id]['full_bank_name']

        PIR_Data[account_id]['Xns'] = dict()
        PIR_Data[account_id]['Xns']['Xn'] = list()
        PIR_Data[account_id]['Xns']['accountNo'] = account_data.get('account_number')
        
        account_category = account_data.get('account_category', None)
        is_od_account = account_data.get('is_od_account', None)
        try:
            credit_limit = int( account_data.get('credit_limit', None) )
            od_limit = int( account_data.get('od_limit', None) )
        except:
            credit_limit = None
            od_limit = None

        PIR_Data[account_id]['SummaryInfo']['accType'] = account_category
        PIR_Data[account_id]['CustomerInfo']['accountType'] = account_category
        PIR_Data[account_id]['Xns']['accountType'] = account_category

        PIR_Data[account_id]['SummaryInfo']['isOverdraftAccount'] = is_od_account

        account_transactions = access_handler({"entity_id": entity_id, "account_id": account_id, "access_type": "ACCOUNT_TRANSACTIONS", "to_reject_account": to_reject_account}, None)
        
        for transaction in account_transactions:
            transaction['narration'] = transaction.pop('transaction_note')
            transaction['chqNo'] = transaction.pop('chq_num', '')
            transaction['category'] = transaction.pop('description')
            transaction['merchantCategory'] = transaction.pop('merchant_category')
            transaction['transactionChannel'] = transaction.pop('transaction_channel')
            transaction['hash'] = transaction.pop('hash')
            account_id = transaction.pop('account_id')
            PIR_Data[account_id]['Xns']['Xn'].append(transaction)

        top_credits_debits = access_handler({"entity_id": entity_id, "account_id": account_id,"access_type": "ACCOUNT_TOP_CREDITS_DEBITS", "to_reject_account": to_reject_account}, None)
        
        PIR_Data[account_id]['Top5FundsReceived'] = dict()
        PIR_Data[account_id]['Top5FundsReceived']['Item'] = list()
        for month in list(top_credits_debits['top_5_credit'].keys()):
            for transaction_note, amount in top_credits_debits['top_5_credit'][month].items():
                credit_transactions = list(filter(lambda account_transactions: account_transactions['narration'] == transaction_note and account_transactions['amount'] == amount, account_transactions))
                if len(credit_transactions) > 0:
                    credit_transaction = {
                        "amount": credit_transactions[0]['amount'],
                        "month": month,
                        "category": credit_transactions[0]['category'],
                        "narration": credit_transactions[0]['narration']
                    }
                    PIR_Data[account_id]['Top5FundsReceived']['Item'].append(credit_transaction)

        PIR_Data[account_id]['Top5FundsTransferred'] = dict()
        PIR_Data[account_id]['Top5FundsTransferred']['Item'] = list()
        for month in list(top_credits_debits['top_5_debit'].keys()):
            for transaction_note, amount in top_credits_debits['top_5_debit'][month].items():
                debit_transactions = list(filter(lambda account_transactions: account_transactions['narration'] == transaction_note and account_transactions['amount'] == amount, account_transactions))
                if len(debit_transactions) > 0:
                    debit_transaction = {
                        "amount": debit_transactions[0]['amount'],
                        "month": month,
                        "category": debit_transactions[0]['category'],
                        "narration": debit_transactions[0]['narration']
                    }
                    PIR_Data[account_id]['Top5FundsTransferred']['Item'].append(debit_transaction)

        PIR_Data[account_id]['EODBalances'] = dict()
        PIR_Data[account_id]['EODBalances']['EODBalance'] = list()

        PIR_Data[account_id]['MonthlyDetails'] = dict()
        PIR_Data[account_id]['MonthlyDetails']['MonthlyDetail'] = list()

        eod_balances = {}
        get_bank_connect_eod_balances(entity_id, eod_balances, adjusted_eod, is_sme)
        eod_balances = eod_balances.get(account_id, {})

        if eod_balances != {}:
            # The fill_missing_months function will add the start dates to 1st of those months,
            # where the transactions are not present but the balances are added due to extrapolation.
            start_dates, missing_months = fill_missing_months(eod_balances['start_date'])
            for month, start_date in zip(eod_balances['Months_order'], start_dates):
                start_datetime = datetime.strptime(start_date, '%d-%b-%y')
                month_datetime = datetime.strptime(month, '%b-%y').strftime('%b-%Y')
                credit_limit_overdrwan_days = 0
                start_date_index = start_datetime.day - 1

                cur_month_eod_balances = eod_balances[month][start_date_index:]
                for day_to_add, eod_balance in enumerate(cur_month_eod_balances):
                    if eod_balance is None:
                        continue
                    if credit_limit != None and eod_balance < -1*abs(credit_limit):
                        credit_limit_overdrwan_days += 1 
                    formatted_eod_balance = {
                        "date": str((start_datetime + timedelta(days=day_to_add)).date()), 
                        "balance": eod_balance
                    }
                    PIR_Data[account_id]['EODBalances']['EODBalance'].append(formatted_eod_balance)
                balance_on_10th = eod_balances[month][9] if len(eod_balances[month])>=10 else None
                balance_on_20th = eod_balances[month][19] if len(eod_balances[month])>=20 else None
                balance_on_30th = eod_balances[month][29] if len(eod_balances[month])>=30 else None
                PIR_Data[account_id]['MonthlyDetails']['MonthlyDetail'].append({
                    "monthName": month_datetime,
                    "startDate": None if start_date in missing_months else str(start_datetime.date()),
                    "overdrawnDays": credit_limit_overdrwan_days,
                    "balanceOn10th": balance_on_10th,
                    "balanceOn20th": balance_on_20th,
                    "balanceOn30th": balance_on_30th
                })
        
        summary_total = dict()
        summary_average = dict()
        monthly_analysis = access_handler({"entity_id": entity_id, "account_id": account_id, "access_type": "ACCOUNT_MONTHLY_ANALYSIS","credit_limit":credit_limit, "to_reject_account": to_reject_account}, None)
        if monthly_analysis != {}:
            for item in PIR_Data[account_id]['MonthlyDetails']['MonthlyDetail']:
                month = item.get('monthName')
                item['balMin'] = monthly_analysis['min_bal'][month]
                item['balAvg'] = monthly_analysis['avg_bal'][month] 
                item['balMax'] = monthly_analysis['max_bal'][month]
                item['credits'] = monthly_analysis['cnt_credit'][month]
                item['debits'] = monthly_analysis['cnt_debit'][month]
                item['totalCredit'] = monthly_analysis['amt_credit'][month] 
                item['totalDebit'] = monthly_analysis['amt_debit'][month] 
                item['cashDeposits'] = monthly_analysis['cnt_cash_deposit_credit'][month] 
                item['cashWithdrawals'] = monthly_analysis['cnt_cash_withdrawl_debit'][month] 
                item['totalCashDeposit'] = monthly_analysis['amt_cash_deposit_credit'][month] 
                item['totalCashWithdrawal'] = monthly_analysis['amt_cash_withdrawl_debit'][month] 
                item['chqDeposits'] = monthly_analysis['cnt_chq_credit'][month] 
                item['chqIssues'] = monthly_analysis['cnt_chq_debit'][month] 
                item['totalChqIssue'] = monthly_analysis['amt_chq_debit'][month] 
                item['totalChqDeposit'] = monthly_analysis['amt_chq_credit'][month] 
                item['inwBounces'] = monthly_analysis['cnt_inward_cheque_bounce_credit'][month] 
                item['outwBounces'] = monthly_analysis['cnt_outward_cheque_bounce_debit'][month] 
                item['inwECSBounces'] = monthly_analysis['cnt_inward_cheque_bounce_insuff_funds_credit'][month] 
                item['salaryCredits'] = monthly_analysis['total_amount_of_salary'][month]
                item['businessCredits'] = monthly_analysis['amt_business_credit'][month]
                item['amtStopEmiCharge'] = monthly_analysis['amt_stop_emi_charge'][month]
                item['cntStopEmiCharge'] = monthly_analysis['cnt_stop_emi_charge'][month]
                item['drawingPowerVariableAmounts'] = credit_limit
                item['sanctionLimitFixedAmount'] = od_limit
                item['fixedDepositAmount'] = None
                item['interestServingDays'] = None


            total_items = len(PIR_Data[account_id]['MonthlyDetails']['MonthlyDetail'])
            # now populating the summary info month count for this account_id
            PIR_Data[account_id]['SummaryInfo']['fullMonthCount'] = total_items
            for item in PIR_Data[account_id]['MonthlyDetails']['MonthlyDetail']:
                for key in item:
                    if key in ['monthName', 'startDate','drawingPowerVariableAmounts','sanctionLimitFixedAmount','fixedDepositAmount','interestServingDays','balanceOn10th','balanceOn20th','balanceOn30th']:
                        continue
                    else:
                        try:
                            summary_total[key] += item[key]
                        except:
                            summary_total[key] = item[key]
            for key in summary_total:
                summary_total[key] = round(summary_total[key], 2)
                try:
                    if key in ['balMin', 'balAvg', 'balMax', 'totalCredit', 'totalDebit', 'totalCashDeposit', 'totalCashWithdrawal', 'totalChqIssue', 'totalChqDeposit']:
                        summary_average[key] = round(summary_total[key]/(1.0*total_items), 2)
                    else:
                        summary_average[key] = round(summary_total[key]/(1.0*total_items))
                except:
                    continue
        
        PIR_Data[account_id]['SummaryInfo']['Total'] = summary_total
        PIR_Data[account_id]['SummaryInfo']['Average'] = summary_average

        PIR_Data[account_id]['RegularDebits'] = dict()
        PIR_Data[account_id]['RegularDebits']['RXn'] = list()
        recurring_transactions = access_handler({"entity_id": entity_id, "account_id": account_id, "access_type": "ACCOUNT_RECURRING_TRANSACTIONS", "to_reject_account": to_reject_account}, None)
        for group, transaction_object in enumerate(recurring_transactions['debit_transactions'], start=1):
            for single_transaction in transaction_object.get('transactions'):
                single_transaction['narration'] = single_transaction.pop('transaction_note')
                single_transaction['chqNo'] = ""
                single_transaction['category'] = single_transaction.pop('description')
                single_transaction['merchantCategory'] = single_transaction.pop('merchant_category')
                single_transaction['group'] = group
                single_transaction['transactionChannel'] = single_transaction.pop('transaction_channel')
                single_transaction['hash'] = single_transaction.pop('hash')
                single_transaction.pop('clean_transaction_note')
                account_id = single_transaction.pop('account_id')
                PIR_Data[account_id]['RegularDebits']['RXn'].append(single_transaction)

        PIR_Data[account_id]['RegularCredits'] = dict()
        PIR_Data[account_id]['RegularCredits']['RXn'] = list()
        for group, transaction_object in enumerate(recurring_transactions['credit_transactions'], start=1):
            for single_transaction in transaction_object.get('transactions'):
                single_transaction['narration'] = single_transaction.pop('transaction_note')
                single_transaction['chqNo'] = ""
                single_transaction['category'] = single_transaction.pop('description')
                single_transaction['merchantCategory'] = single_transaction.pop('merchant_category')
                single_transaction['group'] = group
                single_transaction['transactionChannel'] = single_transaction.pop('transaction_channel')
                single_transaction['hash'] = single_transaction.pop('hash')
                single_transaction.pop('clean_transaction_note')
                account_id = single_transaction.pop('account_id')
                PIR_Data[account_id]['RegularCredits']['RXn'].append(single_transaction)
    final_result = {"resDetails": {"PIR:Data": PIR_Data}}
    response = {}
    if (len(json.dumps(final_result))) < 120000: 
        response = final_result 
    else:
        object_key = f"perfformer/entity_{entity_id}"
        final_result = json.dumps(final_result)
        s3_object = s3_resource.Object(BANK_CONNECT_DDB_FAILOVER_BUCKET, object_key)
        s3_object.put(Body=bytes(final_result, encoding='utf-8'))
        response = {"s3_object_key": object_key}
    return response
