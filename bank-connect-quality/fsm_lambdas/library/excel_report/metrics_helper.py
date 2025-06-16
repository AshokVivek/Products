from datetime import datetime, timedelta
from calendar import monthrange
from library.excel_report.report_analysis import monthwise_calculator

from library.excel_report.constants import DEFAULT_REQUIRED_TRANSACTIONS_COUNT


def get_metrics(transaction_data):
    metrics_data = {
        'max_balance'	: 0.0,
        'avg_monthly_credit'	: 0.0,
        'credit_debit_month_wise': [],
        'max_credit_txn': {},
        'avg_credit_month_wise': {},
        'max_debit_txn': {},
        'avg_debit'	: 0.0,
        'balance_ts'	: [],
        'avg_credit'	: 0.0,
        'avg_monthly_debit': 0.0,
        'avg_balance'	: 0.0,
        'avg_debit_month_wise': {},
        'min_balance'	: 0.0,
        }

    if len(transaction_data) == 0:
        return metrics_data

    if transaction_data[0]['transaction_type'] == 'debit':
        last_bal = transaction_data[0]['balance'] + transaction_data[0]['amount']
    else:
        last_bal = transaction_data[0]['balance'] - transaction_data[0]['amount']
    metrics_data['min_balance'] = last_bal
    metrics_data['max_balance'] = last_bal

    month_var = {}
    EOD_balances = {}
    EOD_balances["Months_order"] = []

    for transact in transaction_data:

        metrics_data['balance_ts'].append({
            'date'	: transact['date'],
            'balance':	transact['balance']
            })

        metrics_data['max_balance'] = max(metrics_data['max_balance'], transact['balance'])
        metrics_data['min_balance'] = min(metrics_data['min_balance'], transact['balance'])

        strDate = transact["date"]
        objDate = datetime.strptime(strDate, '%Y-%m-%d %H:%M:%S')
        transact["month"] = datetime.strftime(objDate, '%Y-%m')
        # month = datetime.strftime(objDate, '%m')

        if len(EOD_balances["Months_order"]) == 0 or EOD_balances["Months_order"][-1] != transact['month']:
            month_var[transact['month']] = {
                                            'cnt_credit': 0,
                                            'cnt_debit': 0,
                                            }
            metrics_data['credit_debit_month_wise'].append({'credit'	:	0,
                                                            'debit'	:	0,
                                                            'month'	: transact['month']})

            EOD_balances["Months_order"].append(transact['month'])
            EOD_balances[transact['month']] = [-1]*(monthrange(int(objDate.strftime("%Y")), int(objDate.strftime("%m")))[1])

        if transact['transaction_type'] == "credit":
            month_var[transact['month']]['cnt_credit'] += 1
            # print('metrics_d[-1]',metrics_data['credit_debit_month_wise'])
            metrics_data['credit_debit_month_wise'][-1]['credit'] += float(transact['amount'])
            if metrics_data['max_credit_txn'] == {} or metrics_data['max_credit_txn']['amount'] < transact['amount']:
                metrics_data['max_credit_txn'] = transact
        else:
            month_var[transact['month']]['cnt_debit'] += 1
            metrics_data['credit_debit_month_wise'][-1]['debit'] += float(transact['amount'])
            if metrics_data['max_debit_txn'] == {} or metrics_data['max_debit_txn']['amount'] < transact['amount']:
                metrics_data['max_debit_txn'] = transact

        EOD_balances[transact['month']][int(objDate.strftime("%d"))-1] = transact['balance']

    total_credit = 0
    credit_count = 0
    total_debit = 0
    debit_count = 0
    for data in metrics_data['credit_debit_month_wise']:
        total_credit += data['credit']
        credit_count += month_var[data['month']]['cnt_credit']
        if month_var[data['month']]['cnt_credit'] > 0:
            metrics_data['avg_credit_month_wise'][data['month']] = round(data['credit']/month_var[data['month']]['cnt_credit'], 2)
        total_debit += data['debit']
        debit_count += month_var[data['month']]['cnt_debit']
        if month_var[data['month']]['cnt_debit'] > 0:
            metrics_data['avg_debit_month_wise'][data['month']] = round(data['debit']/month_var[data['month']]['cnt_debit'], 2)

    if credit_count > 0:
        metrics_data['avg_credit'] = round(total_credit/credit_count, 2)
    if debit_count > 0:
        metrics_data['avg_debit'] = round(total_debit/debit_count, 2)
    if len(metrics_data['credit_debit_month_wise']) > 0:
        metrics_data['avg_monthly_credit'] = round(total_credit/len(metrics_data['credit_debit_month_wise']), 2)
        metrics_data['avg_monthly_debit'] = round(total_debit/len(metrics_data['credit_debit_month_wise']), 2)

    total_balance = 0
    total_days = 0

    for month in EOD_balances["Months_order"]:
        for row in range(1, 32):
            if row <= len(EOD_balances[month]):
                if EOD_balances[month][row-1] == -1:
                    EOD_balances[month][row-1] = last_bal
                else:
                    last_bal = EOD_balances[month][row-1]

                total_balance += EOD_balances[month][row-1]
                total_days += 1
    if total_days > 0:
        metrics_data['avg_balance'] = round(total_balance/total_days, 2)

    return metrics_data

def get_eod_and_monthly_bal_transaction_notes(transaction_data, salary_transactions):
    monthwise_dict = {}
    monthly_bal_transaction_notes = {}
    salary_data = {'transactions': []}

    monthwise_dict, monthly_bal_transaction_notes, salary_data, credit_tags, debit_tags, loan_dict, all_loan_transactions, hash_to_index, net_off_balances = monthwise_calculator(transaction_data, monthwise_dict, monthly_bal_transaction_notes, salary_data, salary_transactions, {})    
    
    return net_off_balances['months_order'], monthly_bal_transaction_notes

def top_debit_credit_corrected(transaction_data, req_transactions_count=DEFAULT_REQUIRED_TRANSACTIONS_COUNT):
    temp_transaction_data = sorted(transaction_data, key=lambda x: x['date'])
    months_order = []
    if len(temp_transaction_data)>0:
        txn_start_date = datetime.strptime(temp_transaction_data[0]['date'].split()[0], '%Y-%m-%d').replace(day=1)
        txn_end_date = datetime.strptime(temp_transaction_data[-1]['date'].split()[0], '%Y-%m-%d').replace(day=1)
        type_transactions = {}
        while txn_start_date<=txn_end_date:
            month = txn_start_date.strftime('%b-%y')
            months_order.append(month)
            type_transactions[month] = {
                'debit': [],
                'credit': [],
            }
            txn_start_date += timedelta(days=32)
            txn_start_date = txn_start_date.replace(day=1)
        
    for i in range(len(transaction_data)):
        strDate = transaction_data[i]["date"]
        strDate = list(strDate.split())
        strDate = strDate[0]
        objDate = datetime.strptime(strDate, '%Y-%m-%d')
        # objDate = strDate
        transaction_data[i]["date"] = datetime.strftime(objDate, '%d-%b-%y')
        month = transaction_data[i]['date'].split('-', 1)
        month = month[1]
        if transaction_data[i]['transaction_type']=="debit":
            type_transactions[month]['debit'].append(transaction_data[i])
        else:
            type_transactions[month]['credit'].append(transaction_data[i])
    debit = {
        "type": f'top_{req_transactions_count}_debit',
        "data": []
    }
    credit = {
        "type": f'top_{req_transactions_count}_credit',
        "data": []
    }
    for month in months_order:
        debit["data"].append({
            "month": month,
            "data": sorted(type_transactions[month]["debit"], key=lambda x: x['amount'], reverse=True)[:req_transactions_count]
        })
        credit["data"].append({
            "month": month,
            "data": sorted(type_transactions[month]["credit"], key=lambda x: x['amount'], reverse=True)[:req_transactions_count]
        })

    return [debit, credit]

def top_debit_credit(transaction_data, correction=False, req_transactions_count=DEFAULT_REQUIRED_TRANSACTIONS_COUNT):
    # TODO: Remove this check after this API is corrected and go live
    if correction:
        credit = {}
        debit = {}
        months_order, monthly_bal_transaction_notes = get_eod_and_monthly_bal_transaction_notes(transaction_data, {})
        for month in months_order:
            credit[month] = []
            debit[month] = []
            if len(monthly_bal_transaction_notes[month]['debit']['amount']) > 0:
                monthly_bal_transaction_notes[month]['debit']['amount'], monthly_bal_transaction_notes[month]['debit']['All transaction notes'], monthly_bal_transaction_notes[month]['debit']['unclean_merchant'] = zip(*sorted(zip(monthly_bal_transaction_notes[month]['debit']['amount'], monthly_bal_transaction_notes[month]['debit']['All transaction notes'], monthly_bal_transaction_notes[month]['debit']['unclean_merchant']), reverse=True))
            if len(monthly_bal_transaction_notes[month]['credit']['amount']) > 0:
                monthly_bal_transaction_notes[month]['credit']['amount'], monthly_bal_transaction_notes[month]['credit']['All transaction notes'], monthly_bal_transaction_notes[month]['credit']['unclean_merchant'] = zip(*sorted(zip(monthly_bal_transaction_notes[month]['credit']['amount'], monthly_bal_transaction_notes[month]['credit']['All transaction notes'], monthly_bal_transaction_notes[month]['credit']['unclean_merchant']), reverse=True))
            for i in range(req_transactions_count):
                try:
                    written_note = f"Transfer to {monthly_bal_transaction_notes[month]['debit']['unclean_merchant'][i]}" if monthly_bal_transaction_notes[month]['debit']['unclean_merchant'][i] else monthly_bal_transaction_notes[month]['debit']['All transaction notes'][i]
                    debit[month].append({
                        'description': written_note,
                        'amount': monthly_bal_transaction_notes[month]['debit']['amount'][i]
                    })
                except IndexError:
                    pass
                try:
                    written_note = f"Transfer from {monthly_bal_transaction_notes[month]['credit']['unclean_merchant'][i]}" if monthly_bal_transaction_notes[month]['credit']['unclean_merchant'][i] else monthly_bal_transaction_notes[month]['credit']['All transaction notes'][i]
                    credit[month].append({
                        'description':written_note,
                        'amount':monthly_bal_transaction_notes[month]['credit']['amount'][i]
                    })
                except IndexError:
                    pass
        return debit, credit
    credit = {}
    debit = {}
    months_order, monthly_bal_transaction_notes = get_eod_and_monthly_bal_transaction_notes(transaction_data, {})
    for month in months_order:
        credit[month] = {}
        debit[month] = {}
        if len(monthly_bal_transaction_notes[month]['debit']['amount']) > 0:
            monthly_bal_transaction_notes[month]['debit']['amount'], monthly_bal_transaction_notes[month]['debit']['All transaction notes'] = zip(*sorted(zip(monthly_bal_transaction_notes[month]['debit']['amount'], monthly_bal_transaction_notes[month]['debit']['All transaction notes']), reverse=True))
        if len(monthly_bal_transaction_notes[month]['credit']['amount']) > 0:
            monthly_bal_transaction_notes[month]['credit']['amount'], monthly_bal_transaction_notes[month]['credit']['All transaction notes'] = zip(*sorted(zip(monthly_bal_transaction_notes[month]['credit']['amount'], monthly_bal_transaction_notes[month]['credit']['All transaction notes']), reverse=True))
        for i in range(req_transactions_count):
            try:
                debit[month][monthly_bal_transaction_notes[month]['debit']['All transaction notes'][i]] = monthly_bal_transaction_notes[month]['debit']['amount'][i]
            except IndexError:
                pass
            try:
                credit[month][monthly_bal_transaction_notes[month]['credit']['All transaction notes'][i]] = monthly_bal_transaction_notes[month]['credit']['amount'][i]
            except IndexError:
                pass
    return debit, credit