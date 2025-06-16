import xlsxwriter
from datetime import datetime, timedelta
from calendar import monthrange
from library.excel_report.report_analysis_dicts import get_monthly_analysis_mapping, monthwise_details_dict
from xlsxwriter.utility import xl_rowcol_to_cell
from library.lender_list import check_loan
import copy
from library.excel_report.constants import V5_SUMMARY_INFO, V5_PERSONAL_DATA, OVERALL_SUMMARY_DICT_V5
from library.excel_report.excel_util import add_analysis_summary_info_to_report
from library.excel_report.report_formats import months_formats_func, transaction_format_func


def EOD_balances_func(workbook, EOD_balances, workbook_num='', sheet_name='Daily EOD Balances', version='v1'):
    pivot_cell = workbook.add_format(
        {'font_color': '#FFFFFF', 'bg_color': '#808080', 'valign': 'vcenter', 'border': 1, 'font_size': 10, 'bold': True, 'align': 'center'})
    vertical_heading_cell = workbook.add_format(
        {'font_color': '#000000', 'bg_color': '#D8E4BC', 'valign': 'vcenter', 'border': 1, 'font_size': 10, 'bold': True, 'align': 'center'})
    horizontal_heading_cell = workbook.add_format(
        {'font_color': '#FFFFFF', 'bg_color': '#002060', 'valign': 'vcenter', 'border': 1, 'font_size': 10, 'bold': True, 'align': 'center', 'num_format': 'mmm-yy'})
    text_body_cell = workbook.add_format(
        {'font_color': '#000000', 'bg_color': '#F1f1f1', 'valign': 'vcenter', 'border': 1, 'font_size': 10, 'align': 'right', 'num_format': '#,##0.00'})
    
    EOD_balances_sheet_name = sheet_name+workbook_num if version!='v5' else 'EOD Balances'+workbook_num
    if version=='v5' and sheet_name=='Adjusted Daily EOD Balances':
        EOD_balances_sheet_name = 'Adjusted EOD Balances'

    EOD_balances_worksheet = workbook.add_worksheet(EOD_balances_sheet_name)
    row = 0
    EOD_balances_worksheet.write('A1', 'Day/Month', pivot_cell)
    EOD_balances_worksheet.freeze_panes(1, 1)

    for i in range(1, 32):
        EOD_balances_worksheet.write(i, 0, i, vertical_heading_cell)

    col = 1
    for month in EOD_balances["Months_order"]:
        row = 0
        mon = datetime.strptime(month, '%b-%y')
        EOD_balances_worksheet.write(row, col, mon, horizontal_heading_cell)
        for row in range(1, 32):
            if row <= len(EOD_balances[month]):
                EOD_balances_worksheet.write(row, col, EOD_balances[month][row-1], text_body_cell)
            else:
                EOD_balances_worksheet.write(row, col, '', text_body_cell)
        cell = xl_rowcol_to_cell(row, col)
        s = '{}:{}'.format(cell[0], cell[0])
        EOD_balances_worksheet.set_column(s, 17)
        col += 1

def create_dict(monthwise_dict, month):
    monthwise_dict[month] = {
        'Total No. of Credit Transactions': 0,
        'Total Amount of Credit Transactions': 0.00,
        'Total No. of Debit Transactions': 0,
        'Total Amount of Debit Transactions': 0.00,
        'Total No. of Cash Deposits': 0,
        'Total Amount of Cash Deposits': 0.00,
        'Total No. of Cash Withdrawals': 0,
        'Total Amount of Cash Withdrawals': 0.00,
        'Total No. of Cheque Deposits': 0,
        'Total Amount of Cheque Deposits': 0.00,
        'Total No. of Cheque Issues': 0,
        'Total Amount of Cheque Issues': 0.00,
        'Total No. of Inward Cheque Bounces': 0,
        'Total No. of Outward Cheque Bounces': 0,
        'Min EOD Balance': 0.00,
        'Max EOD Balance': 0.00,
        'Average EOD Balance': 0.00,
        'Opening Balance': 0.00,
        'Closing balance': 0.00,
        'Net Cashflow': 0.00,
        'Median Balance': 0.00,
        'Average Credit Transaction Size': 0.00,
        'Average Debit Transaction Size': 0.00,
        'Average EMI Transactions Size': 0.00,
        'Mode Balance': 0.00,
        'Number of Transactions': 0,
        'Maximum Balance': 0.00,
        'Minimum Balance': 0.00,
        'Total Amount Credited through transfers': 0.00,
        'Total Amount Credited through UPI': 0.00,
        'Total Amount of Salary': 0.00,
        'Total Amount of International Credit': 0.00,
        'Total Amount of Investment Cash-ins': 0.00,
        'Total Amount of Refund': 0.00,
        'Total Amount of Bank Interest': 0.00,
        'Total Amount Spend through Debit card': 0.00,
        'Total Amount of Auto-Debit Payments': 0.00,
        'Total Amount of Bill Payments': 0.00,
        'Total Amount of Bank Charges': 0.00,
        'Total Amount of Auto debit bounce': 0.00,
        'Total Amount Debited through UPI': 0.00,
        'Total Amount Debited through transfers': 0.00,
        'Total Amount of International Debit': 0.00,
        'Total Amount Debited through Outward Cheque Bounce': 0.00,
        'Total Amount Credited through Inward Cheque Bounce': 0.00,
        'Total Amount Debited through Ach Bounce Charge': 0.00,
        'Total Amount Debited through Cheque Bounce Charge': 0.00,
        'Total Amount Debited through Bounce Charge': 0.00,
        'Total Amount of Payment Gateway Purchase': 0.00,
        'Total Amount Debited through EMI': 0.00,
        'Total Amount Credit Card Bill Debit': 0.00,
        'Total Amount of investments': 0.00,
        'Total Loan Credits Amount': 0.00,
        'Total Debit without OW and Refund': 0.00,
        'Total Credit without IW and Refund': 0.00,
        'Turnover Excluding Loan and Self Credits': 0.00,
        'Total Amount of Credit Self-transfer': 0.00,
        'Total Amount of Debit Self-transfer': 0.00,
        'Total Amount of Business Credit': 0.00,
        'Total No. of Business Credit': 0.00,
        'Number of Loan Credits': 0,
        'Number of investments': 0,
        'Number of Credit Card Bill Debit': 0,
        'Number of EMI Transactions': 0,
        'Number of Cash Deposit Transactions': 0,
        'Number of Net Banking Credit Transactions': 0,
        'Number of Credit Transactions through UPI': 0,
        'Number of Salary Transactions': 0,
        'Number of International Credit transactions': 0,
        'Number of Investment Cash-ins': 0,
        'Number of Refund Transactions': 0,
        'Number of Bank Interest Credits': 0,
        'Number of Debit Card Transactions': 0,
        'Number of Auto-debited payments': 0,
        'Number of Bill Payments': 0,
        'Number of Bank Charge payments': 0,
        'Number of Debit Transactions through Ach Bounce Charge': 0,
        'Number of Debit Transactions through Cheque Bounce Charge': 0,
        'Number of Debit Transactions through Bounce Charge': 0,
        'Number of Auto-Debit Bounces': 0,
        'Number of Debit Transactions through UPI': 0,
        'Number of Net Banking Debit Transactions': 0,
        'Number of International Debit transactions': 0,
        'Number of Payment Gateway Purchase': 0,
        'Number of Self-transfer Credit': 0,
        'Number of Self-transfer Debit': 0,
        '% Salary Spent on Bill Payment (7 days)': 0.00,
        '% Salary Spent Through Cash Withdrawal (7 days)': 0.00,
        '% Salary Spent through Debit Card (7 days)': 0.00,
        '% Salary Spent through Net Banking (7 days)': 0.00,
        '% Salary Spent through UPI (7 days)': 0.00,
        '% Inward Bounce': 0.00,
        '% Outward Bounce': 0.00,
        '% Cash Deposit to Total Credit': 0.00,
        'Net Credit Amount': 0.00,
        'Net Debit Amount': 0.00,
        'Net Debit Count': 0.00,
        'Net Credit Count': 0.00,
        'Number of Days with balance < 25000': 0,
        'Interest on CC/OD': 0.00
    }
    return monthwise_dict


"""index 2 for total sort_values index 4 is for max value"""


def cred_deb_tags():
    debit_tags = {
        'international_transaction_arbitrage': ['Total Amount of International Debit', 'Number of International Debit transactions', 0,  0],
        'bill_payment': ['Total Amount of Bill Payments', 'Number of Bill Payments', 0, 0],
        'cash_withdrawl': ['Total Amount of Cash Withdrawal', 'Number of Cash Withdrawal Transactions', 0, 0],
        'bank_charge': ['Total Amount of Bank Charges', 'Number of Bank Charge payments', 0,  0],
        'debit_card': ['Total Amount Spend through Debit card', 'Number of Debit Card Transactions', 0,  0],
        'outward_cheque_bounce': ['Total Amount Debited through Outward Cheque Bounce', 'Number of Debit Transactions through Outward Cheque Bounce', 0,  0],
        'chq': ['Total Amount Debited through Cheque', 'Number of Debit Transactions through cheque', 0,  0],
        'upi': ['Total Amount Debited through UPI', 'Number of Debit Transactions through UPI', 0,  0],
        'auto_debit_payment': ['Total Amount of Auto-Debit Payments', 'Number of Auto-debited payments', 0,  0],
        'net_banking_transfer': ['Total Amount Debited through transfers', 'Number of Net Banking Debit Transactions', 0,  0],
        'payment_gateway_purchase': ['Total Amount of Payment Gateway Purchase', 'Number of Payment Gateway Purchase', 0,  0],
        'self_transfer': [ 'Total Amount of Debit Self-transfer',  'Number of Self-transfer Debit', 0, 0],
        'Others': ['', '', 0, 0]
        }

    credit_tags = {
        'international_transaction_arbitrage': ['Total Amount of International Credit', 'Number of International Credit transactions', 0,  0],
        'bank_interest': ['Total Amount of Bank Interest', 'Number of Bank Interest Credits', 0,  0],
        'refund': ['Total Amount of Refund', 'Number of Refund Transactions', 0,  0],
        'cash_deposit': ['Total Amount of Cash Deposited', 'Number of Cash Deposit Transactions', 0,  0],
        'upi': ['Total Amount Credited through UPI', 'Number of Credit Transactions through UPI', 0,  0],
        'net_banking_transfer': ['Total Amount Credited through transfers', 'Number of Net Banking Credit Transactions', 0,  0],
        'auto_debit_payment_bounce': ['Total Amount of Auto debit bounce', 'Number of Auto-Debit Bounces', 0,  0],
        'chq': ['Total Amount Credited through Cheque', 'Number of Credit Transactions through cheque', 0,  0],
        'investment_cashin': ['Total Amount of Investment Cash-ins', 'Number of Investment Cash-ins', 0,  0],
        'inward_cheque_bounce': ['Total Amount Credited through Inward Cheque Bounce', 'Number of Credit Transactions through Inward Cheque Bounce', 0,  0],
        'self_transfer': ['Total Amount of Credit Self-transfer',  'Number of Self-transfer Credit', 0, 0],
        'Others': ['', '', 0, 0]
        }

    return credit_tags, debit_tags


def salary_dict_func(month, salary_data):
    salary_data[month] = {
        'salary': 0,
        'Number of Salary Transactions': 0,
        '% Salary Spent on Bill Payment (7 days)': 0,
        '% Salary Spent Through Cash Withdrawal (7 days)': 0,
        '% Salary Spent through Debit Card (7 days)': 0,
        '% Salary Spent through Net Banking (7 days)': 0,
        '% Salary Spent through UPI (7 days)': 0,
    }
    return salary_data


salary_vars = {
    "salary": "total_amount_of_salary",
    "Number of Salary Transactions": "number_of_salary_transactions",
    "% Salary Spent on Bill Payment (7 days)": "perc_salary_spend_bill_payment",
    "% Salary Spent Through Cash Withdrawal (7 days)": "perc_salary_spend_cash_withdrawl",
    "% Salary Spent through Debit Card (7 days)": "perc_salary_spend_debit_card",
    "% Salary Spent through Net Banking (7 days)": "perc_salary_spend_net_banking_transfer",
    "% Salary Spent through UPI (7 days)": "perc_salary_spend_upi"
}


def add_loan_merchant(cre_deb, merchant_name, loan_dict):
    loan_dict[cre_deb][merchant_name] = {
        'Number of Transactions': 0,
        'Average Amount': 0,
        'Total Amount': 0,
        'Average Balance Before Transaction': 0,
        'First Date': None,
        'transactions': [],
        'Balance Before Transaction': 0
    }


xdays = 7
bad_flags = ['outward_cheque_bounce', 'auto_debit_payment_bounce', 'Outward Cheque Bounce', 'Auto Debit Payment Bounce']

# for NET OFF balances
def normalise_net_off_balances(net_off_balances):
    last_balance = None
    for month in net_off_balances["months_order"]:
        for i in range(len(net_off_balances[month])):
            if net_off_balances[month][i] == -1:
                net_off_balances[month][i] = last_balance
            else:
                last_balance = net_off_balances[month][i]
    
    return net_off_balances

def monthwise_calculator(transaction_data, monthwise_dict, Monthly_bal_transaction_notes, salary_data, salary_transactions, monthly_analysis, version='v1', country='IN'):
    credit_tags, debit_tags = cred_deb_tags()
    hash_to_index = {}
    hash_dict = {}
    if salary_transactions != []:
        for trans in salary_transactions:
            hash_dict[trans['hash']] = trans

    loan_dict = {'credit': {}, 'debit': {}}
    all_loan_transactions = {}

    # for NET OFF balances
    net_off_balances = {}
    net_off_balances["months_order"] = []
    net_off_balances["start_date"] = []
    sum_lender_transactions = 0

    # Preparing base monthwise_dict
    temp_transaction_data = sorted(transaction_data, key=lambda x: x['date'])
    months_order = []
    txn_start_date = datetime.strptime(temp_transaction_data[0]['date'].split()[0], '%Y-%m-%d').replace(day=1)
    txn_end_date = datetime.strptime(temp_transaction_data[-1]['date'].split()[0], '%Y-%m-%d').replace(day=1)
    while txn_start_date<=txn_end_date:
        month = txn_start_date.strftime('%b-%y')
        months_order.append(month)
        monthwise_dict = create_dict(monthwise_dict, month)
        Monthly_bal_transaction_notes[month] = {
            'debit': {'Balances': [], 'All transaction notes': [], 'amount': [], 'unclean_merchant': []},
            'credit': {'Balances': [], 'All transaction notes': [], 'amount': [], 'unclean_merchant': []}
            }
        salary_data = salary_dict_func(month, salary_data)
        txn_start_date += timedelta(days=32)
        txn_start_date = txn_start_date.replace(day=1)
    
    for i in range(len(transaction_data)):
        hash_to_index[transaction_data[i]['hash']] = i
        strDate = transaction_data[i]["date"]
        strDate = list(strDate.split())
        strDate = strDate[0]
        objDate = datetime.strptime(strDate, '%Y-%m-%d')
        # objDate = strDate
        transaction_data[i]["date"] = datetime.strftime(objDate, '%d-%b-%y')
        date = transaction_data[i]['date']
        objDate = datetime.strptime(date, '%d-%b-%y')
        month = transaction_data[i]['date'].split('-', 1)
        month = month[1]

        if transaction_data[i]['description'] == 'lender_transaction' and transaction_data[i]["transaction_channel"] != "salary":

            to_consider = True
            if transaction_data[i]['transaction_type']=='debit':
                for j in range(i+1, len(transaction_data)):
                    i_date = datetime.strptime(transaction_data[i]['date'], '%d-%b-%y')
                    j_date = datetime.strptime(transaction_data[j]['date'], '%Y-%m-%d %H:%M:%S')
                    if i_date < j_date:
                        # we don't need to need to on next date
                        break
                    if transaction_data[j]['transaction_type']=='credit' and transaction_data[j]['amount']==transaction_data[i]['amount'] and 'bounce' in transaction_data[j]['transaction_channel']:
                        to_consider = False
                        break
            if transaction_data[i]['transaction_type']=='credit' and 'bounce' in transaction_data[i]['transaction_channel']:
                for j in range(i-1, -1, -1):
                    i_date = datetime.strptime(transaction_data[i]['date'], '%d-%b-%y')
                    j_date = datetime.strptime(transaction_data[j]['date'], '%d-%b-%y')
                    if i_date > j_date:
                        # we don't need to need to on previous date
                        break
                    if transaction_data[j]['transaction_type']=='debit' and transaction_data[j]['amount']==transaction_data[i]['amount']:
                        to_consider = False
                        break
            # for NET OFF balances
            if to_consider:
                if transaction_data[i]["transaction_type"] == "credit":
                    # if here -> means the the transaction is actually a lender_transaction
                    sum_lender_transactions += transaction_data[i]["amount"]

                transaction_data[i] = check_loan(transaction_data[i], country)

                if transaction_data[i]['is_lender']:
                    # print(transaction_data[i]['merchant'])
                    if transaction_data[i]['merchant'] not in loan_dict[transaction_data[i]['transaction_type']]:
                        add_loan_merchant(transaction_data[i]['transaction_type'], transaction_data[i]['merchant'], loan_dict)

                        loan_dict[transaction_data[i]['transaction_type']][transaction_data[i]['merchant']]['First Date'] = transaction_data[i]['date']

                    loan_dict[transaction_data[i]['transaction_type']][transaction_data[i]['merchant']]['Number of Transactions'] += 1
                    loan_dict[transaction_data[i]['transaction_type']][transaction_data[i]['merchant']]['Total Amount'] += transaction_data[i]['amount']
                    if transaction_data[i]['transaction_type'] == 'debit':
                        loan_dict[transaction_data[i]['transaction_type']][transaction_data[i]['merchant']]['Balance Before Transaction'] += (transaction_data[i]['balance'] + transaction_data[i]['amount'])
                    else:
                        loan_dict[transaction_data[i]['transaction_type']][transaction_data[i]['merchant']]['Balance Before Transaction'] += (transaction_data[i]['balance'] - transaction_data[i]['amount'])
                    loan_dict[transaction_data[i]['transaction_type']][transaction_data[i]['merchant']]['transactions'].append(transaction_data[i])

                    if transaction_data[i]['merchant'] not in all_loan_transactions:
                        all_loan_transactions[transaction_data[i]['merchant']] = []
                    all_loan_transactions[transaction_data[i]['merchant']].append(transaction_data[i])

        if month not in net_off_balances["months_order"]:
            
            # for NET OFF balances
            net_off_balances["start_date"].append(transaction_data[i]["date"])
            net_off_balances["months_order"].append(month)
            net_off_balances[month] = [-1]*(monthrange(int(objDate.strftime("%Y")), int(objDate.strftime("%m")))[1])

        if transaction_data[i]['transaction_type'] == "credit":
            Monthly_bal_transaction_notes[month]['credit']['Balances'].append(transaction_data[i]['balance'])
            Monthly_bal_transaction_notes[month]['credit']['All transaction notes'].append(transaction_data[i]['transaction_note'])
            Monthly_bal_transaction_notes[month]['credit']['amount'].append(transaction_data[i]['amount'])
            Monthly_bal_transaction_notes[month]['credit']['unclean_merchant'].append(transaction_data[i]['unclean_merchant'])
            if transaction_data[i]['transaction_channel'] in credit_tags:
                credit_tags[transaction_data[i]['transaction_channel']][2] += transaction_data[i]['amount']
                credit_tags[transaction_data[i]['transaction_channel']][3] = max(transaction_data[i]['amount'], credit_tags[transaction_data[i]['transaction_channel']][3])
            else:
                credit_tags['Others'][2] += transaction_data[i]['amount']
                credit_tags['Others'][3] = max(transaction_data[i]['amount'], credit_tags['Others'][3])
        else:
            Monthly_bal_transaction_notes[month]['debit']['Balances'].append(transaction_data[i]['balance'])
            Monthly_bal_transaction_notes[month]['debit']['All transaction notes'].append(transaction_data[i]['transaction_note'])
            Monthly_bal_transaction_notes[month]['debit']['amount'].append(transaction_data[i]['amount'])
            Monthly_bal_transaction_notes[month]['debit']['unclean_merchant'].append(transaction_data[i]['unclean_merchant'])
            if transaction_data[i]['transaction_channel'] in debit_tags:
                debit_tags[transaction_data[i]['transaction_channel']][2] += transaction_data[i]['amount']
                debit_tags[transaction_data[i]['transaction_channel']][3] = max(transaction_data[i]['amount'], debit_tags[transaction_data[i]['transaction_channel']][3])
            else:
                debit_tags['Others'][2] += transaction_data[i]['amount']
                debit_tags['Others'][3] = max(transaction_data[i]['amount'], debit_tags['Others'][3])

        if salary_transactions != []:
            if transaction_data[i]['hash'] in hash_dict:
                transaction_tmp = transaction_data[i]
                transaction_tmp['salary_month'] = hash_dict[transaction_data[i]['hash']].get('salary_month', None)
                
                salary_data['transactions'].append(transaction_tmp)

        # for NET OFF balances
        net_off_balances[month][int(objDate.strftime("%d"))-1] = transaction_data[i]["balance"] - sum_lender_transactions


    # for NET OFF balances
    net_off_balances = normalise_net_off_balances(net_off_balances)

    # Month Wise Dict from Monthly Analysis
    if monthly_analysis:
        for month in list(monthwise_dict.keys()):
            complete_month = datetime.strptime(month, "%b-%y").strftime("%b-%Y")
            for ma_field, field_mapping in get_monthly_analysis_mapping(version).items():   
                monthwise_dict[month][ma_field] = monthly_analysis.get(field_mapping, {}).get(complete_month, 0)
        
            # Salary Data from Monthly Analysis
            if month in salary_data:
                for salary_field, salary_field_mapping in salary_vars.items():
                    salary_data[month][salary_field] = monthly_analysis.get(salary_field_mapping, {}).get(complete_month, 0)
    
    return monthwise_dict, Monthly_bal_transaction_notes, salary_data, credit_tags, debit_tags, loan_dict, all_loan_transactions, hash_to_index, net_off_balances


def analysis_func(workbook, transaction_data, personal_data, salary_transactions, monthly_analysis, EOD_balances, unadjusted_eod_balances, is_vertical=True, workbook_num='', version='v1', vdict={}, country='IN', attempt_types=[], predictors={}):
    overview_dict = {'Summary': {}, 'Monthwise Details': {}}
    overview_metrics = {}
    monthwise_dict = {}
    Monthly_bal_transaction_notes = {}
    salary_data = {'transactions': []}
    local_fields_list = list(get_monthly_analysis_mapping(version).keys())

    txn_from_date = datetime.strptime(transaction_data[0].get('date'),'%Y-%m-%d %H:%M:%S').strftime('%d-%b-%y')
    txn_to_date = datetime.strptime(transaction_data[-1].get('date'), '%Y-%m-%d %H:%M:%S').strftime('%d-%b-%y')
    personal_data['txn_from_date'] = txn_from_date
    personal_data['txn_to_date'] = txn_to_date
    personal_data['attempt_type'] = " ,".join(attempt_types)
 
    analysis_sheet_name = 'Overview'+workbook_num
    if version=='v5':
        analysis_sheet_name = 'Analysis'
    elif version=='v6':
        analysis_sheet_name = 'Statement'+workbook_num
    analysis_worksheet = workbook.add_worksheet(analysis_sheet_name)

    primary_heading = workbook.add_format(
        {'font_color': '#000000', 'bg_color': '#FFFFFF', 'valign': 'vcenter', 'border': 1, 'font_size': 14})
    green_heading = workbook.add_format(
        {'font_color': '#000000', 'bg_color': '#e2efd9', 'valign': 'vcenter', 'border': 1, 'font_size': 9, 'text_wrap': True})
    light_blue_cell = workbook.add_format(
        {'font_color': '#000000', 'bg_color': '#F1f1f1', 'valign': 'vcenter', 'border': 1, 'font_size': 9})
    text_body_cell = workbook.add_format(
        {'font_color': '#000000', 'bg_color': '#F1f1f1', 'valign': 'vcenter', 'border': 1, 'font_size': 9, 'text_wrap': True, 'num_format': '#,##0.00'})
    horizontal_heading = workbook.add_format(
        {'font_color': '#FFFFFF', 'bg_color': '#002060', 'valign': 'vcenter', 'border': 1, 'font_size': 9, 'align': 'center', "bold": True, 'text_wrap': True, 'num_format': 'mmm-yy'})
    account_blue_cell = workbook.add_format(
        {'font_color': '#000000', 'bg_color': '#F1f1f1', 'valign': 'vcenter', 'border': 1, 'font_size': 9, 'num_format': '0'})
    pivot_cell = workbook.add_format(
        {'font_color': '#FFFFFF', 'bg_color': '#808080', 'valign': 'vcenter', 'border': 1, 'font_size': 9, 'text_wrap': True})
    date_cell = workbook.add_format(
        {'font_color': '#000000', 'bg_color': '#F1f1f1', 'valign': 'vcenter', 'border': 1, 'font_size': 9, 'num_format': 'dd-mmm-yy'})
    negative_body_cell = workbook.add_format(
        {'font_color': '#fc3605', 'bg_color': '#F1f1f1', 'valign': 'vcenter', 'border': 1, 'font_size': 9, 'text_wrap': True, 'num_format': '#,##0.00'})

    entity_id = personal_data.get('entity_id', '')

    primary_heading_text = 'Summary' if version!='v5' else 'Personal Info'
    analysis_worksheet.write('A1', primary_heading_text, primary_heading)
    analysis_worksheet.set_column('A:A', 1)
    analysis_summary_info = [
        ['Name of the Account Holder',  'name'],
        ['Address', 'address'],
        ['IFSC', 'ifsc'],
        ['MICR', 'micr'],
        ['Account Opening Date ', 'account_opening_date'],
        ['Account Category', 'account_category'],
        ['Name of the Bank', 'bank'],
        ['Account Number', 'account_number'],
        ['Source', 'attempt_types'],
        ['BankConnect Score', 'bc_score'],
        ['OD Limit', 'od_limit'],
        ['Drawing Power', 'credit_limit'],
        ['Missing Data', 'missing_data']
    ]

    if version == 'v5':
        analysis_summary_info = V5_PERSONAL_DATA

    if country in ['ID']:
        analysis_summary_info = [entry for entry in analysis_summary_info if entry[0] not in ['BankConnect Score']]
    if version=='v3':
        analysis_summary_info.extend([
            ['Email ID', "email_id"],
            ['Mobile Number', "mobile_number"],
            ['PAN', "pan"],
            ['BankConnect Status', 'is_fraud'],
            ['Banking Period Start Date', 'from_date'],
            ['Banking Period End  Date', 'to_date'],
            ['Account ID', 'account_id']
        ])
    key_highlights = [
        '% Inward Cheque Bounce',
        '% Outward Cheque Bounce',
        'Avg. EOD Balance',
        'Avg. CC Utilization of last 3 Months',
        'Avg. CC Utilization of last 6 Months',
        'Peak CC Utilization of last 3 Months',
        'Peak CC Utilization of last 6 Months',
    ]

    row = 1
    col = 1

    row, col = add_analysis_summary_info_to_report(analysis_summary_info, personal_data, analysis_worksheet, row, col, green_heading, account_blue_cell, light_blue_cell, date_cell, text_body_cell, overview_dict, monthly_analysis, version, predictors)
    if version == 'v5':
        row+=2
        analysis_worksheet.write(row, col-1, 'Summary Info', primary_heading)
        row = row+1
        row, col = add_analysis_summary_info_to_report(V5_SUMMARY_INFO, personal_data, analysis_worksheet, row, col, green_heading, account_blue_cell, light_blue_cell, date_cell, text_body_cell, overview_dict, monthly_analysis, version, predictors)
        
    
    # adding exact transaction start and exact end date for not is_vertical
    if not is_vertical:
        analysis_worksheet.write(row, col, 'Exact Transaction Start Date', green_heading)
        temp_date = datetime.strptime(transaction_data[0]['date'], "%Y-%m-%d %H:%M:%S")
        analysis_worksheet.merge_range(row, col+1, row, col+3, temp_date.strftime("%d-%b-%Y"), light_blue_cell)
        row += 1
        overview_dict['Summary']['Exact Transaction Start Date'] = temp_date.strftime("%d-%b-%Y")
        
        temp_date = datetime.strptime(transaction_data[-1]['date'], "%Y-%m-%d %H:%M:%S")
        analysis_worksheet.write(row, col, 'Exact Transaction End Date', green_heading)
        analysis_worksheet.merge_range(row, col+1, row, col+3, temp_date.strftime("%d-%b-%Y"), light_blue_cell)
        row += 1
        overview_dict['Summary']['Exact Transaction End Date'] = temp_date.strftime("%d-%b-%Y")

    kh_row = row+2
    col = 0
    if version=='v3':
        row += 2
        analysis_worksheet.write(row, col, 'Key Highlights', primary_heading)
        row += len(key_highlights)+1
        
    row += 2
    analysis_worksheet.write(row, col, 'Monthwise Details', primary_heading)

    monthwise_dict, Monthly_bal_transaction_notes, \
        salary_data, credit_tags, debit_tags, \
            loan_dict, all_loan_transactions, \
                hash_to_index, net_off_balances = monthwise_calculator(transaction_data, monthwise_dict, \
                    Monthly_bal_transaction_notes, salary_data, salary_transactions, monthly_analysis, version, country)

    row += 1
    col += 1
    analysis_worksheet.write(row, col, "", pivot_cell)

    next_col = col
    next_row = row
    for heading in local_fields_list:
        if is_vertical:
            next_row +=1
        else:
            next_col += 1
        analysis_worksheet.write(next_row, next_col, heading, green_heading)
        overview_metrics[heading] = 0

    if unadjusted_eod_balances:
        EOD_balances_func(workbook, unadjusted_eod_balances, workbook_num, version=version)
        EOD_balances_func(workbook, EOD_balances, workbook_num, 'Adjusted Daily EOD Balances', version=version)
    else:
        EOD_balances_func(workbook, EOD_balances, workbook_num, version=version)

    next_row = row
    for month in EOD_balances["Months_order"]:
        if is_vertical:
            next_col = next_col + 1
            next_row = row
        else:
            next_col = col
            next_row = next_row + 1

        mon = datetime.strptime(month, '%b-%y')
        analysis_worksheet.write(next_row, next_col, mon, horizontal_heading)

        if len(EOD_balances[month]) > 0:
            all_balances = copy.copy(EOD_balances[month])
            all_balances = list(filter(lambda a: a != None, all_balances))
            all_balances.sort()
            monthwise_dict[month]['Average EOD Balance'] = round(sum(all_balances) / len(all_balances), 2)

        monthwise_dict[month]['Turnover Excluding Loan and Self Credits'] = monthwise_dict[month]['Total Amount of Credit Transactions'] - monthwise_dict[month]['Total Loan Credits Amount']-monthwise_dict[month]['Total Amount of Credit Self-transfer']

        monthwise_dict[month]['Total Credit without IW and Refund'] = monthwise_dict[month]['Total Amount of Credit Transactions'] - monthwise_dict[month]['Total Amount Credited through Inward Cheque Bounce'] - monthwise_dict[month]['Total Amount of Refund']
        monthwise_dict[month]['Total Debit without OW and Refund'] = monthwise_dict[month]['Total Amount of Debit Transactions'] - monthwise_dict[month]['Total Amount Debited through Outward Cheque Bounce'] - monthwise_dict[month]['Total Amount of Refund']

        if monthwise_dict[month]['Number of EMI Transactions'] > 0:
            monthwise_dict[month]['Average EMI Transactions Size'] =round(monthwise_dict[month]['Total Amount Debited through EMI']/monthwise_dict[month]['Number of EMI Transactions'], 2)

        monthwise_dict[month]['Net Cashflow'] = monthwise_dict[month]['Total Amount of Credit Transactions'] - monthwise_dict[month]['Total Amount of Debit Transactions']

        overview_dict['Monthwise Details'][month] = {}
        for field_name in local_fields_list:
            overview_dict['Monthwise Details'][month][field_name] = monthwise_dict[month][field_name]
            if is_vertical:
                next_row = next_row + 1
            else:
                next_col = next_col + 1
            
            try:
                if version=='v3' and monthwise_dict[month][field_name]<0: 
                    analysis_worksheet.write(next_row, next_col, monthwise_dict[month][field_name], negative_body_cell)
                else:
                    analysis_worksheet.write(next_row, next_col, monthwise_dict[month][field_name], text_body_cell)
                if version != 'v5':
                    overview_metrics[field_name] += monthwise_dict[month][field_name]
            except Exception as e:
                print("possible nan/infinity detected, writing None")
                analysis_worksheet.write(next_row, next_col, '', text_body_cell)
                overview_metrics[field_name] += 0
    
    if version=='v3':
        # Writing Total and Average
        next_row = row
        next_col = col + len(EOD_balances["Months_order"]) + 1
        analysis_worksheet.write(next_row, next_col, "Total", horizontal_heading)
        next_col += 1
        analysis_worksheet.write(next_row, next_col, "Average", horizontal_heading)
        next_col -= 1
        next_row += 1
        for _, total in overview_metrics.items():
            if round(total, 2)<0:
                analysis_worksheet.write(next_row, next_col, round(total, 2), negative_body_cell)
            else:
                analysis_worksheet.write(next_row, next_col, round(total, 2), text_body_cell)
            next_col += 1
            try:
                if round(total/len(EOD_balances["Months_order"]), 2)<0: 
                    analysis_worksheet.write(next_row, next_col, round(total/len(EOD_balances["Months_order"]), 2), negative_body_cell)
                else:
                    analysis_worksheet.write(next_row, next_col, round(total/len(EOD_balances["Months_order"]), 2), text_body_cell)
            except:
                analysis_worksheet.write(next_row, next_col, '', text_body_cell)
            next_col -= 1
            next_row += 1
        
        # Writing Key Highlights
        for key in key_highlights:
            kh_row+=1
            analysis_worksheet.write(kh_row, 1, key, green_heading)
            val = ""
            if key == '% Inward Cheque Bounce':
                try:
                    val = round((overview_metrics['Total No. of Inward Cheque Bounces']/overview_metrics['Total No. of Cheque Issues'])*100, 2)
                except:
                    val = ""
            if key == '% Outward Cheque Bounce':
                try:
                    val = round((overview_metrics['Total No. of Outward Cheque Bounces']/overview_metrics['Total No. of Cheque Deposits'])*100, 2)
                except:
                    val = ""
            if key == 'Avg. EOD Balance':
                try:
                    val = overview_metrics['Average EOD Balance']/len(EOD_balances["Months_order"])
                except:
                    val = ""
            if key in ["Avg. CC Utilization of last 3 Months", "Avg. CC Utilization of last 6 Months", "Peak CC Utilization of last 3 Months", "Peak CC Utilization of last 6 Months"]:
                val = vdict.get(key, "")
            if val and val<0:
                analysis_worksheet.write(kh_row, 2, val, negative_body_cell)
            else:
                analysis_worksheet.write(kh_row, 2, val, text_body_cell)

    analysis_worksheet.set_column('B:B', 50)

    if not is_vertical:
        next_col = col
        next_row = row + len(EOD_balances["Months_order"])
        next_row = next_row + 1
        analysis_worksheet.write(next_row, next_col, 'Sum', horizontal_heading)

        # next_row, next_col
        # is row & col value of 'Sum' cell
        # move the cursor to first value of firs trans
        new_row_for_sum = next_row - len(EOD_balances["Months_order"])
        new_col_for_sum = next_col - len(EOD_balances["Months_order"])
        for field_name in local_fields_list:
            if is_vertical:
                next_row = next_row + 1
                starting_cell = xl_rowcol_to_cell(next_row, new_col_for_sum)
                end_cell = xl_rowcol_to_cell(next_row, len(EOD_balances["Months_order"]) + 1)
                cell_value = '=SUM({}:{})'.format(starting_cell, end_cell)
            else:
                next_col = next_col + 1
                starting_cell = xl_rowcol_to_cell(new_row_for_sum, next_col)
                end_cell = xl_rowcol_to_cell(new_row_for_sum + len(EOD_balances["Months_order"]) - 1, next_col)
                cell_value = '=SUM({}:{})'.format(starting_cell, end_cell)

            analysis_worksheet.write(next_row, next_col, cell_value, text_body_cell)
    
    if version=='v5':
        next_row+=3
        analysis_worksheet.write(next_row, 0, 'Overall Summary', primary_heading)
        next_row+=1

        for x,y in OVERALL_SUMMARY_DICT_V5.items():
            value = str(predictors.get(y, 0.0))
            analysis_worksheet.write(next_row, 1, x, green_heading)
            analysis_worksheet.write(next_row, 2, value, text_body_cell )
            next_row+=1

    return Monthly_bal_transaction_notes, salary_data, \
            credit_tags, debit_tags,  loan_dict, \
                all_loan_transactions, monthwise_dict, hash_to_index, net_off_balances, overview_dict


def derived_analysis(workbook, version, EOD_balances):
    derived_analysis_sheet_name = 'Derived Analysis'
    derived_analysis_worksheet = workbook.add_worksheet(derived_analysis_sheet_name)
    primary_heading_text = 'Monthwise Details'
    report_styles = months_formats_func(workbook)


    derived_analysis_worksheet.write('A1', primary_heading_text, report_styles.get('primary_heading'))
    derived_analysis_worksheet.set_column('A:A', 1)
    local_fields_list = list(get_monthly_analysis_mapping(version).keys())

    derived_analysis_worksheet.write(1, 1, "", report_styles.get('pivot_cell'))

    next_row = 1
    next_col = 1
    derived_analysis_values = {}
    for heading in local_fields_list:
        next_row += 1
        derived_analysis_worksheet.write(next_row, next_col, heading, report_styles.get('green_heading'))
        derived_analysis_values[heading] = 0
    
    for month in EOD_balances['Months_order']:
        next_col+=1
        mon = datetime.strptime(month, '%b-%y')
        derived_analysis_worksheet.write(1, next_col, mon, report_styles.get('date_horizontal_heading_cell'))
    
    derived_analysis_worksheet.set_column('B:B', 50)

def monthwise_details(workbook, version, enriched_eod_balances, monthly_analysis):
    monthwise_list = monthwise_details_dict()
    worksheet = workbook.add_worksheet('Monthwise Details')
    months_formats = months_formats_func(workbook)
    txn_formats = transaction_format_func(workbook)

    worksheet.write('A1', 'Monthwise Details', months_formats.get('primary_heading'))
    worksheet.set_column('A:A', 1)

    months_order = enriched_eod_balances.get('Months_order')

    worksheet.write(1,1,"", months_formats.get('pivot_cell'))
    row, col = 1, 2

    month_col_dict = {}
    for month in months_order:
        month_col_dict[month] = col
        worksheet.write(row, col, month, txn_formats.get('horizontal_heading_cell'))
        col+=1
    
    total_col, average_col = col, col+1
    worksheet.write(row,col,'Total',txn_formats.get('horizontal_heading_cell'))
    worksheet.write(row,col+1,'Average',txn_formats.get('horizontal_heading_cell'))
    
    monthwise_list_dict = {}
    row, col = 2, 1
    for i in range(len(monthwise_list)):
        label, value_key = monthwise_list[i][0], monthwise_list[i][1]
        if value_key not in monthwise_list_dict.keys():
            monthwise_list_dict[value_key] = [row]
        else:
            monthwise_list_dict[value_key].append(row)
        
        worksheet.write(row, col, label, months_formats.get('green_heading'))
        row+=1


    for key, row_indexes in monthwise_list_dict.items():
        for target_cell_row in row_indexes:
            total_amt = 0
            cnt_times_amt_present = 0
            for month in months_order:
                target_cell_col = month_col_dict.get(month)
                month_formatted = datetime.strptime(month, '%b-%y').strftime('%b-%Y')
                value = monthly_analysis.get(key, dict()).get(month_formatted, 0.0) if key not in ['total_turnover', 'perc_top_line'] else ""
                if value not in [None, ""]:
                    total_amt+=value 
                    cnt_times_amt_present+=1
                
                worksheet.write(target_cell_row, target_cell_col, value, months_formats.get('text_box_cell'))
            
            worksheet.write(target_cell_row, total_col, total_amt if key not in ['total_turnover', 'perc_top_line'] else "", months_formats.get('text_box_cell'))
            average_val = total_amt/cnt_times_amt_present if cnt_times_amt_present>0 and key not in ['total_turnover', 'perc_top_line'] else ""
            worksheet.write(target_cell_row, average_col, average_val, months_formats.get('text_box_cell'))
        
    row_to_merge = monthwise_list_dict['perc_top_line'][0]
    start_col, end_col = 2, 3+len(months_order)
    worksheet.merge_range(row_to_merge, start_col, row_to_merge, end_col, '(Total Credits/Total Turnover)*100', months_formats.get('text_box_cell'))
    

    worksheet.set_column('B:B', 35)


def monthwise_account_summary(workbook: xlsxwriter.Workbook, personal_data: dict, predictors: dict, EOD_balances: dict, sheet_name: str, sheet_heading: str) -> None:
    months_order = EOD_balances.get('Months_order')
    months_count = min(12, len(months_order))       ## Limiting this count as we get predictors for atmost 12 months.
    account_summary_sheet = workbook.add_worksheet(sheet_name)
    
    ## Cell format definitions.
    green_heading = workbook.add_format({'font_color': '#000000', 'bg_color': '#e2efd9', 'valign': 'vcenter', 'border': 1, 'font_size': 12, 'text_wrap': True, "bold": True})
    text_body_cell = workbook.add_format({'font_color': '#000000', 'bg_color': '#F1f1f1', 'valign': 'vcenter', 'border': 1, 'font_size': 12, 'text_wrap': True, 'num_format': '#,##0.00'})
    horizontal_heading = workbook.add_format({'font_color': '#FFFFFF', 'bg_color': '#002060', 'valign': 'vcenter', 'border': 1, 'font_size': 12, 'align': 'center', "bold": True, 'text_wrap': True, 'num_format': 'mmm-yy'})
    column_pivot_cell = workbook.add_format({'font_color': '#000000', 'bg_color': '#cfcfcf', 'valign': 'vcenter', 'border': 1, 'font_size': 12, 'align': 'center', 'text_wrap': True, "bold": True, 'num_format': '#,##0.00'})
    pivot_cell = workbook.add_format({'font_color': '#FFFFFF', 'bg_color': '#808080', 'valign': 'vcenter', 'border': 1, 'font_size': 12, 'align': 'center', 'text_wrap': True, 'bold': True})

    ## Setting column size to 16 for 0-11.
    account_summary_sheet.set_column(0, 11, 16)
    
    ## Inserting Basic personal info at top.
    analysis_summary_info = {
        "A/c Holder Name": personal_data["name"],
        "Bank Name": personal_data["bank_name"],
        "A/c Number": personal_data["account_number"],
    }
    row, col = 0, 0
    for heading, value in analysis_summary_info.items():
        account_summary_sheet.write(row, col, heading, green_heading)
        account_summary_sheet.write_string(row, col+1, value, text_body_cell)
        row += 1
    
    ## Predictor keyword for Columns.
    predictors_mapping = {
        "Total Credits (Amt)": "credits_",
        "Total Debits (Amt)": "debits_",
        "Inward bounce/ECS bounce": "total_inward_payment_bounce_",
        "Loan Credit": "amt_loan_credits_",
        "Internal Transaction": "amt_self_transfer_credit_",
        "Acutal Credit": "income_",
        "Balance 5th": "balance_on_5th_",
        "Balance 10th": "balance_on_10th_",
        "Balance 15th": "balance_on_15th_",
        "Balance 25th": "balance_on_25th_",
        "Month Average": "month_average_",
    }
    
    predictors_summary = {}
    predictors_total = {}
    for predictor_key in predictors_mapping.keys():
        predictors_summary[predictor_key] = {}
        predictors_total[predictor_key] = None
    
    ## Calculating values for `Month Average` column.
    predictors_copy = copy.deepcopy(predictors)
    for month_num in range(months_count):
        eod_bal_days = [5, 10, 15, 25]
        eod_bal_sum = None
        for eod_bal_day in eod_bal_days:
            if not predictors_copy[f"balance_on_{eod_bal_day}th_{month_num}"]:
                continue
            if eod_bal_sum is None:
                eod_bal_sum = 0
            eod_bal_sum += predictors_copy[f"balance_on_{eod_bal_day}th_{month_num}"]
        if eod_bal_sum is None:
            predictors_copy[f"month_average_{month_num}"] = None
            continue
        predictors_copy[f"month_average_{month_num}"] = round(eod_bal_sum/(len(eod_bal_days)), 2)
    for month_num in range(months_count):
        for key in predictors_summary:
            predictors_summary[key][months_order[month_num]] = predictors_copy[f"{predictors_mapping[key]}{month_num}"]
    
    ## Building table column name row.
    account_summary_sheet.write(row, col, "Month", pivot_cell)
    new_col = 1
    for key in predictors_summary:
        account_summary_sheet.write(row, new_col, key, column_pivot_cell)
        new_col += 1
    
    ## Putting predictor values in table accross each month row.
    row += 1
    for month_num in range(months_count):
        account_summary_sheet.write(row, 0, months_order[month_num], horizontal_heading)
        new_col = 1
        for key in predictors_summary:
            account_summary_sheet.write(row, new_col, predictors_summary[key][months_order[month_num]], text_body_cell)
            new_col += 1
            ## Predictors Total Calculation
            if predictors_summary[key][months_order[month_num]] is None:
                continue
            if predictors_total[key] is None:
                predictors_total[key] = 0.0
            predictors_total[key] += predictors_summary[key][months_order[month_num]]
        row += 1
    if predictors_total.get("Month Average"):
        predictors_total["Month Average"] = round(predictors_total["Month Average"]/months_count, 2)

    ## Setting Predictors Total in table
    account_summary_sheet.write(row, col, "Total", pivot_cell)
    new_col = 1
    for predictors_key in predictors_total:
        account_summary_sheet.write(row, new_col, predictors_total[predictors_key], column_pivot_cell)
        new_col += 1
    
    ## Put Total Average Credits in Sheet
    row += 1
    account_summary_sheet.write(row, 0, "Total Average Credits", pivot_cell)
    total_avg_credits = round(predictors_total["Total Credits (Amt)"]/months_count, 2)
    account_summary_sheet.write(row, 1, total_avg_credits, column_pivot_cell)
