import xlsxwriter
from datetime import datetime, timedelta, date
from dateutil.relativedelta import relativedelta
from copy import deepcopy
from library.excel_report.report_analysis import (
    analysis_func, 
    monthwise_details, 
    monthwise_account_summary,
)
from library.excel_report.report_transactions import xns_func, TransactionColumnsInXlsx, salary_transactions_sheet, high_credit_amounts, top_5_funds_monthwise, top_funds_globally, transactions_breakup, loan_profile_v5, bounced_emi_transactions, penal_charges_transactions, charges, loan_disbursal, abb_sheet, self_and_sister_xns, spend_analysis_sheet
from library.excel_report.report_funds import funds_func
from library.excel_report.report_salary_profile import salary_profile_func
from library.excel_report.report_credit_debit_profile import credit_debit_profile_func, credit_debit_profile_func_indonesia
from library.excel_report.report_loan_profile import loan_profile_func
from library.excel_report.report_predictors import predictors_func
from library.excel_report.report_frauds import frauds_func
from library.recurring_transaction import get_all_unclean_merchant_grouped_transaction
from library.excel_report.report_statement import statemenets_details
from collections import defaultdict

def trim_transactions_to_x_months(transactions, x):
    final_transactions = []
    month_set = set()
    transactions = transactions[::-1]
    for transaction in transactions:
        date = transaction.get("date")
        date = datetime.strptime(date, "%Y-%m-%d %H:%M:%S")
        if date.strftime("%b-%Y") in month_set:
            final_transactions.append(transaction)
        elif len(month_set)<x:
            month_set.add(date.strftime("%b-%Y"))
            final_transactions.append(transaction)
    return final_transactions[::-1]

def trim_eod_to_x_months(eod_balances, months_order):
    trimmed_eod_balances = {'Months_order': [], 'start_date': []}
    for month in eod_balances['Months_order']:
        if month in months_order:
            trimmed_eod_balances['Months_order'].append(month)
    for start_date in eod_balances['start_date']:
        month = datetime.strptime(start_date, "%d-%b-%y").strftime("%b-%y")
        if month in months_order:
            trimmed_eod_balances['start_date'].append(start_date)
    for month in eod_balances.keys():
        if month in ['Months_order', 'start_date']:
            continue
        if month in months_order:
            trimmed_eod_balances[month] = eod_balances[month]
    return trimmed_eod_balances

def create_xlsx_report(transaction_data, personal_data, output_file_name, salary_transactions, recurring_transactions, frauds_list, predictors, monthly_analysis, enriched_eod_balances, unadjusted_eod_balances, version="v1", country='IN', workbook=None, workbook_num='', account_statement_metadata = {}):
    temp_transaction_data = sorted(transaction_data, key=lambda x: x['date'])
    months_order = []
    txn_start_date = datetime.strptime(temp_transaction_data[0]['date'].split()[0], '%Y-%m-%d').replace(day=1)
    txn_end_date = datetime.strptime(temp_transaction_data[-1]['date'].split()[0], '%Y-%m-%d').replace(day=1)
    while txn_start_date<=txn_end_date:
        month = txn_start_date.strftime('%b-%y')
        months_order.append(month)
        txn_start_date += timedelta(days=32)
        txn_start_date = txn_start_date.replace(day=1)
    
    if workbook is None:
        workbook = xlsxwriter.Workbook(output_file_name, {'strings_to_numbers': True})

    data_in_vertical_format = True
    sheet_type = TransactionColumnsInXlsx.CREDIT_DEBIT_IN_ONE_ROW

    vdict = {}
    if version == 'v2':
        data_in_vertical_format = False
        sheet_type = TransactionColumnsInXlsx.CREDIT_DEBIT_IN_SEPERATE
        
        # if version is v2, trim transactions exactly to 12 months
        transaction_data = trim_transactions_to_x_months(transaction_data, 12)
        months_order = months_order[-12:]
        enriched_eod_balances = trim_eod_to_x_months(enriched_eod_balances, months_order)
    elif version == 'v3':
        personal_data.update({
            'email_id': None,
            'mobile_number': None,
            'pan': None,
            'is_fraud': 'FRAUD' if 'metadata' in [fraud_dict['fraud_category'] for fraud_dict in frauds_list] else 'VERIFIED',
            'from_date': transaction_data[0]['date'] if len(transaction_data) > 0 else '',
            'to_date': transaction_data[-1]['date'] if len(transaction_data) > 0 else '',
            'account_id': personal_data.get('account_id')
        })
        
        # Used in Key Highlights in Overview
        vdict['Avg. CC Utilization of last 3 Months'] = predictors.get('avg_od_perc_utilized_last_3_month')*100 if predictors.get('avg_od_perc_utilized_last_3_month') is not None else None
        vdict['Avg. CC Utilization of last 6 Months'] = predictors.get('avg_od_perc_utilized_last_6_month')*100 if predictors.get('avg_od_perc_utilized_last_6_month') is not None else None
        vdict['Peak CC Utilization of last 3 Months'] = predictors.get('max_od_perc_utilized_last_3_month')*100 if predictors.get('max_od_perc_utilized_last_3_month') is not None else None
        vdict['Peak CC Utilization of last 6 Months'] = predictors.get('max_od_perc_utilized_last_6_month')*100 if predictors.get('max_od_perc_utilized_last_6_month') is not None else None
        
        # Used in Obligation in Loan Profile
        vdict['Total No. of EMI/Loan Payment'] = monthly_analysis.get('cnt_emi_debit', {})
        vdict['Total EMI/Loan Payment of the Month'] = monthly_analysis.get('amt_emi_debit', {})
        vdict['CC Interest Amount'] = monthly_analysis.get('amt_ccod_interest', {})
        vdict['Total Obligation'] = {}
        for month in monthly_analysis['cnt_emi_debit']:
            vdict['Total Obligation'][month] = monthly_analysis.get('amt_emi_debit', {}).get(month, 0) +  monthly_analysis.get('amt_ccod_interest', {}).get(month, 0)
        vdict['Loan EMIs'] = monthly_analysis.get('loan_emi', {})

    # fetching grouped transactions based on unclean merchant in case of Indonesia
    if country == 'ID':
        credit_grouped_transactions, debit_grouped_transactions = get_all_unclean_merchant_grouped_transaction(transaction_data)
    # analysis_func creates below sheets
    # Overview, Daily EOD Balances
    attempt_types = list(set([values.get('attempt_type', '') for statement_id, values in account_statement_metadata.items()]))
    if attempt_types is None:
        attempt_types = []
    Monthly_bal_transaction_notes, salary_data, credit_tags, debit_tags, loan_dict, \
        all_loan_transactions, monthwise_dict_, \
            hash_to_index, net_off_balances_, overview_dict = analysis_func(workbook, transaction_data, personal_data, salary_transactions, monthly_analysis, enriched_eod_balances, unadjusted_eod_balances, data_in_vertical_format, workbook_num, version, vdict, country, attempt_types=attempt_types, predictors=predictors) 
        
    if version=='v5':
        salary_transactions_sheet(workbook, salary_transactions,version, personal_data)
        # derived_analysis(workbook, version, enriched_eod_balances)
        high_credit_amounts(workbook, version, transaction_data, personal_data)
        top_5_funds_monthwise(workbook, version, transaction_data, enriched_eod_balances, 'credit', 'Top 5 Funds Recieved', 'Top 5 Credits')
        top_5_funds_monthwise(workbook, version, transaction_data, enriched_eod_balances, 'debit', 'Top 5 Remittance', 'Top 5 Debits')
        top_funds_globally(workbook, version, transaction_data, 'credit', 'Top 10 Customers')
        transactions_breakup(workbook, version, transaction_data, enriched_eod_balances, 'credit', 'BreakUp-Income', 'Breakup of Incomes')
        transactions_breakup(workbook, version, transaction_data, enriched_eod_balances, 'debit', 'BreakUp-Expense', 'Breakup of Expenses')
        loan_profile_v5(workbook, version, transaction_data)
        bounced_emi_transactions(workbook, version, transaction_data, personal_data)
        penal_charges_transactions(workbook, version, transaction_data, personal_data)
        charges(workbook, version, transaction_data, personal_data)
        loan_disbursal(workbook, version, transaction_data, personal_data)
        abb_sheet(workbook, version, transaction_data, personal_data, monthly_analysis, predictors)
        self_and_sister_xns(workbook, version, transaction_data, personal_data)
        statemenets_details(workbook, version, personal_data, account_statement_metadata)
        monthwise_details(workbook, version, enriched_eod_balances, monthly_analysis)
    
    if version=='v8':
        spend_analysis_sheet(workbook, transaction_data, enriched_eod_balances)

    # Below sheet are created in 
    # Transactions and bounce transactions
    if version=='v4':
        sheet_type = TransactionColumnsInXlsx.SME_REPORT
        for transaction_index in range(len(transaction_data)):
            transaction_data[transaction_index]['bank_name'] = personal_data['bank_name']
            transaction_data[transaction_index]['account_number'] = personal_data['account_number']
    xns_func(workbook, transaction_data, personal_data, sheet_type, version, workbook_num)

    if version!='v5':
        funds_func(workbook, Monthly_bal_transaction_notes, months_order,  "Debits", "Credits", workbook_num)
        salary_profile_func(workbook, salary_data, months_order, personal_data["salary_confidence"], workbook_num)
        loan_profile_func(workbook, loan_dict, all_loan_transactions, workbook_num, version, vdict)
    # predictors_func(workbook, EOD_balances, monthwise_dict, transaction_data, personal_data, net_off_balances)
    predictors_func(workbook, predictors, workbook_num, version)
    date_format = '%d-%b-%y'
    start = datetime.strptime(transaction_data[0]['date'], date_format)
    end = datetime.strptime(transaction_data[-1]['date'], date_format)
    total_days = end - start
    total_days = int(total_days.days)

    if country == 'ID':
        credit_debit_profile_func_indonesia(workbook, total_days, 'Credit', credit_tags, credit_grouped_transactions, months_order, workbook_num)
        credit_debit_profile_func_indonesia(workbook, total_days, 'Debit', debit_tags, debit_grouped_transactions, months_order, workbook_num)
    else:
        credit_debit_profile_func(workbook, total_days, 'Credit', credit_tags, recurring_transactions.get("recurring_credit_transactions", {}), workbook_num, version, personal_data=personal_data)
        credit_debit_profile_func(workbook, total_days, 'Debit', debit_tags, recurring_transactions.get("recurring_debit_transactions", {}), workbook_num, version, personal_data=personal_data)
    
    if version == 'v7':
        monthwise_account_summary(workbook, personal_data, predictors, enriched_eod_balances, "Monthly Summary", "Monthly Summary")
    
    frauds_func(workbook, frauds_list, hash_to_index, transaction_data, version, personal_data, country, workbook_num)
    sheet_names = [
        'Overview',
        'Transactions',
        'Daily EOD Balances',
        'Top 5 Credits',
        'Top 5 Debits',
        'Bounce Transactions',
        'Salary Profile',
        'Loan Profile',
        'Credit Profile',
        'Debit Profile',
        'Predictors',
        'Frauds',
        'Frauds Transactions'
    ]
    if unadjusted_eod_balances:
        sheet_names.insert(3, 'Adjusted Daily EOD Balances')
    if version == 'v7':
        sheet_names.insert(4, "Monthly Summary")
    if version == 'v5':
        # Derived Analysis not added yet
        # Top 10 customers were twice in the sheet
        sheet_names = [
            'Analysis',
            'EOD Balances',
            'Xns',
            'Salary Xns',
            'High Credit Amounts > 50000',
            'Top 5 Funds Recieved',
            'Top 5 Remittance',
            'Top 10 Customers',
            'BreakUp-Income',
            'BreakUp-Expense',
            'Loan Track',
            'Bounced Xns',
            'Recurring Credits',
            'Recurring Debits',
            'Bounced EMI',
            'Penal Charges',
            'Charges',
            'Self & Sister XNS',
            'Loan Disbursal',
            'ABB',
            'Predictors',
            'Frauds',
            'Frauds Transactions',
            'Statement Details',
            'Monthwise Details'
        ]
        if unadjusted_eod_balances:
            sheet_names.insert(2, 'Adjusted EOD Balances')
    if version == 'v8':
        sheet_names.insert(10, 'Spend Chart')

    sheet_order = {}
    priority=1
    if not workbook_num:
        for sheet_name in sheet_names:
                sheet_order[sheet_name] = priority
                priority += 1
        if version == 'v2':
            sheet_order['Reversal Transactions'] = priority
            priority += 1
            sheet_order['Bank Charges'] = priority
            priority += 1
    else:
        if version=='v6':
            for index, name_of_sheet in enumerate(sheet_names):
                if name_of_sheet=='Overview':
                    sheet_names[index] = 'Statement'
                    break
            sheet_order['Statement Details'] = priority
            priority += 1
        sheet_order['Aggregated Overview'] = priority
        priority += 1
        if version=='v4':
            sheet_order['Aggregated Transactions'] = priority
            priority += 1
        for number in range(1, int(workbook_num)+1):
            for sheet_name in sheet_names:
                sheet_order[sheet_name+str(number)] = priority
                priority += 1
            if version == 'v2':
                sheet_order['Reversal Transactions'+str(number)] = priority
                priority += 1
                sheet_order['Bank Charges'+str(number)] = priority
                priority += 1
    # Protection of worksheets
    if version=='v3':
        options = {
            'objects': True,
            'scenarios': True,
            'format_cells': True,
            'format_columns': True,
            'format_rows': True,
            'insert_columns': False,
            'insert_rows': False,
            'insert_hyperlinks': False,
            'delete_columns': False,
            'delete_rows': False,
            'select_locked_cells': True,
            'sort': True,
            'autofilter': True,
            'pivot_tables': True,
            'select_unlocked_cells': True,
        }
        for workseet in workbook.worksheets():
            workseet.protect(options=options)

    workbook.worksheets_objs.sort(key=lambda x: sheet_order[x.name])
    if not workbook_num:
        workbook.close()
    else:
        return overview_dict

def generate_aggregated_overview(workbook, worksheet, overviews, is_veritical=True, version='v1', aggregated_eod_balances={}):
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
    negative_body_cell = workbook.add_format(
        {'font_color': '#fc3605', 'bg_color': '#F1f1f1', 'valign': 'vcenter', 'border': 1, 'font_size': 9, 'text_wrap': True, 'num_format': '#,##0.00'})

    worksheet.set_column('B:B', 50)
    worksheet.set_column('A:A', 1)

    row = 1
    col = 0
    worksheet.write(row, col, 'Monthwise Details', primary_heading)
    row += 1
    col += 1
    worksheet.write(row, col, "", pivot_cell)

    fields_list = list(overviews[0]['Monthwise Details'][list(overviews[0]['Monthwise Details'].keys())[0]].keys())
    if is_veritical:
        row += 1
    else:
        col += 1
    fields_dict = {}
    metrics = {}
    for heading in fields_list:
        if (heading=='Opening Balance' or heading =='Closing balance' or heading=='Median Balance' or heading=='Mode Balance' or 
            heading=='Maximum Balance' or heading=='Minimum Balance'):
            pass
        else:
            if 'min' in heading.lower():
                fields_dict[heading] = float('inf')
            elif 'max' in heading.lower():
                fields_dict[heading] = float('-inf')
            else:
                fields_dict[heading] = 0
            worksheet.write(row, col, heading, green_heading)
            if is_veritical:
                row += 1
            else:
                col += 1
            metrics[heading] = 0

    # Complete the range of months from different overview
    months_set = set()
    for overview in overviews:
        for month in list(overview['Monthwise Details'].keys()):          
            months_set.add(month)

    months = list(months_set)

    # Convert month strings to datetime objects
    months_dt = [datetime.strptime(month, "%b-%y") for month in months]

    # Determine the minimum and maximum month and complete the range
    min_month = min(months_dt)
    max_month = max(months_dt)

    overview_months_dt = []
    current_month = min_month
    while current_month <= max_month:
        overview_months_dt.append(current_month)
        current_month += relativedelta(months=1)

    overview_months = [month.strftime("%b-%y") for month in overview_months_dt]

    aggregated_field_values = {}
    for month in overview_months:
        aggregated_field_values[month] = deepcopy(fields_dict)

    for month in overview_months:
        for field in list(fields_dict.keys()):
            for overview in overviews:
                try:
                    metrics[field] += overview['Monthwise Details'][month][field]
                    if field in aggregated_eod_balances.keys():
                        aggregated_field_values[month][field] = aggregated_eod_balances[field][month]
                    elif 'min' in field.lower():
                        aggregated_field_values[month][field] = min(overview['Monthwise Details'][month][field], aggregated_field_values[month][field])
                    elif 'max' in field.lower():
                        aggregated_field_values[month][field] = max(overview['Monthwise Details'][month][field], aggregated_field_values[month][field])
                    else:
                        aggregated_field_values[month][field] += overview['Monthwise Details'][month][field]
                except:
                    metrics[field] += 0
                    aggregated_field_values[month][field] += 0
            if aggregated_field_values[month][field] in [float('inf'), float('-inf')]:
                aggregated_field_values[month][field] = 0

    if is_veritical:
        row -= len(fields_dict) + 1
    else:
        col -= len(fields_dict) + 1
    next_row = row
    next_col = col
    for month in overview_months:
        if is_veritical:
            next_col += 1
        else:
            next_row += 1
        mon = datetime.strptime(month, '%b-%y')
        worksheet.write(next_row, next_col, mon, horizontal_heading)
        r = next_row
        c = next_col
        for field in list(fields_dict.keys()):
            if is_veritical:
                r += 1
            else:
                c += 1
            try:
                if version=='v3' and aggregated_field_values[month][field]<0:
                    worksheet.write(r, c, aggregated_field_values[month][field], negative_body_cell)
                else:
                    worksheet.write(r, c, aggregated_field_values[month][field], text_body_cell)
            except Exception as e:
                print("possible nan/infinity detected, writing None")
                worksheet.write(r, c, '', text_body_cell)
                
    if version=='v3':
        next_row = row
        next_col = col + len(overview_months) + 1
        worksheet.write(next_row, next_col, "Total", horizontal_heading)
        next_col += 1
        worksheet.write(next_row, next_col, "Average", horizontal_heading)
        next_col -= 1
        next_row += 1
        for _, total in metrics.items():
            if round(total, 2)<0:
                worksheet.write(next_row, next_col, round(total, 2), negative_body_cell)
            else:
                worksheet.write(next_row, next_col, round(total, 2), text_body_cell)
            next_col += 1
            try:
                if round(total/len(overview_months), 2)<0:
                    worksheet.write(next_row, next_col, round(total/len(overview_months), 2), negative_body_cell)
                else:
                    worksheet.write(next_row, next_col, round(total/len(overview_months), 2), text_body_cell)
            except:
                worksheet.write(next_row, next_col, '', text_body_cell)
            next_col -= 1
            next_row += 1
            
# Helper function to generate month strings in the format 'MMM-YY'
def generate_month_range(start_month, end_month):
    start = datetime.strptime(start_month, '%b-%y')
    end = datetime.strptime(end_month, '%b-%y')
    month_range = []
    
    while start <= end:
        month_range.append(start.strftime('%b-%y'))
        start += timedelta(days=31)  # Move to the next month, handles month transition
        start = start.replace(day=1)  # Adjust to the first day of the next month
    
    return month_range

# Sorting function that converts month strings to datetime for sorting
def sort_month_key(month):
    return datetime.strptime(month, '%b-%y')

def get_number_of_days(month_year_str):
    date_obj = datetime.strptime(month_year_str, "%b-%y")
    
    year = date_obj.year
    month = date_obj.month
    
    next_month = month % 12 + 1
    next_month_year = year if next_month != 1 else year + 1
    last_day_of_month = date(next_month_year, next_month, 1) - timedelta(days=1)
    
    return last_day_of_month.day

def median(list_val:list):
    """
    median is used to calculate median of the list
    Parameters
    ----------
        - list_val : list
            - A list containing numbers
    Returns:
        result(float): A float number representing median
    """
    list_val_copy = list_val.copy()
    if list_val_copy:
        list_val_copy.sort()
        mid = len(list_val_copy) // 2
        result = (list_val_copy[mid] + list_val_copy[~mid]) / 2
    else:
        result = None
    return result

def get_eod_predictors(aggregated_balances):
    
    eod_predictors = {}

    for month, eod_dict in aggregated_balances.items():
        eod_predictors[month] = {
            "Average EOD Balance": round(sum(eod_dict.values())/len(eod_dict.values()), 2),
            "Median Balance": round(median(list(eod_dict.values())), 2),
            "Min EOD Balance": round(min(list(eod_dict.values())), 2),
            "Max EOD Balance": round(max(list(eod_dict.values())), 2)
        }

    eod_keys = ['Average EOD Balance', 'Min EOD Balance', 'Max EOD Balance', 'Median Balance']

    eod_predictors_final = {}
    for key in eod_keys:
        eod_predictors_final[key]={}
        for month_keys in eod_predictors:
            eod_predictors_final[key][month_keys] = eod_predictors[month_keys][key]
            
    return eod_predictors_final

def calculate_monthwise_aggregated_balance(accounts_dict):
    aggregated_balance = defaultdict(lambda: defaultdict(float))
    
    # Collect all months from all accounts
    all_months = set()
    for account_data in accounts_dict.values():
        all_months.update(account_data['Months_order'])
    
    # Determine the full month range
    all_months = sorted(all_months, key=sort_month_key)
    full_month_range = generate_month_range(all_months[0], all_months[-1])
    
    # Iterate over each account in the dictionary
    for account_id, account_data in accounts_dict.items():
        for month in full_month_range:
            daily_balances = account_data.get(month, [0] * get_number_of_days(month))  #Set default balance on each day to zero if month is missing
            
            # Iterate over each day in the month and add balances to the aggregated dictionary
            for day_index, balance in enumerate(daily_balances):
                if balance is not None:  # Skip None values
                    aggregated_balance[month][day_index + 1] += balance
    
    # Convert defaultdict to a regular dictionary, sorting months
    aggregated_balances = {month: dict(days) for month, days in sorted(aggregated_balance.items(), key=lambda x: sort_month_key(x[0]))}
    eod_predictors_final = get_eod_predictors(aggregated_balances)
    return eod_predictors_final