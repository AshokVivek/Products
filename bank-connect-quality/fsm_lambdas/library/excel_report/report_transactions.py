import xlsxwriter
from xlsxwriter.utility import xl_rowcol_to_cell
from datetime import datetime
from library.excel_report.report_formats import transaction_format_func, months_formats_func
from itertools import groupby
from library.helpers.constants import BOUNCE_SINGLE_CATEGORIES_CHARGES, BOUNCE_SINGLE_CATEGORIES_TRANSACTIONS, TRANSACTION_CHANNELS
from collections import defaultdict
from library.utils import check_date

transaction_sheet_heading = {
    'sl_no': { "cell_title": "Sl. No.",  "cell_width" : 5, "alignment": 'right'},
    'bank_name': { "cell_title": "Bank Name",  "cell_width" : 10, "alignment": 'right'},
    'account_number': { "cell_title": "Account Number",  "cell_width" : 15, "alignment": 'right'},
    'date': { "cell_title": "Date",  "cell_width" : 9, "alignment": 'left'},
    'cheque_no': { "cell_title": "Cheque No.",  "cell_width" : 9, "alignment": 'right'},
    'description': { "cell_title": "Description",  "cell_width" : 20, "alignment": 'right'},
    'amount': { "cell_title": "Amount",  "cell_width" : 10, "alignment": 'left'},
    'credit': { "cell_title": "Credit",  "cell_width" : 10, "alignment": 'left'},
    'debit': { "cell_title": "Debit",  "cell_width" : 10, "alignment": 'left'},
    'category': { "cell_title": "Category",  "cell_width" : 5, "alignment": 'right'},
    'balance': { "cell_title": "Balance",  "cell_width" : 12, "alignment": 'left'},
    'merchant_category': { "cell_title": "Merchant Category",  "cell_width" : 12, "alignment": 'right'},
    'transaction_channel': { "cell_title": "Transaction Channel",  "cell_width" : 17, "alignment": 'right'},
    'transaction_note': { "cell_title": "Transaction Note",  "cell_width" : 50, "alignment": 'right'},
    'txn_description': { "cell_title": "Description",  "cell_width" : 50, "alignment": 'right'},
    'transaction_type': { "cell_title": "Transaction Type",  "cell_width" : 15, "alignment": 'right'},
    'category': {'cell_title': "Category", 'cell_width': 12, 'alignment': 'left'},
    'salary_month': {'cell_title': "Salary Month", 'cell_width': 12, 'alignment': 'left'}
}


from enum import Enum

class TransactionColumnsInXlsx(str, Enum):
    CREDIT_DEBIT_IN_ONE_ROW = 'CREDIT_DEBIT_IN_ONE_ROW',
    CREDIT_DEBIT_IN_SEPERATE = 'CREDIT_DEBIT_IN_SEPERATE'
    SME_REPORT = 'SME_REPORT'

    @staticmethod
    def get_columns(key):
        if key == TransactionColumnsInXlsx.CREDIT_DEBIT_IN_SEPERATE:
            return ['sl_no', 'date', 'cheque_no', 'transaction_note', 'debit', 'credit', 'transaction_channel', 'balance', 'description', 'merchant_category', 'transaction_type']
        elif key == TransactionColumnsInXlsx.SME_REPORT:
            return ['sl_no', 'bank_name', 'account_number', 'date', 'cheque_no', 'transaction_note', 'amount', 'transaction_channel', 'balance', 'description', 'merchant_category', 'transaction_type']
        else:
            return ['sl_no', 'date', 'cheque_no', 'transaction_note', 'amount', 'transaction_channel', 'balance', 'description', 'merchant_category', 'transaction_type']

class SalaryTransactionColumnsInXlsx(str, Enum):
    CREDIT_DEBITE_IN_ONE_ROW = 'CREDIT_DEBIT_IN_ONE_ROW'

    @staticmethod
    def get_columns(key):
        if key == SalaryTransactionColumnsInXlsx.CREDIT_DEBITE_IN_ONE_ROW:
            return ['sl_no', 'date', 'cheque_no', 'txn_description', 'amount', 'category', 'balance', 'salary_month']
        return ['sl_no', 'date', 'cheque_no', 'txn_description', 'amount', 'category', 'balance', 'salary_month']

class SingleCategoryTransactionsColumns(str, Enum):
    CREDIT_DEBIT_IN_ONE_ROW = 'CREDIT_DEBIT_IN_ONE_ROW'
    CREDIT_DEBIT_IN_ONE_ROW_WITH_BANK_ACCOUNT = 'CREDIT_DEBIT_IN_ONE_ROW_WITH_BANK_ACCOUNT'
    CREDIT_DEBIT_IN_ONE_ROW_WITH_TYPE = 'CREDIT_DEBIT_IN_ONE_ROW_WITH_TYPE'

    @staticmethod
    def get_columns(key):
        if key == SingleCategoryTransactionsColumns.CREDIT_DEBIT_IN_ONE_ROW:
            return ['sl_no', 'date', 'cheque_no', 'txn_description', 'amount', 'category', 'balance']
        elif key == SingleCategoryTransactionsColumns.CREDIT_DEBIT_IN_ONE_ROW_WITH_BANK_ACCOUNT:
            return ['sl_no', 'bank_name', 'account_number', 'date', 'cheque_no', 'txn_description', 'amount', 'category', 'balance']
        elif key == SingleCategoryTransactionsColumns.CREDIT_DEBIT_IN_ONE_ROW_WITH_TYPE:
            return ['sl_no', 'date', 'cheque_no', 'txn_description', 'amount', 'category', 'balance', 'transaction_type']
        return ['sl_no', 'date', 'cheque_no', 'txn_description', 'amount', 'category', 'balance']


def write_transaction_column(work_sheet, row, transaction_data, transaction_formats, slNo=-1, sheet_type=TransactionColumnsInXlsx.CREDIT_DEBIT_IN_ONE_ROW):

    headings_key = TransactionColumnsInXlsx.get_columns(sheet_type)

    if slNo == -1:
        slNo = row

    for index, field in enumerate(headings_key):
        values = ' '
        if field in ['transaction_channel', 'transaction_type', 'merchant_category', 'description']:
            if field not in transaction_data:
                val = ""
            else:
                val = transaction_data[field]
            val = val.split('_')
            for i in val:
                if i == 'chq':
                    i = 'Cheque'
                elif i == 'withdrawl':
                    i = 'withdrawal'
                else:
                    pass
                values += i.capitalize() + ' '
        else:
            if field in transaction_data:
                values = transaction_data[field]
    
        if field == 'sl_no':
            values = str(slNo)
            if row % 2 == 0:
                cell_formats = transaction_formats['right_align_cyan_cell']
            else:
                cell_formats = transaction_formats['right_align_generic_cell']

        elif (field == 'amount' and transaction_data['transaction_type'] == 'debit') or (field == 'debit' and transaction_data['transaction_type'] == 'debit'):
            if isinstance(values,float) and values < 0:
                values = '{}'.format(transaction_data['amount'])
            else:
                values = '-{}'.format(transaction_data['amount'])

            if row % 2 == 0:
                cell_formats = transaction_formats['debit_cyan_cell']
            else:
                cell_formats = transaction_formats['debit_generic_cell']

        elif field == 'credit' and transaction_data['transaction_type'] == 'credit':
            values = str(transaction_data['amount'])
            if row % 2 == 0:
                cell_formats = transaction_formats['left_align_cyan_cell']
            else:
                cell_formats = transaction_formats['left_align_generic_cell']

        elif field == 'cheque_no' or field == 'category':
            values = ''
            if row % 2 == 0:
                cell_formats = transaction_formats['right_align_cyan_cell']
            else:
                cell_formats = transaction_formats['right_align_generic_cell']

        elif field == 'date':
            if type(transaction_data['date']) == str:
                try:
                    time_obj = transaction_data['date'].split(' ')
                    seconds = (datetime.strptime("01/01/1970 " + time_obj[1], "%m/%d/%Y %H:%M:%S") - datetime(1970, 1, 1)).total_seconds()
                    if seconds > 0:
                        values = datetime.strptime(transaction_data["date"], '%d-%b-%y %H:%M:%S')
                    else:
                        values = datetime.strptime(transaction_data["date"], '%d-%b-%y')
                except Exception as e:
                    values = datetime.strptime(transaction_data["date"], '%d-%b-%y')
            
            # This check is removed, to prevent converting date values to string
            # values = values.strftime("%d-%b-%y")

            if row % 2 == 0:
                cell_formats = transaction_formats['date_left_align_cyan_cell']
            else:
                cell_formats = transaction_formats['date_left_align_generic_cell']

        elif field in ['transaction_note', 'bank_name', 'account_number']:
            if row % 2 == 0:
                cell_formats = transaction_formats['right_align_cyan_cell']
            else:
                cell_formats = transaction_formats['right_align_generic_cell']

        else:
            if row % 2 == 0:
                cell_formats = transaction_formats['left_align_cyan_cell']
            else:
                cell_formats = transaction_formats['left_align_generic_cell']

        if field in ['amount', 'debit', 'credit', 'balance'] and sheet_type == TransactionColumnsInXlsx.CREDIT_DEBIT_IN_SEPERATE:
            # print("Trying to write the value as a number instead of string. Concerned field: {}, value: {}, type: {}".format(field, values, type(values)))
            try:
                values = float(values)
                work_sheet.write_number(row, index, values, cell_formats)
            except Exception as e:
                work_sheet.write_string(row, index, str(values), cell_formats)
        elif field=='account_number':
            work_sheet.write_string(row, index, values, cell_formats)
        else:
            try:
                work_sheet.write(row, index, values, cell_formats)
            except:
                work_sheet.write(row, index, str(values), cell_formats)    

def write_transaction_with_single_category(work_sheet, row, transaction_data, transaction_formats, headings_key, personal_data, slNo = -1, is_salary_transaction = False):

    if slNo == -1:
        slNo = row

    for index, field in enumerate(headings_key):
        if field == 'sl_no':
            values = str(slNo)
            if row % 2 == 0:
                cell_formats = transaction_formats['right_align_cyan_cell']
            else:
                cell_formats = transaction_formats['right_align_generic_cell']
        elif field == 'date':
            if is_salary_transaction:
                date_string = transaction_data[field]
                date_object = datetime.strptime(date_string, '%Y-%m-%d %H:%M:%S')
                values = date_object.strftime('%d-%b-%y')
            else:
                if type(transaction_data['date']) == str:
                    try:
                        time_obj = transaction_data['date'].split(' ')
                        seconds = (datetime.strptime("01/01/1970 " + time_obj[1], "%m/%d/%Y %H:%M:%S") - datetime(1970, 1, 1)).total_seconds()
                        if seconds > 0:
                            values = datetime.strptime(transaction_data["date"], '%d-%b-%y %H:%M:%S')
                        else:
                            values = datetime.strptime(transaction_data["date"], '%d-%b-%y')
                    except Exception as e:
                        values = datetime.strptime(transaction_data["date"], '%d-%b-%y')
                
            if row % 2 == 0:
                cell_formats = transaction_formats['date_left_align_cyan_cell']
            else:
                cell_formats = transaction_formats['date_left_align_generic_cell']
        elif field in ['amount']:
            values = float(transaction_data.get('amount'))

            if transaction_data.get('transaction_type') == 'debit':
                if values>0:
                    values = values *-1
                if row % 2 == 0:
                    cell_formats = transaction_formats['debit_cyan_cell']
                else:
                    cell_formats = transaction_formats['debit_generic_cell']
            else:
                if row % 2 == 0:
                    cell_formats = transaction_formats['left_align_cyan_cell']
                else:
                    cell_formats = transaction_formats['left_align_generic_cell']
            
        elif field in ['cheque_no']:
            values = str(transaction_data.get('chq_num',''))
            if row % 2 == 0:
                cell_formats = transaction_formats['right_align_cyan_cell']
            else:
                cell_formats = transaction_formats['right_align_generic_cell']
        elif field in ['account_number']:
            values = personal_data.get('account_number')
            if row % 2 == 0:
                cell_formats = transaction_formats['right_align_cyan_cell']
            else:
                cell_formats = transaction_formats['right_align_generic_cell']
        elif field in ['bank_name']:
            values = personal_data.get('bank')
            if row % 2 == 0:
                cell_formats = transaction_formats['right_align_cyan_cell']
            else:
                cell_formats = transaction_formats['right_align_generic_cell']
        elif field in ['txn_description']:
            values = transaction_data.get('transaction_note')
            if row % 2 == 0:
                cell_formats = transaction_formats['right_align_cyan_cell']
            else:
                cell_formats = transaction_formats['right_align_generic_cell']
        elif field in ['category']:
            if row % 2 == 0:
                cell_formats = transaction_formats['right_align_cyan_cell']
            else:
                cell_formats = transaction_formats['right_align_generic_cell']
        elif field in ['transaction_type']:
            values = transaction_data.get('transaction_type', '')
            if isinstance(values, str):
                values = values.capitalize()
            if row % 2 == 0:
                cell_formats = transaction_formats['right_align_cyan_cell']
            else:
                cell_formats = transaction_formats['right_align_generic_cell']
        else:
            if row % 2 == 0:
                cell_formats = transaction_formats['left_align_cyan_cell']
            else:
                cell_formats = transaction_formats['left_align_generic_cell']

        if field in ['sl_no','date', 'cheque_no','txn_description', 'transaction_type', 'amount', 'bank_name','account_number']:
            work_sheet.write(row, index, values, cell_formats)
        elif field in ['balance']:
            try:
                values = float(transaction_data[field])
                work_sheet.write_number(row, index, values, cell_formats)
            except Exception as e:
                work_sheet.write_string(row, index, str(transaction_data[field]), cell_formats)
        else:
            work_sheet.write(row, index, transaction_data[field], cell_formats)

def transaction_column_names(row, work_sheet, formats, sheet_type=TransactionColumnsInXlsx.CREDIT_DEBIT_IN_ONE_ROW):

    headings_key = TransactionColumnsInXlsx.get_columns(sheet_type)

    for index, field in enumerate(headings_key):
        cell = xl_rowcol_to_cell(0, index)
        s = '{}:{}'.format(cell[0], cell[0])
        work_sheet.set_column(s, transaction_sheet_heading[field]['cell_width'])
        work_sheet.write_string(row, index, transaction_sheet_heading[field]['cell_title'], formats['horizontal_heading_cell'])

def transaction_column_names_single_category(row, work_sheet, formats, headings_key):

    for index, field in enumerate(headings_key):
        cell = xl_rowcol_to_cell(0, index)
        s = '{}:{}'.format(cell[0], cell[0])
        work_sheet.set_column(s, transaction_sheet_heading[field]['cell_width'])
        work_sheet.write_string(row, index, transaction_sheet_heading[field]['cell_title'], formats['horizontal_heading_cell'])


def design_formats(workbook):
    horizontal_heading_cell = workbook.add_format(
        {'font_color': '#FFFFFF', 'bg_color': '#003366', 'valign': 'vcenter', 'border': 1, 'font_size': 10, 'bold': True, 'align': 'center', 'text_wrap': True})
    
    left_align_cyan_cell = workbook.add_format(
        {'font_color': '#000000', 'bg_color': '#CCFFFF', 'valign': 'vcenter', 'border': 1, 'font_size': 10, 'bold': True, 'align': 'left', 'text_wrap': True})
    left_align_generic_cell = workbook.add_format(
        {'font_color': '#000000',  'valign': 'vcenter', 'border': 1, 'font_size': 10, 'bold': True, 'align': 'left', 'text_wrap': True})
    
    right_align_cyan_cell = workbook.add_format(
        {'font_color': '#000000', 'bg_color': '#CCFFFF', 'valign': 'vcenter', 'border': 1, 'font_size': 10, 'bold': True, 'align': 'right', 'text_wrap': True})
    right_align_generic_cell = workbook.add_format(
        {'font_color': '#000000',  'valign': 'vcenter', 'border': 1, 'font_size': 10, 'bold': True, 'align': 'right', 'text_wrap': True})
    
    debit_generic_cell = workbook.add_format(
        {'font_color': '#fc3605',  'valign': 'vcenter', 'border': 1, 'font_size': 10,  'align': 'right', 'text_wrap': True})
    debit_cyan_cell = workbook.add_format(
        {'font_color': '#fc3605', 'bg_color': '#CCFFFF', 'valign': 'vcenter', 'border': 1, 'font_size': 10, 'align': 'right', 'text_wrap': True})

    return {'horizontal_heading_cell': horizontal_heading_cell, 'left_align_cyan_cell': left_align_cyan_cell,
            'left_align_generic_cell': left_align_generic_cell, 'right_align_cyan_cell': right_align_cyan_cell,
            'right_align_generic_cell': right_align_generic_cell, 'debit_generic_cell': debit_generic_cell, 
            'debit_cyan_cell': debit_cyan_cell}


def xns_func(workbook, transaction_data, personal_data, sheet_type=TransactionColumnsInXlsx.CREDIT_DEBIT_IN_ONE_ROW, version="v1", workbook_num=''):

    formats = transaction_format_func(workbook)
    transactions_worksheet_name = 'Transactions'+workbook_num if version!='v5' else 'Xns'+workbook_num
    xns_worksheet = workbook.add_worksheet(transactions_worksheet_name)

    bounded_sheet_name = 'Bounced Xns' + workbook_num if version == 'v5' else 'Bounce Transactions' + workbook_num
    bounced_penal_worksheet = workbook.add_worksheet(bounded_sheet_name)

    xns_worksheet.freeze_panes(1, 0)
    bounced_penal_worksheet.freeze_panes(1, 0)

    bounce_flags = ["auto_debit_payment_bounce", "outward_cheque_bounce", "inward_cheque_bounce", "chq_bounce_charge", "auto_debit_payment_bounce", "ach_bounce_charge", 'chq_bounce_insuff_funds']
    reversal_flags = [TRANSACTION_CHANNELS.REFUND, TRANSACTION_CHANNELS.REVERSAL]
    bank_charges_flags = ["bank_charge"]

    if version!='v5':
        transaction_column_names(0, xns_worksheet, formats, sheet_type=sheet_type)
        transaction_column_names(0, bounced_penal_worksheet, formats, sheet_type=sheet_type)
    else:
        transaction_column_names_single_category(0,xns_worksheet,formats, headings_key=SingleCategoryTransactionsColumns.get_columns(SingleCategoryTransactionsColumns.CREDIT_DEBIT_IN_ONE_ROW))
        transaction_column_names_single_category(0, bounced_penal_worksheet, formats, headings_key=SingleCategoryTransactionsColumns.get_columns(SingleCategoryTransactionsColumns.CREDIT_DEBIT_IN_ONE_ROW_WITH_BANK_ACCOUNT))

    row = 0
    bounce_row = 1

    if version == 'v2':
        reversal_penal_worksheet = workbook.add_worksheet('Reversal Transactions'+workbook_num)
        bank_charges_penal_worksheet = workbook.add_worksheet('Bank Charges'+workbook_num)

        reversal_penal_worksheet.freeze_panes(1, 0)
        bank_charges_penal_worksheet.freeze_panes(1, 0)

        transaction_column_names(0, reversal_penal_worksheet, formats, sheet_type=sheet_type)
        transaction_column_names(0, bank_charges_penal_worksheet, formats, sheet_type=sheet_type)
        
        reversal_row = 1
        bank_charges_row = 1

    for row in range(len(transaction_data)):
        if version=='v5':
            if transaction_data[row]['category'] in BOUNCE_SINGLE_CATEGORIES_TRANSACTIONS:
                write_transaction_with_single_category(bounced_penal_worksheet, bounce_row, transaction_data[row], formats, SingleCategoryTransactionsColumns.get_columns(SingleCategoryTransactionsColumns.CREDIT_DEBIT_IN_ONE_ROW_WITH_BANK_ACCOUNT), personal_data)
                bounce_row += 1
        else:
            if transaction_data[row]['transaction_channel'] in bounce_flags or transaction_data[row]['description'] in bounce_flags:
                write_transaction_column(bounced_penal_worksheet, bounce_row, transaction_data[row], formats, sheet_type=sheet_type)
                bounce_row += 1

        if version == 'v2':
            if transaction_data[row]['transaction_channel'] in reversal_flags:
                write_transaction_column(reversal_penal_worksheet, reversal_row, transaction_data[row], formats, sheet_type=sheet_type)
                reversal_row += 1

            if transaction_data[row]['transaction_channel'] in bank_charges_flags:
                write_transaction_column(bank_charges_penal_worksheet, bank_charges_row, transaction_data[row], formats, sheet_type=sheet_type)
                bank_charges_row += 1
        
        if version == 'v5':
            write_transaction_with_single_category(xns_worksheet, row+1, transaction_data[row], formats, SingleCategoryTransactionsColumns.get_columns(SingleCategoryTransactionsColumns.CREDIT_DEBIT_IN_ONE_ROW), personal_data)
        else:
            write_transaction_column(xns_worksheet, row+1, transaction_data[row], formats, sheet_type=sheet_type)

def generate_aggregated_transactions(workbook, xns_worksheet, transaction_data, sheet_type=TransactionColumnsInXlsx.SME_REPORT, version='v4'):
    # This is to generate a worksheet with aggregated transactions of all the accounts, used by Stride Ventures
    formats = transaction_format_func(workbook)
    xns_worksheet.freeze_panes(1, 0)
    transaction_column_names(0, xns_worksheet, formats, sheet_type=sheet_type)
    row = 0
    for row in range(len(transaction_data)):
        write_transaction_column(xns_worksheet, row+1, transaction_data[row], formats, sheet_type=sheet_type)

def salary_transactions_sheet(workbook, salary_transactions, version, personal_data):
    formats = transaction_format_func(workbook)

    salary_transactions_sheet_name = 'Salary Xns'
    salary_worksheet = workbook.add_worksheet(salary_transactions_sheet_name)

    transaction_column_names_single_category(0,salary_worksheet,formats, headings_key=SalaryTransactionColumnsInXlsx.get_columns(SalaryTransactionColumnsInXlsx.CREDIT_DEBITE_IN_ONE_ROW))

    for i in range(len(salary_transactions)):
        write_transaction_with_single_category(salary_worksheet, i+1, salary_transactions[i], formats, SalaryTransactionColumnsInXlsx.get_columns(SalaryTransactionColumnsInXlsx.CREDIT_DEBITE_IN_ONE_ROW), personal_data, i+1, is_salary_transaction=True)

def high_credit_amounts(workbook, version, transaction_data, personal_data):
    high_credit_transactions = list(filter(lambda x: x.get('transaction_type')=='credit' and x.get('amount')>50000, transaction_data))
    high_credit_transactions_sheet_name = 'High Credit Amounts > 50000'
    high_credit_transactions_sheet = workbook.add_worksheet(high_credit_transactions_sheet_name)
    formats = transaction_format_func(workbook)

    transaction_column_names_single_category(0,high_credit_transactions_sheet,formats, headings_key=SingleCategoryTransactionsColumns.get_columns(SingleCategoryTransactionsColumns.CREDIT_DEBIT_IN_ONE_ROW))

    for i in range(len(high_credit_transactions)):
        write_transaction_with_single_category(high_credit_transactions_sheet, i+1, high_credit_transactions[i], formats, SingleCategoryTransactionsColumns.get_columns(SingleCategoryTransactionsColumns.CREDIT_DEBIT_IN_ONE_ROW), personal_data, i+1)

# Can use this function for global grouping, in monthly we need month grouping then category_grouping
# In global grouping we only need grouping on category
def get_categories_each_month(transaction_data, transaction_type, needs_splitting=False, top_documents = 5):
    filtered_transactions = [transaction for transaction in transaction_data if transaction.get('transaction_type')==transaction_type]
    def extract_month(transaction):
        try:
            time_obj = transaction['date'].split(' ')
            seconds = (datetime.strptime("01/01/1970 " + time_obj[1], "%m/%d/%Y %H:%M:%S") - datetime(1970, 1, 1)).total_seconds()
            if seconds > 0:
                values = datetime.strptime(transaction["date"], '%d-%b-%y %H:%M:%S')
            else:
                values = datetime.strptime(transaction["date"], '%d-%b-%y')
        except Exception as e:
            values = datetime.strptime(transaction["date"], '%d-%b-%y')
        
        return datetime.strftime(values, '%b-%y')
    
    def category_group(transaction):
        return transaction.get('category')
    sorted_transactions = sorted(filtered_transactions, key=extract_month)
    grouped_transactions = groupby(sorted_transactions, key=extract_month)

    grouped_dict = {}
    for month, transactions in grouped_transactions:
        grouped_dict[month] = []
        month_transactions = list(transactions)
        month_transactions = sorted(month_transactions, key=category_group)
        month_transactions_grouped_on_category = groupby(month_transactions, key=category_group)
        for category, single_category_transactions in month_transactions_grouped_on_category:
            amounts = sum(single_category_transaction_item['amount'] for single_category_transaction_item in single_category_transactions)
            grouped_dict[month].append({'category':category,'amount':amounts})
        
        grouped_dict[month] = sorted(grouped_dict[month], key=lambda x:x.get('amount'), reverse=True)
        if needs_splitting:
            top_5_categories = grouped_dict[month][:top_documents]
            grouped_dict[month] = top_5_categories

    return grouped_dict

def top_5_funds_monthwise(workbook, version, transaction_data, EOD_balances, transaction_type, sheet_name, sheet_heading):
    grouped_dict = get_categories_each_month(transaction_data, transaction_type, needs_splitting=True)
    months_order = EOD_balances.get('Months_order')
    months_formats = months_formats_func(workbook)

    top_5_credit_sheet = workbook.add_worksheet(sheet_name)

    top_5_credit_sheet.write('A1', sheet_heading, months_formats.get('primary_heading'))
    top_5_credit_sheet.set_column('A:A', 1)

    current_row = 1
    for month in months_order:
        top_5_categories = grouped_dict.get(month, [])
        top_5_credit_sheet.merge_range(current_row,1,current_row,2, month, months_formats.get('date_horizontal_heading_cell'))
        top_5_credit_sheet.write(current_row+1, 1, 'Category', months_formats.get('pivot_cell'))
        top_5_credit_sheet.write(current_row+1, 2, 'Amount', months_formats.get('pivot_cell'))
        current_row+=2

        for i in range(len(top_5_categories)):
            top_5_credit_sheet.write(current_row, 1, top_5_categories[i].get('category'), months_formats.get('green_heading'))
            top_5_credit_sheet.write(current_row, 2, top_5_categories[i].get('amount'), months_formats.get('text_box_cell'))
            current_row+=1
    
    top_5_credit_sheet.set_column('B:B', 25)
    top_5_credit_sheet.set_column('C:C', 15)

def top_funds_globally(workbook, version, transaction_data, transaction_type, sheet_name):
    filtered_transactions = [transaction for transaction in transaction_data if transaction.get('transaction_type')==transaction_type]

    def group_transactions_by_category_helper(transaction):
        return transaction.get('category')
    
    sorted_transactions = sorted(filtered_transactions, key=lambda x:x.get('category'))
    grouped_transactions = groupby(sorted_transactions, key=group_transactions_by_category_helper)

    amounts_by_category = []
    for category, transactions in grouped_transactions:
        transactions = list(transactions)
        total_amount = 0
        for transaction in transactions:
            total_amount+=transaction.get('amount')
        
        amounts_by_category.append({
            'num_transactions': len(transactions),
            'category': category,
            'amount': total_amount
        })

    top_10_amounts = sorted(amounts_by_category, key=lambda x:x.get('amount'), reverse=True)[:10]

    top_10_sheet = workbook.add_worksheet(sheet_name)
    transaction_formats = transaction_format_func(workbook)

    column_names = ['S. No', 'Name of the party', 'No of transactions', 'Amount']
    row, col = 0, 0

    for column_name in column_names:
        top_10_sheet.write(row, col, column_name, transaction_formats.get('horizontal_heading_cell'))
        col+=1
    
    row+=1
    for top_amount_object in top_10_amounts:
        col=0
        for column_name in column_names:
            if column_name in ['S. No']:
                values = str(row)
                if row % 2 == 0:
                    cell_formats = transaction_formats['right_align_cyan_cell']
                else:
                    cell_formats = transaction_formats['right_align_generic_cell']
            if column_name == 'Name of the party':
                values = top_amount_object.get('category')
                if row % 2 == 0:
                    cell_formats = transaction_formats['right_align_cyan_cell']
                else:
                    cell_formats = transaction_formats['right_align_generic_cell']
            elif column_name == 'Amount':
                values = float(top_amount_object.get('amount'))
                if row % 2 == 0:
                    cell_formats = transaction_formats['left_align_cyan_cell']
                else:
                    cell_formats = transaction_formats['left_align_generic_cell']
            elif column_name == 'No of transactions':
                values = str(top_amount_object.get('num_transactions'))
                if row % 2 == 0:
                    cell_formats = transaction_formats['right_align_cyan_cell']
                else:
                    cell_formats = transaction_formats['right_align_generic_cell']

            
            top_10_sheet.write(row, col, values, cell_formats)
            col+=1
        
        row+=1

    top_10_sheet.set_column('A:D', 15)
    top_10_sheet.set_column('B:B', 35)

def fill_row_with_zeroes(worksheet, row, length, format):
    for i in range(length):
        worksheet.write(row, i+2, 0.0, format)

def transactions_breakup(workbook, version, transaction_data, enriched_eod_balances, transaction_type, sheet_name, sheet_heading):
    grouped_dict = get_categories_each_month(transaction_data, transaction_type)
    months_formats = months_formats_func(workbook)
    transaction_formants = transaction_format_func(workbook)

    all_categories_dict = {}
    months_order = enriched_eod_balances.get('Months_order')

    worksheet = workbook.add_worksheet(sheet_name)
    worksheet.write('A1', sheet_heading, months_formats.get('primary_heading'))
    worksheet.set_column('A:A', 1)

    headling_column = 1
    category_row = 2
    worksheet.write(1, headling_column, 'ITEM', transaction_formants.get('horizontal_heading_cell'))
    for month in months_order:
        headling_column+=1
        worksheet.write(1, headling_column, month, transaction_formants.get('horizontal_heading_cell'))

        month_transactions = grouped_dict.get(month, list())
        for month_transaction in month_transactions:
            transaction_category = month_transaction.get('category')
            if transaction_category not in all_categories_dict.keys():
                all_categories_dict[transaction_category] = category_row
                worksheet.write(category_row, 1, transaction_category, months_formats.get('green_heading'))
                fill_row_with_zeroes(worksheet, category_row, len(months_order), months_formats.get('text_box_cell'))
                category_row+=1
            
            worksheet.write(all_categories_dict[transaction_category], headling_column, month_transaction.get('amount'), months_formats.get('text_box_cell'))
    
    worksheet.set_column('B:B', 35)

def loan_profile_v5(workbook, version, transaction_data):
    worksheet = workbook.add_worksheet('Loan Track')
    months_formats = months_formats_func(workbook)
    transaction_formats = transaction_format_func(workbook)
    worksheet.write('A1', 'Loan Profile', months_formats.get('primary_heading'))
    worksheet.set_column('A:A', 1)

    filtered_transactions = [transaction for transaction in transaction_data if transaction.get('transaction_type')=='debit' and transaction.get('category') in ['Loan', 'EMI Payment']]

    column_names = ['Sl No.', 'Date', 'Narration', 'Amount']
    row, col = 1, 1

    for column_name in column_names:
        worksheet.write(row, col, column_name, transaction_formats.get('horizontal_heading_cell'))
        col+=1
    
    row,col = 2, 1
    for transaction in filtered_transactions:
        index = row - 1
        for column_name in column_names:
            if column_name == 'Sl No.':
                values = str(index)
                if index % 2 == 0:
                    cell_formats = transaction_formats['right_align_cyan_cell']
                else:
                    cell_formats = transaction_formats['right_align_generic_cell']
            elif column_name == 'Date':
                try:
                    time_obj = transaction['date'].split(' ')
                    seconds = (datetime.strptime("01/01/1970 " + time_obj[1], "%m/%d/%Y %H:%M:%S") - datetime(1970, 1, 1)).total_seconds()
                    if seconds > 0:
                        values = datetime.strptime(transaction["date"], '%d-%b-%y %H:%M:%S')
                    else:
                        values = datetime.strptime(transaction["date"], '%d-%b-%y')
                except Exception as e:
                    values = datetime.strptime(transaction["date"], '%d-%b-%y')
                
                if index % 2 == 0:
                    cell_formats = transaction_formats['date_left_align_cyan_cell']
                else:
                    cell_formats = transaction_formats['date_left_align_generic_cell']
            elif column_name == 'Narration':
                values = transaction.get('transaction_note')
                if index % 2 == 0:
                    cell_formats = transaction_formats['right_align_cyan_cell']
                else:
                    cell_formats = transaction_formats['right_align_generic_cell']
            elif column_name == 'Amount':
                values = float(transaction.get('amount'))
                if index % 2 == 0:
                    cell_formats = transaction_formats['left_align_cyan_cell']
                else:
                    cell_formats = transaction_formats['left_align_generic_cell']

            worksheet.write(row, col, values, cell_formats)
            col+=1
        
        col = 1
        row += 1
    
    worksheet.set_column('B:E', 15)
    worksheet.set_column('D:D', 45)

def round_to_100_percent(number_list, digit_after_decimal=2):
    """
    This function take a list of number and return a list of percentage,
    which represents the portion of each number in sum of all numbers
    """
    non_round_numbers = [x / float(sum(number_list)) * 100 * 10 ** digit_after_decimal for x in number_list]
    decimal_part_with_index = sorted([(index, non_round_numbers[index] % 1) for index in range(len(non_round_numbers))], key=lambda y: y[1], reverse=True)
    remainder = 100 * 10 ** digit_after_decimal - sum([int(x) for x in non_round_numbers])
    index = 0
    while remainder > 0:
        non_round_numbers[decimal_part_with_index[index][0]] += 1
        remainder -= 1
        index = (index + 1) % len(number_list)
    return [int(x) / float(10 ** digit_after_decimal) for x in non_round_numbers]


def get_spend_analysis_summary_and_counts(transaction_data: dict, EOD_balances: dict) -> tuple[dict, dict]:
    months_order = EOD_balances.get('Months_order')
    months_count = len(months_order)

    category_counts = defaultdict(float)
    expense_summary_dict = defaultdict(float)
    for transaction in transaction_data:
        if transaction.get('transaction_type') == 'debit':
            # consider only debit transactions
            merchant_category = transaction.get('merchant_category')
            transaction_channel = transaction.get('transaction_channel', 'Others')
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
            
            transaction_datetime = check_date(transaction['date'])
            transaction_month = transaction_datetime[0].strftime('%b-%y')

            assigned_merchant_category = merchant_category if merchant_category else transaction_channel
            category_counts[assigned_merchant_category] += transaction['amount']
            if transaction_month in months_order:
                if assigned_merchant_category not in expense_summary_dict:
                    expense_summary_dict[assigned_merchant_category] = dict()
                
                if transaction_month not in expense_summary_dict[assigned_merchant_category]:
                    expense_summary_dict[assigned_merchant_category][transaction_month] = 0.00
                
                expense_summary_dict[assigned_merchant_category][transaction_month] += transaction['amount']
    
    ## Fill all NAN values by 0.00
    expense_summary_dict["Total"] = {}
    for merchant_category in expense_summary_dict:
        for month in months_order:
            if month not in expense_summary_dict[merchant_category]:
                expense_summary_dict[merchant_category][month] = 0.00
    
    for month in months_order:
        expense_summary_dict["Total"][month] = sum(
            expense_summary_dict[merchant_category][month] for merchant_category in expense_summary_dict if merchant_category != "Total"
        )
    
    for merchant_category in expense_summary_dict:
        expense_summary_dict[merchant_category]["Total"] = sum(expense_summary_dict[merchant_category][month] for month in months_order)
        
        expense_summary_dict[merchant_category]["Average"] = 0.00
        if months_count:
            expense_summary_dict[merchant_category]["Average"] = expense_summary_dict[merchant_category]["Total"] / months_count
    
    expense_summary_dict["Total"]["Total"] = sum(expense_summary_dict["Total"][month] for month in months_order)

    expense_summary_dict["Total"]["Average"] = 0.00
    if months_count:
        expense_summary_dict["Total"]["Average"] = expense_summary_dict["Total"]["Total"] / months_count

    expense_summary_dict = {key.replace("_", " ").title(): value for key, value in expense_summary_dict.items()}

    category_list = []
    number_list = []
    for category, count in category_counts.items():
        category = category.replace("_", " ").title()
        category_list.append(category)
        number_list.append(count)
    categories = []

    if any(number_list):
        percentage_list = round_to_100_percent(number_list, 0)
        others_obj = None
        for index in range(0, len(category_list)):
            if percentage_list[index] > 0:
                if category_list[index] == "Others":
                    others_obj = {"category": "Others", "percentage": int(percentage_list[index])}
                else:
                    categories.append({"category": category_list[index], "percentage": int(percentage_list[index])})
        categories.sort(key=lambda item: item['percentage'], reverse=True)
        
        if others_obj:
            categories.append(others_obj)
    
    return categories, expense_summary_dict


def spend_analysis_sheet(workbook: xlsxwriter.Workbook, transaction_data: dict, EOD_balances: dict) -> None:
    months_order = EOD_balances.get('Months_order')
    categories, expense_summary_dict = get_spend_analysis_summary_and_counts(transaction_data, EOD_balances)

    ## Cell format definitions.
    primary_heading = workbook.add_format({'font_color': '#000000', 'bg_color': '#FFFFFF', 'valign': 'vcenter', 'border': 1, 'font_size': 14})
    green_heading = workbook.add_format({'font_color': '#000000', 'bg_color': '#e2efd9', 'valign': 'vcenter', 'border': 1, 'font_size': 12, 'text_wrap': True, "bold": True, 'num_format': '#,##0.00'})
    text_body_cell = workbook.add_format({'font_color': '#000000', 'bg_color': '#F1f1f1', 'valign': 'vcenter', 'border': 1, 'font_size': 12, 'text_wrap': True, 'num_format': '#,##0.00'})
    horizontal_heading = workbook.add_format({'font_color': '#FFFFFF', 'bg_color': '#002060', 'valign': 'vcenter', 'border': 1, 'font_size': 12, 'align': 'center', "bold": True, 'text_wrap': True, 'num_format': 'mmm-yy'})
    pivot_cell = workbook.add_format({'font_color': '#FFFFFF', 'bg_color': '#808080', 'valign': 'vcenter', 'border': 1, 'font_size': 12, 'align': 'center', 'text_wrap': True, 'bold': True})
    
    sheet_name = 'Spend Chart'
    worksheet = workbook.add_worksheet(sheet_name)

    ## Setting column size to 16 for 0-11.
    worksheet.set_column(0, 15, 16)
    
    row, col = 1, 0
    worksheet.write(row, col, "Monthly Expense Summary", primary_heading)
    
    ## Put horizontal row titles.
    row += 1
    worksheet.write(row, col, "Particulars", horizontal_heading)
    for i in range(len(months_order)):
        worksheet.write(row, i+1, months_order[i], horizontal_heading)
    worksheet.write(row, len(months_order)+1, "Total", horizontal_heading)
    worksheet.write(row, len(months_order)+2, "Average", horizontal_heading)
    
    ## Put Particulars and their corresponding values
    row += 1
    for merchant_category in expense_summary_dict:
        worksheet.write(row, 0, merchant_category, green_heading if merchant_category != "Total" else pivot_cell)
        for i in range(len(months_order)):
            worksheet.write(
                row, 
                i + 1, 
                expense_summary_dict[merchant_category][months_order[i]], 
                text_body_cell if merchant_category != "Total" else pivot_cell,
            )
        worksheet.write(
            row,
            len(months_order) + 1,
            expense_summary_dict[merchant_category]["Total"],
            text_body_cell if merchant_category != "Total" else pivot_cell,
        )
        worksheet.write(
            row,
            len(months_order) + 2,
            expense_summary_dict[merchant_category]["Average"],
            text_body_cell if merchant_category != "Total" else pivot_cell,
        )
        row += 1
    
    row += 4
    worksheet.write(row-1, col, "Expense Count", primary_heading)
    worksheet.write(f'A{row+1}', 'Category', horizontal_heading)
    worksheet.write(f'B{row+1}', 'Percentage', horizontal_heading)
    
    for i, item in enumerate(categories, start=1):
        worksheet.write(i+row, 0, item['category'], green_heading)
        worksheet.write(i+row, 1, item['percentage'], text_body_cell)

    chart = workbook.add_chart({'type': 'pie'})

    row += 1
    chart.add_series({
        'name': 'Category Percentages',
        'categories': [sheet_name, row, 0, len(categories)+row, 0],
        'values':  [sheet_name, row, 1, len(categories)+row, 1],
        'data_labels': {
            'value': True,         # Display percentage value,
            'category': True,
            'separator': "\n",     # Separate category name and value with a newline
            'leader_lines': True,  # Add leader lines
        },
    })

    chart.set_title({'name': 'Category Breakdown'})
    chart.set_legend({'position': 'right'})
    worksheet.insert_chart(f'D{row}', chart)


def bounced_emi_transactions(workbook, version, transactions_data, personal_data):
    bounced_emi_transactions = [transaction for transaction in transactions_data if transaction.get('description')=='lender_transaction' and (transaction.get('transaction_channel') in ['inward_cheque_bounce','auto_debit_payment_bounce'])]
    formats = transaction_format_func(workbook)

    worksheet = workbook.add_worksheet('Bounced EMI')
    transaction_column_names_single_category(0,worksheet,formats, headings_key=SingleCategoryTransactionsColumns.get_columns(SingleCategoryTransactionsColumns.CREDIT_DEBIT_IN_ONE_ROW))

    for i in range(len(bounced_emi_transactions)):
        write_transaction_with_single_category(worksheet, i+1, bounced_emi_transactions[i], formats, SingleCategoryTransactionsColumns.get_columns(SingleCategoryTransactionsColumns.CREDIT_DEBIT_IN_ONE_ROW), personal_data, i+1)

def penal_charges_transactions(workbook, version, transactions_data, personal_data):
    penal_charges = [transaction for transaction in transactions_data if transaction.get('category')=='Penal Charges']
    formats = transaction_format_func(workbook)

    worksheet = workbook.add_worksheet('Penal Charges')
    transaction_column_names_single_category(0,worksheet,formats, headings_key=SingleCategoryTransactionsColumns.get_columns(SingleCategoryTransactionsColumns.CREDIT_DEBIT_IN_ONE_ROW_WITH_BANK_ACCOUNT))

    for i in range(len(penal_charges)):
        write_transaction_with_single_category(worksheet, i+1, penal_charges[i], formats, SingleCategoryTransactionsColumns.get_columns(SingleCategoryTransactionsColumns.CREDIT_DEBIT_IN_ONE_ROW_WITH_BANK_ACCOUNT), personal_data, i+1)

# Bounced I/W Payment Charges
# Bounced O/W Payment Charges
# Right now this is not added as we do not support this yet
def charges(workbook, version, transactions_data, personal_data):
    charges_txn = [transaction for transaction in transactions_data if transaction.get('category') in BOUNCE_SINGLE_CATEGORIES_CHARGES]

    formats = transaction_format_func(workbook)

    worksheet = workbook.add_worksheet('Charges')
    transaction_column_names_single_category(0,worksheet,formats, headings_key=SingleCategoryTransactionsColumns.get_columns(SingleCategoryTransactionsColumns.CREDIT_DEBIT_IN_ONE_ROW))

    for i in range(len(charges_txn)):
        write_transaction_with_single_category(worksheet, i+1, charges_txn[i], formats, SingleCategoryTransactionsColumns.get_columns(SingleCategoryTransactionsColumns.CREDIT_DEBIT_IN_ONE_ROW), personal_data, i+1)

def loan_disbursal(workbook, version, transactions_data, personal_data):
    filtered_transactions = [transaction for transaction in transactions_data if transaction.get('transaction_type')=='credit' and transaction.get('category') in ['Loan']]

    formats = transaction_format_func(workbook)

    worksheet = workbook.add_worksheet('Loan Disbursal')
    transaction_column_names_single_category(0,worksheet,formats, headings_key=SingleCategoryTransactionsColumns.get_columns(SingleCategoryTransactionsColumns.CREDIT_DEBIT_IN_ONE_ROW))

    for i in range(len(filtered_transactions)):
        write_transaction_with_single_category(worksheet, i+1, filtered_transactions[i], formats, SingleCategoryTransactionsColumns.get_columns(SingleCategoryTransactionsColumns.CREDIT_DEBIT_IN_ONE_ROW), personal_data, i+1)

def abb_sheet(workbook, version, transactions_data, personal_data, monthly_analysis, predictors):
    worksheet = workbook.add_worksheet('ABB')

    months_formats = months_formats_func(workbook)
    worksheet.write('A1', 'Personal Info', months_formats.get('primary_heading'))
    worksheet.set_column('A:A', 1)

    keys = [
        'Bank Name',
        'Account number',
        'Average Bank Balance (Last 12 months)',
        'Average Bank Balance (Last 6 months)',
        'Average Bank Balance (Last 3 months)',
        'Average Banking credit (Last 6 months)',
        'Average Banking credit (Last 3 months)',
        'Average Banking  (Last 6 months) to EMI Ratio',
        'Avg Banking Credit (Last 6 months) to EMI Ratio',
        'Average Banking (Last 3 months) to EMI Ratio',
        'Avg Banking Credit (Last 3 months) to EMI Ratio'
    ]

    row, col = 1, 1
    for key in keys:
        worksheet.write(row, col, key, months_formats.get('green_heading'))
        row+=1
    
    values = [
        personal_data.get('bank_name'),
        str(personal_data.get('account_number')),
        str(predictors.get('avg_abb_12fullmonths', 0.0)),
        str(predictors.get('avg_abb_6fullmonths', 0.0)),
        str(predictors.get('avg_abb_3fullmonths', 0.0)),
        str(predictors.get('avg_credit_amt_last_6months', 0.0)),
        str(predictors.get('avg_credit_amt_last_3months', 0.0)),
        str(predictors.get('ratio_of_avg_daily_bank_balance_last_6month_to_emi_amt_debit_last_6_month', 0.0)),
        str(predictors.get('ratio_of_avg_credit_amt_last_6month_to_emi_amt_debit_last_6_month', 0.0)),
        str(predictors.get('ratio_of_avg_daily_bank_balance_last_3month_to_emi_amt_debit_last_3_month', 0.0)),
        str(predictors.get('ratio_of_avg_credit_amt_last_3month_to_emi_amt_debit_last_3_month', 0.0))
    ]

    row, col = 1, 2
    for value in values:
        format = months_formats.get('text_box_cell')
        if row == 2:
            format = months_formats.get('account_number_cell')
        worksheet.write(row, col, value, format)
        row+=1
    
    worksheet.set_column('C:C', 15)
    worksheet.set_column('B:B', 40)

def self_and_sister_xns(workbook, version, transactions_data, personal_data):
    worksheet = workbook.add_worksheet('Self & Sister XNS')
    filtered_transactions = [transaction for transaction in transactions_data if transaction.get('category') == 'Self Transfer']
    formats = transaction_format_func(workbook)

    transaction_column_names_single_category(0,worksheet,formats, headings_key=SingleCategoryTransactionsColumns.get_columns(SingleCategoryTransactionsColumns.CREDIT_DEBIT_IN_ONE_ROW))

    for i in range(len(filtered_transactions)):
        write_transaction_with_single_category(worksheet, i+1, filtered_transactions[i], formats, SingleCategoryTransactionsColumns.get_columns(SingleCategoryTransactionsColumns.CREDIT_DEBIT_IN_ONE_ROW), personal_data, i+1)