import xlsxwriter
import datetime
from library.fsm_excel_report import get_month_wise_features, process_raw_data

import warnings
import pandas as pd


warnings.simplefilter(action = "ignore", category = FutureWarning)
pd.options.mode.chained_assignment = None

# NOT USED ANYMORE
def create_raw_excel_report(file_name, identity_dict, period_range, missing_periods, bank, txn_list):
    """
    file_name -- name of the file with .xlsx
    identity_dict -- dictionary containing identity information
    period_range -- string representing period for which data is available
    missing_periods -- string representing periods for which data is missing
    bank -- name of the bank
    txn_list -- list of dictionary containing transaction info

    returns file path after creating the file
    """
    write_path = '/tmp/' + file_name
    workbook = xlsxwriter.Workbook(write_path)

    primary_heading = workbook.add_format(
        {'font_color': '#ffffff', 'bg_color': '#423C8C', 'valign': 'vcenter', 'border': 1, 'font_size': 13})
    green_heading = workbook.add_format(
        {'font_color': '#000000', 'bg_color': '#24CA7A', 'valign': 'vcenter', 'border': 1, 'font_size': 13})
    generic_cell = workbook.add_format(
        {'valign': 'vcenter', 'border': 1, 'font_size': 13})
    amount_cell = workbook.add_format(
        {'valign': 'vcenter', 'border': 1, 'font_size': 13, 'num_format': '#,##0.00'})

    # Identity Worksheet
    identity_worksheet = workbook.add_worksheet('Identity')
    identity = (
        ['Name of the Account Holder', identity_dict.get('name', '')],
        ['Address of the Account Holder', identity_dict.get('address', '')],
        ['Account Number', identity_dict.get('account_number', '')],
        ['Name of the Bank', bank.upper()],
        ['IFSC Code', identity_dict.get('ifsc', '')],
        ['MICR Code', identity_dict.get('micr', '')],
        ['Statement Period', period_range],
        ['Missing Months', missing_periods]
    )
    row = 0
    col = 0
    for heading, value in identity:
        identity_worksheet.write(row, col, heading, primary_heading)
        identity_worksheet.write(row, col+1, value, generic_cell)
        identity_worksheet.set_row(row, 30)
        row += 1
    identity_worksheet.set_column(0, 0, 30)
    identity_worksheet.set_column(1, 1, 80)

    # Transaction Worksheet
    transaction_worksheet = workbook.add_worksheet('Transactions')
    row = 0
    # add headings
    col = 0
    headings = ["Date", "Debit", "Credit", "Balance", "Transaction Note",
                "Transaction Channel", "Description", "Merchant Category"]
    transaction_worksheet.set_column(0, 1, 15)
    transaction_worksheet.set_column(1, 5, 20)
    transaction_worksheet.set_column(5, 6, 80)
    transaction_worksheet.set_column(6, 9, 30)
    transaction_worksheet.write(row, col, 'SN', green_heading)
    for heading in headings:
        col += 1
        transaction_worksheet.write(row, col, heading, primary_heading)
    transaction_worksheet.set_row(row, 30)

    row += 1
    # add transactions
    for transaction in txn_list:
        transaction_worksheet.write(row, 0, row, green_heading)

        txn_date = datetime.datetime.strptime(
            transaction["date"], "%Y-%m-%d %H:%M:%S").strftime("%d-%b-%Y")

        transaction_worksheet.write(row, 1, txn_date, primary_heading)
        transaction_type = transaction["transaction_type"]
        if transaction_type == "debit":
            transaction_worksheet.write(
                row, 2, transaction["amount"], amount_cell)
            transaction_worksheet.write(row, 3, "", amount_cell)
        else:
            transaction_worksheet.write(row, 2, "", amount_cell)
            transaction_worksheet.write(
                row, 3, transaction["amount"], amount_cell)
        transaction_worksheet.write(
            row, 4, transaction["balance"], amount_cell)
        transaction_worksheet.write(
            row, 5, transaction["transaction_note"], generic_cell)
        transaction_worksheet.write(row, 6, transaction["transaction_channel"].replace(
            "_", " ").title(), generic_cell)
        transaction_worksheet.write(
            row, 7, transaction["description"].replace("_", " ").title(), generic_cell)
        transaction_worksheet.write(
            row, 8, transaction["merchant_category"].title(), generic_cell)
        transaction_worksheet.set_row(row, 30)
        row += 1

    # Monthly Analysis Worksheet
    monthly_analysis_worksheet = workbook.add_worksheet('Monthly Analysis')
    # define a dictionary with key and name pairs
    monthly_analysis_keys = {
        "amt_auto_debit_payment_bounce_credit": "Total Amount of Auto debit bounce",
        "amt_auto_debit_payment_debit": "Total Amount of Auto-Debit Payments",
        "amt_bank_charge_debit": "Total Amount of Bank Charges",
        "amt_bank_interest_credit": "Total Amount of Bank Interest",
        "amt_bill_payment_debit": "Total Amount of Bill Payments",
        "amt_cash_deposit_credit": "Total Amount of Cash Deposited",
        "amt_cash_withdrawl_debit": "Total Amount of Cash Withdrawal",
        "amt_chq_credit": "Total Amount Credited through Cheque",
        "amt_chq_debit": "Total Amount Debited through Cheque",
        "amt_credit": "Total Amount Credited",
        "amt_debit": "Total Amount Debited",
        "amt_debit_card_debit": "Total Amount Spend through Debit card",
        "amt_international_transaction_arbitrage_credit": "Total Amount of International Credit",
        "amt_international_transaction_arbitrage_debit": "Total Amount of International Debit",
        "amt_investment_cashin_credit": "Total Amount of Investment Cash-ins",
        "amt_net_banking_transfer_credit": "Total Amount Credited through transfers",
        "amt_net_banking_transfer_debit": "Total Amount Debited through transfers",
        "amt_inward_cheque_bounce_credit": "Total Amount Credited through Inward Cheque Bounce",
        "amt_outward_cheque_bounce_debit": "Total Amount Debited through Outward Cheque Bounce",
        "amt_payment_gateway_purchase_debit": "Total Amount of Payment Gateway Purchase",
        "amt_refund_credit": "Total Amount of Refund",
        "amt_upi_credit": "Total Amount Credited through UPI",
        "amt_upi_debit": "Total Amount Debited through UPI",
        "avg_bal": "Average Balance",
        "avg_credit_transaction_size": "Average Credit Transaction Size",
        "avg_debit_transaction_size": "Average Debit Transaction Size",
        "closing_balance": "Closing balance",
        "cnt_auto_debit_payment_bounce_credit": "Number of Auto-Debit Bounces",
        "cnt_auto_debit_payment_debit": "Number of Auto-debited payments",
        "cnt_bank_charge_debit": "Number of Bank Charge payments",
        "cnt_bank_interest_credit": "Number of Bank Interest Credits",
        "cnt_bill_payment_debit": "Number of Bill Payments",
        "cnt_cash_deposit_credit": "Number of Cash Deposit Transactions",
        "cnt_cash_withdrawl_debit": "Number of Cash Withdrawal Transactions",
        "cnt_chq_credit": "Number of Credit Transactions through cheque",
        "cnt_chq_debit": "Number of Debit Transactions through cheque",
        "cnt_credit": "Number of Credit Transactions",
        "cnt_debit": "Number of Debit Transactions",
        "cnt_debit_card_debit": "Number of Debit Card Transactions",
        "cnt_international_transaction_arbitrage_credit": "Number of International Credit transactions",
        "cnt_international_transaction_arbitrage_debit": "Number of International Debit transactions",
        "cnt_investment_cashin_credit": "Number of Investment Cash-ins",
        "cnt_net_banking_transfer_credit": "Number of Net Banking Credit Transactions",
        "cnt_net_banking_transfer_debit": "Number of Net Banking Debit Transactions",
        "cnt_inward_cheque_bounce_credit": "Number of Credit Transactions through Inward Cheque Bounce",
        "cnt_outward_cheque_bounce_debit": "Number of Debit Transactions through Outward Cheque Bounce",
        "cnt_payment_gateway_purchase_debit": "Number of Payment Gateway Purchase",
        "cnt_refund_credit": "Number of Refund Transactions",
        "cnt_transactions": "Number of Transactions",
        "cnt_upi_credit": "Number of Credit Transactions through UPI",
        "cnt_upi_debit": "Number of Debit Transactions through UPI",
        "max_bal": "Maximum Balance",
        "max_eod_balance": "Maximum EOD Balance",
        "median_balance": "Median Balance",
        "min_bal": "Minimum Balance",
        "min_eod_balance": "Minimum EOD Balance",
        "mode_balance": "Mode Balance",
        "net_cash_inflow": "Net Cashflow",
        "opening_balance": "Opening Balance",
        "number_of_salary_transactions": "Number of Salary Transactions",
        "total_amount_of_salary": "Total Amount of Salary",
        "perc_salary_spend_bill_payment": "% Salary Spent on Bill Payment (7 days)",
        "perc_salary_spend_cash_withdrawl": "% Salary Spent Through Cash Withdrawal (7 days)",
        "perc_salary_spend_debit_card": "% Salary Spent through Debit Card (7 days)",
        "perc_salary_spend_net_banking_transfer": "% Salary Spent through Net Banking (7 days)",
        "perc_salary_spend_upi": "% Salary Spent through UPI (7 days)"
    }
    row = 0

    # get values
    # first form a pandas dataframe from transactions
    data = pd.DataFrame(txn_list)
    # process raw data
    processed_data = process_raw_data(data)
    # get month wise features
    monthly_df, _ = get_month_wise_features(processed_data)
    # round all decimals to 2 decimal points
    monthly_df = monthly_df.round(2)

    # add headings
    total_col = 0
    monthly_analysis_worksheet.write(
        row, total_col, "Parameters", primary_heading)
    for month_text in monthly_df.index.values:
        total_col += 1
        monthly_analysis_worksheet.write(
            row, total_col, month_text, primary_heading)
    monthly_analysis_worksheet.set_row(row, 30)
    monthly_analysis_worksheet.set_column(0, 1, 50)
    monthly_analysis_worksheet.set_column(1, total_col, 30)

    # now add content
    for column_name in monthly_df:
        # ignore date_formatted key
        if column_name == "date_formatted":
            continue
        row += 1
        # check if parameter is amount
        is_amount = True
        if column_name.startswith("cnt") or column_name.startswith("number") or column_name.startswith("total") or column_name.startswith("perc"):
            is_amount = False
        # add parameter name
        monthly_analysis_worksheet.write(row, 0, monthly_analysis_keys.get(
            column_name, column_name), primary_heading)
        # add month wise values
        if is_amount:
            for col in range(1, total_col+1):
                monthly_analysis_worksheet.write(
                    row, col, monthly_df[column_name][col-1], amount_cell)
        else:
            for col in range(1, total_col+1):
                monthly_analysis_worksheet.write(
                    row, col, monthly_df[column_name][col-1], generic_cell)
        monthly_analysis_worksheet.set_row(row, 30)

    workbook.close()
    return write_path
