import datetime
import string
import numpy as np
import warnings
import pandas as pd
import polars as pl

from library import lender_list
from library import recurring_transaction
from library import salary


warnings.simplefilter(action = "ignore", category = FutureWarning)
pd.options.mode.chained_assignment = None


l = list(string.ascii_uppercase)
b = l + [x + y for x in l for y in l]

heading_options = {
    'width': 1500,
    'height': 60,
    'x_offset': 0,
    'y_offset': 0,
    'font': {'bold': True,
             'italic': True,
             'name': 'Arial',
             'color': 'white',
             'size': 18},
    'align': {'vertical': 'middle',
              'horizontal': 'center'},
    'fill': {'color': 'black'},
    'line': {'color': 'white'}
}


def process_raw_data(raw_data, col_date='date'):
    raw_data1 = raw_data.copy()
    if list(raw_data1[col_date])[0] < list(raw_data1[col_date])[-1]:
        raw_data1 = raw_data1[::-1]
        raw_data1 = raw_data1.reset_index(drop=1)
    raw_data1['date_formatted'] = raw_data1[col_date].apply(
        lambda x: pd.to_datetime(str(x)[:10], format='%Y-%m-%d'))
    raw_data1['date_stringtype'] = raw_data1[col_date].apply(
        lambda x: (str(x)[:10]))
    raw_data1['month_year'] = raw_data1['date_formatted'].apply(
        lambda x: x.strftime('%b-%Y'))
    raw_data1['transaction_channel'] = raw_data1['transaction_channel'].apply(
        lambda x: 'cheque' if x == 'chq' else x)
    # raw_data1['S. No.'] =raw_data1.index
    return raw_data1


# In[3]:

def get_basic_features(df):
    obj = dict()
    obj['Month Year'] = df['month_year'].min()
    obj['date_formatted'] = datetime.datetime(
        df['date_formatted'][0].year, df['date_formatted'][0].month, 1).date()

    obj['cnt_transactions'] = df.shape[0]
    obj['cnt_debit'] = df[df.transaction_type == 'debit'].shape[0]
    obj['cnt_credit'] = df[df.transaction_type == 'credit'].shape[0]

    if df['transaction_type'][df.shape[0] - 1] == 'debit':
        obj['opening_balance'] = df['balance'][df.shape[0] - 1] + \
            df['amount'][df.shape[0] - 1]
    elif df['transaction_type'][df.shape[0] - 1] == 'credit':
        obj['opening_balance'] = df['balance'][df.shape[0] - 1] - \
            df['amount'][df.shape[0] - 1]
    else:
        obj['opening_balance'] = df['balance'][df.shape[0] - 1]
    obj['amt_debit'] = df[df.transaction_type == 'debit']['amount'].sum()
    obj['amt_credit'] = df[df.transaction_type == 'credit']['amount'].sum()
    obj['closing_balance'] = df['balance'][0]
    obj['net_cash_inflow'] = obj['amt_credit'] - obj['amt_debit']

    obj['min_bal'] = min(df['balance'].min(), obj['opening_balance'])
    obj['max_bal'] = max(df['balance'].max(), obj['opening_balance'])

    obj['avg_bal'], obj['median_balance'], obj['mode_balance'] = get_daily_balance_mmm(
        df)

    obj['max_eod_balance'] = df.groupby(
        'date_formatted', sort=0).first()['balance'].max()
    obj['min_eod_balance'] = df.groupby(
        'date_formatted', sort=0).first()['balance'].min()

    obj['avg_credit_transaction_size'] = df[df.transaction_type ==
                                            'credit'].amount.mean()
    obj['avg_debit_transaction_size'] = df[df.transaction_type == 'debit'].amount.mean()

    return obj


def get_basic_features_part2(df):
    obj = {}
    debit_tags = ['international_transaction_arbitrage', 'bill_payment', 'cash_withdrawl', 'bank_charge', 'debit_card',
                  'outward_cheque_bounce', 'chq', 'upi', 'auto_debit_payment', 'net_banking_transfer',
                  'payment_gateway_purchase']
    for each in debit_tags:
        obj['amt_' + each + '_debit'] = df[
            (df.transaction_channel == each) & (df.transaction_type == 'debit')].amount.sum()
        obj['cnt_' + each + '_debit'] = df[
            (df.transaction_channel == each) & (df.transaction_type == 'debit')].amount.count()
    credit_tags = ['international_transaction_arbitrage', 'bank_interest', 'refund', 'cash_deposit', 'upi',
                   'net_banking_transfer', 'auto_debit_payment_bounce', 'chq', 'investment_cashin','inward_cheque_bounce']
    for each in credit_tags:
        obj['amt_' + each + '_credit'] = df[
            (df.transaction_channel == each) & (df.transaction_type == 'credit')].amount.sum()
        obj['cnt_' + each + '_credit'] = df[
            (df.transaction_channel == each) & (df.transaction_type == 'credit')].amount.count()
    return obj


# In[4]:


def get_salary_parameters(processed_df, xdays):
    import datetime
    processed_df = processed_df[::-1]

    returned_salary_transactions = salary.get_salary_transactions(processed_df.to_dict('records')).get(
        'salary_transactions', list())

    if len(returned_salary_transactions) == 0:
        salary_transactions = pd.DataFrame(
            columns=['amount', 'hash', 'date_formatted', 'transaction_type', 'transaction_channel', 'balance'])
    else:
        salary_transactions = pd.DataFrame(returned_salary_transactions)

    salary_transactions['amount_1'] = salary_transactions['amount']
    processed_df = processed_df.merge(
        salary_transactions[['hash', 'amount_1']], how='left', on='hash')

    salary_dates = list(
        processed_df[~(processed_df['amount_1'].isin([np.nan, None]))]['date_formatted'])
    salary_amounts = list(
        processed_df[~(processed_df['amount_1'].isin([np.nan, None]))]['amount'])

    # salary_dates = list(processed_df[processed_df['transaction_channel'] == 'salary']['date_formatted'])
    # salary_amounts = list(processed_df[processed_df['transaction_channel'] == 'salary']['amount'])

    result_df = pd.DataFrame(
        columns=['Month Year', 'channel', 'amount', 'salary'])
    para_list = ['net_banking_transfer', 'bill_payment',
                 'cash_withdrawl', 'debit_card', 'upi']

    for i in range(0, len(salary_dates)):
        if i != len(salary_dates) - 1:
            each_limit = min(
                salary_dates[i] + datetime.timedelta(days=xdays), salary_dates[i + 1])
        else:
            each_limit = salary_dates[i] + datetime.timedelta(days=xdays)
        each_df = processed_df[
            (processed_df['date_formatted'] >= salary_dates[i]) & (processed_df['date_formatted'] < each_limit)]
        for each_para in para_list:
            result_df = result_df.append({'Month Year': salary_dates[i].strftime('%b-%Y'), 'salary': salary_amounts[i],
                                          'channel': 'perc_salary_spend_' + each_para, 'amount': each_df[
                (each_df['transaction_channel'] == each_para) & (each_df['transaction_type'] == 'debit')][
                'amount'].sum()}, ignore_index=1)
    if result_df.shape[0] == 0:
        grouped_df = pd.DataFrame(columns=['Month Year'])
    else:
        grouped_df = result_df.groupby(['Month Year', 'channel']).sum()
        grouped_df['perc_spend'] = grouped_df['amount'] / grouped_df['salary']
        grouped_df = grouped_df.unstack()['perc_spend']

    salary_df = processed_df[~(processed_df['amount_1'].isin([np.nan, None]))]
    salary_df['Month Year'] = salary_df['date_formatted'].apply(
        lambda x: x.strftime('%b-%Y'))
    grouped_df['total_amount_of_salary'] = salary_df.groupby('Month Year')[
        'amount'].sum()
    grouped_df['number_of_salary_transactions'] = salary_df.groupby('Month Year')[
        'amount'].count()
    return grouped_df.round(3), salary_df


def get_month_wise_features(processed_data, col_date='date_stringtype'):
    monthly_group = processed_data.groupby('month_year', sort=False)
    group_results = pd.DataFrame()
    for each, each_df in monthly_group:
        each_month_features = get_basic_features(each_df.reset_index(drop=1))
        each_month_features2 = get_basic_features_part2(
            each_df.reset_index(drop=1))
        each_month_features.update(each_month_features2)
        each_month_features.update({'Month Year': each})
        group_results = group_results.append(
            each_month_features, ignore_index=True)
    for each in group_results.columns:
        if (each == 'date_formatted') | (each == 'Month Year'):
            continue
        try:
            group_results[each] = group_results[each].astype(int)
        except Exception as e:
            print(e)

    salary_para_df, salary_df = get_salary_parameters(processed_data, 7)
    if 'Month Year' in salary_para_df.columns:
        salary_para_df = salary_para_df.reset_index(drop=1)
    else:
        salary_para_df = salary_para_df.reset_index(drop=0)
    returndata = group_results.merge(
        salary_para_df, how='left', on='Month Year')
    returndata = returndata.set_index('Month Year')

    returndata = returndata.fillna(0)

    return returndata[::-1], salary_df


def get_groupby(processed_data, col_wise='date_formatted', final_col_name='Date', debit_multiplier=-1):
    grouped = processed_data.groupby([col_wise, 'transaction_type']).sum().unstack(
        fill_value=0)['amount'].reset_index()
    grouped = grouped.rename(
        columns={col_wise: final_col_name, 'credit': 'Sum Credit', 'debit': 'Sum Debit'})
    if 'Sum Credit' not in grouped.columns:
        grouped['Sum Credit'] = 0
    if 'Sum Debit' not in grouped.columns:
        grouped['Sum Debit'] = 0
    grouped['Sum Debit'] = grouped['Sum Debit'].apply(
        lambda x: debit_multiplier * x)

    return grouped


# In[7]:
def process_raw_data_to_add(raw_data):
    data_to_add = raw_data.copy()
    data_to_add['date_formatted'] = data_to_add['date_formatted'].apply(
        lambda x: str(x.date()))
    data_to_add = data_to_add.rename(columns={'date_formatted': 'Date'})
    data_to_add = data_to_add.reset_index()
    if 'description' not in data_to_add.columns:
        data_to_add['description'] = ''
    data_to_add = data_to_add.rename(
        columns={'index': 'S. No.', 'Date': 'Date', 'amount': 'Transaction Amount', 'balance': 'Balance',
                 'transaction_note': 'Note', 'transaction_type': 'Transaction Type',
                 'transaction_channel': 'Transaction Channel', 'merchant_category': 'Merchant Category',
                 'description': 'Description'})
    data_to_add = data_to_add[
        ['S. No.', 'Date', 'Transaction Type', 'Transaction Channel', 'Description', 'Merchant Category',
         'Transaction Amount', 'Balance',
         'Note']]

    data_to_add['S. No.'] = data_to_add['S. No.'] + 1
    data_to_add['Transaction Channel'] = data_to_add['Transaction Channel'].apply(
        lambda x: x if x is np.nan else x.replace('_', ' ').title())

    data_without_channel = data_to_add[
        ['Date', 'Transaction Amount', 'Balance', 'Transaction Type', 'Transaction Channel', 'Note']]
    return data_without_channel, data_to_add


def insert_header(worksheet_name, name_for_header):
    worksheet_name.insert_textbox(
        'A1', name_for_header.upper(), heading_options)
    return worksheet_name


def add_raw_data_sheet(writer, raw_data, sheet_name):
    api1, data_to_add = process_raw_data_to_add(raw_data)

    data_to_add_in_raw_sheet = data_to_add.copy()
    data_to_add_in_raw_sheet['Credit'] = data_to_add_in_raw_sheet[data_to_add_in_raw_sheet['Transaction Type']
                                                                  == 'credit']['Transaction Amount']
    data_to_add_in_raw_sheet['Debit'] = data_to_add_in_raw_sheet[data_to_add_in_raw_sheet['Transaction Type']
                                                                 == 'debit']['Transaction Amount']
    data_to_add_in_raw_sheet = data_to_add_in_raw_sheet.drop(
        ['Transaction Amount'], axis=1)
    data_to_add_in_raw_sheet = data_to_add_in_raw_sheet[[
        'S. No.', 'Date', 'Transaction Type', 'Transaction Channel', 'Description', 'Merchant Category', 'Credit', 'Debit', 'Balance', 'Note']]
    data_to_add_in_raw_sheet.to_excel(writer, sheet_name, index=0)

    raw_data_worksheet = writer.sheets[sheet_name]
    workbook = writer.book
    header_format = workbook.add_format(
        {'bold': True, 'bg_color': '#D9D9D9', 'font_color': 'black'})
    value_format = workbook.add_format(
        {'bold': 0, 'font_color': 'black', 'text_wrap': True})
    value_format.set_align('center')
    value_format.set_align('vcenter')
    bad_format = workbook.add_format(
        {'bold': 0, 'bg_color': '#FFE9D7', 'font_color': 'black', 'text_wrap': True})

    raw_data_worksheet.conditional_format('$A1:' + str(b[data_to_add_in_raw_sheet.shape[1] - 1]) + '1', {'type': 'cell',
                                                                                                         'criteria': "<>",
                                                                                                         'value': -11,
                                                                                                         'format': header_format})
    raw_data_worksheet.conditional_format('$A$2:$J$' + str(data_to_add_in_raw_sheet.shape[0] + 1),
                                          {'type': 'cell', 'criteria': '<>', 'value': -11, 'format': value_format})

    bad_flags = ['outward_cheque_bounce', 'auto_debit_payment_bounce', 'Outward Cheque Bounce',
                 'Auto Debit Payment Bounce']
    bad_index = data_to_add[data_to_add['Transaction Channel'].isin(
        bad_flags)].index
    for each in bad_index:
        raw_data_worksheet.conditional_format('$A$' + str(each + 2) + ':$H$' + str(each + 2),
                                              {'type': 'cell', 'criteria': '<>', 'value': -11, 'format': bad_format})
    raw_data_worksheet.set_column(0, 0, 6)
    raw_data_worksheet.set_column(1, 2, 13)
    raw_data_worksheet.set_column(3, 3, 20)
    raw_data_worksheet.set_column(4, 5, 25)
    raw_data_worksheet.set_column(6, 8, 13)
    raw_data_worksheet.set_column(9, 9, 55)
    for each in range(0, data_to_add_in_raw_sheet.shape[0] + 1):
        raw_data_worksheet.set_row(each, 30)
    raw_data_worksheet.hide_gridlines(2)
    return writer, data_to_add


# In[8]:


def add_salary_sheet(writer, monthly_data_transposed, data_to_add, sheet_name):
    workbook = writer.book

    parameter_format = workbook.add_format({
        'border': 1,
        'bg_color': '#C6EFCE',
        'bold': True,
        'text_wrap': True,
        'align': 'center',
        'valign': 'vcenter',
        'indent': 1,
    })
    header_format = workbook.add_format({
        'border': 1,
        'bg_color': 'black',
        'bold': True,
        'font_color': 'white',
        'font_size': 16,
        'text_wrap': True,
        'align': 'center',
        'valign': 'vcenter',
        'indent': 1,
    })
    percent_fmt = workbook.add_format({'num_format': '0%'})

    salary_variable = ['number_of_salary_transactions', 'total_amount_of_salary', 'perc_salary_spend_bill_payment',
                       'perc_salary_spend_cash_withdrawl', 'perc_salary_spend_debit_card',
                       'perc_salary_spend_net_banking_transfer', 'perc_salary_spend_upi']
    salary_df = monthly_data_transposed[monthly_data_transposed.index.isin(
        salary_variable)]
    salary_df = salary_df.reindex(salary_variable)
    salary_df = salary_df.rename(
        index={'number_of_salary_transactions': 'Number of Salary Transactions',
               'total_amount_of_salary': 'Total Amount of Salary',
               'perc_salary_spend_bill_payment': '% Salary Spent on Bill Payment (7 days)',
               'perc_salary_spend_cash_withdrawl': '% Salary Spent Through Cash Withdrawal (7 days)',
               'perc_salary_spend_debit_card': '% Salary Spent through Debit Card (7 days)',
               'perc_salary_spend_net_banking_transfer': '% Salary Spent through Net Banking (7 days)',
               'perc_salary_spend_upi': '% Salary Spent through UPI (7 days)'})

    salary_df.to_excel(writer, sheet_name, startrow=4)
    # data_to_add = data_to_add.rename({'S No.': 'Transaction No.'})
    data_to_add = data_to_add[
        ['date', 'transaction_type', 'transaction_channel', 'amount', 'balance', 'transaction_note']]
    data_to_add = data_to_add.rename(
        columns={'date': 'Date', 'transaction_type': 'Transaction Type', 'transaction_channel': 'Transaction Channel',
                 'amount': 'Amount', 'balance': 'Balance', 'transaction_note': 'Tranasction Note'})
    data_to_add['Transaction Channel'] = data_to_add['Transaction Channel'].apply(
        lambda x: x.replace('_', ' ').title())
    data_to_add.to_excel(writer, sheet_name, startrow=20, startcol=0, index=0)

    salary_worksheet = writer.sheets[sheet_name]
    salary_worksheet.write('A5', 'Salary Parameters: ', parameter_format)
    salary_worksheet.write('A18', 'Salary Transactions: ', parameter_format)
    salary_worksheet.merge_range("A1:H2", "SALARY PROFILE", header_format)
    # salary_worksheet.insert_textbox('A1', sheet_name.upper(), heading_options)

    salary_worksheet.conditional_format('$B$8:$M$12',
                                        {'type': 'cell', 'criteria': '<>', 'value': -1, 'format': percent_fmt})

    salary_worksheet.hide_gridlines(2)
    salary_worksheet.set_zoom(80)
    for each in range(4, 13):
        salary_worksheet.set_row(each, 20)
    salary_worksheet.set_column(0, 0, 40)
    salary_worksheet.set_column(1, 1, 15)
    salary_worksheet.set_column(2, 2, 30)
    salary_worksheet.set_column(3, 4, 20)
    salary_worksheet.set_column(5, 5, 35)
    salary_worksheet.set_column(6, 20, 20)

    return writer


def get_monthly_flow_chart(workbook, sum_df, startrow, startcol, sheet_name):
    chart = workbook.add_chart({'type': 'column'})
    for each in ['Credit_sum', 'Debit_sum']:
        chart.add_series({'name': each,
                          'categories': '=' + "'" + sheet_name + "'" + '!$C' + str(startrow + 1) + ':' + str(
                              b[sum_df.shape[1] + 1]) + str(startrow + 1),
                          'values': '=' + "'" + sheet_name + "'" + '''!$C$''' + str(
                              sum_df.index.get_loc(each) + startrow + 2) + ':$' + str(
                              b[sum_df.shape[1] + 1]) + '$' + str(sum_df.index.get_loc(each) + startrow + 2),
                          'overlap':    100,
                          'data_labels': {'value': True}
                          })
    chart.set_title({'name': 'Monthly Spending/Income Channel Distribution'})
    chart.set_style(37)
    if sum_df.shape[1]<6:
        width = 620
        height = 356
    else:
        width = 940
        height = 356
    chart.set_size({'width': width, 'height': height})
    return chart


# In[9]:


def get_monthly_bal_chart(workbook, monthly_data_transposed, startrow, startcol, sheet_name, width=620, height=356):
    chart = workbook.add_chart({'type': 'column'})
    chart.add_series({'categories': '=' + "'" + sheet_name + "'" + '!$C' + str(startrow + 1) + ':' + str(
        b[monthly_data_transposed.shape[1] + 1]) + str(startrow + 1),
        'values': '=' + "'" + sheet_name + "'" + '''!$C$''' + str(
        monthly_data_transposed.index.get_loc("Average Balance") + startrow + 2) + ':$' + str(
        b[monthly_data_transposed.shape[1] + 1]) + '$' + str(
        monthly_data_transposed.index.get_loc("Average Balance") + startrow + 2),
        'data_labels': {'value': True},
        'trendline': {'type': 'linear',
                      'line': {'color': 'red', 'width': 1, 'dash_type': 'long_dash', }, },
    })
    chart.set_title({'name': 'Monthly Average Balance'})
    chart.set_style(37)
    chart.set_legend({'none': True})
    chart.set_size({'width': width, 'height': height})
    return chart


# In[10]:

# NOT USED ANYMORE
def add_monthly_data_sheet(writer, processed_data, channel_wise, sheet_name):
    monthly_data, salary_df = get_month_wise_features(processed_data)
    monthly_data_transposed = monthly_data.transpose()
    monthly_data_transposed = monthly_data_transposed.drop(['date_formatted'])
    startrow = 28
    startcol = 1
    salary_vars = ['perc_salary_spend_bill_payment', 'perc_salary_spend_cash_withdrawl', 'perc_salary_spend_debit_card',
                   'perc_salary_spend_net_banking_transfer', 'perc_salary_spend_upi']
    data_to_add = monthly_data_transposed.rename(
        index={'amt_auto_debit_payment_bounce_credit': 'Total Amount of Auto debit bounce',
               'amt_auto_debit_payment_debit': 'Total Amount of Auto-Debit Payments',
               'amt_bank_charge_debit': 'Total Amount of Bank Charges',
               'amt_bank_interest_credit': 'Total Amount of Bank Interest',
               'amt_bill_payment_debit': 'Total Amount of Bill Payments',
               'amt_cash_deposit_credit': 'Total Amount of Cash Deposited',
               'amt_cash_withdrawl_debit': 'Total Amount of Cash Withdrawal',
               'amt_chq_credit': 'Total Amount Credited through Cheque',
               'amt_chq_debit': 'Total Amount Debited through Cheque', 'amt_credit': 'Total Amount Credited',
               'amt_debit': 'Total Amount Debited', 'amt_debit_card_debit': 'Total Amount Spend through Debit card',
               'amt_international_transaction_arbitrage_credit': 'Total Amount of International Credit',
               'amt_international_transaction_arbitrage_debit': 'Total Amount of International Debit',
               'amt_investment_cashin_credit': 'Total Amount of Investment Cash-ins',
               'amt_net_banking_transfer_credit': 'Total Amount Credited through transfers',
               'amt_net_banking_transfer_debit': 'Total Amount Debited through transfers',
               'amt_outward_cheque_bounce_debit': 'Total Amount Debited through Outward Cheque Bounce',
               'amt_inward_cheque_bounce_credit': 'Total Amount Credited through Inward Cheque Bounce',
               'amt_payment_gateway_purchase_debit': 'Total Amount of Payment Gateway Purchase',
               'amt_refund_credit': 'Total Amount of Refund',
               'amt_upi_credit': 'Total Amount Credited through UPI',
               'amt_upi_debit': 'Total Amount Debited through UPI', 'avg_bal': 'Average Balance',
               'avg_credit_transaction_size': 'Average Credit Transaction Size',
               'avg_debit_transaction_size': 'Average Debit Transaction Size',
               'closing_balance': 'Closing balance',
               'cnt_auto_debit_payment_bounce_credit': 'Number of Auto-Debit Bounces',
               'cnt_auto_debit_payment_debit': 'Number of Auto-debited payments',
               'cnt_bank_charge_debit': 'Number of Bank Charge payments',
               'cnt_bank_interest_credit': 'Number of Bank Interest Credits',
               'cnt_bill_payment_debit': 'Number of Bill Payments',
               'cnt_cash_deposit_credit': 'Number of Cash Deposit Transactions',
               'cnt_cash_withdrawl_debit': 'Number of Cash Withdrawal Transactions',
               'cnt_chq_credit': 'Number of Credit Transactions through cheque',
               'cnt_chq_debit': 'Number of Debit Transactions through cheque',
               'cnt_credit': 'Number of Credit Transactions', 'cnt_debit': 'Number of Debit Transactions',
               'cnt_debit_card_debit': 'Number of Debit Card Transactions',
               'cnt_international_transaction_arbitrage_credit': 'Number of International Credit transactions',
               'cnt_international_transaction_arbitrage_debit': 'Number of International Debit transactions',
               'cnt_investment_cashin_credit': 'Number of Investment Cash-ins',
               'cnt_net_banking_transfer_credit': 'Number of Net Banking Credit Transactions',
               'cnt_net_banking_transfer_debit': 'Number of Net Banking Debit Transactions',
               'cnt_outward_cheque_bounce_debit': 'Number of Debit Transactions through Outward Cheque Bounce',
               'cnt_inward_cheque_bounce_credit': 'Number of Credit Transactions through Inward Cheque Bounce',
               'cnt_payment_gateway_purchase_debit': 'Number of Payment Gateway Purchase',
               'cnt_refund_credit': 'Number of Refund Transactions',
               'cnt_transactions': 'Number of Transactions',
               'cnt_upi_credit': 'Number of Credit Transactions through UPI',
               'cnt_upi_debit': 'Number of Debit Transactions through UPI', 'max_bal': 'Maximum Balance',
               'max_eod_balance': 'Maximum EOD Balance', 'median_balance': 'Median Balance',
               'min_bal': 'Minimum Balance', 'min_eod_balance': 'Minimum EOD Balance', 'mode_balance': 'Mode Balance',
               'net_cash_inflow': 'Net Cashflow', 'opening_balance': 'Opening Balance',
               'number_of_salary_transactions': 'Number of Salary Transactions',
               'total_amount_of_salary': 'Total Amount of Salary'})
    data_to_add = data_to_add.reindex(
        ['Opening Balance', 'Total Amount Credited', 'Total Amount Debited', 'Closing balance', 'Net Cashflow',
         'Average Balance', 'Median Balance', 'Average Credit Transaction Size', 'Average Debit Transaction Size',
         'Mode Balance', 'Number of Transactions', 'Number of Credit Transactions',
         'Number of Debit Transactions', 'Maximum Balance', 'Minimum Balance', 'Maximum EOD Balance',
         'Minimum EOD Balance', 'Total Amount of Cash Deposited', 'Total Amount Credited through transfers',
         'Total Amount Credited through UPI', 'Total Amount of Salary', 'Total Amount Credited through Cheque',
         'Total Amount of International Credit', 'Total Amount of Investment Cash-ins', 'Total Amount of Refund',
         'Total Amount of Bank Interest', 'Total Amount Spend through Debit card', 'Total Amount of Cash Withdrawal',
         'Total Amount of Auto-Debit Payments', 'Total Amount of Bill Payments', 'Total Amount of Bank Charges',
         'Total Amount Debited through Cheque', 'Total Amount of Auto debit bounce', 'Total Amount Debited through UPI',
         'Total Amount Debited through transfers', 'Total Amount of International Debit',
         'Total Amount Debited through Outward Cheque Bounce','Total Amount Credited through Inward Cheque Bounce', 'Total Amount of Payment Gateway Purchase',
         'Number of Cash Deposit Transactions', 'Number of Net Banking Credit Transactions',
         'Number of Credit Transactions through UPI', 'Number of Salary Transactions',
         'Number of Credit Transactions through cheque', 'Number of International Credit transactions',
         'Number of Investment Cash-ins', 'Number of Refund Transactions', 'Number of Bank Interest Credits',
         'Number of Debit Card Transactions', 'Number of Cash Withdrawal Transactions',
         'Number of Auto-debited payments', 'Number of Bill Payments', 'Number of Bank Charge payments',
         'Number of Debit Transactions through cheque', 'Number of Auto-Debit Bounces',
         'Number of Debit Transactions through UPI', 'Number of Net Banking Debit Transactions',
         'Number of International Debit transactions', 'Number of Debit Transactions through Outward Cheque Bounce',
         'Number of Credit Transactions through Inward Cheque Bounce','Number of Payment Gateway Purchase'] + salary_vars)
    data_to_add = data_to_add.rename(index={'perc_salary_spend_bill_payment': '% Salary Spent on Bill Payment (7 days)',
                                            'perc_salary_spend_cash_withdrawl': '% Salary Spent Through Cash Withdrawal (7 days)',
                                            'perc_salary_spend_debit_card': '% Salary Spent through Debit Card (7 days)',
                                            'perc_salary_spend_net_banking_transfer': '% Salary Spent through Net Banking (7 days)',
                                            'perc_salary_spend_upi': '% Salary Spent through UPI (7 days)'})
    data_to_add.to_excel(writer, sheet_name=sheet_name,
                         startrow=startrow, startcol=startcol)
    workbook = writer.book
    monthly_worksheet = writer.sheets[sheet_name]
    monthly_data['amt_debit'] = monthly_data['amt_debit'].apply(
        lambda x: x * -1)
    credit_debit_df = monthly_data[['amt_credit', 'amt_debit']].transpose()
    credit_debit_df = credit_debit_df.rename(
        index={'amt_credit': 'Credit_sum', 'amt_debit': 'Debit_sum'})
    credit_debit_df.to_excel(writer, sheet_name, startrow=200, startcol=1)
    monthly_worksheet.set_column(0, monthly_data_transposed.shape[1] + 1, 15)
    monthly_worksheet.hide_gridlines(2)
    monthly_worksheet.set_zoom(80)
    monthly_worksheet.set_column(1, 1, 38)
    for each in range(startrow, startrow + data_to_add.shape[0]):
        monthly_worksheet.set_row(each, 20)
    monthly_worksheet.insert_textbox('A1', sheet_name.upper(), heading_options)
    monthly_bal_chart = get_monthly_bal_chart(
        workbook, data_to_add, startrow, startcol, sheet_name)
    monthly_worksheet.insert_chart('B6', monthly_bal_chart, {
                                   'x_offset': -30, 'y_offset': 0})
    monthly_flow_chart = get_monthly_flow_chart(
        workbook, credit_debit_df, 200, 2, sheet_name)
    if credit_debit_df.shape[1]<6:
        start_position = 'H6'
    else:
        start_position = 'G6'
    monthly_worksheet.insert_chart(start_position, monthly_flow_chart, {
                                   'x_offset': -10, 'y_offset': 0})
    index_format = workbook.add_format(
        {'bold': True, 'bg_color': '#000000', 'font_color': 'white', 'text_wrap': True, 'align': 'left',
         'valign': 'vcenter'})
    for each in range(0, data_to_add.shape[0]):
        monthly_worksheet.merge_range("A" + str(startrow + 2 + each) + ":B" + str(startrow + 2 + each),
                                      data_to_add.index[each], index_format)
    return writer, monthly_data_transposed.columns, monthly_data_transposed, credit_debit_df, data_to_add, salary_df


# In[11]:

def get_daily_balance_mmm(df):
    import calendar
    its_month = df.reset_index()['date_formatted'][0].month
    its_year = df.reset_index()['date_formatted'][0].date().year
    eod_balance = pd.DataFrame(df.groupby('date_formatted').first()[
                               'balance']).reset_index()
    eod_balance['date_formatted'] = eod_balance['date_formatted'].apply(
        lambda x: x.date())
    daily_balance = pd.DataFrame()

    for each in range(df['date_formatted'].min().day, df['date_formatted'].max().day+1):
        daily_balance = daily_balance.append(
            {'date': datetime.date(its_year, its_month, each)}, ignore_index=1)

    daily_balance = daily_balance.merge(
        eod_balance, left_on='date', right_on='date_formatted', how='left')
    daily_balance = daily_balance.fillna(method="ffill")

    df = df.reset_index()
    if df['transaction_type'][df.shape[0] - 1] == 'credit':
        initial_bal = df['balance'][df.shape[0] - 1] - \
            df['amount'][df.shape[0] - 1]
    else:
        initial_bal = df['balance'][df.shape[0] - 1] + \
            df['amount'][df.shape[0] - 1]
    daily_balance = daily_balance.fillna(initial_bal)
    return daily_balance['balance'].mean(), daily_balance['balance'].median(), daily_balance['balance'].mode()[0]


# In[12]:


def add_daily_data_sheet(writer, processed_data, month_year, raw_data, sheet_name):
    workbook = writer.book

    daily = get_groupby(processed_data, col_wise='date_formatted',
                        final_col_name='Date', debit_multiplier=-1)
    daily_header_format = workbook.add_format({
        'border': 1,
        'bg_color': '#C6EFCE',
        'bold': True,
        'text_wrap': True,
        'align': 'center',
        'valign': 'vcenter',
        'indent': 1,
    })

    date_format = workbook.add_format({'num_format': 'dd/mm/yy'})
    debit_parameter_format = workbook.add_format({'border': 1,
                                                  'bg_color': '#EEB5B5',
                                                  'bold': True,
                                                  'text_wrap': True,
                                                  'align': 'center',
                                                  'valign': 'vcenter',
                                                  'indent': 1,
                                                  })
    credit_parameter_format = workbook.add_format({'border': 1,
                                                   'bg_color': '#D4E7B8',
                                                   'bold': True,
                                                   'text_wrap': True,
                                                   'align': 'center',
                                                   'valign': 'vcenter',
                                                   'indent': 1,
                                                   })
    daily = daily[['Date', 'Sum Credit', 'Sum Debit']]
    daily.to_excel(writer, sheet_name=sheet_name, index=0)
    daily_worksheet = writer.sheets[sheet_name]
    daily_worksheet.hide_gridlines(2)
    daily_worksheet.set_zoom(80)
    daily_worksheet.set_column('F:F', 25)
    daily_worksheet.set_row(4, 30)
    daily_worksheet.insert_textbox('A1', sheet_name.upper(), heading_options)

    raw_data['month_year'] = raw_data['date_formatted'].apply(
        lambda x: datetime.date(x.year, x.month, 1))
    raw_data.to_excel(writer, sheet_name=sheet_name, startrow=0, startcol=60)

    daily_worksheet.write_comment('F5', 'Select a month from dropdown to see the Parameters below',
                                  {'visible': True})
    # daily_worksheet.write('F6', " *Select a month from dropdown to see the Parameters below")

    parameter_start = 14

    daily_worksheet.set_row(parameter_start - 1, 35)
    daily_worksheet.write('E' + str(parameter_start),
                          "Count of Credit Transactions", credit_parameter_format)
    daily_worksheet.write_array_formula('F' + str(parameter_start),
                                        '''{=count(IF(BO:BO=$F$5,if(BN:BN="credit",BK:BK)))}''',
                                        credit_parameter_format)
    parameter_start = parameter_start + 1

    daily_worksheet.set_row(parameter_start - 1, 35)
    daily_worksheet.write('E' + str(parameter_start),
                          "Total Credit Amount", credit_parameter_format)
    daily_worksheet.write_array_formula('F' + str(parameter_start),
                                        '''{=sum(IF(BO:BO=$F$5,if(BN:BN="credit",BK:BK)))}''', credit_parameter_format)
    parameter_start = parameter_start + 1

    daily_worksheet.set_row(parameter_start - 1, 35)
    daily_worksheet.write('E' + str(parameter_start), "Max Credit Amount in Single Transaction",
                          credit_parameter_format)
    daily_worksheet.write_array_formula('F' + str(parameter_start),
                                        '''{=sum(IF(BO:BO=$F$5,if(BN:BN="credit",BK:BK)))}''', credit_parameter_format)
    parameter_start = parameter_start + 1

    daily_worksheet.set_row(parameter_start - 1, 35)
    daily_worksheet.write('E' + str(parameter_start),
                          "Total Debit Amount", debit_parameter_format)
    daily_worksheet.write_array_formula('F' + str(parameter_start),
                                        '''{=sum(IF(BO:BO=$F$5,if(BN:BN="debit",BK:BK)))}''', debit_parameter_format)
    parameter_start = parameter_start + 1

    daily_worksheet.set_row(parameter_start - 1, 35)
    daily_worksheet.write('E' + str(parameter_start),
                          "Count of Debit Transactions", debit_parameter_format)
    daily_worksheet.write_array_formula('F' + str(parameter_start),
                                        '''{=count(IF(BO:BO=$F$5,if(BN:BN="debit",BK:BK)))}''', debit_parameter_format)
    parameter_start = parameter_start + 1

    daily_worksheet.set_row(parameter_start - 1, 35)
    daily_worksheet.write('E' + str(parameter_start),
                          "Max Debit Amount in Single Transaction", debit_parameter_format)
    daily_worksheet.write_array_formula('F' + str(parameter_start),
                                        '''{=MAX(IF(BO:BO=$F$5,if(BN:BN="debit",BK:BK)))}''', debit_parameter_format)
    parameter_start = parameter_start + 1

    daily_worksheet.write('E5', "Enter Month :", daily_header_format)
    daily_worksheet.write('F5', str(month_year[0]), daily_header_format)

    daily_worksheet.data_validation('F5', {'validate': 'list',
                                           'source': list(month_year)})
    daily_worksheet.set_column(0, 3, 16, None, {'level': 1, 'hidden': True})
    daily_worksheet.set_column('E:E', 30, None, {'collapsed': True})

    for each_day in range(1, 32):
        daily_worksheet.write_formula('BA' + str(each_day), '=DATE(YEAR(F5),MONTH(F5),' + str(each_day) + ') ',
                                      date_format)
        daily_worksheet.write_formula('BB' + str(each_day),
                                      '=VLOOKUP(BA' + str(each_day) + ',$A$2:$D$' +
                                      str(daily.shape[0] + 1) + ',1,0)',
                                      date_format)
        daily_worksheet.write_formula('BC' + str(each_day),
                                      '=VLOOKUP(BA' + str(each_day) + ',$A$2:$D$' + str(daily.shape[0] + 1) + ',2,0)')
        daily_worksheet.write_formula('BD' + str(each_day),
                                      '=VLOOKUP(BA' + str(each_day) + ',$A$2:$D$' + str(daily.shape[0] + 1) + ',3,0)')

    daily_chart = workbook.add_chart({'type': 'column'})
    daily_chart.add_series({'categories': '=' + "'" + sheet_name + "'" + '''!$BA1:BA31''',
                            'values': '=' + "'" + sheet_name + "'" + '''!$BC$1:BC31''',
                            'gap': 10,
                            'overlap': 100,
                            'name': 'Credits',
                            })
    daily_chart.add_series({'categories': '=' + "'" + sheet_name + "'" + '''!$BA1:BA31''',
                            'values': '=' + "'" + sheet_name + "'" + '''!$BD$1:BD31''',
                            'gap': 10,
                            'overlap': 100,
                            'name': 'Debits',
                            })
    daily_chart.set_y_axis({'visible': True,
                            'major_gridlines': {'visible': False}})
    daily_chart.show_hidden_data()
    daily_chart.set_title({'name': 'Daily Cash-flow'})
    daily_chart.set_style(4)

    daily_chart.set_legend({'none': True})
    daily_chart.set_size({'width': 850, 'height': 500})
    daily_worksheet.insert_chart('H8', daily_chart)
    return writer


# In[13]:

def add_debit_sheet(writer, channel_wise, monthly_data_transposed, processed_data, debit_recurring_df,
                    debit_recurring_list, sheet_name):
    workbook = writer.book

    debit_parameter_format = workbook.add_format({'border': 1,
                                                  'bg_color': '#EEB5B5',
                                                  'bold': True,
                                                  'text_wrap': True,
                                                  'align': 'center',
                                                  'valign': 'vcenter',
                                                  'indent': 1,
                                                  })
    hyperlink_format = workbook.add_format({
        'border': 1,
        'bg_color': '#C6EFCE',
        'bold': True,
        'text_wrap': True,
        'align': 'center',
        'valign': 'vcenter',
        'indent': 1,
    })

    # data for chart
    debit = channel_wise[channel_wise['Sum Debit'] !=
                         0].sort_values('Sum Debit', ascending=False)
    debit = debit[['Channel', 'Sum Debit']]

    debit['Channel'] = debit['Channel'].apply(
        lambda x: x.replace('_', ' ').title())
    debit = debit.sort_values(
        'Sum Debit', ascending=False).set_index('Channel')
    chart_reference = 20
    if debit.shape[0] == 0:  # otherwise it wil print the name of column_list
        debit = pd.DataFrame()
    debit.transpose().to_excel(writer, sheet_name,
                               startrow=chart_reference, startcol=2, index=0)
    column_format = workbook.add_format(
        {'border': 1, 'bg_color': 'black', 'font_color': 'white', 'align': 'center'})
    value_format = workbook.add_format(
        {'border': 1, 'bg_color': 'white', 'font_color': 'black', 'align': 'center'})
    debit_sheet = writer.sheets[sheet_name]
    debit_sheet.conditional_format('$C21:$O21', {
                                   'type': 'cell', 'criteria': '<>', 'value': 0, 'format': column_format})
    debit_sheet.conditional_format('$C22:$O22', {
                                   'type': 'cell', 'criteria': '<>', 'value': 0, 'format': value_format})
    debit_sheet.merge_range(
        "A19:C19", "Total Amount Spent Through Different Channels", hyperlink_format)

    # recurring parameters
    recurring_start = 26
    debit_sheet.merge_range("A" + str(recurring_start - 1) + ":C" + str(recurring_start - 1),
                            "Recurring Debit Transactions", hyperlink_format)

    debit_recurring_df['Frequency'] = debit_recurring_df['Number of Transactions'] / (
        1 + (processed_data['date_formatted'].max() - processed_data['date_formatted'].min()).days)
    debit_recurring_df['Frequency'] = debit_recurring_df['Frequency'].apply(
        lambda x: round(x, 3))

    debit_recurring_df.to_excel(
        writer, sheet_name, startrow=recurring_start, startcol=2, index=1)
    debit_sheet.write_comment(recurring_start, 2, 'Scroll down to see transactions of each case',
                              {'visible': True, 'start_cell': 'B27', 'x_offset': -40, 'y_offset': 12,
                               'width': 100, 'height': 50})

    # Individual Transactions

    counter = recurring_start + debit_recurring_df.shape[0] + 15
    debit_sheet.merge_range(counter - 3, 0, counter - 3, 2, "Individual Transacations of Each Destination",
                            hyperlink_format)
    for i in range(0, len(debit_recurring_df.index)):
        debit_sheet.write(recurring_start + 1, 2,
                          debit_recurring_df.index[i][:11] + ' ' + debit_recurring_df.index[i][11:], hyperlink_format)
        recurring_start = recurring_start + 1
        debit_sheet.merge_range(counter - 1, 0, counter, 1,
                                debit_recurring_df.index[i][:11] + ' ' + debit_recurring_df.index[i][
                                    11:] + " Transactions", hyperlink_format)
        df = pd.DataFrame(columns=['date_stringtype', 'transaction_type', 'amount', 'balance', 'transaction_channel',
                                   'transaction_note'])
        for each in debit_recurring_list[i]:
            df = df.append(each, ignore_index=1)
        df = df[['date_stringtype', 'transaction_type', 'amount',
                 'balance', 'transaction_channel', 'transaction_note']]
        df = df.rename(columns={'date_stringtype': 'Date', 'transaction_type': 'Transaction Type', 'amount': 'Amount',
                                'balance': 'Balance', 'transaction_note': 'Transaction Note',
                                'transaction_channel': 'Transaction Channel'})
        df['Transaction Channel'] = df['Transaction Channel'].apply(
            lambda x: x.replace('_', ' ').title())
        df.to_excel(writer, sheet_name, startrow=counter, startcol=2, index=0)
        counter = counter + df.shape[0] + 6

    # Max parameters
    para_dict = {
        'net_banking_transfer': 'Max Transferred Amount in Single Transaction',
        'cash_withdrawl': 'Max Cash Withdrawal in Single Transaction',
        'auto_debit_payment': 'Max Auto Debit Amount in Single Transaction',
        'debit_card': 'Max Debit Card Transaction',
        'upi': 'Max UPI Transaction Amount'
    }
    parameter_start = 11
    for each_para, each_description in para_dict.items():
        value1 = processed_data[
            (processed_data.transaction_channel == each_para) & (processed_data.transaction_type == 'debit')].max()[
            'amount']
        value1 = 0 if str(value1) == 'nan' else value1
        debit_sheet.set_row(parameter_start - 1, 35)
        debit_sheet.write('G' + str(parameter_start),
                          each_description, debit_parameter_format)
        debit_sheet.write('H' + str(parameter_start), 'INR ' +
                          str(value1), debit_parameter_format)
        parameter_start = parameter_start + 1

    # worksheet formatting
    debit_sheet.insert_textbox('A1', sheet_name.upper(), heading_options)
    debit_sheet.hide_gridlines(2)
    debit_sheet.set_column(2, 14, 22)
    debit_sheet.set_column(6, 6, 30)
    debit_sheet.set_zoom(80)

    # Chart
    debit_chart = workbook.add_chart({'type': 'pie'})
    debit_chart.set_size({'width': 620, 'height': 356})
    debit_chart.add_series({
        'name': 'Total Debit Amount: Channel Distribution',
        'categories': ['Debit Profile', chart_reference, 2, chart_reference, debit.shape[0] + 1],
        'values': ['Debit Profile', chart_reference + 1, 2, chart_reference + 1, debit.shape[0] + 1],
        'data_labels': {'percentage': True}
    })
    debit_chart.set_style(12)
    debit_sheet.insert_chart('B6', debit_chart)

    return writer


# In[14]:

def add_credit_sheet(writer, channel_wise, monthly_data_transposed, processed_data, credit_recurring_df,
                     credit_recurring_list, sheet_name):
    workbook = writer.book
    credit_parameter_format = workbook.add_format({'border': 1,
                                                   'bg_color': '#D4E7B8',
                                                   'bold': True,
                                                   'text_wrap': True,
                                                   'align': 'center',
                                                   'valign': 'vcenter',
                                                   'indent': 1,
                                                   })
    hyperlink_format = workbook.add_format({
        'border': 1,
        'bg_color': '#C6EFCE',
        'bold': True,
        'text_wrap': True,
        'align': 'center',
        'valign': 'vcenter',
        'indent': 1,
    })
    # data for chart
    credit = channel_wise[channel_wise['Sum Credit'] !=
                          0].sort_values('Sum Credit', ascending=False)
    credit = credit[['Channel', 'Sum Credit']]
    credit['Channel'] = credit['Channel'].apply(
        lambda x: x.replace('_', ' ').title())
    credit = credit.sort_values(
        'Sum Credit', ascending=False).set_index('Channel')
    chart_reference = 20
    if credit.shape[0] == 0:  # otherwise it wil print the name of column_list
        credit = pd.DataFrame()
    credit.transpose().to_excel(writer, sheet_name, startrow=20, startcol=2, index=0)
    column_format = workbook.add_format(
        {'border': 1, 'bg_color': 'black', 'font_color': 'white', 'align': 'center'})
    value_format = workbook.add_format(
        {'border': 1, 'bg_color': 'white', 'font_color': 'black', 'align': 'center'})
    credit_sheet = writer.sheets[sheet_name]
    credit_sheet.conditional_format('$C21:$O21',
                                    {'type': 'cell', 'criteria': '<>', 'value': 0, 'format': column_format})
    credit_sheet.conditional_format(
        '$C22:$O22', {'type': 'cell', 'criteria': '<>', 'value': 0, 'format': value_format})
    credit_sheet.merge_range(
        "A19:C19", "Total Amount Credited Through Different Channels", hyperlink_format)
    # recurring parameters
    recurring_start = 26
    recurring_start_for_link = 26
    credit_sheet.merge_range("A" + str(recurring_start - 1) + ":C" + str(recurring_start - 1),
                             "Recurring Credit Transactions", hyperlink_format)
    credit_recurring_df['Frequency'] = credit_recurring_df['Number of Transactions'] / (
        1 + (processed_data['date_formatted'].max() - processed_data['date_formatted'].min()).days)
    credit_recurring_df['Frequency'] = credit_recurring_df['Frequency'].apply(
        lambda x: round(x, 3))
    credit_recurring_df.to_excel(
        writer, sheet_name, startrow=recurring_start, startcol=2, index=1)
    credit_sheet.write_comment(recurring_start, 2, 'Scroll down to see transactions of each case',
                               {'visible': True, 'start_cell': 'B27', 'x_offset': -40, 'y_offset': 12,
                                'width': 100, 'height': 50})
    # Individual Transactions
    counter = recurring_start + credit_recurring_df.shape[0] + 15
    credit_sheet.merge_range(counter - 3, 0, counter - 3, 2, "Individual Transacations of Each Destination",
                             hyperlink_format)
    for i in range(0, len(credit_recurring_df.index)):
        credit_sheet.write(recurring_start + 1, 2,
                           credit_recurring_df.index[i][:6] + ' ' + credit_recurring_df.index[i][6:], hyperlink_format)
        recurring_start = recurring_start + 1
        credit_sheet.merge_range(counter - 1, 0, counter, 1,credit_recurring_df.index[i][:6] +' ' + credit_recurring_df.index[i][6:],  hyperlink_format)
        df = pd.DataFrame(columns=['date_stringtype', 'transaction_type', 'amount', 'balance', 'transaction_channel',
                                   'transaction_note'])
        for each in credit_recurring_list[i]:
            df = df.append(each, ignore_index=1)
        df = df[['date_stringtype', 'transaction_type', 'amount','balance', 'transaction_channel', 'transaction_note']]
        df = df.rename(columns={'date_stringtype': 'Date', 'transaction_type': 'Transaction Type', 'amount': 'Amount',
                     'balance': 'Balance','transaction_note': 'Transaction Note', 'transaction_channel': 'Transaction Channel'})
        df['Transaction Channel'] = df['Transaction Channel'].apply(lambda x: x.replace('_', ' ').title())
        df.to_excel(writer, sheet_name, startrow=counter, startcol=2, index=0)
        #Link the transtactions
        credit_sheet.write_url('C'+str(recurring_start_for_link+i+2), "internal:A"+str(counter), hyperlink_format, string='Source '+str(i+1))
        counter = counter + df.shape[0] + 6
    # Max parameters
    para_dict = {
        'net_banking_transfer': 'Max Transferred-In Amount in Single Transaction',
        'salary': 'Max Salary Amount',
        'cash_deposit': 'Max Cash Deposited in Single Transaction',
        'upi': 'Max UPI Credit in Single Transaction'
    }
    parameter_start = 11
    for each_para, each_description in para_dict.items():
        value1 = processed_data[
            (processed_data.transaction_channel == each_para) & (processed_data.transaction_type == 'credit')].max()[
            'amount']
        value1 = 0 if str(value1) == 'nan' else value1
        credit_sheet.set_row(parameter_start - 1, 35)
        credit_sheet.write('G' + str(parameter_start),
                           each_description, credit_parameter_format)
        credit_sheet.write('H' + str(parameter_start),
                           'INR ' + str(value1), credit_parameter_format)
        parameter_start = parameter_start + 1
    credit_sheet.set_row(parameter_start, 35)
    # worksheet formatting
    credit_sheet.insert_textbox('A1', sheet_name.upper(), heading_options)
    credit_sheet.hide_gridlines(2)
    credit_sheet.set_column(1, 1, 10)
    credit_sheet.set_column(2, 14, 22)
    credit_sheet.set_column(6, 6, 30)
    credit_sheet.set_zoom(80)
    # Chart
    credit_chart = workbook.add_chart({'type': 'pie'})
    credit_chart.add_series({
        'name': 'Total Credit Amount: Channel Distribution',
        'categories': ['Credit Profile', chart_reference, 2, chart_reference, credit.shape[0] + 1],
        'values': ['Credit Profile', chart_reference + 1, 2, chart_reference + 1, credit.shape[0] + 1],
        'data_labels': {'percentage': True}
    })
    credit_chart.set_size({'width': 620, 'height': 356})
    credit_chart.set_style(13)
    credit_sheet.insert_chart('B6', credit_chart)
    return writer


# In[15]:


def add_home_sheet(writer, user_dict):
    workbook = writer.book
    home_worksheet = workbook.add_worksheet('HOME')
    home_worksheet.set_tab_color('red')
    home_worksheet.set_zoom(80)

    home_format = workbook.add_format(
        {'font_size': 12, 'font_color': 'white', 'bg_color': '#2F275B'})

    for row_number in range(0, 100):
        home_worksheet.write_row(
            row_number, 0, [None for x in range(0, 30)], home_format)
    home_worksheet.conditional_format('$A1:Z100', {'type': 'blanks',
                                                   'format': home_format})

    heading_options = {
        'width': 1700,
        'height': 80,
        'x_offset': -60,
        'y_offset': 0,

        'font': {'bold': True,
                 'italic': True,
                 'underline': 0,
                 'name': 'Arial',
                 'color': 'white',
                 'size': 20},
        'align': {'vertical': 'middle',
                  'horizontal': 'center'
                  },
        'fill': {'color': 'black'},
        'line': {'color': 'white'}
    }

    tab_format = workbook.add_format(
        {'font_size': 18, 'bg_color': '#90C14A', 'font_color': 'black', 'align': 'center', 'valign': 'vcenter'})

    for each in range(0, 20):
        home_worksheet.set_row(each, 40)
    home_worksheet.set_column(1, 2, 20)
    home_worksheet.set_column(3, 3, 30)
    home_worksheet.set_column(4, 4, 20)
    home_worksheet.set_column(5, 10, 30)

    start_row = 5
    for key, value in user_dict.items():

        if key == 'Address':
            home_worksheet.write(start_row, 2, str(key), home_format)
            home_worksheet.merge_range(
                start_row, 3, start_row + 1, 3, str(value).upper(), home_format)
            start_row = start_row + 1
        else:
            home_worksheet.write(start_row, 2, str(key), home_format)
            home_worksheet.write(start_row, 3, str(value).upper(), home_format)
        start_row = start_row + 1

    home_worksheet.insert_textbox(
        'A1', 'Finbox Statement Report', heading_options)
    home_worksheet.insert_image(
        'A1', '/Users/piyush/Desktop/finboxlog.png', {'x_offset': 15, 'y_offset': 10})

    home_worksheet.write_url(
        'F4', "internal:'Raw Data'!A1", tab_format, string='Raw Data -->>')
    home_worksheet.write_url(
        'F7', "internal:'Summary'!A1", tab_format, string='Summary -->>')
    home_worksheet.write_url(
        'F10', "internal:'Monthly Analysis'!A1", tab_format, string='Monthly Data  -->>')
    home_worksheet.write_url('F13', "internal:'Top 5 Transactions'!A1",
                             tab_format, string='Top Transactions  -->>')
    home_worksheet.write_url(
        'H4', "internal:'Salary Profile'!A1", tab_format, string='Salary Profile  -->>')
    home_worksheet.write_url(
        'H7', "internal:'Loan Profile'!A1", tab_format, string='Loan Profile  -->>')
    home_worksheet.write_url(
        'H10', "internal:'Credit Profile'!A1", tab_format, string='Credit Profile  -->>')
    home_worksheet.write_url(
        'H13', "internal:'Debit Profile'!A1", tab_format, string='Debit Profile  -->>')

    return writer


# In[16]

# In[17]:


def get_summ_monthly_flow_chart(workbook, sheet_name, startrow, startcol):
    flow_chart = workbook.add_chart({'type': 'column'})
    flow_chart.add_series({'name': 'Credited Amount',
                           'categories': '=' + "'" + sheet_name + "'" + '!$C' + str(startrow + 1) + ':E' + str(
                               startrow + 1),
                           'values': '=' + "'" + sheet_name + "'" + '!$C' + str(startrow + 3) + ':E' + str(
                               startrow + 3),
                           'fill': {'color': '#90C14A'}
                           })
    flow_chart.add_series({'name': 'Debited Amount',
                           'categories': '=' + "'" + sheet_name + "'" + '!$C' + str(startrow + 1) + ':F' + str(
                               startrow + 1),
                           'values': '=' + "'" + sheet_name + "'" + '!$C' + str(startrow + 4) + ':E' + str(
                               startrow + 4),
                           'fill': {'color': '#CD4443'}
                           })
    flow_chart.set_title({'name': 'Monthly Cash flow ', 'name_font': {'name': 'Calibri',
                                                                      'size': 13}})
    flow_chart.set_size({'width': 420, 'height': 256})
    flow_chart.set_legend({'position': 'top'})
    return flow_chart


# In[18]:


def get_summ_monthly_bal_chart(workbook, sheet_name, startrow, startcol):
    bal_chart = workbook.add_chart({'type': 'column'})

    bal_chart.add_series({'name': 'Balance',
                          'categories': '=' + "'" + sheet_name + "'" + '!$C' + str(startrow + 1) + ':E' + str(
                              startrow + 1),
                          'values': '=' + "'" + sheet_name + "'" + '!$C' + str(startrow + 2) + ':E' + str(startrow + 2),
                          'data_labels': {'value': True},
                          'fill': {'color': '#FDB078'}
                          })
    bal_chart.set_title({'name': 'Monthly Average Balance ', 'name_font': {'name': 'Calibri',
                                                                           'size': 13}})

    bal_chart.set_size({'width': 420, 'height': 256})
    bal_chart.set_legend({'none': True})
    bal_chart.set_y_axis(
        {'visible': False, 'major_gridlines': {'visible': False}})
    return bal_chart


# In[19]:


def get_daily_chart(workbook, sheet_name, daily_data, startrow, startcol):
    daily_chart = workbook.add_chart({'type': 'line'})
    balance_column_excel = str(
        b[startcol + 1 + daily_data.columns.get_loc('balance')])
    date_column_excel = str(
        b[startcol + 1 + daily_data.columns.get_loc('date_stringtype')])

    daily_chart.add_series({'name': 'Daily Balance',
                            'categories': '=' + "'" + sheet_name + "'" + '!$' + date_column_excel + '2:' + date_column_excel + str(
                                daily_data.shape[0] + 1),
                            'values': '=' + "'" + sheet_name + "'" + '!$' + balance_column_excel + '2:' + balance_column_excel + str(
                                daily_data.shape[0] + 1),
                            'smooth': True
                            })
    daily_chart.set_title({'name': 'Daily Balance ', 'name_font': {'name': 'Calibri',
                                                                   'size': 13}})
    daily_chart.set_size({'width': 420, 'height': 256})
    daily_chart.set_legend({'none': True})
    return daily_chart


# In[20]:


def get_summary_parameters(daily_data):
    obj = {}
    diff_dates = (daily_data['date_formatted'].max() -
                  daily_data['date_formatted'].min()).days
    credit_txn = daily_data[daily_data.transaction_type == 'credit'].shape[0]
    if diff_dates == 0:
        obj['Credit Transactions per Day'] = "NA"
    else:
        obj['Credit Transactions per Day'] = round(
            1.0 * credit_txn / diff_dates, 1)
    debit_txn = daily_data[daily_data.transaction_type == 'debit'].shape[0]
    if diff_dates == 0:
        obj['Debit Transactions per Day'] = "NA"
    else:
        obj['Debit Transactions per Day'] = round(
            1.0 * debit_txn / diff_dates, 1)
    avg_credit = daily_data[daily_data.transaction_type ==
                            'credit']['amount'].mean()
    avg_debit = daily_data[daily_data.transaction_type ==
                           'debit']['amount'].mean()
    obj['Avg Credit Amount'] = 'INR ' + str(round(avg_credit, 1))
    obj['Avg Debit Amount'] = 'INR ' + str(round(avg_debit, 1))
    return obj


def add_summary_sheet(writer, monthly_data_transposed, processed_data, sheet_name):
    import dateutil.relativedelta
    workbook = writer.book

    parameter_format = workbook.add_format({
        'border': 1,
        'bg_color': '#C6EFCE',
        'bold': True,
        'text_wrap': True,
        'align': 'center',
        'valign': 'vcenter',
        'indent': 1,
    })

    data_start = 23
    if len(monthly_data_transposed.columns) <= 3:
        last_3_months = monthly_data_transposed.columns
    else:
        last_3_months = monthly_data_transposed.columns[len(
            monthly_data_transposed.columns) - 3:]
    monthly_data_transposed = monthly_data_transposed[last_3_months]
    data_to_add = monthly_data_transposed[monthly_data_transposed.index.isin(
        ['amt_credit', 'amt_debit', 'avg_bal'])]
    data_to_add = data_to_add.rename(
        index={'amt_credit': 'Credited Amount', 'amt_debit': 'Debited Amount', 'avg_bal': 'Average Balance'})
    data_to_add = data_to_add.reindex(
        ['Average Balance', 'Credited Amount', 'Debited Amount'])
    # data_to_add['Average'] = data_to_add.mean(axis=1)
    data_to_add.round(2).to_excel(writer, sheet_name,
                                  startrow=data_start, startcol=1, index=1)

    data_to_add2 = data_to_add.transpose()
    data_to_add2['Debited Amount'] = data_to_add2['Debited Amount'].apply(
        lambda x: x * -1)
    data_to_add2 = data_to_add2.transpose().round(2)
    data_to_add2.to_excel(writer, sheet_name,
                          startrow=100, startcol=1, index=1)

    summary_sheet = writer.sheets[sheet_name]
    summary_sheet.set_column(1, 1, 20)
    summary_sheet.set_column(2, 5, 12)
    summary_sheet.hide_gridlines(2)
    summary_sheet.set_zoom(80)

    summary_sheet.write(data_start, 5, 'Monthly Average', parameter_format)
    summary_sheet.write(
        data_start + 1, 5, round(data_to_add.mean(axis=1)[0], 2), parameter_format)
    summary_sheet.write(
        data_start + 2, 5, round(data_to_add.mean(axis=1)[1], 2), parameter_format)
    summary_sheet.write(
        data_start + 3, 5, round(data_to_add.mean(axis=1)[2], 2), parameter_format)

    # Charts
    bal_chart = get_summ_monthly_bal_chart(workbook, sheet_name, data_start, 1)
    summary_sheet.insert_chart(
        'B7', bal_chart, {'x_offset': -10, 'y_offset': 0})

    flow_chart = get_summ_monthly_flow_chart(
        workbook, sheet_name, data_start, 1)
    summary_sheet.insert_chart(
        'G7', flow_chart, {'x_offset': -50, 'y_offset': 0})

    last_date = processed_data['date_formatted'][processed_data.shape[0] - 1]
    origin_date = last_date - \
        dateutil.relativedelta.relativedelta(months=2, days=last_date.day - 1)
    daily_data = processed_data[processed_data.date_formatted >= origin_date]

    reference_data_start = 60

    daily_data_to_add = daily_data[['balance', 'date_stringtype']]
    daily_data_to_add.to_excel(
        writer, sheet_name, startrow=0, startcol=reference_data_start)

    daily_chart = get_daily_chart(
        workbook, sheet_name, daily_data_to_add, 0, reference_data_start)
    summary_sheet.insert_chart(
        'M7', daily_chart, {'x_offset': -80, 'y_offset': 0})

    parameters = get_summary_parameters(daily_data)

    # Summary Parameters
    summary_sheet.set_column(11, 12, 30)
    parameter_start = data_start
    for each_para, each_value in parameters.items():
        summary_sheet.set_row(parameter_start, 30)
        summary_sheet.write(parameter_start, 11, each_para, parameter_format)
        summary_sheet.write(parameter_start, 12, each_value, parameter_format)
        parameter_start = parameter_start + 1
    summary_sheet.set_row(parameter_start, 30)

    summary_sheet = insert_header(summary_sheet, sheet_name)
    summary_sheet.write(3, 0, "All Analysis done here are for last 3 months")

    return writer


def add_top_transaction_sheet(writer, processed_data, sheet_name):
    workbook = writer.book
    parameter_format = workbook.add_format({
        'border': 1,
        'bg_color': '#C6EFCE',
        'bold': True,
        'text_wrap': True,
        'align': 'center',
        'valign': 'vcenter',
        'indent': 1,
    })
    api1, raw_data = process_raw_data_to_add(processed_data)

    top_5_credit = raw_data[(raw_data['Transaction Type'] == 'credit')].sort_values(['Transaction Amount'],
                                                                                    ascending=False)[:5]
    top_5_debit = raw_data[(raw_data['Transaction Type'] == 'debit')].sort_values(['Transaction Amount'],
                                                                                  ascending=False)[:5]
    bottom_5_credit = raw_data[(raw_data['Transaction Type'] == 'credit')].sort_values(
        ['Transaction Amount'])[:5]
    bottom_5_debit = raw_data[(raw_data['Transaction Type'] == 'debit')].sort_values(
        ['Transaction Amount'])[:5]

    start_row = 5
    gap = 5
    top_5_credit.to_excel(writer, sheet_name, startrow=start_row, index=0)
    top_5_debit.to_excel(writer, sheet_name,
                         startrow=start_row + 5 + gap, index=0)
    bottom_5_credit.to_excel(
        writer, sheet_name, startrow=start_row + 10 + gap + gap, index=0)
    bottom_5_debit.to_excel(
        writer, sheet_name, startrow=start_row + 15 + gap + gap + gap, index=0)

    top_tansaction_worksheet = writer.sheets[sheet_name]

    top_tansaction_worksheet.merge_range(
        "A5:I5", "Top 5 Credit Transactions", parameter_format)
    top_tansaction_worksheet.merge_range(
        "A15:I15", "Top 5 Debit Transactions", parameter_format)
    top_tansaction_worksheet.merge_range(
        "A25:I25", "Bottom 5 Credit Transactions", parameter_format)
    top_tansaction_worksheet.merge_range(
        "A35:I35", "Bottom 5 Debit Transactions", parameter_format)

    top_tansaction_worksheet.hide_gridlines(2)
    top_tansaction_worksheet = insert_header(
        top_tansaction_worksheet, sheet_name)
    top_tansaction_worksheet.set_column(1, 2, 15)
    top_tansaction_worksheet.set_column(3, 7, 20)
    top_tansaction_worksheet.set_column(8, 8, 50)
    top_tansaction_worksheet.set_zoom(80)
    return writer


def add_lender_sheet(writer, processed_data, sheet_name):
    workbook = writer.book
    parameter_format = workbook.add_format({
        'border': 1,
        'bg_color': '#C6EFCE',
        'bold': True,
        'text_wrap': True,
        'align': 'center',
        'valign': 'vcenter',
        'indent': 1,
    })
    header_format = workbook.add_format({
        'border': 1,
        'bg_color': 'black',
        'bold': True,
        'font_color': 'white',
        'font_size': 16,
        'text_wrap': True,
        'align': 'center',
        'valign': 'vcenter',
        'indent': 1,
    })

    credit_df, debit_df = lender_list.get_lenders_parameter_for_excel(
        processed_data)
    all_lender_transactions = pd.DataFrame(
        lender_list.get_loan_transactions(processed_data))
    if all_lender_transactions.shape[0] == 0:
        all_lender_transactions = pd.DataFrame(
            columns=['amount', 'balance', 'date', 'hash', 'transaction_channel', 'transaction_note', 'transaction_type',
                     'merchant_category', 'date_formatted', 'date_stringtype', 'month_year', 'is_lender', 'merchant'])

    start_row = 4
    credit_df = credit_df.reset_index(drop=1)
    credit_df.index = credit_df.index + 1
    credit_df.to_excel(writer, sheet_name, startrow=start_row)
    lender_worksheet = writer.sheets[sheet_name]
    lender_worksheet.write('A' + str(start_row + 1),
                           "Loans Taken", parameter_format)

    gap = 4
    debit_df = debit_df.reset_index(drop=1)
    debit_df.index = debit_df.index + 1
    debit_df.to_excel(writer, sheet_name,
                      startrow=start_row + credit_df.shape[0] + gap)
    lender_worksheet.write(
        'A' + str(start_row + credit_df.shape[0] + gap + 1), "Loans Repayment", parameter_format)

    inter_gap = 0
    grouped_merchant = all_lender_transactions.groupby('merchant')
    start_transaction = start_row + \
        credit_df.shape[0] + gap + 1 + debit_df.shape[0] + gap + 2

    lender_worksheet.merge_range('A' + str(start_transaction) + ':C' + str(start_transaction + 1),
                                 "Individual Transactions of Each Merchant", parameter_format)
    start_transaction = start_transaction + 4

    for each, each_df in grouped_merchant:
        lender_worksheet.write(
            'A' + str(start_transaction + inter_gap), each, parameter_format)
        data_to_add = each_df[
            ['date_stringtype', 'transaction_type', 'transaction_channel', 'merchant', 'amount', 'balance',
             'transaction_note']]
        data_to_add = data_to_add.rename(columns={'date_stringtype': 'Date', 'transaction_type': 'Transaction Type',
                                                  'transaction_channel': 'Transaction Channel', 'merchant': 'Merchant',
                                                  'amount': 'Transaction Amount', 'balance': 'Balance',
                                                  'transaction_note': 'Note'})
        data_to_add['Transaction Channel'] = data_to_add['Transaction Channel'].apply(
            lambda x: x.replace('_', ' ').title())
        data_to_add['Balance'] = data_to_add['Balance'].apply(lambda x: int(x))
        data_to_add['Transaction Amount'] = data_to_add['Transaction Amount'].apply(
            lambda x: int(x))
        data_to_add.to_excel(writer, sheet_name,
                             startrow=start_transaction + inter_gap, index=0)
        inter_gap = inter_gap + data_to_add.shape[0] + 4

    lender_worksheet.set_column(0, 4, 20)
    lender_worksheet.set_column(5, 6, 30)
    lender_worksheet.merge_range("A1:H2", "LOAN PROFILE", header_format)
    # lender_worksheet = insert_header(lender_worksheet ,sheet_name)
    lender_worksheet.hide_gridlines(2)
    lender_worksheet.set_zoom(80)
    return writer

# NOT USED ANYMORE
def make_dashboard(data_path='/Users/piyush/Downloads/csv_for_testing.csv',
                   user_dict={'Name': 'Piyush', 'Bank': 'RBS', 'IFSC code': 'ICIC0001044'}):
    data = pd.read_csv(data_path)
    print(data.shape)

    processed_data = process_raw_data(data)

    raw_data = processed_data[
        ['date_formatted', 'amount', 'balance', 'transaction_note', 'transaction_type']]  # to be added in dashboard
    channel_wise = get_groupby(
        processed_data, 'transaction_channel', 'Channel', 1)

    # sheet name :
    name_summary = 'Summary'
    name_monthly = 'Monthly Analysis'
    name_daily = 'Daily Analysis'
    name_raw = 'Raw Data'
    name_credit = 'Credit Profile'
    name_debit = 'Debit Profile'
    name_salary = 'Salary Profile'
    name_lender = 'Loan Profile'
    name_top_transactions = 'Top 5 Transactions'

    writer = pd.ExcelWriter('BSM_dashboard_sample6.xlsx', engine='xlsxwriter')
    workbook = writer.book

    # add Raw data sheet
    raw_data_columns = ['date_formatted', 'amount', 'balance', 'transaction_note', 'transaction_type',
                        'transaction_channel', 'merchant_category', 'description']
    raw_data_to_add = processed_data[[
        x for x in processed_data.columns if x in raw_data_columns]]
    writer, raw_data_added = add_raw_data_sheet(
        writer, raw_data_to_add, name_raw)

    # add monthly data sheet
    writer, month_year, monthly_data_transposed, credit_debit_df, data_to_add, salary_df = add_monthly_data_sheet(
        writer, processed_data, channel_wise, name_monthly)

    # add Daily data sheet
    # writer = add_daily_data_sheet(writer, processed_data, month_year, raw_data, name_daily)

    # add credit and debit sheetss

    if processed_data[processed_data.transaction_type == 'credit'].shape[0] > 0:
        credit_recurring_list = recurring_transaction.get_credit_recurring_transactions(
            processed_data)
    else:
        credit_recurring_list = []
    if processed_data[processed_data.transaction_type == 'debit'].shape[0] > 0:
        debit_recurring_list = recurring_transaction.get_debit_recurring_transaction(
            processed_data)
    else:
        debit_recurring_list = []

    credit_recurring_df, credit_recurring_list = recurring_transaction.get_parameters(credit_recurring_list,
                                                                                      'Source')
    debit_recurring_df, debit_recurring_list = recurring_transaction.get_parameters(debit_recurring_list,
                                                                                    'Destination')

    writer = add_credit_sheet(writer, channel_wise, monthly_data_transposed, processed_data, credit_recurring_df,
                              credit_recurring_list, name_credit)

    writer = add_debit_sheet(writer, channel_wise, monthly_data_transposed, processed_data, debit_recurring_df,
                             debit_recurring_list, name_debit)

    # add lenders sheet
    writer = add_lender_sheet(writer, processed_data, name_lender)

    # add top transactions

    writer = add_top_transaction_sheet(
        writer, processed_data, name_top_transactions)

    # add Salary Sheet
    writer = add_salary_sheet(
        writer, monthly_data_transposed, salary_df, sheet_name=name_salary)

    # add summary
    writer = add_summary_sheet(
        writer, monthly_data_transposed, processed_data, name_summary)

    # add Home

    writer = add_home_sheet(writer, user_dict)

    sheet_order = {
        'HOME': 1,
        name_raw: 2,
        name_summary: 3,
        name_monthly: 4,
        name_salary: 5,
        name_lender: 6,
        name_daily: 7,
        name_credit: 8,
        name_debit: 9,
        name_top_transactions: 10

    }
    workbook.worksheets_objs.sort(key=lambda x: sheet_order[x.name])

    writer.save()
    return 1

def add_bounce_data_sheet(writer, bounce_data, sheet_name):
    bounce_data['Credit'] = bounce_data[bounce_data['Transaction Type']
                                                                  == 'credit']['Transaction Amount']
    bounce_data['Debit'] = bounce_data[bounce_data['Transaction Type']
                                                                 == 'debit']['Transaction Amount']
    bounce_data = bounce_data.drop(
        ['Transaction Amount'], axis=1)
    bounce_data = bounce_data[[
        'S. No.', 'Date', 'Transaction Type', 'Transaction Channel', 'Description', 'Merchant Category', 'Credit', 'Debit', 'Balance', 'Note']]
    bounce_data.to_excel(writer, sheet_name, index=0)
    bounce_data_worksheet = writer.sheets[sheet_name]
    workbook = writer.book
    header_format = workbook.add_format(
        {'bold': True, 'bg_color': '#D9D9D9', 'font_color': 'black'})
    value_format = workbook.add_format(
        {'bold': 0, 'font_color': 'black', 'text_wrap': True})
    value_format.set_align('center')
    value_format.set_align('vcenter')
    bad_format = workbook.add_format(
        {'bold': 0, 'bg_color': '#FFE9D7', 'font_color': 'black', 'text_wrap': True})
    bounce_data_worksheet.conditional_format('$A1:' + str(b[bounce_data.shape[1] - 1]) + '1', {'type': 'cell',
                                                                                                         'criteria': "<>",
                                                                                                         'value': -11,
                                                                                                         'format': header_format})
    bounce_data_worksheet.conditional_format('$A$2:$J$' + str(bounce_data.shape[0] + 1),
                                          {'type': 'cell', 'criteria': '<>', 'value': -11, 'format': value_format})
    bounce_data_worksheet.set_column(0, 0, 6)
    bounce_data_worksheet.set_column(1, 2, 13)
    bounce_data_worksheet.set_column(3, 3, 20)
    bounce_data_worksheet.set_column(4, 5, 25)
    bounce_data_worksheet.set_column(6, 8, 13)
    bounce_data_worksheet.set_column(9, 9, 55)
    for each in range(0, bounce_data.shape[0] + 1):
        bounce_data_worksheet.set_row(each, 30)
    bounce_data_worksheet.hide_gridlines(2)
    return writer

# NOT USED ANYMORE
def create_excel_report(txn_list, filename=None, user_dict=dict(), write_path='/tmp/'):
    data = pd.DataFrame(txn_list)
    processed_data = process_raw_data(data)
    # this is test
    raw_data = processed_data[
        ['date_formatted', 'amount', 'balance', 'transaction_note', 'transaction_type']]  # to be added in dashboard
    channel_wise = get_groupby(
        processed_data, 'transaction_channel', 'Channel', 1)
    # sheet name :
    name_summary = 'Summary'
    name_monthly = 'Monthly Analysis'
    name_daily = 'Daily Analysis'
    name_raw = 'Raw Data'
    name_credit = 'Credit Profile'
    name_debit = 'Debit Profile'
    name_salary = 'Salary Profile'
    name_lender = 'Loan Profile'
    name_top_transactions = 'Top 5 Transactions'
    name_bounce_transaction = 'Bounce Transactions'
    writer = pd.ExcelWriter(write_path + filename.replace('.xls', '').replace('.xlsx', '') + '.xlsx',
                            engine='xlsxwriter')
    workbook = writer.book
    # add Raw data sheet
    raw_data_columns = ['date_formatted', 'amount', 'balance', 'transaction_note', 'transaction_type',
                        'transaction_channel', 'merchant_category', 'description']
    raw_data_to_add = processed_data[[
        x for x in processed_data.columns if x in raw_data_columns]]
    writer, raw_data_added = add_raw_data_sheet(
        writer, raw_data_to_add, name_raw)
    # add bounce sheet
    bounce_tags =['outward_cheque_bounce','inward_cheque_bounce', 'auto_debit_payment_bounce', 'Outward Cheque Bounce',
                 'Inward Cheque Bounce','Auto Debit Payment Bounce']
    bounce_data = raw_data_added[raw_data_added['Transaction Channel'].isin(bounce_tags)]
    writer = add_bounce_data_sheet(writer,bounce_data,name_bounce_transaction)
    # add monthly data sheet
    writer, month_year, monthly_data_transposed, credit_debit_df, data_to_add, salary_df = add_monthly_data_sheet(
        writer,
        processed_data,
        channel_wise,
        name_monthly)
    # add Daily data sheet
    # writer = add_daily_data_sheet(writer, processed_data, month_year, raw_data, name_daily)
    # add credit and debit sheets
    if data[data.transaction_type == 'credit'].shape[0] > 0:
        credit_recurring_list = recurring_transaction.get_credit_recurring_transactions(
            data)
    else:
        credit_recurring_list = []
    if data[data.transaction_type == 'debit'].shape[0] > 0:
        debit_recurring_list = recurring_transaction.get_debit_recurring_transaction(
            data)
    else:
        debit_recurring_list = []
    credit_recurring_df, credit_recurring_list = recurring_transaction.get_parameters(credit_recurring_list,
                                                                                      'Source')
    debit_recurring_df, debit_recurring_list = recurring_transaction.get_parameters(debit_recurring_list,
                                                                                    'Destination')
    writer = add_credit_sheet(writer, channel_wise, monthly_data_transposed, processed_data, credit_recurring_df,
                              credit_recurring_list, name_credit)
    writer = add_debit_sheet(writer, channel_wise, monthly_data_transposed, processed_data, debit_recurring_df,
                             debit_recurring_list, name_debit)
    # add lenders sheet
    writer = add_lender_sheet(writer, processed_data, name_lender)
    writer = add_top_transaction_sheet(
        writer, processed_data, name_top_transactions)
    # add Salary Sheet
    writer = add_salary_sheet(
        writer, monthly_data_transposed, salary_df, sheet_name=name_salary)
    # add summary
    writer = add_summary_sheet(
        writer, monthly_data_transposed, processed_data, name_summary)
    # add Home
    writer = add_home_sheet(writer, user_dict)
    sheet_order = {
        'HOME': 1,
        name_raw: 2,
        name_summary: 3,
        name_monthly: 4,
        name_salary: 5,
        name_lender: 6,
        name_daily: 7,
        name_credit: 8,
        name_debit: 9,
        name_top_transactions: 10,
        name_bounce_transaction:11
    }
    workbook.worksheets_objs.sort(key=lambda x: sheet_order[x.name])
    writer.save()
    return filename + '.xlsx', write_path + filename + '.xlsx'


# API 0
def get_user_dict(txn_list=0, user_dict=dict()):
    return user_dict


# API 1
def produce_raw_data(txn_list=0):
    data = pd.DataFrame(txn_list)
    # data = pd.read_csv('/Users/piyush/Downloads/gangs.csv')
    processed_data = process_raw_data(data)
    raw_data, raw_data_with_channel = process_raw_data_to_add(processed_data)
    return_data = raw_data.to_dict('records')
    for i in range(0, len(return_data)):
        return_data[i] = convert_keys_to_lower_case(return_data[i])
    return return_data


# API 2
def produce_raw_data_with_channel(txn_list=0):
    data = pd.DataFrame(txn_list)
    # data = pd.read_csv('/Users/piyush/Downloads/gangs.csv')
    processed_data = process_raw_data(data)
    raw_data, raw_data_with_channel = process_raw_data_to_add(processed_data)
    return_data = raw_data_with_channel.to_dict('records')
    for i in range(0, len(return_data)):
        return_data[i] = convert_keys_to_lower_case(return_data[i])
    return return_data


# API 3
def produce_basic_features_part(txn_list=0):
    data = pd.DataFrame(txn_list)
    # data = pd.read_csv('/Users/piyush/Downloads/gangs.csv')
    processed_data = process_raw_data(data)
    monthly_data, salary_df = get_month_wise_features(processed_data)
    basic_part1 = ['cnt_transactions', 'cnt_debit', 'cnt_credit', 'opening_balance', 'amt_debit', 'amt_credit',
                   'closing_balance', 'net_cash_inflow', 'min_bal', 'max_bal', 'avg_bal', 'max_eod_balance',
                   'min_eod_balance', 'avg_credit_transaction_size', 'avg_debit_transaction_size']
    basic_features_part1 = monthly_data[basic_part1]
    basic_features_part1 = basic_features_part1.rename(
        convert_monthyear_to_yearmonth, axis='index')
    basic_features_part1_dict = basic_features_part1.to_dict('index')
    basic_features_part1_list = [{'month_year': key, 'data': value}
                                 for key, value in basic_features_part1_dict.items()]
    return basic_features_part1_list


# API 4
# NOT USED ANYMORE
def produce_basic_features_full(txn_list=0):
    data = pd.DataFrame(txn_list)
    # data = pd.read_csv('/Users/piyush/Downloads/gangs.csv')
    processed_data = process_raw_data(data)
    basic_features_full, salary_df = get_month_wise_features(processed_data)
    basic_features_full = basic_features_full.rename(
        convert_monthyear_to_yearmonth, axis='index')
    basic_feautures_full_dict = basic_features_full.to_dict('index')
    basic_feautures_full_list = [{'month_year': key, 'data': value}
                                 for key, value in basic_feautures_full_dict.items()]
    return basic_feautures_full_list


def convert_monthyear_to_yearmonth(monthyear):
    month_dict = {
        'Jan': '01',
        'Feb': '02',
        'Mar': '03',
        'Apr': '04',
        'May': '05',
        'Jun': '06',
        'Jul': '07',
        'Aug': '08',
        'Sep': '09',
        'Oct': '10',
        'Nov': '11',
        'Dec': '12'
    }
    month_number = month_dict[monthyear[:3]]
    year_number = monthyear[4:8]
    return year_number + '-' + month_number


def clean_recurring_list(recurring_list, tag):
    tag_counter = 1
    keep_key = [
        'transaction_type', 'transaction_note', 'chq_num', 'amount', 'balance', 'date', 
        'transaction_channel', 'hash', 'merchant_category', 'description', 'account_id', 'month_year', 
        'salary_month', 'category', 'perfios_txn_category', 'clean_transaction_note'
    ]
    return_list = []
    for each in range(0, len(recurring_list)):
        obj = {}
        for tansaction_number in range(0, len(recurring_list[each])):
            recurring_list[each][tansaction_number] = dict(
                (key, value) for key, value in recurring_list[each][tansaction_number].items() if key in keep_key)
        obj[tag.lower()] = tag + str(tag_counter)
        obj['transactions'] = recurring_list[each]
        tag_counter = tag_counter + 1
        return_list.append(obj)
    return return_list


# API 5

def produce_advanced_features(txn_list=[], use_workers=False):
    for txn in txn_list:
        for key in txn.keys():
            if txn[key] is None:
                txn[key]=""
    
    data = pl.DataFrame(txn_list)
    columns = data.columns
    required_columns = ['transaction_type', 'transaction_note', 'amount', 'date', 'balance', 'transaction_channel', 'merchant_category', 'description', 'hash', 'category', 'account_id', 'unclean_merchant']
    
    final_columns = [elem for elem in required_columns if elem in columns]
    data = data.select(pl.col(final_columns))

    recurring_parameters = get_recurring_parameters(data, use_workers)
    return recurring_parameters


def get_recurring_parameters(data, use_workers=False):
    if data.filter(pl.col('transaction_type') == 'credit').height > 0:
        credit_recurring_list = recurring_transaction.get_credit_recurring_transactions(data, use_workers)
    else:
        credit_recurring_list = []
    if data.filter(pl.col('transaction_type') == 'debit').height > 0:
        debit_recurring_list = recurring_transaction.get_debit_recurring_transaction(data, use_workers)
    else:
        debit_recurring_list = []

    clean_credit_transaction_recurring_list = clean_recurring_list(credit_recurring_list, 'Destination')
    clean_debit_transaction_recurring_list = clean_recurring_list(debit_recurring_list, 'Source')

    ans = {
        'recurring_credit_transactions': clean_credit_transaction_recurring_list,
        'recurring_debit_transactions': clean_debit_transaction_recurring_list,
    }
    return ans

def lower_case_keys(listx):
    for i in range(0, len(listx)):
        for key, value in listx[i].items():
            if key.lower().replace(' ', '_') != key:
                listx[i][key.lower().replace(' ', '_')] = listx[i][key]
                del listx[i][key]
    return listx


def convert_keys_to_lower_case(dictx):
    obj = {}
    for key, value in dictx.items():
        obj[key.lower().replace(" ", '_')] = value
    return obj


def get_basic_features(df):
    obj = dict()
    obj['Month Year'] = df['month_year'].min()
    obj['date_formatted'] = datetime.datetime(
        df['date_formatted'][0].year, df['date_formatted'][0].month, 1).date()

    obj['cnt_transactions'] = df.shape[0]
    obj['cnt_debit'] = df[df.transaction_type == 'debit'].shape[0]
    obj['cnt_credit'] = df[df.transaction_type == 'credit'].shape[0]

    if df['transaction_type'][df.shape[0] - 1] == 'debit':
        obj['opening_balance'] = df['balance'][df.shape[0] - 1] + \
            df['amount'][df.shape[0] - 1]
    elif df['transaction_type'][df.shape[0] - 1] == 'credit':
        obj['opening_balance'] = df['balance'][df.shape[0] - 1] - \
            df['amount'][df.shape[0] - 1]
    else:
        obj['opening_balance'] = df['balance'][df.shape[0] - 1]
    obj['amt_debit'] = df[df.transaction_type == 'debit']['amount'].sum()
    obj['amt_credit'] = df[df.transaction_type == 'credit']['amount'].sum()
    obj['closing_balance'] = df['balance'][0]
    obj['net_cash_inflow'] = obj['amt_credit'] - obj['amt_debit']

    obj['min_bal'] = min(df['balance'].min(), obj['opening_balance'])
    obj['max_bal'] = max(df['balance'].max(), obj['opening_balance'])

    obj['avg_bal'], obj['median_balance'], obj['mode_balance'] = get_daily_balance_mmm(
        df)

    obj['max_eod_balance'] = df.groupby(
        'date_formatted', sort=0).first()['balance'].max()
    obj['min_eod_balance'] = df.groupby(
        'date_formatted', sort=0).first()['balance'].min()

    obj['avg_credit_transaction_size'] = df[df.transaction_type ==
                                            'credit'].amount.mean()
    obj['avg_debit_transaction_size'] = df[df.transaction_type == 'debit'].amount.mean()

    return obj
