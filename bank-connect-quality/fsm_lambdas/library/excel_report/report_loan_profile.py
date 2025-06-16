from library.excel_report.report_formats import transaction_format_func, months_formats_func
from library.excel_report.report_transactions import write_transaction_column, transaction_column_names
from datetime import datetime


def loan_description(cred_deb, order_list, row, work_sheet, months_formats, loan_dict):

    count = 1
    for merchant in loan_dict[cred_deb]:
        col = 0
        flag = True
        loan_dict[cred_deb][merchant]['Average Amount'] = round(loan_dict[cred_deb][merchant]['Total Amount']/loan_dict[cred_deb][merchant]['Number of Transactions'], 2)
        loan_dict[cred_deb][merchant]['Average Balance Before Transaction'] = round(loan_dict[cred_deb][merchant]['Balance Before Transaction']/loan_dict[cred_deb][merchant]['Number of Transactions'], 2)
        for order in order_list:
            if flag:
                work_sheet.write(row, col, str(count), months_formats['debit_generic_cell'])
                count += 1
                flag = False
            elif order == 'First Date':
                date = datetime.strptime(loan_dict[cred_deb][merchant][order], '%d-%b-%y')
                work_sheet.write(row, col, date, months_formats['date_generic_cell'])
            elif order == 'Lender Name':
                work_sheet.write(row, col, merchant, months_formats['debit_generic_cell'])
            else:
                work_sheet.write(row, col, loan_dict[cred_deb][merchant][order], months_formats['debit_generic_cell'])
            col += 1
        row += 1


def loan_profile_func(workbook, loan_dict, all_loan_transactions, workbook_num='', version='v1',vdict={}):

    months_formats = months_formats_func(workbook)
    transaction_formats = transaction_format_func(workbook)
    work_sheet = workbook.add_worksheet('Loan Profile'+workbook_num)
    work_sheet.write('A1', 'Loan Profile', months_formats['primary_heading'])
    row = 0
    col = 0

    if version=='v3':
        obligation_vars = ['Total No. of EMI/Loan Payment', 'Total EMI/Loan Payment of the Month', 'CC Interest Amount', 'Total Obligation']
        row += 3
        work_sheet.write(row, col, 'Obligation', months_formats['primary_heading'])
        row += 1
        col += 1
        work_sheet.write(row, col, 'Field', transaction_formats['horizontal_heading_cell'])
        var_row = row
        for var in obligation_vars:
            var_row += 1
            work_sheet.write(var_row, col, var, months_formats['debit_generic_cell'])
        months_order = list(vdict['Total No. of EMI/Loan Payment'].keys())
        for month in months_order:
            short_month = datetime.strptime(month, "%b-%Y").strftime("%b-%y")
            col += 1
            work_sheet.write(row, col, short_month, transaction_formats['horizontal_heading_cell'])
            var_row = row
            for var in obligation_vars:
                var_row += 1
                work_sheet.write(var_row, col, vdict[var].get(month, 0.00), months_formats['number_cell'])

        loan_emis = vdict['Loan EMIs']
        emi_formatted = {}
        emi_count = {}
        measures = {"min":[None]*len(months_order), "max":[None]*len(months_order)}
        for index, (month, month_emis) in enumerate(loan_emis.items()):
            for merchant, emis in month_emis.items():
                for emi in emis:
                    date = datetime.strptime(emi['date'], '%Y-%m-%d %H:%M:%S')
                    
                    merchant_amount = f"{merchant}_{emi['amount']}"
                    value = date.day
                    
                    if merchant_amount not in emi_count:
                        emi_count[merchant_amount] = 1
                        
                    retry = 0   
                    while retry < emi_count[merchant_amount]:
                        retry += 1
                        key = f"{merchant_amount}_({retry})"

                        if key not in emi_formatted:
                            emi_formatted[key] = [None]*len(months_order)
                    
                        if emi_formatted[key][index] is None:
                            emi_formatted[key][index] = value
                            if measures['min'][index] is None:
                                measures['min'][index] = value
                            else:
                                measures['min'][index] = min(measures['min'][index], value)
                            if measures['max'][index] is None:
                                measures['max'][index] = value
                            else:
                                measures['max'][index] = max(measures['max'][index], value)
                            break
                        else:
                            emi_count[merchant_amount] += 1
        emi_formatted = dict(sorted(emi_formatted.items(), key=lambda item: item[0])) 
        
        row = var_row+1
        col = 1
        work_sheet.write(row, col, '', months_formats['pivot_cell'])
        row += 1
        work_sheet.write(row, col, "Min Date", months_formats['debit_generic_cell'])
        work_sheet.write_row(row, col+1, measures['min'], months_formats['debit_generic_cell'])
        row += 1
        work_sheet.write(row, col, "Max Date", months_formats['debit_generic_cell'])
        work_sheet.write_row(row, col+1, measures['max'], months_formats['debit_generic_cell'])
        row += 1
        work_sheet.write(row, col, 'EMI Amount', transaction_formats['horizontal_heading_cell'])
        col -= 1
        work_sheet.write(row, col, 'Bank Name/Institution Name', transaction_formats['horizontal_heading_cell'])

        var_row = row
        for merchant_amount, monthwise_emi in emi_formatted.items():
            var_row += 1
            merchant_amount = merchant_amount.rsplit('_', maxsplit=1)[0]
            name = merchant_amount.rsplit('_', maxsplit=1)[0]
            amount = merchant_amount.rsplit('_', maxsplit=1)[1]
            work_sheet.write(var_row, col, name, months_formats['debit_generic_cell'])
            col += 1
            work_sheet.write(var_row, col, amount, months_formats['number_cell'])
            col += 1
            work_sheet.write_row(var_row, col, monthwise_emi, months_formats['debit_generic_cell'])
            col -= 2
        row = var_row
            
    row += 3
    col = 0
    if len(all_loan_transactions) == 0:
        return

    loan_heading = ['Lender Name', 'Number of Transactions', 'Average Amount', 'Total Amount', 'Average Balance Before Transaction', 'First Date']
    work_sheet.write(row, col, 'Loans Taken', months_formats['pivot_cell'])
    col += 1
    for co, parameters in enumerate(loan_heading):
        work_sheet.write(row, co+col, parameters, transaction_formats['horizontal_heading_cell'])
    row += 1
    col = 0
    if len(loan_dict['credit']) > 0:
        loan_description('credit', ['Loans Taken'] + loan_heading, row, work_sheet, months_formats, loan_dict)

    row += len(loan_dict['credit']) + 3

    work_sheet.write(row, col, 'Loans Repayment', months_formats['pivot_cell'])
    col += 1
    for co, parameters in enumerate(loan_heading):
        work_sheet.write(row, co+col, parameters, transaction_formats['horizontal_heading_cell'])
    row += 1

    if len(loan_dict['debit']) > 0:
        loan_description('debit',  ['Loans Repayment'] + loan_heading, row, work_sheet, months_formats, loan_dict)

    row += len(loan_dict['debit']) + 3

    row += 1

    work_sheet.write(row, 0, 'Individual Transactions of Each Merchant', months_formats['primary_heading'])

    for merchant in all_loan_transactions:
        row += 3
        work_sheet.write(row, 0, merchant, months_formats['primary_heading'])
        row += 1
        transaction_column_names(row, work_sheet, transaction_formats)
        row += 1

        for ro, transaction_data in enumerate(all_loan_transactions[merchant]):
            # print(transaction_data)
            write_transaction_column(work_sheet, row, transaction_data, transaction_formats, ro+1)
            row += 1

    size = [['A', 15], ['B', 12], ['C', 21], ['D', 25], ['E', 12], ['F', 30]]

    for i in range(len(size)):
        s = '{}:{}'.format(size[i][0], size[i][0])
        work_sheet.set_column(s, size[i][1])
