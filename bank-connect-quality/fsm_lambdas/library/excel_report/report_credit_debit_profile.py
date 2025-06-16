from library.excel_report.report_formats import transaction_format_func, months_formats_func
from library.excel_report.report_transactions import write_transaction_column, transaction_column_names, write_transaction_with_single_category, SingleCategoryTransactionsColumns, transaction_column_names_single_category
from xlsxwriter.utility import xl_rowcol_to_cell
from datetime import datetime


def add_group(data_dict, name):
    data_dict[name] = {
        'amount of transaction': [],
        'balance after transaction': []
        }
    return data_dict

def add_month_wise_group(data_dict, name, months_order):
    data_dict[name] = {}
    for month in months_order:
        data_dict[name][month] = {
                            'amount of transaction': [],
                            'balance after transaction': []
                    }
    return data_dict


data_vars = ['Total Amount', 'Median Amount',	'Number of Transactions',	'Max Amount',	'Average Balance at Transaction',	'Frequency']

def credit_debit_profile_func(workbook, total_days, cred_deb, tags, transactions, workbook_num='', version='v1', personal_data = dict()):

    hyperlink_address = {}
    data_dict = {'orders': []}

    row = 0
    sheet_name = cred_deb + ' Profile' + workbook_num
    sheet_name_prev = sheet_name
    if version=='v5':
        sheet_name = 'Recurring ' + cred_deb + 's'
    work_sheet = workbook.add_worksheet(sheet_name)
    transaction_formats = transaction_format_func(workbook)
    months_formats = months_formats_func(workbook)

    work_sheet.write(row, 0, 'Individual Transactions of Each Destination', months_formats['primary_heading'])

    tuple_tags = []
    max_tags = []
    for key in tags:
        tuple_tags.append((key, tags[key][2]))
        max_tags.append((key, tags[key][3]))

    length_to_sub = 0
    filtered_transactions = transactions

    if version=='v5' and cred_deb=='Credit':
        credit_filtered_transaction_groups = []
        for group in transactions:
            if len(group['transactions'])<2:
                length_to_sub+=1
                continue

            total_amount = 0
            for transac in group['transactions']:
                total_amount += transac.get('amount')
            
            if total_amount<100000:
                length_to_sub+=1
                continue
            credit_filtered_transaction_groups.append(group)
        
        filtered_transactions = credit_filtered_transaction_groups

    row += 22 + len(transactions) 
    if version == 'v5':
        row-=20
        row-=length_to_sub

    for group in filtered_transactions:
        group_name = None
        flag = True
        ro = 0
        for transac in group['transactions']:
            if flag:
                group_name = transac.get('clean_transaction_note', '')
                data_dict = add_group(data_dict, group_name)
                row += 3
                data_dict['orders'].append(group_name)
                work_sheet.write(row, 0, group_name, months_formats['primary_heading'])
                hyperlink_address[group_name] = [row, 0]
                row += 1
                if version == 'v5':
                    transaction_column_names_single_category(row, work_sheet, transaction_formats, headings_key=SingleCategoryTransactionsColumns.get_columns(SingleCategoryTransactionsColumns.CREDIT_DEBIT_IN_ONE_ROW))
                else:
                    transaction_column_names(row, work_sheet, transaction_formats)
                row += 1
                flag = False

            data_dict[group_name]['amount of transaction'].append(transac['amount'])
            data_dict[group_name]['balance after transaction'].append(transac['balance'])
            transac['date'] = datetime.strftime(datetime.strptime(transac['date'], "%Y-%m-%d %H:%M:%S"), '%d-%b-%y')

            # need to do this
            # if version=='v5':
            #     print(transac)
            #     write_transaction_with_single_category(work_sheet, row, transac, transaction_formats, SingleCategoryTransactionsColumns.get_columns(SingleCategoryTransactionsColumns.CREDIT_DEBIT_IN_ONE_ROW))
            # else:
            if version=='v5':
                write_transaction_with_single_category(work_sheet, row, transac, transaction_formats, SingleCategoryTransactionsColumns.get_columns(SingleCategoryTransactionsColumns.CREDIT_DEBIT_IN_ONE_ROW), personal_data)
            else:
                write_transaction_column(work_sheet, row, transac, transaction_formats, ro+1)
            ro += 1
            row += 1

        data_dict[group_name]['Total Amount'] = sum(data_dict[group_name]['amount of transaction'])
        data_dict[group_name]['Number of Transactions'] = len(data_dict[group_name]['amount of transaction'])
        data_dict[group_name]['Max Amount'] = max(data_dict[group_name]['amount of transaction'])
        data_dict[group_name]['Average Balance at Transaction'] = round(sum(data_dict[group_name]['balance after transaction'])/len(data_dict[group_name]['balance after transaction']), 2)

        if total_days == 0:
            total_days = 1
        data_dict[group_name]['Frequency'] = round(float(len(data_dict[group_name]['amount of transaction']))/float(total_days), 3)
        n = data_dict[group_name]['Number of Transactions']
        if n % 2 == 0:
            median1 = data_dict[group_name]['amount of transaction'][n//2]
            median2 = data_dict[group_name]['amount of transaction'][n//2 - 1]
            median = (median1 + median2)/2
        else:
            median = data_dict[group_name]['amount of transaction'][n//2]
        data_dict[group_name]['Median Amount'] = median

    row = 0

    if version!='v5':
        work_sheet.write(row, 0, sheet_name_prev, months_formats['primary_heading'])
        row += 2

        max_sorted_tags = sorted(max_tags, key=lambda x: x[1], reverse=True)
        for co, var in enumerate(max_sorted_tags):
            if var[1] == 0:
                break
            values = ''
            val = var[0]
            val = val.split('_')
            for i in val:
                if i == 'chq':
                    i = 'Cheque'
                elif i == 'withdrawl':
                    i = 'withdrawal'
                else:
                    pass
                values += i.capitalize() + ' '

            work_sheet.write(row, 1, 'Max ' + values, months_formats['vertical_heading_cell'])
            work_sheet.write(row, 2, str(var[1]), months_formats['text_box_cell'])
            row += 1
        row += 2

        work_sheet.write(row, 0, 'Total Amount ' + cred_deb + 'ed Through Different Channels', months_formats['primary_heading'])
        row += 2
        col = 1

        credit_vars = sorted(tuple_tags, key=lambda x: x[1], reverse=True)
        for co, var in enumerate(credit_vars):
            if var[1] == 0:
                break
            values = ''
            val = var[0]
            val = val.split('_')
            for i in val:
                if i == 'chq':
                    i = 'Cheque'
                elif i == 'withdrawl':
                    i = 'withdrawal'
                else:
                    pass
                values += i.capitalize() + ' '
            work_sheet.write(row, col + co, values, transaction_formats['horizontal_heading_cell'])
            work_sheet.write(row+1, col + co, str(var[1]), months_formats['text_box_cell'])
        row += 4
        work_sheet.write(row, 0, 'Source Transaction', months_formats['primary_heading'])
    else:
        work_sheet.write(row, 0, sheet_name_prev, months_formats['primary_heading'])

    row += 1
    col = 1
    work_sheet.write(row, col, 'Source', months_formats['pivot_cell'])
    col += 1
    for co, var in enumerate(data_vars):
        work_sheet.write(row, col + co, var, transaction_formats['horizontal_heading_cell'])
    row += 1
    col = 1
    for source in data_dict['orders']:

        col = 1
        cell = xl_rowcol_to_cell(hyperlink_address[source][0], hyperlink_address[source][1])
        work_sheet.write_url(row, col, "internal:" + str(cell), months_formats['vertical_heading_cell'], string=source)

        col += 1
        for co, var in enumerate(data_vars):
            work_sheet.write(row, col + co, data_dict[source][var], months_formats['text_box_cell'])
        row += 1

    size = [['B', 25], ['C', 10], ['D', 35], ['E', 12], ['F', 17], ['G', 20], ['H', 12]]

    for i in range(len(size)):
        s = '{}:{}'.format(size[i][0], size[i][0])
        work_sheet.set_column(s, size[i][1])

def credit_debit_profile_func_indonesia(workbook, total_days, cred_deb, tags, transactions, months_order, workbook_num=''):
    hyperlink_address = {}
    data_dict = {'orders': []}
    data_vars_id = ['Number of Transactions', 'Max Amount', f'Total {cred_deb} Amount', 'Average Amount']

    row = 0
    sheet_name = cred_deb + ' Profile' + workbook_num
    work_sheet = workbook.add_worksheet(sheet_name)
    transaction_formats = transaction_format_func(workbook)
    months_formats = months_formats_func(workbook)

    work_sheet.write(row, 0, 'Individual Transactions of Each Destination', months_formats['primary_heading'])

    tuple_tags = []
    max_tags = []
    for key in tags:
        tuple_tags.append((key, tags[key][2]))
        max_tags.append((key, tags[key][3]))

    row += 22 + len(transactions)

    for group in transactions:
        group_name = None
        flag = True
        ro = 0
    
        for transac in group:
            if flag:
                group_name = transac.get('unclean_merchant', '')
                data_dict = add_month_wise_group(data_dict, group_name, months_order)
                row += 3
                data_dict['orders'].append(group_name)
                work_sheet.write(row, 0, group_name, months_formats['primary_heading'])
                hyperlink_address[group_name] = [row, 0]
                row += 1
                transaction_column_names(row, work_sheet, transaction_formats)
                row += 1
                flag = False
            transac['date'] = datetime.strftime(datetime.strptime(transac['date'], "%Y-%m-%d %H:%M:%S"), '%d-%b-%y')
            current_month = datetime.strptime(transac['date'], "%d-%b-%y")
            current_month = current_month.strftime("%b-%y")
            data_dict[group_name][current_month]['amount of transaction'].append(transac['amount'])
            data_dict[group_name][current_month]['balance after transaction'].append(transac['balance'])

            write_transaction_column(work_sheet, row, transac, transaction_formats, ro+1)
            ro += 1
            row += 1

        for month in months_order:
            if group_name is not None:
                data_dict[group_name][month][f'Total {cred_deb} Amount'] = sum(data_dict[group_name][month]['amount of transaction'])
                data_dict[group_name][month]['Number of Transactions'] = len(data_dict[group_name][month]['amount of transaction'])
                if data_dict[group_name][month]['Number of Transactions'] > 0:
                    data_dict[group_name][month]['Max Amount'] = max(data_dict[group_name][month]['amount of transaction'])
                    data_dict[group_name][month]['Average Amount'] = round(data_dict[group_name][month][f'Total {cred_deb} Amount']/data_dict[group_name][month]['Number of Transactions'], 2)
                else:
                    data_dict[group_name][month]['Max Amount'] = 0
                    data_dict[group_name][month]['Average Amount'] = 0

    row = 0
    work_sheet.write(row, 0, sheet_name, months_formats['primary_heading'])
    row += 2

    max_sorted_tags = sorted(max_tags, key=lambda x: x[1], reverse=True)
    for co, var in enumerate(max_sorted_tags):
        if var[1] == 0:
            break
        values = ''
        val = var[0]
        val = val.split('_')
        for i in val:
            if i == 'chq':
                i = 'Cheque'
            elif i == 'withdrawl':
                i = 'withdrawal'
            else:
                pass
            values += i.capitalize() + ' '

        work_sheet.write(row, 1, 'Max ' + values, months_formats['vertical_heading_cell'])
        work_sheet.write(row, 2, str(var[1]), months_formats['text_box_cell'])
        row += 1
    row += 2
    work_sheet.write(row, 0, 'Total Amount ' + cred_deb + 'ed Through Different Channels', months_formats['primary_heading'])
    row += 2
    col = 1

    credit_vars = sorted(tuple_tags, key=lambda x: x[1], reverse=True)
    for co, var in enumerate(credit_vars):
        if var[1] == 0:
            break
        values = ''
        val = var[0]
        val = val.split('_')
        for i in val:
            if i == 'chq':
                i = 'Cheque'
            elif i == 'withdrawl':
                i = 'withdrawal'
            else:
                pass
            values += i.capitalize() + ' '
        work_sheet.write(row, col + co, values, transaction_formats['horizontal_heading_cell'])
        work_sheet.write(row+1, col + co, str(var[1]), months_formats['text_box_cell'])
    row += 4
    work_sheet.write(row, 0, 'Source Transaction', months_formats['primary_heading'])

    row += 1
    col = 1
    work_sheet.write(row, col, 'Source', months_formats['pivot_cell'])
    col += 1
    
    m_col = col
    for month in months_order:
        work_sheet.merge_range(row - 1, m_col, row - 1, m_col+3, month, months_formats['pivot_cell'])
        m_col += 4
    
    co = 0
    for month in months_order:
        for _, var in enumerate(data_vars_id):
            work_sheet.write(row, col + co, var, transaction_formats['horizontal_heading_cell'])
            co += 1
    work_sheet.write(row, col + co, f'Total {cred_deb} Amount', months_formats['pivot_cell']) 
    work_sheet.write(row, col + co + 1, 'Average Transaction', months_formats['pivot_cell'])   

    row += 1
    col = 1
    for source in data_dict['orders']:

        col = 1
        cell = xl_rowcol_to_cell(hyperlink_address[source][0], hyperlink_address[source][1])
        work_sheet.write_url(row, col, "internal:" + str(cell), months_formats['vertical_heading_cell'], string=source)
        total_amount = 0
        num_trans = 0
        col += 1
        co = 0
        for month in months_order:
            total_amount += data_dict[source][month][f'Total {cred_deb} Amount']
            num_trans += data_dict[source][month][f'Number of Transactions']
            for _, var in enumerate(data_vars_id):
                work_sheet.write(row, col + co, data_dict[source][month][var], months_formats['text_box_cell'])
                co += 1
        work_sheet.write(row, col + co, total_amount, months_formats['text_box_cell'])
        if num_trans > 0:
            work_sheet.write(row, col + co + 1, total_amount/num_trans, months_formats['text_box_cell'])
        row += 1

    size = [['B', 25], ['C', 10], ['D', 35], ['E', 12], ['F', 17], ['G', 20], ['H', 12]]
    if len(months_order) >= 2:
        for i in range(min((len(months_order) - 2)*4 + 2, 16)):
            size.append([chr(ord('J')+i+1), 12])

    for i in range(len(size)):
        s = '{}:{}'.format(size[i][0], size[i][0])
        work_sheet.set_column(s, size[i][1])
