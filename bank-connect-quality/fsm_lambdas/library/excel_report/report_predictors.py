from library.excel_report.report_formats import months_formats_func, transaction_format_func
from library.excel_report.excel_util import *

from datetime import datetime

def predictors_func(workbook, predictors, workbook_num='', version='v1'):
    monthly_analysis = predictors

    months_formats = months_formats_func(workbook)
    transaction_formats = transaction_format_func(workbook)
    work_sheet = workbook.add_worksheet('Predictors'+workbook_num)
    write_to_sheet(work_sheet, 0, 0,  data='Predictors', cell_format=transaction_formats['horizontal_heading_cell'])
    write_to_sheet(work_sheet, 0, 1, data='Values', cell_format=transaction_formats['horizontal_heading_cell'])
    personal = ['customer_name', 'bank_name', 'account_type', 'accountnumber', 'ifsc_code']
    row = 1
    for key in personal:
        k = key
        value = monthly_analysis.get(key, '')
        row += 1
        if key == 'accountnumber':
            key = 'Account Number'
        key = key.replace('_', " ")
        key = key.title()
                
        if key == 'Account Number':
            write_to_sheet(work_sheet, row, 1, value, months_formats['account_number_cell'])
        else:
            write_to_sheet(work_sheet, row, 1, value, months_formats['right_text_box_cell'])
        work_sheet.write(row, 0, key, months_formats['left_vertical_heading_cell'])
        if k in monthly_analysis:
            del monthly_analysis[k]
    row += 1
    if version=='v3':
        lis = zip(monthly_analysis.keys(), monthly_analysis.values())
    else:
        lis = sorted(zip(monthly_analysis.keys(), monthly_analysis.values()))
    for key, value in lis:
        key = key.replace('_', " ")
        key = key.title()
        if value is None:
            write_to_sheet(work_sheet, row, 1, "", months_formats['right_text_box_cell'])  
        elif (key == "Start Date") or (key == "End Date"):
            if value is not None:
                value = datetime.strptime(value, "%d-%b-%y")
            else:
                value = ""
            write_to_sheet(work_sheet, row, 1, value, months_formats['date_text_box_cell'])
        elif key[:5] == "Month":
            if key[-1].isnumeric():
                value = datetime.strptime(value, "%b-%y")
                write_to_sheet(work_sheet, row, 1, value, months_formats['month_date_text_box_cell'])
            else:
                write_to_sheet(work_sheet, row, 1, value, months_formats['right_text_box_cell'])
        else:
            write_to_sheet(work_sheet, row, 1, value, months_formats['right_text_box_cell'])

        write_to_sheet(work_sheet, row, 0, key, months_formats['left_vertical_heading_cell'])
        row += 1

    size = [['A', 30], ['B', 15]]

    for i in range(len(size)):
        s = '{}:{}'.format(size[i][0], size[i][0])
        work_sheet.set_column(s, size[i][1])
