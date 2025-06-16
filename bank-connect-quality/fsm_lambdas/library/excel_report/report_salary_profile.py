from library.excel_report.report_formats import transaction_format_func, months_formats_func
from library.excel_report.report_transactions import write_transaction_column, transaction_column_names, TransactionColumnsInXlsx
from datetime import datetime
from xlsxwriter.utility import xl_rowcol_to_cell


def salary_profile_func(workbook, salary_data, months_order,salary_confidence, workbook_num=''):

    months_formats = months_formats_func(workbook)
    work_sheet = workbook.add_worksheet('Salary Profile'+workbook_num)
    work_sheet.write('A1', 'Salary Profile', months_formats['primary_heading'])

    row = 1
    row += 2
    col = 1
    work_sheet.write(row,col,'Confidence Percentage',months_formats['pivot_cell'])
    work_sheet.write(row,col+1,salary_confidence,months_formats['text_box_cell'])
    row += 1
    work_sheet.write(row, col, 'Salary Parameters', months_formats['pivot_cell'])
    for co, month in enumerate(months_order):
        mon = datetime.strptime(month, '%b-%y')
        work_sheet.write(row, co+col+1, mon, months_formats['date_horizontal_heading_cell'])

    salary_parameters = [
        'Number of Salary Transactions',
        'Total Amount of Salary',
        '% Salary Spent on Bill Payment (7 days)',
        '% Salary Spent Through Cash Withdrawal (7 days)',
        '% Salary Spent through Debit Card (7 days)',
        '% Salary Spent through Net Banking (7 days)',
        '% Salary Spent through UPI (7 days)'
        ]

    text_body_cell = workbook.add_format({'font_color': '#000000', 'bg_color': '#F1f1f1', 'valign': 'vcenter', 'border': 1, 'font_size': 10, 'align': 'center', 'text_wrap': True, 'num_format': '#,##0.00\\%'})

    row += 1
    for ro, parameters in enumerate(salary_parameters):
        col = 1
        work_sheet.write(row, col, parameters, months_formats['vertical_heading_cell'])
        col += 1
        for co, month in enumerate(months_order):
            if parameters == 'Number of Salary Transactions':
                # print(salary_data[month]['Number of Salary Transactions'])
                work_sheet.write(row, col+co, (salary_data[month]['Number of Salary Transactions']), months_formats['text_box_cell'])
            elif parameters == 'Total Amount of Salary':
                work_sheet.write(row, col+co, (salary_data[month]['salary']), months_formats['text_box_cell'])
            else:

                work_sheet.write(row, col+co, salary_data[month][parameters], text_body_cell)
        row += 1
    col += 1

    row += 3
    work_sheet.write(row, 0, 'Salary Transactions', months_formats['primary_heading'])
    row += 3
    transaction_formats = transaction_format_func(workbook)

    transaction_column_names(row, work_sheet, transaction_formats)
    ## TODO Modify the main function and remove extra line of codes
    col_num = len(TransactionColumnsInXlsx.get_columns('CREDIT_DEBIT_IN_ONE_ROW'))
    cell = xl_rowcol_to_cell(0, col_num)
    s = '{}:{}'.format(cell[0], cell[0])
    cell_width = 15
    work_sheet.set_column(s, cell_width)
    work_sheet.write_string(row, col_num, 'Salary of Month', transaction_formats['horizontal_heading_cell'])
    row += 1

    for ro, transaction_data in enumerate(salary_data['transactions']):
        write_transaction_column(work_sheet, row, transaction_data, transaction_formats, ro+1)
        values = transaction_data['salary_month']
        if ro % 2 == 0:
            cell_formats = transaction_formats['right_align_generic_cell']
        else:
            cell_formats = transaction_formats['right_align_cyan_cell']
        work_sheet.write(row, col_num, values, cell_formats)
        row += 1

    size = [['B', 38]]

    for i in range(len(size)):
        s = '{}:{}'.format(size[i][0], size[i][0])
        work_sheet.set_column(s, size[i][1])
