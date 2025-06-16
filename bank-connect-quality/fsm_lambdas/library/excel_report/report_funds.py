import math
from library.excel_report.excel_util import *

def funds_func(workbook, Monthly_bal_tran, months_order, str1, str2, workbook_num=''):
    primary_heading = workbook.add_format(
        {'font_color': '#000000', 'bg_color': '#FFFFFF', 'valign': 'vcenter', 'border': 1, 'font_size': 14})
    vertical_heading_cell = workbook.add_format(
        {'font_color': '#000000', 'bg_color': '#D8E4BC', 'valign': 'vcenter', 'border': 1, 'font_size': 9, 'text_wrap': True})
    horizontal_heading_cell = workbook.add_format(
        {'font_color': '#ffffff', 'bg_color': '#002060', 'valign': 'vcenter', 'border': 1, 'font_size': 9, "bold": True, 'align': 'center'})
    blue_cell = workbook.add_format(
        {'font_color': '#000000', 'bg_color': '#F1f1f1', 'valign': 'vcenter', 'border': 1, 'font_size': 9, 'num_format': '#,##0.00'})
    pivot_cell = workbook.add_format(
        {'font_color': '#ffffff', 'bg_color': '#808080', 'valign': 'vcenter', 'border': 1, 'font_size': 10, 'bold': True, 'align': 'center'})

    funds_worksheet_str1 = workbook.add_worksheet('Top 5 {}'.format(str1)+workbook_num)
    write_to_sheet(funds_worksheet_str1, 0, 0, data='Top 5 {}'.format(str1), cell_format=primary_heading)

    funds_worksheet_str2 = workbook.add_worksheet('Top 5 {}'.format(str2)+workbook_num)
    write_to_sheet(funds_worksheet_str2, 0, 0, data='Top 5 {}'.format(str2), cell_format=primary_heading)

    row = 1
    for month in months_order:
        if len(Monthly_bal_tran[month]['debit']['amount']) > 0:
            Monthly_bal_tran[month]['debit']['amount'], Monthly_bal_tran[month]['debit']['All transaction notes'], Monthly_bal_tran[month]['debit']['unclean_merchant'] = zip(*sorted(zip(Monthly_bal_tran[month]['debit']['amount'], Monthly_bal_tran[month]['debit']['All transaction notes'], Monthly_bal_tran[month]['debit']['unclean_merchant']), reverse=True))
        if len(Monthly_bal_tran[month]['credit']['amount']) > 0:
            Monthly_bal_tran[month]['credit']['amount'], Monthly_bal_tran[month]['credit']['All transaction notes'], Monthly_bal_tran[month]['credit']['unclean_merchant'] = zip(*sorted(zip(Monthly_bal_tran[month]['credit']['amount'], Monthly_bal_tran[month]['credit']['All transaction notes'], Monthly_bal_tran[month]['credit']['unclean_merchant']), reverse=True))

        funds_worksheet_str1.merge_range(row, 1, row, 2, month, horizontal_heading_cell)
        funds_worksheet_str2.merge_range(row, 1, row, 2, month, horizontal_heading_cell)
        row += 1
        write_to_sheet(funds_worksheet_str1, row, 1, 'Description', pivot_cell)
        write_to_sheet(funds_worksheet_str2, row, 1, 'Description', pivot_cell)
        write_to_sheet(funds_worksheet_str1, row, 2, 'Amount', pivot_cell)
        write_to_sheet(funds_worksheet_str2, row, 2, 'Amount', pivot_cell)
        row += 1
        for i in range(5):
            r = row
            try:
                debit_amount = Monthly_bal_tran[month]['debit']['amount'][i]
                credit_amount = Monthly_bal_tran[month]['credit']['amount'][i]
            except Exception as e:
                '''
                This occured because these fields are not present in the Monthly_bal_tran. Continue.
                '''
                print("Exception at funds_func getting debit and credit amount: ",e)
                continue
            if debit_amount in ["inf","-inf",math.inf,-math.inf]:
                print("Got infinty in debit")
                temp = list(Monthly_bal_tran[month]['debit']['amount'])
                if i<len(temp):temp[i]="0"
                Monthly_bal_tran[month]['debit']['amount'] = temp

            if credit_amount in ["inf","-inf",math.inf,-math.inf]:
                print("Got infinty in credit")
                temp = list(Monthly_bal_tran[month]['credit']['amount'])
                if i<len(temp):temp[i]="0"
                Monthly_bal_tran[month]['credit']['amount'] = temp
            try:
                written_note = f"Transfer to {Monthly_bal_tran[month]['debit']['unclean_merchant'][i]}" if Monthly_bal_tran[month]['debit']['unclean_merchant'][i] else Monthly_bal_tran[month]['debit']['All transaction notes'][i]
                write_to_sheet(funds_worksheet_str1, r, 1, written_note, vertical_heading_cell)
                write_to_sheet(funds_worksheet_str1, r, 2, Monthly_bal_tran[month]['debit']['amount'][i], blue_cell)
            except IndexError:
                write_to_sheet(funds_worksheet_str1, r, 1, '', vertical_heading_cell)
                write_to_sheet(funds_worksheet_str1, r, 2, '', blue_cell)
            r = row
            try:
                written_note = f"Transfer from {Monthly_bal_tran[month]['credit']['unclean_merchant'][i]}" if Monthly_bal_tran[month]['credit']['unclean_merchant'][i] else Monthly_bal_tran[month]['credit']['All transaction notes'][i]
                write_to_sheet(funds_worksheet_str2, r, 1, written_note, vertical_heading_cell)
                write_to_sheet(funds_worksheet_str2, r, 2, Monthly_bal_tran[month]['credit']['amount'][i], blue_cell)
            except IndexError:
                write_to_sheet(funds_worksheet_str2, r, 1, '', vertical_heading_cell)
                write_to_sheet(funds_worksheet_str2, r, 2, '', blue_cell)
            row += 1


    funds_worksheet_str1.set_column('A:A', 1)
    funds_worksheet_str1.set_column('B:B', 37)
    funds_worksheet_str1.set_column('C:C', 16)

    funds_worksheet_str2.set_column('A:A', 1)
    funds_worksheet_str2.set_column('B:B', 37)
    funds_worksheet_str2.set_column('C:C', 16)
