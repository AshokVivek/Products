from library.excel_report.report_formats import transaction_format_func, months_formats_func

def statemenets_details(workbook, version, personal_data, account_statement_metadata, worksheet=None):
    print(account_statement_metadata)
    if not worksheet:
        worksheet = workbook.add_worksheet('Statement Details')
    txn_formats = transaction_format_func(workbook)
    months_formats = months_formats_func(workbook)

    row, col = 0, 0
    column_names = ['File Name', 'Institution', 'Account No', 'Transaction Start Date', 'Transaction End Date', 'Name as in Statement']
    if version=='v6':
        column_names = ['S.No'] + column_names + ['Status', 'Failure Reason']
    for column_name in column_names:
        worksheet.write(row, col, column_name, txn_formats.get('horizontal_heading_cell'))
        col+=1
    
    row, col = 1, 0
    worksheet.set_row(0, 30)

    for serial_no, (statement_id, statement_data) in enumerate(account_statement_metadata.items()):
        for column_name in column_names:
            values = ''
            current_format = months_formats.get('text_box_cell')

            if column_name=='S.No' and version=='v6':
                values = serial_no+1
                current_format = workbook.add_format({'font_color': '#000000', 'bg_color': '#F1f1f1', 'valign': 'vcenter', 'border': 1, 'font_size': 10, 'align': 'center', 'text_wrap': True, 'num_format': '0'})
            elif column_name == 'File Name':
                values = statement_data.get('file_name')
            elif column_name == 'Institution':
                values = personal_data.get('bank_name') if version!='v6' else statement_data.get('bank_name')
            elif column_name == 'Account No':
                values = personal_data.get('account_number') if version!='v6' else statement_data.get('account_number')
                current_format = months_formats.get('account_number_cell')
            elif column_name == 'Transaction Start Date':
                values = statement_data.get('txn_from_date')
                current_format = months_formats.get('date_text_box_cell')
            elif column_name == 'Transaction End Date':
                values = statement_data.get('txn_to_date')
                current_format = months_formats.get('date_text_box_cell')
            elif column_name == 'Name as in Statement':
                values = personal_data.get('name') if version!='v6' else statement_data.get('name')
            elif column_name == 'Status' and version=='v6':
                values = statement_data.get('status')
            elif column_name == 'Failure Reason' and version=='v6':
                values = statement_data.get('failure_reason')
            
            worksheet.write(row, col, values, current_format)
            col+=1
            worksheet.set_row(row, 30)
        col=0
        row+=1
            



        # file_name = ''
        # values = [statement_data.get('file_name') ,personal_data.get('bank_name'), str(personal_data.get('account_number')), statement_data.get('txn_from_date'), statement_data.get('txn_to_date'), personal_data.get('name')]
        # for i in range(len(values)):
        #     format = {}
        #     if i==2:
        #         format = months_formats.get('account_number_cell')
        #     if i in [3, 4]:
        #         format = months_formats.get('date_text_box_cell')
        #     else:
        #         format = months_formats.get('text_box_cell')
        #     worksheet.write(row, col, values[i], format)
        #     col+=1
        # row+=1

    if version=='v6':
        worksheet.set_column('A:A', 5)
        worksheet.set_column('B:B', 40)
        worksheet.set_column('C:I', 20)
    else:
        worksheet.set_column('A:A', 40)
        worksheet.set_column('B:F', 20)