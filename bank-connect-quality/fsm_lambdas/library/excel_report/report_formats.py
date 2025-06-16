def transaction_format_func(workbook):

    transaction_formats = {
        'horizontal_heading_cell': workbook.add_format(
            {'font_color': '#FFFFFF', 'bg_color': '#002060', 'valign': 'vcenter', 'border': 1, 'font_size': 10, 'bold': True, 'align': 'center', 'text_wrap': True}),
        'date_left_align_cyan_cell': workbook.add_format(
            {'font_color': '#000000', 'bg_color': '#CCFFFF', 'valign': 'vcenter', 'border': 1, 'font_size': 10, 'bold': True, 'align': 'center', 'text_wrap': True, 'num_format': 'dd-mmm-yy'}),
        'date_left_align_generic_cell': workbook.add_format(
            {'font_color': '#000000',  'valign': 'vcenter', 'border': 1, 'font_size': 10, 'bold': True, 'align': 'center', 'text_wrap': True, 'num_format': 'dd-mmm-yy'}),
        'left_align_cyan_cell': workbook.add_format(
            {'font_color': '#000000', 'bg_color': '#CCFFFF', 'valign': 'vcenter', 'border': 1, 'font_size': 10, 'bold': True, 'align': 'center', 'text_wrap': True, 'num_format': '#,##0.00'}),
        'left_align_generic_cell': workbook.add_format(
            {'font_color': '#000000',  'valign': 'vcenter', 'border': 1, 'font_size': 10, 'bold': True, 'align': 'center', 'text_wrap': True, 'num_format': '#,##0.00'}),
        'right_align_cyan_cell': workbook.add_format(
            {'font_color': '#000000', 'bg_color': '#CCFFFF', 'valign': 'vcenter', 'border': 1, 'font_size': 10, 'bold': True, 'align': 'center', 'text_wrap': True}),
        'right_align_generic_cell': workbook.add_format(
            {'font_color': '#000000',  'valign': 'vcenter', 'border': 1, 'font_size': 10, 'bold': True, 'align': 'center', 'text_wrap': True, }),
        'debit_generic_cell': workbook.add_format(
            {'font_color': '#fc3605',  'valign': 'vcenter', 'border': 1, 'font_size': 10,  'align': 'center', 'text_wrap': True, 'num_format': '#,##0.00'}),
        'debit_cyan_cell': workbook.add_format(
            {'font_color': '#fc3605', 'bg_color': '#CCFFFF', 'valign': 'vcenter', 'border': 1, 'font_size': 10, 'align': 'center', 'text_wrap': True, 'num_format': '#,##0.00'}),
    }
    return transaction_formats


def months_formats_func(workbook):
    months_formats = {
        'primary_heading': workbook.add_format(
            {'font_color': '#000000', 'bg_color': '#FFFFFF', 'valign': 'vcenter', 'border': 1, 'font_size': 14}),
        'pivot_cell': workbook.add_format(
            {'font_color': '#FFFFFF', 'bg_color': '#808080', 'valign': 'vcenter', 'border': 1, 'font_size': 10, 'bold': True, 'align': 'center', 'text_wrap': True}),
        'vertical_heading_cell': workbook.add_format(
            {'font_color': '#000000', 'bg_color': '#D8E4BC', 'valign': 'vcenter', 'border': 1, 'font_size': 10, 'bold': True, 'align': 'center', 'text_wrap': True}),
        'left_vertical_heading_cell': workbook.add_format(
            {'font_color': '#000000', 'bg_color': '#D8E4BC', 'valign': 'vcenter', 'border': 1, 'font_size': 10, 'bold': True, 'align': 'left', 'text_wrap': True}),
        'text_box_cell': workbook.add_format(
            {'font_color': '#000000', 'bg_color': '#F1f1f1', 'valign': 'vcenter', 'border': 1, 'font_size': 10, 'align': 'center', 'text_wrap': True, 'num_format': '#,##0.00'}),
        'right_text_box_cell': workbook.add_format(
            {'font_color': '#000000', 'bg_color': '#F1f1f1', 'valign': 'vcenter', 'border': 1, 'font_size': 10, 'align': 'right', 'text_wrap': True, 'num_format': '#,##0.00'}),
        'left_text_box_cell': workbook.add_format(
            {'font_color': '#000000', 'bg_color': '#F1f1f1', 'valign': 'vcenter', 'border': 1, 'font_size': 10, 'align': 'left', 'text_wrap': True, 'num_format': '#,##0.00'}),

        'debit_generic_cell': workbook.add_format(
            {'valign': 'vcenter', 'border': 1, 'font_size': 10,  'align': 'center', 'text_wrap': True}),
        'button_cell': workbook.add_format(
            {'font_color': '#0040FF','valign': 'vcenter', 'border': 1, 'font_size': 10,  'align': 'center', 'text_wrap': True}),
        'date_text_box_cell': workbook.add_format(
            {'font_color': '#000000', 'bg_color': '#F1f1f1', 'valign': 'vcenter', 'border': 1, 'font_size': 10, 'align': 'right', 'text_wrap': True, 'num_format': 'dd-mmm-yy'}),
        'month_date_text_box_cell': workbook.add_format(
            {'font_color': '#000000', 'bg_color': '#F1f1f1', 'valign': 'vcenter', 'border': 1, 'font_size': 10, 'align': 'right', 'text_wrap': True, 'num_format': 'mmm-yy'}),
        'date_horizontal_heading_cell': workbook.add_format(
            {'font_color': '#FFFFFF', 'bg_color': '#002060', 'valign': 'vcenter', 'border': 1, 'font_size': 10, 'bold': True, 'align': 'center', 'text_wrap': True, 'num_format': 'mmm-yy'}),
        'date_generic_cell': workbook.add_format(
            {'font_color': '#000000',  'valign': 'vcenter', 'border': 1, 'font_size': 10, 'bold': True, 'align': 'center', 'text_wrap': True, 'num_format': 'dd-mmm-yy'}),
        'account_number_cell': workbook.add_format(
            {'font_color': '#000000', 'bg_color': '#F1f1f1', 'valign': 'vcenter', 'border': 1, 'font_size': 9, 'num_format': '0'}),
        'number_cell': workbook.add_format(
            {'font_color': '#000000',  'valign': 'vcenter', 'border': 1, 'font_size': 10, 'align': 'center', 'text_wrap': True, 'num_format': '#,##0.00'}),
        'green_heading' :workbook.add_format(
            {'font_color': '#000000', 'bg_color': '#e2efd9', 'valign': 'vcenter', 'border': 1, 'font_size': 9, 'text_wrap': True})
        }
        
    return months_formats
