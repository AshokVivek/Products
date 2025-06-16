
import math
import traceback
from datetime import datetime

def write_to_sheet(worksheet, row, col, data='', cell_format=None):
    if data in ["inf", "-inf", math.inf, -math.inf]:
        data = ''

    try:
        worksheet.write(row, col, data, cell_format)
    except Exception as e:
        print(str(e))
        traceback.print_exc()
        worksheet.write(row, col, data)

def add_analysis_summary_info_to_report(analysis_summary_info, personal_data, worksheet, row, col, green_heading, account_blue_cell, light_blue_cell, date_cell, text_body_cell, overview_dict, monthly_analysis, version, predictors = {}):

    for heading, value in analysis_summary_info:
        worksheet.write(row, col, heading, green_heading)
        try:
            val = personal_data[value]
            if value in ['from_date', 'to_date']:
                val = datetime.strptime(val, '%Y-%m-%d %H:%M:%S')
            elif value == 'missing_data': 
                val = ', '.join([f"{datetime.strptime(date_range['from_date'], '%Y-%m-%d').strftime('%d/%m/%y')} - {datetime.strptime(date_range['to_date'], '%Y-%m-%d').strftime('%d/%m/%y')}" for date_range in val])
        except KeyError:
            val = ""
        if value == 'credit_limit' and (val == "" or val is None):
            val = personal_data.get('od_limit',None)
        if value == 'source':
            val = personal_data.get('attempt_type', '')

        if value == 'avg_of_avg_monthly_bal':
            worksheet.merge_range(row, col+1, row, col+3, val, text_body_cell)
            val = '0.0'
            if predictors.get('avg_of_avg_monthly_bal', None) is not None:
                val = str(predictors.get('avg_of_avg_monthly_bal', None))
            worksheet.write_string(row, col+1, val)
        elif value == 'account_number':
            worksheet.merge_range(row, col+1, row, col+3, val, account_blue_cell)
            if len(str(val))>5:
                worksheet.write_string(row, col+1, val)
        elif value == 'account_category':
            if personal_data.get('is_od_account',None):
                val = 'Overdraft'
            if isinstance(val, str):
                val = val.upper()
            worksheet.merge_range(row, col+1, row, col+3, val, light_blue_cell)
        elif value in ['from_date', 'to_date']:
            worksheet.merge_range(row, col+1, row, col+3, val, date_cell)
        elif value in ['txn_from_date', 'txn_to_date']:
            worksheet.merge_range(row, col+1, row, col+3, personal_data.get(value), date_cell)
        elif value in ['od_limit', 'credit_limit', 'source', 'pan', 'applicant_id', 'transaction_id', 'fraud_status', 'email']:
            worksheet.merge_range(row, col+1, row, col+3, val, text_body_cell)
        else:
            if value not in ['account_id', 'missing_data', 'applicant_id' ,'transaction_id'] and isinstance(val, str):
                val = val.upper()
            worksheet.merge_range(row, col+1, row, col+3, val, light_blue_cell)
        
        overview_dict['Summary'][heading] = val
        row += 1
    
    return row, col