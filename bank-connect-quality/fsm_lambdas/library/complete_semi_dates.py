import os
import json
from datetime import datetime
import warnings
import pandas as pd
from library.utils import  check_date, validate_amount, check_semi_date, convert_date_indo_to_eng, check_29th_feb
from library.helpers.constants import DEFAULT_LEAP_YEAR


warnings.simplefilter(action = "ignore", category = FutureWarning)
pd.options.mode.chained_assignment = None


END_OF_YEAR = datetime.strptime("Dec 31", '%b %d')
START_OF_YEAR = datetime.strptime("Jan 01", '%b %d')


def complete_semi_dates(row, opening_date, country="IN"):
    if opening_date is None:
        return row
    
    if isinstance(row['date'], datetime):
        return row
    
    if country == "ID":
        row['date'] = convert_date_indo_to_eng(row['date'])
    
    if isinstance(opening_date, str):
        opening_date = datetime.strptime(opening_date, "%Y-%m-%d")
    
    if validate_amount(row['balance']) or validate_amount(row.get('amount', '')):
        is_semi_date = check_semi_date(row['date'])
        if is_semi_date:
            year_to_add = ''
            semi_formats = ['%b %d', '%d-%m-', '%d/%m', '%d %b', '%d-%m', '%d-%b-', '%d- %b-', '%d-%b', '%d/ %m/']
            for format in semi_formats:
                try:
                    temp_from_date = datetime.strptime(opening_date.strftime('%b %d'), '%b %d')
                    if check_29th_feb(row['date']):
                        temp_format = format + '-%Y'
                        temp_date = row['date'] + f'-{DEFAULT_LEAP_YEAR}'
                        our_date = datetime.strptime(temp_date, temp_format)
                        if temp_from_date.month <= our_date.month:
                            year_to_add = str(opening_date.year)
                        else:
                            year_to_add = str(opening_date.year+1)
                    else:
                        our_date = datetime.strptime(row['date'], format)
                        
                        if temp_from_date <= our_date <= END_OF_YEAR:
                            year_to_add =  str(opening_date.year)
                        elif START_OF_YEAR <= our_date < temp_from_date:
                            year_to_add = str(opening_date.year + 1)
                    
                    if format == '%b %d' or format == '%d %b' or format == '%d-%b-' or format == '%d- %b-':
                        row['date'] += ' ' + year_to_add
                    elif format == '%d-%m-' or format == '%d/ %m/':
                        row['date'] += year_to_add
                    elif format == '%d/%m':
                        row['date'] += '/'+ year_to_add
                    elif format == '%d-%m':
                        row['date'] += '-'+ year_to_add
                    elif format == '%d-%b':
                        row['date'] += '-'+ year_to_add
                except (ValueError, TypeError):
                    continue
    return row
    
def complete_semi_dates_from_txn(df, country="IN", key=None):
    row_dicts = df.to_dict('records')
    prev_date = None
    for row in reversed(row_dicts):
        date, _ = check_date(row.get('date'), key=key)
        if date:
            prev_date = date
            break
    if prev_date is not None:
        for index, row in reversed(list(enumerate(row_dicts))):
            date = check_semi_date(row.get('date'))
            if date:
                row_dicts[index] = complete_semi_dates(row, prev_date, country)
                break
    df = pd.DataFrame(row_dicts)
    return df

# this fn adds a semi date in transactions where there is no date for federal fi case
def populate_semi_date(df):
    row_dicts = df.to_dict('records')
    prev_date = None
    for index,row in list(enumerate(row_dicts)):
        date = check_semi_date(row.get('date'))
        if date:
            prev_date = row.get('date')
        elif prev_date and row_dicts[index]['date'] in [None, ''] :
            row_dicts[index]['date'] = prev_date
    df = pd.DataFrame(row_dicts)
    return df