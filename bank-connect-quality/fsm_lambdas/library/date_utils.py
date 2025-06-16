from datetime import datetime, timedelta

supported_formats = ["%Y-%m-%d", "%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%dT%H:%M:%S.%f%z", "%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%dT%H:%M:%S.%f", "%Y-%m-%dT%H:%M:%S"]

def monthlist_fast(dates):
    for format in supported_formats:
        try:
            start, end = [datetime.strptime(_, format) for _ in dates]
            break
        except ValueError:
            pass

    def total_months(dt): return dt.month + 12 * dt.year
    mlist = []
    for tot_m in range(total_months(start) - 1, total_months(end)):
        y, m = divmod(tot_m, 12)
        mlist.append(datetime(y, m + 1, 1).strftime("%Y-%m"))
    return mlist


def get_months_from_periods(periods):
    months = list()
    for item in periods:
        if item.get('from_date') is None:
            continue
        dates = list()
        dates.append(item.get('from_date'))
        dates.append(item.get('to_date'))
        months += monthlist_fast(dates)

    months_set = set(months)
    return sorted(list(months_set))

def daterange(start_date, end_date):
    for n in range(int ((end_date - start_date).days) + 1):
        yield start_date + timedelta(n)

def find_missing_intervals(session_date_range, date_ranges):
    start_from = session_date_range.get('from_date')
    start_to = session_date_range.get('to_date')
    overall_dates = list(daterange(start_from, start_to))
    for date_range in date_ranges:
        from_date = datetime.strptime(date_range.get('from_date'), '%Y-%m-%d')
        to_date = datetime.strptime(date_range.get('to_date'), '%Y-%m-%d')
        current_range = list(daterange(from_date, to_date))
        overall_dates = [date for date in overall_dates if date not in current_range]
    
    return overall_dates

def get_missing_date_range_on_extraction(extracted_date_range_list, session_date_range):
    month_dict = {}
    if not session_date_range:
        return month_dict
    missing_dates = find_missing_intervals(session_date_range, extracted_date_range_list)
    for date in missing_dates:
        month_key = date.strftime("%b %Y")
        if month_key not in month_dict:
            month_dict[month_key] = set()
        month_dict[month_key].add(date.day)
    for key, value in month_dict.items():
        month_dict[key] = list(value)
    return month_dict

def is_month_missing(months, session_date_range):
    if not session_date_range:
        return [], []
    session_date_range['from_date'] = session_date_range['from_date'].strftime("%Y-%m-%d")
    session_date_range['to_date'] = session_date_range['to_date'] - timedelta(days=7)
    session_date_range['to_date'] = session_date_range['to_date'].strftime("%Y-%m-%d")
    requested_months = get_months_from_periods([session_date_range])

    missing_months = []
    for month in requested_months:
        if month not in months:
            missing_months.append(month)       
    return missing_months, requested_months


def is_missing_dates(missing_date_range, date_range_approval_criteria):
    if date_range_approval_criteria == 0:
        return False
    num_missing_dates = 0
    for month in missing_date_range.keys():
        if len(missing_date_range[month]) > 0:
            num_missing_dates += len(missing_date_range[month])
    return date_range_approval_criteria <= num_missing_dates


def convert_date_range_to_datetime(date_range, format):
    response = {}
    if isinstance(date_range, dict) and date_range.get('from_date') != None and date_range.get('from_date') != None:
        response['from_date'] = datetime.strptime(date_range['from_date'], format)
        response['to_date'] = datetime.strptime(date_range['to_date'], format)
    else:
        response = None
    return response

def change_date_format(cur_dates, cur_format, to_format):
    formatted_dates = []
    for date in cur_dates:
        parsed_date = datetime.strptime(date, cur_format)
        formatted_date = parsed_date.strftime(to_format)
        formatted_dates.append(formatted_date)
    return formatted_dates