# txns -> list of txn objects
# txn objects is dict with following keys:
# merchant transaction_channel hash description unclean_merchant is_lender
# transaction_type amount date balance transaction_note

from datetime import datetime
from math import sqrt


def mean(numbers):
    if len(numbers) == 0:
        return 0.0
    return sum(numbers) / len(numbers)


def get_std_dev(array):
    if len(array) < 2:
        return 0.0
    return sqrt(sum([(number - mean(array)) ** 2 for number in array]) / (len(array) - 1))


def insert_month(txns):
    for item in txns:
        item['month'] = datetime.strptime(
            item['date'], "%Y-%m-%d %H:%M:%S").strftime("%Y-%m")
    return txns


def get_avg(txns):
    txns = [txn['amount'] for txn in txns]

    if len(txns) == 0:
        return 0.0
    else:
        return round(sum(txns) / len(txns), 2)


def get_avg_balance(txns):
    txns = [txn['balance'] for txn in txns]

    if len(txns) == 0:
        return 0.0
    else:
        return round(sum(txns) / len(txns), 2)


def get_max(txns):
    max_amount = 0
    max_txn = None

    for txn in txns:
        if txn['amount'] > max_amount:
            max_amount = txn['amount']
            max_txn = txn

    return max_txn


def filter_by_month(txns, month):
    txns = [txn for txn in txns if txn['month'] == month]
    return txns


def filter_by_txn_type(txns, txn_type):
    txns = [txn for txn in txns if txn['transaction_type'] == txn_type]
    return txns


def get_avg_credit_month_wise(txns):
    avg_credit_month_wise = dict()
    txns = filter_by_txn_type(txns, 'credit')

    for month in set([txn['month'] for txn in txns]):
        avg_credit_month_wise[month] = get_avg(filter_by_month(txns, month))

    return avg_credit_month_wise


def get_avg_debit_month_wise(txns):
    avg_credit_month_wise = dict()
    txns = filter_by_txn_type(txns, 'debit')

    for month in set([txn['month'] for txn in txns]):
        avg_credit_month_wise[month] = get_avg(filter_by_month(txns, month))

    return avg_credit_month_wise


def get_avg_credit(txns):
    txns = filter_by_txn_type(txns, 'credit')
    return get_avg(txns)


def get_avg_debit(txns):
    txns = filter_by_txn_type(txns, 'debit')
    return get_avg(txns)


def get_max_credit_txn(txns):
    txns = filter_by_txn_type(txns, 'credit')
    return get_max(txns)


def get_max_debit_txn(txns):
    txns = filter_by_txn_type(txns, 'debit')
    return get_max(txns)


def get_balance_ts(txns):
    return [{'date': str(txn['date']), 'balance': txn['balance']} for txn in txns]


def safe_max(items):
    if len(items) == 0:
        return None
    else:
        return max(items)


def get_max_balance(txns):
    return safe_max([txn['balance'] for txn in txns])


def safe_min(items):
    if len(items) == 0:
        return None
    else:
        return min(items)


def get_min_balance(txns):
    return safe_min([txn['balance'] for txn in txns])


def get_balance_volatility(txns):
    balances = [txn.get('balance') for txn in txns]
    return get_std_dev(balances)


def avg_monthly_credit(txns):
    txns = filter_by_txn_type(txns, 'credit')
    monthly_credit = list()

    for month in set([txn['month'] for txn in txns]):
        m_txns = filter_by_month(txns, month)
        monthly_credit.append(sum([txn['amount'] for txn in m_txns]))

    if len(monthly_credit) > 0:
        return round(sum(monthly_credit) / len(monthly_credit), 2)

    return 0.0


def avg_monthly_debit(txns):
    txns = filter_by_txn_type(txns, 'debit')
    monthly_credit = list()

    for month in set([txn['month'] for txn in txns]):
        m_txns = filter_by_month(txns, month)
        monthly_credit.append(sum([txn['amount'] for txn in m_txns]))

    if len(monthly_credit) > 0:
        return round(sum(monthly_credit) / len(monthly_credit), 2)

    return 0.0


def get_credit_debit_month_wise(txns):
    month_list = list()
    for month in set([txn['month'] for txn in txns]):
        credit = sum([item.get('amount') for item in filter_by_txn_type(
            filter_by_month(txns, month), 'credit')])
        debit = sum([item.get('amount') for item in filter_by_txn_type(
            filter_by_month(txns, month), 'debit')])
        month_list.append({'month': month, 'credit': credit, 'debit': debit})
    return month_list


def get_entity_metrics(txns):
    txns = insert_month(txns)
    return {
        'avg_credit': get_avg_credit(txns),
        'avg_debit': get_avg_debit(txns),
        'avg_credit_month_wise': get_avg_credit_month_wise(txns),
        'avg_debit_month_wise': get_avg_debit_month_wise(txns),
        'max_credit_txn': get_max_credit_txn(txns),
        'max_debit_txn': get_max_debit_txn(txns),
        'balance_ts': get_balance_ts(txns),
        'min_balance': get_min_balance(txns),
        'max_balance': get_max_balance(txns),
        'avg_balance': get_avg_balance(txns),
        'balance_volatility': get_balance_volatility(txns),
        'avg_monthly_credit': avg_monthly_credit(txns),
        'avg_monthly_debit': avg_monthly_debit(txns),
        'credit_debit_month_wise': get_credit_debit_month_wise(txns)
    }
