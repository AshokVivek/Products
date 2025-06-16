import hashlib
import re
from datetime import datetime
import warnings
import pandas as pd
from library.helpers.constants import DEFAULT_LEAP_YEAR, FEB_29TH_REGEXES, HEADERS_OPENING_BALANCE, HEADERS_CLOSING_BALANCE, BANKS_WITH_TRANSACTIONS_SPLIT_ENABLED, DEFAULT_TIMESTAMP_UTC, DEFAULT_BALANCE_FLOAT, SPLIT_TRANSACTION_NOTES_PATTERNS
from copy import deepcopy
from sentry_sdk import capture_exception, capture_message
from collections import defaultdict
import pdfplumber
import struct
from library.custom_exceptions import NonParsablePDF
from pdfminer.pdfdocument import PDFEncryptionError, PDFPasswordIncorrect
from pdfminer.pdfparser import PDFSyntaxError
from pdfminer.psparser import PSEOF


warnings.simplefilter(action = "ignore", category = FutureWarning)
pd.options.mode.chained_assignment = None


# TODO - Move the keywords to DB.
AMOUNT_WORDS = ['amount', 'credit', 'debit', 'deposit', 'withdrawl', ' dr ', ' cr ', "ﺰﻣﺭﻦﺋﺍﺩﻦﻳﺪﻣﺪﻴﺻﺮﻟﺍ ﺦﻳﺭﺎﺘﻟﺍﻞﻴﺻﺎﻔﺘﻟﺍﺕﺎﻈﺣﻼﻣﻊﺟﺮﻤﻟﺍﺔﻴﻠﻤﻌﻟﺍ",
                    "مدين", "دائن", "ﺪﻴﺻﺮﻟﺍﻦﻳﺪﻣﻦﺋﺍﺩﺔﻴﻠﻤﻌﻟﺍ ﻞﻴﺻﺎﻔﺗﻱﺩﻼﻴﻤﻟﺍ ﺦﻳﺭﺎﺘﻟﺍ", "ﺦﻳﺭﺎﺘﻟﺍﻦﺋﺍﺩﻦﻳﺪﻣﺔﻠﻤﻌﻟﺍﺔﻴﻠﻤﻌﻟﺍ", "dramt", "cramt", "recipt", "payment", "abu dhabi islamic bank", "mata uang", "debet", "jumlah", "mutasi", "გასული თანხა", "შემოსული თანხა", 'gel']
BALANCE_WORD = ['balance', 'bookbal', "ﺰﻣﺭﻦﺋﺍﺩﻦﻳﺪﻣﺪﻴﺻﺮﻟﺍ ﺦﻳﺭﺎﺘﻟﺍﻞﻴﺻﺎﻔﺘﻟﺍﺕﺎﻈﺣﻼﻣﻊﺟﺮﻤﻟﺍﺔﻴﻠﻤﻌﻟﺍ",
                    "ﺪﻴﺻﺮﻟﺍﻦﻳﺪﻣﻦﺋﺍﺩﺔﻴﻠﻤﻌﻟﺍ ﻞﻴﺻﺎﻔﺗﻱﺩﻼﻴﻤﻟﺍ ﺦﻳﺭﺎﺘﻟﺍ", "ﺦﻳﺭﺎﺘﻟﺍﻦﺋﺍﺩﻦﻳﺪﻣﺔﻠﻤﻌﻟﺍﺔﻴﻠﻤﻌﻟﺍ", 'bal', 'saldo', 'total', 'gel']
DATE_WORD = ['date', 'value dt', 'value. dt.', 'txn dt', 'post dt', 'dat_value', "ﺰﻣﺭﻦﺋﺍﺩﻦﻳﺪﻣﺪﻴﺻﺮﻟﺍ ﺦﻳﺭﺎﺘﻟﺍﻞﻴﺻﺎﻔﺘﻟﺍﺕﺎﻈﺣﻼﻣﻊﺟﺮﻤﻟﺍﺔﻴﻠﻤﻌﻟﺍ",
                 "ﺪﻴﺻﺮﻟﺍﻦﻳﺪﻣﻦﺋﺍﺩﺔﻴﻠﻤﻌﻟﺍ ﻞﻴﺻﺎﻔﺗﻱﺩﻼﻴﻤﻟﺍ ﺦﻳﺭﺎﺘﻟﺍ", "ﻒﺻﻮﻟﺍﻱﺩﻼﻴﻤﻟﺍ", "dat_txn", "transaction", "postdt","value date", "tgl", "tanggal", "saldo awal", "dt dt", "waktu"]
INDONESIAN_BANKS = ['danamon','rakayat','masind','bjb','mnc','bjbs','cimb','nobu','ocbc','bsi','panin','permata','bcabnk','bnibnk','mandiribnk','uob','megabnk','btpn','dbsbnk']

DEBIT_ONLY_MERCHANT_CATEGORY = ['fuel', 'shopping', 'food', 'travel', 'entertainment', 'alchohol', 'bills', 'groceries', 'medical', 'utilities', 'online_shopping']
DEBIT_ONLY_MERCHANT_CATEGORY_REGEX = ['alchohol_regex', 'entertainment_regex', 'food_regex', 'fuel_regex', 'utilities_regex', 'travel_regex', 'shopping_regex', 'online_shopping_regex', 'medical_regex']

CREDIT_ONLY_MERCHANT_CATEGORY = ['dividend']

NON_LENDER_MERCHANT_KEYWORDS = ['simple', 'pdfsimpli', 'simplifi', 'assampower', 'dividend', 'indiainfra', 'indiainflatable', 'indiainfotech', 'healthinsltd', 'hrmdisb', 'disbursingsalary', 'treasurydisb', 'ringdisbur', 'maturitydateamountdisb', 'simply', 'simplify', 'unicef']

def remove_unicode(text):
    if isinstance(text, str):
        if type(text) == int:
            return str(text)
        if type(text) == float:
            return str(text)
        if text is None:
            return ''

        return ''.join([i if ord(i) < 128 else ' ' for i in text])
    return text


validate_amount_regex_1 = re.compile(
    '(?i)([-+\s]*?[0-9,]*\.+[0-9]{1,3})\s*\(?(?:cr|dr)?\.?\)?\-?$')
validate_amount_regex_2 = re.compile('(?i)(?:cr|dr)?([-+]?[0-9,]+)\s*\(?(?:cr|dr)?\.?\)?\-?$')
validate_amount_regex_3 = re.compile(
    '(?i)([-+]?[0-9,]*\.+[0-9]{1,2})\s*\(?(?:cr|dr)?\.?\)?\-?(?:[A-Z])?$')
validate_amount_regex_4 = re.compile(
    '(?i)((?:cr|dr|d)?\s*[-+]?[0-9,]*\.+[0-9]{1,2})\s*\(?(?:cr|dr)?\.?\)?\-?$')
validate_amount_regex_5 = re.compile(
    '(?i)((\.)+\s*[-+]?[0-9,]*\.+[0-9]{1,2})\s*\(?(?:cr|dr)?\.?\)?\-?$')
EPOCH_DATE = datetime(1970, 1, 1)
EPOCH_DATE_STR = datetime.strftime(datetime(1970, 1, 1), '%Y-%m-%d %H:%M:%S')

#regexes for date
date_regexes = [re.compile('(?i)([0-9]{2}[\/\-][a-zA-Z]{3,4}[\/\-][0-9]{2,4}).*'), 
                re.compile('(?i)([0-9]{2}[\/\-][0-9]{2}[\/\-][0-9]{2,4}).*'),
                re.compile('(?i)([0-9]{2}[\/\-][0-9]{2}[\/\-][0-9]{2}).*')]


def validate_amount(string):
    if isinstance(string, float):
        return True

    string = string.encode('unicode-escape').decode('ASCII')
    try:
        _ = float(string)
        return True
    except Exception:
        pass
    try:
        if isinstance(string, str):
            string = string.replace(",", "") \
                            .replace('\n', '') \
                            .replace('\s+', '') \
                            .replace('\\uf156','') \
                            .replace(' ', '') \
                            .replace('..', '.') \
                            .replace('(', '') \
                            .replace(')', '') \
                            .replace('\\u20b9', '') \
                            .replace('\\xa0', '') \
                            .replace('INR', '') \
                            .replace('IDR', '') \
                            .replace('Rs.', '') \
                            .replace('.CR', 'CR') \
                            .replace('.DR', 'DR') \
                            .replace('|','') \
                            .replace('DB', '') \
                            .replace('PHP','') \
                            .replace('+','') \
                            .replace('\\ufffd', '') \
                            .replace('\\t', '') \
                            .replace('  ', ' ') \
                            .replace('    ', ' ') \
                            .replace('Re.', '') \
                            .replace('\\xde', '') \
                            .replace('/', '')
            if 'DR' not in string.upper():
                string = string.replace('D', '').replace('K', '')
        if re.match(validate_amount_regex_1, string) or (re.match(validate_amount_regex_2, string)) or (re.match(validate_amount_regex_3, string)) or re.match(validate_amount_regex_4, string) or re.match(validate_amount_regex_5, string):
            return True
    except Exception as e:
        print(e)
    return False

def get_opening_balances(transaction):
    transaction_dict = transaction.to_dict()
    transaction_note = transaction_dict.get('transaction_note', None)

    for element in HEADERS_OPENING_BALANCE:
        if re.match(element, transaction_note.strip(), re.IGNORECASE):
            return True
        
    return False

def get_closing_balances(transaction):
    transaction_dict = transaction.to_dict()
    transaction_note = transaction_dict.get('transaction_note', None)

    for element in HEADERS_CLOSING_BALANCE:
        if re.match(element, transaction_note.strip(), re.IGNORECASE):
            return True
        
    return False

def memoize_check_date(f):
    memory = {}
    memory2 = {}
    
    def inner(input_date, is_credit_card=False, format=None, key=None):
        if input_date in memory2:
            return memory2[input_date], None
        
        if key:
            a, b= f(input_date, is_credit_card, memory.get(key), key)
            if a and b[0] and b[1]:
                memory[key] = b[0]
                memory2[input_date] = a
                return a, b
            if a:
                memory2[input_date] = a
                return a, b
        
        return f(input_date, is_credit_card=is_credit_card, format=format, key=key)

    return inner

@memoize_check_date
def check_date(input_date, is_credit_card = False, format=None, key=None):

    if not input_date:
        return False, None
    
    if isinstance(input_date, datetime):
        return input_date, None
    
    if input_date.strip() in ['', None]:
        return False, None
    
    current_year = datetime.today().year
    epoch_year = EPOCH_DATE.year
    try:
        if format:
            date = datetime.strptime(input_date, format)
            if ((date.year > current_year - 15) and (date.year < current_year+1)) or (date.year == epoch_year) or (is_credit_card and (date.year == current_year+1)):
                return date, (format, False)
    except Exception as _:
        pass
    
    # TODO make sure wrong format does not get captured
    date_formats = ['%B%Y', '%d %m %Y', '%m %d %Y', '%d %m %y', '%m %d %y', '%d %b %Y', '%b %d %Y', '%d %b %y', '%b %d %y',
                    '%Y %m %d', '%B %d %y', '%B %d ,%Y', '%b %d ,%Y', '%d %B %Y', '%B %d %Y', '%Y %m', '%m,%d,%Y']
    separators = ['-', '.', '/', ',', '']

    random_formats = ['%d/%m/%Y%H:%M:%S', '%d/%m/%y(%S/%H/%M)', '%d/%m%Y', '%d%m/%Y', '(%S/%H/%M)%d/%m/%y',
                      '%UI(%S/%H/%M)%d/%m/%y', '0I(%S/%H/%M)%d/%m/%y', '2I(%S/%H/%M)%d/%m/%y', '3I(%S/%H/%M)%d/%m/%y',
                      '4I(%S/%H/%M)%d/%m/%y', '5I(%S/%H/%M)%d/%m/%y', '6I(%S/%H/%M)%d/%m/%y', '7I(%S/%H/%M)%d/%m/%y',
                      '8I(%S/%H/%M)%d/%m/%y', '9I(%S/%H/%M)%d/%m/%y', '%d%b%Y%I:%M%p', '%d/%m/%Y%I:%M:%S%p',
                      '%Y-%m-%d%H:%M:%S', '%d-%m-%Y%H:%M:%S', '%d-%m-%Y%H:%M:%S%p', '%m-%d-%Y%H:%M:%S%p', 
                      '%d%b%y%H:%M', '%d%b,%Y', '%d-%b-%y%H:%M:%S', '%m/%d/%Y%H:%M:%S%p', '%d/%m/%y%H:%M:%S', 
                      '%m/%d/%y%I:%M%p', '%d/%m/%Y%H.%M.%S', '%Y-%m-%dT%H:%M:%S', '%Y-%m-%dT', '%m/%d/%y%H:%M', '%d%B%Y%H:%M:%S', '%d%b%Y%H:%M']
    all_formats = []

    # weird issue for new india bank where it calls September as Sept
    sept_abb_pattern = r'\b(sept)(?=[-/., ]?\d|\b)'
    input_date = re.sub(sept_abb_pattern, lambda m: m.group(0)[0] + "ep", input_date, flags=re.IGNORECASE)
    
    # Cleaning Ordinal Dates (e.g. 21stJanuary2024 -> 21January2024)
    input_date = re.sub(r'(\s*\d{1,2})[\s\/\.\,\-]?(?:st|nd|rd|th)', r'\1', input_date, count=1)
    
    for date_format in date_formats:
        for separator in separators:
            new_date_format = separator.join(date_format.split(' '))
            all_formats.append(new_date_format)

    for random_format in random_formats:
        all_formats.append(random_format)
    if input_date:
        input_date = input_date.replace(' ', '').replace('\n', '').replace("'", '')
        if len(input_date) > 5 and len(input_date) < 21:
            for formats in all_formats:
                try:
                    date = datetime.strptime(input_date, formats)
                    if ((date.year > current_year - 15) and (date.year < current_year+1)) or (date.year == epoch_year) or (is_credit_card and (date.year == current_year+1)):
                        return date, (formats, True)
                except (ValueError, TypeError):
                    continue
        # this is to handle when sbi gives dates 2 dates with one in bracket
        if len(input_date) >= 19:
            input_date1 = input_date.lower()
            if "(" in input_date:
                input_date = input_date.split("(")[0]
                # specific for iob multi date issue
                # try:
                #     if int(input_date[-4:]) < 2000:
                #         input_date = input_date[:-4] + input_date[-3:]
                # except (ValueError, TypeError):
                #     return False
            elif input_date1.islower():
                input_date = input_date[:11]
            else:
                input_date = input_date[:10]
            for formats in all_formats:
                try:
                    date = datetime.strptime(input_date, formats)
                    if ((date.year > current_year - 15) and (date.year < current_year+1)) or (date.year == epoch_year) or (is_credit_card and (date.year == current_year+1)):
                        return date, (formats, True)
                except (ValueError, TypeError):
                    continue
    return False, None


def match_regex(text, regex, group_to_return):
    """
    Matches a regex in the text and returns the specified group to return
    :param: text (to search in), regex, group_to_return (integer > 0)
    :return: text found in the group or None (if not found)
    """
    data_to_return = None
    if isinstance(regex, str) and isinstance(text, str):
        regex_match = re.match(regex, text)
        if regex_match:
            data_to_return = regex_match.group(group_to_return)
    return data_to_return


def match_compiled_regex(text, compiled_regex, group_to_return):
    """
    Matches a regex in the text and returns the specified group to return
    :param: text (to search in), compiled_regex (compiled using re.compile(regex)), group_to_return (integer > 0)
    :return: text found in the group or None (if not found)
    """
    data_to_return = None
    if isinstance(text, str):
        regex_match = compiled_regex.match(text)
        if regex_match:
            data_to_return = regex_match.group(group_to_return)
    return data_to_return


def add_hash_to_transactions_df(df):
    row_1 = dict()
    row_2 = dict()

    for index, row in df.iterrows():
        df.loc[index, 'hash'] = create_transaction_hash(row, row_1, row_2)

        row_2 = row_1
        row_1 = row

    return df

# hack for arabic
cid_regex = re.compile(r"(\(cid\:[0-9]+\))")
only_char_digit_regex = re.compile(r"[^\w]+", re.UNICODE)

def remove_special_chars(input_text):
    input_text = input_text.replace(" ", "").replace("_", "")
    input_text = cid_regex.sub("", input_text)
    input_text = only_char_digit_regex.sub("", input_text)
    return input_text

def create_transaction_hash(row, row_1, row_2):
    amount = row.get('amount')
    date = row.get('date')
    balance = row.get('balance')
    transaction_note = row.get('transaction_note')
    transaction_type = row.get('transaction_type')

    amount_1 = row_1.get('amount')
    date_1 = row_1.get('date')
    balance_1 = row_1.get('balance')
    transaction_note_1 = row_1.get('transaction_note')
    transaction_type_1 = row_1.get('transaction_type')

    amount_2 = row_2.get('amount')
    date_2 = row_2.get('date')
    balance_2 = row_2.get('balance')
    transaction_note_2 = row_2.get('transaction_note')
    transaction_type_2 = row_2.get('transaction_type')

    to_be_hashed_list = [
        str(amount),
        str(date),
        str(balance),
        str(transaction_note),
        str(transaction_type),
        str(amount_1),
        str(date_1),
        str(balance_1),
        str(transaction_note_1),
        str(transaction_type_1),
        str(amount_2),
        str(date_2),
        str(balance_2),
        str(transaction_note_2),
        str(transaction_type_2)]
    to_be_hashed = "".join(to_be_hashed_list)

    to_be_hashed = remove_special_chars(to_be_hashed)

    to_be_hashed = to_be_hashed.encode('utf-8')
    result = hashlib.md5(to_be_hashed)
    return result.hexdigest()

def single_transaction_hash(transaction):
    amount = transaction.get('amount')
    date = transaction.get('date')
    balance = transaction.get('balance')
    transaction_note = transaction.get('transaction_note')
    transaction_type = transaction.get('transaction_type')
    chq_num = transaction.get('chq_num', '')

    to_be_hashed_list = [str(amount), str(date), str(balance), str(transaction_note), str(transaction_type), str(chq_num)]
    to_be_hashed = remove_special_chars("".join(to_be_hashed_list)).encode("utf-8")

    return hashlib.md5(to_be_hashed).hexdigest()

def get_date_format(input_date, key=None):
    # print("get_date format key", key)
    if input_date and isinstance(input_date, datetime):
        return input_date
    
    if input_date and isinstance(input_date, str):
        input_date = input_date.replace(' ', '').replace('\n', '')
        if len(input_date) > 5 and len(input_date) < 21:
            date_formatted, _ = check_date(input_date, key=key)
            return date_formatted
        # this is to handle when sbi gives dates 2 dates with one in bracket
        if len(input_date) >= 19:
            input_date = input_date[0:10]
            date_formatted, _ = check_date(input_date, key=key)
            return date_formatted
    return False


def check_semi_date(input_date):

    all_formats = []

    # all_formats have all the different type of partial date format
    all_formats = ['%d %m', '%d-%m', '%d-%m-', '%d.%m', '%d/%m', '%d%m', '%m %d', '%m-%d', '%m.%d', '%m/%d', '%m%d', '%d %b', '%d-%b', '%d.%b', '%d/%b', '%d%b', '%b %d', '%b-%d', '%b.%d',
                   '%b/%d', '%b%d', '%Y %m', '%Y-%m', '%Y.%m', '%Y/%m', '%Y%m', '%B %d', '%B-%d', '%B.%d', '%B/%d', '%B%d', '%d %B', '%d-%B', '%d.%B', '%d/%B', '%d%B', '%Y', '%d-%b-', '%d- %b-', '%d/%m/']

    # for random_format in random_formats:
    #     all_formats.append(random_format)
    
    if isinstance(input_date, datetime):
        return False
    
    if input_date:
        input_date = input_date.replace(
            ' ', '').replace('\n', '').replace("'", '')
        for formats in all_formats:
            try:
                date = datetime.strptime(input_date, formats)
                if date.year > 1890:
                    return True
            except (ValueError, TypeError):
                if check_29th_feb(input_date) and 'Y' not in formats.upper():
                    temp_input_date = input_date
                    temp_input_date += f'-{DEFAULT_LEAP_YEAR}'
                    temp_format = formats
                    temp_format += '-%Y'

                    try:
                        date = datetime.strptime(temp_input_date, temp_format)
                        if str(date.year)==DEFAULT_LEAP_YEAR:
                            return True
                    except (ValueError, TypeError):
                        continue
                else:
                    continue

    return False

def check_29th_feb(input_date):
    for regex in FEB_29TH_REGEXES:
        regex_match = re.match(regex, input_date)
        if regex_match is not None:
            return True
    
    return False

def get_date_from_datetime(datetime):
    datetime.strptime()

def get_compiled_regex_list(str_list):
    """
    Takes a list of strings and return a list of compiled regex
    :param: a list of strings
    :return: a list of compiled regexes
    """
    compiled_regex_list = []
    for single_str in str_list:
        compiled_regex_list.append(re.compile(single_str))
    return compiled_regex_list

def check_word_presence(all_text, list_words):
    for each_word in list_words:
        if each_word in all_text:
            return True
    return False

def keyword_helper(all_text):
    # print(all_text)
    all_present = False
    amount_present_ever = False
    balance_present_ever = False
    date_present_ever = False
    if all_text is not None:
            all_text = all_text.replace('\n', ' ').lower()
            amount_present = check_word_presence(all_text, AMOUNT_WORDS)
            balance_present = check_word_presence(all_text, BALANCE_WORD)
            date_present = check_word_presence(all_text, DATE_WORD)
            if amount_present:
                amount_present_ever = True
            if balance_present:
                balance_present_ever = True
            if date_present:
                date_present_ever = True
            if (amount_present_ever and balance_present_ever and date_present_ever):
                all_present = True
    return {
        "amount_present": amount_present_ever,
        "balance_present": balance_present_ever,
        "date_present": date_present_ever,
        "all_present": all_present
    }

def add_notes(dic, is_from_next_page = False, bank_name = None):
    # add notes in the last transaction until we find other non-null fields
    for k, v in dic.items():
        if k in ['transaction_note', 'account_number', 'account_category']:
            continue
        if k in ['date'] and bank_name in BANKS_WITH_TRANSACTIONS_SPLIT_ENABLED:
            continue
        if k in ['chq_num'] and is_from_next_page and bank_name in BANKS_WITH_TRANSACTIONS_SPLIT_ENABLED:
            continue
        else:
            if isinstance(v, str):
                if v.strip() == '':
                    continue
                else:
                    return False
    return True

def check_format_row(row, key=None):
    row['is_balance'] = validate_amount(row['balance'])
    is_valid_date = get_date_format(row['date'], key=key)
    if is_valid_date:
        row['is_date_used'] = True
    else:
        row["is_date_used"] = False
    return row

def check_transaction_beginning(note, start_flag=False):
    # TODO: Refine on bank wise and country wise.
    if start_flag:
        # check the beginning of a transaction
        words = ['broughtforward', 'broughtfoward', 'broughtfowad', 'broughtforwad', 'particulars', 'b/f', 'mutasi']
    else:
        words = ['carriedforward', 'carriedfoward', 'camiedfowad', 'closingbalance', 'pagetotal']

    for word in words:
        if word in "".join(str(note).split(" ")).lower():
            return True
    return False

def fix_decimals(value):
    default_balance = '-1.0'
    # if value is not None and value != default_balance and len(value) >=3:
    if value is not None and value != default_balance:
        # if value[-3] != '.' and '.' not in value:
        if '.' not in value:
            value = value[:-2] + '.' + value[-2:]
    return value

def convert_date_indo_to_eng(date):
    # Convert indonesian month to its corresponding english month
    indonesian_months = {
        "Januari": "January",
        "Februari": "February",
        "Maret": "March",
        "April": "April",
        "Mei": "May",
        "Juni": "June",
        "Juli": "July",
        "Agustus": "August",
        "September": "September",
        "Oktober": "October",
        "November": "November",
        "Desember": "December"
    }
    date = date.lower()
    for indonesian_month, english_month in indonesian_months.items():
        if indonesian_month.lower() in date:
            date = date.replace(indonesian_month.lower(), english_month)
            break
    return date


def update_transaction_channel_for_cheque_bounce(transactions):
    """
    Update transaction_channel for debit entries to match the corresponding credit entries in inward cheque bounce cases
    and vice-versa for outward_cheque_bounce.

    :param transactions: List of transactions (each transaction is a dictionary)
    :return: Updated Transaction list
    """
    # In inward cheque bounce, transaction happens in sender's bank, 
    # while in outward cheque bounce, transaction happens in reciever's bank.
    if not transactions or not isinstance(transactions[0]['date'], pd.Timestamp):
        return transactions
    increasing_trxn_date_order = transactions[0]['date'] >= transactions[-1]['date']

    cheque_bounce_type_list = ['inward_cheque_bounce', 'outward_cheque_bounce']
    for cheque_bounce_type in cheque_bounce_type_list:
        # In inward_cheque_bounce, amount is first debited and then credited after bounce.
        first_trxn_type = 'credit'
        second_trxn_type = 'debit'
        if cheque_bounce_type == 'inward_cheque_bounce':
            first_trxn_type = 'debit'
            second_trxn_type = 'credit'
        rows_with_inward_chq_bounce = [row for row in transactions if row['transaction_channel'] == cheque_bounce_type and row['transaction_type'] == second_trxn_type]
        rows_to_update_transaction_channel = []
        for inward_chq_credit_row in rows_with_inward_chq_bounce:
            matching_debit_rows_with_amount = [
                row for row in transactions if
                row['transaction_type'] == first_trxn_type and          # Rows with 'debit' as transaction_type in inward_cheque_bounce and vice-versa
                row['amount'] == inward_chq_credit_row['amount'] and    # Matching this amount to corresponding transaction_type
                row['date'] <= inward_chq_credit_row['date'] and        # We want to update first transaction entry basec on second one
                row['transaction_channel'] in ['Other', 'outward_cheque_bounce', 'inward_cheque_bounce'] # Previously transaction_channel is marked as 'Other', 'outward_cheque_bounce', 'inward_cheque_bounce'
            ]
            if len(matching_debit_rows_with_amount) > 0:
                if increasing_trxn_date_order:
                    latest_matching_row = matching_debit_rows_with_amount[0]
                else:
                    latest_matching_row = matching_debit_rows_with_amount[-1]
                rows_to_update_transaction_channel.append(latest_matching_row)
        
        for row_to_update in rows_to_update_transaction_channel:
            row_to_update['transaction_channel'] = 'chq'
    return transactions

def amount_to_float(amount_string):
    if amount_string is not None:
        amount_string = str(amount_string).encode('unicode-escape').decode('ASCII')
        amount_string = amount_string.replace(",", "").replace(
            '\n', '').replace('\\s+', '').replace('\\uf156','').replace(' ', '').replace(
            '..', '.').replace('(', '').replace(')', '').replace(
            '--', '-').replace('\\u20b9', '').replace('\\xa0', '').replace('INR', '').replace(
            'IDR', '').replace('Rs.', '').replace('Rs', '').replace('|','').replace('PHP', '').replace(
            '+', '').replace('\\ufffd', '').replace('\\t', '').replace('  ', ' ').replace(
            '    ', ' ').replace('Re.', '').replace('\\xde', '').replace('/', '').replace('DB', '')
        try:
            amount_string = re.sub('\\-$', '', str(amount_string))
        except Exception as e:
            print(e)
        try:
            amount = float(
                re.sub('(?i)\\(?(?:cr|dr|db)?\\.?\\)?[A-Z]?$', '', str(amount_string.lower())))
            if amount not in [None,float("inf")]:
                return amount
        except ValueError:
            pass
        except Exception as e:
            print(e)
        try:
            amount = float(re.sub('(?i)(?:cr|dr|d)', '',
                                  str(amount_string.lower())))
            return amount
        except ValueError:
            pass
        except Exception as e:
            print(e)
        try:
            amount_string = re.sub('(?i)(?:cr|dr|d)', '',
                                   str(amount_string.lower()))
            amount = float(re.sub('(?i)^\\.+', '', str(amount_string.lower())))
            return amount
        except ValueError:
            pass
        except Exception as e:
            print(e)
        try:
            amount = float(re.sub('(?i)^\\.+|\\.+$', '',
                                  str(amount_string.lower())))
            return amount
        except ValueError:
            pass
        except Exception as e:
            print(e)
    else:
        return None
    

def remove_re_compile(compiled_regex):
    if compiled_regex!=None and isinstance(compiled_regex, re.Pattern):
        return compiled_regex.pattern

    return compiled_regex

def get_ocr_condition_for_credit_card_statement(doc, page_number=0):
    if isinstance(doc,int):
        return False
    
    all_text = doc[page_number].get_text()
    all_text_len = len(all_text)
    cnt=0
    for txt in all_text:
        if txt!=None and '�' in txt:
            cnt=cnt+1
    
    if all_text_len>0 and cnt/all_text_len >= 0.7:
        return True
    return False

def convert_pandas_timestamp_to_date_string(date_obj):
    if date_obj is None:
        return None
    
    if(type(date_obj)==pd._libs.tslibs.timestamps.Timestamp):
        date_obj = date_obj.to_pydatetime()
    
    if type(date_obj) == datetime:
        date_obj = date_obj.strftime("%Y-%m-%d %H:%M:%S")

    return date_obj

def print_on_condition(statement: str, should_be_printed=False) -> None:
    
    if should_be_printed:
        print(statement)

def fix_hsbc_ocr_dates(row):
    row['date'] = row['date'].replace(' ', '').replace('\n', '').replace("'", '')

    # replace O with 0 in day and year
    regex_match = re.match('(?i).*^([0-9]{2}.*[0-9]{4}).*', row['date'])
    if regex_match is not None:
        row['date'] = row['date'][:2].replace('O', '0') + row['date'][2:-4] + row['date'][-4:].replace('O', '0')

    months = {'Jan': [], 'Feb': [], 'Mar': [], 'Apr': [], 'May': [], 'Jun': [],
              'Jul': [], 'Aug': [], 'Sep': ['s8ep', '8ep'], 'Oct': ['0ct', '0\u00A2t', '0c\u00A2t', '0\u00A2t', 'O\u00A2t', 'Oc\u00A2t'], 
              'Nov': ['n0v'], 'Dec': []}
    date_month = None
    if len(row['date']) > 6: 
        date_month = row['date'][2:-4]

    if date_month is not None:
        for correct, incorrect in months.items():
            if date_month.lower() in incorrect:
                row['date'] = row['date'][:2] + correct + row['date'][-4:] 
    
    date_regex_match = re.match('(?i).*^([0-9]{2}[a-zA-Z]{3}[0-9]{4}).*', row['date'])
    if date_regex_match is not None:
        day = row['date'][:2]
        if int(day) > 31 and int(day) <=39:
            temp_date = '01' + row['date'][2:]
            temp_date = datetime.datetime.strptime(temp_date, "%d%b%Y")
            next_month = temp_date.replace(day=28) + datetime.timedelta(days=4)
            res = next_month - datetime.timedelta(days=next_month.day)
            res_date = str(res.day) + row['date'][2:]  
            row['date'] = res_date  
        elif int(day) >= 40 and int(day) <= 49:
            row['date'] = '1' + row['date'][1:]
    return row


def format_hsbc_ocr_rows(df):
    df = df.apply(lambda row: fix_hsbc_ocr_dates(row), axis=1)
    df = df.apply(lambda row: check_format_row(row), axis=1)
    df["transaction_merge_flag"] = False
    total_num_transaction_row = df[df['is_balance'] == True].shape[0]
    num_transaction_row_done = 0
    row_dicts = df.to_dict('records')
    transaction_started = False
    str_epoch_date = EPOCH_DATE.strftime("%d-%m-%Y")
    prev_date = str_epoch_date
    default_amount = "-1.0"
    transaction_i = -1
    next_transaction_i = -1
    transactions_begining = False
    clean_df_index = None

    for i in range(0, len(row_dicts)):
        
        if (
            i > 0 and
            i < len(row_dicts) - 1  and
            row_dicts[i].get('balance') and
            row_dicts[i - 1].get('is_balance') and
            ('CLOSING' in row_dicts[i].get("transaction_note") or 'BALANCE' in row_dicts[i].get("transaction_note"))
        ):
            clean_df_index = i
            break

        if check_transaction_beginning(row_dicts[i].get("transaction_note"), True) or check_transaction_beginning(row_dicts[i].get("date"), True):
            transactions_begining = True
            next_transaction_i = i+1
        elif (check_transaction_beginning(row_dicts[i].get("transaction_note"), False) or check_transaction_beginning(row_dicts[i].get("date"),False)) and next_transaction_i > -1 and not row_dicts[next_transaction_i].get("is_balance") and transactions_begining:
            row_dicts[next_transaction_i]["transaction_merge_flag"] = True
            row_dicts[next_transaction_i]["date"] = prev_date
            row_dicts[next_transaction_i]["credit"] = default_amount
            row_dicts[next_transaction_i]["balance"] = default_amount
            row_dicts[i]["balance"] = ""
            break
        elif row_dicts[i]["is_date_used"] and num_transaction_row_done <= total_num_transaction_row and transactions_begining:
            transaction_started = True
            if row_dicts[i]["is_balance"]:
                transaction_started = False
                num_transaction_row_done += 1
                next_transaction_i = i+1
            else:
                transaction_i = i
                prev_date = row_dicts[i]["date"]
        elif row_dicts[i].get("is_balance") and transaction_started and num_transaction_row_done <= total_num_transaction_row and transactions_begining and (validate_amount(row_dicts[i].get("credit")) or validate_amount(row_dicts[i].get("debit"))):
            row_dicts[transaction_i]["credit"] = row_dicts[i].get("credit")
            row_dicts[transaction_i]["debit"] = row_dicts[i].get("debit")
            row_dicts[transaction_i]["balance"] = row_dicts[i].get("balance")
            row_dicts[transaction_i]["is_balance"] = row_dicts[i].get("is_balance")
            if i!=transaction_i:
                row_dicts[i]["credit"] = ""
                row_dicts[i]["debit"] = ""
                row_dicts[i]["balance"] = ""
                row_dicts[i]["is_balance"] = row_dicts[i].get("is_balance")
            transaction_started = False
            num_transaction_row_done += 1
            next_transaction_i = i+1
        elif row_dicts[i].get("is_balance") and num_transaction_row_done <= total_num_transaction_row and next_transaction_i>-1 and transactions_begining and (validate_amount(row_dicts[i].get("credit")) or validate_amount(row_dicts[i].get("debit"))):
            if prev_date == str_epoch_date:
                row_dicts[next_transaction_i]["transaction_merge_flag"] = True
            row_dicts[next_transaction_i]["date"] = prev_date
            row_dicts[next_transaction_i]["credit"] = row_dicts[i].get("credit")
            row_dicts[next_transaction_i]["debit"] = row_dicts[i].get("debit")
            row_dicts[next_transaction_i]["balance"] = row_dicts[i].get("balance")
            row_dicts[next_transaction_i]["is_balance"] = row_dicts[i].get("is_balance")
            if i!=next_transaction_i or check_transaction_beginning(row_dicts[i].get("transaction_note"), False):
                row_dicts[i]["date"] = ""
                row_dicts[i]["credit"] = ""
                row_dicts[i]["debit"] = ""
                row_dicts[i]["balance"] = ""
                row_dicts[i]["is_balance"] = row_dicts[i].get("is_balance")
            transaction_started = False
            num_transaction_row_done += 1
            next_transaction_i = i+1
        elif i > 0 and (validate_amount(row_dicts[i].get("credit")) or validate_amount(row_dicts[i].get("debit"))) and num_transaction_row_done <= total_num_transaction_row and next_transaction_i>-1 and transactions_begining and row_dicts[i-1].get("is_balance") and not(validate_amount(row_dicts[i-1].get("credit")) or validate_amount(row_dicts[i-1].get("debit"))) :
            if prev_date == str_epoch_date:
                row_dicts[next_transaction_i]["transaction_merge_flag"] = True
            row_dicts[next_transaction_i]["date"] = prev_date
            row_dicts[next_transaction_i]["credit"] = row_dicts[i].get("credit")
            row_dicts[next_transaction_i]["debit"] = row_dicts[i].get("debit")
            row_dicts[next_transaction_i]["balance"] = row_dicts[i-1].get("balance")
            row_dicts[next_transaction_i]["is_balance"] = row_dicts[i].get("is_balance")
            if i!=next_transaction_i or check_transaction_beginning(row_dicts[i].get("transaction_note"), False):
                row_dicts[i]["date"] = ""
                row_dicts[i]["credit"] = ""
                row_dicts[i]["debit"] = ""
                row_dicts[i]["balance"] = ""
                row_dicts[i]["is_balance"] = row_dicts[i]["is_balance"]
            transaction_started = False
            num_transaction_row_done += 1
            next_transaction_i = i+1
        elif i > 0 and (validate_amount(row_dicts[i-1].get("credit")) and not row_dicts[i-1]["is_balance"]) and num_transaction_row_done <= total_num_transaction_row and next_transaction_i>-1 and transactions_begining and row_dicts[i].get("is_balance"):
            if prev_date == str_epoch_date:
                row_dicts[next_transaction_i]["transaction_merge_flag"] = True
            row_dicts[next_transaction_i]["date"] = prev_date
            row_dicts[next_transaction_i]["credit"] = row_dicts[i-1].get("credit")
            row_dicts[next_transaction_i]["debit"] = row_dicts[i].get("debit")
            row_dicts[next_transaction_i]["balance"] = row_dicts[i].get("balance")
            row_dicts[next_transaction_i]["is_balance"] = row_dicts[i].get("is_balance")
            if i!=next_transaction_i or check_transaction_beginning(row_dicts[i].get("transaction_note"), False):
                row_dicts[i]["date"] = ""
                row_dicts[i]["credit"] = ""
                row_dicts[i]["debit"] = ""
                row_dicts[i]["balance"] = ""
                row_dicts[i]["is_balance"] = row_dicts[i].get("is_balance")
            transaction_started = False
            num_transaction_row_done += 1
            next_transaction_i = i+1
        elif i > 0 and (validate_amount(row_dicts[i-1].get("debit")) and not row_dicts[i-1]["is_balance"]) and num_transaction_row_done <= total_num_transaction_row and next_transaction_i>-1 and transactions_begining and row_dicts[i].get("is_balance"):
            if prev_date == str_epoch_date:
                row_dicts[next_transaction_i]["transaction_merge_flag"] = True
            row_dicts[next_transaction_i]["date"] = prev_date
            row_dicts[next_transaction_i]["credit"] = row_dicts[i].get("credit")
            row_dicts[next_transaction_i]["debit"] = row_dicts[i-1].get("debit")
            row_dicts[next_transaction_i]["balance"] = row_dicts[i].get("balance")
            row_dicts[next_transaction_i]["is_balance"] = row_dicts[i].get("is_balance")
            if i!=next_transaction_i or check_transaction_beginning(row_dicts[i].get("transaction_note"), False):
                row_dicts[i]["date"] = ""
                row_dicts[i]["credit"] = ""
                row_dicts[i]["debit"] = ""
                row_dicts[i]["balance"] = ""
                row_dicts[i]["is_balance"] = row_dicts[i].get("is_balance")
            transaction_started = False
            num_transaction_row_done += 1
            next_transaction_i = i+1

    df = pd.DataFrame(row_dicts)

    # removing extra lines in df as the transactions have ended
    if clean_df_index and isinstance(clean_df_index, int):
        df = df.iloc[:clean_df_index]
    
    df.drop(['is_balance', 'is_date_used'], axis=1, inplace=True)
    df.replace(r'^s*$', float('NaN'), regex = True, inplace=True)
    df.dropna(how='all', inplace=True)
    df.replace(float('NaN'), '', inplace=True)
    return df

def process_hsbc_ocr_transactions(all_transactions):

    default_balance = -1.0
    pages_updated = set()

    '''
    Santize transactions -> Fill missing dates, club transaction notes and fill missing balances.
    '''
    for transaction in all_transactions:
        if transaction.get("transaction_merge_flag") and transaction["date"] != EPOCH_DATE_STR and transaction["balance"] == default_balance:
            index = all_transactions.index(transaction)
            index += 1 
            try:
                next_transaction = all_transactions[index]
                transaction["transaction_note"] += ' ' + next_transaction["transaction_note"] 
                transaction["balance"] = next_transaction["balance"]
                transaction["amount"] = next_transaction["amount"]
                transaction["transaction_type"] = next_transaction["transaction_type"]
                pages_updated.add(all_transactions[index - 1].get('page_number', None))
                pages_updated.add(all_transactions[index].get('page_number', None))
                del all_transactions[index]
            except Exception as _:
                pages_updated.add(all_transactions[index - 1].get('page_number', None))
                del all_transactions[index-1]
        elif transaction.get("transaction_merge_flag") and transaction["date"] == EPOCH_DATE_STR and transaction["balance"] != default_balance:
            index = all_transactions.index(transaction)
            index -= 1 
            previous_transaction = all_transactions[index]
            transaction["date"] = previous_transaction["date"]
            pages_updated.add(transaction.get('page_number', None))
        elif transaction.get("transaction_merge_flag") and transaction["date"] == EPOCH_DATE_STR and transaction["balance"] == default_balance:
            index = all_transactions.index(transaction)
            next_index = index + 1
            try:
                next_transaction = all_transactions[next_index]
                transaction["transaction_note"] += ' ' + next_transaction["transaction_note"] 
                transaction["balance"] = next_transaction["balance"]
                transaction["amount"] = next_transaction["amount"]
                transaction["transaction_type"] = next_transaction["transaction_type"]
                prev_index = index -  1
                previous_transaction = all_transactions[prev_index]
                transaction["date"] = previous_transaction["date"]
                pages_updated.add(transaction.get('page_number', None))
                pages_updated.add(all_transactions[next_index].get('page_number', None))
                del all_transactions[next_index]
            except Exception as _:
                pages_updated.add(all_transactions[next_index - 1].get('page_number', None))
                del all_transactions[next_index -  1]
        if "transaction_merge_flag" in transaction:
            del transaction["transaction_merge_flag"]
    
    '''
    Sanitise transactions
    '''
    temp_transactions = deepcopy(all_transactions)
    if len(temp_transactions) > 0:
        temp_transactions = temp_transactions[::-1]
        last_date = temp_transactions[0]["date"]
        for transaction in temp_transactions[1:]:
            if transaction["date"]==EPOCH_DATE_STR:
                transaction["date"]=last_date
                pages_updated.add(transaction.get('page_number', None))
            else:
                last_date = transaction["date"]
    all_transactions = temp_transactions[::-1]

    return all_transactions, pages_updated

def sanitize_transaction_note(row):
    if 'transaction_note' in row.keys() and isinstance(row['transaction_note'], str):
        if row['transaction_note'].strip().startswith('-'):
            row['transaction_note'] = row['transaction_note'].strip()
            row['transaction_note'] = row['transaction_note'][1:]
    return row

# This method for "indusind" only
# Where last page last transactions extracting from page footer
def remove_indusind_invalid_transactions(total_pages, current_page, all_transactions):
    try:
        if current_page == (total_pages - 1):
            last_2_trans = all_transactions[-2:]
            # Check last page has 2 or more trans
            if len(last_2_trans) > 1:
                if last_2_trans[0]['balance'] == last_2_trans[1]['balance']:
                    all_transactions = all_transactions[:-1]
    except Exception as e:
        capture_exception(e)
        print(str(e))
    
    return all_transactions


def get_account_wise_transactions_dict(transaction_df, identity_account_number, statement_level_call=False, all_extracted_accounts={}):
    
    if isinstance(transaction_df, pd.DataFrame):
        transaction_df = transaction_df.to_dict('records')
    account_transactions_dict = defaultdict(list)
    prev_account_number = ''
    prev_account_category = ''

    if statement_level_call:
        account_transactions_dict[identity_account_number] = []
        prev_account_number = identity_account_number
    
    for txn in transaction_df:
        account_number = txn.get('account_number')

        if prev_account_number and not account_number:
            txn['account_number'] = prev_account_number
            txn['account_category'] = prev_account_category

        prev_account_number = txn.get('account_number')
        prev_account_category = txn.get('account_category')
        account_number = txn.get('account_number')
        
        if txn.get('zero_transaction_page_account') and statement_level_call:
            account_transactions_dict[account_number]
        else:
            account_transactions_dict[account_number].append(txn)

        if txn.get('last_account_number'):
            prev_account_number = txn.get('last_account_number')
            prev_account_category = txn.get('last_account_category')
        
        if statement_level_call:
            txn.pop('last_account_number', None)
            txn.pop('last_account_category', None)

    for account in all_extracted_accounts:
        if account and account not in account_transactions_dict.keys():
            txn = {}
            txn['date'] = datetime.strptime(DEFAULT_TIMESTAMP_UTC, '%Y-%m-%d %H:%M:%S')
            txn['account_number'] = account
            txn['account_category'] = all_extracted_accounts[account]
            txn['transaction_type'] = "credit"
            txn['transaction_note'] = ""
            txn['zero_transaction_page_account'] = True
            txn['amount'] = DEFAULT_BALANCE_FLOAT
            txn['balance'] = DEFAULT_BALANCE_FLOAT
            txn['chq_num'] = ""
            txn.pop('transaction_merge_flag', None)
            txn.pop('date_formatted', None)

            account_transactions_dict[account].append(txn)
    
    if statement_level_call:
        account_transactions_dict = merge_same_accounts_data(account_transactions_dict, identity_account_number)
    
    return account_transactions_dict

def merge_same_accounts_data(account_transactions_dict, identity_account_number):

    account_transactions_dict = dedup_accounts(identity_account_number, account_transactions_dict)
    accounts = list(account_transactions_dict.keys())

    for acc in accounts:
        account_transactions_dict = dedup_accounts(acc, account_transactions_dict)

    return account_transactions_dict

def dedup_accounts(account_number, account_transactions_dict):

    accounts = list(account_transactions_dict.keys())
    for account in accounts:
        if (
            account != account_number 
            and len(account) > 3 
            and len(account_number) > 3 
            and account[-3:] == account_number[-3:] 
            and len(account_transactions_dict[account_number]) == 0
        ):
            account_transactions_dict[account_number] = account_transactions_dict[account]
            account_transactions_dict.pop(account)
        elif (
            account != account_number 
            and len(account) > 3 
            and len(account_number) > 3 
            and account[-2:] == account_number[-2:]
            ):
            capture_message(f"accounts have same last 2 digits {account}, {account_number}")

    return account_transactions_dict

def find_all_accounts_in_table(txn_dataframe):

    all_extracted_accounts = defaultdict(str)
    txn_list = txn_dataframe

    if isinstance(txn_dataframe, pd.DataFrame):
        txn_list = txn_dataframe.to_dict('records')

    for row in txn_list:
        if row['account_number']:
            all_extracted_accounts[row['account_number']] = row['account_category']

    return all_extracted_accounts

def recombine_account_transctions(account_transactions_dict):
    return_data = []

    for account_number, txns in account_transactions_dict.items():
        if isinstance(txns, pd.DataFrame):
            txns = txns.to_dict('records')
        return_data.extend(txns)

    return return_data


def get_pages(path, password):
    try:
        pdf = pdfplumber.open(path, password=password)
        all_pages = pdf.pages
    except (ValueError, PSEOF, TypeError, PDFSyntaxError):
        raise NonParsablePDF
    except (PDFEncryptionError, struct.error):
        if password:
            return get_pages(path, '')
        raise PDFPasswordIncorrect

    return all_pages

def get_amount_sign(amount_string, bank=''):
    amount_sign = match_regex(amount_string, '(?i).*(cr|dr|d|-).*', 1)
    if amount_sign is not None:
        if amount_sign.upper() == 'CR':
            return 1
        elif amount_sign.upper() == 'DR':
            if amount_string and amount_string[0] == '-':
                return 1
            return -1
        elif amount_sign.upper() == 'D':
            if amount_string and amount_string[0] == '-':
                return 1
            return -1
        elif amount_sign == '-' and amount_string and amount_string[-1] == '-' and bank in ['sbi']:
            return -1
    return None

def remove_next_page_data_after_footer(txn_df):
    # print(txn_df.columns)
    key_words_came = False
    index = -1
    for i in range(txn_df.shape[0]):
        combined_row = ' '.join(txn_df.iloc[i].astype(str))
        if combined_row.startswith("Date Value Date Particulars Cheque# Withdrawals Deposits Balance"):
            if key_words_came:
                index = i
                txn_df = txn_df.iloc[:index]
                break
            key_words_came = True
    return txn_df

def get_first_transaction_type(transaction_df, opening_balance):
    if not opening_balance or transaction_df.shape[0] < 1:
        return transaction_df

    if opening_balance - transaction_df['amount'][0] == transaction_df['balance'][0]:
        transaction_df['transaction_type'][0] = 'debit'
    elif opening_balance + transaction_df['amount'][0] == transaction_df['balance'][0]:
        transaction_df['transaction_type'][0] = 'credit'

    return transaction_df

def log_data(message, LOGGER=None, local_logging_context=None, log_type=None):
    if LOGGER and local_logging_context and log_type:
        if log_type == "info":
            LOGGER.info(
                msg = message,
                extra = local_logging_context.store
            )
        elif log_type == "debug":
            LOGGER.debug(
                msg = message,
                extra = local_logging_context.store
            )
        elif log_type == "warning":
            LOGGER.warning(
                msg = message,
                extra = local_logging_context.store
            )
        elif log_type == "error":
            LOGGER.error(
                msg = message,
                extra = local_logging_context.store
            )
        else:
            raise Exception("Log Type should be one in info, debug, warning, error")
    print(message)

def get_bank_threshold_diff(bank_name):
    '''
        This threshold decides how much to ignore while checking inconsistent balances.
    '''
    if isinstance(bank_name, str) and bank_name.lower() in ["central", "indusind", "mahabk"]:
        return 1
    return 0.1


def process_mock_transaction(transaction_df, bank):
    if isinstance(transaction_df, pd.DataFrame):
        transaction_df = transaction_df.to_dict('records')
    else:
        return transaction_df

    make_next_transaction_mock = False
    mock_count = 0  # To track the number of mock transactions
    
    # Regex patterns for matching the date
    combined_pattern = SPLIT_TRANSACTION_NOTES_PATTERNS.get(bank).get("regex")
    keys = SPLIT_TRANSACTION_NOTES_PATTERNS.get(bank).get("keys")

    for _, txn in enumerate(transaction_df):
        if txn.get('is_transaction_row', False):
            break
        
        test_string = ' '.join(str(txn.get(key, '')) for key in keys if key in txn)

        # Ensure only date, account_number and transaction_note are non-empty, other keys should be empty
        # Assuming all partial transaction will be encountered in the starting itself
        if isinstance(txn.get('date', ''), str) and ([False if val not in ['account_number', 'transaction_note','date'] and txn.get(val) not in ['', None, False] else True for val in txn]) and\
            re.match(combined_pattern, test_string):
            make_next_transaction_mock = True

        elif make_next_transaction_mock and\
             mock_count <= 3 and\
             txn.get('date') in [None, ""] and\
             txn.get('balance') in [None, ""] and\
             txn.get('amount') in [None, ""]:
            if not mock_count:
                make_mock_transaction(txn)
            else:
                make_mock_transaction(txn, txn.get('transaction_note'))
            mock_count += 1  # Increment mock transaction count
    return pd.DataFrame(transaction_df)


def make_mock_transaction(txn, transaction_note=None):
    if transaction_note is None:
        txn['date'] = datetime.strptime(DEFAULT_TIMESTAMP_UTC, '%Y-%m-%d %H:%M:%S')
        txn['amount'] = str(DEFAULT_BALANCE_FLOAT)
        txn['balance'] = str(DEFAULT_BALANCE_FLOAT)
        txn['is_balance'] = True
        txn['is_date_used'] = True
        txn['is_transaction_row'] = True
        txn['is_amount_used'] = True
        txn['date_formatted'] = txn['date']
        txn['transaction_merge_flag'] = True
    else:
        txn['transaction_note'] = transaction_note

    return txn