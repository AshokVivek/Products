import os
import re
import json
from itertools import groupby
from operator import itemgetter
import datetime
# import pdftables_api
import csv
from dateutil.tz import tzutc, tzoffset
import uuid
import pdfplumber

import fitz
import pymupdf
import requests
from library.helpers.identity import check_company_name
from library.utils import check_date, match_compiled_regex, remove_unicode, amount_to_float, get_amount_sign
from library.utils import get_compiled_regex_list, match_regex, convert_date_indo_to_eng, INDONESIAN_BANKS
from library.statement_plumber import get_amount_sign_cc

# only for federal for now
from library.identity_plumber import extract_essential_identity_plumber

from library.stream_frauds import get_stream_fonts
from library.helpers.constants import CREDIT_CARD_TYPE_WORDS_TO_REMOVE, CREDIT_CARD_TYPE_WORDS_TO_REMOVE_STARTING, CREDIT_CARD_TYPE_WORDS_TO_REMOVE_END, CREDIT_CARD_TYPE_WORDS_TO_REMOVE_ANYWHERE, SKIP_UNICODE_REMOVAL_LIST, EMAIL_MINIMUM_PERMISSIBLE_LENGTH, INDIAN_PAN_NUMBER_MINIMUM_PERMISSIBLE_LENGTH, INDIAN_PHONE_NUMBER_MINIMUM_PERMISSIBLE_LENGTH

import boto3
from boto3.dynamodb.conditions import Key
import hashlib
from fuzzywuzzy import process, fuzz
from decimal import Decimal
import ast
import pikepdf
from sentry_sdk import capture_exception, capture_message

from library.utils import AMOUNT_WORDS, BALANCE_WORD, DATE_WORD, keyword_helper
from typing import Any, List, Tuple, Optional, Union

REGION = os.environ.get('REGION', 'ap-south-1')
dynamo_db = boto3.resource('dynamodb', region_name=REGION)

CURRENT_STAGE = os.environ.get('CURRENT_STAGE', 'dev')
if CURRENT_STAGE=='dev2':
    CURRENT_STAGE='dev'
api_keys_table_name = "pdf_tables_api_key_{}".format(CURRENT_STAGE)
api_keys_table = dynamo_db.Table(api_keys_table_name)


def collect_results(table_f, qp):
    items = []
    while True:
        r = table_f(**qp)
        items.extend(r['Items'])
        lek = r.get('LastEvaluatedKey')
        if lek is None or lek == '':
            break
        qp['ExclusiveStartKey'] = lek
    return items


def read_pdf(path, password):
    """
    Takes a pdf file path, and password and returns a fitz document
    :param: path (path to pdf file), password (password for the pdf file)
    :return: fitz document object, returns 0 if authentication fails
             or -1 if file doesn't exists or is not a pdf file
    """
    try:
        doc = fitz.Document(path)
        if doc.needs_pass and doc.authenticate(password=password) == 0:
            # password authentication failed
            return 0
    except RuntimeError:
        # file not found or is not a pdf file
        return -1
    return doc


def relu(x):
    if x > 0:
        return x
    return 0


def get_vertical_overlap(box1, box2):
    x11, y11, x12, y12 = box1[:4]
    x21, y21, x22, y22 = box2[:4]
    if y11 < y21:
        overlap = relu(y12 - y21) / float(y22 - y11)
    else:
        try:
            overlap = relu(y22 - y11) / float(y12 - y21)
        except ZeroDivisionError:
            overlap = 0.0
    return overlap


def get_horizontal_overlap(box1, box2):
    x11, y11, x12, y12 = box1[:4]
    x21, y21, x22, y22 = box2[:4]
    if x11 < x21:
        overlap = relu(x12 - x21) / float(x22 - x11)
    else:
        try:
            overlap = relu(x22 - x11) / float(x12 - x21)
        except ZeroDivisionError:
            overlap = 0.0
    return overlap


def get_sorted_boxes(words, is_rotated = False):
    prev_word = None
    if is_rotated:
        words.sort(key=lambda k: (k[2], -k[1]))
    else:
        words.sort(key=itemgetter(3, 0))
    for word in words:
        if prev_word is None:
            prev_word = word
            continue
        if get_horizontal_overlap(word, prev_word) < 0.1 and get_vertical_overlap(word, prev_word) > 0.7:
            word[1] = prev_word[1]
            word[3] = prev_word[3]
        prev_word = word
    if is_rotated:
        words.sort(key=lambda k: (k[2], -k[1]))
    else:
        words.sort(key=itemgetter(3, 0))
    return words


def de_dup_words(words):
    present_words = set()
    new_list = list()
    for word in words:
        c_word = (word[0], word[1], word[4].encode('utf-8'))
        if c_word in present_words:
            continue
        else:
            new_list.append(word)
            present_words.add(c_word)

    return new_list


def get_text_in_box(page, box):
    is_rotated = False
    if page.derotation_matrix[5] != 0:
        is_rotated = True

    rect = fitz.Rect(box)
    try:
        words = page.get_text_words(flags=pymupdf.TEXTFLAGS_WORDS)
    except Exception as _:
        words=[]

    extracted_words = [list(w) for w in words if fitz.Rect(w[:4]) in rect]
    # print("raw words -> ", extracted_words)
    extracted_words = de_dup_words(extracted_words)
    extracted_words = get_sorted_boxes(extracted_words, is_rotated)

    if is_rotated:
        group = groupby(extracted_words, key=itemgetter(2))
    else:
        group = groupby(extracted_words, key=itemgetter(3))

    string_list = list()
    for y1, g_words in group:
        string_list.append(" ".join(w[4] for w in g_words))
    # print("List -> ", string_list)
    return '\n'.join(string_list)


def get_date_range(doc, bank, bbox, get_only_all_text=False, path=None):

    from_date, to_date = None, None

    for i in range(min(3, doc.page_count)):
        for template in bbox:
            if template.get("image_flag",False):
                continue
            from_date = get_date(doc[i], template['from_bbox'], template['from_regex'], bank, get_only_all_text, date_format=template.get('date_format'))
            to_date = get_date(doc[i], template['to_bbox'], template['to_regex'], bank, get_only_all_text, date_format=template.get('date_format'))

            if get_only_all_text:
                return {'from_date': from_date, 'to_date': to_date}, None
            
            if from_date and to_date and from_date > to_date:
                from_date, to_date = to_date, from_date
            
            if from_date is not None and to_date is not None:
                return {'from_date': from_date.strftime("%Y-%m-%d"), 'to_date': to_date.strftime("%Y-%m-%d")}, template.get('uuid')

    return {'from_date': None, 'to_date': None}, None

def get_opening_date(doc, bank, bbox, path=None, get_only_all_text=False):
    date = None
    for page_number in range(0, min(doc.page_count, 3)):
        for template in bbox:
            if template.get("image_flag",False):
                continue
            text = get_text_in_box(doc[page_number], template['bbox'])
            all_text = text.replace('\n', '').replace(' ', '').replace('(cid:9)', '')

            if bank not in SKIP_UNICODE_REMOVAL_LIST:
                all_text = remove_unicode(all_text)
            
            if get_only_all_text:
                return all_text, None
            
            date = match_regex(all_text, template['regex'], 1)
            if date is not None and date != '':
                if bank in INDONESIAN_BANKS:
                    date = convert_date_indo_to_eng(date)

                date, _ = check_date(date)
                if date:
                    if bank in ['maybnk']:
                        date = date.replace(day=1)
                    return date.strftime("%Y-%m-%d"), template.get('uuid')
    return None, None

def get_account_opening_date(doc, bank, bbox, path=None):
    date = None
    for page_number in range(0, min(doc.page_count, 1)):
        for template in bbox:
            text = get_text_in_box(doc[page_number], template['bbox'])
            all_text = text.replace('\n', '').replace(' ', '').replace('(cid:9)', '')

            if bank not in SKIP_UNICODE_REMOVAL_LIST:
                all_text = remove_unicode(all_text)
            
            date = match_regex(all_text, template['regex'], 1)
            if date is not None and date != '':
                if bank in INDONESIAN_BANKS:
                    date = convert_date_indo_to_eng(date)

                date, _ = check_date(date)
                if date:
                    return date.strftime("%Y-%m-%d")
    return None

def get_date(page, bbox, regex, bank, get_only_all_text, date_format=None):
    text = get_text_in_box(page, bbox)
    all_text = text.replace('\n', '').replace(' ', '').replace('(cid:9)', '')

    if bank not in SKIP_UNICODE_REMOVAL_LIST:
        all_text = remove_unicode(all_text)

    if get_only_all_text:
        return all_text
    
    date = match_regex(all_text, regex, 1)
    if bank in ['indbnk'] and date is not None and date != '':
        regex_31 = re.compile('(?i)(31[\-\/](?:09|04|06|11)[\/\-][0-9]{2,4}).*')
        if re.match(regex_31, date):
            date = date.replace('31', '30', 1)
    # print("\n\"", all_text, "\" -->",regex, "-->", date)
    if date not in [None,''] and bank in INDONESIAN_BANKS:
        date = convert_date_indo_to_eng(date)

    date_to_return, _ = check_date(date, format=date_format)
    if date_to_return:
        return date_to_return
    else:
        return None


def get_name(doc, bbox, bank, path=None, get_only_all_text=False, is_credit_card=False):
    words_to_remove = ['date', 'branch', 'account', 'report', 'name', 'transaction', 'STATEMENT', ':', '-', 'sector',
                       'state', 'bank', 'salary', 'inr', 'download', 'number', 'from', 'period',
                       'amount', 'type', 'search', 'remarks', 'cheque', 'Accoun', 'POST', 'NARRATION',
                       "Primary", "Holder", "customer", "address",  "customer address", "ifsc", "micr", "account", 
                       "account no.", "main", "null", "DETAILS"]

    for page_number in range(0, min(doc.page_count, 3)):
        for each_temp in bbox:
            if each_temp.get("image_flag",False):
                continue
            name = get_temp_name(each_temp, doc[page_number], bank, get_only_all_text=get_only_all_text)
            if get_only_all_text:
                return name, None, page_number
            
            name = name.upper().split()
            name = [ _ for _ in name if _ not in list(map(str.upper, words_to_remove))]
            name = " ".join(name)

            name = name.lstrip().rstrip()
            skip_check_name_correctness = ["alrajhi"]
            if len(name.replace(' ', '')) > 2:
                template_uuid = each_temp.get('uuid', '')
                if bank in skip_check_name_correctness:
                    return name.strip(), template_uuid, page_number

                # elif check_name_correctness(name):
                response_name = name.strip()
                if bank in ['mandiribnk']:
                    response_name = removeduplicates_from_name(response_name)
                if bank in ['federal1','federal']:
                    response_name = removeduplicates_from_name_double_occurances(response_name)
                return response_name, template_uuid, page_number
    return None, None, None

def removeduplicates_from_name(name):
    # SAFARINDO INTERNUSA SAFARINDO INTERNUSA -> SAFARINDO INTERNUSA

    splitted = name.split(" ")
    if(len(splitted)%2)==0:
        first_half = " ".join(splitted[:len(splitted)//2])
        second_half = " ".join(splitted[len(splitted)//2:])
        if first_half==second_half:
            return first_half
        else:
            return name
    else:
        return name
    
def removeduplicates_from_name_double_occurances(name):
    # RAM RAM KUMAR KUMAR -> RAM KUMAR

    modified_list = []
    splitted = name.split(" ")
    if(len(splitted)%2)==0:
        for i in range(0,len(splitted),2):
            if splitted[i]==splitted[i+1]:
                modified_list.append(splitted[i])
            else:
                return name
    else:
        return name
    
    return " ".join(modified_list)

def get_joint_account_holders_name(doc: Any, templates: List[dict], bank: str, get_only_all_text: bool, extract_from_page_number: Optional[int]=None) -> Tuple[List[str], Optional[str]]:
    # For joint account holder name extraction, we are not using bbox and removing the coordinate based extraction and using all text and regex for extraction
    joint_account_holders = []
    template_uuid =  None
    
    if not templates:
        return joint_account_holders, template_uuid
    
    page_count = doc.page_count
    for page_number in range(0, min(page_count, 3)):
        if isinstance(extract_from_page_number, int) and page_number != extract_from_page_number:
            continue
        for template in templates:
            joint_account_holder_names = get_temporary_joint_account_holder_names(template, doc[page_number], bank, get_only_all_text=get_only_all_text)
            if get_only_all_text:
                return joint_account_holder_names, template_uuid
            if joint_account_holder_names:
                template_uuid = template.get('uuid')
                return joint_account_holder_names, template_uuid

    return joint_account_holders, template_uuid

def get_temporary_joint_account_holder_names(template: dict, page: Any, bank: str, get_only_all_text: Optional[bool]=False) ->  Union[str, List[str]]:
    joint_account_holders = []
    all_text = page.get_text()
    if get_only_all_text:
        return all_text

    regex = template.get('regex')
    if regex is not None and all_text is not None:
        regex_match = re.match(regex, all_text)
        if regex_match is not None:
            joint_account_holders = list(regex_match.groups())
    
    return joint_account_holders
        

def get_temp_address(page, template, get_only_all_text=False):
    address = ''
    all_text = get_text_in_box(page, template.get('bbox'))

    if all_text is not None:
        all_text = all_text.replace('\n', ' ').replace('(cid:9)', '')

    regex = template.get('regex')
    all_text = remove_unicode(all_text)

    if get_only_all_text:
        return all_text

    if regex is not None and all_text is not None:
        regex_match = re.match(regex, all_text)
        if regex_match is not None:
            address = ' '.join(re.match(regex, all_text).groups())
    if address != None:
        expr = re.compile('\d{2}/\d{2}/\d{4}')
        address1 = re.sub(expr, '', address)
        address = re.sub('\\s+', ' ', address1)
    # print("\n\"", all_text, "\" -->",regex, "-->", address)
    return address


def get_address(doc, bbox, path=None, get_only_all_text=False, is_credit_card=False):
    for page_number in range(0, min(doc.page_count, 2)):
        for template in bbox:
            if template.get("image_flag",False):
                continue
            address = get_temp_address(doc[page_number], template, get_only_all_text=get_only_all_text)
            if get_only_all_text:
                return address, None, page_number
            if address != None and len(address) > 5:
                template_uuid = template.get('uuid', '')
                return address.strip(), template_uuid, page_number
    return None, None, None


def get_temp_name(template, page, bank, get_only_all_text=False):
    name = ''
    all_text = get_text_in_box(page, template['bbox'])

    if all_text is not None:
        all_text = all_text.replace('(cid:9)', '')

    if bank not in SKIP_UNICODE_REMOVAL_LIST:
        all_text = remove_unicode(all_text)
    elif bank in ("mahagrambnk", "spcb"):
        all_text = all_text.replace('\x01', ' ')
    else:
        all_text = all_text.replace('\n', ' ')

    if get_only_all_text:
        return all_text

    regex = template.get('regex')

    if regex is not None and all_text is not None:
        regex_match = re.match(regex, all_text)
        if regex_match is not None:
            # print("GROUPS -> ", regex_match.groups(), " -> ", len(regex_match.groups()))
            name = ' '.join(regex_match.groups())
    # print("\n\"", all_text, "\" -->",regex, "-->", name)
    return re.sub(r'(\n|\s)+', ' ', name)

# This is a bank specific logic function required to extract name from multiple regex groups


def get_temp_name_multiple_groups(regex_match, bank):
    # bank = "ncb"
    if bank == "ncb" and len(regex_match.groups()) == 3:
        return regex_match.group(3) + regex_match.group(2)

    # fallback to group 1
    return regex_match.group(1)


def get_temp_account_number(page, template, bank, get_only_all_text=False):
    acc_num = ''
    all_text = get_text_in_box(page, template.get('bbox'))
    if all_text is not None:
        all_text = all_text.replace('\n', '').replace('(cid:9)', '')

    regex = template.get('regex')

    if bank not in SKIP_UNICODE_REMOVAL_LIST:
        all_text = remove_unicode(all_text)
    elif bank in ("mahagrambnk", "spcb"):
        all_text = all_text.replace('\x01', ' ')
    
    if get_only_all_text:
        return all_text

    if regex is not None and all_text is not None:
        regex_match = re.match(regex, all_text)
        if regex_match is not None:
            acc_num = regex_match.group(1)
    # print("\n\"", all_text, "\" -->",regex, "-->", acc_num)
    return acc_num.strip()


def get_account_num(doc, bbox, bank, path=None, get_only_all_text=False, is_credit_card=False):
    for page_number in range(0, min(doc.page_count, 4)):
        for template in bbox:
            if template.get("image_flag",False):
                continue
            acc_num = get_temp_account_number(doc[page_number], template, bank, get_only_all_text=get_only_all_text)
            if get_only_all_text:
                return acc_num, None, page_number
            if len(acc_num) > 3:
                template_uuid = template.get('uuid', '')
                return acc_num, template_uuid, page_number
    return None, None, None

def get_temp_limit(page, template, bank, get_only_all_text=False):
    limit = None
    is_od_account = False
    all_text = get_text_in_box(page, template.get('bbox'))
    
    if all_text is not None:
        all_text = all_text.replace('\n', '').replace('(cid:9)', '')

    regex = template.get('regex')

    if bank not in SKIP_UNICODE_REMOVAL_LIST:
        all_text = remove_unicode(all_text)

    if get_only_all_text:
        return all_text, None

    if regex is not None and all_text is not None:
        regex_match = re.match(regex, all_text)
        if regex_match is not None:
            limit = regex_match.group(1)
            if type(limit) == str and limit is not None:
                limit = limit.replace(',', '')
                try:
                    limit = Decimal(str(limit))
                except:  
                    print("limit bbox didn't convert str to decimal")  
                    return None, False
            if limit is not None and limit > 0:
                is_od_account = True
            
    print("\n\"", all_text, "\" -->",regex, "-->", limit)
    return limit, is_od_account


def get_credit_limit(doc, bbox, bank, path=None, get_only_all_text=False):
    for page_number in range(0, min(doc.page_count, 3)):
        for template in bbox:
            if template.get("image_flag",False):
                continue
            limit , is_od_account = get_temp_limit(doc[page_number], template, bank, get_only_all_text=get_only_all_text)
            if get_only_all_text:
                return limit, True, None
            if limit is not None and limit != '':
                return limit, is_od_account, template.get('uuid')
    return None, False, None


def get_micr(doc, bank, bbox, path=None, get_only_all_text=False):
    max_page_count = 5         

    adv_check_list = ['icici', 'karur', 'ubi', 'idbi', 'baroda']
    n = doc.page_count

    index_list =[*range(min(n, max_page_count))]
    
    if bank in adv_check_list and n > max_page_count:
        index_list+=[*range(n-3,n)]
        index_list = set(index_list)

    for page_number in index_list:
        for template in bbox:
            if template.get("image_flag",False):
                continue
            micr = get_temp_ifsc_micr(doc[page_number], template, get_only_all_text=get_only_all_text)
            if get_only_all_text:
                return micr, None
            if len(micr) > 2:
                return micr, template.get('uuid')
    return None, None

def get_currency(doc, bank, bbox, path=None):
    max_page_count = 1
    
    n = doc.page_count

    index_list =[*range(min(n, max_page_count))]

    for page_number in index_list:
        for template in bbox:
            if template.get("image_flag",False):
                continue
            currency = get_temp_currency(doc[page_number], template)
            if currency:
                return currency, template.get('uuid')
    return None, None


def get_account_category(doc, bank, bbox, mapping, path=None, get_only_all_text=False):
    max_page_count = 3
    for page_number in range(0, min(doc.page_count, max_page_count)):
        for template in bbox:
            if template.get("image_flag",False):
                continue
            account_category = get_temp_account_category(doc[page_number], template, get_only_all_text)
            if get_only_all_text:
                return account_category, None, None

            mapped_account_category = None
            if isinstance(mapping, list) and len(mapping) > 0:
                mapping = mapping[0]
            if mapping not in [{},None] and isinstance(mapping, dict):
                for key in mapping.keys():
                    if key in account_category.upper():
                        mapped_account_category = mapping[key]
            if mapped_account_category is not None:
                return mapped_account_category, account_category, template.get('uuid')
    return None, None, None


def get_ifsc(doc, bank, bbox, path=None, get_only_all_text=False):
    max_page_count = 5

    adv_check_list = ['icici', 'karur', 'ubi', 'idbi', 'baroda']
    n = doc.page_count

    index_list =[*range(min(n, max_page_count))]
    
    if bank in adv_check_list and n > max_page_count:
        index_list+=[*range(n-3,n)]
        index_list = set(index_list)

    for page_number in index_list:
        for template in bbox:
            if template.get("image_flag",False):
                continue
            ifsc = get_temp_ifsc_micr(doc[page_number], template)
            if get_only_all_text:
                return ifsc, None
            if len(ifsc) > 2:
                return ifsc, template.get('uuid')
    return None, None


def get_temp_ifsc_micr(page, template, get_only_all_text=False):
    ifsc = ''
    all_text = get_text_in_box(page, template.get('bbox'))

    if all_text is not None:
        all_text = all_text.replace('\n', ' ').replace('(cid:9)', '')

    regex = template.get('regex')

    all_text = remove_unicode(all_text)
    if get_only_all_text:
        return all_text

    if regex is not None and all_text is not None:
        regex_match = re.match(regex, all_text)

        if regex_match is not None:
            ifsc = regex_match.group(1)

    #print("\n\"", all_text, "\" -->",regex, "-->", ifsc)
    return re.sub(r'\s+', ' ', ifsc)

def get_temp_currency(page, template):
    currency = ''
    all_text = get_text_in_box(page, template.get('bbox'))

    if all_text is not None:
        all_text = all_text.replace('\n', ' ').replace('(cid:9)', '')

    regex = template.get('regex')

    all_text = remove_unicode(all_text)

    if regex is not None and all_text is not None:
        regex_match = re.match(regex, all_text)

        if regex_match is not None:
            currency = regex_match.group(1)

    # print("\n\"", all_text, "\" -->",regex, "-->", currency)
    return re.sub(r'\s+', ' ', currency)


def get_temp_account_category(page, template, get_only_all_text=False):
    account_category = ''
    all_text = get_text_in_box(page, template.get('bbox'))

    if all_text is not None:
        all_text = all_text.replace('\n', ' ').replace('(cid:9)', '')

    regex = template.get('regex')

    all_text = remove_unicode(all_text)

    if get_only_all_text:
        return all_text

    if regex is not None and all_text is not None:
        regex_match = re.match(regex, all_text)

        if regex_match is not None:
            account_category = regex_match.group(1)

    # print(all_text, regex, account_category)
    return re.sub(r'\s+', ' ', account_category).strip()

def get_generic_text(page, template, get_only_all_text=False, template_type=None):
    all_text = get_text_in_box(page, template.get('bbox'))
    if all_text is not None:
        all_text = all_text.replace('\n', ' ').replace('(cid:9)', '')
    # print(all_text)
    regex = template.get('regex')

    all_text = remove_unicode(all_text)

    if template_type == 'card_type_bbox':
        for word in CREDIT_CARD_TYPE_WORDS_TO_REMOVE:
            all_text = all_text.replace(word,'')

    if get_only_all_text:
        return all_text
    
    if regex is not None and all_text is not None:
        regex_match = re.match(regex, all_text)

        if regex_match is not None:
            if template_type in ['payment/credits', 'purchase/debits']:
                groups = regex_match.groups()
                total_value = None
                for ele, val in enumerate(groups):
                    if isinstance(val, str) and amount_to_float(val) is not None:
                        sign = get_amount_sign_cc(val)
                        val = amount_to_float(val)
                        if sign is not None and val is not None:
                            val = str(sign * val) 
                    else: 
                        return None
                    total_value = (total_value + val) if total_value is not None else val
                if total_value is not None:
                    return str(round(total_value, 2)).strip()
            elif template_type in ['rewards_points_credited_bbox','rewards_points_claimed_bbox','rewards_points_expired_bbox','rewards_closing_balance_bbox','rewards_opening_balance_bbox','finance_charges','opening_balance','avl_cash_limit','avl_credit_limit','credit_limit','min_amt_due','total_dues']:
                converted_val = amount_to_float(regex_match.group(1))
                if converted_val is not None:
                    amount_sign = get_amount_sign_cc(regex_match.group(1))
                    amount_sign = 1 if amount_sign is None else amount_sign
                    converted_val = 0.0 if converted_val == 0 else amount_sign * converted_val
                    return str(round(converted_val, 2)).strip()
            else:
                account_category = regex_match.group(1)
                return re.sub(r'\s+', ' ', account_category).strip()
    return None

def get_generic_text_from_bank_pdf(doc, templates, get_only_all_text, template_type):
    max_page_count = 3

    for page_number in range(min(max_page_count, doc.page_count)):
        for template in templates:
            extracted_text = extract_generic_text_bank_pdf(doc[page_number], template, template_type, get_only_all_text)
            if get_only_all_text:
                return extracted_text, None, None
        
            if isinstance(extracted_text, str):
                return extracted_text, template.get('uuid'), page_number
    
    return None, None, None

def extract_generic_text_bank_pdf(page, template, template_type, get_only_all_text):
    all_text = get_text_in_box(page, template.get('bbox'))
    if all_text is not None:
        all_text = all_text.replace('\n', ' ').replace('(cid:9)', '')
    regex = template.get('regex')
    all_text = remove_unicode(all_text)

    if get_only_all_text:
        return all_text

    if regex is not None and all_text is not None:
        regex_match = re.match(regex, all_text)

        if regex_match is not None:
            extracted_text = regex_match.group(1)
            if isinstance(extracted_text, str):
                extracted_text = extracted_text.strip()
                if template_type == "phone_number_bbox" and len(extracted_text) >= INDIAN_PHONE_NUMBER_MINIMUM_PERMISSIBLE_LENGTH:
                    return extracted_text

                if template_type == "pan_number_bbox" and len(extracted_text) == INDIAN_PAN_NUMBER_MINIMUM_PERMISSIBLE_LENGTH:
                    return extracted_text

                if template_type == "email_bbox" and len(extracted_text) >= EMAIL_MINIMUM_PERMISSIBLE_LENGTH:
                    return extracted_text

                # can have a reference to check if it is a valid datetime format in future and return strftime'd format
                if template_type == "dob_bbox" and extracted_text:
                    return extracted_text

    return None

def get_generic_text_from_bbox(doc, bbox, type='', get_only_all_text=False, path=None, is_credit_card=False, template_type=None, bank_name=None):
    max_page_count = 3
    total_pages = doc.page_count
    pages_to_see = [*range(min(total_pages, max_page_count))]
    pages_to_see += [*range(total_pages-3,total_pages)]
    pages_to_see = set(pages_to_see)

    for page_number in pages_to_see:
        for template in bbox:
            retrieved = get_generic_text(doc[page_number], template, get_only_all_text=get_only_all_text, template_type=template_type)
            if get_only_all_text:
                return retrieved, None, page_number
            
            template_uuid = template.get('uuid', '')
            if type == 'date':
                date, _ = check_date(retrieved, is_credit_card = True)
                if date:
                    return date, template_uuid, page_number
            elif retrieved  == 'NIL':
                return None, None, page_number
            elif retrieved:
                val = retrieved.replace(',','')
                if len(val)>0:
                    if template_type in ['card_type_bbox']:
                        val = enhance_credit_card_type_quality(bank_name, val)
                    return val, template_uuid, page_number
                return None, None, page_number
    return None, None, None

def enhance_credit_card_type_quality(bank_name, val):
    capitalized_bank_name = bank_name.upper()
    capitalized_val = val.upper()

    splitted_words = val.split(' ')
    while(len(splitted_words)>0 and splitted_words[0] in CREDIT_CARD_TYPE_WORDS_TO_REMOVE_STARTING):
        splitted_words = splitted_words[1:]
    
    while(len(splitted_words)>0 and splitted_words[-1] in CREDIT_CARD_TYPE_WORDS_TO_REMOVE_END):
        splitted_words = splitted_words[:-1]

    for WORD_TO_REMOVE in CREDIT_CARD_TYPE_WORDS_TO_REMOVE_ANYWHERE:
        capitalized_val = capitalized_val.replace(WORD_TO_REMOVE,"")
    
    if capitalized_bank_name in capitalized_val:
        capitalized_val = capitalized_val.replace(capitalized_bank_name,"")

    return " ".join([word.capitalize() for word in capitalized_val.strip().split(" ")]) + " " + capitalized_bank_name


def transform_date(compiled_regex, date_str):
    """
    Convert a pdf date such as "D:20120321183444+07'00'" into a usable datetime
    (D:YYYYMMDDHHmmSSOHH'mm')
    :param compiled_regex: regex for the pdf date
    :param date_str: pdf date string
    :return: datetime object
    """
    if not date_str:
        return None
    match = re.match(compiled_regex, date_str)
    if match:
        date_info = match.groupdict()
        for k, v in date_info.items():  # transform values
            if v is None:
                pass
            elif k == 'tz_offset':
                date_info[k] = v.lower()  # so we can treat Z as z
            else:
                date_info[k] = int(v)
        if date_info['tz_offset'] in ('z', None):  # UTC
            date_info['tzinfo'] = tzutc()
        else:
            multiplier = 1 if date_info['tz_offset'] == '+' else -1
            date_info['tzinfo'] = tzoffset(
                None, multiplier*(3600 * date_info['tz_hour'] + 60 * date_info['tz_minute']))
        for k in ('tz_offset', 'tz_hour', 'tz_minute'):  # no longer needed
            del date_info[k]
        return datetime.datetime(**date_info)
    return None


# def getTrace(doc):
#     trace=[]
#     for page in doc:
#         trace+=page.get_texttrace()
#     return trace


# def getFontInfo(doc):
#     trace = getTrace(doc)
#     font_set = set()
#     for i in trace:
#         font_set.add(i['font'])
#     return font_set

def get_doc_font_list(doc,serialized=None):
    if serialized:
        '''
        handle case conversion for already present serialized data.
        typecast float to double for library purposes.
        '''
        if not isinstance(doc,dict):
            return -1

        cleaned_font_colors_serialized = []
        cleaned_linewidth_serialized = []
        cleaned_font_size_serialized = []
        for items in doc.get('identity',{}).get('doc_metadata',{}).get('font_colors',[]):
            temp_item = list()
            for vals in items:
                temp_item.append(Decimal(str(vals)).quantize(Decimal('0.000')))
            cleaned_font_colors_serialized.append(temp_item)

        for items in doc.get('identity',{}).get('doc_metadata',{}).get('linewidth',[]):
            cleaned_linewidth_serialized.append(Decimal(str(items)).quantize(Decimal('0.000')))

        for items in doc.get('identity',{}).get('doc_metadata',{}).get('font_size',[]):
            cleaned_font_size_serialized.append(Decimal(str(items)).quantize(Decimal('0.000')))

        doc.get('identity',{}).get('doc_metadata',{})['font_colors']=cleaned_font_colors_serialized
        doc.get('identity',{}).get('doc_metadata',{})['linewidth']=cleaned_linewidth_serialized
        doc.get('identity',{}).get('doc_metadata',{})['font_size']=cleaned_font_size_serialized
        
        return doc
    
    doc_fonts = []
    doc_font_size = []
    doc_colors = []
    doc_linewidth = []
    doc_traces = []

    for page in doc:
        try:
            doc_traces += page.get_texttrace()
        except Exception as e:
            print("No trace available, returned exception")
            pass
    
    temp_fonts_set = set()
    temp_colors_set = set()
    temp_linewidth_set = set()
    temp_font_size_set = set()

    for trace in doc_traces:
        temp_font = trace.get("font", None)
        temp_color = trace.get("color",None)
        temp_linewidth = trace.get("linewidth",None)
        temp_font_size = trace.get("size",None)

        if temp_font is not None and temp_font != "":
            temp_fonts_set.add(temp_font.lower().strip())
        
        if temp_color is not None and temp_color != "":
            temp_colors_set.add(temp_color)
        
        if temp_linewidth is not None and temp_linewidth != "":
            temp_linewidth_set.add(temp_linewidth)
        
        if temp_font_size is not None and temp_font_size != "":
            temp_font_size_set.add(temp_font_size)
        
    cleaned_colors = []
    cleaned_linewidth = []
    cleaned_font_size = []
    
    try:
        doc_fonts = list(temp_fonts_set)
        doc_font_size = list(temp_font_size_set)
        doc_colors = list(temp_colors_set)
        doc_linewidth = list(temp_linewidth_set)
        xref_id = None
        doc_filter = None
        doc_keys = None

        for items in doc_colors:
            temp_item = list()
            for vals in items:
                temp_item.append(Decimal(str(vals)).quantize(Decimal('0.000')))
            cleaned_colors.append(temp_item)

        for items in doc_linewidth:
            cleaned_linewidth.append(Decimal(str(items)).quantize(Decimal('0.000')))

        for items in doc_font_size:
            cleaned_font_size.append(Decimal(str(items)).quantize(Decimal('0.000')))
        
        try:
            doc_keys = doc.xref_get_keys(-1)
            doc_keys = [str(_).upper() for _ in doc_keys]
        except Exception as e:
            doc_keys = None

        # id retrieved from xref_get_key has stringified list
        # for reference --> https://stackoverflow.com/a/66118491/13800305  -> refer doc: 14.4 File Identifiers
        try:
            xref_id = doc.xref_get_key(-1, "ID")[1][2:-2].replace("><",",").split(',') if 'ID' in doc_keys else None
        except Exception as e:
            xref_id = None
        try:
            doc_filter = doc.xref_get_key(-1, "Filter")[1] if 'FILTER' in doc_keys else None
        except Exception as e:
            doc_filter = None
        
    except Exception as e:
        print("exeception occured while converting font set to list: {}".format(e))

    
    return {
        "doc_fonts" : doc_fonts, 
        "cleaned_font_size" : cleaned_font_size, 
        "cleaned_colors" : cleaned_colors, 
        "cleaned_linewidth" : cleaned_linewidth, 
        "xref_id" : xref_id,
        "doc_filter" : doc_filter 
    }


""" def test_metadata():

    doc=read_pdf('/Users/siddhanttiwary/Downloads/perfios_statements/b5b9daa0-3368-4645-950e-7237b657313e_icici.pdf')
    metadata=doc.metadata
    author = metadata.get('author')
    bank='icici'
    m = {'abhinav_sahakari': {'Helvetica'}, 'abhyudaya': {'Helvetica'}, 'airtel': {'Helvetica', 'Helvetica-Bold'}, 'akhand_anand': {'CourierNew', 'Bold'}, 'ausfbnk': {'Helvetica-Bold', 'ArialMT', 'Helvetica', 'Arial-BoldMT'}, 'axis': {'Helvetica', 'Times-Bold', 'Times-Roman'}, 'bandhan': {'Helvetica-Oblique', 'Helvetica', 'Times-Bold', 'Times-Roman'}, 'baroda': {'Helvetica-Bold', 'ArialMT', 'OpenSans', 'OpenSans-Bold', 'Helvetica', 'Arial-ItalicMT', 'Arial-BoldMT'}, 'baroda_gujratbnk': {'OpenSans-Bold', 'OpenSans'}, 'bassein': {'LucidaConsole', 'Times-Roman', 'Times-Bold'}, 'boi': {'Helvetica', 'Helvetica-Bold', 'Times-Roman'}, 'canara': {'Helvetica', 'Times-Roman', 'Helvetica-Bold', 'Times-Bold'}, 'central': {'Helvetica'}, 'citi': {'Helvetica-Bold', 'Times-Roman', 'Times-Italic', 'Helvetica', 'Times-Bold'}, 'city_union': {'Helvetica', 'Courier-Bold', 'Courier'}, 'cosmos': {'Courier'}, 'dbsbnk': {'ArialMT', 'Bold', 'Gotham-Bold', 'AllAndNone', 'Gotham-Book'}, 'dcbbnk': {'Helvetica-Bold', 'Helvetica'}, 'dmk_jaoli': {'Verdana', 'Times New Roman', 'Bold'}, 'federal': {'Helvetica-Bold', 'Times-Roman', 'Gilroy-Bold', 'Gilroy-SemiBold', 'Inter-Medium', 'Inter-Bold', 'Helvetica', 'Times-Bold'}, 'financial': {'Helvetica', 'Helvetica-Bold'}, 'gp_parsik': {'Helvetica-Bold', 'Helvetica'}, 'hdfc': {'Times-Bold', 'Times-Roman'}, 'icici': {'RupeeForadian', 'ArialMT', 'Arial-BoldMT', 'Times-Roman', 'Wingdings-Regular', 'Bold', 'ZurichBT-Roman', 'Webdings', 'ZurichBT-Bold', 'Mulish-Regular', 'Helvetica', 'Times-Bold'}, 'idbi': {'ArialMT', 'Helvetica-Bold', 'Helvetica', 'Arial-BoldMT'}, 'idfc': {'ArialMT', 'Gotham-Medium',
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      'Gotham-Bold', 'Gotham-Book', 'Arial-BoldMT', 'Gotham-Light'}, 'indbnk': {'Helvetica', 'Helvetica-Bold'}, 'indusind': {'Helvetica-Bold', 'Wingdings-Regular', 'Bold', 'Tahoma', 'Times-Roman', 'BatangChe', 'AllAndNone', 'Helvetica-Oblique', 'Helvetica-BoldOblique', 'Calibri', 'Helvetica', 'Calibri-Bold'}, 'iob': {'Helvetica-Bold', 'Courier-Oblique', 'Times-Roman', 'Courier', 'Helvetica'}, 'jantasahakari': {'Helvetica-Bold', 'Helvetica'}, 'karnataka': {'Helvetica-Bold', 'ArialMT', 'Helvetica', 'Arial-BoldMT'}, 'karur': {'Verdana', 'Helvetica', 'Helvetica-Bold', 'VerdanaBold'}, 'kotak': {'Helvetica-Bold', 'Roboto-Regular', 'Roboto-Bold', 'Bold', 'Roboto-Light', 'TimesNewRoman', 'ArialNarrow', 'Roboto-Medium', 'Helvetica', 'Arial'}, 'mahabk': {'Helvetica-Bold', 'Helvetica'}, 'paytm': {'OpenSans-Semibold', 'OpenSans', 'PingFangHK-Regular-Propo', 'OpenSans-Light'}, 'pnbbnk': {'ArialMT', 'Helvetica-Bold', 'Times-Roman', 'Helvetica', 'Times-Bold'}, 'punjab_sind': {'Helvetica', 'Sakalbharati'}, 'rbl': {'Helvetica', 'Helvetica-Bold'}, 'saraswat': {'Helvetica', 'Helvetica-Bold'}, 'sbi': {'Helvetica', 'Helvetica-Bold'}, 'sib': {'Helvetica-Bold', 'Helvetica'}, 'svcbnk': {'Verdana', 'Verdana-Bold', 'Tahoma'}, 'tamil_mercantile': {'Helvetica-Bold', 'Helvetica', 'Helvetica-Oblique'}, 'ubi': {'ArialMT', 'Times-Roman', 'Arial-BoldMT', 'Times-Bold'}, 'uco': {'Helvetica', 'Helvetica-Bold', 'ArialUnicodeMS'}, 'ujjivan': {'Helvetica', 'Helvetica-Bold'}, 'varachha': {'Helvetica', 'Helvetica-Bold'}, 'vilas': {'Times-Roman', 'Times-Bold'}, 'yesbnk': {'Helvetica', 'Times-Roman', 'Arial-Bold', 'Times-Bold', 'Arial'}}
    bad_authors= ['Samsung Electronics', 'iLovePDF', 'MicrosoftÂ® Word 2019', 'Microsoft: Print To PDF', '3-Heights(TM) PDF Security Shell 4.8.25.2 (http://', '2.3.5 (4.2.16) ', 'Skia/PDF m98', 'www.ilovepdf.com', 'iOS Version 15.4.1 (Build 19E258) Quartz PDFContex', 'MicrosoftÂ® Word 2021', 'MicrosoftÂ® Word for Microsoft 365', 'Soda PDF', 'Skia/PDF m94', '2.4.6 (4.3.3) ', 'iOS Version 15.4 (Build 19E241) Quartz PDFContext', 'iOS Version 15.5 (Build 19F77) Quartz PDFContext', '2.4.19 (4.3.6) ', 'PDF Candy']

    if author in bad_authors:
        return True, 'author_fraud'
    
    font=getFontInfo(doc)
    encryption_details = metadata['encryption']

    flag=0
    for i in font:
        if i not in m[bank]:
            flag+=1
            break
    if encryption_details is None or encryption_details=='':
        flag+=1
    if flag==2:
        return True,'font or encryption fraud' """

def strict_fraud_metadata_check_fallback(bank, unicode_flag, pdf_version, metadata_details, uncleaned_metadata_details):
    # metadata details consists of [cleaned_creator, cleaned_producer, cleaned_author]

    cleaned_creator, cleaned_producer, cleaned_author = metadata_details
    creator, producer, author = uncleaned_metadata_details

    hdfc_char_flag = False
    hdfc_special_char_regex = re.compile('.*([^A-Za-z]).*')
    hdfc_new_char_regex = re.compile('(^[A-Za-z]+$)')

    
    if hdfc_special_char_regex.findall(producer) or hdfc_new_char_regex.findall(producer):
        hdfc_char_flag = True 

    if bank=='hdfc':
        if hdfc_char_flag and pdf_version in ['PDF 1.3', 'PDF 1.7']:  # unicode_flag
            return False, None
        elif pdf_version == 'PDF 1.3' and cleaned_producer in [None,'']:
            return False, None
        elif pdf_version == 'PDF 1.7' and cleaned_producer == 'pdfium':
            return False, None
        elif pdf_version == 'PDF 1.4' and cleaned_producer == '[uniserve version 7.0.0] on linux':
            return False, None
        elif pdf_version == 'PDF 1.4' and cleaned_author == '' and cleaned_creator == '' and cleaned_producer == 'itext 5.4.4 2000-2013 1t3xt bvba (agpl-version)':
            return False, None
        return True, 'good_author_fraud'
    elif bank=='sbi':

        if pdf_version == 'PDF 1.4' and cleaned_producer == 'itext 2.0.4 (by lowagie.com)' and cleaned_author == '' and cleaned_creator == '' :
            return False, None
        elif pdf_version == 'PDF 1.4' and cleaned_producer == 'itext 5.5.0 2000-2013 itext group nv (agpl-version)' and cleaned_author == '' and cleaned_creator == 'jasperreports library version 6.0.0' :
            return False, None
        elif pdf_version == 'PDF 1.4' and cleaned_producer == 'itext 2.0.4 (by lowagie.com); modified using itext 5.5.10 2000-2015 itext group nv (agpl-version)' and cleaned_author == '' and cleaned_creator == '' :
            return False, None
        elif pdf_version == 'PDF 1.4' and cleaned_producer == 'itext 5.5.0 2000-2013 itext group nv (agpl-version) (agpl-version)' and cleaned_author == '' and cleaned_creator == 'jasperreports library version 6.0.0' :
            return False, None
        elif pdf_version == 'PDF 1.4' and cleaned_producer == 'itext 5.5.6 2000-2015 itext group nv (agpl-version)' and cleaned_author == '' and cleaned_creator == '' :
            return False, None
        elif pdf_version == 'PDF 1.4' and cleaned_producer == 'itext 5.5.4 2000-2014 itext group nv (agpl-version)' and cleaned_author == '' and cleaned_creator == '' :
            return False, None
        elif pdf_version == 'PDF 1.4' and cleaned_producer == 'itext 5.5.0 2000-2013 itext group nv (agpl-version) (agpl-version) (agpl-version)' and cleaned_author == '' and cleaned_creator == 'jasperreports library version 6.0.0' :
            return False, None
        elif pdf_version == 'PDF 1.4' and cleaned_producer == 'itext 5.5.0 2000-2013 itext group nv (agpl-version) (agpl-version) (agpl-version) (agpl-version)' and cleaned_author == '' and cleaned_creator == 'jasperreports library version 6.0.0' :
            return False, None
        elif pdf_version == 'PDF 1.4' and cleaned_producer == 'itext 5.5.0 2000-2013 itext group nv (agpl-version) (agpl-version) (agpl-version) (agpl-version) (agpl-version) (agpl-version) (agpl-version)' and cleaned_author == '' and cleaned_creator == 'jasperreports library version 6.0.0' :
            return False, None
        elif pdf_version == 'PDF 1.4' and cleaned_producer == 'itextsharp 5.0.0 (c) 1t3xt bvba' and cleaned_author == '' and cleaned_creator == '' :
            return False, None
        elif pdf_version == 'PDF 1.4' and cleaned_producer == 'itext 5.5.0 2000-2013 itext group nv (agpl-version) (agpl-version) (agpl-version) (agpl-version) (agpl-version) (agpl-version) (agpl-version) (agpl-version) (agpl-version) (agpl-version) (agpl-version) (agpl-version) (agpl-version) (agpl-version) (agpl-version) (agpl-version) (agpl-version) (agpl-version)' and cleaned_author == '' and cleaned_creator == 'jasperreports library version 6.0.0' :
            return False, None
        elif pdf_version == 'PDF 1.4' and cleaned_producer == 'itext 5.5.0 2000-2013 itext group nv (agpl-version) (agpl-version) (agpl-version) (agpl-version) (agpl-version) (agpl-version) (agpl-version) (agpl-version)' and cleaned_author == '' and cleaned_creator == 'jasperreports library version 6.0.0' :
            return False, None
        elif pdf_version == 'PDF 1.4' and cleaned_producer == 'itext 5.5.0 2000-2013 itext group nv (agpl-version) (agpl-version) (agpl-version) (agpl-version) (agpl-version) (agpl-version) (agpl-version) (agpl-version) (agpl-version) (agpl-version) (agpl-version) (agpl-version) (agpl-version) (agpl-version) (agpl-version)' and cleaned_author == '' and cleaned_creator == 'jasperreports library version 6.0.0' :
            return False, None
        elif pdf_version == 'PDF 1.4' and cleaned_producer == 'itext 2.0.4 (by lowagie.com); modified using openpdf unknown' and cleaned_author == '' and cleaned_creator == '' :
            return False, None
        elif pdf_version == 'PDF 1.4' and cleaned_producer == 'itext 5.5.0 2000-2013 itext group nv (agpl-version) (agpl-version) (agpl-version) (agpl-version) (agpl-version) (agpl-version) (agpl-version) (agpl-version) (agpl-version) (agpl-version) (agpl-version) (agpl-version)' and cleaned_author == '' and cleaned_creator == 'jasperreports library version 6.0.0' :
            return False, None
        elif pdf_version == 'PDF 1.4' and cleaned_producer == 'itext 5.5.0 2000-2013 itext group nv (agpl-version) (agpl-version) (agpl-version) (agpl-version) (agpl-version)' and cleaned_author == '' and cleaned_creator == 'jasperreports library version 6.0.0' :
            return False, None
        elif pdf_version == 'PDF 1.4' and cleaned_producer == 'itext 2.0.4 (by lowagie.com); modified using itext 5.5.10 2000-2015 itext group nv (agpl-version); modified using openpdf unknown' and cleaned_author == '' and cleaned_creator == '' :
            return False, None
        elif pdf_version == 'PDF 1.4' and cleaned_producer == 'itext 5.5.0 2000-2013 itext group nv (agpl-version) (agpl-version) (agpl-version) (agpl-version) (agpl-version) (agpl-version) (agpl-version) (agpl-version) (agpl-version)' and cleaned_author == '' and cleaned_creator == 'jasperreports library version 6.0.0' :
            return False, None
        elif pdf_version == 'PDF 1.4' and cleaned_producer == 'itext 5.5.0 2000-2013 itext group nv (agpl-version); modified using openpdf unknown' and cleaned_author == '' and cleaned_creator == 'jasperreports library version 6.0.0' :
            return False, None
        elif pdf_version == 'PDF 1.4' and cleaned_producer == 'itext 2.0.8 (by lowagie.com)' and cleaned_author == '' and cleaned_creator == '' :
            return False, None
        elif pdf_version == 'PDF 1.4' and cleaned_producer == 'itext 5.5.13 2000-2018 itext group nv (agpl-version)' and cleaned_author == '' and cleaned_creator == '' :
            return False, None
        elif pdf_version == 'PDF 1.4' and cleaned_producer == 'itext 5.5.0 2000-2013 itext group nv (agpl-version) (agpl-version) (agpl-version) (agpl-version) (agpl-version) (agpl-version)' and cleaned_author == '' and cleaned_creator == 'jasperreports library version 6.0.0' :
            return False, None
        elif pdf_version == 'PDF 1.4' and cleaned_producer == 'itext 5.5.0 2000-2013 itext group nv (agpl-version) (agpl-version) (agpl-version) (agpl-version) (agpl-version) (agpl-version) (agpl-version) (agpl-version) (agpl-version) (agpl-version)' and cleaned_author == '' and cleaned_creator == 'jasperreports library version 6.0.0' :
            return False, None
        elif pdf_version == 'PDF 1.4' and cleaned_producer == 'itext 5.5.0 2000-2013 itext group nv (agpl-version) (agpl-version) (agpl-version) (agpl-version) (agpl-version) (agpl-version) (agpl-version) (agpl-version) (agpl-version) (agpl-version) (agpl-version)' and cleaned_author == '' and cleaned_creator == 'jasperreports library version 6.0.0' :
            return False, None
        elif pdf_version == 'PDF 1.4' and cleaned_producer == 'itext 5.5.0 2000-2013 itext group nv (agpl-version) (agpl-version) (agpl-version); modified using openpdf unknown' and cleaned_author == '' and cleaned_creator == 'jasperreports library version 6.0.0' :
            return False, None
        elif pdf_version == 'PDF 1.4' and cleaned_producer == 'itext 5.5.6 2000-2015 itext group nv (agpl-version); modified using openpdf unknown' and cleaned_author == '' and cleaned_creator == '' :
            return False, None
        elif pdf_version == 'PDF 1.4' and cleaned_producer == 'itext 5.5.0 2000-2013 itext group nv (agpl-version) (agpl-version) (agpl-version) (agpl-version); modified using openpdf unknown' and cleaned_author == '' and cleaned_creator == 'jasperreports library version 6.0.0' :
            return False, None
        elif pdf_version == 'PDF 1.4' and cleaned_producer == 'itext 5.5.6 2000-2015 itext group nv (agpl-version) (agpl-version)' and cleaned_author == '' and cleaned_creator == '' :
            return False, None
        elif pdf_version == 'PDF 1.4' and cleaned_producer == 'itext 5.5.0 2000-2013 itext group nv (agpl-version) (agpl-version); modified using openpdf unknown' and cleaned_author == '' and cleaned_creator == 'jasperreports library version 6.0.0' :
            return False, None
        elif pdf_version == 'PDF 1.4' and cleaned_producer == 'itext 5.5.0 2000-2013 itext group nv (agpl-version); modified using itext 5.5.10 2000-2015 itext group nv (agpl-version)' and cleaned_author == '' and cleaned_creator == 'jasperreports library version 6.0.0' :
            return False, None
        elif pdf_version == 'PDF 1.4' and cleaned_producer == 'itext 5.4.1 2000-2012 1t3xt bvba (agpl-version)' and cleaned_author == '' and cleaned_creator == '' :
            return False, None
        elif pdf_version == 'PDF 1.4' and cleaned_producer == 'itext 5.0.6 (c) 1t3xt bvba' and cleaned_author == '' and cleaned_creator == '' :
            return False, None
        elif pdf_version == 'PDF 1.4' and cleaned_producer == 'itext 5.5.13 2000-2018 itext group nv (agpl-version); modified using itext 5.5.13 2000-2018 itext group nv (agpl-version)' and cleaned_author == '' and cleaned_creator == '' :
            return False, None
        elif pdf_version == 'PDF 1.4' and cleaned_producer == 'itext 5.5.6 2000-2015 itext group nv (agpl-version); modified using itext 5.5.10 2000-2015 itext group nv (agpl-version)' and cleaned_author == '' and cleaned_creator == '' :
            return False, None
        elif pdf_version == 'PDF 1.4' and cleaned_producer == 'itext 5.5.0 2000-2013 itext group nv (agpl-version) (agpl-version) (agpl-version) (agpl-version) (agpl-version) (agpl-version) (agpl-version) (agpl-version) (agpl-version) (agpl-version) (agpl-version) (agpl-version) (agpl-version) (agpl-version) (agpl-version) (agpl-version)' and cleaned_author == '' and cleaned_creator == 'jasperreports library version 6.0.0' :
            return False, None
        elif pdf_version == 'PDF 1.4' and cleaned_producer == 'itext 5.5.10 2000-2015 itext group nv (agpl-version); modified using openpdf unknown' and cleaned_author == '' and cleaned_creator == '' :
            return False, None
        elif pdf_version == 'PDF 1.4' and cleaned_producer == 'itextsharp 5.0.0 (c) 1t3xt bvba; modified using openpdf unknown' and cleaned_author == '' and cleaned_creator == '' :
            return False, None
        elif pdf_version == 'PDF 1.4' and cleaned_producer == 'itext1.3.1 by lowagie.com (based on itext-paulo-154)' and cleaned_author == '' and cleaned_creator == '' :
            return False, None
        elif pdf_version == 'PDF 1.4' and cleaned_producer == 'itext 5.5.0 2000-2013 itext group nv (agpl-version) (agpl-version) (agpl-version) (agpl-version) (agpl-version) (agpl-version) (agpl-version) (agpl-version) (agpl-version) (agpl-version) (agpl-version) (agpl-version) (agpl-version) (agpl-version) (agpl-version); modified using openpdf unknown' and cleaned_author == '' and cleaned_creator == 'jasperreports library version 6.0.0' :
            return False, None
        elif pdf_version == 'PDF 1.4' and cleaned_producer == 'itext 5.5.0 2000-2013 itext group nv (agpl-version) (agpl-version) (agpl-version) (agpl-version) (agpl-version) (agpl-version) (agpl-version) (agpl-version); modified using openpdf unknown' and cleaned_author == '' and cleaned_creator == 'jasperreports library version 6.0.0' :
            return False, None
        elif pdf_version == 'PDF 1.4' and cleaned_producer == 'itext 5.5.13.2 2000-2020 itext group nv (agpl-version)' and cleaned_author == '' and cleaned_creator == '' :
            return False, None
        elif pdf_version == 'PDF 1.7' and cleaned_producer == 'pdfium' and cleaned_author == '' and cleaned_creator == 'pdfium' :
            return False, None
        elif pdf_version == 'PDF 1.5' and cleaned_producer == 'openpdf 1.3.23' and cleaned_author == '' and cleaned_creator == 'jasperreports library version null' :
            return False, None
        return True, 'good_author_fraud'
    
    elif bank=='axis':

        if pdf_version == 'PDF 1.4' and cleaned_producer == 'itext 2.1.7 by 1t3xt' and cleaned_author == '' and cleaned_creator == '' :
            return False, None
        elif pdf_version == 'PDF 1.5' and cleaned_producer == '3-heights(tm) pdf optimization api 4.6.26.2 (http://www.pdf-tools.com)' and cleaned_author == 'cpss' and cleaned_creator == 'style report' :
            return False, None
        elif pdf_version == 'PDF 1.5' and cleaned_producer == '3-heights(tm) pdf optimization api 4.6.26.5 (http://www.pdf-tools.com)' and cleaned_author == 'cpss' and cleaned_creator == 'style report' :
            return False, None
        elif pdf_version == 'PDF 1.5' and cleaned_producer == '3-heights(tm) pdf optimization api 4.6.26.5 (http://www.pdf-tools.com)' and cleaned_author == 'admin' and cleaned_creator == 'style report' :
            return False, None
        elif pdf_version == 'PDF 1.5' and cleaned_producer == '3-heights(tm) pdf optimization api 4.6.26.2 (http://www.pdf-tools.com)' and cleaned_author == 'admin' and cleaned_creator == 'style report' :
            return False, None
        elif pdf_version == 'PDF 1.4' and cleaned_producer == '3-heights(tm) pdf security shell 4.8.25.2 (http://www.pdf-tools.com)' and cleaned_author == '' and cleaned_creator == '' :
            return False, None
        elif pdf_version == 'PDF 1.7' and cleaned_producer == 'pdfium' and cleaned_author == '' and cleaned_creator == 'pdfium' :
            return False, None
        return True, 'good_author_fraud'
    
    elif bank=='kotak':

        if pdf_version == 'PDF 1.3' and cleaned_producer == 'kotak mahindra bank ltd' and cleaned_author == 'kotak mahindra bank ltd' and cleaned_creator == 'kotak mahindra bank ltd' :
            return False, None
        elif pdf_version == 'PDF 1.7' and cleaned_producer == 'kotak mahindra bank ltd' and cleaned_author == 'kotak mahindra bank ltd' and cleaned_creator == 'kotak mahindra bank ltd' :
            return False, None
        elif pdf_version == 'PDF 1.3' and cleaned_producer == 'trejhara solutions limited' and cleaned_author == 'trejhara solutions limited' and cleaned_creator == 'trejhara solutions limited' :
            return False, None
        elif pdf_version == 'PDF 1.4' and cleaned_producer == 'itext1.3.1 by lowagie.com (based on itext-paulo-154)' and cleaned_author == '' and cleaned_creator == 'jasperreports (psphalf1)' :
            return False, None
        elif pdf_version == 'PDF 1.4' and cleaned_producer == 'itext 5.3.4 2000-2012 1t3xt bvba (agpl-version)' and cleaned_author == '' and cleaned_creator == '' :
            return False, None
        elif pdf_version == 'PDF 1.7' and cleaned_producer == 'pdfium' and cleaned_author == '' and cleaned_creator == 'pdfium' :
            return False, None
        elif pdf_version == 'PDF 1.4' and cleaned_producer == 'itext1.3.1 by lowagie.com (based on itext-paulo-154); modified using openpdf unknown' and cleaned_author == '' and cleaned_creator == 'jasperreports (psphalf1)' :
            return False, None
        elif pdf_version == 'PDF 1.4' and cleaned_producer == 'itext1.3.1 by lowagie.com (based on itext-paulo-154); modified using openpdf unknown' and cleaned_author == '' and cleaned_creator == 'jasperreports (repo001_pds)' :
            return False, None
        elif pdf_version == 'PDF 1.4' and cleaned_producer == 'itext1.3.1 by lowagie.com (based on itext-paulo-154)' and cleaned_author == '' and cleaned_creator == 'jasperreports (psphalf1)' :
            return False, None
        elif pdf_version == 'PDF 1.4' and cleaned_producer == 'itext1.3.1 by lowagie.com (based on itext-paulo-154)' and cleaned_author == '' and cleaned_creator == 'jasperreports (repo001_pds)' :
            return False, None
        elif pdf_version == 'PDF 1.4' and cleaned_producer == 'itext1.3.1 by lowagie.com (based on itext-paulo-154)' and cleaned_author == '' and cleaned_creator == 'jasperreports (fullstatementrpt)' :
            return False, None
        elif pdf_version == 'PDF 1.4' and cleaned_producer == 'itext1.3.1 by lowagie.com (based on itext-paulo-154)' and cleaned_author == '' and cleaned_creator == 'jasperreports (repo001_pdc)' :
            return False, None
        elif pdf_version == 'PDF 1.4' and cleaned_producer == 'itext1.3.1 by lowagie.com (based on itext-paulo-154)' and cleaned_author == '' and cleaned_creator == 'jasperreports (repo001_pds)' :
            return False, None
        elif pdf_version == 'PDF 1.3' and cleaned_producer == 'style report; modified using itext 5.3.4 2000-2012 1t3xt bvba (agpl-version)' and cleaned_author == '' and cleaned_creator == 'style report' :
            return False, None
        elif pdf_version == 'PDF 1.4' and cleaned_producer == 'itext 5.5.13 2000-2018 itext group nv (agpl-version)' and cleaned_author == '' and cleaned_creator == '' :
            return False, None
        elif pdf_version == 'PDF 1.7' and cleaned_producer == 'samsung electronics' and cleaned_author == '' and cleaned_creator == 'samsung electronics' :
            return False, None
        return True, 'good_author_fraud'
        
    elif bank=='icici':

        if pdf_version == 'PDF 1.4' and cleaned_producer == 'itext 1.4.8 (by lowagie.com)' and cleaned_author == 'icici bank ltd' and cleaned_creator == '' :
            return False, None
        elif pdf_version == 'PDF 1.5' and cleaned_producer == 'openpdf 1.3.28' and cleaned_author == 'icici bank ltd' and cleaned_creator == '' :
            return False, None
        elif pdf_version == 'PDF 1.4' and cleaned_producer == 'itext 1.4.8 (by lowagie.com); modified using openpdf unknown' and cleaned_author == 'icici bank ltd' and cleaned_creator == '' :
            return False, None
        elif pdf_version == 'PDF 1.4' and cleaned_producer == 'itext 1.4.8 (by lowagie.com); modified using itext 5.5.10 2000-2015 itext group nv (agpl-version)' and cleaned_author == 'icici bank ltd' and cleaned_creator == '' :
            return False, None
        elif pdf_version == 'PDF 1.4' and cleaned_producer == 'itext 2.1.5 (by lowagie.com)' and cleaned_author == '' and cleaned_creator == 'jasperreports (optransactionhistory)' :
            return False, None
        elif pdf_version == 'PDF 1.4' and cleaned_producer == 'itext 2.1.5 (by lowagie.com)' and cleaned_author == '' and cleaned_creator == 'jasperreports (optransactionhistorytpr)' :
            return False, None
        elif pdf_version == 'PDF 1.4' and cleaned_producer == 'itext 2.1.5 (by lowagie.com)' and cleaned_author == '' and cleaned_creator == 'jasperreports (optransactionhistoryux3)' :
            return False, None
        elif pdf_version == 'PDF 1.4' and cleaned_producer == 'itext 2.1.7 by 1t3xt' and cleaned_author == '' and cleaned_creator == '' :
            return False, None
        elif pdf_version == 'PDF 1.4' and cleaned_producer == 'itext 5.1.0 2000-2011 1t3xt bvba' and cleaned_author == '' and cleaned_creator == '' :
            return False, None
        elif pdf_version == 'PDF 1.4' and cleaned_producer == 'itext 2.1.5 (by lowagie.com); modified using openpdf unknown' and cleaned_author == '' and cleaned_creator == '' :
            return False, None
        elif pdf_version == 'PDF 1.7' and cleaned_producer == '' and cleaned_author == '' and cleaned_creator == 'quadient cxm ag~inspire~14.0.196.0' :
            return False, None
        elif pdf_version == 'PDF 1.4' and cleaned_producer == '' and cleaned_author == '' and cleaned_creator == 'quadient cxm ag~inspire~14.0.196.0' :
            return False, None
        elif pdf_version == 'PDF 1.7' and cleaned_producer == 'pdfium' and cleaned_author == '' and cleaned_creator == 'pdfium' :
            return False, None
        return True, 'good_author_fraud'
    
    elif bank=='baroda':

        if pdf_version == 'PDF 1.4' and cleaned_producer == 'openhtmltopdf.com' and cleaned_author == '' and cleaned_creator == 'openhtmltopdf.com' :
            return False, None
        elif pdf_version == 'PDF 1.6' and cleaned_producer == '' and cleaned_author == '' and cleaned_creator == 'quadient cxm ag~inspire~15.0.734.0' :
            return False, None
        elif pdf_version == 'PDF 1.4' and cleaned_producer == '' and cleaned_author == '' and cleaned_creator == 'gmc software ag~inspire~11.0.61.0' :
            return False, None
        elif pdf_version == 'PDF 1.4' and cleaned_producer == '' and cleaned_author == '' and cleaned_creator == '' :
            return False, None
        elif pdf_version == 'PDF 1.4' and cleaned_producer == 'itext 2.1.5 (by lowagie.com)' and cleaned_author == '' and cleaned_creator == 'jasperreports (optransactionhistoryux5)' :
            return False, None
        elif pdf_version == 'PDF 1.6' and cleaned_producer == 'modified using itext 5.5.10 2000-2015 itext group nv (agpl-version)' and cleaned_author == '' and cleaned_creator == 'quadient cxm ag~inspire~15.0.734.0' :
            return False, None
        elif pdf_version == 'PDF 1.7' and cleaned_producer == 'pdfium' and cleaned_author == '' and cleaned_creator == 'pdfium' :
            return False, None
        return True, 'good_author_fraud'
    
    elif bank=='pnbbnk':

        if pdf_version == 'PDF 1.4' and cleaned_producer == 'itext 4.2.0 by 1t3xt' and cleaned_author == '' and cleaned_creator == '' :
            return False, None
        elif pdf_version == 'PDF 1.4' and cleaned_producer == 'itext 2.1.5 (by lowagie.com)' and cleaned_author == '' and cleaned_creator == 'jasperreports (optransactionhistory)' :
            return False, None
        elif pdf_version == 'PDF 1.4' and cleaned_producer == 'itext 2.1.5 (by lowagie.com)' and cleaned_author == '' and cleaned_creator == 'jasperreports (optransactionhistorytpr)' :
            return False, None
        elif pdf_version == 'PDF 1.4' and cleaned_producer == 'itext 2.1.5 (by lowagie.com)' and cleaned_author == '' and cleaned_creator == 'jasperreports (lntransactionhistory)' :
            return False, None
        elif pdf_version == 'PDF 1.4' and cleaned_producer == 'itext1.3.1 by lowagie.com (based on itext-paulo-154)' and cleaned_author == '' and cleaned_creator == 'jasperreports (untitled_report_1)' :
            return False, None
        elif pdf_version == 'PDF 1.7' and cleaned_producer == 'pdfium' and cleaned_author == '' and cleaned_creator == 'pdfium' :
            return False, None
        elif pdf_version == 'PDF 1.4' and cleaned_producer == 'itext 5.0.0 (c) 1t3xt bvba' and cleaned_author == '' and cleaned_creator == '' :
            return False, None
        elif pdf_version == 'PDF 1.4' and cleaned_producer == 'itext 4.2.0 by 1t3xt' and cleaned_author == 'punjab national bank' and cleaned_creator == 'punjab national bank' :
            return False, None
        elif pdf_version == 'PDF 1.4' and cleaned_producer == 'itext 5.2.1 2000-2012 1t3xt bvba' and cleaned_author == 'punjab national bank' and cleaned_creator == 'punjab national bank' :
            return False, None
        return True, 'good_author_fraud'
    
    elif bank=='ubi':

        if pdf_version == 'PDF 1.7' and cleaned_producer == 'openhtmltopdf.com' and cleaned_author == '' and cleaned_creator == '' :
            return False, None
        elif pdf_version == 'PDF 1.7' and cleaned_producer == 'pdfium' and cleaned_author == '' and cleaned_creator == 'pdfium' :
            return False, None
        elif pdf_version == 'PDF 1.5' and cleaned_producer == '' and cleaned_author == '' and cleaned_creator == 'jasperreports library version 6.10.0-dddd38218a3c404d01eecdb9d9a7636fe2d02d7a' :
            return False, None
        elif pdf_version == 'PDF 1.4' and cleaned_producer == 'itext 2.1.5 (by lowagie.com)' and cleaned_author == '' and cleaned_creator == 'jasperreports (optransactionhistorylstntxnux3)' :
            return False, None
        elif pdf_version == 'PDF 1.4' and cleaned_producer == 'itext 5.5.3 2000-2014 itext group nv (agpl-version)' and cleaned_author == '' and cleaned_creator == '' :
            return False, None
        return True, 'good_author_fraud'
    
    elif bank=='boi':

        if pdf_version == 'PDF 1.4' and cleaned_producer == 'itext 5.5.4 2000-2014 itext group nv (agpl-version)' and cleaned_author == 'boi' and cleaned_creator == 'boi' :
            return False, None
        elif pdf_version == 'PDF 1.7' and cleaned_producer == 'itext core 7.2.5 (agpl version), pdfhtml 4.0.5 (agpl version) 2000-2023 itext group nv' and cleaned_author == '' and cleaned_creator == '' :
            return False, None
        elif pdf_version == 'PDF 1.5' and cleaned_producer == '' and cleaned_author == '' and cleaned_creator == '' :
            return False, None
        elif pdf_version == 'PDF 1.7' and cleaned_producer == 'itext core 7.2.5 (agpl version), pdfhtml 4.0.5 (agpl version) 2000-2023 itext group nv; modified using itext 5.5.13.3 2000-2022 itext group nv (agpl-version)' and cleaned_author == '' and cleaned_creator == '' :
            return False, None
        elif pdf_version == 'PDF 1.7' and cleaned_producer == 'pdfium' and cleaned_author == '' and cleaned_creator == 'pdfium' :
            return False, None
        return True, 'good_author_fraud'
    
    elif bank=='canara':

        if pdf_version == 'PDF 1.4' and cleaned_producer == 'itext 5.5.13.2 2000-2020 itext group nv (agpl-version)' and cleaned_author == '' and cleaned_creator == '' :
            return False, None
        elif pdf_version == 'PDF 1.4' and cleaned_producer == 'apache fop version 2.3' and cleaned_author == '' and cleaned_creator == 'apache fop version 2.3' :
            return False, None
        elif pdf_version == 'PDF 1.4' and cleaned_producer == 'openhtmltopdf.com' and cleaned_author == '' and cleaned_creator == 'openhtmltopdf.com' :
            return False, None
        elif pdf_version == 'PDF 1.7' and cleaned_producer == 'pdfium' and cleaned_author == '' and cleaned_creator == 'pdfium' :
            return False, None
        elif pdf_version == 'PDF 1.4' and cleaned_producer == 'oracle xml publisher 5.6.2' and cleaned_author == '' and cleaned_creator == '' :
            return False, None
        elif pdf_version == 'PDF 1.4' and cleaned_producer == 'itextsharp 4.1.2 (based on itext 2.1.2u)' and cleaned_author == '' and cleaned_creator == '' :
            return False, None
        elif pdf_version == 'PDF 1.4' and cleaned_producer == 'itext 5.5.13.2 2000-2020 itext group nv (agpl-version); modified using openpdf unknown' and cleaned_author == '' and cleaned_creator == '' :
            return False, None
        return True, 'good_author_fraud'
    
    elif bank=='indbnk':

        if pdf_version == 'PDF 1.7' and cleaned_producer == 'openhtmltopdf.com' and cleaned_author == '' and cleaned_creator == '' :
            return False, None
        elif pdf_version == 'PDF 1.4' and cleaned_producer == 'itext 1.3 by lowagie.com (based on itext-paulo-153)' and cleaned_author == '' and cleaned_creator == '' :
            return False, None
        elif pdf_version == 'PDF 1.7' and cleaned_producer == 'pdfium' and cleaned_author == '' and cleaned_creator == 'pdfium' :
            return False, None
        elif pdf_version == 'PDF 1.4' and cleaned_producer == 'itext 1.3 by lowagie.com (based on itext-paulo-153); modified using itext 5.5.10 2000-2015 itext group nv (agpl-version)' and cleaned_author == '' and cleaned_creator == '' :
            return False, None
        elif pdf_version == 'PDF 1.4' and cleaned_producer == 'itext 2.1.7 by 1t3xt' and cleaned_author == '' and cleaned_creator == 'jasperreports library version 6.6.0' :
            return False, None
        elif pdf_version == 'PDF 1.4' and cleaned_producer == 'itext 1.3 by lowagie.com (based on itext-paulo-153); modified using openpdf unknown' and cleaned_author == '' and cleaned_creator == '' :
            return False, None
        elif pdf_version == 'PDF 1.4' and cleaned_producer == 'itext 5.5.13 2000-2018 itext group nv (agpl-version)' and cleaned_author == '' and cleaned_creator == '' :
            return False, None
        elif pdf_version == 'PDF 1.4' and cleaned_producer == 'itext 5.5.10 2000-2015 itext group nv (agpl-version)' and cleaned_author == '' and cleaned_creator == '' :
            return False, None
        return True, 'good_author_fraud'
    
    elif bank=='idbi':

        if pdf_version == 'PDF 1.3' and cleaned_producer == 'idbi intech' and cleaned_author == 'idbi bank' and cleaned_creator == 'idbi bank' :
            return False, None
        elif pdf_version == 'PDF 1.7' and cleaned_producer == 'idbi intech' and cleaned_author == 'idbi bank' and cleaned_creator == 'idbi bank' :
            return False, None
        elif pdf_version == 'PDF 1.4' and cleaned_producer == 'itext 2.1.5 (by lowagie.com)' and cleaned_author == '' and cleaned_creator == 'jasperreports (optransactionhistoryux5)' :
            return False, None
        elif pdf_version == 'PDF 1.7' and cleaned_producer == 'pdfium' and cleaned_author == '' and cleaned_creator == 'pdfium' :
            return False, None
        elif pdf_version == 'PDF 1.5' and cleaned_producer == 'pdfjet v5.53 (http://pdfjet.com)' and cleaned_author == '' and cleaned_creator == '' :
            return False, None
        return True, 'good_author_fraud'
    
    elif bank=='iob':
            
        if pdf_version == 'PDF 1.4' and cleaned_producer == 'itext 5.5.4 2000-2014 itext group nv (agpl-version)' and cleaned_author == 'iob' and cleaned_creator == 'iob' :
            return False, None
        elif pdf_version == 'PDF 1.4' and cleaned_producer == 'itext 5.5.11 2000-2017 itext group nv (agpl-version); modified using itext 5.5.11 2000-2017 itext group nv (agpl-version)' and cleaned_author == '' and cleaned_creator == '' :
            return False, None
        return True, 'good_author_fraud'

    elif bank=='indusind':

        if pdf_version == 'PDF 1.7' and cleaned_producer == 'itext 5.5.13.3 2000-2022 itext group nv (agpl-version)' and cleaned_author == 'indusind bank' and cleaned_creator == 'generated by indusind bank' :
            return False, None
        elif pdf_version == 'PDF 1.4' and cleaned_producer == '[uniserve version 8.0.0] on linux' and cleaned_author == '' and cleaned_creator == 'uniserve, using pdf engine.' :
            return False, None
        elif pdf_version == 'PDF 1.4' and cleaned_producer == 'itext1.3.1 by lowagie.com (based on itext-paulo-154)' and cleaned_author == '' and cleaned_creator == 'jasperreports (fullstatementonlinerpt)' :
            return False, None
        elif pdf_version == 'PDF 1.4' and cleaned_producer == 'itext1.3.1 by lowagie.com (based on itext-paulo-154)' and cleaned_author == '' and cleaned_creator == 'jasperreports (ibl0157mn002)' :
            return False, None
        elif pdf_version == 'PDF 1.4' and cleaned_producer == 'itext1.3.1 by lowagie.com (based on itext-paulo-154)' and cleaned_author == '' and cleaned_creator == 'jasperreports (crf001mn003)' :
            return False, None
        elif pdf_version == 'PDF 1.3' and cleaned_producer == 'microsoft reporting services pdf rendering extension 13.0.0.0; modified using itextsharp‚Ñ¢ 5.5.8 2000-2015 itext group nv (agpl-version)' and cleaned_author == '' and cleaned_creator == 'microsoft reporting services 13.0.0.0' :
            return False, None
        elif pdf_version == 'PDF 1.7' and cleaned_producer == 'pdfium' and cleaned_author == '' and cleaned_creator == 'pdfium' :
            return False, None
        elif pdf_version == 'PDF 1.4' and cleaned_producer == 'itext 2.1.7 by 1t3xt' and cleaned_author == '' and cleaned_creator == 'jasperreports library version 6.17.0-6d93193241dd8cc42629e188b94f9e0bc5722efd' :
            return False, None
        elif pdf_version == 'PDF 1.4' and cleaned_producer == 'itext 5.5.11 2000-2017 itext group nv (agpl-version)' and cleaned_author == '' and cleaned_creator == '' :
            return False, None
        elif pdf_version == 'PDF 1.7' and cleaned_producer == 'itext core 7.2.3 (agpl version), pdfhtml 4.0.3 (agpl version) 2000-2022 itext group nv; modified using itext core 7.2.3 (agpl version) 2000-2022 itext group nv' and cleaned_author == '' and cleaned_creator == '' :
            return False, None
        elif pdf_version == 'PDF 1.3' and cleaned_producer == 'microsoft reporting services pdf rendering extension 13.0.0.0; modified using itextsharp™ 5.5.8 2000-2015 itext group nv (agpl-version)' and cleaned_author == '' and cleaned_creator == 'microsoft reporting services 13.0.0.0' :
            return False, None
        elif pdf_version == 'PDF 1.3' and cleaned_producer == 'microsoft reporting services pdf rendering extension 13.0.0.0; modified using itextsharp™ 5.5.8 2000-2015 itext group nv (agpl-version); modified using openpdf unknown' and cleaned_author == '' and cleaned_creator == 'microsoft reporting services 13.0.0.0' :
            return False, None
        elif pdf_version == 'PDF 1.3' and cleaned_producer == 'microsoft reporting services pdf rendering extension 13.0.0.0; modified using itext 5.5.10 2000-2015 itext group nv (agpl-version)' and cleaned_author == '' and cleaned_creator == 'microsoft reporting services 13.0.0.0' :
            return False, None
        return True, 'good_author_fraud'
    
    elif bank=='federal':

        if pdf_version == 'PDF 1.4' and cleaned_producer == 'itext 5.4.0 2000-2012 1t3xt bvba (agpl-version)' and cleaned_author == '' and cleaned_creator == '' :
            return False, None
        elif pdf_version == 'PDF 1.7' and cleaned_producer == 'pdfium' and cleaned_author == '' and cleaned_creator == 'pdfium' :
            return False, None
        elif pdf_version == 'PDF 1.4' and cleaned_producer == 'pyfpdf 1.7.2 http://pyfpdf.googlecode.com/' and cleaned_author == '' and cleaned_creator == '' :
            return False, None
        elif pdf_version == 'PDF 1.7' and cleaned_producer == 'pdfcpu v0.3.7 dev' and cleaned_author == '' and cleaned_creator == 'wkhtmltopdf 0.12.6.1' :
            return False, None
        elif pdf_version == 'PDF 1.4' and cleaned_producer == 'itext 5.4.0 2000-2012 1t3xt bvba (agpl-version) (agpl-version)' and cleaned_author == '' and cleaned_creator == '' :
            return False, None
        elif pdf_version == 'PDF 1.4' and cleaned_producer == 'qt 4.8.7' and cleaned_author == '' and cleaned_creator == 'wkhtmltopdf 0.12.4' :
            return False, None
        elif pdf_version == 'PDF 1.3' and cleaned_producer == 'pypdf2' and cleaned_author == '' and cleaned_creator == '' :
            return False, None
        elif pdf_version == 'PDF 1.4' and cleaned_producer == 'itext 5.4.0 2000-2012 1t3xt bvba (agpl-version) (agpl-version) (agpl-version)' and cleaned_author == '' and cleaned_creator == '' :
            return False, None
        elif pdf_version == 'PDF 1.4' and cleaned_producer == 'itext 5.4.0 2000-2012 1t3xt bvba (agpl-version); modified using openpdf unknown' and cleaned_author == '' and cleaned_creator == '' :
            return False, None
        elif pdf_version == 'PDF 1.4' and cleaned_producer == 'itext 5.4.0 2000-2012 1t3xt bvba (agpl-version) (agpl-version) (agpl-version) (agpl-version)' and cleaned_author == '' and cleaned_creator == '' :
            return False, None
        elif pdf_version == 'PDF 1.4' and cleaned_producer == 'itext1.3.1 by lowagie.com (based on itext-paulo-154); modified using openpdf unknown' and cleaned_author == '' and cleaned_creator == '' :
            return False, None
        elif pdf_version == 'PDF 1.4' and cleaned_producer == 'itext 5.4.0 2000-2012 1t3xt bvba (agpl-version); modified using itext 5.5.10 2000-2015 itext group nv (agpl-version)' and cleaned_author == '' and cleaned_creator == '' :
            return False, None
        elif pdf_version == 'PDF 1.4' and cleaned_producer == 'itext1.3.1 by lowagie.com (based on itext-paulo-154)' and cleaned_author == '' and cleaned_creator == 'jasperreports (acct_stmt_dets)' :
            return False, None
        elif pdf_version == 'PDF 1.4' and cleaned_producer == 'itext 2.1.5 (by lowagie.com)' and cleaned_author == '' and cleaned_creator == 'jasperreports (optransactionhistory)' :
            return False, None
        elif pdf_version == 'PDF 1.5' and cleaned_producer == '' and cleaned_author == '' and cleaned_creator == '' :
            return False, None
        return True, 'good_author_fraud'
    
    elif bank=='central':

        if pdf_version == 'PDF 1.5' and cleaned_producer == 'itext 2.1.7 by 1t3xt' and cleaned_author == '' and cleaned_creator == 'jasperreports library version 6.20.0-2bc7ab61c56f459e8176eb05c7705e145cd400ad' :
            return False, None
        elif pdf_version == 'PDF 1.4' and cleaned_producer == 'itext 1.3 by lowagie.com (based on itext-paulo-153)' and cleaned_author == '' and cleaned_creator == '' :
            return False, None
        elif pdf_version == 'PDF 1.4' and cleaned_producer == 'openhtmltopdf.com' and cleaned_author == '' and cleaned_creator == 'openhtmltopdf.com' :
            return False, None
        elif pdf_version == 'PDF 1.7' and cleaned_producer == 'pdfium' and cleaned_author == '' and cleaned_creator == 'pdfium' :
            return False, None
        elif pdf_version == 'PDF 1.4' and cleaned_producer == 'altasigna' and cleaned_author == '' and cleaned_creator == 'odyssey altasigna (c) odyssey technologies ltd, chennai, india 2001-2006' :
            return False, None
        elif pdf_version == 'PDF 1.4' and cleaned_producer == 'itext 5.5.3 2000-2014 itext group nv (agpl-version)' and cleaned_author == '' and cleaned_creator == '' :
            return False, None
        return True, 'good_author_fraud'
    
    elif bank=='karnataka':

        if pdf_version == 'PDF 1.4' and cleaned_producer == 'itext1.3.1 by lowagie.com (based on itext-paulo-154)' and cleaned_author == '' and cleaned_creator == 'jasperreports (racstatement_clab)' :
            return False, None
        elif pdf_version == 'PDF 1.4' and cleaned_producer == 'openhtmltopdf.com' and cleaned_author == '' and cleaned_creator == 'openhtmltopdf.com' :
            return False, None
        elif pdf_version == 'PDF 1.4' and cleaned_producer == 'itext 2.1.5 (by lowagie.com)' and cleaned_author == '' and cleaned_creator == 'jasperreports (optransactionhistoryux3)' :
            return False, None
        elif pdf_version == 'PDF 1.7' and cleaned_producer == 'pdfium' and cleaned_author == '' and cleaned_creator == 'pdfium' :
            return False, None
        return True, 'good_author_fraud'
    
    elif bank=='idfc':

        if pdf_version == 'PDF 1.4' and cleaned_producer == '' and cleaned_author == '' and cleaned_creator == '' :
            return False, None
        elif pdf_version == 'PDF 1.3' and cleaned_producer == 'style report' and cleaned_author == 'aurionpro' and cleaned_creator == 'aurionpro solutions ltd.' :
            return False, None
        elif pdf_version == 'PDF 1.7' and cleaned_producer == 'pdfium' and cleaned_author == '' and cleaned_creator == 'pdfium' :
            return False, None
        return True, 'good_author_fraud'
    
    elif bank=='karur':
                
        if pdf_version == 'PDF 1.4' and cleaned_producer == 'altasigna' and cleaned_author == '' and cleaned_creator == 'odyssey altasigna (c) odyssey technologies ltd, chennai, india 2001-2006' :
            return False, None
        elif pdf_version == 'PDF 1.4' and cleaned_producer == 'oracle xml publisher 5.6.2' and cleaned_author == '' and cleaned_creator == '' :
            return False, None
        elif pdf_version == 'PDF 1.7' and cleaned_producer == 'pdfium' and cleaned_author == '' and cleaned_creator == 'pdfium' :
            return False, None
        return True, 'good_author_fraud'

    elif bank=='yesbnk':

        if pdf_version == 'PDF 1.4' and cleaned_producer == 'apache fop version 2.3' and cleaned_author == '' and cleaned_creator == 'apache fop version 2.3' :
            return False, None
        elif pdf_version == 'PDF 1.7' and cleaned_producer == 'itext 7.1.16 2000-2021 itext group nv (agpl-version); modified using itext 7.1.16 2000-2021 itext group nv (agpl-version)' and cleaned_author == '' and cleaned_creator == '' :
            return False, None
        elif pdf_version == 'PDF 1.4' and cleaned_producer == '[uniserve version 7.0.0] on linux' and cleaned_author == '' and cleaned_creator == '[uniserve version 7.0.0] on linux' :
            return False, None
        elif pdf_version == 'PDF 1.4' and cleaned_producer == 'apache fop version 1.0' and cleaned_author == '' and cleaned_creator == 'apache fop version 1.0' :
            return False, None
        elif pdf_version == 'PDF 1.7' and cleaned_producer == 'pdfium' and cleaned_author == '' and cleaned_creator == 'pdfium' :
            return False, None
        elif pdf_version == 'PDF 1.6' and cleaned_producer == 'oracle bi publisher 12.2.1.4.0' and cleaned_author == '' and cleaned_creator == '' :
            return False, None
        elif pdf_version == 'PDF 1.7' and cleaned_producer == 'oracle bi publisher 11.1.1.7.150120' and cleaned_author == '' and cleaned_creator == '' :
            return False, None
        return True, 'good_author_fraud'
    
    elif bank=='mahabk':
                
        if pdf_version == 'PDF 1.4' and cleaned_producer == 'itext 5.5.4 2000-2014 itext group nv (agpl-version)' and cleaned_author == '' and cleaned_creator == '' :
            return False, None
        elif pdf_version == 'PDF 1.4' and cleaned_producer == 'itext 5.5.4 2000-2014 itext group nv (agpl-version) (agpl-version) (agpl-version)' and cleaned_author == '' and cleaned_creator == '' :
            return False, None
        elif pdf_version == 'PDF 1.4' and cleaned_producer == 'itext 5.5.4 2000-2014 itext group nv (agpl-version) (agpl-version)' and cleaned_author == '' and cleaned_creator == '' :
            return False, None
        elif pdf_version == 'PDF 1.4' and cleaned_producer == 'itext 5.5.4 2000-2014 itext group nv (agpl-version) (agpl-version) (agpl-version) (agpl-version) (agpl-version) (agpl-version) (agpl-version)' and cleaned_author == '' and cleaned_creator == '' :
            return False, None
        elif pdf_version == 'PDF 1.4' and cleaned_producer == 'itext 5.5.4 2000-2014 itext group nv (agpl-version) (agpl-version) (agpl-version) (agpl-version) (agpl-version)' and cleaned_author == '' and cleaned_creator == '' :
            return False, None
        elif pdf_version == 'PDF 1.4' and cleaned_producer == 'itext 5.5.4 2000-2014 itext group nv (agpl-version) (agpl-version) (agpl-version) (agpl-version) (agpl-version) (agpl-version) (agpl-version) (agpl-version) (agpl-version) (agpl-version) (agpl-version) (agpl-version) (agpl-version)' and cleaned_author == '' and cleaned_creator == '' :
            return False, None
        elif pdf_version == 'PDF 1.7' and cleaned_producer == 'pdfium' and cleaned_author == '' and cleaned_creator == 'pdfium' :
            return False, None
        elif pdf_version == 'PDF 1.4' and cleaned_producer == 'itext 5.5.4 2000-2014 itext group nv (agpl-version) (agpl-version) (agpl-version) (agpl-version)' and cleaned_author == '' and cleaned_creator == '' :
            return False, None
        elif pdf_version == 'PDF 1.4' and cleaned_producer == 'itext 5.5.4 2000-2014 itext group nv (agpl-version) (agpl-version) (agpl-version) (agpl-version) (agpl-version) (agpl-version) (agpl-version) (agpl-version)' and cleaned_author == '' and cleaned_creator == '' :
            return False, None
        elif pdf_version == 'PDF 1.4' and cleaned_producer == 'itext 5.5.4 2000-2014 itext group nv (agpl-version) (agpl-version) (agpl-version) (agpl-version) (agpl-version) (agpl-version) (agpl-version) (agpl-version) (agpl-version) (agpl-version) (agpl-version)' and cleaned_author == '' and cleaned_creator == '' :
            return False, None
        elif pdf_version == 'PDF 1.4' and cleaned_producer == 'itext 5.5.4 2000-2014 itext group nv (agpl-version) (agpl-version) (agpl-version) (agpl-version) (agpl-version) (agpl-version) (agpl-version) (agpl-version) (agpl-version) (agpl-version) (agpl-version) (agpl-version) (agpl-version) (agpl-version) (agpl-version)' and cleaned_author == '' and cleaned_creator == '' :
            return False, None
        elif pdf_version == 'PDF 1.4' and cleaned_producer == 'itext 5.5.4 2000-2014 itext group nv (agpl-version); modified using openpdf unknown' and cleaned_author == '' and cleaned_creator == '' :
            return False, None
        elif pdf_version == 'PDF 1.4' and cleaned_producer == 'itext 5.5.4 2000-2014 itext group nv (agpl-version) (agpl-version) (agpl-version) (agpl-version) (agpl-version) (agpl-version)' and cleaned_author == '' and cleaned_creator == '' :
            return False, None
        elif pdf_version == 'PDF 1.4' and cleaned_producer == 'itext 5.5.4 2000-2014 itext group nv (agpl-version) (agpl-version) (agpl-version) (agpl-version) (agpl-version) (agpl-version) (agpl-version) (agpl-version) (agpl-version) (agpl-version) (agpl-version) (agpl-version) (agpl-version) (agpl-version) (agpl-version) (agpl-version) (agpl-version) (agpl-version)' and cleaned_author == '' and cleaned_creator == '' :
            return False, None
        elif pdf_version == 'PDF 1.4' and cleaned_producer == 'itext 5.5.4 2000-2014 itext group nv (agpl-version) (agpl-version) (agpl-version) (agpl-version) (agpl-version) (agpl-version) (agpl-version) (agpl-version) (agpl-version) (agpl-version)' and cleaned_author == '' and cleaned_creator == '' :
            return False, None
        elif pdf_version == 'PDF 1.4' and cleaned_producer == 'itext 5.5.4 2000-2014 itext group nv (agpl-version); modified using itext 5.5.10 2000-2015 itext group nv (agpl-version)' and cleaned_author == '' and cleaned_creator == '' :
            return False, None
        elif pdf_version == 'PDF 1.4' and cleaned_producer == 'itext 5.5.4 2000-2014 itext group nv (agpl-version) (agpl-version) (agpl-version) (agpl-version) (agpl-version) (agpl-version) (agpl-version) (agpl-version) (agpl-version)' and cleaned_author == '' and cleaned_creator == '' :
            return False, None
        elif pdf_version == 'PDF 1.4' and cleaned_producer == 'itext 5.5.4 2000-2014 itext group nv (agpl-version) (agpl-version) (agpl-version) (agpl-version) (agpl-version) (agpl-version) (agpl-version) (agpl-version) (agpl-version) (agpl-version) (agpl-version) (agpl-version) (agpl-version) (agpl-version) (agpl-version) (agpl-version) (agpl-version) (agpl-version) (agpl-version) (agpl-version) (agpl-version) (agpl-version) (agpl-version) (agpl-version) (agpl-version) (agpl-version) (agpl-version) (agpl-version) (agpl-version) (agpl-version) (agpl-version)' and cleaned_author == '' and cleaned_creator == '' :
            return False, None
        elif pdf_version == 'PDF 1.4' and cleaned_producer == 'itext 5.5.4 2000-2014 itext group nv (agpl-version) (agpl-version) (agpl-version) (agpl-version) (agpl-version) (agpl-version) (agpl-version) (agpl-version) (agpl-version) (agpl-version); modified using openpdf unknown' and cleaned_author == '' and cleaned_creator == '' :
            return False, None
        elif pdf_version == 'PDF 1.4' and cleaned_producer == 'itext 5.5.4 2000-2014 itext group nv (agpl-version) (agpl-version) (agpl-version); modified using openpdf unknown' and cleaned_author == '' and cleaned_creator == '' :
            return False, None
        elif pdf_version == 'PDF 1.4' and cleaned_producer == 'itext 5.5.4 2000-2014 itext group nv (agpl-version) (agpl-version) (agpl-version) (agpl-version) (agpl-version) (agpl-version) (agpl-version) (agpl-version) (agpl-version) (agpl-version) (agpl-version) (agpl-version) (agpl-version) (agpl-version)' and cleaned_author == '' and cleaned_creator == '' :
            return False, None
        elif pdf_version == 'PDF 1.4' and cleaned_producer == 'itext 5.5.13 2000-2018 itext group nv (agpl-version)' and cleaned_author == '' and cleaned_creator == '' :
            return False, None
        elif pdf_version == 'PDF 1.4' and cleaned_producer == 'itext 5.5.4 2000-2014 itext group nv (agpl-version) (agpl-version) (agpl-version); modified using itext 5.5.10 2000-2015 itext group nv (agpl-version)' and cleaned_author == '' and cleaned_creator == '' :
            return False, None
        elif pdf_version == 'PDF 1.4' and cleaned_producer == 'itext 5.5.4 2000-2014 itext group nv (agpl-version) (agpl-version) (agpl-version) (agpl-version) (agpl-version) (agpl-version) (agpl-version) (agpl-version) (agpl-version) (agpl-version) (agpl-version) (agpl-version) (agpl-version); modified using openpdf unknown' and cleaned_author == '' and cleaned_creator == '' :
            return False, None
        elif pdf_version == 'PDF 1.4' and cleaned_producer == 'itext 5.5.4 2000-2014 itext group nv (agpl-version) (agpl-version) (agpl-version) (agpl-version) (agpl-version) (agpl-version) (agpl-version); modified using openpdf unknown' and cleaned_author == '' and cleaned_creator == '' :
            return False, None
        elif pdf_version == 'PDF 1.4' and cleaned_producer == 'itext 5.5.4 2000-2014 itext group nv (agpl-version) (agpl-version) (agpl-version) (agpl-version) (agpl-version) (agpl-version) (agpl-version) (agpl-version) (agpl-version) (agpl-version) (agpl-version) (agpl-version)' and cleaned_author == '' and cleaned_creator == '' :
            return False, None
        elif pdf_version == 'PDF 1.4' and cleaned_producer == 'itext 5.5.4 2000-2014 itext group nv (agpl-version) (agpl-version) (agpl-version) (agpl-version) (agpl-version) (agpl-version) (agpl-version) (agpl-version) (agpl-version) (agpl-version) (agpl-version); modified using openpdf unknown' and cleaned_author == '' and cleaned_creator == '' :
            return False, None
        elif pdf_version == 'PDF 1.4' and cleaned_producer == 'itext 5.5.4 2000-2014 itext group nv (agpl-version) (agpl-version) (agpl-version) (agpl-version) (agpl-version) (agpl-version) (agpl-version) (agpl-version) (agpl-version) (agpl-version) (agpl-version) (agpl-version) (agpl-version) (agpl-version) (agpl-version) (agpl-version) (agpl-version) (agpl-version); modified using openpdf unknown' and cleaned_author == '' and cleaned_creator == '' :
            return False, None
        elif pdf_version == 'PDF 1.4' and cleaned_producer == 'itext 5.5.4 2000-2014 itext group nv (agpl-version) (agpl-version) (agpl-version) (agpl-version) (agpl-version); modified using itext 5.5.10 2000-2015 itext group nv (agpl-version)' and cleaned_author == '' and cleaned_creator == '' :
            return False, None
        elif pdf_version == 'PDF 1.4' and cleaned_producer == 'itext 5.5.10 2000-2015 itext group nv (agpl-version)' and cleaned_author == '' and cleaned_creator == '' :
            return False, None
        elif pdf_version == 'PDF 1.4' and cleaned_producer == 'itext 5.5.4 2000-2014 itext group nv (agpl-version) (agpl-version) (agpl-version) (agpl-version); modified using openpdf unknown' and cleaned_author == '' and cleaned_creator == '' :
            return False, None
        elif pdf_version == 'PDF 1.4' and cleaned_producer == 'itext 5.5.4 2000-2014 itext group nv (agpl-version) (agpl-version); modified using openpdf unknown' and cleaned_author == '' and cleaned_creator == '' :
            return False, None
        elif pdf_version == 'PDF 1.4' and cleaned_producer == 'itext 5.5.4 2000-2014 itext group nv (agpl-version); modified using itext 5.5.9 2000-2015 itext group nv (agpl-version)' and cleaned_author == '' and cleaned_creator == '' :
            return False, None
        elif pdf_version == 'PDF 1.4' and cleaned_producer == 'qt 5.5.1' and cleaned_author == '' and cleaned_creator == '' :
            return False, None
        return True, 'good_author_fraud'
    
    elif bank=='ausfbnk':

        if pdf_version == 'PDF 1.4' and cleaned_producer == 'itext 2.1.7 by 1t3xt' and cleaned_author == '' and cleaned_creator == 'jasperreports library version 6.10.0-dddd38218a3c404d01eecdb9d9a7636fe2d02d7a' :
            return False, None
        elif pdf_version == 'PDF 1.5' and cleaned_producer == 'openpdf 1.3.30' and cleaned_author == '' and cleaned_creator == 'jasperreports library version 6.20.5-3efcf2e67f959db3888d79f73dde2dbd7acb4f8e' :
            return False, None
        elif pdf_version == 'PDF 1.4' and cleaned_producer == 'itext 2.1.7 by 1t3xt; modified using openpdf unknown' and cleaned_author == '' and cleaned_creator == 'jasperreports library version 6.10.0-dddd38218a3c404d01eecdb9d9a7636fe2d02d7a' :
            return False, None
        elif pdf_version == 'PDF 1.7' and cleaned_producer == 'pdfium' and cleaned_author == '' and cleaned_creator == 'pdfium' :
            return False, None
        elif pdf_version == 'PDF 1.6' and cleaned_producer == 'rdyna 5.4.0.0' and cleaned_author == '' and cleaned_creator == '' :
            return False, None
        return True, 'good_author_fraud'
    
    elif bank=='equitas':
                
        if pdf_version == 'PDF 1.4' and cleaned_producer == 'qt 5.5.1' and cleaned_author == '' and cleaned_creator == '' :
            return False, None
        elif pdf_version == 'PDF 1.5' and cleaned_producer == 'itext 2.1.7 by 1t3xt' and cleaned_author == '' and cleaned_creator == 'birt report engine /dm_home/oracle/products/oracle_home/user_projects/domains/vmw_digital_domain/servers/dig_msr3_consumer/tmp/_wl_user/appzilloncorpserver/s154n9/war/web-inf/lib/org.eclipse.birt.runtime-4.4.1.jar.' :
            return False, None
        elif pdf_version == 'PDF 1.5' and cleaned_producer == 'itext 2.1.7 by 1t3xt' and cleaned_author == '' and cleaned_creator == 'birt report engine /dm_home/oracle/products/oracle_home/user_projects/domains/vmw_digital_domain/servers/new_server1/tmp/_wl_user/newconsumerappzillonserver1/uyxub9/war/web-inf/lib/org.eclipse.birt.runtime-4.4.1.jar.' :
            return False, None
        elif pdf_version == 'PDF 1.5' and cleaned_producer == 'itext 2.1.7 by 1t3xt' and cleaned_author == '' and cleaned_creator == 'birt report engine /dm_home/oracle/products/oracle_home/user_projects/domains/vmw_digital_domain/servers/new_server1/tmp/_wl_user/newconsumerappzillonserver2/udxvzr/war/web-inf/lib/org.eclipse.birt.runtime-4.4.1.jar.' :
            return False, None
        elif pdf_version == 'PDF 1.5' and cleaned_producer == 'itext 2.1.7 by 1t3xt' and cleaned_author == '' and cleaned_creator == 'birt report engine /dm_home/oracle/products/oracle_home/user_projects/domains/vmw_digital_domain/servers/new_server2/tmp/_wl_user/newconsumerappzillonserver1/bobho6/war/web-inf/lib/org.eclipse.birt.runtime-4.4.1.jar.' :
            return False, None
        elif pdf_version == 'PDF 1.5' and cleaned_producer == 'itext 2.1.7 by 1t3xt' and cleaned_author == '' and cleaned_creator == 'birt report engine /dm_home/oracle/products/oracle_home/user_projects/domains/vmw_digital_domain/servers/new_server2/tmp/_wl_user/newconsumerappzillonserver2/b3bjco/war/web-inf/lib/org.eclipse.birt.runtime-4.4.1.jar.' :
            return False, None
        elif pdf_version == 'PDF 1.5' and cleaned_producer == 'itext 2.1.7 by 1t3xt' and cleaned_author == '' and cleaned_creator == 'birt report engine /dm_home/oracle/products/oracle_home/user_projects/domains/vmw_digital_domain/servers/new_server3/tmp/_wl_user/newconsumerappzillonserver1/rw960n/war/web-inf/lib/org.eclipse.birt.runtime-4.4.1.jar.' :
            return False, None
        elif pdf_version == 'PDF 1.5' and cleaned_producer == 'itext 2.1.7 by 1t3xt' and cleaned_author == '' and cleaned_creator == 'birt report engine /dm_home/oracle/products/oracle_home/user_projects/domains/vmw_digital_domain/servers/new_server3/tmp/_wl_user/newconsumerappzillonserver2/rb97p5/war/web-inf/lib/org.eclipse.birt.runtime-4.4.1.jar.' :
            return False, None
        elif pdf_version == 'PDF 1.5' and cleaned_producer == 'itext 2.1.7 by 1t3xt' and cleaned_author == '' and cleaned_creator == 'birt report engine /dm_home/oracle/products/oracle_home/user_projects/domains/vmw_digital_domain/servers/new_server4/tmp/_wl_user/newconsumerappzillonserver1/8lmtdk/war/web-inf/lib/org.eclipse.birt.runtime-4.4.1.jar.' :
            return False, None
        elif pdf_version == 'PDF 1.5' and cleaned_producer == 'itext 2.1.7 by 1t3xt' and cleaned_author == '' and cleaned_creator == 'birt report engine /dm_home/oracle/products/oracle_home/user_projects/domains/vmw_digital_domain/servers/new_server4/tmp/_wl_user/newconsumerappzillonserver2/80mv22/war/web-inf/lib/org.eclipse.birt.runtime-4.4.1.jar.' :
            return False, None
        elif pdf_version == 'PDF 1.7' and cleaned_producer == 'pdfium' and cleaned_author == '' and cleaned_creator == 'pdfium' :
            return False, None
        elif pdf_version == 'PDF 1.4' and cleaned_producer == 'itext 5.5.13.1 2000-2019 itext group nv (newgen software technologies; licensed version)' and cleaned_author == 'newgen software technologies ltd.' and cleaned_creator == 'omnioms' :
            return False, None
        elif pdf_version == 'PDF 1.6' and cleaned_producer == 'oracle bi publisher 11.1.1.7.150120' and cleaned_author == '' and cleaned_creator == '' :
            return False, None
        return True, 'good_author_fraud'
    
    elif bank=='dbsbnk':
                
        if pdf_version == 'PDF 1.4' and cleaned_producer == '' and cleaned_author == '' and cleaned_creator == 'quadient cxm ag~inspire~15.0.681.5' :
            return False, None
        elif pdf_version == 'PDF 1.4' and cleaned_producer == 'itext 5.5.13 2000-2018 itext group nv (agpl-version)' and cleaned_author == '' and cleaned_creator == '' :
            return False, None
        elif pdf_version == 'PDF 1.7' and cleaned_producer == 'pdfium' and cleaned_author == '' and cleaned_creator == 'pdfium' :
            return False, None
        return True, 'good_author_fraud'
    
    elif bank=='uco':
                
        if pdf_version == 'PDF 1.4' and cleaned_producer == 'openhtmltopdf.com' and cleaned_author == '' and cleaned_creator == 'openhtmltopdf.com' :
            return False, None
        elif pdf_version == 'PDF 1.4' and cleaned_producer == 'openhtmltopdf.com' and cleaned_author == '' and cleaned_creator == '' :
            return False, None
        elif pdf_version == 'PDF 1.4' and cleaned_producer == 'itext 2.1.7 by 1t3xt' and cleaned_author == '' and cleaned_creator == 'jasperreports library version 6.4.1' :
            return False, None
        elif pdf_version == 'PDF 1.7' and cleaned_producer == 'pdfium' and cleaned_author == '' and cleaned_creator == 'pdfium' :
            return False, None
        elif pdf_version == 'PDF 1.4' and cleaned_producer == 'itextâ 5.5.9 â 2000-2015 itext group nv (agpl-version)' and cleaned_author == '' and cleaned_creator == '' :
            return False, None
        return True, 'good_author_fraud'
    return False, None

def strict_fraud_metadata_check(bank, unicode_flag, pdf_version, metadata_details, uncleaned_metadata_details, strict_metadata_fraud_list=[]):
    if bank in ['hdfc'] or len(strict_metadata_fraud_list)==0:
        print("Either Bank is HDFC or the strict metadata fraud list was empty")
        return strict_fraud_metadata_check_fallback(bank, unicode_flag, pdf_version, metadata_details, uncleaned_metadata_details)
    
    cleaned_creator, cleaned_producer, cleaned_author = metadata_details
    
    for valid_combination in strict_metadata_fraud_list:
        if pdf_version in ["PDF 1.7", valid_combination.get('pdf_version', None)] and cleaned_producer==valid_combination.get('cleaned_producer', None) and cleaned_author==valid_combination.get('cleaned_author', None) and cleaned_creator==valid_combination.get('cleaned_creator', None):
            return False, None

    return True, 'good_author_fraud'

def update_dump(dump):
    length=len(dump)
    i=0
    final_dump=[]
    while i<length:
        while i<length and dump[i]!='BT':
            i+=1
        while i<length:
            final_dump.append(dump[i])
            if 'ET'==dump[i]:
                break
            i+=1
    return final_dump  

def get_cnt_of_Bt_ET_Groups(dump):
    cnt=0
    for i in range(len(dump)):
        if dump[i]=='BT':
            cnt+=1
    return cnt
    
def get_first_occur_cnt(dump,word):
    
    first=99999999999
    cnt=0
    for i in range(len(dump)):
        if word in dump[i] and len(word)+3 >= len(dump[i]):
            first=min(first,i)
            cnt+=1
    if first==99999999999:
        first=-1
    return {
        'first_occur':first,
        'cnt':cnt
    }

def check_hex_using_regex(test_string,pattern='<([0-9a-zA-Z]*)>'):
    result = re.match(pattern, test_string)
    if result:
        return True
    else:
        return False

def get_xy_corrdinates(dump_with_BTET_Group):
    x=list()
    y=list()
    cnt_positive_y=0
    cnt_negative_y=0
    td_present = False
    for tmp in dump_with_BTET_Group:
        if check_hex_using_regex(tmp,'.*TM$')==True or check_hex_using_regex(tmp,'.*Tm$') == True:
            tmp_split=tmp.split(' ')
            x.append(float(tmp_split[4]))
            y.append(float(tmp_split[5]))
            
            if float(tmp_split[5]) < 0:
                cnt_negative_y+=1
            else:
                cnt_positive_y+=1
        elif check_hex_using_regex(tmp,'.*TD$')==True or check_hex_using_regex(tmp,'.*Td$') == True:
            td_present=True
            tmp_split=tmp.split(' ')
            x.append(float(tmp_split[0]))
            y.append(float(tmp_split[1]))

            if float(tmp_split[1]) < 0:
                cnt_negative_y+=1
            else:
                cnt_positive_y+=1
    y_coor_max_diff=0
    for i in range(0,len(y)-1):
        y_coor_max_diff=max(y[i+1] - y[i],y_coor_max_diff)
    return y_coor_max_diff,td_present,cnt_positive_y,cnt_negative_y

def get_q_in_start_and_end_fraud(dump_split):
    i=0
    length = len(dump_split)
    q_front=0
    while i< length and 'q' in dump_split[i].lower():
        q_front+=1
        i+=1
    i=length-1
    q_back=0
    while i>=0 and 'q' in dump_split[i].lower():
        q_back+=1
        i-=1
    return q_front,q_back

def decode_hex_into_string(test_string):
    pattern = '<([0-9a-zA-Z]*)>'
    result = re.match(pattern, test_string)
    if result:
        try:
            word=bytes.fromhex(result.group(1)).decode('utf-8')
            return word
        except Exception as e:
            print("Exception= {}, cannot decode".format(e))
            return None
    else:
        return None
    
def get_xy_corrdinates_for_kotak(dump_with_BTET_Group,page_num):
    x=list()
    y=list()
    for tmp in dump_with_BTET_Group:
        if check_hex_using_regex(tmp,'.*TM$')==True or check_hex_using_regex(tmp,'.*Tm$')==True:
            tmp_split=tmp.split(' ')
            x.append(float(tmp_split[4]))
            y.append(float(tmp_split[5]))

    y_coor_max_diff=-1
    
    dump_length=len(dump_with_BTET_Group)
    counter=dump_length-1
    last_word=''
    
    if page_num==0:
        while counter>=0:
            if dump_length-counter>20:
                break
            match=decode_hex_into_string(dump_with_BTET_Group[counter])
            if match!=None:
                last_word=match
                break
            counter-=1
    
    for i in range(0,len(y)-1):
        last_word=last_word.lower()
        condition = ('account' in last_word ) and ('statement' in last_word)
        if i==len(y)-2 and page_num==0 and condition == True:
            break
        y_coor_max_diff=max(y[i+1] - y[i],y_coor_max_diff)
    return y_coor_max_diff

def get_max_no_of_values_inone_btet_group(dump_with_BTET_Group):
    max_cnt=0
    
    length=len(dump_with_BTET_Group)
    i=0
    while i<length:
        curr_cnt=0
        while i<length and dump_with_BTET_Group[i]!='ET':
            pattern = '<([0-9a-zA-Z]*)>'
            result = re.match(pattern,dump_with_BTET_Group[i])
            if result:
                curr_cnt+=1
            i+=1
        max_cnt=max(max_cnt,curr_cnt)
        i+=1
    return max_cnt

def check_for_sbi_special_format(dump_with_BTET_Group):
    keyword=['https://sbi.co.in','compliant','kycstatus','customercare@sbi.co.in','pleasedonotshareyouratm',
    'segment','myinformation','myhomebranchinformation','myaddress','myname','branchphone','homebranch','branchemail']

    complete_data=''
    for test_string in dump_with_BTET_Group:
        match=decode_hex_into_string(test_string)
        if match != None:
            complete_data=complete_data+str(match).lower()
    complete_data=complete_data.replace(' ','')
    complete_data=complete_data.replace('\n','')
    cnt=0
    for word in keyword:
        if word in complete_data:
            cnt+=1
    if cnt>=9:
        return True
    else:
        return False

def get_Tm_address_not_present_in_all_BT_ET_group(dump_with_BTET_Group):
    cnt=0
    length=len(dump_with_BTET_Group)
    i=0
    while i<length:
        curr_cnt=0
        flag=False
        while i<length and dump_with_BTET_Group[i]!='ET':
            if check_hex_using_regex(dump_with_BTET_Group[i],'.*TM$')==True:
                flag=True 
            i+=1
        if flag==False:
            cnt+=1
        i+=1
    return cnt

def get_stream_fraud_data_page(path, password, bank_name, page_no):
    f_dict={}
    f_dict['is_rgb_fraud']=False
    f_dict['rgb_cnt']=0
    f_dict['page_no_for_flag_000rg'] = set()
    f_dict['flag_000rg_50']=False
    f_dict['flag_000rg_60']=False
    f_dict['flag_000rg_70']=False
    f_dict['flag_000rg_80']=False
    f_dict['flag_000rg_90']=False
    f_dict['tag_hex_fraud']=True
    f_dict['tag_hex_cnt']=0
    f_dict['y_cordinates_max_diff']=-1
    f_dict['id_Xf1_fraud']=False
    f_dict['min_noof_bt_et_grps']=999999999999
    f_dict['flag_cm']=False
    f_dict['max_q_front']=0
    f_dict['max_q_back']=0
    f_dict['max_values_in_btet']=0
    f_dict['is_sbi_special_format']=False
    f_dict['cnt_of_tag_hex_present_pages']=0
    f_dict['pattern_scn_fraud']=False
    f_dict['td_present_not_tm']=False
    f_dict['Td_cnt']=0
    f_dict['TD_cnt']=0
    f_dict['touchup_textedit_fraud']=False
    f_dict['list_of_font_page'] = list()
    f_dict['TM_cnt'] = 0
    f_dict['TJ_cnt'] = 0
    f_dict['Tm_address_not_present_in_all_BT_ET_group']=0
    f_dict['font_all_sets']=set()
    f_dict['cnt_positive_y']=0
    f_dict['cnt_negative_y']=0
    f_dict['t_star_cnt'] = 0
    f_dict['non_hex_values_cnt']=0
    f_dict['colour_codes_in_betet'] = set()
    f_dict['all_colour_codes'] = set()
    f_dict['TJ_null_cnt']=0
    f_dict['Tj_null_cnt']=0

    try:
        pdf=pikepdf.open(path)
    except:
        pdf = pikepdf.open(path,password=str(password))        

    page = pdf.pages[page_no]
    instructions = pikepdf.parse_content_stream(page)
    data = pikepdf.unparse_content_stream(instructions)
    dump= data.decode('ascii')

    if ('devicergb' in dump.lower() ) or ( 'devicegrey' in dump.lower() ):
        f_dict['rgb_cnt'] += 1
        f_dict['is_rgb_fraud'] = True
    if 'pattern' in dump.lower():
        f_dict['pattern_scn_fraud']=True
    if ('/xf1' in dump.lower()):
        f_dict['id_Xf1_fraud']=True
        
    dump_split=dump.split('\n')
    dump_with_BTET_Group=update_dump(dump_split)
    BTET_Groups_length=len(dump_with_BTET_Group)
    noof_bt_et_group = get_cnt_of_Bt_ET_Groups(dump_with_BTET_Group)
    f_dict['min_noof_bt_et_grps']=min(noof_bt_et_group, f_dict['min_noof_bt_et_grps'])
    
    keywords=['0 0 0 rg','0 g','0 G','1 1 1 rg','0 0 0 sc','1 1 1 sc',
            '0 0 0 scn','0 SCN','0 scn','0 0 0 RG']
    first_occur_cnt_dict=dict()
    for word in keywords:
        first_occur_cnt_dict[word]=get_first_occur_cnt(dump_with_BTET_Group,word)
        
    #checking for keyword like 0 0 0 rg present in just end
    if bank_name=='sbi' and page_no==0:
        f_dict['is_sbi_special_format']=check_for_sbi_special_format(dump_with_BTET_Group)
    
    if BTET_Groups_length >= 100:
        for word in keywords:
            if (first_occur_cnt_dict[word]['first_occur']/BTET_Groups_length)*100 > 50:
                f_dict['page_no_for_flag_000rg'].add(page_no)
                f_dict['flag_000rg_50']=True
            if (first_occur_cnt_dict[word]['first_occur']/BTET_Groups_length)*100 > 60:
                f_dict['page_no_for_flag_000rg'].add(page_no)
                f_dict['flag_000rg_60']=True
            if (first_occur_cnt_dict[word]['first_occur']/BTET_Groups_length)*100 > 70:
                f_dict['page_no_for_flag_000rg'].add(page_no)
                f_dict['flag_000rg_70']=True
            if (first_occur_cnt_dict[word]['first_occur']/BTET_Groups_length)*100 > 80:
                f_dict['page_no_for_flag_000rg'].add(page_no)
                f_dict['flag_000rg_80']=True
            if (first_occur_cnt_dict[word]['first_occur']/BTET_Groups_length)*100 > 90:
                f_dict['page_no_for_flag_000rg'].add(page_no)
                f_dict['flag_000rg_90']=True
    #------------------------------------
    
    # checking for Tm_add_not_present_in_all_BT_ET_group
    tmp1_cnt = get_Tm_address_not_present_in_all_BT_ET_group(dump_with_BTET_Group)
    f_dict['Tm_address_not_present_in_all_BT_ET_group'] += tmp1_cnt
    #--------------------------------------------------

    #checking for tag hex fraud
    tag_hex_present_on_this_page=False
    for test_string in dump_split:
        match=check_hex_using_regex(test_string)
        if match == True:
            f_dict['tag_hex_cnt'] += 1
            f_dict['tag_hex_fraud'] = False
            tag_hex_present_on_this_page = True
    #BTET_Groups_length == 0 this when last page is empty
    if tag_hex_present_on_this_page==True or BTET_Groups_length == 0:
        f_dict['cnt_of_tag_hex_present_pages'] += 1
    #----------------------------
    
    #checking for cm
    for tmp in dump_split:
        if '1 0 0 1 0 0 cm' in tmp:
            f_dict['flag_cm'] = True
    #------------------------
    
    #checking for q front cnt and q back cnt
    q_front,q_back=get_q_in_start_and_end_fraud(dump_split)
    f_dict['max_q_front'] = max(q_front, f_dict['max_q_front'])
    f_dict['max_q_back'] = max(q_back, f_dict['max_q_back'])
    #-------------------
    
    #coordinates related predictors start here
    if bank_name=='kotak':
        f_dict['y_cordinates_max_diff'] = max(get_xy_corrdinates_for_kotak(dump_with_BTET_Group,page_no), f_dict['y_cordinates_max_diff'])
    else:
        y_coor, td_present, positive_y, negative_y = get_xy_corrdinates(dump_with_BTET_Group)
        f_dict['cnt_positive_y'] += positive_y
        f_dict['cnt_negative_y'] += negative_y
        f_dict['y_cordinates_max_diff'] = max(y_coor, f_dict['y_cordinates_max_diff'])
        f_dict['td_present_not_tm'] = f_dict['td_present_not_tm'] or td_present
    #---------------------------
    f_dict['max_values_in_btet']=max(f_dict['max_values_in_btet'], get_max_no_of_values_inone_btet_group(dump_with_BTET_Group))
    
    for test_string in dump_with_BTET_Group:
        test_string = test_string.replace(' ','')
        pattern = '.*Td$'
        result = re.match(pattern, test_string)
        if result:
            f_dict['Td_cnt'] += 1
    
    for test_string in dump_with_BTET_Group:
        test_string = test_string.replace(' ','')
        pattern = '.*TD$'
        result = re.match(pattern, test_string)
        if result:
            f_dict['TD_cnt'] += 1
        
    for test_string in dump_with_BTET_Group:
        test_string = test_string.replace(' ','')
        pattern = '.*TM$'
        result = re.match(pattern, test_string)
        if result:
            f_dict['TM_cnt'] += 1
    
    for test_string in dump_with_BTET_Group:
        test_string = test_string.replace(' ','')
        pattern = '.*TJ$'
        result = re.match(pattern, test_string)
        if result:
            f_dict['TJ_cnt'] += 1

    for test_string in dump_with_BTET_Group:
        test_string = test_string.replace(' ','')
        pattern = '<>TJ$'
        result = re.match(pattern, test_string)
        if result:
            f_dict['TJ_null_cnt'] += 1

    for test_string in dump_with_BTET_Group:
        test_string = test_string.replace(' ','')
        pattern = '<>Tj$'
        result = re.match(pattern, test_string)
        if result:
            f_dict['Tj_null_cnt'] += 1

    for test_string in dump_with_BTET_Group:
        test_string = test_string.replace(' ','')
        pattern = '.*(T*)$'
        result = re.match(pattern, test_string)
        if result:
            f_dict['t_star_cnt'] += 1

    if 'touchup' in dump.lower() or 'textedit' in dump.lower():
        f_dict['touchup_textedit_fraud'] = True
    
    for test_string in dump_with_BTET_Group:
        test_string = test_string.replace(' ','')
        tmp_flag_TJ_present = (check_hex_using_regex(test_string,'.*TJ')==True or check_hex_using_regex(test_string,'.*Tj')==True)
        if tmp_flag_TJ_present==True and check_hex_using_regex(test_string,'<([0-9a-zA-Z]*)>') == False:
            f_dict['non_hex_values_cnt']+=1
    
    #getting colour codes
    for test_string in dump_with_BTET_Group:
        patterns = ['.*(g)$','.*(G)$','.*(rg)$','.*(RG)$','.*(sc)$','.*(scn)$','.*(SCN)$']
        for pattern in patterns:
            result = re.match(pattern,test_string)
            if result:
                f_dict['colour_codes_in_betet'].add(test_string)
    
    for test_string in dump_split:
        patterns = ['.*(g)$','.*(G)$','.*(rg)$','.*(RG)$','.*(sc)$','.*(scn)$','.*(SCN)$']
        for pattern in patterns:
            result = re.match(pattern,test_string)
            if result:
                f_dict['all_colour_codes'].add(test_string)
    #--------------------------------------------
    
    #checking f3 version
    set_f123=set()
    for tmp in dump_split:
        pattern = '.*Tf$'
        result = re.match(pattern, tmp)
        if result:
            tmp=tmp.replace('Tf','')
            tmp=tmp.replace('/','')
            while len(tmp)>0 and tmp[len(tmp)-1]==' ':
                tmp=tmp[:-1]
            set_f123.add(tmp)
    tmp_list = list(set_f123)
    tmp_list.sort()
    f_dict['list_of_font_page'] = tmp_list
    for s in set_f123:
        f_dict['font_all_sets'].add(s)
    
    f_dict['font_all_sets'] = list(f_dict['font_all_sets'])
    f_dict['colour_codes_in_betet'] = list(f_dict['colour_codes_in_betet'])
    f_dict['all_colour_codes'] = list(f_dict['all_colour_codes'])
    f_dict['page_no_for_flag_000rg'] = list(f_dict['page_no_for_flag_000rg'])
    return f_dict
    #-------------------------
        
def add_stream_fraud_data_all_pages(stream_pages_dict_data, total_pages):
    is_rgb_fraud=False
    rgb_cnt=0
    flag_000rg_50=False
    flag_000rg_60=False
    flag_000rg_70=False
    flag_000rg_80=False
    flag_000rg_90=False
    tag_hex_fraud=True
    tag_hex_cnt=0
    y_cordinates_max_diff=-1
    id_Xf1_fraud=False
    min_noof_bt_et_grps=999999999999
    flag_cm=False
    max_q_front=0
    max_q_back=0
    max_values_in_btet=0
    is_sbi_special_format=False
    cnt_of_tag_hex_present_pages=0
    pattern_scn_fraud=False
    td_present_not_tm=False
    Td_cnt=0
    TD_cnt=0
    touchup_textedit_fraud=False
    list_of_font_pages = list()
    TM_cnt = 0
    TJ_cnt = 0
    Tm_address_not_present_in_all_BT_ET_group=0
    font_all_sets=list()
    cnt_positive_y=0
    cnt_negative_y=0
    t_star_cnt = 0
    non_hex_values_cnt=0
    colour_codes_in_betet = list()
    all_colour_codes = list()
    exception_in_fraud_logic = False
    page_no_for_flag_000rg = list()
    TJ_null_cnt=0
    Tj_null_cnt=0

    for page in range(total_pages):
        if (stream_pages_dict_data[page] == None or stream_pages_dict_data[page] == {} or stream_pages_dict_data[page]['exception_in_fraud_logic'] == True):
            continue
        is_rgb_fraud = is_rgb_fraud or stream_pages_dict_data[page]['is_rgb_fraud']
        rgb_cnt += stream_pages_dict_data[page]['rgb_cnt']
        flag_000rg_50 = flag_000rg_50 or stream_pages_dict_data[page]['flag_000rg_50']
        flag_000rg_60 = flag_000rg_60 or stream_pages_dict_data[page]['flag_000rg_60']
        flag_000rg_70 = flag_000rg_70 or stream_pages_dict_data[page]['flag_000rg_70']
        flag_000rg_80 = flag_000rg_80 or stream_pages_dict_data[page]['flag_000rg_80']
        flag_000rg_90 = flag_000rg_90 or stream_pages_dict_data[page]['flag_000rg_90']
        tag_hex_fraud = tag_hex_fraud and stream_pages_dict_data[page]['tag_hex_fraud']
        tag_hex_cnt += stream_pages_dict_data[page]['tag_hex_cnt']
        y_cordinates_max_diff = max(y_cordinates_max_diff, stream_pages_dict_data[page]['y_cordinates_max_diff'])
        id_Xf1_fraud = id_Xf1_fraud or stream_pages_dict_data[page]['id_Xf1_fraud']
        min_noof_bt_et_grps = min(min_noof_bt_et_grps, stream_pages_dict_data[page]['min_noof_bt_et_grps'])
        flag_cm = flag_cm or stream_pages_dict_data[page]['flag_cm']
        max_q_front = max(max_q_front, stream_pages_dict_data[page]['max_q_front'])
        max_q_back = max(max_q_back, stream_pages_dict_data[page]['max_q_back'])
        max_values_in_btet = max(max_values_in_btet, stream_pages_dict_data[page]['max_values_in_btet'])
        is_sbi_special_format = is_sbi_special_format or stream_pages_dict_data[page]['is_sbi_special_format']
        cnt_of_tag_hex_present_pages += stream_pages_dict_data[page]['cnt_of_tag_hex_present_pages']
        pattern_scn_fraud = pattern_scn_fraud or stream_pages_dict_data[page]['pattern_scn_fraud']
        td_present_not_tm = td_present_not_tm or stream_pages_dict_data[page]['td_present_not_tm']
        TM_cnt += stream_pages_dict_data[page]['TM_cnt']
        TJ_cnt += stream_pages_dict_data[page]['TJ_cnt']
        Td_cnt += stream_pages_dict_data[page]['Td_cnt']
        TD_cnt += stream_pages_dict_data[page]['TD_cnt']
        Tm_address_not_present_in_all_BT_ET_group += stream_pages_dict_data[page]['Tm_address_not_present_in_all_BT_ET_group']
        list_of_font_pages.append(stream_pages_dict_data[page]['list_of_font_page'])
        cnt_positive_y += stream_pages_dict_data[page]['cnt_positive_y']
        cnt_negative_y += stream_pages_dict_data[page]['cnt_negative_y']
        t_star_cnt += stream_pages_dict_data[page]['t_star_cnt']
        non_hex_values_cnt += stream_pages_dict_data[page]['non_hex_values_cnt']
        colour_codes_in_betet.extend(stream_pages_dict_data[page]['colour_codes_in_betet'])
        all_colour_codes.extend(stream_pages_dict_data[page]['all_colour_codes'])
        font_all_sets.extend(stream_pages_dict_data[page]['font_all_sets'])
        exception_in_fraud_logic = exception_in_fraud_logic or stream_pages_dict_data[page]['exception_in_fraud_logic']
        touchup_textedit_fraud = touchup_textedit_fraud or stream_pages_dict_data[page]['touchup_textedit_fraud']
        page_no_for_flag_000rg.extend(stream_pages_dict_data[page]['page_no_for_flag_000rg'])
        TJ_null_cnt += stream_pages_dict_data[page]['TJ_null_cnt']
        Tj_null_cnt += stream_pages_dict_data[page]['Tj_null_cnt']
    
    cnt_of_pagefonts_not_equal=0
    for i in range(len(list_of_font_pages)-1):
        if list_of_font_pages[i]!=list_of_font_pages[i+1]:
            cnt_of_pagefonts_not_equal+=1
    
    final_fraud_dict=dict()
    final_fraud_dict['flag_000rg_50']=flag_000rg_50
    final_fraud_dict['flag_000rg_60']=flag_000rg_60
    final_fraud_dict['flag_000rg_70']=flag_000rg_70
    final_fraud_dict['flag_000rg_80']=flag_000rg_80
    final_fraud_dict['flag_000rg_90']=flag_000rg_90
    final_fraud_dict['is_rgb_fraud']=is_rgb_fraud
    final_fraud_dict['y_cordinates_max_diff']=Decimal(str(y_cordinates_max_diff))
    final_fraud_dict['tag_hex_fraud']=tag_hex_fraud
    final_fraud_dict['tag_hex_cnt']=tag_hex_cnt
    final_fraud_dict['id_Xf1_fraud']=id_Xf1_fraud
    final_fraud_dict['min_noof_bt_et_grps']=min_noof_bt_et_grps
    final_fraud_dict['flag_cm']=flag_cm
    final_fraud_dict['max_q_front']=max_q_front
    final_fraud_dict['max_q_back']=max_q_back
    final_fraud_dict['max_values_in_btet']=max_values_in_btet
    final_fraud_dict['is_sbi_special_format']=is_sbi_special_format
    final_fraud_dict['no_of_pages']=total_pages
    final_fraud_dict['cnt_of_tag_hex_present_pages']=cnt_of_tag_hex_present_pages
    final_fraud_dict['pattern_scn_fraud']=pattern_scn_fraud
    final_fraud_dict['Td_cnt']=Td_cnt
    final_fraud_dict['TD_cnt']=TD_cnt
    final_fraud_dict['touchup_textedit_fraud']=touchup_textedit_fraud
    final_fraud_dict['td_present_not_tm']=td_present_not_tm
    final_fraud_dict['cnt_of_pagefonts_not_equal']=cnt_of_pagefonts_not_equal
    final_fraud_dict['TM_cnt']=TM_cnt
    final_fraud_dict['TJ_cnt']=TJ_cnt
    list_font_all_sets = list(set(font_all_sets))
    list_font_all_sets.sort()
    final_fraud_dict['list_of_font_pages']=list_font_all_sets
    final_fraud_dict['Tm_address_not_present_in_all_BT_ET_group']=Tm_address_not_present_in_all_BT_ET_group
    final_fraud_dict['cnt_positive_y']=cnt_positive_y
    final_fraud_dict['cnt_negative_y']=cnt_negative_y
    final_fraud_dict['rgb_cnt']=rgb_cnt
    final_fraud_dict['t_star_cnt']=t_star_cnt
    final_fraud_dict['non_hex_values_cnt']=non_hex_values_cnt
    colour_codes_in_betet_list = list(set(colour_codes_in_betet))
    colour_codes_in_betet_list.sort()
    all_colour_codes_list = list(set(all_colour_codes))
    all_colour_codes_list.sort()
    final_fraud_dict['colour_codes_in_betet']=colour_codes_in_betet_list
    final_fraud_dict['all_colour_codes']=all_colour_codes_list
    final_fraud_dict['exception_in_fraud_logic']=exception_in_fraud_logic
    final_fraud_dict['page_no_for_flag_000rg']=list(set(page_no_for_flag_000rg))
    final_fraud_dict['TJ_null_cnt'] = TJ_null_cnt
    final_fraud_dict['Tj_null_cnt'] = Tj_null_cnt
    return final_fraud_dict

def get_temp_fraud_for_other_countries(producer, author, creator, response_metadata):
    print("Inside other countries fraud calculation")
    # check for bad authors
    bad_creator_words = get_compiled_regex_list(['(?i).*(word).*',
                                                '(?i).*(office).*',
                                                '(?i).*(Winnovative).*',
                                                '(?i).*(ilovepdf).*',
                                                '(?i).*(windows user).*',
                                                '(?i).*(Online2PDF).*',
                                                '(?i).*(desygner).*',
                                                '(?i).*(EXCEL).*',
                                                '(?i).*(intsig).*',
                                                '(?i).*(camscanner).*',
                                                '(?i).*(adobe).*',
                                                '(?i).*(pybrary).*',
                                                '(?i).*(cloudconvert).*',
                                                '(?i).*(canon).*',
                                                '(?i).*(inetsoft).*',
                                                '(?i).*(abcpdf).*',
                                                '(?i).*(pdfaid).*',
                                                '(?i).*(foxit).*',
                                                '(?i).*(sejda).*',
                                                '(?i).*(sambox).*',
                                                '(?i).*(pdfmake).*',
                                                '(?i).*(powerpoint).*',
                                                '(?i).*(PDF24).*',
                                                '(?i).*(LibreOffice).*',
                                                '(?i).*(HiQPdf).*',
                                                '(?i).*(WPS).*',
                                                '(?i).*((4.3.4)).*',
                                                '(?i).*(2\.4\.).*',
                                                '(?i).*(icecream\s*pdf).*',
                                                '(?i).*(pdf-tools.com).*',
                                                '(?i).*(3.0.2).*',
                                                '(?i).*(3.0.6).*',
                                                '(?i).*(visual\s*paradigm).*',
                                                '(?i).*(pdf\-*lib).*',
                                                '(?i).*(canva).*',
                                                '(?i).*(aspose).*',
                                                '(?i).*(pdfkit).*',
                                                '(?i).*(samsung\s*electronic).*',
                                                '(?i).*(print\s*to\s*pdf).*']) 

    for bad_author_regex in bad_creator_words:
        check_producer = match_compiled_regex(producer, bad_author_regex, 1)
        check_author = match_compiled_regex(author, bad_author_regex, 1)
        check_creator = match_compiled_regex(creator, bad_author_regex, 1)
        if (check_producer is not None) or (check_author is not None) or (check_creator is not None):
            return True, 'author_fraud', response_metadata, ['author_fraud']
    return False, None, response_metadata, []

def get_metadata_fraud(new_content_stream_data, doc, bank, path = '', password = '', country = 'IN', stream_font_list = [], encryption_algo_list=[], good_font_list=[], strict_metadata_fraud_list=[]):
    metadata = doc.metadata
    
    #marking if pdf is rotated or not
    num_pages = doc.page_count
    is_pdf_rotated = False
    if num_pages>0:
        page = doc[0]
        if page.derotation_matrix[5] != 0:
            is_pdf_rotated = True

    author = metadata.get('author', None)
    producer = metadata.get('producer',None)
    creator = metadata.get('creator',None)

    unicode_flag =not (author.isascii() and producer.isascii() and creator.isascii())

    cleaned_author = re.sub(r'(\s|\u180B|\u200B|\u200C|\u200D|\u2060|\uFEFF|\u00AE|\u00E2|\u00A9|\u00E2|\u201E|\u00A2)+', " ", " ".join(author.split())).strip().lower()
    cleaned_producer = re.sub(r'(\s|\u180B|\u200B|\u200C|\u200D|\u2060|\uFEFF|\u00AE|\u00E2|\u00A9|\u00E2|\u201E|\u00A2)+', " ", " ".join(producer.split())).strip().lower()
    cleaned_creator = re.sub(r'(\s|\u180B|\u200B|\u200C|\u200D|\u2060|\uFEFF|\u00AE|\u00E2|\u00A9|\u00E2|\u201E|\u00A2)+', " ", " ".join(creator.split())).strip().lower()

    pdf_format_creation_date = metadata.get('creationDate', None)
    pdf_format_modification_date = metadata.get('modDate', None)

    meta_details = get_doc_font_list(doc)
    exception_in_fraud_logic = new_content_stream_data.pop('exception_in_fraud_logic', False)

    encryption_algo = metadata.get('encryption', None)

    
    fonts = meta_details.get("doc_fonts", None)
    response_metadata = {
        "author": cleaned_author,
        "producer": cleaned_producer,
        "creator": cleaned_creator,
        "pdf_format_creation_date": pdf_format_creation_date,
        "pdf_format_modification_date": pdf_format_modification_date,
        "fonts": meta_details.get("doc_fonts", None),
        "encryption_algo": encryption_algo,
        "font_size": meta_details.get("cleaned_font_size",None),
        "font_colors": meta_details.get("cleaned_colors",None),
        "linewidth": meta_details.get("cleaned_linewidth",None),
        "trapped": metadata.get('trapped', None),
        "subject": metadata.get('subject', None),
        "keywords": metadata.get('keywords', None),
        "format": metadata.get('format', None),
        "doc_filter": meta_details.get("doc_filter",None),
        "xref_id": meta_details.get("xref_id",None),
        "is_pdf_rotated": is_pdf_rotated
    }

    for key in new_content_stream_data.keys():
        response_metadata[key]=new_content_stream_data[key]

    #TODO: remove this condition and add fraud logic for other countries also
    print("country of origin ", country)
    if country != "IN" and country != None:
        return get_temp_fraud_for_other_countries(producer, author, creator, response_metadata)

    pdf_version = metadata.get('format', None)

    #axis new format which is received through mail
    stream_font = new_content_stream_data.get('list_of_font_pages','')
    if stream_font != None:
        str_stream_font = str(stream_font)
    else:
        str_stream_font = ''
    is_axis_email_format = False
    if bank == 'axis' and ("['F1 1', 'F1 10', 'F1 11'," in str_stream_font or "['F1 8', 'F2 10', 'F2 11', 'F2 7', " in str_stream_font  
        or "['F1 8', 'F10 8', 'F11 8', 'F2 10', 'F2 11', " in str_stream_font or "['F1 10', 'F1 7', 'F1 8', 'F1 9', 'F2 7', 'F3 10', 'F3 11', " in str_stream_font
        or "['F1 10.5', 'F1 7.5', 'F1 9', 'F2 9']" in str_stream_font or "['F1 8', 'F11 8', 'F2 10', 'F2 11', 'F2 7', " in str_stream_font
        or "['F1 8', 'F2 10', 'F2 11', 'F2 7', 'F2 8', 'F2 9', " in str_stream_font or "['F1 8', 'F11 8', 'F2 10', 'F2 11', 'F2 7', " in str_stream_font
        or "['F1 8', 'F10 8', 'F2 10', 'F2 11', 'F2 7', 'F2 8', " in str_stream_font ) :
        is_axis_email_format = True
    is_icici_email_format = False
    #icici new format which is received through mail
    if bank == "icici" and ("['F 6', 'F 7', 'F 7.5', 'F 8', " in str_stream_font or "['F 6', 'F 7', 'F 7.25', 'F 7.5', 'F 8', 'F 8.25', 'F 9', " in str_stream_font
        or "['F 5.3', 'F 5.5', 'F 6', 'F 7', 'F 7.5', 'F 8', " in str_stream_font or "['F 3', 'F 6', 'F 7', 'F 7.5', 'F 8', " in str_stream_font 
        or "['F 5', 'F 5.3', 'F 5.5', 'F 6', 'F 7', 'F 7.5', 'F 8', " in str_stream_font or "['F 10', 'F 6', 'F 6.75', 'F 7', 'F 8', 'F 8.25', 'F 9', 'F 9.75', " in str_stream_font
        or ", 'F 6', 'F 7', 'F 7.5', 'F 8', 'F 8.25', 'F 9', " in str_stream_font or "['F 4', 'F 5.5', 'F 6', 'F 7', 'F 7.5', 'F 8', 'F 8.25', 'F 9', " in str_stream_font 
        or "['F 5', 'F 5.3', 'F 6', 'F 7', 'F 7.5', 'F 8', 'F0 5', " in str_stream_font or "['F 5', 'F 6', 'F 7', 'F 7.5', 'F 8', 'F0 5', 'F0 5.3', 'F0 5.5', 'F0 5.8', " in str_stream_font ) :
        is_icici_email_format = True
    
    is_kotak_email_format = False
    # Kotak new format which is received through mail
    if bank == 'kotak' and ("['F1 21', 'F1 24', 'F2 10', 'F3 10', 'F3 7', 'F3 8', 'F4 10', 'F5 10', 'F5 12', 'F5 6', " in str_stream_font or "['F1 21', 'F1 24', 'F2 10', 'F3 10', 'F3 12', 'F3 6', 'F3 7', 'F3 8', " in str_stream_font
        or "['F1 10', 'F2 5', 'F2 6', 'F3 10', 'F3 21', 'F3 22', 'F3 24', 'F3 28', " in str_stream_font 
        or "['F1 10', 'F10 8', 'F12 6', 'F2 5', 'F2 6', 'F3 10', 'F3 21', 'F3 22', " in str_stream_font ) :
        is_kotak_email_format = True
        
    is_sbi_email_format = False
    # sbi new format which is received through mail
    if bank == 'sbi' and "['F1 10', 'F1 11', 'F1 12', 'F1 14', 'F1 6', 'F1 7', " in str_stream_font :
        is_sbi_email_format = True
    
    is_ausfbnk_email_format=False
    ausf_new_form_regex = re.compile('(\[.*EvoPdf\_.*\])')
    # Ausf new format received from client
    if bank=='ausfbnk' and ausf_new_form_regex.findall(str_stream_font):
        is_ausfbnk_email_format=True

    is_baroda_email_format = False
    # sbi new format which is received through mail
    if bank == 'baroda' and ("['F 11', 'F 5', 'F 6.5', 'F 7.5', 'F 8', 'F 8.7', " in str_stream_font or "['F 11', 'F 6.5', 'F 7.5', 'F 8', 'F 8.7', " in str_stream_font 
        or "['F 11', 'F 7.5', 'F 8', 'F 8.7', 'F0 5', " in str_stream_font or "['F 11', 'F 7.5', 'F 8', 'F 8.7', 'F0 11', " in str_stream_font) :
        is_baroda_email_format = True

    axis_ignore_flag50 = [["F1 10","F1 7","F1 8","F1 9","F2 10","F2 7","F2 8","F2 9","F3 8","F4 10","F4 7","F4 8","F5 8","F6 7"],
    ["F1 10","F1 7","F1 9","F2 10","F2 7","F2 8","F2 9","F3 8","F4 10","F4 7","F5 8","F6 7"],
    ["F1 10","F1 7","F1 8","F1 9","F2 10","F2 7","F2 8","F2 9","F3 8","F4 8","F5 10","F5 7","F5 8","F6 7"]]
    
    # ********************************************
    #stream fraud logic starts here

    #handling for exception case
    fraud_list = []
    if exception_in_fraud_logic and bank != 'fincarebnk':
        fraud_list.append('pikepdf_exception')

    # checking for rgb fraud logic
    banks_not_supported = ['idfc', 'saurashtra', 'karnataka_vikas', 'stanchar', 'rmgbbnk', 'apgvbnk', 'municipalbnk', 'sabarkantha', 'telangana', 'mizoram', 'panchmahal_district', 'amreli_jilla_sahakari',
    'chhattisgarh', 'coastal_area_bnk', 'apcob', 'banas', 'fingrowth', 'andhra_pragathi', 'mahagrambnk', 'gscb', 'uttrakhand_gramin', 
    'rajkot_district', 'jharkhand_rajya', 'greaterbnk', 'suratbnk']
    if bank not in banks_not_supported:
        flag_is_rgb_fraud = new_content_stream_data.get("is_rgb_fraud",False)
        if bank in banks_not_supported:
            if new_content_stream_data['list_of_font_pages'] != ["F1 8", "F1 9", "F2 7", "F2 8", "F3 9"] and flag_is_rgb_fraud == True:
                fraud_list.append('rgb_fraud')
        elif flag_is_rgb_fraud == True:
            fraud_list.append('rgb_fraud')

    #checking for tag hex fraud
    if 'tag_hex_fraud' in new_content_stream_data.keys() and new_content_stream_data['tag_hex_fraud']==True:
        if bank in ['axis','pnbbnk','baroda','central','idbi','icici','idfc','sbi','ubi','uco','iob']:    
            fraud_list.append('tag_hex_fraud')
        if bank in ['boi'] and new_content_stream_data['list_of_font_pages'] == ["F1 12","F2 10","F2 9"]:
            fraud_list.append('tag_hex_fraud')
        if bank in ['canara'] and new_content_stream_data['list_of_font_pages'] != ["F1 11","F1 7","F1 8","F1 9","F3 18","F3 8","F3 9"]:
            fraud_list.append('tag_hex_fraud')
    #----------------------------

    #checking for flag_000rg_50 fraud
    #can be added for iob
    if bank in ['sbi']:
        pages_apart_from_0_1_2 = False
        for tmp_page in new_content_stream_data['page_no_for_flag_000rg']:
            if tmp_page not in [0,1,2]:
                pages_apart_from_0_1_2 = True
        if (new_content_stream_data['is_sbi_special_format'] == False or  pages_apart_from_0_1_2 == True) and is_sbi_email_format == False and new_content_stream_data['flag_000rg_50']==True:
            fraud_list.append('flag_000rg_50_fraud')

    if bank in ['pnbbnk','central','canara']:
        if new_content_stream_data['flag_000rg_50']==True:
            fraud_list.append('flag_000rg_50_fraud')
    if bank in ['hdfc'] and new_content_stream_data.get('list_of_font_pages', []) != '["F1 6","F1 8","F2 12","F3 6","F3 8"]':
        if new_content_stream_data['flag_000rg_50']==True:
            fraud_list.append('flag_000rg_50_fraud')
    if bank in ['axis'] and is_axis_email_format == False and new_content_stream_data.get('list_of_font_pages',[]) not in axis_ignore_flag50:
        if new_content_stream_data.get('flag_000rg_50',False):
            fraud_list.append('flag_000rg_50_fraud')
    if bank in ['ubi'] and new_content_stream_data.get('list_of_font_pages', []) != ["F1 12","F2 12","F2 20","F2 8","F3 12","F3 13"]:
        if new_content_stream_data.get('flag_000rg_50',False):
            fraud_list.append('flag_000rg_50_fraud')
    #------------------------------

    #checking for new tag hex fraud basis on number of pages hex present
    if 'cnt_of_tag_hex_present_pages' in new_content_stream_data.keys() and 'no_of_pages' in new_content_stream_data.keys() and new_content_stream_data['cnt_of_tag_hex_present_pages'] < new_content_stream_data['no_of_pages']:
        if bank in ['axis','pnbbnk','baroda','central','idbi','icici','idfc','sbi','uco','iob', 'dbsbnk', 'federal', 'indbnk', 'indusind', 'kotak']:
            fraud_list.append('tag_hex_on_page_cnt_fraud')
        if bank in ['boi'] and new_content_stream_data['list_of_font_pages'] == ["F1 12","F2 10","F2 9"]:
            fraud_list.append('tag_hex_on_page_cnt_fraud')
        if bank in ['canara'] and new_content_stream_data['list_of_font_pages'] != ["F1 11","F1 7","F1 8","F1 9","F3 18","F3 8","F3 9"]:
            fraud_list.append('tag_hex_on_page_cnt_fraud')
    #----------------------------------
   #------------------------------

    #checking for new tag hex fraud basis on non tag hex value and hex present value
    if 'tag_hex_cnt' in new_content_stream_data.keys() and 'non_hex_values_cnt' in new_content_stream_data.keys() and new_content_stream_data['tag_hex_cnt']>20 and new_content_stream_data['non_hex_values_cnt']>20:
        if bank in ['jnkbnk','varachha','hsbc','kalupurbnk','pune_district','tamil_mercantile','ahmedabad_mercantile','sbi','stanchar','sbmbank','mahabk','federal','ujjivan','primebnk','suryodaybnk','citizens','financial','city_union','vasai_janata','karnavati','kukarwada_nagrik','sncb','rajkot_cobnk','utkarshbnk','nawanagar','bharatbnk','india_post','indbnk','udaipur_mahila_urban','satara_district','sarvodaya','kankariabnk','jana','saraswat','rbl','karnataka','equitas','andhra','jivan','fingrowth','pune_urban','deogiri','associatebnk','uttrakhand_gramin','kerala_gramin','jio_payments','punjab_sind','yesbnk','karur','dbsbnk','indusind']:
            fraud_list.append('Non_hex_fraud')
            
   #------------------------------        
    #checking for TD cnt fraud
    if bank in ['pnbbnk','baroda','central','canara''hdfc','kotak','idbi','icici','idfc','ubi','uco','iob','boi','karnataka','mahabk','equitas','yesbnk','karur','federal','indusind','dbsbnk','indbnk']:
        if 'TD_cnt' in new_content_stream_data.keys() and new_content_stream_data['TD_cnt'] > 0:
            fraud_list.append('TD_cnt_fraud')
    #------------------------

    #checking for TJ cnt fraud
    #for boi TJ_cnt not working for ["F1 9","F2 9","F3 10"] this font,rest works we can add later
    if bank in ['axis','pnbbnk','baroda','central','kotak','idbi','icici','idfc','ubi','uco','iob']:
        if 'TJ_cnt' in new_content_stream_data.keys() and new_content_stream_data['TJ_cnt'] > 0:
            fraud_list.append('TJ_cnt_fraud')
    #------------------------

    #checking for touchup_textedit_fraud 
    if bank in ['axis','baroda','central','canara','hdfc','kotak','idbi','icici','idfc','sbi','ubi','uco','iob','boi']:
        if 'touchup_textedit_fraud' in new_content_stream_data.keys() and new_content_stream_data['touchup_textedit_fraud'] == True:
            fraud_list.append('touchup_textedit_fraud')
    #---------------------

    #checking for cnt_of_pagefonts_not_equal(mostly detects multiple merged pdfs)
    if bank in ['baroda','idbi','idfc','ubi','uco','iob','boi','canara','federal'] and is_baroda_email_format == False :
        if 'cnt_of_pagefonts_not_equal' in new_content_stream_data.keys() and new_content_stream_data['cnt_of_pagefonts_not_equal'] >= 5:
            fraud_list.append('cnt_of_pagefonts_not_equal_fraud')

    if bank in ['baroda'] and is_baroda_email_format == True :
        if 'cnt_of_pagefonts_not_equal' in new_content_stream_data.keys() and new_content_stream_data['cnt_of_pagefonts_not_equal'] >= 6:
            fraud_list.append('cnt_of_pagefonts_not_equal_fraud')


    if bank in ['axis','sbi','indusind','dbsbnk','central'] and is_sbi_email_format == False and is_axis_email_format == False :
        if 'cnt_of_pagefonts_not_equal' in new_content_stream_data.keys() and new_content_stream_data['cnt_of_pagefonts_not_equal'] >= 7:
            fraud_list.append('cnt_of_pagefonts_not_equal_fraud')
    
    if bank in ['mahabk','equitas']:
        if 'cnt_of_pagefonts_not_equal' in new_content_stream_data.keys() and new_content_stream_data['cnt_of_pagefonts_not_equal'] >= 6:
            fraud_list.append('cnt_of_pagefonts_not_equal_fraud')
            
    if bank in ['indbnk','hdfc','karnataka','yesbnk','karur']:
        if 'cnt_of_pagefonts_not_equal' in new_content_stream_data.keys() and new_content_stream_data['cnt_of_pagefonts_not_equal'] >= 4:
            fraud_list.append('cnt_of_pagefonts_not_equal_fraud')

    if bank in ['icici'] and is_icici_email_format == False:
        if new_content_stream_data.get('cnt_of_pagefonts_not_equal', -1) >= 5:
            fraud_list.append('cnt_of_pagefonts_not_equal_fraud')

    if bank not in ['indbnk','pnbbnk','stanchar','federal','indusind','dbsbnk','central','hdfc','karnataka','mahabk','equitas','yesbnk','karur','idfc','icici','idbi','sbi','axis','ubi','iob','uco','boi','baroda','canara', 'phonepe_bnk'] and is_kotak_email_format == False:
        if 'cnt_of_pagefonts_not_equal' in new_content_stream_data.keys() and new_content_stream_data['cnt_of_pagefonts_not_equal'] >= 20:
            fraud_list.append('cnt_of_pagefonts_not_equal_fraud')
    #----------------------------------------

    #checking for good stream fonts
    good_stream_font_list = stream_font_list
    if len(good_stream_font_list)==0:
        print('Getting Good Stream Font List from Fallback Function')
        good_stream_font_list = get_stream_fonts(bank)

    if good_stream_font_list != None and len(good_stream_font_list)>0 and exception_in_fraud_logic == False:
        list_of_font_pages = new_content_stream_data.get('list_of_font_pages', [])
        if bank == 'axis':
            if not is_axis_email_format and list_of_font_pages not in good_stream_font_list:
                fraud_list.append('good_font_type_size_fraud')
        elif bank == 'icici':
            if not is_icici_email_format and list_of_font_pages not in good_stream_font_list:
                fraud_list.append('good_font_type_size_fraud')
        elif bank == 'sbi':
            if not is_sbi_email_format and list_of_font_pages not in good_stream_font_list:
                fraud_list.append('good_font_type_size_fraud')
        elif bank == 'kotak':
            if not is_kotak_email_format and list_of_font_pages not in good_stream_font_list:
                fraud_list.append('good_font_type_size_fraud')
        elif bank == 'boi':
            boi_not_check_condition = (author == '' and producer == '' and creator == '' and pdf_version == 'PDF 1.5' and list_of_font_pages == [])
            if not boi_not_check_condition and list_of_font_pages not in good_stream_font_list:
                fraud_list.append('good_font_type_size_fraud')
        elif bank == 'ausfbnk':
            if is_ausfbnk_email_format == False and list_of_font_pages not in good_stream_font_list:
                fraud_list.append('good_font_type_size_fraud')
        elif list_of_font_pages not in good_stream_font_list:
            fraud_list.append('good_font_type_size_fraud')

    # checking for Tj_null_cnt fraud
    if bank in ['sbi']:
        if 'Tj_null_cnt' in new_content_stream_data.keys() and new_content_stream_data['Tj_null_cnt'] == 0 :
            fraud_list.append('Tj_null_cnt_fraud')
    #-------------------------------------------------------------
    #stream fraud logic ends here
    # ********************************************

    #print('author', author, 'producer', producer, 'creator', creator)
    #print('cleaned_author', cleaned_author, 'cleaned_producer', cleaned_producer, 'cleaned_creator', cleaned_creator)

    # check for bad authors
    bad_creator_words = get_compiled_regex_list(['(?i).*(word).*',
                                                 '(?i).*(office).*',
                                                 '(?i).*(Winnovative).*',
                                                 '(?i).*(ilovepdf).*',
                                                 '(?i).*(windows user).*',
                                                 '(?i).*(Online2PDF).*',
                                                 '(?i).*(desygner).*',
                                                 '(?i).*(EXCEL).*',
                                                 '(?i).*(intsig).*',
                                                 '(?i).*(camscanner).*',
                                                 '(?i).*(adobe).*',
                                                 '(?i).*(pybrary).*',
                                                 '(?i).*(cloudconvert).*',
                                                 '(?i).*(canon).*',
                                                 '(?i).*(inetsoft).*',
                                                 '(?i).*(abcpdf).*',
                                                 '(?i).*(pdfaid).*',
                                                 '(?i).*(foxit).*',
                                                 '(?i).*(sejda).*',
                                                 '(?i).*(sambox).*',
                                                 '(?i).*(pdfmake).*',
                                                 '(?i).*(powerpoint).*',
                                                 '(?i).*(PDF24).*',
                                                 '(?i).*(LibreOffice).*',
                                                 '(?i).*(HiQPdf).*',
                                                 '(?i).*(WPS).*',
                                                 '(?i).*((4.3.4)).*',
                                                 '(?i).*(2\.4\.).*',
                                                 '(?i).*(icecream\s*pdf).*',
                                                 '(?i).*(visual\s*paradigm).*',
                                                 '(?i).*(pdf\-*lib).*',
                                                 '(?i).*(canva).*',
                                                 '(?i).*(aspose).*',
                                                 '(?i).*(pdfkit).*',
                                                 '(?i).*(samsung\s*electronic).*',
                                                 '(?i).*(print\s*to\s*pdf).*'])

    for bad_author_regex in bad_creator_words:
        check_producer = match_compiled_regex(producer, bad_author_regex, 1)
        check_author = match_compiled_regex(author, bad_author_regex, 1)
        check_creator = match_compiled_regex(creator, bad_author_regex, 1)
        if (check_producer is not None) or (check_author is not None) or (check_creator is not None):
            # print(check_creator,check_author,check_producer)
            fraud_list.append('author_fraud')

    bad_authors= ['ilovepdf', 'microsoftâ® word 2019', '2.3.5 (4.2.16)', 'www.ilovepdf.com', 'ios version 15.4.1 (build 19e258) quartz pdfcontex', 'microsoftâ® word 2021', 'microsoftâ® word for microsoft 365', 'soda pdf', 'skia/pdf m94', '2.4.6 (4.3.3)', 'ios version 15.4 (build 19e241) quartz pdfcontext', 'ios version 15.5 (build 19f77) quartz pdfcontext', '2.4.19 (4.3.6)', 'pdf candy', 'neevia pdfmerge v4.1 build 650', 'ios version 14.8.1 (build 18h107) quartz pdfcontext','3.0.0.m3 (5.0.0.m3)']

    # if 'quadient' in author.lower().split() or 'quadient' in producer.lower().split() or 'quadient' in creator.lower().split():
    #     return True, 'author_fraud', response_metadata

    if cleaned_author in bad_authors or cleaned_producer in bad_authors or cleaned_creator in bad_authors:
        fraud_list.append('author_fraud')


    # iText logic by Mihir
    producer_list = ['2.4.24 (4.3.13)','2.4.24 (4.3.17)']
    if cleaned_author in producer_list or cleaned_producer in producer_list or cleaned_creator in producer_list:
        if pdf_version in ['PDF 1.5','PDF 1.6']:
            fraud_list.append('author_fraud')
    
    sbi_specific_fraud_creators = ['itext 2.0.4((by lowagie.com)']
    if (bank in ['sbi']) and (pdf_version in ['PDF 1.5','PDF 1.6']) and ((cleaned_author in sbi_specific_fraud_creators) or (cleaned_producer in sbi_specific_fraud_creators) or (cleaned_creator in sbi_specific_fraud_creators)):
        fraud_list.append('author_fraud')

    # inference logic by nikhil and abhilash and new addition by divik
    regex_list = ['.*(3-heights).*','.*(pypdf).*']
    for regex in regex_list:
        compiled_regex = re.compile(regex)
        if (compiled_regex.findall(cleaned_author) or compiled_regex.findall(cleaned_creator) or compiled_regex.findall(cleaned_producer)): 
            if pdf_version=='PDF 1.6':
                fraud_list.append('author_fraud')
            if regex == '.*(3-heights).*' and bank not in ['axis']:
                fraud_list.append('author_fraud')
            if regex == '.*(pypdf).*' and bank not in ['federal']:
                fraud_list.append('author_fraud')

    # iob_regex_list = ['.*itext\s*5.5.11.*']
    # if bank=='iob':
    #     for regex in iob_regex_list:
    #         regex = re.compile(regex)
    #         if regex.findall(cleaned_author) or regex.findall(cleaned_creator) or regex.findall(cleaned_producer):
    #             return True, 'author_fraud', response_metadata

    flag=0
    if len(good_font_list)==0:
        good_font_data_from_fallback = json.load(open('./library/good_fonts.json'))
        good_font_list = good_font_data_from_fallback.get(bank, [])
        print('Getting Good Fonts from Fallback')

    if len(good_font_list)>0:
        for i in fonts:
            if i.lower().strip() not in good_font_list:
                flag+=1
                break

    # check for encryption    
    # if encryption_algo is None or encryption_algo == "":
    #     flag+=1
    # bank_wise_good_encryptions = json.load(open('./library/good_encryption.json'))
    if len(encryption_algo_list)==0:
        encryption_data_from_fallback = json.load(open('./library/good_encryption.json'))
        encryption_algo_list = encryption_data_from_fallback.get(bank, [])
        print('Getting Encryption Algos from Fallback')

    if len(encryption_algo_list)>0:
        if encryption_algo not in encryption_algo_list:
            flag+=1
    elif encryption_algo in [None,'']:
        flag+=1

    # flag value 2 means font and encryption both frauds found
    if flag==2 and is_icici_email_format == False and is_axis_email_format == False and is_kotak_email_format == False:
        fraud_list.append('font_and_encryption_fraud')

    print('Checking fraud based on strict metadata checks')
    is_author_fraud, author_fraud_type = strict_fraud_metadata_check(bank, unicode_flag, pdf_version, [cleaned_creator, cleaned_producer, cleaned_author],[creator,producer, author], strict_metadata_fraud_list=strict_metadata_fraud_list)
    if is_author_fraud:
        fraud_list.append(author_fraud_type)
    
    fraud_list = list(dict.fromkeys(fraud_list))
    
    if len(fraud_list)>0:
        return True, fraud_list[0], response_metadata, fraud_list
    return False, None, response_metadata, fraud_list

def check_if_image(doc):
    """
    Check if given fitz document is actual pdf or a scanned image
    :param: path (pdf file path), password (for pdf file)
    :return: bool indicating whether given file is just a scanned image inside pdf or not
    """

    num_pages = doc.page_count
    #num_pages = len(doc)
    if num_pages > 0:
        regex = re.compile('.*([0-9]+).*')
        num_numeric = 0  # number of numeric words found
        try:
            all_text = doc[0].get_text_words(flags=pymupdf.TEXTFLAGS_WORDS)  # get words from first page
        except Exception as _:
            all_text = []
        no_of_words = len(all_text)  # total length of words

        for each in all_text:
            # each is a tuple with value (x0, y0, x1, y1, word, block_n, line_n, word_n)
            # therefore actual word is at index 4
            numbers_exist = match_compiled_regex(each[4], regex, 1)
            if numbers_exist is not None:
                num_numeric += 1

        if num_pages > 1:
            try:
                all_text = doc[1].get_text_words(flags=pymupdf.TEXTFLAGS_WORDS)  # get words from second page
            except Exception as _:
                all_text = []

            for each in all_text:
                numbers_exist = match_compiled_regex(each[4], regex, 1)
                if numbers_exist is not None:
                    num_numeric += 1

            no_of_words += len(all_text)

        # no of words should atleast be 50 or no of numeric words should atleast 5
        if (no_of_words < 50) or (num_numeric < 5):
            keywords_check = get_account_key_words(doc)
            if keywords_check["all_present"]:
                return False
            return True
        return False

    return True


def check_invalid(path, password):
    """
    Check if given fitz document is a valid document
    :param: path (pdf file path), password (for pdf file)
    :return: bool indicating whether given file is a valid file or not
    """
    doc = read_pdf(path, password)

    if isinstance(doc, int):
        # password invalid or couldn't parse pdf
        return True

    if check_if_image(doc):
        return True

    return False


def extract_essential_identity(path, bank, password, preshared_names=[], template={}, country='IN'):
    """
    Extracts essential identity information for a given pdf file
    :param: path (path to pdf file), bank and password (for pdf file), preshared_names (list of strings)
    :return: dict having identity info, metadata fraud, date range, etc
    """

    identity_dict = dict()  # stores identity info
    result_dict = dict()  # stores final dictionary to return

    doc = read_pdf(path, password)

    if isinstance(doc, int):
        if doc == -1:
            # file doesn't exists or is not a valid pdf file
            result_dict['is_image'] = True
        else:
            # password is incorrect
            result_dict['password_incorrect'] = True
        return result_dict
    
    result_dict['page_count'] = doc.page_count

    if check_if_image(doc):
        result_dict['is_image'] = True
        return result_dict
    
    od_keywords = template.get('od_keywords',[])

    page = doc.load_page(0)
    # open json file containing information retrieval data
    if template.get('accnt_bbox') and template:
        data = template
        # print("Received template from server: ", data)
    else:
        try:
            # capture_message("Did not receive Identity bbox for bank {}".format(bank))
            print("Did not receive Identity bbox set for bank {}".format(bank))
        except Exception as e:
            print(e)

        file_path = 'library/bank_data/'+bank+'.json'

        if os.path.exists(file_path):
            with open(file_path, 'r') as data_file:
                try:
                    data = json.load(data_file)
                except ValueError:
                    print("Invalid JSON file\nPlease check")
                    data = {}
                except Exception as e:
                    print("Error loading file\nPlease check", e)
                    data = {}
                finally:
                    data_file.close()
        else:
            print("Incorrect bank name")
            data = {}

    # get identity information
    identity_dict['account_number'], account_number_template_uuid, account_number_page_number = get_account_num(doc, data.get('accnt_bbox', []), bank, path=path)
    identity_name, name_template_uuid, name_page_number = get_name(doc, data.get('name_bbox', []), bank, path=path)
    identity_dict['name'] = identity_name
    identity_dict['address'], address_template_uuid, address_page_number = get_address(doc, data.get('address_bbox', []), path=path)
    identity_dict['ifsc'], ifsc_template_uuid = get_ifsc(doc, bank, data.get('ifsc_bbox', []), path=path)
    identity_dict['micr'], micr_template_uuid = get_micr(doc, bank, data.get('micr_bbox', []), path=path)
    identity_dict['currency'], currency_template_uuid = get_currency(doc, bank, data.get('currency_bbox', []), path=path)
    identity_dict['account_opening_date'] = get_account_opening_date(doc, bank, data.get('account_opening_date_bbox', []), path=path)
    identity_dict['account_category'], identity_dict['raw_account_category'], account_category_template_uuid = get_account_category(doc, bank, data.get('account_category_bbox', []), data.get('account_category_mapping', {}), path=path)
    identity_dict['phone_number'], phone_number_template_uuid, phone_number_page_number = get_generic_text_from_bank_pdf(doc, data.get('phone_number_bbox', []), False, 'phone_number_bbox')
    identity_dict['pan_number'], pan_card_template_uuid, pan_card_page_number = get_generic_text_from_bank_pdf(doc, data.get('pan_number_bbox', []), False, 'pan_number_bbox')
    identity_dict['email'], email_template_uuid, email_page_number = get_generic_text_from_bank_pdf(doc, data.get('email_bbox', []), False, 'email_bbox')
    identity_dict['dob'], dob_template_uuid, dob_page_number = get_generic_text_from_bank_pdf(doc, data.get('dob_bbox', []), False, 'dob_bbox')
    identity_dict['joint_account_holders'], joint_account_holders_template_uuid = get_joint_account_holders_name(doc, data.get('joint_account_holders_regex', []), bank, False, None)

    # Update account category to corporate if name ends with company name
    company_end_keywords_list = template.get('company_end_keywords_list', [])
    if identity_dict['account_category'] is None and identity_name:
        is_corporate, corporate_keyword = check_company_name(identity_name, country, company_end_keywords_list=company_end_keywords_list)
        if is_corporate:
            identity_dict['account_category'] = 'corporate'
            identity_dict['raw_account_category'] = f'COMPANY_END_KEYWORDS__{corporate_keyword}'
 
    # Check for OD Accounts
    identity_dict['credit_limit'], is_credit_limit, credit_limit_template_uuid = get_credit_limit(doc, data.get('limit_bbox', []), bank, path=path)
    identity_dict['od_limit'], is_od_limit_present, od_limit_template_uuid  = get_od_limit(doc, data.get('od_limit_bbox', []), path=path)
    identity_dict['is_od_account'], od_account_template_uuid = is_od_account_check(doc, data.get('is_od_account_bbox', []), path=path, od_keywords=od_keywords)
    
    result_dict['identity'] = identity_dict
    # if bank == 'india_post':
    #     pass
    # else:
    # removed india_post keywords bypass check
    result_dict['keywords'] = get_account_key_words(doc)
    if bank in ['phonepe_bnk']:
        if result_dict['keywords']['date_present'] and result_dict['keywords']['amount_present']:
            result_dict['keywords']['balance_present'] = True
            result_dict['keywords']['all_present'] = True
    window_dict = {
        'rbl' : 3.1,
        'crsucbnk' : 2.2,
        'lokmangal_multistate' : 2.2,
        'jankalyan_co_op_bnk' : 2.2,
        'dhanlaxmi' : 3.4,
        'mahalaxmi_co_op': 2.2,
        'thane': 2.4,
        'municipalbnk': 2.2,
        'idbi': 2.2,
        'sharad': 2.2,
        'model': 2.5,
        'karur': 2.2,
        'suratbnk': 3.2,
        'sadhana': 2.3,
        'umiya_urban_co_op': 2.1,
        'nagarik_sahakari': 2.1,
        'jantasahakari': 2.1,
        'bhandara_dccb': 2.1
    }
    window = 2
    window = window_dict.get(bank, window)
    result_dict['keywords_in_line'] = are_all_keywords_in_line(path, password, doc, window)
    if bank in ['dbsbnk', 'phonepe_bnk']:
        result_dict['keywords_in_line'] = True
    # get date range
    result_dict['date_range'], date_range_template_uuid = get_date_range(doc, bank, data.get('date_bbox', []), get_only_all_text=False, path=path)
    result_dict['opening_date'], opening_date_template_uuid = get_opening_date(doc, bank, data.get('opening_date_bbox', []), path=path)
    result_dict['opening_bal'], opening_bal_template_uuid = get_opening_closing_bal(doc, data.get('opening_bal_bbox', []), path=path, bank=bank)
    result_dict['closing_bal'], closing_bal_template_uuid  = get_opening_closing_bal(doc, data.get('closing_bal_bbox', []), path=path, bank=bank) 
    
    templates_used = {}
    templates_used['account_number_template_uuid'] = account_number_template_uuid
    templates_used['name_template_uuid'] = name_template_uuid
    templates_used['address_template_uuid'] = address_template_uuid
    templates_used['ifsc_template_uuid'] = ifsc_template_uuid
    templates_used['micr_template_uuid'] = micr_template_uuid
    templates_used['currency_template_uuid'] = currency_template_uuid
    templates_used['account_category_template_uuid'] = account_category_template_uuid
    templates_used['credit_limit_template_uuid'] = credit_limit_template_uuid
    templates_used['od_limit_template_uuid'] = od_limit_template_uuid
    templates_used['od_account_template_uuid'] = od_account_template_uuid
    templates_used['date_range_template_uuid'] = date_range_template_uuid
    templates_used['opening_date_template_uuid'] = opening_date_template_uuid
    templates_used['opening_bal_template_uuid'] = opening_bal_template_uuid
    templates_used['closing_bal_template_uuid'] = closing_bal_template_uuid
    templates_used['joint_account_holders_template_uuid'] = joint_account_holders_template_uuid

    result_dict['templates_used'] = templates_used

    # # check for metadata fraud
    # is_fraud, error, doc_metadata_dict = get_metadata_fraud(doc, bank,path,password)
    result_dict['is_fraud'] = False
    result_dict['fraud_type'] = None
    # result_dict["doc_metadata"] = doc_metadata_dict


    #
    # if (bank == 'andhra' or bank == 'mahabk') and  identity_dict['account_number'] == None:
    #     identity_dict['account_number'] = get_account_number_pdf_tables(path,bank)

    # identity for rotated banks
    banks = ['ausfbnk', 'mahabk', 'uco', 'andhra',
             'saraswat', 'ubi', 'rbl', 'indusind', 'sadhana', 'adarsh', 'district_co_op', 'jansewa_urban', 'jana', 'danamon']
    if bank in banks and page.derotation_matrix[5] != 0 and (identity_dict['account_number'] == None or identity_dict['name'] == None):
        # print("Rotated Case")
        pdf = pdfplumber.open(path, password=password)
        p0 = pdf.pages[0]
        text = p0.extract_text()
        text_remove = text.translate(
            {ord(c): " " for c in "!@#$%^&*()[]{};:,./<>?\|`~-=_+"}).lower()
        print("text: ", text_remove)
        try:
            if bank == "ausfbnk":
                name = re.search(r'name\s*(.*?)\s*statement',
                                 text_remove).group(1)
                account_number = re.search(
                    r'\s*account\s*number\s*(.*?)\s*[a-z]', text_remove).group(1)
                from_date, _ = check_date(
                    re.search(r'from\s*(.*?)\s*to', text_remove).group(1))
                to_date, _ = check_date(
                    re.search(r'to\s*(.*?)\n\s*joint', text_remove).group(1))
            elif bank == "mahabk":
                name = re.search(
                    r'account\s*details\s*(.*?)branch', text_remove).group(1)
                account_number = re.search(
                    r'\s*account\s*no\s*(.*?)\s*[a-z]', text_remove).group(1)
                from_date, _ = check_date(re.search(r'statement.*from(.*?)to.*', text_remove).group(1))
                to_date, _ = check_date(re.search(r'statement.*to(.*)\s*\n.*', text_remove).group(1))
            elif bank == "uco":
                name = re.search(r'name\s*(.*?)\n', text_remove).group(1)
                account_number = re.search(
                    r'a\s*c\s*no\s*(.*?)\n', text_remove).group(1)   
            elif bank == "andhra":
                name = re.search(r'name\s*(.*?)sol', text_remove).group(1)
                account_number = re.search(
                    r'account\s*no\s*(.*?)\s*ifsc', text_remove).group(1)
            elif bank == "saraswat":
                name = re.search(r'\nname\s*(.*?)\naddress', text_remove).group(1)
                account_number = re.search(r'15\s*digit\s*account\s*id\s*(.*?)\s*nomination\s*', text_remove).group(1)
            elif bank == "ubi":
                name = re.search(r'account\s*(.*?)\s*union',
                                 text_remove).group(1)
                account_number = re.search(
                    r'a\s*c\s*number\s*(.*?)\s*city', text_remove).group(1)
            elif bank == "rbl":
                name = re.search(
                    r'account\s*name\s*([A-Za-z\s]+)\s*address.*', text_remove).group(1)
                account_number = re.search(
                    r'account\s*number\s*([0-9]+)', text_remove).group(1)
            elif bank == "indusind":
                name = re.search(
                    r'account\s*statement\n(.*?)\n*customer', text_remove).group(1)    
                if name == None or name == "":        
                    name = re.findall(re.compile('.*customer name(.*)\n.*'),text_remove)[0]
                account_number = re.search(
                    r'account\s*no\s*([0-9]+)', text_remove).group(1)
            elif bank == "sadhana":
                name = re.search(
                    r'a\s*c\s*name\s*(.*?)\s*\n*address', text_remove).group(1)
                account_number = re.search(
                    r'a\s*c\s*no\s*([0-9]+)\s*a', text_remove).group(1)
                from_date, _ = check_date(re.search(r'[\s\S]+from\s*date\s*([0-9]{1,2} [0-9]{1,2} [0-9]{2,4})\s*to', text_remove).group(1))
                to_date, _ = check_date(re.search(r'[\s\S]+to\s*date\s*([0-9]{1,2} [0-9]{1,2} [0-9]{2,4}).*', text_remove).group(1))     
            elif bank == 'adarsh':
                name = re.search(r'.*name\s*(.*?)\s*\n*account', text_remove).group(1)
                account_number = re.search(r'ref\s*a\s*c\s*no\s*:?\s*([0-9]{8,})\s*\n*ifsc', text_remove).group(1)
                from_date, _ = check_date(re.search(r'.*period\s*(.*)\s*to.*', text_remove).group(1))
                to_date, _ = check_date(re.search(r'.*to\s*(.*)\s*\n*', text_remove).group(1))
            elif bank == 'district_co_op':
                name = re.search(r'[\s\S]+account\s*no[\s\S]+?name\s*(.*)\s*generated.*', text_remove).group(1)
                account_number = re.search(r'[\s\S]+account\s*no\s*([0-9]{8,}).*', text_remove).group(1)
                from_date, _ = check_date(re.search(r'[\s\S]+statement\s*of\s*account\s*from\s*([0-9]{1,2} [0-9]{1,2} [0-9]{2,4})\s*to.*', text_remove).group(1))
                to_date, _ = check_date(re.search(r'[\s\S]+statement\s*of\s*account\s*from.*to\s*([0-9]{1,2} [0-9]{1,2} [0-9]{2,4}).*', text_remove).group(1))
            elif bank == 'jansewa_urban':
                name = re.search(r'[\s\S]+gl\s*name[\s\S]+?a\s*c\s*name\s*(.*)\s*address.*', text_remove).group(1)
                account_number = re.search(r'[\s\S]+gl\s*name[\s\S]+?a\s*c\s*no\s*([0-9]{8,}).*', text_remove).group(1)
                from_date, _ = check_date(re.search(r'[\s\S]+from\s*date\s*([0-9]{1,2} [0-9]{1,2} [0-9]{2,4})\s*to.*', text_remove).group(1))
                to_date, _ = check_date(re.search(r'[\s\S]+from\s*date.*to\s*date\s*([0-9]{1,2} [0-9]{1,2} [0-9]{2,4}).*', text_remove).group(1))
            elif bank == 'jana':
                name = re.search(r'(.*)\s*crn.*', text_remove).group(1)
                account_number = re.search(r'[\s\S]+account\s*number\s*([0-9]{8,}).*', text_remove).group(1)
                from_date, _ = check_date(re.search(r'[\s\S]+statement\s*period\s*([0-9]{1,2} [0-9]{1,2} [0-9]{2,4})\s*to.*', text_remove).group(1))
                to_date, _ = check_date(re.search(r'[\s\S]+statement\s*period.*to\s*([0-9]{1,2} [0-9]{1,2} [0-9]{2,4}).*', text_remove).group(1))
                ifsc = re.search(r'[\s\S]+ifsc\s*code\s*([0-9a-z]+).*', text_remove).group(1)
                micr = re.search(r'[\s\S]+micr\s*code\s*([0-9]+).*', text_remove).group(1)
            elif bank == 'danamon':
                name = re.search(r'[\s\S]+?name\s*(.*?)\s*print\s*date', text_remove).group(1)
                account_number = re.search(r'[\s\S]+?account\s*number\s*([0-9]+)\s*period', text_remove).group(1)
            
            identity_dict['name'] = name
            identity_dict['account_number'] = account_number
            
            extracted_using_old_method = False
            if identity_dict.get('ifsc', None) != None:
                identity_dict['ifsc'] = ifsc
                extracted_using_old_method = True
            if identity_dict.get('micr', None) != None:
                identity_dict['micr'] = micr
                extracted_using_old_method = True
            
            if (from_date != None and to_date != None and result_dict.get('date_range',dict()).get('from_date', None) != None
            and result_dict.get('date_range',dict()).get('to_date', None) != None):
                result_dict['date_range'] = {'from_date': from_date.strftime(
                    "%Y-%m-%d"), 'to_date': to_date.strftime("%Y-%m-%d")}
            if extracted_using_old_method:
                print("Rotated statement using old method for ".format(path))
        except Exception as e:
            print(e)

    # calculating metadata matches
    metadata_name_matches = get_metadata_name_matches(doc, preshared_names, identity_name)

    result_dict["metadata_analysis"] = dict()
    result_dict["metadata_analysis"]["name_matches"] = metadata_name_matches
    print("result_dict", result_dict)

    if bank == 'federal':
        # use plumber in case of federal
        result_dict_plumber = extract_essential_identity_plumber(path, bank, password)
        doc = read_pdf(path, password)
        result_dict_plumber['is_fraud'] = False
        result_dict_plumber['fraud_type'] = None

        if result_dict['identity']['ifsc'] == None:
            result_dict['identity']['ifsc'] = result_dict_plumber.get('identity', dict()).get('ifsc', None)
        if result_dict['identity']['micr'] == None:
            result_dict['identity']['micr'] = result_dict_plumber.get('identity', dict()).get('micr', None)
        if result_dict['identity']['account_number'] == None:
            result_dict['identity']['account_number'] = result_dict_plumber.get('identity', dict()).get('account_number', None)
        if result_dict['identity']['name'] == None:
            result_dict['identity']['name'] = result_dict_plumber.get('identity', dict()).get('name', None)
        if result_dict['identity']['address'] == None:
            result_dict['identity']['address'] = result_dict_plumber.get('identity', dict()).get('address', None)
        if result_dict['identity']['account_category'] == None:
            result_dict['identity']['account_category'] = result_dict_plumber.get('identity', dict()).get('account_category', None)

    return result_dict


def get_metadata_name_matches(doc, preshared_names, extracted_name=""):
    """
    This method takes in preshared names (list of strings) and returns a dict object for name matches
    """
    final_name_match_result = []
    cleaned_combined_page_text = "" if extracted_name is None else extracted_name

    for name in preshared_names:
        temp_match_result = dict()

        name = re.sub("[^A-Za-z ]+", "", name).upper()

        _, best_full_match_score = process.extractOne(
            cleaned_combined_page_text, [name], scorer=fuzz.partial_token_set_ratio)
        temp_match_result["name"] = name
        temp_match_result["matches"] = best_full_match_score > 90
        temp_match_result["score"] = best_full_match_score
        temp_match_result["tokenized_matches"] = []

        # check if separated score is required
        splitted_name_list = name.split()
        if len(splitted_name_list) > 1:
            tokenized_matches_list = []
            for splitted_name in splitted_name_list:
                splitted_match_score = fuzz.partial_token_set_ratio(
                    cleaned_combined_page_text, splitted_name)
                temp_tokenized_result = dict()
                temp_tokenized_result["token"] = splitted_name
                temp_tokenized_result["matches"] = splitted_match_score > 90
                temp_tokenized_result["score"] = splitted_match_score
                tokenized_matches_list.append(temp_tokenized_result)

            temp_match_result["tokenized_matches"] = tokenized_matches_list

        final_name_match_result.append(temp_match_result)

    # print(final_name_match_result)
    return final_name_match_result

def are_all_keywords_in_line(path, password, doc, window=2):
    from library.get_edges_test import get_words_with_boxes
    max_page_count = 5
    all_keywords_flag = False
    for page_num in range(0, min(doc.page_count, max_page_count)):
        words = get_words_with_boxes(path, password, page_num)
        sorted_words = sorted(words, key=lambda x: x['top'])
        sum_word_height = 0
        avg_word_height = 0
        count_rows = 0
        distance_between_rows = 0
        avg_distance_between_rows = 0
        for i in range(len(sorted_words)):
            h = sorted_words[i].get('bottom') - sorted_words[i].get('top')
            sum_word_height += h
            if (i < len(sorted_words) - 1) and (sorted_words[i].get('bottom') < sorted_words[i+1].get('top')):
                distance_between_rows += sorted_words[i+1].get('top') - sorted_words[i].get('bottom')
                count_rows += 1
        
        if count_rows > 0:
            avg_distance_between_rows = distance_between_rows/count_rows

        if len(sorted_words) > 0:
            avg_word_height = sum_word_height/len(sorted_words)
    
        height = window * avg_word_height + (window - 1) * avg_distance_between_rows
        # sliding window approach
        prev_top = -1
        i = 0
        while i < len(sorted_words):
            text_list = []
            while i < len(sorted_words) and prev_top == sorted_words[i].get('top'):
                i += 1
            if i < len(sorted_words):
                prev_top = sorted_words[i].get('top') 
                tmp_bot = sorted_words[i].get('top') + height
            
            for j in range(i, len(sorted_words)):
                if sorted_words[j].get('bottom') <= tmp_bot:
                    text_list.append(sorted_words[j].get('text'))
                else:
                    break
            keywords_result = keyword_helper(' '.join(text_list))
            if keywords_result.get('all_present'):
                all_keywords_flag = True
                return all_keywords_flag
            
    return all_keywords_flag

def get_account_key_words(doc):

    # FOR NCB BANK - a hack
    # "ﺰﻣﺭﻦﺋﺍﺩﻦﻳﺪﻣﺪﻴﺻﺮﻟﺍ ﺦﻳﺭﺎﺘﻟﺍﻞﻴﺻﺎﻔﺘﻟﺍﺕﺎﻈﺣﻼﻣﻊﺟﺮﻤﻟﺍﺔﻴﻠﻤﻌﻟﺍ" -> this contains words for balance, amount and date
    # FOR AL RAJHI BANL - a hack
    # "ﺪﻴﺻﺮﻟﺍﻦﻳﺪﻣﻦﺋﺍﺩﺔﻴﻠﻤﻌﻟﺍ ﻞﻴﺻﺎﻔﺗﻱﺩﻼﻴﻤﻟﺍ ﺦﻳﺭﺎﺘﻟﺍ" -> contains words for balance, amount and date
    # "ﺦﻳﺭﺎﺘﻟﺍﻦﺋﺍﺩﻦﻳﺪﻣﺔﻠﻤﻌﻟﺍﺔﻴﻠﻤﻌﻟﺍ" -> contains words for balance, amount
    # "ﻒﺻﻮﻟﺍﻱﺩﻼﻴﻤﻟﺍ" -> contains words for date only

    # only provide small case words
    amount_words = AMOUNT_WORDS
    balance_word = BALANCE_WORD
    date_word = DATE_WORD

    max_page_count = 4
    all_present = False
    amount_present_ever = False
    balance_present_ever = False
    date_present_ever = False
    for page_number in range(0, min(doc.page_count, max_page_count)):
        all_text = get_text_in_box(doc[page_number], [0, 0, 5000, 5000])
        result = keyword_helper(all_text)
        if result.get("all_present",False):
            all_present = True
        if result.get("balance_present",False):
            balance_present_ever = True
        if result.get("date_present",False):
            date_present_ever = True
        if result.get("amount_present",False):
            amount_present_ever = True
    return {
        "amount_present": amount_present_ever,
        "balance_present": balance_present_ever,
        "date_present": date_present_ever,
        "all_present": all_present
    }


def check_word_presence(all_text, list_words):
    for each_word in list_words:
        if each_word in all_text:
            return True
    return False


def get_account_number_pdf_tables(path, bank):
    # returns account from csv
    # csv created using pdf_tables_api

    pdf_tables_api_key = get_api_key_pdf_tables()
    pdf_tables = pdftables_api.Client(pdf_tables_api_key)
    csv_table_file_name = "/tmp/{}.csv".format(str(uuid.uuid4()))
    pdf_tables.csv(path, csv_table_file_name)

    csv_data = []
    try:
        with open(csv_table_file_name, "r") as csv_file:
            reader = csv.reader(csv_file)
            csv_data = list(reader)
    except Exception as e:
        print(e)
    account_number = ''
    for i, column in enumerate(csv_data[:10]):
        if len(column) and bank == 'andhra' and column[0] == 'ACCOUNT NO':
            if len(column[2]):
                account_number = column[2]
            break
        if len(column) and bank == 'mahabk':
            temp = column[3].split(':')
            # print(temp)
            temp1 = column[4].split(":")
            if temp[0].strip() == 'Account No':
                account_number = temp[1].strip()
                break
            elif temp1[0].strip() == 'Account No':
                account_number = temp1[1].strip()
                break
    if os.path.exists(csv_table_file_name):
        os.remove(csv_table_file_name)
    print('---', account_number)
    return account_number


def get_api_key_pdf_tables():
    # only used for roated pdf
    # Check and give api_key for pdf_tables

    items = collect_results(api_keys_table.scan, {})
    api_keys = [item['api_key'] for item in items]
    api_keys.sort()

    try:
        key_1 = api_keys[0]
        key_2 = api_keys[1]
        URL = "https://pdftables.com/api/remaining"
        PARAMS = {'key': key_1}
        print(PARAMS)

        # unused codepath, suppressing this vulnerability
        res = requests.get(url=URL, params=PARAMS) # nosec
        print('page_limit_left', res.text)
        if int(res.text) > 15:
            print('key_to_send', key_1)
            return key_1
        else:
            # Delete key_1
            api_keys_table.delete_item(Key={'api_key': key_1})

            print('key_to_send', key_2)
            return key_2
    except Exception as e:
        print(e)
        return ''


def get_page_hash(page_string: str):
    hash_object = hashlib.sha512(page_string.encode())
    hash_digest = hash_object.hexdigest()
    # print("hash created: {}".format(hash_digest))
    return hash_digest


def get_pdf_page_hashes_with_page_text(file_path, password=None):
    """
    This methods returns a dict
    key -> hash
    value -> page text (uncleaned & raw)
    for all the pages in a pdf file
    """
    pdf_page_hashes_with_text = {}

    doc = fitz.Document(file_path)
    if doc.needs_pass:
        is_password_correct = doc.authenticate(password=password) != 0
        if not is_password_correct:
            print("the password was incorrect")
            return

    for page_number in range(doc.page_count):
        page_text = doc[page_number].get_text()
        # cleaned_page_text = page_text.translate({ord(c): " " for c in "!@#$%^&*()[]{};:,./<>?\|`~-=_+\n"}).lower()
        page_hash = get_page_hash(page_text)
        pdf_page_hashes_with_text[page_hash] = page_text

    return pdf_page_hashes_with_text

def get_od_limit(doc, bbox, path=None, get_only_all_text=False):
    for page_number in range(0, min(doc.page_count, 3)):
        for template in bbox:
            if template.get("image_flag",False):
                continue
            
            od_limit, is_od_account = get_temp_od_limit(doc[page_number], template , get_only_all_text=get_only_all_text)
            if get_only_all_text:
                return od_limit, True, None
            if od_limit is not None and od_limit != '':
                return od_limit, is_od_account, template.get('uuid')
    return None, False, None

def get_opening_closing_bal(doc, bbox, path=None, get_only_all_text=False, bank=None):
    max_page_count = 3
    total_pages = doc.page_count
    pages_to_see = [*range(min(total_pages, max_page_count))]
    if bank in ['yesbnk','hdfc'] and total_pages>1:
        pages_to_see += [*range(total_pages-2,total_pages)]
    pages_to_see = sorted(list(set(pages_to_see)))
    for page_number in pages_to_see:
        for template in bbox:
            if template.get("image_flag",False):
                continue
            
            opening_bal = get_tmp_bal(doc[page_number], template, get_only_all_text=get_only_all_text)
            if get_only_all_text:
                return opening_bal, None
            
            if isinstance(opening_bal, str) and opening_bal.endswith('-'):
                opening_bal = '-' + opening_bal[:-1]
            
            if isinstance(opening_bal, str) and amount_to_float(opening_bal) is not None:
                sign = get_amount_sign(opening_bal)
                amount = amount_to_float(opening_bal)
                if sign is not None and amount is not None:
                    opening_bal = str(sign * amount)
                    return opening_bal, template.get('uuid')
                if amount:
                    opening_bal = str(amount)
                    return opening_bal, template.get('uuid')
    return None, None

def get_tmp_bal(page, template, get_only_all_text = False):
    opening_bal = None
    all_text = get_text_in_box(page, template.get('bbox'))

    if all_text is not None:
        all_text = all_text.replace('\n', '').replace('(cid:9)', '')
   
    regex = template.get('regex')
    all_text = remove_unicode(all_text)

    if get_only_all_text:
        return all_text

    if regex is not None and all_text is not None:
        regex_match = re.match(regex, all_text)

        if regex_match is not None:
            opening_bal = regex_match.group(1)

            if type(opening_bal) == str and opening_bal is not None:
                opening_bal = opening_bal.replace(',', '')
                try:
                    opening_bal = str(opening_bal)
                except:
                    return None
            if opening_bal != None and opening_bal != '':
                return opening_bal
    return None

def get_temp_od_limit(page, template, get_only_all_text=False):
    od_limit = None
    is_od_account = False
    all_text = get_text_in_box(page, template.get('bbox'))

    if all_text is not None:
        all_text = all_text.replace('\n', '').replace('(cid:9)', '')

    regex = template.get('regex')
    all_text = remove_unicode(all_text)

    if get_only_all_text:
        return all_text, None

    if regex is not None and all_text is not None:
        regex_match = re.match(regex, all_text)
        if regex_match is not None:
            od_limit = regex_match.group(1) 
            if type(od_limit) == str and od_limit is not None:
                od_limit = od_limit.replace(',', '')
                try:
                    od_limit = Decimal(str(od_limit))
                except:
                    print("od_limit bbox didn't convert str to decimal") 
                    return None, False    
            if od_limit != None and od_limit > 0:
                is_od_account = True

    # print("\n\"", all_text, "\" -->",regex, "-->", od_limit)
    return od_limit, is_od_account

# "Overdraft", "Overdraft Account
def is_od_account_check(doc, bbox, path=None, get_only_all_text=False, od_keywords = []):
    for page_number in range(0, min(doc.page_count, 2)):
        for template in bbox:
            if template.get("image_flag",False):
                continue
            is_od_account = get_temp_od_account(doc[page_number], template, get_only_all_text=get_only_all_text, od_keywords=od_keywords)
            if get_only_all_text:
                return is_od_account, None
            if is_od_account is not None and is_od_account != '' and is_od_account!=False:
                return is_od_account, template.get('uuid')
    return None, None

# change the list name
def get_temp_od_account(page, template, get_only_all_text=False, od_keywords = []):
    is_od_account = False
    all_text = get_text_in_box(page, template.get('bbox'))

    if all_text is not None:
        all_text = all_text.replace('\n', '').replace('(cid:9)', '')

    regex = template.get('regex')
    all_text = remove_unicode(all_text)

    if get_only_all_text:
        return all_text
    
    if regex is not None and all_text is not None:
        regex_match = re.match(regex, all_text)
        if regex_match is not None:
            regex_match_group = regex_match.group(1) 
            
            if regex_match_group.upper() in [x.upper() for x in od_keywords] or 'overdraft' in regex_match_group.lower():
                is_od_account = True
            # if regex_match_group in od_accounts_list or 'overdraft' in regex_match_group.lower():
            #     is_od_account = True
                  
    # print("\n\"", all_text, "\" -->",regex, "-->", is_od_account)
    return is_od_account
