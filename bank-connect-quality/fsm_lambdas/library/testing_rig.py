################################################################################
# call these functions in python interpreter outside fsmlib folder             #
################################################################################


import json
import time
import re
from library.fitz_functions import read_pdf, get_text_in_box, get_metadata_fraud
import os
import warnings
import pandas as pd
import boto3
import datetime
import time
from library.merchant_category import get_merchant_category_dict
from library.transaction_channel import get_merchant_category
# from library.lender_list import check_loan, check_loan_unclean
from library.lender_list import check_loan
from library.excel_report.rolling_monthly.rolling_monthly_analysis import rolling_month_analysis_func
from library.transactions import get_transactions_finvu_aa
from library.finvu_aa_inconsistency_removal import *
from library.fraud import transaction_balance_check, optimise_transaction_type, optimise_refund_transactions, process_merged_pdf_transactions, get_correct_transaction_order, solve_merged_or_jumbled_transactions
from library.salary import *
from uuid import uuid4

from library.utils import get_account_wise_transactions_dict
from python.api_utils import call_api_with_session


warnings.simplefilter(action = "ignore", category = FutureWarning)
pd.options.mode.chained_assignment = None


BASE_URL = "https://apis.bankconnect.finbox.in"
HEADERS = {
        'x-api-key': "<API_KEY>"
    }

def get_templates_from_quality(bank, template_type):
    # login into quality_prod and get the token. the credentials are requester only.
    url = "https://bankconnectqualityapis.finbox.in/api/token"
    payload = 'username=<USERNAME>&password=<PASSWORD>'
    headers = {'Content-Type': 'application/x-www-form-urlencoded'}
    response = call_api_with_session(url, "POST", payload, headers).json()
    token = response.get("access_token")

    # now call the get_available_templates for this bank and template_type
    url = f"https://bankconnectqualityapis.finbox.in/template/available_templates?bank_name={bank}&template_type={template_type}"
    headers = {
        'Authorization': 'Bearer '+token,
        'Content-Type': 'application/json'
    }
    template_response = call_api_with_session(url, "GET",payload, headers).json()
    templates = template_response.get("templates")
    
    # sort the templates on the basis of priority
    templates = sorted(templates, key=lambda d: d['priority'])
    
    final_templates = []
    for i in templates:
        template_json = i.get("template_json")
        template_json['uuid'] = i.get("template_uuid")
        final_templates.append(template_json)
    
    return final_templates

def get_everything_from_server(bank_name, country="IN"):
    url = BASE_URL + f"/bank-connect/v1/internal/get_enrichment_regexes/?type=all&bank_name={bank_name}&country={country}"
    response = call_api_with_session(url,"GET", None, HEADERS).json()
    return response

def check_and_get_everything(bank_name, country="IN"):
    print(f"Trying to get the data for bank_name : {bank_name} and country: {country}")
    files_to_check = [f"/tmp/merchant_category_{country}.json", f"/tmp/lender_list_{country}.json", f"/tmp/lender_regex_list_{country}.json", f"/tmp/transaction_channel_{bank_name}_{country}.json", f"/tmp/unclean_merchant_{bank_name}_{country}.json", f"/tmp/description_{country}.json"]
    
    all_present = True
    for items in files_to_check:
        if not os.path.exists(items):
            all_present = False
            break
    
    if all_present:
        print("Everything that is needed, is here. No need to get anymore.")
        return
    
    print("Getting all from the server, and caching everything in /tmp")

    if not all_present:
        # calling the api, getting the data and caching everything
        data_from_the_server = get_everything_from_server(bank_name, country)
    
    with open(f"/tmp/merchant_category_{country}.json", "w") as outfile:
        outfile.write(json.dumps(data_from_the_server.get('merchant_category', {}), indent=4))
        print(f"cached merchant category for {country}")
    
    with open(f"/tmp/lender_list_{country}.json", "w") as outfile:
        outfile.write(json.dumps(data_from_the_server.get('lender_list', {}).get('lenders', []), indent=4))
        print(f"cached lender list for {country}")

    with open(f"/tmp/lender_regex_list_{country}.json", "w") as outfile:
        outfile.write(json.dumps(data_from_the_server.get('lender_regex_list', []), indent=4))
        print(f"cached lender regex list for {country}")
    
    transaction_channel_data = data_from_the_server.get('transaction_channel', {})
    debit_channel_dict = transaction_channel_data.get("debit", {})
    debit_priority_order = list(debit_channel_dict.keys())
    credit_channel_dict = transaction_channel_data.get("credit", {})
    credit_priority_order = list(credit_channel_dict.keys())

    final_data = {
        "debit_channel_dict" : debit_channel_dict,
        "debit_priority_order" : debit_priority_order,
        "credit_channel_dict" : credit_channel_dict,
        "credit_priority_order" : credit_priority_order
    }

    with open(f"/tmp/transaction_channel_{bank_name}_{country}.json", "w") as outfile:
        outfile.write(json.dumps(final_data, indent=4))
        print(f"cached transaction channel for {bank_name} and country {country}")
    
    unclean_merchant_regex_data = data_from_the_server.get('unclean_merchant')
    merchant_debit_regex_list = unclean_merchant_regex_data.get("debit")
    merchant_credit_regex_list = unclean_merchant_regex_data.get("credit")

    final_data = {
        "merchant_debit_regex_list": merchant_debit_regex_list,
        "merchant_credit_regex_list": merchant_credit_regex_list
    }
    
    with open(f"/tmp/unclean_merchant_{bank_name}_{country}.json", "w") as outfile:
        outfile.write(json.dumps(final_data, indent=4))
        print(f"cached unclean merchant for {bank_name} and country {country}")
    
    with open(f"/tmp/description_{country}.json", "w") as outfile:
        outfile.write(json.dumps(data_from_the_server.get('description'), indent=4))
        print(f"cached description for {country}")


def test_transactions(path, bank, local_logging_context=None, LOGGER=None, password='', page_num=0, name='', key='', bucket='', account_number='', trans_bbox=[], last_page_regex=[], country="IN"):
    """
    Prints the transactions extracted using fitz (and plumber in case of federal bank)
    :param: path_list (list of pdf files), bank, password (optional), page_num (optional, default is 0)

    Example script:

    from library.testing_rig import test_transactions
    path_list = ['/Users/shouryaps/Downloads/1.pdf']
    test_transactions(path_list, 'idfc')

    """
    start = time.time()
    key = str(uuid4())
    from library.transactions import get_transaction
    from library.fraud import transaction_balance_check
    def date_formatter(row):
        if not isinstance(row['date'], str):
            row['date'] = row['date'].strftime("%Y-%m-%d %H:%M:%S")
        return row
    
    doc = read_pdf(path, password)
    if isinstance(doc, int):
        print("unable to open pdf")
        return
    account_number = 'XXXXXXX2329'

    trans_bbox = get_templates_from_quality(bank, 'trans_bbox')
    last_page_regex = get_templates_from_quality(bank, 'last_page_regex')
    account_delimiter_regex = get_templates_from_quality(bank, 'account_delimiter_regex')
    check_and_get_everything(bank)
    print("==========================", time.time() - start)
    start = time.time()    
    transaction_input_payload = {
        'path': path,
        'bank': bank,
        'password': password,
        'page_number': page_num,
        'name': name,
        'key': key,
        'bucket': bucket,
        'number_of_pages': doc.page_count,
        'account_number': account_number,
        'trans_bbox': trans_bbox,
        'last_page_regex': last_page_regex,
        'account_delimiter_regex': account_delimiter_regex,
        'country': country,
        'extract_multiple_accounts': True
    }
    output_dict = get_transaction(transaction_input_payload, local_logging_context, LOGGER)
    transaction_list = output_dict.get('transactions', [])
    last_page = output_dict.get('last_page_flag')
    removed_opening_balance_date = output_dict.get('removed_opening_balance_date')
    removed_closing_balance_date = output_dict.get('removed_closing_balance_date')
    print(removed_opening_balance_date, removed_closing_balance_date)
    print("page number : {}, last page: {}".format(page_num, last_page))
    print("\n{} TRANSACTIONS For {}".format(len(transaction_list), path))
    transaction_list = list(map(date_formatter, transaction_list))
    print("IS FRAUD ----> ", transaction_balance_check(transaction_list))
    
    print("========================= total time taken ", time.time() - start)
    return transaction_list

def get_inconsistent_hash():
    data = {}
    path = '/Users/mayankagarwal/work/fsmlambdas/library/data.json'
    with open(path, 'r') as f:
        data = json.load(f)
    txns = data.get('transactions')
    for t in txns:
        t['optimizations'] = []
    with open(path, "w") as f:
        json.dump({"transactions":txns}, f)
    start = time.time()
    print("IS FRAUD ----> ", transaction_balance_check(txns))
    print(len(txns))
    txns, is_fraud, inconsistent_data = process_merged_pdf_transactions(txns)
    print(len(txns))
    print("is_fraud", is_fraud)
    print("=================", time.time() - start)
    path = '/Users/mayankagarwal/work/fsmlambdas/library/data2.json'
    with open(path, "w") as f:
        json.dump({"transactions":txns}, f)
    print(len(txns))
    print("=================")
    print("IS FRAUD ----> ", transaction_balance_check(txns))


def test_rotate_pdf(path, password=""):
    import fitz
    from library.fitz_functions import read_pdf
    doc = fitz.open()
    old_doc = read_pdf(path, "")
    for page in old_doc:
        print(page.rotationMatrix, page.derotation_matrix, page * page.rotationMatrix)
        page.setRotation(0)
    
    old_doc.save("/Users/karanbalani/Desktop/alrajhirotated.pdf")

def process_multi_account():
    path = '/Users/mayankagarwal/work/fsmlambdas/library/data2.json'
    with open(path, 'r') as f:
        data = json.load(f)
    txns = data.get('transactions')
    data = get_account_wise_transactions_dict(transaction_df=txns, identity_account_number='9812708692', update_account_number=True)

    for acc in data:
        txns = data[acc]
        print("IS FRAUD ----> ", transaction_balance_check(txns), f"for account {acc} with transctions {len(txns)}")

    path = '/Users/mayankagarwal/work/fsmlambdas/library/data3.json'
    with open(path, "w") as f:
        json.dump(data, f)

def test_get_all_transactions(path, bank, local_logging_context=None, LOGGER=None, password='',name='', key='', bucket='',account_number='', trans_bbox=[], last_page_regex=[], country='IN'):
    """
    Prints ALL the transactions extracted using fitz (and plumber in case of federal bank)
    :param: path_list (list of pdf files), bank, password (optional)

    Example script:

    from library.testing_rig import test_get_all_transactions
    path_list = ['/Users/shouryaps/Downloads/1.pdf']
    test_get_all_transactions(path_list, 'idfc')

    """
    from library.fitz_functions import read_pdf
    from library.transactions import get_transaction

    def date_formatter(row):
        row['date'] = row['date'].strftime("%Y-%m-%d %H:%M:%S")
        return row
    
    doc = read_pdf(path, password)
    if isinstance(doc, int):
        print("unable to open pdf")
        return
    
    list_of_all_txn_lists = []
    account_number = 'XXXXXXX2329'
    trans_bbox = get_templates_from_quality(bank, 'trans_bbox')
    last_page_regex = get_templates_from_quality(bank, 'last_page_regex')
    account_delimiter_regex = get_templates_from_quality(bank, 'account_delimiter_regex')
    check_and_get_everything(bank)
    transaction_input_payload = {
        'path': path,
        'bank': bank,
        'password': password,
        'name': name,
        'key': key,
        'bucket': bucket,
        'number_of_pages': doc.page_count,
        'account_number': account_number,
        'trans_bbox': trans_bbox,
        'last_page_regex': last_page_regex,
        'country': country,
        'account_delimiter_regex': account_delimiter_regex,
        'extract_multiple_accounts': True
    }
    
    transaction_list = []
    page_wise_transactions = []
    doc = read_pdf(path, password)  # gets fitz document object for number of pages
    if isinstance(doc, int):
        # password incorrect or file doesn't exist or file is not a pdf
        transaction_list = []
        print("incorrect password or file not found...")
        return
    
    num_pages = doc.page_count  # get the page count
    print("Total pages : ", num_pages)
    last_page_achieved = False
    for page_num in range (num_pages):
        transaction_input_payload['page_number'] = page_num
        transaction_output_payload = get_transaction(transaction_input_payload, None, None)
        transaction = transaction_output_payload.get('transactions', [])
        last_page = transaction_output_payload.get('last_page', False)
        print("page number : {}, last page: {}".format(page_num, last_page))
        if not last_page_achieved:
            transaction_list += transaction
            page_wise_transactions += [transaction]
            last_page_achieved = last_page
        # print("\n{} TRANSACTIONS For {}, page: {}\n".format(len(transaction), path, page_num))
    # print("\n{} TRANSACTIONS For {}".format(len(transaction_list), path))
    transaction_list = list(map(date_formatter, transaction_list))
    list_of_all_txn_lists.append(transaction_list)
    path = '/Users/mayankagarwal/work/fsmlambdas/library/data2.json'
    with open(path, "w") as f:
        json.dump({"transactions":transaction_list}, f)

    # salary_txns = get_salary_transactions(transaction_list, recurring_salary_flag=True)
    # json.dump(page_wise_transactions, open("all_transactions.json", "w"))
    # return transaction_list, salary_txns
    
    # return list_of_all_txn_lists
    # uncomment if out is needed as a json file
    # output_path = "some path"
    # with open(output_path + "all_txn_list.json", "w") as f:
    #     json.dump(list_of_all_txn_lists, f)

def get_transactions_list_of_lists_finvu_aa(transactions_list):
    transactions_list_of_lists = []

    if len(transactions_list) <= 30:
        transactions_list_of_lists.append(transactions_list)
    else:
        transactions_list_of_lists = [transactions_list[i:i+25] for i in range(0, len(transactions_list), 25)]

        last_transactions_list = transactions_list_of_lists[-1]

        # if last txn list has less than 10 txns and total batches are at least 2
        if len(last_transactions_list) < 10 and len(transactions_list_of_lists) >= 2:
            second_last_transactions_list = transactions_list_of_lists[-2]
            combined_last_and_second_last_transactions_list = second_last_transactions_list + last_transactions_list
            transactions_list_of_lists[-2] = combined_last_and_second_last_transactions_list
            transactions_list_of_lists = transactions_list_of_lists[:-1]

    for page_n in range(len(transactions_list_of_lists)):
        for index in range(len(transactions_list_of_lists[page_n])):
            transactions_list_of_lists[page_n][index]['page_number'] = page_n
    return transactions_list_of_lists

import warnings
warnings.filterwarnings("ignore")

def process_page(params):
    page_number = params['page_number']
    txns_list_of_lists = params['txns_list_of_lists']
    bank = params['bank']
    name = params['name']
    transaction, _ = get_transactions_finvu_aa(txns_list_of_lists[page_number], bank, name)
    return transaction

def process_page_2(params):
    page_number = params['page_number']
    txns_list_of_lists = params['txns_list_of_lists']
    bank = params['bank']
    name = params['name']
    # print(f'VP:: page_num: {page_number}')
    transaction, _ = get_transactions_finvu_aa(txns_list_of_lists[page_number], bank, name, None)
    return transaction

def test_aa_transactions(json_path, bank, cnt_c = 0.0):
    """
    Prints the transactions after processing json file
    :param: path_list (list of json files), bank
    Example script:
        from library.testing_rig import test_transactions
        path_list = ['/Users/shouryaps/Downloads/1.json']
        test_aa_transactions(path_list, 'idfc')
    """
    from python.utils import is_raw_aa_transactions_inconsistent
    print(f"VP:: {json_path.split('/')[-1]} \t-- {cnt_c}")
    # to check inconsistent
    from concurrent.futures import ThreadPoolExecutor
    # check_and_get_everything(bank, 'IN')
    BASE_TRXN_FOLDER = '/Users/vivek.pal/work/rough/Data/data_json/fraud_comp_14MAR/'
    if not os.path.exists(BASE_TRXN_FOLDER):
        os.makedirs(BASE_TRXN_FOLDER)

    aa_json_data = None
    with open(json_path, 'r') as json_file:
        aa_json_data = json.load(json_file)
    # getting body from aa data -> array
    body = aa_json_data.get("body", dict())
    # getting financial info objects -> array # getting the first fiObject
    firstFiObject = body[0]["fiObjects"][0]
    name = firstFiObject.get('Profile', dict()).get('Holders', dict()).get('Holder', dict()).get('name', dict())

    # getting the transactions list from aa
    aa_transactions_list = firstFiObject.get("Transactions", dict()).get("Transaction", [])
    # print("number of aa transactions: {}".format(len(aa_transactions_list)))

    # sort the transactions in ascending order
    does_raw_trxns_contains_inconsistency = is_raw_aa_transactions_inconsistent(aa_transactions_list, bank)
    if does_raw_trxns_contains_inconsistency:
        # sort the transactions in ascending order
        aa_transactions_list = sorted(aa_transactions_list, key=lambda d: d["valueDate"])
        aa_transactions_list = sorted(aa_transactions_list, key=lambda d: d["transactionTimestamp"])
        print("\n\nVP:: Going with sorting flow")
    else:
        print("\n\nVP:: We didn't sort the transactions")
    sorted_aa_transactions_list = aa_transactions_list

    # print(sorted_aa_transactions_list)
    file_name = json_path.split('/')[-1].split('.')[0]
    original_trxn_len = len(aa_transactions_list)
    with open(BASE_TRXN_FOLDER + f'{cnt_c}_{file_name}_origin.json', 'w') as json_file:
        json.dump(sorted_aa_transactions_list, json_file, indent=4)

    # getting transactions list of list for page count
    txns_list_of_lists = get_transactions_list_of_lists_finvu_aa(sorted_aa_transactions_list)

    number_of_pages = len(txns_list_of_lists)
    # print(f'VP:: #pages = {number_of_pages}')
    process_list = [None]*number_of_pages
    for page_num in range(number_of_pages):
        params = {
            'page_number': page_num,
            'txns_list_of_lists': txns_list_of_lists,
            'bank': bank,
            'name': name
        }
        process_list[page_num] = params
    # old_transaction_list = []
    # old_trxn_itr = []
    # with ThreadPoolExecutor(max_workers=20) as old_executor:
    #     old_trxn_itr = old_executor.map(process_page, process_list)
    # for old_trxn in old_trxn_itr:
    #     old_transaction_list.extend(old_trxn)
    # previous_trxn_len = len(old_transaction_list)
    # previous_inc_hash = transaction_balance_check(old_transaction_list, bank, 'aa')
    # with open(f'/Users/vivek.pal/work/rough/Data/data_json/fraud_comp/{file_name}_old.json', 'w') as json_file:
    #     json.dump(old_transaction_list, json_file, indent=4)

    # ##
    new_transaction_list = []
    new_trxn_itr = []
    with ThreadPoolExecutor(max_workers=20) as new_executor:
        new_trxn_itr = new_executor.map(process_page_2, process_list)
    for new_trxn in new_trxn_itr:
        new_transaction_list.extend(new_trxn)
    ##
    # for page_num in range(number_of_pages):
    #     new_transaction_list.extend(get_transactions_finvu_aa(txns_list_of_lists[page_num], bank, name, page_num)[0])
    ##
    # pg_lvl_incon_hash = transaction_balance_check(new_transaction_list, bank, 'aa')
    # print(f'VP:: pg_lvl_incon_hash: {pg_lvl_incon_hash}\n\n')
    ##
    # new_transaction_list, _ = remove_finvu_aa_inconsistency(new_transaction_list, bank)
    # new_transaction_list, _, _, _ = optimise_transaction_type(new_transaction_list, bank, 'aa')
    new_transaction_list = process_and_optimze_transactions_aa(new_transaction_list, bank, 'aa')
    # new_transaction_list, _ = get_transactions_finvu_aa(sorted_aa_transactions_list, bank, name)
    ##
    # new_trxn_len = len(new_transaction_list)
    new_inc_hash = transaction_balance_check(new_transaction_list, bank, 'aa')
    with open(BASE_TRXN_FOLDER + f'{cnt_c}_{file_name}_new.json', 'w') as json_file:
        json.dump(new_transaction_list, json_file, indent=4)
    # return transaction_list
    # return original_trxn_len, previous_trxn_len, previous_inc_hash, new_trxn_len, new_inc_hash
    try:
        transactions_hash_list = [_['hash'] for _ in new_transaction_list]
        in_in = transactions_hash_list.index(new_inc_hash)
    except:
        in_in = -1
    # return original_trxn_len, new_trxn_len, new_inc_hash, in_in
    return original_trxn_len, new_inc_hash, in_in


def test_aa_transactions_input_param(input_param):
    json_path = input_param['json_path']
    bank = input_param['bank']
    cnt_c = input_param['cnt_c']
    return test_aa_transactions(json_path, bank, cnt_c)

def vishnu_test():
    stat_list = [
        '88def297-4133-47a2-90c5-4903751f49b0_iob',
    ]
    json_folder = '/Users/vivek.pal/Desktop/Daily/Inconsistent_Transactions/PB_inconsistency_aa/MAR_07_AA/MAR_07_AA_DUMP/'
    bank_name = 'karnataka'
    retu_list = []
    cnt = len(stat_list)
    param_list = [None]*cnt
    for i in range(cnt):
        param_list[i] = {
            'stat_id': stat_list[i],
            'json_path': json_folder + stat_list[i] + '.json',
            'bank': stat_list[i].split('_')[1],
            'cnt_c': i
        }
    import concurrent.futures
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        for param, res in zip(param_list, executor.map(test_aa_transactions_input_param, param_list)):
            lax_dict = {
                'stat_id': param['stat_id'],
                'trxn_cnt': res[0],
                'inc_hash': res[1],
                'in_index': res[2]
            }
            retu_list.append(lax_dict)
    print(f'\n\n\n\n\n')
    cnt = 0
    df = pd.DataFrame(retu_list)
    df.to_excel('/Users/vivek.pal/Desktop/Daily/Inconsistent_Transactions/PB_inconsistency_aa/MAR_07_AA/local_testing_data_iob.xlsx')
    for item in retu_list:
        print(f'{cnt}    {item["stat_id"]} -- {item["trxn_cnt"]} \t-- {item["in_index"]}\t-- {item["inc_hash"]} -- ')
        cnt += 1


def process_and_optimze_transactions_aa(transaction_list, bank_name, statement_attempt_type):
    if not transaction_list:
        return None
    
    for i in range(len(transaction_list)):
        if 'optimizations' not in transaction_list[i].keys():
            transaction_list[i]['optimizations'] = []
    
    inconsistent_transaction_hash = transaction_balance_check(transaction_list, bank_name, statement_attempt_type)
    if not inconsistent_transaction_hash:
        return transaction_list
    
    transaction_list, _, _, _ = optimise_transaction_type(transaction_list, bank_name, statement_attempt_type)
    transaction_list = remove_finvu_aa_inconsistency(transaction_list, bank_name)
    if inconsistent_transaction_hash and bank_name in ['canara']:
        transaction_list, _ = optimise_refund_transactions(transaction_list, bank_name)
    
    return transaction_list


def test_identity(path, bank, password='',ocr=False):
    """
    Prints the essential identify after extraction
    :param: path_list (list of pdf files), bank, password (optional)

    Example script:

    from library.testing_rig import test_identity
    path_list = ['/Users/shouryaps/Downloads/1.pdf']
    test_identity(path_list, 'idfc')

    """
    from library.fitz_functions import extract_essential_identity
    from library.hsbc_ocr import extract_essential_identity_ocr
    
    keys = ["micr_bbox", "account_category_bbox", "name_bbox", "ifsc_bbox", "limit_bbox", "name_quality", "account_category_mapping", "address_bbox", "accnt_bbox", "date_bbox", "is_od_account_bbox", "od_limit_bbox", "currency_bbox", "opening_date_bbox", "opening_bal_bbox", "closing_bal_bbox", "email_bbox", "phone_number_bbox", "pan_number_bbox", "joint_account_holders_regex"]
    identity_templates = {}
    for k in keys:
        identity_templates[k] = get_templates_from_quality(bank, k)
    check_and_get_everything(bank)
    if not ocr:
        identity_dict = extract_essential_identity(path, bank, password, template=identity_templates)
    else:
        identity_dict = extract_essential_identity_ocr(path, bank, password)
    return identity_dict


def test_box(bbox, path, password='', page_num=0):
    """
    Prints the words extracted from a given bounded box in the page of a document
    :param: bbox (tuple containing x0, y0, x1, y1), path (path to pdf file), password,
            page_num (page number, default value is 0)

    Example script:

    from library.testing_rig import test_box
    path = '/Users/shouryaps/Downloads/1.pdf'
    bbox = (100, 60, 400, 100)
    bank = 'idfc'
    test_box(bbox, path, bank)

    """
    doc = read_pdf(path, password)
    page = doc[page_num]
    print(get_text_in_box(page, bbox))

def test_old_excel_report(path, bank, password=''):
    """
    Generates the old excel report for the given statement
    :param: path (path of the pdf file), bank, password
    """
    from library.extract_txns_fitz import get_transactions_using_fitz
    from library.fsm_excel_report import create_excel_report
    from library.fitz_functions import read_pdf

    def date_formatter(row):
        row['date'] = row['date'].strftime("%Y-%m-%d %H:%M:%S")
        return row
    transaction_list = []
    number_of_pages = read_pdf(path, password).page_count
    for page_num in range(0, number_of_pages):
        transaction_list.extend(get_transactions_using_fitz(path, bank, password, page_num))
    transaction_list = list(map(date_formatter, transaction_list))
    create_excel_report(transaction_list, 'bank_statement_summary', {}, '/Users/shouryaps/Desktop/reports/')

def test_excel_report_generator(entity_id, output_file_path):
    """
    Genrates a testing excel report sheet based on transactions list provided
    """

    url = "https://apis-dev.bankconnect.finbox.in/bank-connect/v1/entity/{}/transactions/".format(entity_id)

    headers = {'x-api-key': '<API_KEY>'}

    response = call_api_with_session(url, "GET", None, headers).json()

    test_transaction_data = response.get('transactions',[])
    
    url = "https://apis-dev.bankconnect.finbox.in/bank-connect/v1/entity/{}/salary/".format(entity_id)
    headers = {'x-api-key': '<API_KEY>'}
    response = call_api_with_session(url,"GET", None, headers).json()
    test_salary_transactions = response.get('transactions',[])
    # print("--test_salary_transactions--", test_salary_transactions, '\n')
    
    url = f"https://apis-dev.bankconnect.finbox.in/bank-connect/v1/entity/{entity_id}/predictors/"

    response = call_api_with_session(url,"GET", None, headers).json()
    predictors = response.get("predictors", [{}])[0].get("predictors", {})
    identity_data = {}

    from library.excel_report.report_generator import create_xlsx_report

    test_frauds_list = [] # type - list
    test_recurring_transactions = {} # type - dict
    # test_salary_transactions = [] # type - list
    test_personal_data = {"salary_confidence":0} # type - dict
    enriched_eod_balances = final_eod_calculator(test_transaction_data, False, identity_data)
    monthly_analysis = {}

    # if the dates are not in increasing order
    if test_transaction_data[0]["date"] > test_transaction_data[-1]["date"]:
        # reversing to make in increasing order
        test_transaction_data.reverse()

    overview_dic = create_xlsx_report(test_transaction_data, test_personal_data, output_file_path, test_salary_transactions, test_recurring_transactions, test_frauds_list, predictors, monthly_analysis, enriched_eod_balances, "v1")

def test_combine_and_dedup_transactions(list_of_txn_list):
    # print total transactions found
    print("\n\nTOTAL TRANSACTIONS CONTAINING DUPLICATES -> ", sum([len(x) for x in list_of_txn_list]))
    starttime = time.time()
    combined_transactions = list()
    if len(list_of_txn_list) == 0:
        return list()
    # tracking processed hashes
    seen_hashes = set()
    current_date = None
    current_date_index = None
    while max([len(txn_list) for txn_list in list_of_txn_list]) > 0:
        dates = [txn_list[0]['date'] if len(
            txn_list) > 0 else None for txn_list in list_of_txn_list]
        min_date = min([date for date in dates if date is not None])
        min_date_index = dates.index(min_date)
        # new
        if len(combined_transactions) > 0:
            if list_of_txn_list[min_date_index][0]["hash"] in seen_hashes:
                # simply skip
                list_of_txn_list[min_date_index].pop(0)
            else:
                seen_hashes.add(list_of_txn_list[min_date_index][0]["hash"])
                found_similar = False
                for i in range(current_date_index, len(combined_transactions)):
                    if combined_transactions[i]["date"] == list_of_txn_list[min_date_index][0]["date"] and combined_transactions[i]["transaction_type"] == list_of_txn_list[min_date_index][0]["transaction_type"] and combined_transactions[i]["amount"] == list_of_txn_list[min_date_index][0]["amount"] and combined_transactions[i]["balance"] == list_of_txn_list[min_date_index][0]["balance"] and combined_transactions[i]["transaction_channel"] == list_of_txn_list[min_date_index][0]["transaction_channel"] and combined_transactions[i]["transaction_note"] == list_of_txn_list[min_date_index][0]["transaction_note"]:
                        # means we need to skip that transaction
                        found_similar = True
                        list_of_txn_list[min_date_index].pop(0)
                        break
                # here means we have not found a similar transaction
                if not found_similar:
                    combined_transactions.append(list_of_txn_list[min_date_index].pop(0))
                    if current_date < combined_transactions[-1]["date"]:
                        current_date = combined_transactions[-1]["date"]
                        current_date_index = len(combined_transactions) - 1
        else:
            seen_hashes.add(list_of_txn_list[min_date_index][0]["hash"])
            combined_transactions.append(list_of_txn_list[min_date_index].pop(0))
            current_date = combined_transactions[0]["date"]
            current_date_index = 0
    
    print("TIME TAKEN TO COMBINE -> ", time.time() - starttime)
    print("UNIQUE TRANSACTIONS -> ", len(combined_transactions), "\n\n")
    # generate a new report 
    test_excel_report_generator(combined_transactions, "some path with extension")
    # print(combined_transactions)
    print("DONE...")

def test_multiline_fix():
    """
    This function compares dev vs. prod excel files row by row for `transactions` page
    and saves a result in csv file
    """

    import warnings
    import pandas as pd


    warnings.simplefilter(action = "ignore", category = FutureWarning)
    pd.options.mode.chained_assignment = None


    csv_file_path = "/Users/karanbalani/Downloads/multilinefix/statements.csv"
    reports_folder_path = "/Users/karanbalani/Downloads/multilinefix/reports/"

    results_excel_path = "/Users/karanbalani/Downloads/multilinefix/results.xlsx"
    results_dict = {
        "name": [],
        "is_same_size": [],
        "bank": [],
        "differences": []
    }

    csv_data = pd.read_csv(csv_file_path)
    csv_data = csv_data.fillna("")
    for index, rows in csv_data.iterrows():
        report_name = str(rows[0])
        report_password = str(rows[1])
        report_bank = str(rows[2])

        results_dict["name"].append(report_name)
        results_dict["bank"].append(report_bank)

        prod_excel_path = reports_folder_path + "PROD_" + report_name + "_" + report_bank + ".xlsx"
        prod_excel_df = pd.read_excel(prod_excel_path, "Transactions")
        prod_excel_df = prod_excel_df[["Date", "Transaction Note", "Amount", "Balance", "Transaction Type"]]

        dev_excel_path = reports_folder_path + "DEV_" + report_name + "_" + report_bank + ".xlsx"
        dev_excel_df = pd.read_excel(dev_excel_path, "Transactions")
        dev_excel_df = dev_excel_df[["Date", "Transaction Note", "Amount", "Balance", "Transaction Type"]]

        if prod_excel_df.shape[0] == dev_excel_df.shape[0]:
            results_dict["is_same_size"].append(True)
        else:
            results_dict["is_same_size"].append(False)
            results_dict["differences"].append([])
            continue
        
        temp_df = pd.DataFrame()

        temp_df["date_prod"] = prod_excel_df["Date"]
        temp_df["note_prod"] = prod_excel_df["Transaction Note"]
        temp_df["amt_prod"] = prod_excel_df["Amount"]
        temp_df["bal_prod"] = prod_excel_df["Balance"]
        temp_df["type_prod"] = prod_excel_df["Transaction Type"]
        
        temp_df["date_dev"] = dev_excel_df["Date"]
        temp_df["note_dev"] = dev_excel_df["Transaction Note"]
        temp_df["amt_dev"] = dev_excel_df["Amount"]
        temp_df["bal_dev"] = dev_excel_df["Balance"]
        temp_df["type_dev"] = dev_excel_df["Transaction Type"]

        diffs = []
        further_analyze = False
        for r in temp_df.itertuples():
            if r.date_prod == r.date_dev and r.amt_prod == r.amt_dev and r.bal_prod == r.bal_dev and r.type_prod == r.type_dev and str(r.note_prod).strip() != str(r.note_dev).strip():
                diffs.append(r.date_prod)
                further_analyze = True
        
        results_dict["differences"].append(diffs)

        if further_analyze:
            analysis_excel_file_path = "/Users/karanbalani/Downloads/multilinefix/analysis/" + "ANALYSIS_" + report_name + ".xlsx"
            analysis_writer = pd.ExcelWriter(analysis_excel_file_path)
            analysis = {
                "everything_else_fine": [],
                "date": [],
                "note_prod": [],
                "note_dev": [],
            }
            for r in temp_df.itertuples():
                if r.note_prod != r.note_dev:
                    analysis["everything_else_fine"].append(True)
                    analysis["date"].append(r.date_prod)
                    analysis["note_prod"].append(r.note_prod)
                    analysis["note_dev"].append(r.note_dev)
            analysis_df = pd.DataFrame(analysis)
            analysis_df.to_excel(analysis_writer)
            analysis_writer.save()

        # print("Index -> ", index)
        # if index == 10:
        #     break
    
    writer = pd.ExcelWriter(results_excel_path)
    results_df = pd.DataFrame(results_dict)
    results_df.to_excel(writer)
    writer.save()
    # print("DONE")


def test_improved_multiline_fix(pdf_path, bank, output_file_path, password = ""):
    # get all the transactions
    transactions_data = test_get_all_transactions([pdf_path], bank, password)[0]
    # generate report
    test_excel_report_generator(transactions_data, output_file_path)
    print("DONE")


def test_extraction_cprofile():
    import cProfile
    pr = cProfile.Profile()
    pr.enable()
    txns = test_transactions("/Users/siddhanttiwary/Downloads/sample_statements/perfios_statements/ffdb7802-6ac2-4ef4-936d-5699d752c8a2_sbi.pdf", "sbi")
    pr.disable()
    pr.dump_stats('./test.prof')
    return txns


def test_metadata_fraud(pdf_path, bank, password=""):
    """
    Check for metadata/author fraud for the given file and bank
    :param: path (path of the pdf file), bank and password (for pdf the file)
    """
    from library.fitz_functions import get_metadata_fraud, read_pdf
    doc = read_pdf(pdf_path, password)
    fraud_and_metadata = get_metadata_fraud(doc, bank)
    # print(fraud_and_metadata)
    return fraud_and_metadata


def get_perfios_fraud_data_for_statement(statement_id, entity_id, aws_access_key_id: str, aws_secret_access_key: str):
    
    ddb_resource = boto3.resource(
        "dynamodb", 
        region_name="ap-south-1", 
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key
    )

    s3 = boto3.client(
        "s3", 
        region_name="ap-south-1", 
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key
    )
    
    fsm_results_table = ddb_resource.Table("fsm-results-prod")
    ddb_response = fsm_results_table.get_item(
        Key={
            "entity_id": entity_id,
            "sort_key": "statement_{}_identity".format(statement_id)
        }
    )
    # print(ddb_response)

    identity_obj = ddb_response["Item"]["item_data"]["identity"]

    perfios_statement_status = identity_obj.get("perfios_statement_status", None)
    is_fraud = identity_obj.get("is_fraud", False)
    fraud_type = identity_obj.get("fraud_type", None)

    return perfios_statement_status, is_fraud, fraud_type


def test_bulk_metadata_fraud_perfios(input_csv_path, pdf_files_folder_path, output_csv_path, aws_access_key_id: str, aws_secret_access_key: str):
    """
    USED FOR PERFIOS BULK COMPARISON
    format of csv:
    perfios_entity_id, perfios_statement_id, perfios_link_id, bank_name, pdf_password
    """

    perfios_extracted_csv = pd.read_csv("{}".format(input_csv_path))
    # print(perfios_extracted_csv.head())
    perfios_extracted_csv.fillna("", inplace=True)

    mapping_data = []
    completed_count = 0
    for row in perfios_extracted_csv.itertuples(index=False):
        try:
            data_for_this_pdf = {}

            data_for_this_pdf["perfios_statement_id"] = row.statement_id
            data_for_this_pdf["perfios_entity_id"] = row.entity_id
            data_for_this_pdf["perfios_link_id"] = row.link_id
            data_for_this_pdf["bank_name"] = row.bank_name
            data_for_this_pdf["pdf_password"] = row.pdf_password
            
            pdf_path = pdf_files_folder_path + "/{}_{}.pdf".format(row.statement_id, row.bank_name)

            perfios_statement_status, perfios_is_fraud, perfios_fraud_type = get_perfios_fraud_data_for_statement(
                statement_id=row.statement_id,
                entity_id=row.entity_id,
                aws_access_key_id=aws_access_key_id,
                aws_secret_access_key=aws_secret_access_key
            )
            data_for_this_pdf["perfios_statement_status"] = perfios_statement_status
            data_for_this_pdf["perfios_is_fraud"] = perfios_is_fraud
            data_for_this_pdf["perfios_fraud_type"] = perfios_fraud_type

            is_bc_fraud, bc_fraud_type, fraud_and_metadata = test_metadata_fraud(pdf_path, row.bank_name, row.pdf_password)
            data_for_this_pdf["bank_connect_is_fraud"] = is_bc_fraud
            data_for_this_pdf["bank_connect_fraud_type"] = bc_fraud_type
            data_for_this_pdf["author"] = fraud_and_metadata["author"]
            data_for_this_pdf["producer"] = fraud_and_metadata["producer"]
            data_for_this_pdf["creator"] = fraud_and_metadata["creator"]

            pdf_format_creation_date = fraud_and_metadata["pdf_format_creation_date"]
            pdf_format_modification_date = fraud_and_metadata["pdf_format_modification_date"]
            data_for_this_pdf["pdf_format_creation_date"] = pdf_format_creation_date
            data_for_this_pdf["pdf_format_modification_date"] = pdf_format_modification_date
            data_for_this_pdf["fonts"] = fraud_and_metadata["fonts"]
            data_for_this_pdf["encryption_algo"] = fraud_and_metadata["encryption_algo"]

            # print(fraud_and_metadata)

            creation_date, modification_date = None, None
            if pdf_format_creation_date is not None:
                try:
                    creation_date = datetime.datetime.strptime(pdf_format_creation_date.replace("'", ""), "D:%Y%m%d%H%M%S%z")
                except Exception as e:
                    creation_date = None

            if pdf_format_modification_date is not None:
                try:
                    modification_date = datetime.datetime.strptime(pdf_format_modification_date.replace("'", ""), "D:%Y%m%d%H%M%S%z")
                except Exception as e:
                    modification_date = None

            data_for_this_pdf["creation_date"] = creation_date
            data_for_this_pdf["modification_date"] = modification_date

            mapping_data.append(data_for_this_pdf)
            completed_count = completed_count + 1

            if completed_count % 10 == 0:
                print("completed: {}".format(completed_count))
        except Exception as e:
            print("Exception occured: {}".format(e))

    # return mapping_data
    final_df = pd.DataFrame(mapping_data)
    final_df.to_csv("{}/final_mapping_{}.csv".format(output_csv_path, "only500"))

def get_description(x):
    row = check_loan(x)
    if row['is_lender']:
        return ('lender_transaction')
    if (row['is_lender'] == False) and (row['transaction_channel'] == 'auto_debit_payment') and (row['merchant_category'] not in ['investments','trading/investments','insurance']):
        return ('unidentified obligation')
    return ''
    

def getSal(row):
    sal_patt=re.compile('(?i)(.*[^A-Za-z]+SAL[^A-Za-z]+.*)')
    t=sal_patt.findall(row['transaction_note'].upper())
    if t:
        for i in t:
            i=re.sub(" ","",i)
            if "REVERSAL" in i:
                continue
            else:
                print("Salary Found")
                return "Salary Found"
    else:
        i=re.sub(" ","",row['transaction_note'].upper())
        if "SALARY" in i or "EARLYSAL" in i or "SALARIE" in i:
            print("Salary Found")
            return "Salary Found"
        sal_start_patt = re.compile('(?i)(^SAL[^A-Za-z]+.*)')
        t=sal_start_patt.findall(row['transaction_note'].upper())
        if t:
            print("Salary Found")
            return "Salary Found"
    return ""

def test_classification(transaction_note_path,output_csv_path):
    start=time.time()
    print("Started at {}".format(start))
    df=pd.read_csv(transaction_note_path)
    df = df[:100000]
    # print (df)
    df.fillna("",inplace=True)
    merchant_category_dict = get_merchant_category_dict()
    merchant_categories = merchant_category_dict.keys()
    tag_merchant_categories = [_ for _ in merchant_categories if "_regex" not in _]
    regex_merchant_categories = [_ for _ in merchant_categories if "_regex" in _]
    df['transaction_channel'] = df.apply(lambda x: tx_channel_helper(x), axis=1)
    # print(df)
    print("Transaction Channel Finished")
    df['unclean_merchant'] = df.apply(lambda x: tx_merchant_helper(x), axis=1)
    print("unclean merchant Finished")
    df = df.apply(lambda x: get_merchant_category(x, tag_merchant_categories, regex_merchant_categories, merchant_category_dict, False), axis=1)
    print("merchant category Finished")
    df['description_new']=df.apply(lambda x: get_description(x),axis=1)
    # df['Sal or not']=df.apply(lambda x : getSal(x),axis=1)
    #print("SAL or not finished")
    # print(df[['transaction_note','transaction_channel','description_new']])
    df.to_csv(output_csv_path) 
    end=time.time()
    print("Time elapsed: ",(end-start)/60)


def test_transaction_note(perfios_transactions,bank_connect_transactions):
    df_perfios = pd.read_csv(perfios_transactions)
    df_bc = pd.read_csv(bank_connect_transactions)
    df_perfios = df_perfios.fillna('')
    df_bc = df_bc.fillna('')
    x=pd.merge(df_perfios,df_bc,on=['amount','balance','date'],how='inner')
    x.to_csv('/Users/siddhanttiwary/Downloads/merged.csv')
    return
        
def test_salary_against_perfios(data_file,output_csv,perfios_data):
    start=time.time()
    print("Started at {}".format(start))
    df = pd.read_csv(data_file)
    df.fillna('',inplace=True)
    df = df[0:100000]
    perfios_data_df = pd.read_csv(perfios_data)
    df.fillna('')
    df['transaction_channel']=df.apply(lambda x: tx_channel_helper(x),axis=1)
    print("TRANSACTION CHANNEL FINISHED")
    merchant_category_dict = get_merchant_category_dict()
    merchant_categories = merchant_category_dict.keys()
    tag_merchant_categories = [_ for _ in merchant_categories if "_regex" not in _]
    regex_merchant_categories = [_ for _ in merchant_categories if "_regex" in _]
    df = df.apply(lambda x: get_merchant_category(x, tag_merchant_categories, regex_merchant_categories, merchant_category_dict, False), axis=1)
    print("CATEGORY DONE")
    df['description']=df.apply(lambda x: get_description(x),axis=1)
    print("DESCRIPTION FINISHED")
    x=pd.merge(df,perfios_data_df,on=['amount','balance','date'],how="inner")
    print(df)
    x.to_excel(output_csv)
    print("Finished")
    
    end=time.time()
    print("Time elapsed: ",(end-start)/60)
    return

def test_metadata_fraud(folder_path, csv, output_csv):
    file = pd.read_csv(csv)
    file.fillna('',inplace=True)
    columns=['statement_id','is_fraud','fraud_type',"author","producer","creator","pdf_format_creation_date","pdf_format_modification_date","fonts","encryption_algo","font_size","font_colors","linewidth","trapped","subject","keywords","format","doc_filter","xref_id","devicergb_list","q_start_count_list","max_push_q_cnt","max_pop_Q_cnt","max_both_qQ_cnt","set_f123","o_tr_list"]
    result_df = pd.DataFrame(columns=columns)
    print(len(result_df))
    for index, items in file.iterrows():
        if items['statement_id'] in list(result_df['statement_id']):
            continue
        file_name = folder_path+items['statement_id']+'_'+items['bank_name']+".pdf"
        doc = read_pdf(file_name,items['pdf_password'])
        if doc in [-1,0]:
            row = [items['statement_id']]+[None]*(len(columns)-1)
        else:
            is_fraud, fraud_type, metadata_dict = get_metadata_fraud(doc, items['bank_name'], file_name, items['pdf_password'])
            row = [items['statement_id'], is_fraud, fraud_type]
            for keys in columns[3:]:
                row.append(metadata_dict[keys])
        print(row)
        result_df.loc[len(result_df)]=row
        result_df.to_csv(output_csv)
    result_df.to_csv(output_csv)

def get_credit_card_transactions(path, password, bank):
    doc = read_pdf(path, password)
    from library.credit_card_extraction_with_ocr import get_cc_transactions_using_fitz
    
    all_transactions = []
    if isinstance(doc, int):
        return []
    for i in range(doc.page_count):
        temp_txns, template_id = get_cc_transactions_using_fitz(path, password, bank, i)
        print("extracted {} transactions from page number {}".format(len(temp_txns), i))
        all_transactions += temp_txns

    return all_transactions

def download_pdf(statement_id, bank_name, dump_path):
    import boto3
    client = boto3.client('s3', aws_access_key_id="", aws_secret_access_key="")
    key = f"pdf/{statement_id}_{bank_name}.pdf"
    print(f"key -> {key}")
    response = client.get_object(Bucket="bank-connect-uploads-prod", Key=key)
    response_metadata = response.get('Metadata')
    file_path = f"{dump_path}/{statement_id}_{bank_name}.pdf"
    with open(file_path, 'wb') as file_obj:
        file_obj.write(response['Body'].read())

def get_prod_transactions(entity_id):
    url = f"https://apis.bankconnect.finbox.in/bank-connect/v1/entity/{entity_id}/transactions/"

    payload = {}
    headers = {
        'x-api-key': '<API_KEY>',
        'server-hash': '<SERVER_HASH>'
    }
    response = call_api_with_session(url, "GET", payload, headers).json()
    return response['transactions']


def test_prod(sheet):
    df = pd.read_excel(sheet)
    df = df.sample(n=100)
    result = pd.DataFrame(columns=['statement_id', 'length_match', 'items_match', 'message'])
    df.fillna('', inplace=True)
    print(df.head())
    dump_path = "/tmp/test_pdfs"
    if not os.path.exists(dump_path):
        os.mkdir(dump_path)
    for index, row in df.iterrows():
        statement_id = row['statement_id']
        bank_name = row['bank_name']
        entity_id = row['entity_id']
        pdf_password = row['pdf_password']
        if bank_name in ['karnataka', 'uco', 'federal']:
            continue
        print(f"doing for statement id : {statement_id}")
        download_pdf(statement_id, bank_name, dump_path)
        prod_transactions = get_prod_transactions(entity_id)
        
        try:
            file_path = f"{dump_path}/{statement_id}_{bank_name}.pdf"

            local_transactions = test_get_all_transactions([file_path], bank_name, pdf_password)

            prod_transactions = sorted(prod_transactions, key=lambda x: x['date'])
            local_transactions = sorted(local_transactions, key=lambda x: x['date'])

            print("\n\n\n\n")
            match = True
            message = None
            for p, l in zip(prod_transactions, local_transactions):
                del p['hash']
                del p['account_id']
                del p['transaction_channel']
                del p['merchant_category']
                del p['description']

                del l['unclean_merchant']
                del l['hash']
                del l['is_lender']
                del l['merchant']
                del l['transaction_channel']
                del l['merchant_category']
                del l['description']

                if p!=l:
                    print(" p and l not same")
                    print("p -> ", p)
                    print("l -> ", l)
                    if len(l['transaction_note']) != len(p['transaction_note']):
                        message = "transaction note length mismatch"
                    else:
                        match = False
                    break
            result.loc[len(result)] = [statement_id, len(prod_transactions)==len(local_transactions), match, message]
        except Exception as e:
            result.loc[len(result)] = [statement_id, False, False, None]
    
        result.to_excel("/Users/siddhanttiwary/Downloads/result.xlsx")


def test_categorisation(bank_name, regex_dict):
    from library.transaction_channel import transactionchannel
    from library.utils import get_compiled_regex_list
    debit = regex_dict["debit"]
    credit = regex_dict["credit"]
    file = f"/Users/siddhanttiwary/Desktop/fixed_deposit/{bank_name}.xlsx"
    df = pd.read_excel(file)
    debit_priority_order = credit_priority_order = ["investments"]
    debit_regexes = {
        "investments": get_compiled_regex_list(debit)
    }
    credit_regexes = {
        "investments": get_compiled_regex_list(credit)
    }
    df.fillna('', inplace=True)
    df['transaction_channel'] = df.apply(
        lambda x : transactionchannel(
            x, debit_regexes, debit_priority_order, credit_regexes, credit_priority_order
        ),
        axis = 1
    )
    # print(df.head(5))
    df.to_excel("/Users/siddhanttiwary/Desktop/fd.xlsx")

def test_fraud(path, password, bank_name):
    fraud_data = pd.read_excel("/Users/siddhanttiwary/Downloads/fsmlib_fraud_data.xlsx")
    from library.fitz_functions import get_stream_fraud_data_page, add_stream_fraud_data_all_pages
    doc = read_pdf(path, password)
    page_data = {}
    for page in range(0, doc.page_count):
        final_dict = get_stream_fraud_data_page(path, password, bank_name, page)
        final_dict['exception_in_fraud_logic'] = False
        page_data[page] = final_dict
    
    # print(page_data)
    stream_fraud_data = add_stream_fraud_data_all_pages(page_data, doc.page_count)
    filtered_fraud_data = fraud_data[fraud_data["bank_name"]==bank_name]
    fraud_dict_from_db = {
        'strict_metadata_fraud_list':[],
        'good_font_list':[],
        'stream_font_list':[],
        'encryption_algo_list':[]
    }

    if len(filtered_fraud_data)>0:
        for index, fraud_data in filtered_fraud_data.iterrows():
            data_type = fraud_data.get('type', None)
            if data_type in fraud_dict_from_db.keys():
                fraud_dict_from_db[data_type] = fraud_data.get('data_list', [])
                if isinstance(fraud_dict_from_db[data_type], str):
                    fraud_dict_from_db[data_type] = fraud_dict_from_db[data_type].replace("\'", "\"")
                    fraud_dict_from_db[data_type] = json.loads(fraud_dict_from_db[data_type])
    
    # print(fraud_dict_from_db["strict_metadata_fraud_list"])
    is_fraud, fraud_type, doc_metadata_dict, all_fraud_list = get_metadata_fraud(stream_fraud_data, doc, bank_name, path, password, "IN",\
                                stream_font_list=fraud_dict_from_db["stream_font_list"], encryption_algo_list=fraud_dict_from_db["encryption_algo_list"], \
                                    good_font_list=fraud_dict_from_db["good_font_list"], \
                                    strict_metadata_fraud_list=fraud_dict_from_db["strict_metadata_fraud_list"])
    
    return fraud_type


def test_aa_transactions_2(path):
    from library.transactions import get_transactions_finvu_aa
    with open(path, "r") as f:
        aa_data = json.load(f)
    
    body = aa_data.get("body", dict())
    fiObjects = body[0]["fiObjects"]
    firstFiObject = fiObjects[0]
    aa_transactions_list = firstFiObject.get("Transactions", dict()).get("Transaction", [])
    check_and_get_everything("icici")
    import cProfile
    pr = cProfile.Profile()
    print("Enabling profiler")
    pr.enable()
    transactions, error_message = get_transactions_finvu_aa(aa_transactions_list, "icici", "", {})
    pr.disable()
    pr.dump_stats('./test_aa_2.prof')