import os
import json
import warnings
import pandas as pd
import fitz
import datetime
from pdf2image import convert_from_path
import threading
import time
import numpy as np
import re
import cv2
import pytesseract
from library.utils import add_hash_to_transactions_df, EPOCH_DATE, convert_pandas_timestamp_to_date_string, format_hsbc_ocr_rows
from library.transaction_channel import get_transaction_channel
from library.transaction_description import get_transaction_description
from library.extract_txns_fitz import balance_date_rows, remove_opening_balance
from library.table import parse_table
from library.statement_plumber import transaction_rows, map_correct_columns
from library.fitz_functions import *
import concurrent.futures
from copy import deepcopy


warnings.simplefilter(action = "ignore", category = FutureWarning)
pd.options.mode.chained_assignment = None


def multiprocessing_helper(params):
    transactions, removed_date_opening_balance = get_single_page_transactions(params[0], params[1])
    return transactions

def celery_job_creator(path, password, page_number):
    images = convert_from_path(path, userpw=password,first_page=page_number, last_page=page_number)
    if len(images)>0:
        return images[0]

def get_all_transactions_ocr(path, bank, password, name, key, bucket):
    all_images = get_images(path, password)
    all_transactions = []
    
    
    page_breaks = []

    # sync solution
    # for page in all_images:
    #     single_page_transactions = get_single_page_transactions(page, bank)
    #     page_breaks.append(len(single_page_transactions))
    #     all_transactions += single_page_transactions

    num_of_pages = len(all_images)
    '''
    Shared variable declaration which can be used by both multiprocessing and multithreading solutions
    '''
    # thread_list = [None]*num_of_pages
    # all_transactions = [None]*num_of_pages
    # page_breaks = [None]*num_of_pages
    
    '''
    Multithreading solution starts
    '''
    # # prepare threads and invoke them
    # for page in range(len(all_images)):
    #     thread_list[page] = threading.Thread(target=get_single_page_transactions,args=(all_images[page], bank, all_transactions, page_breaks, page))
    #     thread_list[page].start()
    
    # # join all the threads
    # for threads in thread_list:
    #     threads.join()

    # # flatten all_transactions
    # all_transactions = list(np.concatenate(all_transactions))
    '''
    Multithreading solution ends
    '''

    '''
    Multi-processing solution starts
    '''

    # multiprocessing won't work the sameway as threads because they dont share the same memory space
    process_list = [None]*num_of_pages
    for page in range(len(all_images)):
        process_list[page] = (all_images[page], bank)

    # prepare concurrent pool
    with concurrent.futures.ProcessPoolExecutor(max_workers=3) as executor:
        transacton_iterable = executor.map(multiprocessing_helper, process_list)

    print("Tranasction Iterable",transacton_iterable) # --> should return executor generator object

    final_set_transactions = []
    page_breaks = []
    for transaction in transacton_iterable:
        final_set_transactions+=transaction
        page_breaks.append(len(transaction))
    all_transactions = final_set_transactions.copy()

    '''
    Multiprocessing solution ends
    '''
    # print("Page Breaks: ",page_breaks)
    return process_transactions(all_transactions, page_breaks, bank, name)

def process_transactions(all_transactions, page_breaks, bank, name):
    transaction_no = 0
    page_break_index = 0
    default_balance = -1.0

    '''
    Santize transactions -> Fill missing dates, club transaction notes and fill missing balances.
    '''
    for transaction in all_transactions:
        transaction_no +=1
        if transaction_no > page_breaks[page_break_index]:
            transaction_no = 0
            page_break_index += 1
        if transaction["transaction_merge_flag"] and transaction["date"] != EPOCH_DATE and transaction["balance"] == default_balance:
            index = all_transactions.index(transaction)
            index += 1 
            try:
                next_transaction = all_transactions[index]
                transaction["transaction_note"] += ' ' + next_transaction["transaction_note"] 
                transaction["balance"] = next_transaction["balance"]
                transaction["amount"] = next_transaction["amount"]
                transaction["transaction_type"] = next_transaction["transaction_type"]
                page_breaks[page_break_index+1] -=1
                del all_transactions[index]
            except:
                del all_transactions[index-1]
        elif transaction["transaction_merge_flag"] and transaction["date"] == EPOCH_DATE and transaction["balance"] != default_balance:
            index = all_transactions.index(transaction)
            index -= 1 
            previous_transaction = all_transactions[index]
            transaction["date"] = previous_transaction["date"]
        elif transaction["transaction_merge_flag"] and transaction["date"] == EPOCH_DATE and transaction["balance"] == default_balance:
            index = all_transactions.index(transaction)
            next_index = index + 1
            try:
                next_transaction = all_transactions[next_index]
                transaction["transaction_note"] += ' ' + next_transaction["transaction_note"] 
                transaction["balance"] = next_transaction["balance"]
                transaction["amount"] = next_transaction["amount"]
                transaction["transaction_type"] = next_transaction["transaction_type"]
                page_breaks[page_break_index+1] -=1
                prev_index = index -  1
                previous_transaction = all_transactions[prev_index]
                transaction["date"] = previous_transaction["date"]
                del all_transactions[next_index]
            except:
                del all_transactions[next_index -  1]
        del transaction["transaction_merge_flag"]
    
    '''
    Sanitise transactions
    '''
    temp_transactions = deepcopy(all_transactions)
    if len(temp_transactions) > 0:
        temp_transactions = temp_transactions[::-1]
        last_date = temp_transactions[0]["date"]
        for transaction in temp_transactions[1:]:
            if transaction["date"]==EPOCH_DATE:
                transaction["date"]=last_date
            else:
                last_date = transaction["date"]
    all_transactions = temp_transactions[::-1]
    
    '''
    Fill Transaction categorisations. Same as other transactions
    '''
    all_transactions = get_transaction_channel(pd.DataFrame(all_transactions), bank)
    all_transactions = get_transaction_description(all_transactions, name)
    all_transactions = add_hash_to_transactions_df(all_transactions)
    all_transactions = all_transactions.to_dict('records')
    page_wise_transactions = []

    index_el = 0 
    '''
    Prepare pagewise transactions nested list. Using page-breaks which were initialised earlier.
    '''
    for i in range(len(page_breaks)):
        next = index_el + page_breaks[i]
        page_wise_transactions.append(all_transactions[index_el:next])
        index_el = next

    return page_wise_transactions

def get_single_page_transactions(relevant_page, bank):
    file_path = 'library/bank_data/' + bank + '.json'
    if os.path.exists(file_path):
        with open(file_path, 'r') as data_file:
            try:
                extraction_parameter = json.load(data_file).get('trans_bbox', [])
            except ValueError:
                print("Invalid JSON file\nPlease check")
                extraction_parameter = []
            except Exception as e:
                print(e)
                extraction_parameter = []
            finally:
                data_file.close()
    else:
        print("Incorrect bank name")
        extraction_parameter = []
    transactions, removed_date_opening_balance = get_tables_each_page_ocr(bank, relevant_page, extraction_parameter)
    return transactions, removed_date_opening_balance

def get_tables_each_page_ocr(bank, page, extraction_parameter):
    return_data_page = []
    width, height = page.size
    for each_parameter in extraction_parameter:
        '''
        Only use those templates which are configured for OCR
        '''
        if 'image_flag' not in each_parameter.keys():
            continue

        vertical_lines = each_parameter['vertical_lines']
        # image_flag required in extraction of OCR transactions
        image_flag = each_parameter.get('image_flag', False)
        # range_involved is required to merge transactions having y co-ordinate difference of 1 
        range_involved = each_parameter.get('range', False)
        # image_flag = True if image_flag else False
        if vertical_lines:
            tables, _, _ = parse_table(page, [0, 0, width, height], columns=vertical_lines, image_flag=image_flag, range_involved=range_involved)

        columns = each_parameter['column']
        actual_table = tables
        txn_df = pd.DataFrame(actual_table)
        for each_column_list in columns:
            transaction_list = []
            if txn_df.shape[1] == len(each_column_list):
                txn_df.columns = each_column_list
                format_rows_page = format_hsbc_ocr_rows(txn_df)
                balance_date_rows_page = balance_date_rows(format_rows_page)
                transaction_rows_page,_ = transaction_rows(balance_date_rows_page, bank)
                if transaction_rows_page.shape[0] > 0:
                    transaction_list.extend(transaction_rows_page.apply(
                        lambda row: map_correct_columns(row, bank, "IN"), axis=1))
                    transaction_df = pd.DataFrame(transaction_list)
                    for col in transaction_df:
                        if col in ['amount', 'balance']:
                            transaction_df[col] = transaction_df[col].fillna(0)
                        else:
                            transaction_df[col] = transaction_df[col].fillna('')
                    if transaction_df.shape[0] > 0:
                        transaction_df = transaction_df[((transaction_df['transaction_type'] == 'credit') | (
                            transaction_df['transaction_type'] == 'debit')) & (abs(transaction_df['amount']) > 0)]
                    return_data_page = transaction_df.to_dict('records')
    return_data_page, removed_date_opening_balance = remove_opening_balance(return_data_page)
    removed_date_opening_balance = convert_pandas_timestamp_to_date_string(removed_date_opening_balance)
    return return_data_page, removed_date_opening_balance

def get_images(path, password):
    images = convert_from_path(path, userpw=password)
    return images

def extract_essential_identity_ocr(path, bank, password, preshared_names=[]):
    doc = read_pdf(path, password)
    page_count = doc.page_count

    images = convert_from_path(path, userpw=password, first_page=1, last_page=min(2,page_count))

    identity_dict = dict()  # stores identity info
    result_dict = dict()  # stores final dictionary to return

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

    first_page_words = []
    second_page_words = []
    '''
    Get Identity quickly by extracting using threads.
    '''
    try:
        t1 = time.time()
        page_words = [None]*2

        thread1 = threading.Thread(target=get_text_words_ocr,args=(images[0],page_words,0))
        thread2 = threading.Thread(target=get_text_words_ocr,args=(images[1],page_words,1))

        thread1.start()
        thread2.start()
        thread1.join()
        thread2.join()
        
        t2 = time.time()
        print("Identity OCR Extraction took: ",t2-t1)
        first_page_words = page_words[0]
        second_page_words = page_words[1]
    except Exception as e:
        print("Some exception occured while extraction")
    page_words = [first_page_words, second_page_words]

    identity_dict['account_number'] = get_account_num_ocr(data.get('accnt_bbox', []), page_words)
    identity_name = get_name_ocr(data.get('name_bbox', []), page_words)
    identity_dict['name'] = identity_name
    identity_dict['address'] = get_address_ocr(data.get('address_bbox', []), page_words)
    identity_dict['ifsc'] = get_ifsc_ocr(data.get('ifsc_bbox', []), page_words)
    identity_dict['micr'] = get_micr_ocr(data.get('micr_bbox', []), page_words)

    result_dict['identity'] = identity_dict
    result_dict['keywords'] = get_account_key_words_ocr(page_words)
    result_dict['date_range'] = get_date_range_ocr(data.get('date_bbox', []), page_words)

    # is_fraud, error, doc_metadata_dict = get_metadata_fraud(doc, bank)
    result_dict['is_fraud'] = False
    result_dict['fraud_type'] = None
    # result_dict["doc_metadata"] = doc_metadata_dict

    result_dict["page_count"] = page_count

    # metadata_name_matches = get_metadata_name_matches(doc, preshared_names)

    result_dict["metadata_analysis"] = dict()
    result_dict["metadata_analysis"]["name_matches"] = []
    
    return result_dict

def get_date_range_ocr(bbox, words):
    from_date, to_date = None, None
    for page_wise_words in words:
        for template in bbox:
            from_date = get_date_ocr(template['from_bbox'], template['from_regex'], page_wise_words)
            to_date = get_date_ocr(template['to_bbox'], template['to_regex'], page_wise_words)
        if from_date is not None and to_date is not None:
                return {'from_date': from_date.strftime("%Y-%m-%d"), 'to_date': to_date.strftime("%Y-%m-%d")}
    return {'from_date': None, 'to_date': None}

def get_date_ocr(bbox, regex, words):
    text = get_text_in_box_ocr(bbox, words)
    all_text = text.replace('\n', '').replace(' ', '').replace('(cid:9)', '')
    all_text = remove_unicode(all_text)

    date = match_regex(all_text, regex, 1)
    date_to_return, _ = check_date(date)
    if date_to_return:
        return date_to_return
    else:
        return None

def get_account_key_words_ocr(words):
    all_present = False
    amount_present_ever = False
    balance_present_ever = False
    date_present_ever = False
    for page_wise_words in words:
        if words:
            all_text = get_text_in_box_ocr([0, 0, 5000, 5000], page_wise_words)
            result = keyword_helper(all_text)
            if result.get("all_present",False):
                all_present = True
            if result.get("balance_present",False):
                balance_present_ever = True
            if result.get("date_present",False):
                date_present_ever = True
            if result.get("amount_present",False):
                amount_present_ever = True
    if amount_present_ever and balance_present_ever and date_present_ever:
        all_present = True
    return {
        "amount_present": amount_present_ever,
        "balance_present": balance_present_ever,
        "date_present": date_present_ever,
        "all_present": all_present
    }

def get_micr_ocr(bbox, words):
    for page_wise_words in words:
        for template in bbox:
            if 'image_flag' not in template.keys():
                continue
            micr = get_temp_micr_ocr(template, page_wise_words)
            if len(micr) > 2:
                return micr
    return None

def get_temp_micr_ocr(template, words):
    micr = ''
    all_text = get_text_in_box_ocr(template.get('bbox'), words)

    regex = template.get('regex')

    all_text = remove_unicode(all_text)
    all_text = all_text.replace('\n', '')
    if regex is not None and all_text is not None:
        regex_match = re.match(regex, all_text)

        if regex_match is not None:
            micr = regex_match.group(1)
    return re.sub(r'\s+', ' ', micr)

def get_ifsc_ocr(bbox, words):
    for page_wise_words in words:
        for template in bbox:
            if 'image_flag' not in template.keys():
                continue
            ifsc = get_temp_ifsc_ocr(template, page_wise_words)
            if len(ifsc) > 2:
                return ifsc
    return None

def get_temp_ifsc_ocr(template, words):
    ifsc = ''
    all_text = get_text_in_box_ocr(template.get('bbox'), words)
    regex = template.get('regex')

    all_text = remove_unicode(all_text)
    all_text = all_text.replace('\n', ' ')
    if regex is not None and all_text is not None:
        regex_match = re.match(regex, all_text)

        if regex_match is not None:
            ifsc = regex_match.group(1)
    return re.sub(r'\s+', ' ', ifsc)

def get_address_ocr(bbox, words):
    for page_wise_words in words:
        for template in bbox:
            if 'image_flag' not in template.keys():
                continue
            address = get_temp_address_ocr(template, page_wise_words)
            if len(address) > 5:
                return address.strip()
    return None

def get_temp_address_ocr(template, words):
    address = ''
    all_text = get_text_in_box_ocr(template.get('bbox'), words)
    
    regex = template.get('regex')
    all_text = remove_unicode(all_text)

    if regex is not None and all_text is not None:
        regex_match = re.match(regex, all_text)
        if regex_match is not None:
            address = regex_match.group(1)

    expr = re.compile('\d{2}/\d{2}/\d{4}')
    address1 = re.sub(expr, '', address)
    address = re.sub('\\s+', ' ', address1)
    return address

def get_name_ocr(bbox, words):
    for page_wise_words in words:
        for template in bbox:
            if 'image_flag' not in template.keys():
                continue
            name = get_temp_name_ocr(template, page_wise_words)
            if len(name.replace(' ', '')) > 2 and (len(name) <= 60):
                return name.strip()
    return None

def get_temp_name_ocr(template, words):
    name = ''
    all_text = get_text_in_box_ocr(template['bbox'], words)
    all_text = remove_unicode(all_text)
    regex = template.get('regex')

    if regex is not None and all_text is not None:
        regex_match = re.match(regex, all_text)
        if regex_match is not None:
            name = regex_match.group(1)

    return re.sub(r'(\n|\s)+', ' ', name)

def get_account_num_ocr(bbox, words):
    for page_wise_words in words:
        for template in bbox:
            if 'image_flag' not in template.keys():
                continue
            acc_num = get_temp_account_number_ocr(template, page_wise_words)
            acc_num = acc_num.replace(" ","")
            if len(acc_num) > 3:
                return acc_num
    return None

def get_temp_account_number_ocr(template, words):
    acc_num = ''
    all_text = get_text_in_box_ocr(template.get('bbox'), words)
    if all_text is not None:
        all_text = all_text.replace('\n', '').replace(' ','')

    regex = template.get('regex')
    all_text = remove_unicode(all_text)
    if regex is not None and all_text is not None:
        regex_match = re.match(regex, all_text)
        if regex_match is not None:
            acc_num = regex_match.group(1)
    return acc_num.strip()

def get_text_in_box_ocr(box, words):
    rect = fitz.Rect(box)

    extracted_words = [list(w) for w in words if fitz.Rect(w[:4]) in rect]
    extracted_words = de_dup_words(extracted_words)
    extracted_words = get_sorted_boxes(extracted_words)
    
    group = groupby(extracted_words, key=itemgetter(3))

    string_list = list()
    for y1, g_words in group:
        string_list.append(" ".join(w[4] for w in g_words))
    return '\n'.join(string_list)

def get_text_words_ocr(page, result_array=None, index=None):
    image = np.array(page) 
    gray_image = cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)
    threshold_img = cv2.threshold(gray_image, 0, 255, cv2.THRESH_BINARY | cv2.THRESH_OTSU)[1]
    custom_config = r'--oem 3 --psm 6'
    page_data = pytesseract.image_to_data(threshold_img, config = custom_config)
    got_cols = False
    for line in str(page_data).splitlines():
        if not got_cols:
            df = pd.DataFrame(columns=line.split())
            got_cols = True
        elif len(line.split()) == len(df.columns):
            df.loc[len(df)] = line.split()
    df = df[["left", "top", "width", "height", "text"]]
    df = df.astype({"left":int, "top":int, "width":int, "height":int})
    df = df.assign(width=lambda row: row["left"] + row["width"], height=lambda row: row["top"] + row["height"])
    df.rename(columns={"left":"x0", "top":"x1", "width":"y0", "height":"y1"}, inplace=True)
    if result_array:
        result_array[index] = list(df.itertuples(index=False, name=None))
    return list(df.itertuples(index=False, name=None))