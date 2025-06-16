import warnings
import pandas as pd
from library.utils import check_date, get_pages
from library.transaction_channel import get_merchant_category
from library.get_edges_test import get_df_graphical_lines, get_lines
from library.table import parse_table
from library.transaction_description import get_transaction_description
from library.utils import add_hash_to_transactions_df, check_semi_date
from library.fitz_functions import read_pdf, get_name, get_address, get_account_num, get_generic_text_from_bbox, get_account_key_words, get_date_range
from library.excel_report.report_formats import transaction_format_func
from library.merchant_category import get_merchant_category_dict
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from library.utils import get_ocr_condition_for_credit_card_statement, check_29th_feb
from library.helpers.constants import DEFAULT_LEAP_YEAR

import datetime
import re
import json, os
import xlsxwriter
import pdf2image, ocrmypdf 


warnings.simplefilter(action = "ignore", category = FutureWarning)
pd.options.mode.chained_assignment = None


END_OF_YEAR = datetime.datetime.strptime("Dec 31", '%b %d')
START_OF_YEAR = datetime.datetime.strptime("Jan 01", '%b %d')


def credit_card_extraction(table, col_names, bank):
    if len(table) == 0 or len(col_names) != len(table[0]):
        return []
    
    index_of_date = col_names.index('date')
    index_of_transaction_note = col_names.index('transaction_note')
    index_of_amount = col_names.index('amount')
    
    final_table = []

    for _ in table:
        # removing special characters from date
        _[index_of_date] = re.sub('[^a-zA-Z0-9\/\:\- \n\.]', '', _[index_of_date])
        is_date, unused_var = check_date(_[index_of_date])
        if is_date:
            obj = {}
            temp_amt = _[index_of_amount]
            transaction_type=None
            
            if isinstance(temp_amt, str):
                temp_amt = temp_amt.replace('₹','')
                if (temp_amt.upper().find('CR') > -1):
                    transaction_type = 'credit'
                elif (temp_amt.upper().find('C') > -1):
                    transaction_type = 'credit'
                elif (temp_amt.upper().find('DR') > -1):
                    transaction_type = 'debit'
                elif (temp_amt.upper().find('D') > -1):
                    transaction_type = 'debit'
                elif (temp_amt.upper().find('+') > -1):
                    transaction_type = 'credit'
                else:
                    transaction_type = 'debit'
                temp_amt = temp_amt.upper().replace('CR.', '').replace("CR",'').replace('C', '').replace("DR.", '').replace("DR",'').replace('D', '').replace(',','')
            try:
                obj['amount'] = float(temp_amt)
                if transaction_type == 'credit':
                    obj['amount'] = -1*obj['amount']
                # obj['amount'] = validate_amount(temp_amt)
            except Exception:
                # not a proper amount object
                continue
            obj['transaction_type']=transaction_type
            obj['date']=is_date
            obj['transaction_note']=_[index_of_transaction_note]
        
            final_table.append(obj)
    
    return final_table

def extract_transactions_from_cc_statement(path, password, bank, page, extraction_parameter, page_num, date_bbox=[]):

    return_data_page = []
    page_coordinates = page.mediabox
    width = page_coordinates.width
    height = page_coordinates.height
    list_of_y = None

    transaction_template = None
    all_pages = get_pages(path, password)
    p_page = all_pages[page_num]
    p_page_edges = p_page.edges

    for index, each_parameter in enumerate(extraction_parameter):
        transaction_list = []
        vertical_lines = each_parameter['vertical_lines']
        horizontal = each_parameter.get('horizontal_lines')
        signed_amount = each_parameter.get('signed_amount')
        uuid = each_parameter.get('uuid')
        last_page = False
    
    # image_flag required in extraction of OCR transactions
        image_flag = each_parameter.get('image_flag', False)
        # range_involved is required to merge transactions having y co-ordinate difference of 1 
        range_involved = each_parameter.get('range', False)
        try:
            if vertical_lines == True:
                if horizontal == True:
                    tables, last_page = get_df_graphical_lines(path, password, page_num, horizontal=True, vertical=True, plumber_page_edges=p_page_edges)
                elif horizontal == 'Text':
                    tables, last_page = get_df_graphical_lines(path, password, page_num, horizontal='Text', vertical=True, plumber_page_edges=p_page_edges)
                else:
                    horizontal_lines, vertical_lines = get_lines(path, password, page_num, horizontal=False, vertical=True, plumber_page_edges=p_page_edges)
                    if len(vertical_lines) > 0:
                        vertical_lines.pop(0)
                    if len(vertical_lines) > 0:
                        vertical_lines.pop(len(vertical_lines) - 1)
                    for line_iterator in range(0, len(vertical_lines)):
                        vertical_lines[line_iterator] = int(
                            vertical_lines[line_iterator])
                    tables, list_of_y, last_page = parse_table(page, [0, 0, width, height], columns=vertical_lines, image_flag=image_flag, range_involved=range_involved)
            elif horizontal == True:
                tables, last_page = get_df_graphical_lines(path, password, page_num, horizontal=True, vertical=vertical_lines, plumber_page_edges=p_page_edges)
            elif horizontal == 'Text':
                tables, last_page = get_df_graphical_lines(path, password, page_num, horizontal='Text', vertical=vertical_lines, plumber_page_edges=p_page_edges)
            else:
                tables, list_of_y, last_page = parse_table(page, [0, 0, width, height], columns=vertical_lines, image_flag=image_flag, range_involved=range_involved)
        except Exception as e:
            print(e)
            # skipping templates for which pdf gives KeyError while fetching tables
            # also password incorrect errors or parsing errors while fetching tables (when plumber is used)
            # also assertion errors in understanding fonts
            continue
    
        if bank in ['citi', 'amex']:
            txn_df = pd.DataFrame(tables)
            if txn_df.shape[1] == len(each_parameter['column']):
                txn_df.columns = each_parameter['column']
            
            doc = read_pdf(path, password)
            if len(date_bbox)==0:
                file_path = 'library/bank_data/'+bank+'_cc.json'

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
            else:
                data = {'date_bbox':date_bbox}

            date_range, date_range_template_uuid = get_date_range(doc, bank, data.get('date_bbox', []), False)
            if date_range['from_date'] is not None:
                from_date = datetime.datetime.strptime(date_range['from_date'], '%Y-%m-%d')
                if date_range['to_date'] is not None:
                    to_date = datetime.datetime.strptime(date_range['to_date'], '%Y-%m-%d')
                    txn_df.apply(lambda row: complete_cc_dates(row, from_date, to_date), axis=1)
            tables = txn_df.values.tolist()
        cc_transactions = credit_card_extraction(tables, each_parameter['column'] ,bank)
        if len(cc_transactions)>len(return_data_page):
            return_data_page = cc_transactions
            transaction_template = each_parameter
    
    df = pd.DataFrame(return_data_page)
    extracted_template_id = None
    if transaction_template!=None:
        extracted_template_id = transaction_template.get("uuid", None)
    # df = get_transaction_channel(df, bank)
    # df = get_transaction_description(df, '')

    # populate merchant category
    merchant_category_dict = get_merchant_category_dict()
    merchant_categories = merchant_category_dict.keys()
    tag_merchant_categories = [_ for _ in merchant_categories if "_regex" not in _]
    regex_merchant_categories = [_ for _ in merchant_categories if "_regex" in _]
    df = df.apply(lambda x: get_merchant_category(x, tag_merchant_categories, regex_merchant_categories, merchant_category_dict, True), axis=1)
    df = add_hash_to_transactions_df(df)
    return df.to_dict('records'), extracted_template_id

def get_cc_transactions_using_fitz(path, password, bank, page_num, template={}):
    doc = read_pdf(path, password)
    
    if isinstance(doc, int):
        return [], None
    
    date_bbox = [] if not isinstance(template,dict) or template=={} else template.get('date_bbox')
    trans_bbox = [] if not isinstance(template,dict) or template=={} else template.get('trans_bbox')
    num_pages = doc.page_count
    all_transactions = []
    if page_num < num_pages:  
        relevant_page = doc[page_num]
        if len(trans_bbox)==0:
            file_path = 'library/bank_data/' + bank + '_cc.json'
            if os.path.exists(file_path):
                with open(file_path, 'r') as data_file:
                    try:
                        data = json.load(data_file)
                        extraction_parameter = data.get('trans_bbox', [])
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
        else:
            extraction_parameter=trans_bbox
        
        all_transactions, template_id = extract_transactions_from_cc_statement(path, password, bank, relevant_page, extraction_parameter, page_num, date_bbox)
    return all_transactions, template_id

def perform_ocr_on_page(processing_object):
    page_number = processing_object.get('page_number')
    template = processing_object.get('template')
    main_file_path = processing_object.get('path')
    password = processing_object.get('password')
    bank = processing_object.get('bank')

    output_file_path = '/tmp/'

    time_now = datetime.datetime.now()

    paths_to_images = pdf2image.convert_from_path(
        main_file_path, dpi=250, userpw=password, output_folder=output_file_path, 
        paths_only=True, fmt="jpeg", first_page=page_number+1, last_page=page_number+1,
        grayscale=True, transparent=True, ownerpw=password
    )

    identity = {}
    template_info = {}
    if len(paths_to_images)>0:
        ocr_file_path = paths_to_images[0].replace('.jpg', '.pdf')
        ocrmypdf.ocr(
            paths_to_images[0],
            ocr_file_path,
            deskew=True,
            force_ocr=True,
            progress_bar=False
        )
        page_doc = read_pdf(ocr_file_path,password)
        if not isinstance(page_doc,int):
            identity['credit_card_number'], credit_card_number_template_uuid, _ = get_account_num(page_doc, template.get('card_number_bbox', []), bank, path=main_file_path, is_credit_card=True)
            template_info['credit_card_number'] = {'template_uuid': credit_card_number_template_uuid, 'page_number': page_number}

            identity['name'], credit_card_name_template_uuid, _ = get_name(page_doc, template.get('name_bbox', []), bank, path=main_file_path, is_credit_card=True)
            template_info['name'] = {'template_uuid': credit_card_name_template_uuid, 'page_number': page_number}

            identity['address'], credit_card_address_template_uuid, _ = get_address(page_doc, template.get('address_bbox', []), path=main_file_path, is_credit_card=True)
            template_info['address'] = {'template_uuid': credit_card_address_template_uuid, 'page_number': page_number}

            identity['payment_due_date'], pdd_template_uuid, _ = get_generic_text_from_bbox(page_doc, template.get('payment_due_date', []), 'date', path=main_file_path, is_credit_card=True)
            template_info['payment_due_date'] = {'template_uuid': pdd_template_uuid, 'page_number': page_number}

            identity['total_dues'], total_dues_template_uuid, _ = get_generic_text_from_bbox(page_doc, template.get('total_dues', []), path=main_file_path, is_credit_card=True)
            template_info['total_dues'] = {'template_uuid': total_dues_template_uuid, 'page_number': page_number}

            identity['min_amt_due'], min_amt_template_uuid, _ = get_generic_text_from_bbox(page_doc, template.get('min_amt_due', []), path=main_file_path, is_credit_card=True)
            template_info['min_amt_due'] = {'template_uuid': min_amt_template_uuid, 'page_number': page_number}

            identity['credit_limit'], credit_lim_template_uuid, _ = get_generic_text_from_bbox(page_doc, template.get('credit_limit', []), path=main_file_path, is_credit_card=True)
            template_info['credit_limit'] = {'template_uuid': credit_lim_template_uuid, 'page_number': page_number}

            identity['avl_credit_limit'], avl_credit_lim_template_uuid, _ = get_generic_text_from_bbox(page_doc, template.get('avl_credit_limit', []), path=main_file_path, is_credit_card=True)
            template_info['avl_credit_limit'] = {'template_uuid': avl_credit_lim_template_uuid, 'page_number': page_number}

            identity['avl_cash_limit'], avl_cash_lim_template_uuid, _ = get_generic_text_from_bbox(page_doc, template.get('avl_cash_limit', []), path=main_file_path, is_credit_card=True)
            template_info['avl_cash_limit'] = {'template_uuid': avl_cash_lim_template_uuid, 'page_number': page_number}

            identity['opening_balance'], open_bal_template_uuid, _ = get_generic_text_from_bbox(page_doc, template.get('opening_balance', []), path=main_file_path, is_credit_card=True)
            template_info['opening_balance'] = {'template_uuid': open_bal_template_uuid, 'page_number': page_number}

            identity['payment_or_credits'], pay_cred_template_uuid, _ = get_generic_text_from_bbox(page_doc, template.get('payment/credits', []), path=main_file_path, is_credit_card=True, template_type='payment/credits')
            template_info['payment_or_credits'] = {'template_uuid': pay_cred_template_uuid, 'page_number': page_number}

            identity['purchase_or_debits'], pur_deb_template_uuid, _ = get_generic_text_from_bbox(page_doc, template.get('purchase/debits', []), path=main_file_path, is_credit_card=True, template_type='purchase/debits')
            template_info['purchase_or_debits'] = {'template_uuid': pur_deb_template_uuid, 'page_number': page_number}

            identity['finance_charges'], fin_charges_template_uuid, _ = get_generic_text_from_bbox(page_doc, template.get('finance_charges', []), path=main_file_path, is_credit_card=True)
            template_info['finance_charges'] = {'template_uuid': fin_charges_template_uuid, 'page_number': page_number}

            identity['statement_date'], stat_date_template_uuid, _ = get_generic_text_from_bbox(page_doc, template.get('statement_date', []), 'date', path=main_file_path, is_credit_card=True)
            template_info['statement_date'] = {'template_uuid': stat_date_template_uuid, 'page_number': page_number}

            identity['card_type'], card_type_template_uuid, _ = get_generic_text_from_bbox(page_doc, template.get('card_type_bbox', []), path=main_file_path, is_credit_card=True, template_type='card_type_bbox', bank_name=bank)
            template_info['card_type'] = {'template_uuid': card_type_template_uuid, 'page_number': page_number}
            
            rewards_opening_balance, reward_opening_bal_template_uuid, _ = get_generic_text_from_bbox(page_doc, template.get('rewards_opening_balance_bbox', []), path=main_file_path, is_credit_card=True)
            template_info['rewards_opening_balance'] = {'template_uuid': reward_opening_bal_template_uuid, 'page_number': page_number}

            rewards_closing_balance, reward_closing_bal_template_uuid, _ = get_generic_text_from_bbox(page_doc, template.get('rewards_closing_balance_bbox', []), path=main_file_path, is_credit_card=True)
            template_info['rewards_closing_balance'] = {'template_uuid': reward_closing_bal_template_uuid, 'page_number': page_number}

            rewards_points_expired, reward_expired_template_uuid, _ = get_generic_text_from_bbox(page_doc, template.get('rewards_points_expired_bbox', []), path=main_file_path, is_credit_card=True)
            template_info['rewards_points_expired'] = {'template_uuid': reward_expired_template_uuid, 'page_number': page_number}

            rewards_points_claimed, reward_claimed_template_uuid, _ = get_generic_text_from_bbox(page_doc, template.get('rewards_points_claimed_bbox', []), path=main_file_path, is_credit_card=True)
            template_info['rewards_points_claimed'] = {'template_uuid': reward_claimed_template_uuid, 'page_number': page_number}

            rewards_points_credited, reward_cred_template_uuid, _ = get_generic_text_from_bbox(page_doc, template.get('rewards_points_credited_bbox', []), path=main_file_path, is_credit_card=True)
            template_info['rewards_points_credited'] = {'template_uuid': reward_cred_template_uuid, 'page_number': page_number}

            identity['rewards'] = {
                'opening_balance': rewards_opening_balance,
                'closing_balance': rewards_closing_balance,
                'points_expired': rewards_points_expired,
                'points_claimed': rewards_points_claimed,
                'points_credited': rewards_points_credited
            }
    
        if os.path.exists(ocr_file_path):
            os.remove(ocr_file_path)
        if os.path.exists(paths_to_images[0]):
            os.remove(paths_to_images[0])

    print(f'page : {page_number} took {datetime.datetime.now()-time_now}')
    return identity, template_info
    

def ocr_cc_identity(path, password, bank, data, image_hash_list):
    pages_to_explore = [
        {
            "password": password,
            "path": path,
            "template": data,
            "page_number": 0
        },
        {
            "password": password,
            "path": path,
            "template": data,
            "page_number": 1
        }
    ]

    starttime = datetime.datetime.now()

    with ThreadPoolExecutor(max_workers=3) as executor:
        identity_objects = executor.map(perform_ocr_on_page, pages_to_explore)
    
    identity = {
        'bank': bank
    }

    template_info = {}
    identity_parameters = ['credit_card_number','name','address','payment_due_date','total_dues','min_amt_due','credit_limit','avl_credit_limit','avl_cash_limit','opening_balance','payment_or_credits','purchase_or_debits','finance_charges','statement_date','card_type','rewards']
    for identity_object in identity_objects:
        for identity_paramter in identity_parameters:
            if identity.get(identity_paramter, None) == None:
                identity[identity_paramter] = identity_object.get(identity_paramter, None)

    identity["is_calculated_payment_due_date"] = False
    statement_date = identity['statement_date']
    payment_due_date = identity['payment_due_date']
    if payment_due_date in ['', None] and isinstance(statement_date, datetime.datetime):
        payment_due_date = statement_date + datetime.timedelta(days=20)
        identity['payment_due_date'] = payment_due_date
        identity["is_calculated_payment_due_date"] = True
    
    if identity.get('card_type', None) is None:
        identity['card_type'] = get_card_type_from_hash(image_hash_list, data.get('card_type_hash', dict()))

    print(f'everything took : {datetime.datetime.now()-starttime}')
    print(f'identity extracted --> {identity}')
    return identity, template_info

def extract_cc_identity(path, password, bank, template={}, image_hash_list=[], statement_meta_data_for_warehousing = dict()):

    template_info = {
        'credit_card_number':{'template_uuid':'', 'page_number':''},
        'name':{'template_uuid':'', 'page_number':''},
        'address':{'template_uuid':'', 'page_number':''},
        'payment_due_date':{'template_uuid':'', 'page_number':''},
        'total_dues':{'template_uuid':'', 'page_number':''},
        'min_amt_due':{'template_uuid':'', 'page_number':''},
        'credit_limit':{'template_uuid':'', 'page_number':''},
        'avl_credit_limit':{'template_uuid':'', 'page_number':''},
        'avl_cash_limit':{'template_uuid':'', 'page_number':''},
        'opening_balance':{'template_uuid':'', 'page_number':''},
        'payment_or_credits':{'template_uuid':'', 'page_number':''},
        'purchase_or_debits':{'template_uuid':'', 'page_number':''},
        'finance_charges':{'template_uuid':'', 'page_number':''},
        'statement_date':{'template_uuid':'', 'page_number':''},
        'card_type':{'template_uuid':'', 'page_number':''},
        'rewards_opening_balance':{'template_uuid':'', 'page_number':''},
        'rewards_closing_balance':{'template_uuid':'', 'page_number':''},
        'rewards_points_expired':{'template_uuid':'', 'page_number':''},
        'rewards_points_claimed':{'template_uuid':'', 'page_number':''},
        'rewards_points_credited':{'template_uuid':'', 'page_number':''}
    }

    result_dict = {
        'identity':{
            'bank': bank,
            'credit_card_number':None,
            'name':None,
            'address':None,
            'payment_due_date':None,
            'total_dues':None,
            'min_amt_due':None,
            'credit_limit':None,
            'avl_credit_limit':None,
            'avl_cash_limit':None,
            'opening_balance':None,
            'payment_or_credits':None,
            'purchase_or_debits':None,
            'finance_charges':None,
            'statement_date':None,
            'card_type':None,
            'is_calculated_payment_due_date': False,
            'date_range': {
                'from_date': None,
                'to_date': None
            },
            'rewards':{
                'opening_balance':None,
                'closing_balance':None,
                'points_expired':None,
                'points_claimed':None,
                'points_credited':None
            }
        },
        'meta' : {
            'is_image': False,
            'password_incorrect': False,
            'all_present': False
        }
    }

    doc = read_pdf(path, password)

    if isinstance(doc, int):
        if doc == -1:
            result_dict["meta"]['is_image'] = True
        else:
            result_dict["meta"]['password_incorrect'] = True

        statement_meta_data_for_warehousing['template_info'] = template_info
        statement_meta_data_for_warehousing['identity'] = result_dict.get('identity', dict())
        return result_dict
    
    is_all_text_zero = False
    if len(doc)>0:
        all_text_in_0th_page = doc[0].get_text()
        if len(all_text_in_0th_page)==0 or get_ocr_condition_for_credit_card_statement(doc, page_number=0):
            print(f'found no text in the first page, doing ocr : {path}')
            is_all_text_zero=True
    
    keyword_check = get_account_key_words(doc)
    if not is_all_text_zero:
        result_dict['meta']['is_ocr'] = False
        if keyword_check.get("amount_present") and keyword_check.get("date_present"):
            result_dict["meta"]['all_present'] = True
        else:
            result_dict["meta"]['all_present'] = False
            statement_meta_data_for_warehousing['template_info'] = template_info
            statement_meta_data_for_warehousing['identity'] = result_dict.get('identity', dict())
            return result_dict
    else:
        result_dict['meta']['all_present'] = True
        result_dict['meta']['is_ocr'] = True
    
    # here we will get all templates from db
    if template is None or len(template.get('card_number_bbox',[]))==0:
        file_path = 'library/bank_data/'+bank+'_cc.json'
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
        
    else:
        data = template

    if is_all_text_zero:

        # When using this function, check all the functionality is working 
        # result_dict['identity']=ocr_cc_identity(path, password, bank, data, image_hash_list)
        statement_meta_data_for_warehousing['template_info'] = template_info
        statement_meta_data_for_warehousing['identity'] = result_dict.get('identity', dict())
        return result_dict
    
    credit_card_number, credit_card_number_template_uuid, credit_card_number_page_number = get_account_num(doc, data.get('card_number_bbox', []), bank, path, is_credit_card=True)
    template_info['credit_card_number'] = {'template_uuid': credit_card_number_template_uuid, 'page_number': credit_card_number_page_number}
    if credit_card_number==None:
        result_dict['identity']['credit_card_number']=credit_card_number
        statement_meta_data_for_warehousing['template_info'] = template_info
        statement_meta_data_for_warehousing['identity'] = result_dict.get('identity', dict())
        return result_dict
    else:
        result_dict['identity']['credit_card_number']=credit_card_number.replace('-','')
    
    result_dict["identity"]['name'], credit_card_name_template_uuid, credit_card_name_page_number = get_name(doc, data.get('name_bbox', []), bank, path=path, is_credit_card=True)
    template_info['name'] = {'template_uuid': credit_card_name_template_uuid, 'page_number': credit_card_name_page_number}

    result_dict["identity"]['address'], credit_card_address_template_uuid, credit_card_address_page_number = get_address(doc, data.get('address_bbox', []), path=path, is_credit_card=True)
    template_info['address'] = {'template_uuid': credit_card_address_template_uuid, 'page_number': credit_card_address_page_number}

    result_dict["identity"]['payment_due_date'], pdd_template_uuid, pdd_page_num = get_generic_text_from_bbox(doc, data.get('payment_due_date', []), 'date', path=path, is_credit_card=True, template_type='payment_due_date')
    template_info['payment_due_date'] = {'template_uuid': pdd_template_uuid, 'page_number': pdd_page_num}

    result_dict["identity"]['total_dues'], total_dues_template_uuid, total_dues_page_num = get_generic_text_from_bbox(doc, data.get('total_dues', []), path=path, is_credit_card=True, template_type='total_dues')
    template_info['total_dues'] = {'template_uuid': total_dues_template_uuid, 'page_number': total_dues_page_num}

    result_dict["identity"]['min_amt_due'], min_amt_template_uuid, min_amt_page_num = get_generic_text_from_bbox(doc, data.get('min_amt_due', []), path=path, is_credit_card=True, template_type='min_amt_due')
    template_info['min_amt_due'] = {'template_uuid': min_amt_template_uuid, 'page_number': min_amt_page_num}

    result_dict["identity"]['credit_limit'], credit_lim_template_uuid, credit_lim_page_num = get_generic_text_from_bbox(doc, data.get('credit_limit', []), path=path, is_credit_card=True, template_type='credit_limit')
    template_info['credit_limit'] = {'template_uuid': credit_lim_template_uuid, 'page_number': credit_lim_page_num}

    result_dict["identity"]['avl_credit_limit'], avl_credit_lim_template_uuid, avl_credit_lim_page_num = get_generic_text_from_bbox(doc, data.get('avl_credit_limit', []), path=path, is_credit_card=True, template_type='avl_credit_limit')
    template_info['avl_credit_limit'] = {'template_uuid': avl_credit_lim_template_uuid, 'page_number': avl_credit_lim_page_num}

    result_dict["identity"]['avl_cash_limit'], avl_cash_lim_template_uuid, avl_cash_limt_page_num = get_generic_text_from_bbox(doc, data.get('avl_cash_limit', []), path=path, is_credit_card=True, template_type='avl_cash_limit')
    template_info['avl_cash_limit'] = {'template_uuid': avl_cash_lim_template_uuid, 'page_number': avl_cash_limt_page_num}

    result_dict["identity"]['opening_balance'], open_bal_template_uuid, open_bal_page_num = get_generic_text_from_bbox(doc, data.get('opening_balance', []), path=path, is_credit_card=True, template_type='opening_balance')
    template_info['opening_balance'] = {'template_uuid': open_bal_template_uuid, 'page_number': open_bal_page_num}

    result_dict["identity"]['payment_or_credits'], pay_cred_template_uuid, pay_cred_page_num  = get_generic_text_from_bbox(doc, data.get('payment/credits', []), path=path, is_credit_card=True, template_type='payment/credits')
    template_info['payment_or_credits'] = {'template_uuid': pay_cred_template_uuid, 'page_number': pay_cred_page_num}

    result_dict["identity"]['purchase_or_debits'], pur_deb_template_uuid, pur_deb_page_num = get_generic_text_from_bbox(doc, data.get('purchase/debits', []), path=path, is_credit_card=True, template_type='purchase/debits')
    template_info['purchase_or_debits'] = {'template_uuid': pur_deb_template_uuid, 'page_number': pur_deb_page_num}

    result_dict["identity"]['finance_charges'], fin_charges_template_uuid, fin_charges_page_num = get_generic_text_from_bbox(doc, data.get('finance_charges', []), path=path, is_credit_card=True, template_type='finance_charges')
    template_info['finance_charges'] = {'template_uuid': fin_charges_template_uuid, 'page_number': fin_charges_page_num}

    result_dict["identity"]['statement_date'], stat_date_template_uuid, stat_date_page_num = get_generic_text_from_bbox(doc, data.get('statement_date', []), 'date', path=path, is_credit_card=True, template_type='statement_date')
    template_info['statement_date'] = {'template_uuid': stat_date_template_uuid, 'page_number': stat_date_page_num}

    result_dict["identity"]['card_type'], card_type_template_uuid, card_type_page_num = get_generic_text_from_bbox(doc, data.get('card_type_bbox', []), path=path, is_credit_card=True, template_type='card_type_bbox', bank_name=bank)
    template_info['card_type'] = {'template_uuid': card_type_template_uuid, 'page_number': card_type_page_num}

    if result_dict["identity"]['card_type'] == None:
        result_dict['identity']['card_type'] = get_card_type_from_hash(image_hash_list, data.get('card_type_hash', dict()))
        template_info['card_type'] = {'template_uuid': 'from_hash', 'page_number': 0}

    rewards_opening_balance, reward_opening_bal_template_uuid, reward_opening_bal_page_num = get_generic_text_from_bbox(doc, template.get('rewards_opening_balance_bbox', []), path=path, is_credit_card=True, template_type='rewards_opening_balance_bbox')
    template_info['rewards_opening_balance'] = {'template_uuid': reward_opening_bal_template_uuid, 'page_number': reward_opening_bal_page_num}

    rewards_closing_balance, reward_closing_bal_template_uuid, reward_closing_bal_page_num = get_generic_text_from_bbox(doc, template.get('rewards_closing_balance_bbox', []), path=path, is_credit_card=True, template_type='rewards_closing_balance_bbox')
    template_info['rewards_closing_balance'] = {'template_uuid': reward_closing_bal_template_uuid, 'page_number': reward_closing_bal_page_num}

    rewards_points_expired, reward_expired_template_uuid, reward_expired_page_num = get_generic_text_from_bbox(doc, template.get('rewards_points_expired_bbox', []), path=path, is_credit_card=True, template_type='rewards_points_expired_bbox')
    template_info['rewards_points_expired'] = {'template_uuid': reward_expired_template_uuid, 'page_number': reward_expired_page_num}

    rewards_points_claimed, reward_claimed_template_uuid, reward_claimed_page_num = get_generic_text_from_bbox(doc, template.get('rewards_points_claimed_bbox', []), path=path, is_credit_card=True, template_type='rewards_points_claimed_bbox')
    template_info['rewards_points_claimed'] = {'template_uuid': reward_claimed_template_uuid, 'page_number': reward_claimed_page_num}

    rewards_points_credited, reward_cred_template_uuid, reward_cred_page_num = get_generic_text_from_bbox(doc, template.get('rewards_points_credited_bbox', []), path=path, is_credit_card=True, template_type='rewards_points_credited_bbox')
    template_info['rewards_points_credited'] = {'template_uuid': reward_cred_template_uuid, 'page_number': reward_cred_page_num}

    result_dict['identity']['rewards'] = {
        'opening_balance': rewards_opening_balance,
        'closing_balance': rewards_closing_balance,
        'points_expired': rewards_points_expired,
        'points_claimed': rewards_points_claimed,
        'points_credited': rewards_points_credited
    }

    date_range, date_range_template_uuid = get_date_range(doc, bank, data.get('date_bbox', []), False)
    if date_range!=None:
        from_date = date_range.get('from_date',None)
        to_date = date_range.get('to_date',None)
        if from_date!=None and to_date!=None:
            result_dict['identity']['date_range'] = {'from_date': from_date, 'to_date':to_date}
    
    result_dict["identity"]["is_calculated_payment_due_date"] = False
    statement_date = result_dict["identity"]['statement_date']
    payment_due_date = result_dict["identity"]['payment_due_date']
    if payment_due_date in ['', None] and isinstance(statement_date, datetime.datetime):
        payment_due_date = statement_date + datetime.timedelta(days=20)
        result_dict["identity"]['payment_due_date'] = payment_due_date
        result_dict["identity"]["is_calculated_payment_due_date"] = True

    statement_meta_data_for_warehousing['template_info'] = template_info
    statement_meta_data_for_warehousing['identity'] = result_dict.get('identity', dict())

    return result_dict

def generate_cc_report(identity_dict, transactions, output_file_name):

    if isinstance(identity_dict.get('payment_due_date'), datetime.datetime):
        identity_dict['payment_due_date'] = identity_dict['payment_due_date'].strftime("%d-%m-%Y")
    
    validated_transactions = []
    for row in transactions: 
        if not isinstance(row['date'], str):
            row['date'] = row['date'].strftime("%d-%m-%Y")
        validated_transactions.append(row)
    
    workbook = xlsxwriter.Workbook(output_file_name, {'strings_to_numbers': True})
    
    transaction_headings = {
        'Sl. No.': None,
        'Date': 'date',
        'Transaction Note': 'transaction_note',
        'Amount': 'amount',
        'Merchant Category': 'merchant_category',
        'Merchant': 'merchant',
        'Transaction Type': 'transaction_type'
    }

    width_dict = {
        'Sl. No.': [5, 'right'],
        'Date': [10, 'left'],
        'Transaction Note': [100, 'right'],
        'Amount': [10, 'left'],
        'Merchant Category': [12, 'right'],
        'Transaction Type': [15, 'right'],
        'Merchant': [12, 'right']
    }

    cyan_bg = workbook.add_format({'bg_color': '#CCFFFF', 'align': 'center', 'border': 1})
    normal_bg = workbook.add_format({'align': 'center', 'border': 1})
    primary_heading = workbook.add_format({'font_color': '#000000', 'bg_color': '#FFFFFF', 'valign': 'vcenter', 'border': 1, 'font_size': 14})
    green_heading = workbook.add_format({'font_color': '#000000', 'bg_color': '#e2efd9', 'valign': 'vcenter', 'border': 1, 'font_size': 9, })
    light_blue_cell = workbook.add_format({'font_color': '#000000', 'bg_color': '#F1f1f1', 'valign': 'vcenter', 'border': 1, 'font_size': 9})

    overview_page = workbook.add_worksheet('Overview')
    transactions_page = workbook.add_worksheet('Transactions')

    # Overview page items
    overview_page.write('A1', 'Summary', primary_heading)
    overview_page.set_column('A:A', 1)
    summary_params = (
        ['Name of the Account Holder',  'name'],
        ['Address', 'address'],
        ['Name of the Bank', 'bank'],
        ['Credit Card Number', 'credit_card_number'],
        ['Credit Card Type', 'card_type']
    )
    row, col = 1, 1
    for heading, value in summary_params:
        overview_page.set_column(col, col, 50)
        overview_page.write(row, col, heading, green_heading)
        overview_page.merge_range(row, col+1, row, col+5, str(identity_dict.get(value, '')), light_blue_cell)
        row += 1
    
    overview_page.write('A8', 'Credit Card Statement', primary_heading)
    row, col = 8, 1

    cc_statement_params = (
        ['Payment Due Date',  'payment_due_date'],
        ['Total Dues', 'total_dues'],
        ['Minimum Amount Due', 'min_amt_due'],
        ['Credit Limit', 'credit_limit'],
        ['Account Category', 'account_category'],
        ['Available Credit Limit', 'avl_credit_limit'],
        ['Available Cash Limit', 'avl_cash_limit']
    )
    for heading, value in cc_statement_params:
        overview_page.set_column(col, col, 50)
        overview_page.write(row, col, heading, green_heading)
        overview_page.merge_range(row, col+1, row, col+5, str(identity_dict.get(value, 0)), light_blue_cell)
        row += 1


    overview_page.write('A16', 'Account Summary', primary_heading)
    row, col = 16, 1

    acc_summary_params = (
        ['Opening Balance',  'opening_balance'],
        ['Payment/ Credits', 'payment_or_credits'],
        ['Purchase/ Debits', 'purchase_or_debits'],
        ['Finance Charges', 'finance_charges'],
        ['Total Dues', 'total_dues'],
    )
    for heading, value in acc_summary_params:
        overview_page.set_column(col, col, 50)
        overview_page.write(row, col, heading, green_heading)
        overview_page.merge_range(row, col+1, row, col+5, str(identity_dict.get(value, 0)), light_blue_cell)
        row += 1

    overview_page.write('A22', 'Rewards Summary', primary_heading)
    row, col = 22, 1

    rewards_summary_params = (
        ['Rewards Closing Balance', 'closing_balance'],
        ['Rewards Opening Balance', 'closing_balance'],
        ['Rewards Points Claimed', 'points_claimed'],
        ['Rewards Points Credited', 'points_credited'],
        ['Rewards Points Expired', 'points_expired']
    )
    for heading, value in rewards_summary_params:
        overview_page.set_column(col, col, 50)
        overview_page.write(row, col, heading, green_heading)
        overview_page.merge_range(row, col+1, row, col+5, str(identity_dict.get('rewards', dict()).get(value, None)), light_blue_cell)
        row += 1

    # Transactions page items
    formats = transaction_format_func(workbook)
    for index, item in enumerate(transaction_headings.keys()):
        transactions_page.write_string(0, index, item, formats['horizontal_heading_cell'])
    
    for index, item in enumerate(validated_transactions):
        col_index = 0
        for heading_index in transaction_headings:
            transactions_page.set_column(col_index, col_index, width_dict[heading_index][0])
            color = cyan_bg if (index+1)% 2==0 else normal_bg
            val = ''
            if heading_index in ['Sl. No.']:
                val = str(index + 1)
            else:
                if heading_index=='Date':
                    val = item[transaction_headings[heading_index]]
                    val = datetime.datetime.strptime(val, "%Y-%m-%d %H:%M:%S")
                    val = val.strftime("%d-%b-%y")
                else:
                    val = str(item[transaction_headings[heading_index]])
            transactions_page.write_string(index+1, col_index, val, color) 
            col_index += 1

    workbook.close()

def complete_cc_dates(row, from_date, to_date, country="IN"):
    is_semi_date = check_semi_date(row['date'])
    if is_semi_date:
        year_to_add = ''
        semi_formats = ['%d/%m', '%B %d']
        if str(from_date.year) != str(to_date.year):
            for format in semi_formats:
                try:
                    temp_from_date = datetime.datetime.strptime(from_date.strftime('%b %d'), '%b %d')
                    temp_to_date = datetime.datetime.strptime(to_date.strftime('%b %d'), '%b %d')
                    if check_29th_feb(row['date']):
                        temp_format = format + '-%Y'
                        temp_date = row['date'] + f'-{DEFAULT_LEAP_YEAR}'
                        our_date = datetime.datetime.strptime(temp_date, temp_format)

                        if temp_from_date.month <= our_date.month:
                            year_to_add = str(from_date.year)
                        else:
                            year_to_add = str(to_date.year+1)
                    else:
                        our_date = datetime.datetime.strptime(row['date'], format)

                        if temp_from_date <= our_date <= END_OF_YEAR:
                            year_to_add =  str(from_date.year)
                        elif START_OF_YEAR <= our_date <= temp_to_date:
                            year_to_add = str(to_date.year)

                    if format == '%d/%m':
                        row['date'] += '/'+ year_to_add
                except (ValueError, TypeError):
                    continue
        else:
            for format in semi_formats:
                try:
                    if check_29th_feb(row['date']):
                        temp_format = format + '-%Y'
                        temp_date = row['date'] + f'-{DEFAULT_LEAP_YEAR}'
                        our_date = datetime.datetime.strptime(temp_date, temp_format)
                    else:
                        our_date = datetime.datetime.strptime(row['date'], format)
                    
                    year_to_add = str(from_date.year)

                    if format == '%d/%m':
                        row['date'] += '/'+ year_to_add
                    if format == '%B %d':
                        row['date'] += ' '+ year_to_add
                except (ValueError, TypeError):
                    continue
    return row

def get_card_type_from_hash(hash_list, hash_template):
    if isinstance(hash_template, list):
        if len(hash_template)==0:
            hash_template = {}
        else:
            hash_template = hash_template[0]
    
    for statement_hash in hash_list:
        for hash_item, hash_value in hash_template.items():
            if hash_item == statement_hash:
                return hash_value
    
    return None