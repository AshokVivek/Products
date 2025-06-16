# read the file 
# get the templates
# get the file from s3
# run the statement_date and payment_due_date on it and save to the excell sheet

from app.database_utils import portal_db
from app.conf import *
import os
import pandas as pd
from app.pdf_utils import read_pdf
from datetime import datetime
import fitz
from operator import itemgetter
from itertools import groupby
import re
import csv
import concurrent.futures
import json

template_dict={}

def put_file_to_s3(file_path, bucket, key, metadata = None):
    if metadata is None:
        s3_resource.Bucket(bucket).upload_file(file_path, key)
    else:
        s3_resource.Bucket(bucket).upload_file(file_path, key, ExtraArgs={'Metadata': metadata})
    return "s3://{}/{}".format(bucket, key)

cwd = os.getcwd()
new_data = []
async def get_templates_from_rds_for_payment_due_date_and_statement_date(bank_name):
    portal_query = """
            select * from bank_connect_fsmlibcctemplates where template_type in ('payment_due_date') and bank_name=:bank_name
                    """
    
    portal_query_data = await portal_db.fetch_all(query=portal_query,values={
        'bank_name':bank_name
    })

    payment_due_templates = []
    for item in portal_query_data:
        payment_due_templates.append(dict(item))

    portal_query_sd = """
            select * from bank_connect_fsmlibcctemplates where template_type in ('statement_date') and bank_name=:bank_name
                    """
    
    portal_query_data_sd = await portal_db.fetch_all(query=portal_query_sd,values={
        'bank_name':bank_name
    })

    statement_date_templates = []
    for item in portal_query_data_sd:
        statement_date_templates.append(dict(item))
    
    return_value={
        'payment_due_date':payment_due_templates,
        'statement_date':statement_date_templates
    }

    return return_value


def get_file_from_s3(statement_id,bank_name):
    file_key = f'cc_pdfs/{statement_id}_{bank_name}.pdf'
    try:
        pdf_bucket_response = s3.get_object(Bucket=CC_PDF_BUCKET, Key=file_key)
    except Exception as e:
        print(e)
        return None
    return pdf_bucket_response

async def get_password_from_rds(statement_id):
    portal_query = """
                select pdf_password from bank_connect_creditcardstatement where statement_id=:statement_id
                """
    portal_data = await portal_db.fetch_one(query=portal_query,values={
        'statement_id':statement_id
    })

    if portal_data!=None:
        portal_data = dict(portal_data)
        return portal_data.get('pdf_password')
    return None

EPOCH_DATE = datetime(1970, 1, 1)
def check_date(input_date):
    if not input_date:
        return False
    current_year = datetime.today().year
    epoch_year = EPOCH_DATE.year
    # TODO make sure wrong format does not get captured
    date_formats = ['%B%Y', '%d %m %Y', '%m %d %Y', '%d %m %y', '%m %d %y', '%d %b %Y', '%b %d %Y', '%d %b %y', '%b %d %y',
                    '%Y %m %d', '%B %d %y', '%B %d ,%Y', '%b %d ,%Y', '%d %B %Y', '%B %d %Y']
    separators = ['-', '.', '/', ',', '']

    random_formats = ['%d/%m/%Y%H:%M:%S', '%d/%m/%y(%S/%H/%M)', '%d/%m%Y', '%d%m/%Y', '(%S/%H/%M)%d/%m/%y',
                      '%UI(%S/%H/%M)%d/%m/%y', '0I(%S/%H/%M)%d/%m/%y', '2I(%S/%H/%M)%d/%m/%y', '3I(%S/%H/%M)%d/%m/%y',
                      '4I(%S/%H/%M)%d/%m/%y', '5I(%S/%H/%M)%d/%m/%y', '6I(%S/%H/%M)%d/%m/%y', '7I(%S/%H/%M)%d/%m/%y',
                      '8I(%S/%H/%M)%d/%m/%y', '9I(%S/%H/%M)%d/%m/%y', '%d%b%Y%I:%M%p', '%d/%m/%Y%I:%M:%S%p',
                      '%Y-%m-%d%H:%M:%S', '%d-%m-%Y%H:%M:%S', '%d-%m-%Y%H:%M:%S%p', '%m-%d-%Y%H:%M:%S%p', 
                      '%d%b%y%H:%M', '%d%b,%Y', '%d-%b-%y%H:%M:%S', '%m/%d/%Y%H:%M:%S%p', '%d/%m/%y%H:%M:%S', 
                      '%m/%d/%y%I:%M%p', '%d/%m/%Y%H.%M.%S', '%Y-%m-%dT%H:%M:%S', '%Y-%m-%dT', '%m/%d/%y%H:%M', '%d%B%Y%H:%M:%S']
    all_formats = []

    # weird issue for new india bank where it calls September as Sept
    input_date = input_date.replace("Sept-", "Sep-")

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
                    if ((date.year > current_year - 15) and (date.year < current_year+1)) or (date.year == epoch_year):
                        return date
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
                    if ((date.year > current_year - 15) and (date.year < current_year+1)) or (date.year == epoch_year):
                        return date
                except (ValueError, TypeError):
                    continue
    return False

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

def get_text_in_box(page, box):
    is_rotated = False
    if page.derotation_matrix[5] != 0:
        is_rotated = True

    rect = fitz.Rect(box)
    words = page.get_text_words()

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

def get_generic_text(page, template, get_only_all_text=False):
    # bbox_cords = template.get('bbox')
    # print(bbox_cords)
    # print(type(bbox_cords))

    template_json = json.loads(template.get('template_json'))
    bbox_cords = template_json.get('bbox')
    regex_val = template_json.get("regex")
    all_text = get_text_in_box(page, bbox_cords)
    if all_text is not None:
        all_text = all_text.replace('\n', ' ').replace('(cid:9)', '')
    # print(all_text)
    regex = regex_val

    all_text = remove_unicode(all_text)

    if get_only_all_text:
        return all_text

    if regex is not None and all_text is not None:
        regex_match = re.match(regex, all_text)

        if regex_match is not None:
            account_category = regex_match.group(1)
            return re.sub(r'\s+', ' ', account_category).strip()
    return None

def get_generic_text_from_bbox(doc, bbox, type='', get_only_all_text=False):
    max_page_count = 2
    for page_number in range(0, min(doc.page_count, max_page_count)):
        for template in bbox:
            retrieved = get_generic_text(doc[page_number], template, get_only_all_text=get_only_all_text)
            if get_only_all_text:
                return retrieved
            if type == 'date':
                date = check_date(retrieved)
                if date:
                    return date
            elif retrieved  == 'NIL':
                return None
            elif retrieved:
                val = retrieved.replace(',','')
                return None if len(val)==0 else val
    return None

def convert_into_csv_and_save():
    statement_ids=[]
    payment_due_dates=[]
    statement_dates=[]

    for d in new_data:
        statement_ids.append(d[0])
        payment_due_dates.append(d[1])
        statement_dates.append(d[2])
    
    result_dict = {
        "statement_id":statement_ids,
        "payment_due_date_retry":payment_due_dates,
        "statement_date_retry":statement_dates
    }

    df = pd.DataFrame(result_dict)
    df.to_csv('output.csv',index=False)

def get_payment_due_date_and_statement_date_for_failed_case(data):
    statement_id=data.get('statement_id')
    bank_name=data.get('bank_name')
    pdf_password=data.get('pdf_password')
    index = data.get('index')

    s3_file = get_file_from_s3(statement_id, bank_name)
    if s3_file!=None:
        temp_file_path = f'/tmp/temp_{statement_id}'
        with open(temp_file_path,'wb') as theFile:
            theFile.write(s3_file['Body'].read())

        doc = read_pdf(temp_file_path,pdf_password)
        if isinstance(doc,int):
            new_data.append([
                statement_id,
                "Could Not OPEN THE FILE",
                "COULD NOT OPEN THE FILE"
            ])

        if isinstance(doc,int):
            return

        templates = template_dict.get(bank_name)

        
        extracted_payment_date = get_generic_text_from_bbox(doc,templates.get('payment_due_date',[]),'date')
        extracted_statement_date = get_generic_text_from_bbox(doc,templates.get('statement_date',[]),'date')
        doc.close()
        if os.path.exists(temp_file_path):
            os.remove(temp_file_path)
        new_data.append([
                    statement_id,
                    extracted_payment_date,
                    extracted_statement_date,
                ])
    else:
        new_data.append([
                    statement_id,
                    "Could Not GET THE FILE FROM S3",
                    "Could Not GET THE FILE FROM S3"
                ])
        
    if (index%100)==0:
        print(f'---------at index ------{index}')
        convert_into_csv_and_save()


async def execute_func():
    bank_list = ['hdfc', 'axis', 'icici', 'stanchar', 'kotak', 'indusind', 'sbi', 'hsbc', 'citi', 'idfc', 'amex']
    for bank in bank_list:
        template_dict[bank] = await get_templates_from_rds_for_payment_due_date_and_statement_date(bank)

    num_threads = 10
    with concurrent.futures.ThreadPoolExecutor(max_workers=num_threads) as executor:
        # Iterate through PDF files and submit each to the executor
        csv_file_path = cwd+'/app/scripts/Mobikwik_Statement_Details_2024_01_03 (1).csv'
        csv_data = pd.read_csv(csv_file_path)

        lst = []
        for i in range(len(csv_data)):
            statement_id=csv_data['statement_id'][i]
            pdf_password=csv_data['pdf_password'][i]
            bank_name = csv_data['bank_name'][i]
            lst.append({
                'statement_id':statement_id,
                'pdf_password':pdf_password,
                'bank_name':bank_name,
                'index':i
            })
        
        executor.map(get_payment_due_date_and_statement_date_for_failed_case, lst)
        
    
    convert_into_csv_and_save()
    put_file_to_s3(
                    'output.csv', 
                    'bank-connect-uploads-replica-prod', 
                    'output3434_new.csv')
    

    print('saved to s3')