from uuid import uuid4
import boto3
from botocore.exceptions import ClientError
import boto3
import json
from app.conf import *
import fitz
from operator import itemgetter
from itertools import groupby

# frontend template_type mapping with fsmlibtemplates table keys
template_bbox_name_mapping = {
    "name": "name_bbox",
    "account_no": "accnt_bbox",
    "isfc": "ifsc_bbox",
    "address": "address_bbox",
    "micr": "micr_bbox",
    "date": "date_bbox",
    "transactions": "trans_bbox",
    "ifsc": "ifsc_bbox",
    "micr": "micr_bbox"
}

def check_text_set(text_set, page):
    curr_text_set_status = True
    for text in text_set.keys():
        curr_text_set_status = curr_text_set_status and check_valid_text_location(text_set, text, page)
        if not curr_text_set_status:
            return False
    return True

def check_valid_text_location(text_search, text, page):
    #Check if text occurs in valid locations within the pdf
    # areas = page.searchFor(text, hit_max=10)
    areas = page.search_for(text, hit_max=10)
    if areas is None:
        return False
    
    status = False
    for area in areas:
        if check_rect_contains(fitz.Rect(text_search[text]), area):
            status = True
            break
    if not status:
        return False
    return True

def check_rect_contains(rect1, rect2):
    # Checks if rect2 is contained inside rect1 (rect1 : fitz.Rect object)
    rect1 = list(rect1)
    rect2 = list(rect2)
    if (rect1[0] < rect2[0] and rect1[2] > rect2[2] and rect1[1] < rect2[1] and rect1[3] > rect2[3]):
        return True
    return False

def get_validated_template_type_mapping(template_type_frontend):
    #validate and convert frontend template 
    return template_bbox_name_mapping.get(template_type_frontend, None)


def create_template_uuid(validated_template_type_mapping):
    #function to create unique template_uuid
    return "{}_{}".format(validated_template_type_mapping, str(uuid4()))

def check_and_migrate(statement_id, bank_name):
    replica_path = f"pdf/{statement_id}_{bank_name}.pdf"
    try:
        pdf_response = s3.get_object(Bucket = PDF_BUCKET, Key=replica_path)
    except ClientError as e:
        # this means the file does not exist in the new replica pdf, this should
        # be fetched from the moshpittech replica bucket if the item is present.
        old_path = f"{statement_id}_{bank_name}.pdf"
        try:
            pdf_bucket_response = s3_client_old.get_object(Bucket = OLD_PDF_BUCKET, Key = old_path)
            print("file is present in the moshpittech replica")
            tmp_file_path = f"/tmp/{old_path}"
            with open(tmp_file_path, 'wb') as file_obj:
                file_obj.write(pdf_bucket_response['Body'].read())
            metadata  = pdf_bucket_response.get('Metadata')
            # upload this pdf in current prod replica
            s3_resource.Bucket(PDF_BUCKET).upload_file(tmp_file_path, replica_path, ExtraArgs={'Metadata': metadata})
            print(f"Migrated statement_id : {statement_id} to the current replica.")
            if os.path.exists(tmp_file_path):
                os.remove(tmp_file_path)
        except Exception as e:
            print(f"file of statement id : {statement_id} is not present in the replica bucket of old account")
            return False
    return True


def create_presigned_url(statement_id: str, bank_name: str, is_cc=False):
    #function to create presigned url from the statement_id and bank_name
    # check_and_migrate(statement_id, bank_name)
    key = f"{'cc_pdfs' if is_cc else 'pdf'}/{statement_id}_{bank_name}.pdf"
    try:
        s3_client.generate_presigned_url(
                "get_object",
                Params={"Bucket": CC_PDF_BUCKET if is_cc else PDF_BUCKET, "Key": key},
                ExpiresIn=120,
            )

    except ClientError as e:
        print(e)
        return None


def create_presigned_url_by_bucket(bucket_name: str, bucket_key: str, expire_in: int):
    '''
        function to create presigned url from bucket_name and bucket_key with expiration time
    '''
    try:
        return s3.generate_presigned_url(
            "get_object",
            Params={"Bucket": bucket_name, "Key": bucket_key},
            ExpiresIn=expire_in,
        )
    except ClientError as e:
        print(e)
        return None


def create_viewable_presigned_url(statement_id: str, bank_name: str, is_cc=False):
    # function to create viewable presigined url from statement_id and bank_name
    # check_and_migrate(statement_id, bank_name)

    if is_cc:
        key=f"cc_pdfs/{statement_id}_{bank_name}.pdf"
    else:
        key=f"pdf/{statement_id}_{bank_name}.pdf"

    try:
        return s3.generate_presigned_url(
            "get_object",
            Params={
                "Bucket": CC_PDF_BUCKET if is_cc else PDF_BUCKET ,
                "Key": key,
                "ResponseContentDisposition": "inline",
                "ResponseContentType": "application/pdf"
            },
            ExpiresIn=900
        )
        
    except ClientError as e:
        print(e)
        return None

def viewable_presigned_url(key: str, bucket: str, content_type : str):
    try:
        return s3.generate_presigned_url(
            "get_object",
            Params={
                "Bucket": bucket,
                "Key": key,
                "ResponseContentDisposition": "inline",
                "ResponseContentType": content_type
            },
            ExpiresIn=900
        )
    except ClientError as e:
        print(e)
        return None

def invoke_lambda(lambda_payload):
    #function to invoke template handller lambda 
    
    try:
        return lambda_client.invoke(
            FunctionName = TEMPLATE_HANDLER_LAMBDA_FUNCTION_NAME,
            Payload = json.dumps(lambda_payload),
            InvocationType = "RequestResponse"
        )
    except Exception as e:
        print("Exception is", e)
        return None

def get_text_in_box(page, box):
    rect = fitz.Rect(box)
    words = page.get_text_words()

    extracted_words = [list(w) for w in words if fitz.Rect(w[:4]) in rect]
    # print("raw words -> ", extracted_words)
    extracted_words = de_dup_words(extracted_words)
    extracted_words = get_sorted_boxes(extracted_words)

    group = groupby(extracted_words, key=itemgetter(3))

    string_list = list()
    for y1, g_words in group:
        string_list.append(" ".join(w[4] for w in g_words))
    # print("List -> ", string_list)
    return '\n'.join(string_list)

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

def get_sorted_boxes(words):
    prev_word = None
    words.sort(key=itemgetter(3, 0))
    for word in words:
        if prev_word is None:
            prev_word = word
            continue
        if get_horizontal_overlap(word, prev_word) < 0.1 and get_vertical_overlap(word, prev_word) > 0.7:
            word[1] = prev_word[1]
            word[3] = prev_word[3]
        prev_word = word
    words.sort(key=itemgetter(3, 0))
    return words

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


def check_date(input_date):
    from datetime import datetime
    if not input_date:
        return False
    current_year = datetime.today().year
    # TODO make sure wrong format does not get captured
    date_formats = ['%d %m %Y', '%m %d %Y', '%d %m %y', '%m %d %y', '%d %b %Y', '%b %d %Y', '%d %b %y', '%b %d %y',
                    '%Y %m %d', '%B %d %y', '%B %d ,%Y', '%b %d ,%Y', '%d %B %Y']
    separators = ['-', '.', '/', ',', '']

    random_formats = ['%d/%m/%Y%H:%M:%S', '%d/%m/%y(%S/%H/%M)', '%d/%m%Y', '%d%m/%Y', '(%S/%H/%M)%d/%m/%y',
                      '%UI(%S/%H/%M)%d/%m/%y', '0I(%S/%H/%M)%d/%m/%y', '2I(%S/%H/%M)%d/%m/%y', '3I(%S/%H/%M)%d/%m/%y',
                      '4I(%S/%H/%M)%d/%m/%y', '5I(%S/%H/%M)%d/%m/%y', '6I(%S/%H/%M)%d/%m/%y', '7I(%S/%H/%M)%d/%m/%y',
                      '8I(%S/%H/%M)%d/%m/%y', '9I(%S/%H/%M)%d/%m/%y', '%d%b%Y%I:%M%p', '%d/%m/%Y%I:%M:%S%p',
                      '%Y-%m-%d%H:%M:%S', '%d-%m-%Y%H:%M:%S', '%d-%m-%Y%H:%M:%S%p', '%m-%d-%Y%H:%M:%S%p', '%d%b%y%H:%M', '%d%b,%Y', '%d-%b-%y%H:%M:%S', '%m/%d/%Y%H:%M:%S%p']
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
                    if (date.year > current_year - 5) and (date.year < current_year+1):
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
                    if (date.year > current_year - 5) and (date.year < current_year+1):
                        return date
                except (ValueError, TypeError):
                    continue
    return False