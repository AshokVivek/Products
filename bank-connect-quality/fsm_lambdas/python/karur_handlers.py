import pytesseract
import pdf2image
import os
import warnings
import pandas as pd
from datetime import datetime
import json
import os
from datetime import datetime
from python.configs import *


warnings.simplefilter(action = "ignore", category = FutureWarning)
pd.options.mode.chained_assignment = None


dr_list= ['DR', 'ATM', 'CSW', 'To', 'Charges','TO', 'CHARGES','MIN BAL','POS']
cr_list= ['CR', 'DEPOSIT', 'CASH DEP']

def karur_ocr_extraction_handler(event, context):
    #print("event was: {}".format(event))

    bucket = str(event.get('bucket'))
    key = str(event.get('key'))
    page_number = int(event.get('page_number'))
    name = event.get('name', '')
    number_of_pages = event.get('number_of_pages', 0)
    country = event.get("country", "IN")
    enrichment_regexes = event.get("enrichment_regexes", {})
    account_category = event.get("account_category", None)
    identity = event.get("identity", {})
    session_date_range = event.get("session_date_range", {'from_date':None, 'to_date':None})

    print("Number of pages received after being invoked from analyzed_pdf handler: ", number_of_pages)
    response = s3.get_object(Bucket=bucket, Key=key)

    response_metadata = response.get('Metadata')

    bank = response_metadata.get('bank_name')
    password = response_metadata.get('pdf_password')
    statement_id = response_metadata.get('statement_id')
    
    if bank !='karur':
        return {"message": "not a karur bank statement"}
    

    # write a temporary file with content
    file_path = "/tmp/{}.pdf".format(statement_id)
    with open(file_path, 'wb') as file_obj:
        file_obj.write(response['Body'].read())

    page_num = int(page_number)

    raw_data = None
    try:
        # get raw data of table from OCR
        raw_data = get_karur_raw_data_page(file_path, page_num, password)
    except Exception as e:
        print("Some exception occured: {}".format(e))

    #invoking other lambda for fsmlib processing of transaction.
    function_name = KARUR_EXTRACTION_PAGE_FUNCTION

    params = {
                "transaction_data": json.dumps(raw_data, default=str),
                "key": key,
                "bucket": bucket,
                "page_num": page_num,
                "name": name,
                "number_of_pages": number_of_pages,
                "enrichment_regexes": enrichment_regexes,
                "country": country,
                "account_category": account_category,
                "identity": identity,
                "session_date_range": session_date_range
            }
    payload = json.dumps(params)
    
    lambda_client.invoke(FunctionName=function_name,Payload=payload, InvocationType='Event')
    if os.path.exists(file_path):
        os.remove(file_path)


def get_karur_raw_data_page(pdf_path, page_num, password = None):
    # converting the pdf pages into images and getting the paths
    #base_file_name = os.path.basename(pdf_path).split(".")[0]
    output_folder_path = "/tmp/"

    paths_to_images = pdf2image.convert_from_path(
        pdf_path, dpi=300, userpw=password, output_folder=output_folder_path, 
        paths_only=True, fmt="jpeg", first_page=page_num+1, last_page=page_num+1,
        grayscale=True, transparent=True, ownerpw=password)
    
    #print(paths_to_images)
    ocr_data = get_data_from_image_page(paths_to_images[0])

    merged_useful_lines = get_merged_useful_lines_from_ocr_data(ocr_data)
    #print("merged usefull lines",len(merged_useful_lines))
    df = pd.DataFrame(merged_useful_lines)
    # adding a column for transaction type in df # default: CR
    df["txn_type"] = "CR"
    #print("columns" ,df.columns)
    # inferring the transaction type for each transaction
    for ind in df.index:
        # checking if the transaction note contains debit or credit keywords
        if any(ext in df[2][ind] for ext in dr_list):
            df["txn_type"][ind] = "DR"
        if any(ext in df[2][ind] for ext in cr_list):
            df["txn_type"][ind] = "CR"
        
        if df["txn_type"][ind] == "" and ind == 0:
            df["txn_type"][ind] = "DR"
        # re-checking the debit or credit transaction type 
        # by surplus of balance and amount values    
        if ind > 0:
            cr = round(float(df[4][ind-1])+float(df[3][ind]),2)
            dr = round(float(df[4][ind-1])-float(df[3][ind]),2)
            if float(df[4][ind]) == cr:
                df["txn_type"][ind] = "CR"
            elif float(df[4][ind])== dr:
                #print(ind,True)
                df["txn_type"][ind] = "DR"   
    raw_data = None

    try:
        table = df.values.tolist()
        #print(df.to_markdown())
        raw_data =  table
        #print("raw_data: ",raw_data)
    except Exception as e:
        print("Exception occured in get_karur_raw_data_page: {}".format(e))
        # raise HTTPException(status_code=500, detail=str(e))
    finally:
        # clean up
        clean_up_paths(paths_to_images)
    
    return raw_data

def get_data_from_image_page(image_path):
    # cv2_image = cv2.imread(image_path, cv2.IMREAD_GRAYSCALE)

    # doing ocr on the image
    ocr_data = pytesseract.image_to_string(image_path, lang='eng', config='--psm 6')

    # cv2.imshow('image', cv2_image)
    # cv2.waitKey()

    #print("returning ocr data: {}".format(ocr_data))

    return ocr_data

def get_merged_useful_lines_from_ocr_data(ocr_data):
    # cleaning the ocr data
    ocr_data = clean_ocr_data(ocr_data)
    # print(ocr_data)
    useful_lines = []
    for line in ocr_data:
        # splitting line into array of strings
        splitted_line = line.split(" ")
        date_format = "%d/%m/%Y"

        # removing/skipping opening balance
        if len(splitted_line) > 2 and "b/f" in splitted_line[2].lower():
            continue
        
        try:
            # because we are only interested in the lines
            # which are actually transactions and they
            # contain 2 dates in starting
            datetime.strptime(splitted_line[0], date_format)
            datetime.strptime(splitted_line[1], date_format)
            useful_lines.append(line)
        except Exception as e:
            # print("not a transaction line")
            continue
    
    # print(useful_lines)
    #print("useful lines: {}".format(len(useful_lines)))
    # merging the useful data to convert into fixed number of columns
    merged_useful_lines = []
    for line in useful_lines:
        splitted_line = line.split(" ")
        transaction_note = " ".join(splitted_line[3:-2])
        temp_merged_line = splitted_line[:2]
        temp_merged_line.extend([transaction_note])
        temp_merged_line.extend(splitted_line[-2:])
        merged_useful_lines.append(temp_merged_line)
    
    #print(merged_useful_lines)
    return merged_useful_lines

def clean_ocr_data(ocr_data):
    ocr_data = ocr_data.replace(",", "")
    ocr_data = ocr_data.replace("|", "")
    ocr_data = ocr_data.replace("}", "")
    ocr_data = ocr_data.replace("  ", " ")
    ocr_data = ocr_data.split("\n")
    return ocr_data

def clean_up_paths(path_list):
    print("cleaning up: {}".format(path_list))
    for path in path_list:
        if os.path.exists(path):
            os.remove(path)