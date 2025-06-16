import time
import json
import os 
from library.fitz_functions import read_pdf
from python.configs import *
from python.api_utils import call_api_with_session

def upload_pdf_to_bank_connect(bank_name,file_path,link_id,pdf_password, blue_green_flag=False):
    
    BASE_URL = "https://apis-uat.bankconnect.finbox.in" if blue_green_flag else DJANGO_BASE_URL
    KEY = "7pSkyY1wPQomIQy8PDaGf2O1FVvvaCmeYYqHKreI" if blue_green_flag else API_KEY
    url = "{}/bank-connect/v1/statement/upload/".format(BASE_URL)
    payload = {
            'bank_name': bank_name,
            'link_id': link_id,
            'pdf_password': pdf_password
    }
    files = {'file': open(file_path,'rb')}
    headers = {'x-api-key': KEY}
    response = call_api_with_session(url, "POST", payload, headers, files=files)
    return response.json(),response.status_code

def update_perfios_bc_mapping(b_entity_id,p_entity_id,b_statement_id,p_statement_id,message):
    url = "{}/bank-connect/v1/internal/update_bc_p_mapping/".format(DJANGO_BASE_URL)
    payload = {
            'b_entity_id': b_entity_id,
            'p_entity_id': p_entity_id,
            'b_statement_id': b_statement_id,
            'p_statement_id': p_statement_id,
            'message':message
    }
    headers = {'x-api-key': API_KEY}
    response = call_api_with_session(url,"POST", payload, headers)
    print(response.json(),response.status_code)

def upload_perfios_pdf_to_bc(event, context):

    records = event.get("Records")

    for record in records:

        start_time = time.time()

        try:
            record_body = json.loads(record.get("body", ""))
        except Exception as e:
            print("could not parse body, record: {}, exception: {}".format(record, e))
            continue

        entity_id = record_body.get("entity_id", None)
        link_id  = record_body.get("link_id", None)
        s3_file_key = record_body.get("s3_file_key", None)
        s3_file_bucket = record_body.get("s3_file_bucket", None)
        blue_green_flag = record_body.get("blue_green_enabled", False)

        if entity_id is None or link_id is None or s3_file_key is None or s3_file_bucket is None:
            message = "Invalid event for event  {}".format(event)
            print(message)
            return {
                "message": message
            }

        file_response = s3.get_object(Bucket=s3_file_bucket, Key=s3_file_key)
    
        file_metadata = file_response.get('Metadata')

        statement_id = file_metadata.get('statement_id')
        bank = file_metadata.get('bank_name')
        pdf_password = file_metadata.get('pdf_password')

        print("file_metadata: {}".format(file_metadata))

        if not statement_id:
            # no need to process if can't get statement id
            message = "ignored, statement_id not found in metadata"
            print(message)
            return {"message": message}

        # write a temporary file with content
        file_path = "/tmp/{}.pdf".format(statement_id)
        with open(file_path, 'wb') as file_obj:
            file_obj.write(file_response['Body'].read())
        
        if blue_green_flag:
            test_link_id = "blue_green_test_" + link_id
            upload_pdf_to_bank_connect(bank, file_path, test_link_id, pdf_password, blue_green_flag)
            return
        
        doc = read_pdf(file_path, pdf_password)
        num_pages = 0
        if isinstance(doc, int) == False:
            num_pages = doc.page_count
            # print("Could not upload pdf to Bank Connect, unable to read pdf using fitz for entity_id ={}".format(entity_id))
            # return {"message":"failure"}

        print("Page count of pdf is {} for statement_id {}".format(num_pages,statement_id))
        if num_pages > 200:
            print("Could not upload pdf to Bank Connect, page count greater than 200 for entity_id ={}".format(entity_id))
            return {"message":"failure"}

        bc_entity_id = None
        bc_statement_id = None
        message = None
        try:
            bc_link_id = "bcp_comp_auto_"+link_id
            response_json,status_code = upload_pdf_to_bank_connect(bank,file_path,bc_link_id,pdf_password)
            bc_entity_id = response_json.get('entity_id',None)
            bc_statement_id = response_json.get('statement_id',None)
            message = response_json.get('message', None)
            if status_code == 200:
                print("succesfully uploaded pdf on bank connect with entity_id = {} and statment_id = {}".format(bc_entity_id,bc_statement_id))
            else:
                print("Unable to upload pdf on bank connect with entity_id = ",response_json.get('entity_id',None))
        except Exception as e:
            print("\nCould not upload pdf to bank connect, exception: {}\n".format(e))
            return

        update_perfios_bc_mapping(bc_entity_id, entity_id, bc_statement_id, statement_id, message)
    
        if os.path.exists(file_path):
            os.remove(file_path)

        end_time = time.time()
        print("It took {} seconds to upload perfios pdf on bank connect".format(end_time-start_time))
        return {"message":"success"}