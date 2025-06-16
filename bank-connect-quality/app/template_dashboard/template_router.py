import base64
from fastapi import APIRouter, Depends, Response, status, UploadFile, File
from typing import Optional
import fitz
import base64
import requests
import pandas as pd
from datetime import datetime
from .models.request import *
from .db_utils import get_account_mismatch_test_cases_from_pg,get_name_mismatch_test_cases_from_pg, get_all_templates, add_template_to_db, get_date_null_test_cases_from_pg, get_identity_data_from_statement, get_statements_postgress_data_time_range, get_bank_name_from_statement_id, get_new_template_data_from_db, get_transactions_data_time_range, get_logo_test_cases_from_pg ,get_logo_null_test_cases_from_pg,get_Account_null_test_cases_from_pg,get_Name_null_test_cases_from_pg,get_statement_id_password_from_pg,get_unextracted_statements_from_pg,get_low_transaction_count_from_pg
from datetime import datetime
from .utils import create_template_uuid, create_presigned_url, get_validated_template_type_mapping, invoke_lambda, \
    create_viewable_presigned_url, create_presigned_url_by_bucket
import time
import shutil
import json
import cv2
import os
from .utils import *
import re
from app.dependencies import get_current_user
from app.conf import *
import random
import hashlib

from app.constants import BULK_PDF_DOWNLOAD_KEY

def dhash(image_path, hashSize=8):
    image = cv2.imread(image_path)
    # print ("Image -> {}".format(image))
    if image is None:
        return 0
    image = cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)
    # print ("BW Image -> {}".format(image))
    resized = cv2.resize(image, (hashSize + 1, hashSize))
    diff = resized[:, 1:] > resized[:, :-1]
    return sum([2 ** i for (i, v) in enumerate(diff.flatten()) if v])

router = APIRouter(prefix="/template_dashboard")


@router.post("/validate_template")
def validate_template(request: ValidateRequest, response: Response, is_cc:Optional[bool]=False, user=Depends(get_current_user)):
    """
        api to validate template from fsmlib 
    """
    template = request.template
    if(request.statement_id == None or request.statement_id == ""):
        response.status_code = status.HTTP_400_BAD_REQUEST
        return {
            "message": "Statement_id can not be empty",
            "data": {}
        }
    if(request.bank_name == None or request.bank_name == ""):
        response.status_code = status.HTTP_400_BAD_REQUEST
        return {
            "message": "Bank name can not be empty",
            "data": {}
        }
    if(request.page_no == None or request.page_no == ""):
        response.status_code = status.HTTP_400_BAD_REQUEST
        return {
            "message": "Page No can not be empty",
            "data": {}
        }
    if not request.template:
        response.status_code = status.HTTP_400_BAD_REQUEST
        return {
            "message": " Template can not be empty",
            "data": {}
        }

    
    pdf_password = request.password or ""
    key = f"pdf/{request.statement_id}_{request.bank_name}.pdf"
    if is_cc:
        key=f"cc_pdfs/{request.statement_id}_{request.bank_name}.pdf"
    bucket = CC_PDF_BUCKET if is_cc else PDF_BUCKET

    # before invoking the lambda, check if this key is present in this bucket
    # if not raise an issue accordingly

    try:
        pdf_bucket_response = s3.get_object(Bucket=bucket, Key=key)
    except Exception as e:
        response.status_code = status.HTTP_404_NOT_FOUND
        return {"message": "key is not present in the bucket"}
    if request.transaction_flag:
        # perform validations before triggering lambda

        # validation 1: the template should have either vertical lines : true or a python list with numerals
        vertical_lines = template.get("vertical_lines", None)
        if vertical_lines is None:
            response.status_code = status.HTTP_400_BAD_REQUEST
            return {"message": "vertical lines cannot be None"}
        
        if isinstance(vertical_lines, list):
            for item in vertical_lines:
                if not isinstance(item, int):
                    response.status_code = status.HTTP_400_BAD_REQUEST
                    return {"message": "vertical lines items should be an integer coordinate"}
        

        # validation 2: column should be an array of array with 
        column = template.get("column", None)
        if column is None or not isinstance(column, list):
            response.status_code = status.HTTP_400_BAD_REQUEST
            return {"message": "column should be present and should be an array of an array"}
        
        # validation 3: the first item of the column should also be a list
        col_item = column[0]
        if not is_cc and not isinstance(col_item, list):
            response.status_code = status.HTTP_400_BAD_REQUEST
            return {"message": "first col item should also be a python list"}
        
        # validation 4: atleast date, transaction_note , (debit, credit or amount) and balance should be present
        check_item = column if is_cc else col_item
        if "date" not in check_item:
            response.status_code = status.HTTP_400_BAD_REQUEST
            return {"message": "date should be present as a column"}
        if "transaction_note" not in check_item:
            response.status_code = status.HTTP_400_BAD_REQUEST
            return {"message": "transaction note should be present as a column"}
        if not is_cc:
            if ("debit" not in col_item or "credit" not in col_item) and "amount" not in col_item:
                response.status_code = status.HTTP_400_BAD_REQUEST
                return {"message": "either both debit & credit should be presnet, or amount should be present"}
        else:
            if "amount" not in check_item:
                response.status_code = status.HTTP_400_BAD_REQUEST
                return {"message": "amount should be present in a credit card template"}
        identity = {}
        opening_date = request.opening_date
        if opening_date not in [None, '']:
            identity = {
                'opening_date': opening_date
            }
        lambda_payload = {
            "key": key,
            "bucket": bucket,
            "template": template,
            "password": pdf_password,
            "page_num": request.page_no - 1,
            "transaction_flag": request.transaction_flag,
            "bank":request.bank_name,
            "table_data": request.table_data,
            "template_type": request.template_type,
            'country': request.country,
            'identity': identity
        }
    else:
        lambda_payload = {
            "key": key,
            "bucket": bucket,
            "template": request.template,
            "password": pdf_password,
            "page_num": request.page_no - 1,
            "bank":request.bank_name,
            "template_type": request.template_type
        }
    #print("lambda payload: ", lambda_payload)

    lambda_response = invoke_lambda(lambda_payload)
    if not lambda_response:
        response.status_code = status.HTTP_400_BAD_REQUEST
        return {
            "message": "Template Excraction Failed",
            "data": {}
        }
    result = json.loads(lambda_response['Payload'].read().decode('utf-8'))
    if isinstance(result,str):
        result=json.loads(result)

    ticket_module_cases = request.template_type in ['update_table_data','update_processed_table_data','get_processed_ddb_data','get_table_data']
    if (ticket_module_cases and result == None) or (ticket_module_cases == False and not result):
        response.status_code = status.HTTP_400_BAD_REQUEST
        return {
            "message": "Not a valid template",
            "data": {}
        }
    response.status_code = status.HTTP_200_OK
    return {
        "message": "success",
        "data": {
            "extracted_result": result
        }
    }


@router.get('/view_all_template_data')
async def view_all_template_data(statement_id: str, bank_name: str, template_type: str, transaction_page_num: str, response: Response, password: Optional[str] = None, user=Depends(get_current_user)):
    # api checks and returns the data extracted through all extracted templates

    if(template_type == None or template_type == ""):
        response.status_code = status.HTTP_400_BAD_REQUEST
        return {
            "message": "Template_type can not be empty",
            "data": {}
        }
    if(statement_id == None or statement_id == ""):
        response.status_code = status.HTTP_400_BAD_REQUEST
        return {
            "message": "Statement_id can not be empty",
            "data": {}
        }
    if(bank_name == None or bank_name == ""):
        response.status_code = status.HTTP_400_BAD_REQUEST
        return {
            "message": "Bank name can not be empty",
            "data": {}
        }
    # validating and mapping template_type recieved from front-end
    validated_template_type_mapping = get_validated_template_type_mapping(
        template_type)
    if validated_template_type_mapping is None:
        response.status_code = status.HTTP_400_BAD_REQUEST
        return {
            "message": "Enter a valid template type",
            "data": {}
        }

    # print("template type: ", validated_template_type_mapping)
    # print("bank_name: ", bank_name)

    # geting all the templates for a perticular bank from db
    templates_arr: list = await get_all_templates(bank_name.upper(), validated_template_type_mapping)
    if(templates_arr == None):
        response.status_code = status.HTTP_200_OK
        return {
            "message": "success",
            "data": {
                "extracted_result": []
            }
        }

    extracted_data = []
    # print("records: {}".format(templates_arr))
    # print("template_array_list", templates_arr)
    # iterating over each template in template_arr and invoking lambda to extract data for statement_id
    for template_data in templates_arr:
        template = json.loads(template_data["template_json"])
        template_uuid = template_data["template_uuid"]
        # print("template: {}, type: {}, uuid: {}".format(template, type(template), template_uuid))
        pdf_password = password or ''
        key = f"pdf/{statement_id}_{bank_name}.pdf"
        bucket = PDF_BUCKET
        if validated_template_type_mapping == "trans_bbox":
            # transaction flag adde for transaction extraction flow
            lambda_payload = {
                "key": key,
                "bucket": bucket,
                "template": template,
                "password": pdf_password,
                "transaction_flag": True,
                "page_num": int(transaction_page_num)
            }
        else:
            # identity extraction flow lambda payload
            lambda_payload = {
                "key": key,
                "bucket": bucket,
                "template": template,
                "password": pdf_password
            }
        lambda_response = invoke_lambda(lambda_payload)
        if not lambda_response:
            response.status_code = status.HTTP_400_BAD_REQUEST

            return{
                "message": "Failed to Extract Template",
                "data": {}
            }

        lambda_result = json.loads(
            lambda_response['Payload'].read().decode('utf-8'))

        if len(lambda_result) == 0:
            continue

        # print("lambda result: {}, type: {}".format(lambda_result, type(lambda_result)))
        result = dict()
        result["template_id"] = template_uuid
        result["template_output"] = lambda_result
        # print("result dict: {}".format(result))
        extracted_data.append(result)

    response.status_code = status.HTTP_200_OK
    return {
        "message": "success",
        "data": {
            "extracted_result": extracted_data
        }
    }


@router.post('/add_template')
async def add_template(request: AddTemplate, response: Response, user=Depends(get_current_user)):
    # api used to add new template in fsmlibtemplates table in db
    print("request: {}".format(request))
    if(request.bank_name == None or request.bank_name == ""):
        response.status_code = status.HTTP_400_BAD_REQUEST
        return {
            "message": "Bank Name can not be empty",
            "data": {}
        }

    template_type_frontend = request.template_type
    print("template type frontend: {}".format(template_type_frontend))
    if template_type_frontend == None or template_type_frontend == "":
        response.status_code = status.HTTP_400_BAD_REQUEST
        return {
            "message": "Template Type can not be empty",
            "data": {}
        }
    if not request.template:
        response.status_code = status.HTTP_400_BAD_REQUEST
        return {
            "message": " Template can not be empty",
            "data": {}
        }

    validated_template_type_mapping = get_validated_template_type_mapping(
        template_type_frontend)
    if validated_template_type_mapping is None:
        response.status_code = status.HTTP_400_BAD_REQUEST
        return {
            "message": "Enter a valid template type",
            "data": {}
        }
    # creating unique template id for newly added template
    templateUUID = create_template_uuid(validated_template_type_mapping)
    # adding template to fsmlibtemplates table in postgress
    templateAdded = await add_template_to_db(templateUUID, request.template, request.bank_name, validated_template_type_mapping)
    if templateAdded:
        response.status_code = status.HTTP_200_OK
        return{
            "message": "success",
            "data": {"template_id": templateUUID}
        }
    else:
        response.status_code = status.HTTP_400_BAD_REQUEST
        return {
            "message": "Failed Adding a template",
            "data": {}
        }


@router.get("/download_pdf")
async def get_presigned_url_pdf(statement_id: str, response: Response, user=Depends(get_current_user)):
    # api to get the presigned url for the request statement id
    if(statement_id == None or statement_id == ""):
        response.status_code = status.HTTP_400_BAD_REQUEST
        return {
            "message": "Statement_id can not be empty",
            "data": {}
        }

    result = await get_bank_name_from_statement_id(statement_id=statement_id, is_cc=False)
    bank_name = result.get('bank_name')
    #print("bank_name: ", bank_name)
    if(bank_name == None or bank_name == ""):
        response.status_code = status.HTTP_400_BAD_REQUEST
        return {
            "message": "Unable to fetch bank_name from RDS",
            "data": {}
        }
    # function to create presigined_url for statement_id and bank_name
    presignedUrl = create_presigned_url(statement_id, bank_name, is_cc=False)
    if presignedUrl:
        response.status_code = status.HTTP_200_OK
        return{
            "message": "success",
            "data": {
                "presignedUrl": presignedUrl
            }
        }
    else:
        response.status_code = status.HTTP_400_BAD_REQUEST
        return {
            "message": "PDF File Not Found",
            "data": {}
        }

@router.get("/download_pdfs_url")
async def get_url_for_statement_ids(statement_type: str, country: str, response: Response, user=Depends(get_current_user), xlsx_file: UploadFile = File(...)):
    response.status_code = status.HTTP_400_BAD_REQUEST
    if statement_type not in ["pdf", "cc_pdfs","aa"]:
        return {
            "message": "give proper value of statement_type",
            "zip_url": None
        }
    if(country in [None, ""] or country not in ["IN","ID"]):
        return {
            "message": "give proper value of country",
            "zip_url": None
        }
    if not xlsx_file or not xlsx_file.filename.endswith('.xlsx'):
        return {
            "message": "give proper value of xlsx file",
            "zip_url": None
        }
    
    pdf_downloading_user = redis_cli.get(BULK_PDF_DOWNLOAD_KEY)
    if pdf_downloading_user is not None:
        return {
            'message': f"Please wait for some time, pdfs are being downloaded already by {pdf_downloading_user}",
            'zip_url': None
        }

    contents = await xlsx_file.read()
    df = pd.read_excel(contents)
    df.fillna("", inplace=True)
    no_of_pdfs_to_download = len(df)
    if no_of_pdfs_to_download > 20000:
        return {
            "message": "No. of pdfs to download should be less than 20,000.",
            "zip_url": None
        }
    
    required_column_list = list(df)
    if("statement_id" not in required_column_list or "bank_name" not in required_column_list):
        return {
            "message": "give proper column names like: statement_id and bank_name",
            "zip_url": None
        }
    
    redis_cli.set(BULK_PDF_DOWNLOAD_KEY, user.username, ex=4000)

    response.status_code = status.HTTP_200_OK
    tmp_folder = f"/tmp/pdf_{datetime.now().strftime('%Y%m%d%H%M%S')}"
    os.makedirs(tmp_folder)
    for index in df.index:
        statement_id = df["statement_id"][index]
        bank_name = df["bank_name"][index]
        if statement_id in [None, ""] or bank_name in [None, ""]:
            continue
        pdf_name = f"{statement_id}_{bank_name}.pdf"
        if statement_type=='aa':
            pdf_name = f"{statement_id}_{bank_name}.json"
        bucket_key = f"{statement_type}/{pdf_name}"
        pdf_url = create_presigned_url_by_bucket(PDF_BUCKET, bucket_key, 7200)
        pdf_response = requests.get(pdf_url, allow_redirects=True)
        open(f"{tmp_folder}/{pdf_name}", 'wb').write(pdf_response.content)
    
    pdf_file = tmp_folder.split('/')[2]
    shutil.make_archive(tmp_folder, "zip", tmp_folder)
    shutil.rmtree(tmp_folder)
    zip_file = tmp_folder+".zip"
    cache_bucket_key = pdf_file+'.zip'
    s3_resource.Bucket(FSM_ARBITER_BUCKET).upload_file(zip_file, cache_bucket_key)
    os.remove(zip_file)
    zip_url = create_presigned_url_by_bucket(FSM_ARBITER_BUCKET, cache_bucket_key, 7200)

    redis_cli.delete(BULK_PDF_DOWNLOAD_KEY)
    return {"message": "Successfully created zip file", "zip_url": zip_url}


@router.get('/get_identity_from_statement_id')
async def get_identity_from_statement_id(Statement_id, response: Response, user=Depends(get_current_user)):
    # api to get all the identity information for the request
    if Statement_id == "" or Statement_id == None:
        response.status_code = status.HTTP_400_BAD_REQUEST
        return {
            "message": "both from date and to date are required",
            "data": {}
        }

    # function to get full statement information from statement and identity table from postgress
    identity_data = None
    try:
        identity_data: StatementEntityData = await get_identity_data_from_statement(statement_id=Statement_id)
    except Exception as e:
        print("Some error occured while getting entity data from RDS")
        print("get_statement_data exception: {}".format(e))

    if identity_data == None:
        response.status_code = status.HTTP_500_INTERNAL_SERVER_ERROR
        return {
            "message": "Some error occured while getting entity data from RDS",
        }

    #print("data is: ",identity_data)

    return {
        "message": "success",
        "data": identity_data
    }


@router.get('/get_from_to_statements_data')
async def get_from_to_statements_data(from_date: str, to_date: str, bbox_type: str, response: Response, user=Depends(get_current_user)):
    # api to get the bank connect failed data between the given date range

    # validating payload
    if from_date == "" or to_date == "":
        response.status_code = status.HTTP_400_BAD_REQUEST
        return {
            "message": "both from date and to date are required",
            "data": {}
        }

    if bbox_type == "" or bbox_type == None:
        response.status_code = status.HTTP_400_BAD_REQUEST
        return {
            "message": "bbox_type is required",
            "data": {}
        }
    # parsing the from and to date
    from_date: datetime
    to_date: datetime
    # print("from date: ", from_date)
    # print("to_date: ",to_date)
    from_date = from_date.replace("T", " ")
    from_date = from_date[:-5]
    to_date = to_date.replace("T", " ")
    to_date = to_date[:-5]
    # print("from date: ", from_date)
    # print("to_date: ",to_date)
    try:
        # validating the date format
        from_date = datetime.strptime(from_date, "%Y-%m-%d %H:%M:%S")
        to_date = datetime.strptime(to_date, "%Y-%m-%d %H:%M:%S")
    except ValueError:
        response.status_code = status.HTTP_400_BAD_REQUEST
        return {
            "message": "date should be in YYYY-MM-DD HH:MM:SS format",
            "data": {}
        }
    # function to get the failed cases data from postgress between the given date range
    if bbox_type == "transactions":
        statements_data: list[StatementFromToData] = await get_transactions_data_time_range(from_date=from_date, to_date=to_date)
    else:
        statements_data: list[StatementFromToData] = await get_statements_postgress_data_time_range(from_date=from_date, to_date=to_date, bbox_type=bbox_type)

    if(statements_data == None):
        response.status_code = status.HTTP_400_BAD_REQUEST
        return {
            "message": "Statement data is either not available in this date range or there is some error while fetching data",
            "data": {}
        }

    response.status_code = status.HTTP_200_OK
    return {
        "message": "success",
        "data": {
            "statements_data": statements_data
        }
    }


@router.get("/create_pdf_view")
async def get_viewable_presigned_url_pdf(statement_id: str, response: Response, is_cc: Optional[bool]=False, user=Depends(get_current_user)):
    # api to get the viewable pdf presigned url for the statement_id

    if(statement_id == None or statement_id == ""):
        response.status_code = status.HTTP_400_BAD_REQUEST
        return {
            "message": "Statement_id can not be empty",
            "data": {}
        }
    # get the bank name for the statement id from pg
    result = await get_bank_name_from_statement_id(statement_id=statement_id, is_cc=is_cc)
    if(result == None or result == ""):
        response.status_code = status.HTTP_400_BAD_REQUEST
        return {
            "message": "Cannot find bank name from statement id",
            "data": {}
        }
    bank_name = result.get('bank_name')
    # print("bank_name: ", bank_name)
    if(bank_name == None or bank_name == ""):
        response.status_code = status.HTTP_400_BAD_REQUEST
        return {
            "message": "Unable to fetch bank_name from RDS",
            "data": {}
        }
    # function to create vieable presigned url
    presignedUrl = create_viewable_presigned_url(statement_id, bank_name, is_cc=is_cc)
    if presignedUrl:
        response.status_code = status.HTTP_200_OK
        return{
            "message": "success",
            "data": {
                "presignedUrl": presignedUrl
            }
        }
    else:
        response.status_code = status.HTTP_400_BAD_REQUEST
        return {
            "message": "PDF File Not Found",
            "data": {}
        }


@router.get("/new_template_data")
async def get_new_template_data(template_id: str, response: Response, user=Depends(get_current_user)):
    # api to get rtemplate data for a template_id from postgress

    if template_id == '' or template_id is None:
        response.status_code = status.HTTP_400_BAD_REQUEST
        return {
            "message": "template id is required",
            "data": {}
        }
    # function to get the added template data from db using template _uuid
    new_template_data = await get_new_template_data_from_db(template_id=template_id)

    print(type(new_template_data["template_json"]))
    print(json.loads(new_template_data["template_json"]))

    template_json = json.loads(new_template_data["template_json"])

    template_id = new_template_data.get("template_uuid")

    if new_template_data:
        response.status_code = status.HTTP_200_OK
        if new_template_data["template_type"] == "trans_bbox":
            # transaction template payload
            vertical_lines = template_json.get("vertical_lines", None)
            horizontal_lines = template_json.get("horizont_lines", None)
            column = template_json.get("column", None)
            return{
                "message": "success",
                "data": {
                    "template_name": new_template_data.get("template_type"),
                    "template_data": {
                        "vertical_lines": vertical_lines,
                        "horizontal_lines": horizontal_lines,
                        "column": column,
                        "uuid": template_id
                    }
                }
            }
        else:
            # Identity template payload
            bbox = template_json.get("bbox")
            regex = template_json.get("regex")
            return{
                "message": "success",
                "data": {
                    "template_name": new_template_data.get("template_type"),
                    "template_data": {
                        "bbox": bbox,
                        "regex": regex,
                        "uuid": template_id
                    }
                }
            }
    else:
        response.status_code = status.HTTP_400_BAD_REQUEST
        return {
            "message": "new template data not found",
            "data": {}
        }


@router.get("/get_testcase_pdfs_logo_mismatch")
async def get_logo_test_cases(from_date: str, to_date: str, response: Response, user=Depends(get_current_user)):
    # validating payload
    if from_date == "" or to_date == "":
        response.status_code = status.HTTP_400_BAD_REQUEST
        return {
            "message": "both from date and to date are required",
            "data": {}
        }

    # parsing the from and to date
    from_date: datetime
    to_date: datetime
   # print("from date: ", from_date)
    #print("to_date: ", to_date)
    from_date = from_date.replace("T", " ")
    from_date = from_date[:-5]
    to_date = to_date.replace("T", " ")
    to_date = to_date[:-5]
    #print("from date: ", from_date)
    #print("to_date: ", to_date)
    try:
        # validating the date format
        from_date = datetime.strptime(from_date, "%Y-%m-%d %H:%M:%S")
        to_date = datetime.strptime(to_date, "%Y-%m-%d %H:%M:%S")
    except ValueError:
        response.status_code = status.HTTP_400_BAD_REQUEST
        return {
            "message": "date should be in YYYY-MM-DD HH:MM:SS format",
            "data": {}
        }
    print("from date==>>",from_date)
    print("To date==>>",to_date)    
    test_case_data: list[LogoMismatchData] = await get_logo_test_cases_from_pg(from_date, to_date)
    if(test_case_data == None):
        response.status_code = status.HTTP_400_BAD_REQUEST
        return {
            "message": "Statement data is either not available in this date range or there is some error while fetching data",
            "data": {}
        }
    di = dict()
    # print("test case data==========", test_case_data)
    for ele in test_case_data:
       # print("element is=======", ele)

        statement_id = ele['statement_id']
       # print("this is statement id=>>>>>", statement_id)
        bank = ele['concat'].split("-")[0]
        #print("this is bank=>>>>>", bank)
        presignedUrl = create_viewable_presigned_url(statement_id, bank)

        if di.get(ele['concat']) is None:
            di[ele['concat']] = [presignedUrl]
        else:
            di[ele['concat']].append(presignedUrl)

    # testdata = await create_presigned_url(test_case_data)
    # print("dict========", di)
    response.status_code = status.HTTP_200_OK
    return {
        "message": "success",
        "data": di
    }

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

def get_as_base64(url):
    return base64.b64encode(requests.get(url).content)

@router.post("/get_image_hash")
async def get_images_from_pdf(request: Imagehashdata, response : Response, base_dir = None, user=Depends(get_current_user)):
    url=request.path
    x=get_as_base64(url)
    path = f'/tmp/temp_{random.randint(0, 10000000)}.pdf'
    with open(path,'wb') as theFile:
        theFile.write(base64.b64decode(x))
    
    if path == "" :
        response.status_code = status.HTTP_400_BAD_REQUEST
        if os.path.exists(path):
            os.remove(path)
        return {
            "message": "Path is empty",
            "data": {}
        }
    try:
        doc = read_pdf(path, request.password)
        if doc in [0, -1]:
            response.status_code = status.HTTP_400_BAD_REQUEST
            if os.path.exists(path):
                os.remove(path)
            return {
                "message": "Invalid password",
                "data": {}
            }
    except RuntimeError:
        print("Unable to extract images")
        response.status_code = status.HTTP_400_BAD_REQUEST
        if os.path.exists(path):
            os.remove(path)
        return {
            "message": "Unable to extract images",
            "data": {}
        }
            
    images = []
    hashes_list = []
    mapping = list()

    for i in range(min(2,doc.page_count)):
        try:
            images = doc.get_page_images(i)
        except RuntimeError:
            print("Unable to extract images")
            response.status_code = status.HTTP_400_BAD_REQUEST
            if os.path.exists(path):
                os.remove(path)
            return {
                "message": "Unable to extract images",
                "data": {}
            }
        
        for img in images:
            xref = img[0]
            pix = fitz.Pixmap(doc, xref)
            png_name = "/tmp/{}-{}.png".format((path.split('/')[-1]).split('.')[0], xref)
            try:
                if pix.n == 0:
                    pix.save(png_name)
                else:
                    pix1 = fitz.Pixmap(fitz.csRGB, pix)
                    pix1.save(png_name)
                hash_image = dhash(png_name)
                mapping.append({"path": base64.b64encode(open(png_name,'rb').read()), "hash": hash_image})
                if hash_image not in hashes_list:
                    hashes_list += [hash_image]
                if os.path.exists(png_name):
                    os.remove(png_name)
            except RuntimeError as e:
                print (e)
                # skip image for which couldn't write PNG file
                pass
    # pd.DataFrame(mapping).to_csv("{}/mapping.csv".format(base_dir))
    doc.close() # close the fitz document after work is done
    response.status_code = status.HTTP_200_OK
    if os.path.exists(path):
        os.remove(path)
    return {
        "message": "success",
        "data": hashes_list,
        "mapping":mapping
    }

@router.post('/get_image_hash_sha256')
async def get_images_from_pdf(request: Imagehashdata, response: Response, base_dir=None, user=Depends(get_current_user)):
    url=request.path
    x=get_as_base64(url)
    path = f'/tmp/temp_{random.randint(0, 10000000)}.pdf'
    with open(path,'wb') as theFile:
        theFile.write(base64.b64decode(x))
    
    if path == "" :
        response.status_code = status.HTTP_400_BAD_REQUEST
        if os.path.exists(path):
            os.remove(path)
        return {
            "message": "Path is empty",
            "data": {}
        }
    try:
        doc = read_pdf(path, request.password)
        if doc in [0, -1]:
            response.status_code = status.HTTP_400_BAD_REQUEST
            if os.path.exists(path):
                os.remove(path)
            return {
                "message": "Invalid password",
                "data": {}
            }
    except RuntimeError:
        print("Unable to extract images")
        response.status_code = status.HTTP_400_BAD_REQUEST
        if os.path.exists(path):
            os.remove(path)
        return {
            "message": "Unable to extract images",
            "data": {}
        }
            
    images = []
    hash_list = []
    mapping = list()
    for i in range(min(2, doc.page_count)):
        try:
            images = doc.get_page_images(i)
        except RuntimeError:
            print("Unable to extract images")
            response.status_code = status.HTTP_400_BAD_REQUEST
            if os.path.exists(path):
                os.remove(path)
            return {
                "message": "Unable to extract images",
                "data": {}
            }

        for img in images:
            xref = img[0]
            pix = fitz.Pixmap(doc, xref)
            png_name = "/tmp/{}-{}.png".format((path.split('/')[-1]).split('.')[0], xref)
            if pix.n == 0:
                pix.save(png_name)
            else:
                pix1 = fitz.Pixmap(fitz.csRGB, pix)
                pix1.save(png_name)
            
            with open(png_name,"rb") as f:
                bytes = f.read() # read entire file as bytes
                readable_hash = hashlib.sha256(bytes).hexdigest()
                mapping.append({"path": base64.b64encode(open(png_name,'rb').read()), "hash": readable_hash})
                hash_list.append(readable_hash)
            
            if os.path.exists(png_name):
                os.remove(png_name)
    
    doc.close()
    response.status_code = status.HTTP_200_OK
    if os.path.exists(path):
        os.remove(path)
    return {
        "message": "success",
        "data": hash_list,
        "mapping":mapping
    }
    


@router.get("/get_null_logo_test_cases")
async def get_null_logo_test(from_date: str, to_date: str, response: Response, user=Depends(get_current_user)):
    #print("Hii there ")
    # validating payload
    if from_date == "" or to_date == "":
        response.status_code = status.HTTP_400_BAD_REQUEST
        return {
            "message": "both from date and to date are required",
            "data": {}
        }
    
    # parsing the from and to date
    from_date: datetime
    to_date: datetime
   # print("from date: ", from_date)
    #print("to_date: ", to_date)
    from_date = from_date.replace("T", " ")
    from_date = from_date[:-5]
    to_date = to_date.replace("T", " ")
    to_date = to_date[:-5]
    #print("from date: ", from_date)
    #print("to_date: ", to_date)
    try:
        # validating the date format
        from_date = datetime.strptime(from_date, "%Y-%m-%d %H:%M:%S")
        to_date = datetime.strptime(to_date, "%Y-%m-%d %H:%M:%S")
    except ValueError:
        response.status_code = status.HTTP_400_BAD_REQUEST
        return {
            "message": "date should be in YYYY-MM-DD HH:MM:SS format",
            "data": {}
        }
    test_case_data: list[LogoNullMismatchData] = await get_logo_null_test_cases_from_pg(from_date, to_date)
    if(test_case_data == None):
        response.status_code = status.HTTP_400_BAD_REQUEST
        return {
            "message": "Statement data is either not available in this date range or there is some error while fetching data",
            "data": {}
        }
    di = dict()
    # print("test case data==========", test_case_data)
    for ele in test_case_data:
       # print("element is=======", ele)

        statement_id = ele['statement_id']
       # print("this is statement id=>>>>>", statement_id)
        bank = ele['bank_name']
        #print("this is bank=>>>>>", bank)
        presignedUrl = create_viewable_presigned_url(statement_id, bank)

        if di.get(ele['bank_name']) is None:
            di[ele['bank_name']] = [presignedUrl]
        else:
            di[ele['bank_name']].append(presignedUrl)

    # testdata = await create_presigned_url(test_case_data)
    # print("dict========", di)
    response.status_code = status.HTTP_200_OK
    return {
        "message": "success",
        "data": di      
    }

@router.get("/get_date_null_cases")
async def get_date_null_test(from_date: str, to_date: str, response: Response, user=Depends(get_current_user)):
    #print("Hii there ")
    # validating payload
    if from_date == "" or to_date == "":
        response.status_code = status.HTTP_400_BAD_REQUEST
        return {
            "message": "both from date and to date are required",
            "data": {}
        }
    
    # parsing the from and to date
    from_date: datetime
    to_date: datetime
   # print("from date: ", from_date)
    #print("to_date: ", to_date)
    from_date = from_date.replace("T", " ")
    from_date = from_date[:-5]
    to_date = to_date.replace("T", " ")
    to_date = to_date[:-5]
    #print("from date: ", from_date)
    #print("to_date: ", to_date)
    try:
        # validating the date format
        from_date = datetime.strptime(from_date, "%Y-%m-%d %H:%M:%S")
        to_date = datetime.strptime(to_date, "%Y-%m-%d %H:%M:%S")
    except ValueError:
        response.status_code = status.HTTP_400_BAD_REQUEST
        return {
            "message": "date should be in YYYY-MM-DD HH:MM:SS format",
            "data": {}
        }
    test_case_data: list[LogoNullMismatchData] = await get_date_null_test_cases_from_pg(from_date, to_date)
    if(test_case_data == None):
        response.status_code = status.HTTP_400_BAD_REQUEST
        return {
            "message": "Statement data is either not available in this date range or there is some error while fetching data",
            "data": {}
        }
    di = dict()
    # print("test case data==========", test_case_data)
    for ele in test_case_data:
       # print("element is=======", ele)

        statement_id = ele['statement_id']
       # print("this is statement id=>>>>>", statement_id)
        bank = ele['bank_name']
        #print("this is bank=>>>>>", bank)
        password=ele['pdf_password']
        presignedUrl = create_viewable_presigned_url(statement_id, bank)

        if di.get(ele['bank_name']) is None:
            di[ele['bank_name']] = [presignedUrl]
        else:
            di[ele['bank_name']].append(presignedUrl)

    # testdata = await create_presigned_url(test_case_data)
    # print("dict========", di)
    response.status_code = status.HTTP_200_OK
    return {
        "message": "success",
        "data": di      
    }

@router.post('/account_num_quality')
async def account_num_quality(request:RegexData,response: Response, user=Depends(get_current_user)):

    b64_file = request.b64_file
    password= request.password
    page_num= request.page_num
    bbox= request.bbox
    regex= request.regex

    if not regex:
        regex='(.*)'

    path = f'/tmp/temp_{random.randint(0, 10000000)}.pdf'

    with open(path, 'wb') as theFile:
        theFile.write(base64.b64decode(b64_file))
    doc = read_pdf(path, password)
    
    page = doc.load_page(page_num)
    all_text = get_text_in_box(page, bbox)
    doc.close()
    if all_text is not None:
        all_text = all_text.replace('\n', '').replace('(cid:9)', '')
        all_text = remove_unicode(all_text)
        all_text = all_text.replace('\x01', ' ')
    regex_match = re.match(regex, all_text)

    if os.path.exists(path):
        os.remove(path)
    if regex_match is None:
        response.status_code = status.HTTP_200_OK
        return {
            "acc_num" :'',
            "all_text":all_text     
        }
    if regex_match is not None:
        acc_num = regex_match.group(1)
        response.status_code = status.HTTP_200_OK
        return {
            "acc_num" :acc_num,
            "all_text":all_text     
        }

@router.post('/name_quality')
async def name_quality(request:RegexData ,response: Response, user=Depends(get_current_user)):

    b64_file = request.b64_file
    password= request.password
    page_num= request.page_num
    bbox= request.bbox
    regex= request.regex

    if b64_file == '':
        response.status_code = status.HTTP_400_BAD_REQUEST
        return {
            "error":"Not a valid data",
        }

    if not regex:
        regex='(.*)'
    path = f'/tmp/temp_{random.randint(0, 10000000)}.pdf'

    with open(path, 'wb') as theFile:
        theFile.write(base64.b64decode(b64_file))

    doc = read_pdf(path, password)
    page = doc.load_page(page_num)
    all_text = get_text_in_box(page, bbox)
    doc.close()
    name = ''
    if all_text is not None:
        all_text = all_text.replace('(cid:9)', '')
        all_text = remove_unicode(all_text)
        all_text = all_text.replace('\x01', ' ')

    if os.path.exists(path):
        os.remove(path)

    if regex is not None and all_text is not None:
        regex_match = re.match(regex, all_text)
        if regex_match is not None:
            name = regex_match.group(1)
            response.status_code = status.HTTP_200_OK
            return {
                "name" :name,
                "all_text":all_text     
            }
        else:
            response.status_code = status.HTTP_200_OK
            return {
                "name" :"",
                "all_text":all_text     
            }
    response.status_code = status.HTTP_500_INTERNAL_SERVER_ERROR
    return {
        "error":"Internal Server Error",
    }

@router.post('/name_quality_check')
async def name_quality(request:Oneapicall ,response: Response, user=Depends(get_current_user)):

    b64_file = request.b64_file
    password= request.password
    page_num= request.page_num
    arr_bbox= request.bbox
    arr_regex= request.regex
    
    if not len(arr_bbox) or not len(arr_regex):
        response.status_code = status.HTTP_400_BAD_REQUEST
        return {
            "error":" invalid request",
        }
    
    l=len(arr_bbox)
    print("lenght of bbox is",l)
    if len(arr_regex)!=l:
        while len(arr_regex)!=l:
            arr_regex.append('(.*)')
    for i in range(l):
        if not arr_regex[i]:
            arr_regex[i]='(.*)'
    
    path = f'/tmp/temp_{random.randint(0, 10000000)}.pdf'

    with open(path, 'wb') as theFile:
        theFile.write(base64.b64decode(b64_file))

    doc = read_pdf(path, password)
    page = doc.load_page(page_num)
    all_text=[]
    response_array=[]
    for i in range (l):
        all_text.append(get_text_in_box(page,arr_bbox[i]))
        print("all text ======>",all_text) 
        name = ''
        if all_text[i]:
            all_text[i] = all_text[i].replace('(cid:9)', '')
            all_text[i] = remove_unicode(all_text[i])
            all_text[i] = all_text[i].replace('\x01', ' ')

        if arr_regex[i] is not None and all_text[i] is not None:
            regex_match = re.match(arr_regex[i], all_text[i])
            if regex_match:
                name = regex_match.group(1)
                response_array.append({'name':name,'all_text':all_text[i]})
                # response.status_code = status.HTTP_200_OK
                # return {
                #     "name" :name,
                #     "all_text":all_text     
                # }
            else:
                response_array.append({'name':'','all_text':all_text[i]})
                # response.status_code = status.HTTP_200_OK
                # return {
                #     "name" :"",
                #     "all_text":all_text     
                # }
    doc.close()
    if os.path.exists(path):
        os.remove(path)
    if response_array and len(response_array):
        response.status_code = status.HTTP_200_OK
        return {
                'data':response_array    
            }
    else:
        response.status_code = status.HTTP_500_INTERNAL_SERVER_ERROR
        return {
            "error":"Internal Server Error",
        }


@router.post('/address_quality')
def address_quality(request:RegexData, response: Response, user=Depends(get_current_user)):
    b64_file = request.b64_file
    password= request.password
    page_num= request.page_num
    bbox= request.bbox
    regex= request.regex

    if not regex:
        regex='(.*)'
    path = f'/tmp/temp_{random.randint(0, 10000000)}.pdf'

    with open(path, 'wb') as theFile:
        theFile.write(base64.b64decode(b64_file))
    doc = read_pdf(path, password)
    page = doc.load_page(page_num)
    all_text = get_text_in_box(page, bbox)
    doc.close()
    address = ''
    if all_text is not None:
        all_text = all_text.replace('\n', ' ').replace('(cid:9)', '')
        all_text = remove_unicode(all_text)
        all_text = all_text.replace('\x01', ' ')
    
    if os.path.exists(path):
        os.remove(path)

    if regex is not None and all_text is not None:
        regex_match = re.match(regex, all_text)
        if regex_match is not None:
            address = regex_match.group(1)
            expr = re.compile('\d{2}/\d{2}/\d{4}')
            address = re.sub(expr, '', address)
            address = re.sub('\\s+', ' ', address)
            # print("\n\"", all_text, "\" -->",regex, "-->", address)

            response.status_code = status.HTTP_200_OK
            return {
                "address" :address,
                "all_text":all_text     
            }
        else:
            response.status_code = status.HTTP_200_OK
            return {
                "address" :"",
                "all_text":all_text     
            }
    response.status_code = status.HTTP_500_INTERNAL_SERVER_ERROR
    return {
        "error":"Internal Server Error",
    }


@router.post('/ifsc_quality')
def ifsc_quality(request:RegexData, response: Response, user=Depends(get_current_user)):
    b64_file = request.b64_file
    password= request.password
    page_num= request.page_num
    bbox= request.bbox
    regex= request.regex

    if not regex:
        regex='(.*)'
    
    path = f'/tmp/temp_{random.randint(0, 10000000)}.pdf'
    with open(path, 'wb') as theFile:
        theFile.write(base64.b64decode(b64_file))
    
    doc = read_pdf(path, password)
    page = doc.load_page(page_num)
    all_text = get_text_in_box(page, bbox)
    doc.close()

    ifsc = ''
    if all_text is not None:
        all_text = all_text.replace('\n', ' ').replace('(cid:9)', '')
        all_text = remove_unicode(all_text)
        all_text = all_text.replace('\x01', ' ')
    
    if os.path.exists(path):
        os.remove(path)

    if regex is not None and all_text is not None:
        regex_match = re.match(regex, all_text)
        if regex_match is not None:
            ifsc = regex_match.group(1)

            response.status_code = status.HTTP_200_OK
            return {
                "ifsc" : ifsc,
                "all_text":all_text     
            }
        else:
            response.status_code = status.HTTP_200_OK
            return {
                "ifsc" :"",
                "all_text":all_text     
            }
    response.status_code = status.HTTP_500_INTERNAL_SERVER_ERROR
    return {
        "error":"Internal Server Error",
    }


@router.post('/micr_quality')
def micr_quality(request:RegexData, response: Response, user=Depends(get_current_user)):
    b64_file = request.b64_file
    password= request.password
    page_num= request.page_num
    bbox= request.bbox
    regex= request.regex

    if not regex:
        regex='(.*)'
    path = f'/tmp/temp_{random.randint(0, 10000000)}.pdf'
    
    with open(path, 'wb') as theFile:
        theFile.write(base64.b64decode(b64_file))
    
    doc = read_pdf(path, password)
    page = doc.load_page(page_num)
    all_text = get_text_in_box(page, bbox)
    doc.close()

    micr = ''
    if all_text is not None:
        all_text = all_text.replace('\n', ' ').replace('(cid:9)', '')
        all_text = remove_unicode(all_text)
        all_text = all_text.replace('\x01', ' ')

    if os.path.exists(path):
        os.remove(path)
    
    if regex is not None and all_text is not None:
        regex_match = re.match(regex, all_text)
        if regex_match is not None:
            micr = regex_match.group(1)

            response.status_code = status.HTTP_200_OK
            return {
                "micr" : micr,
                "all_text":all_text     
            }
        else:
            response.status_code = status.HTTP_200_OK
            return {
                "micr" :"",
                "all_text":all_text     
            }
    response.status_code = status.HTTP_500_INTERNAL_SERVER_ERROR
    return {
        "error":"Internal Server Error",
    }


@router.post('/account_category_quality')
def account_category_quality(request:RegexData, response: Response, user=Depends(get_current_user)):
    b64_file = request.b64_file
    password= request.password
    page_num= request.page_num
    bbox= request.bbox
    regex= request.regex

    if not regex:
        regex='(.*)'
    path = f'/tmp/temp_{random.randint(0, 10000000)}.pdf'
    with open(path, 'wb') as theFile:
        theFile.write(base64.b64decode(b64_file))
    doc = read_pdf(path, password)
    page = doc.load_page(page_num)
    all_text = get_text_in_box(page, bbox)
    doc.close()

    account_category = ''
    if all_text is not None:
        all_text = all_text.replace('\n', ' ').replace('(cid:9)', '')
        all_text = remove_unicode(all_text)
        all_text = all_text.replace('\x01', ' ')
    
    if os.path.exists(path):
        os.remove(path)

    if regex is not None and all_text is not None:
        regex_match = re.match(regex, all_text)
        if regex_match is not None:
            account_category = regex_match.group(1)

            response.status_code = status.HTTP_200_OK
            return {
                "account_category" : account_category,
                "all_text":all_text     
            }
        else:
            response.status_code = status.HTTP_200_OK
            return {
                "account_category" :"",
                "all_text":all_text     
            }
    response.status_code = status.HTTP_500_INTERNAL_SERVER_ERROR
    return {
        "error":"Internal Server Error",
    }


@router.post('/credit_limit_quality')
def credit_limit_quality(request:RegexData, response: Response, user=Depends(get_current_user)):
    b64_file = request.b64_file
    password= request.password
    page_num= request.page_num
    bbox= request.bbox
    regex= request.regex

    if not regex:
        regex='(.*)'
    
    path = f'/tmp/temp_{random.randint(0, 10000000)}.pdf'
    with open(path, 'wb') as theFile:
        theFile.write(base64.b64decode(b64_file))
    doc = read_pdf(path, password)
    page = doc.load_page(page_num)
    all_text = get_text_in_box(page, bbox)
    doc.close()

    credit_limit = ''
    if all_text is not None:
        all_text = all_text.replace('\n', '').replace('(cid:9)', '')
        all_text = remove_unicode(all_text)
        all_text = all_text.replace('\x01', ' ')
    
    if os.path.exists(path):
        os.remove(path)

    if regex is not None and all_text is not None:
        regex_match = re.match(regex, all_text)
        if regex_match is not None:
            credit_limit = regex_match.group(1)

            response.status_code = status.HTTP_200_OK
            return {
                "credit_limit" : credit_limit,
                "all_text":all_text     
            }
        else:
            response.status_code = status.HTTP_200_OK
            return {
                "credit_limit" :"",
                "all_text":all_text     
            }
    response.status_code = status.HTTP_500_INTERNAL_SERVER_ERROR
    return {
        "error":"Internal Server Error",
    }


@router.post('/date_quality')
def date_quality(request:FromDateToDateRegexData, response: Response, user=Depends(get_current_user)):
    b64_file = request.b64_file
    password= request.password
    page_num= request.page_num

    from_bbox = request.from_bbox
    from_regex= request.from_regex
    to_bbox= request.to_bbox
    to_regex= request.to_regex

    if not from_regex:
        from_regex='(.*)'
    if not to_regex:
        to_regex='(.*)'
    path = f'/tmp/temp_{random.randint(0, 10000000)}.pdf'
    with open(path, 'wb') as theFile:
        theFile.write(base64.b64decode(b64_file))
    doc = read_pdf(path, password)
    if isinstance(doc, int):
        response.status_code = status.HTTP_400_BAD_REQUEST
        return {
            "error":"Internal Server Error",
            "doc": doc
        }

    page = doc.load_page(page_num)

    if os.path.exists(path):
        os.remove(path)

    try:
        from_date, from_bbox_all_text = get_date_from_doc(page, from_bbox, from_regex)
        to_date, to_bbox_all_text = get_date_from_doc(page, to_bbox, to_regex)
        response.status_code = status.HTTP_200_OK
        doc.close()
        return {
                "from_date" : from_date,
                "to_date" : to_date,
                "from_bbox_all_text": from_bbox_all_text,
                "to_bbox_all_text": to_bbox_all_text
            }
    except Exception as e:
        print(str(e))
        doc.close()
        return {
            "error":"Internal Server Error",
        }


def get_date_from_doc(page, bbox, regex):
    all_text = get_text_in_box(page, bbox)
    date_text = ''
    if all_text is not None:
        all_text = all_text.replace('\n', '').replace(' ', '').replace('(cid:9)', '')
        all_text = remove_unicode(all_text)
        all_text = all_text.replace('\x01', ' ')

    if regex is not None and all_text is not None:
        regex_match = re.match(regex, all_text)
        if regex_match is not None:
            date_text = regex_match.group(1)
            date_to_return = check_date(date_text)
            if not date_to_return:
                date_text = ""

    return date_text, all_text


@router.get("/get_null_Account_test_cases")
async def get_null_Account_test(from_date: str, to_date: str, response: Response, user=Depends(get_current_user)):
    # validating payload
    if from_date == "" or to_date == "":
        response.status_code = status.HTTP_400_BAD_REQUEST
        return {
            "message": "both from date and to date are required",
            "data": {}
        }
    
    # parsing the from and to date
    from_date: datetime
    to_date: datetime
   # print("from date: ", from_date)
    #print("to_date: ", to_date)
    from_date = from_date.replace("T", " ")
    from_date = from_date[:-5]
    to_date = to_date.replace("T", " ")
    to_date = to_date[:-5]
    #print("from date: ", from_date)
    #print("to_date: ", to_date)
    try:
        # validating the date format
        from_date = datetime.strptime(from_date, "%Y-%m-%d %H:%M:%S")
        to_date = datetime.strptime(to_date, "%Y-%m-%d %H:%M:%S")
    except ValueError:
        response.status_code = status.HTTP_400_BAD_REQUEST
        return {
            "message": "date should be in YYYY-MM-DD HH:MM:SS format",
            "data": {}
        }
    test_case_data: list[LogoNullMismatchData] = await get_Account_null_test_cases_from_pg(from_date, to_date)
    if(test_case_data == None):
        response.status_code = status.HTTP_400_BAD_REQUEST
        return {
            "message": "Statement data is either not available in this date range or there is some error while fetching data",
            "data": {}
        }
    di = dict()
    # print("test case data==========", test_case_data)
    for ele in test_case_data:
       # print("element is=======", ele)

        statement_id = ele['statement_id']
       # print("this is statement id=>>>>>", statement_id)
        bank = ele['bank_name']
        #print("this is bank=>>>>>", bank)
        presignedUrl = create_viewable_presigned_url(statement_id, bank)

        if di.get(ele['bank_name']) is None:
            di[ele['bank_name']] = [presignedUrl]
        else:
            di[ele['bank_name']].append(presignedUrl)

    # testdata = await create_presigned_url(test_case_data)
    # print("dict========", di)
    response.status_code = status.HTTP_200_OK
    return {
        "message": "success",
        "data": di      
    }

@router.get("/get_null_Name_test_cases")
async def get_null_Name_test(from_date: str, to_date: str, response: Response, user=Depends(get_current_user)):
    # validating payload
    if from_date == "" or to_date == "":
        response.status_code = status.HTTP_400_BAD_REQUEST
        return {
            "message": "both from date and to date are required",
            "data": {}
        }
    
    # parsing the from and to date
    from_date: datetime
    to_date: datetime
   # print("from date: ", from_date)
    #print("to_date: ", to_date)
    from_date = from_date.replace("T", " ")
    from_date = from_date[:-5]
    to_date = to_date.replace("T", " ")
    to_date = to_date[:-5]
    #print("from date: ", from_date)
    #print("to_date: ", to_date)
    try:
        # validating the date format
        from_date = datetime.strptime(from_date, "%Y-%m-%d %H:%M:%S")
        to_date = datetime.strptime(to_date, "%Y-%m-%d %H:%M:%S")
    except ValueError:
        response.status_code = status.HTTP_400_BAD_REQUEST
        return {
            "message": "date should be in YYYY-MM-DD HH:MM:SS format",
            "data": {}
        }
    test_case_data: list[LogoNullMismatchData] = await get_Name_null_test_cases_from_pg(from_date, to_date)
    if(test_case_data == None):
        response.status_code = status.HTTP_400_BAD_REQUEST
        return {
            "message": "Statement data is either not available in this date range or there is some error while fetching data",
            "data": {}
        }
    di = dict()
    # print("test case data==========", test_case_data)
    for ele in test_case_data:
       # print("element is=======", ele)

        statement_id = ele['statement_id']
       # print("this is statement id=>>>>>", statement_id)
        bank = ele['bank_name']
        #print("this is bank=>>>>>", bank)
        presignedUrl = create_viewable_presigned_url(statement_id, bank)

        if di.get(ele['bank_name']) is None:
            di[ele['bank_name']] = [presignedUrl]
        else:
            di[ele['bank_name']].append(presignedUrl)

    # testdata = await create_presigned_url(test_case_data)
    # print("dict========", di)
    response.status_code = status.HTTP_200_OK
    return {
        "message": "success",
        "data": di      
    }

@router.get("/get_base64")
async def get_b64_from_statement_id(statement_id: str, response : Response, is_cc: Optional[bool] = False, base_dir = None, user=Depends(get_current_user)):
    if statement_id == "" :
        response.status_code = status.HTTP_400_BAD_REQUEST
        return {
            "message": "statement id is empty",
            "data": {}
        }
    result = await get_bank_name_from_statement_id(statement_id=statement_id, is_cc=is_cc)
    if(result==None or result==""):
        response.status_code = status.HTTP_400_BAD_REQUEST
        return {
            "message": "Unable to fetch bank_name from RDS",
            "data": {}
        }
    bank_name = result.get('bank_name')

    if(bank_name == None or bank_name == ""):
        response.status_code = status.HTTP_400_BAD_REQUEST
        return {
            "message": "Unable to fetch bank_name from RDS",
            "data": {}
        }
    # function to create vieable presigned url
    presignedUrl = create_viewable_presigned_url(statement_id, bank_name, is_cc=is_cc)
    if presignedUrl:
        base64=get_as_base64(presignedUrl)
        response.status_code = status.HTTP_200_OK
        return{
            "message": "success",
            "data": {
                "base64": base64
            }
        }
    else:
        response.status_code = status.HTTP_400_BAD_REQUEST
        return {
            "message": "PDF File Not Found",
            "data": {}
        }
    
@router.post("/get_image_base64")
async def get_b64_from_url(request: Imagehashdata, response : Response, base_dir = None, user=Depends(get_current_user)):
    url=request.path
    if url == "" :
        response.status_code = status.HTTP_400_BAD_REQUEST
        return {
            "message": "Url is empty",
            "data": {}
        }
    x=get_as_base64(url)
    response.status_code = status.HTTP_200_OK
    return {
        "message": "success",
        "data": x,
    }

@router.get("/get_pdf_password")
async def get_pdf_password(statement_id: str, response: Response, is_cc: Optional[bool]=False, user=Depends(get_current_user)):
    # api to get the viewable pdf presigned url for the statement_id
    if(statement_id == None or statement_id == ""):
        response.status_code = status.HTTP_400_BAD_REQUEST
        return {
            "message": "Statement_id can not be empty",
            "data": {}
        }
    # get the bank name for the statement id from pg
    result = await get_statement_id_password_from_pg(statement_id=statement_id, is_cc=is_cc)
    # print("result=>",result)
    if(result == None or result == ""):
        response.status_code = status.HTTP_400_BAD_REQUEST
        return {
            "message": "pdf_password not found",
            "data": {}
        }
    pdf_password = result.get('pdf_password')
    # print(pdf_password)
    response.status_code = status.HTTP_200_OK
    return{
            "message": "success",
            "password":pdf_password
        }

@router.post('/get_transaction_channel')
async def get_transaction(request:Transaction ,response: Response, user=Depends(get_current_user)):

    from_date = request.from_date
    to_date= request.to_date
    bank_name= request.bank_name
    transaction_id= request.transaction_id

    if from_date == "" or to_date == "":
        response.status_code = status.HTTP_400_BAD_REQUEST
        return {
            "message": "both from date and to date are required",
            "data": {}
        }
    if bank_name == "" or transaction_id=="":
        response.status_code = status.HTTP_400_BAD_REQUEST
        return {
            "message": "both bank_name and transaction_id are required",
            "data": {}
        }

@router.get("/get_unextracted_statements")
async def get_unextracted(from_date: str, to_date: str, response: Response, user=Depends(get_current_user)):
    # validating payload
    if from_date == "" or to_date == "":
        response.status_code = status.HTTP_400_BAD_REQUEST
        return {
            "message": "both from date and to date are required",
            "data": {}
        }
    
    # parsing the from and to date
    from_date: datetime
    to_date: datetime
   # print("from date: ", from_date)
    #print("to_date: ", to_date)
    from_date = from_date.replace("T", " ")
    from_date = from_date[:-5]
    to_date = to_date.replace("T", " ")
    to_date = to_date[:-5]
    print("from date: ", from_date)
    print("to_date: ", to_date)
    try:
        # validating the date format
        from_date = datetime.strptime(from_date, "%Y-%m-%d %H:%M:%S")
        to_date = datetime.strptime(to_date, "%Y-%m-%d %H:%M:%S")
    except ValueError:
        response.status_code = status.HTTP_400_BAD_REQUEST
        return {
            "message": "date should be in YYYY-MM-DD HH:MM:SS format",
            "data": {}
        }
    test_case_data: list[LogoNullMismatchData] = await get_unextracted_statements_from_pg(from_date, to_date)
    
    if(test_case_data == None):
        response.status_code = status.HTTP_400_BAD_REQUEST
        return {
            "message": "Statement data is either not available in this date range or there is some error while fetching data",
            "data": {}
        }
    di = dict()
    # print("test case data==========", test_case_data)
    for ele in test_case_data:
       # print("element is=======", ele)

        statement_id = ele['statement_id']
       # print("this is statement id=>>>>>", statement_id)
        bank = ele['bank_name']
        #print("this is bank=>>>>>", bank)
        presignedUrl = create_viewable_presigned_url(statement_id, bank)

        if di.get(ele['bank_name']) is None:
            di[ele['bank_name']] = [presignedUrl]
        else:
            di[ele['bank_name']].append(presignedUrl)

    # testdata = await create_presigned_url(test_case_data)
    # print("dict========", di)
    response.status_code = status.HTTP_200_OK
    return {
        "message": "success",
        "data": di      
    }
    
@router.get("/get_low_transaction_count")
async def get_low_transaction(from_date: str, to_date: str, response: Response, user=Depends(get_current_user)):
    # validating payload
    if from_date == "" or to_date == "":
        response.status_code = status.HTTP_400_BAD_REQUEST
        return {
            "message": "both from date and to date are required",
            "data": {}
        }
    # parsing the from and to date
    from_date: datetime
    to_date: datetime
   # print("from date: ", from_date)
    #print("to_date: ", to_date)
    from_date = from_date.replace("T", " ")
    from_date = from_date[:-5]
    to_date = to_date.replace("T", " ")
    to_date = to_date[:-5]
    #print("from date: ", from_date)
    #print("to_date: ", to_date)
    try:
        # validating the date format
        from_date = datetime.strptime(from_date, "%Y-%m-%d %H:%M:%S")
        to_date = datetime.strptime(to_date, "%Y-%m-%d %H:%M:%S")
    except ValueError:
        response.status_code = status.HTTP_400_BAD_REQUEST
        return {
            "message": "date should be in YYYY-MM-DD HH:MM:SS format",
            "data": {}
        }
    test_case_data: list[LogoNullMismatchData] = await get_low_transaction_count_from_pg(from_date, to_date)
    if(test_case_data == None):
        response.status_code = status.HTTP_400_BAD_REQUEST
        return {
            "message": "Statement data is either not available in this date range or there is some error while fetching data",
            "data": {}
        }
    di = dict()
    # print("test case data==========", test_case_data)
    for ele in test_case_data:
       # print("element is=======", ele)

        statement_id = ele['statement_id']
       # print("this is statement id=>>>>>", statement_id)
        bank = ele['bank_name']
        #print("this is bank=>>>>>", bank)
        pagecount=ele['page_count']
        tc=ele['transaction_count']
        ratio=ele['ratio']
        presignedUrl = create_viewable_presigned_url(statement_id, bank)

        if di.get(ele['bank_name']) is None:
            di[ele['bank_name']] = [[presignedUrl,pagecount,tc,ratio]]
        else:
            di[ele['bank_name']].append([presignedUrl,pagecount,tc,ratio])
            # di[ele[presignedUrl]].append(pagecount)
            # di[ele[presignedUrl]].append(tc)

    # testdata = await create_presigned_url(test_case_data)
    # print("dict========", di)
    response.status_code = status.HTTP_200_OK
    return {
        "message": "success",
        "data": di      
    }
    
@router.get("/get_name_mismatch")
async def get_name_mismatch_test_cases(from_date: str, to_date: str, response: Response, user=Depends(get_current_user)):
    # validating payload
    if from_date == "" or to_date == "":
        response.status_code = status.HTTP_400_BAD_REQUEST
        return {
            "message": "both from date and to date are required",
            "data": {}
        }
    
    # parsing the from and to date
    from_date: datetime
    to_date: datetime
   # print("from date: ", from_date)
    #print("to_date: ", to_date)
    from_date = from_date.replace("T", " ")
    from_date = from_date[:-5]
    to_date = to_date.replace("T", " ")
    to_date = to_date[:-5]
    #print("from date: ", from_date)
    #print("to_date: ", to_date)
    try:
        # validating the date format
        from_date = datetime.strptime(from_date, "%Y-%m-%d %H:%M:%S")
        to_date = datetime.strptime(to_date, "%Y-%m-%d %H:%M:%S")
    except ValueError:
        response.status_code = status.HTTP_400_BAD_REQUEST
        return {
            "message": "date should be in YYYY-MM-DD HH:MM:SS format",
            "data": {}
        }
    test_case_data: list[LogoNullMismatchData] = await get_name_mismatch_test_cases_from_pg(from_date, to_date)
    if(test_case_data == None):
        response.status_code = status.HTTP_400_BAD_REQUEST
        return {
            "message": "Statement data is either not available in this date range or there is some error while fetching data",
            "data": {}
        }
    di = dict()
    # print("test case data==========", test_case_data)
    for ele in test_case_data:
       # print("element is=======", ele)

        statement_id = ele['statement_id']
       # print("this is statement id=>>>>>", statement_id)
        bank = ele['bank_name']
        #print("this is bank=>>>>>", bank)
        presignedUrl = create_viewable_presigned_url(statement_id, bank)
        predicted_bank=ele['predicted_bank']
        if di.get(ele['bank_name']) is None:
            di[ele['bank_name']] = [{'presignedUrl':presignedUrl,'predicted_bank':predicted_bank}]
        else:
            di[ele['bank_name']].append({'presignedUrl':presignedUrl,'predicted_bank':predicted_bank})

    # testdata = await create_presigned_url(test_case_data)
    # print("dict========", di)
    response.status_code = status.HTTP_200_OK
    return {
        "message": "success",
        "data": di      
    }


@router.get("/get_account_mismatch")
async def get_name_mismatch_test_cases(from_date: str, to_date: str, response: Response, user=Depends(get_current_user)):
    # validating payload
    if from_date == "" or to_date == "":
        response.status_code = status.HTTP_400_BAD_REQUEST
        return {
            "message": "both from date and to date are required",
            "data": {}
        }
    
    # parsing the from and to date
    from_date: datetime
    to_date: datetime
   # print("from date: ", from_date)
    #print("to_date: ", to_date)
    from_date = from_date.replace("T", " ")
    from_date = from_date[:-5]
    to_date = to_date.replace("T", " ")
    to_date = to_date[:-5]
    try:
        # validating the date format
        from_date = datetime.strptime(from_date, "%Y-%m-%d %H:%M:%S")
        to_date = datetime.strptime(to_date, "%Y-%m-%d %H:%M:%S")
    except ValueError:
        response.status_code = status.HTTP_400_BAD_REQUEST
        return {
            "message": "date should be in YYYY-MM-DD HH:MM:SS format",
            "data": {}
        }
    test_case_data: list[LogoNullMismatchData] = await get_account_mismatch_test_cases_from_pg(from_date, to_date)
    if(test_case_data == None):
        response.status_code = status.HTTP_400_BAD_REQUEST
        return {
            "message": "Statement data is either not available in this date range or there is some error while fetching data",
            "data": {}
        }
    di = dict()
    # print("test case data==========", test_case_data)
    for ele in test_case_data:
       # print("element is=======", ele)

        statement_id = ele['statement_id']
       # print("this is statement id=>>>>>", statement_id)
        bank = ele['bank_name']
        #print("this is bank=>>>>>", bank)
        presignedUrl = create_viewable_presigned_url(statement_id, bank)

        if di.get(ele['bank_name']) is None:
            di[ele['bank_name']] = [presignedUrl]
        else:
            di[ele['bank_name']].append(presignedUrl)

    response.status_code = status.HTTP_200_OK
    return {
        "message": "success",
        "data": di      
    }
    
@router.post("/extract_quality_data")
async def extract_quality_data(request:RegexData, response: Response, bank_name: str, template_type: Optional[str] = None, user=Depends(get_current_user)):

    b64_file = request.b64_file
    password = request.password
    page_num = request.page_num
    bbox = request.bbox
    regex = request.regex

    if not regex:
        regex = '(.*)'
    
    path = f'/tmp/temp_{random.randint(0, 10000000)}.pdf'
    with open(path, 'wb') as theFile:
        theFile.write(base64.b64decode(b64_file))
    
    doc = read_pdf(path, password)

    if os.path.exists(path):
        os.remove(path)
    if isinstance(doc, int):
        return {
            "error":"Internal Server Error",
            "doc": doc
        }
    page = doc.load_page(page_num)
    all_text = get_text_in_box(page, bbox)
    doc.close()
    all_text=all_text.replace('\n', ' ').replace('(cid:9)', '')
    template_type = template_type.split('_')[0]
    
    if all_text is not None:
        all_text = all_text.replace('(cid:9)', '')
        all_text = remove_unicode(all_text)
        all_text = all_text.replace('\x01', ' ')
    if regex is not None and all_text is not None:
        regex_match = re.match(regex, all_text)
        if regex_match is not None:
            data = regex_match.group(1)
            response.status_code = status.HTTP_200_OK
            return {
                template_type: data,
                "all_text":all_text     
            }
        else:
            response.status_code = status.HTTP_200_OK
            return {
                template_type: "",
                "all_text":all_text     
            }
    response.status_code = status.HTTP_500_INTERNAL_SERVER_ERROR
    return {
        "error":"Internal Server Error",
    }    

async def get_b64_from_statements(statement_id: str):
    print("statement id is",statement_id)
    if statement_id == "" :
        return False
    result = await get_bank_name_from_statement_id(statement_id=statement_id, is_cc=False)
    if(result==None or result==""):
        return False
    bank_name = result.get('bank_name')

    if(bank_name == None or bank_name == ""):
        return False
    # function to create viewable presigned url
    presignedUrl = create_viewable_presigned_url(statement_id, bank_name)
    print("upto here")
    if presignedUrl:
        base64=get_as_base64(presignedUrl)
        return base64
    else:
       return False

async def extract_quality_data_from_statements( b64_file: str , password: Optional[str] , regex: str ,bbox: list,template_type: str , page_num : Optional[int]):

    b64_file = b64_file
    password = password
    page_num = page_num
    bbox = bbox
    regex = regex

    if not regex:
        regex = '(.*)'
    path = f'/tmp/temp_{random.randint(0, 10000000)}.pdf'
    
    with open(path, 'wb') as theFile:
        theFile.write(base64.b64decode(b64_file))
    
    doc = read_pdf(path, password)
    page = doc.load_page(page_num)
    all_text = get_text_in_box(page, bbox)
    doc.close()
    template_type = template_type.split('_')[0]

    if os.path.exists(path):
        os.remove(path)
    
    if all_text is not None:
        all_text = all_text.replace('(cid:9)', '')
        all_text = remove_unicode(all_text)
        all_text = all_text.replace('\x01', ' ')
    if regex is not None and all_text is not None:
        regex_match = re.match(regex, all_text)
        if regex_match is not None:
            data = regex_match.group(1)
            return {
                template_type: data,
                "all_text":all_text     
            }
        else:
            return {
                template_type: "",
                "all_text":all_text     
            }
    return False 

@router.post("/extract_multiple_data")
async def extract_multiple_quality_data(request:MultipleData, response: Response, template_type: Optional[str] = None, user=Depends(get_current_user)):
    statement_list=request.statement_list
    passwords= request.passwords
    page_num = request.page_num
    bbox = request.bbox
    regex = request.regex

    if(statement_list is None):
        response.status_code = status.HTTP_400_BAD_REQUEST
        return {
            "message": "Statement id's are required",
            "data": {}
        }
    base_64_list=[]
    print("statement list is ",statement_list)
    for i in statement_list:
        base64=await get_b64_from_statements(i)
        if base64 == False or base64 is None:
            response.status_code = status.HTTP_400_BAD_REQUEST
            return {
                "message": "Statement id does not exists",
                "data": {}
            }
        else:
            base_64_list.append(base64)   
    extracted_data=[]
    if base_64_list is not None:
        for i in base_64_list:
            data=await extract_quality_data_from_statements(i,"",regex,bbox,template_type,page_num)
            if data is False or data is None:
                response.status_code = status.HTTP_400_BAD_REQUEST
                return {
                    "message": "Unable to retrieve data",
                    "data": {}
                    }
            else:
                extracted_data.append(data)

        response.status_code = status.HTTP_200_OK
        return {
                    "message": "ok",
                    "data": extracted_data
                }
    else:
        response.status_code = status.HTTP_400_BAD_REQUEST
        return {
                "message": "Unable to retrieve data",
                "data": {}
            }

    
