from fastapi import APIRouter, Response, Depends, status

from app.constants import QUALITY_DATABASE_NAME, PORTAL_DATABASE_NAME
from app.template_solutioning.request_models import TemplateAddition, GetExtractedData, IgnoreType, SuperUserApproval
from app.database_utils import portal_db, quality_database, DBConnection
from app.dependencies import get_current_user
from app.template_solutioning.logo_hash import get_images
from uuid import uuid4
from app.conf import *
from app.template_solutioning.quality_data import invoke_template_handler_lambda,check_all_text_logo_less_bbox
from app.template_dashboard.utils import viewable_presigned_url, create_viewable_presigned_url, check_text_set
from app.template_solutioning.dashboard_calls import fb_dashboard_api_cc_create_or_update, fb_dashboard_api_update_cc_password
import json
from typing import Optional
from datetime import datetime
from itertools import combinations
import random
import re
import math
from app.pdf_utils import read_pdf
import fitz
import hashlib
from app.template_solutioning.redis import get_credit_card_invalid_text_bbox_templates, get_credit_card_template_with_template_type

credit_router = APIRouter()
STATEMENT_TYPE = 'credit_card' 

async def create_or_update_password_types(detected_password_type, bank_name):
    portal_query = f"select * from bank_connect_fsmlibgeneraldata where tag='cc_password_types_{bank_name}' and type='cc_password_types_{bank_name}'"
    portal_data = DBConnection(PORTAL_DATABASE_NAME).execute_query(portal_query)
    request_body = {'bank_name':bank_name, 'password_type':detected_password_type, 'operation':'create'}
    if portal_data is not None:
        request_body['operation'] = 'update'
    fb_response = fb_dashboard_api_update_cc_password(request_body)

# Need to add lambdas support for all these things
# Need to see how are we going to handle, date_bbox and trans_bbox
invalid_map = {
    "card_number_bbox":"cc_null",
    "payment_due_date":"cc_payment_due_date_null",
    "total_dues":"cc_total_dues_null",
    "statement_date":"cc_statement_date_null",
    "min_amt_due":"cc_min_amt_due_null",
    "purchase/debits":"cc_purchase_debits_null",
    "name_bbox":"cc_name_null",
    "credit_limit":"cc_credit_limit_null",
    "avl_credit_limit":"cc_avl_credit_limit_null",
    "opening_balance":"cc_opening_balance_null",
    "avl_cash_limit":"cc_avl_cash_limit_null",
    "payment/credits":"cc_payment_credits_null",
    "address_bbox":"cc_address_null"
}

identity_map = {
    "card_number_bbox":"credit_card_number",
    "payment_due_date":"payment_due_date",
    "total_dues":"total_dues",
    "statement_date":"statement_date",
    "min_amt_due":"min_amt_due",
    "purchase/debits":"purchase_or_debits",
    "name_bbox":"name",
    "credit_limit":"credit_limit",
    "avl_credit_limit":"avl_credit_limit",
    "opening_balance":"opening_balance",
    "avl_cash_limit":"avl_cash_limit",
    "payment/credits":"payment_or_credits",
    "address_bbox":"address"
}

reverse_invalid_map = {v:k for k,v in invalid_map.items()}

async def insert_into_cc_mocktemplates(values: dict):
    query = """
                    INSERT INTO cc_mocktemplates (template_uuid, template_type, template_json, bank_name, request_by, active_status, statement_id, priority, priority_to) 
                VALUES (:template_uuid, :template_type, :template_json, :bank_name, :request_by, :active_status, :statement_id, :priority, :priority_to)
                    """
    
    if "statement_id" not in values.keys():
        values["statement_id"]=""

    try:
        await quality_database.execute(query = query, values = values)
    except Exception as e:
        print(e)
        return {"message": "some error occured"}
    
    return {"message": "successfully inserted"}

async def perform_simulation_ingestion(statement_id, bank_name, template_type):
    quality_data = await get_credit_card_template_with_template_type(bank_name, template_type)

    template_id = None
    data_from_template_handler = None
    for i in range(len(quality_data)):
        # quality_data[i] = dict(quality_data[i])
        template_id = quality_data[i].get('template_uuid')
        data_from_template_handler = invoke_template_handler_lambda({
            "bucket": CC_PDF_BUCKET,
            "key": f"cc_pdfs/{statement_id}_{bank_name}.pdf",
            "template": json.loads(quality_data[i].get("template_json")),
            "template_type":template_type if template_type!='name_bbox' else 'cc_name_bbox',
            "new_flow":True,
            "bank":bank_name
        })

        if len(data_from_template_handler)>0:
            data_from_template_handler = data_from_template_handler[0]
            print(data_from_template_handler)
            if not data_from_template_handler:
                continue
            if len(data_from_template_handler['data'])==0:
                continue
            if not data_from_template_handler['data'][0]:
                continue

        return data_from_template_handler, template_id
    return data_from_template_handler, template_id

async def get_all_requested_cc_templates(bank_name: Optional[str] = None, template_type: Optional[str] = None):
    
    if bank_name in [None, '']:
        query = """    
                SELECT template_uuid, template_type, template_json, bank_name, active_status, created_at, request_by, statement_id, priority, priority_to
                FROM cc_mocktemplates where active_status in (1,2,4)
            """
        try:
            query_result = await quality_database.fetch_all(query=query)
        except Exception as e:
            print(e)
            return {"message": "some error occured"}
    else:
        query = """    
                SELECT template_uuid, template_type, template_json, bank_name, active_status, created_at, request_by, statement_id, priority, priority_to
                FROM cc_mocktemplates where bank_name=:bank_name and active_status in (1,2,4)
            """
        try:
            query_result = await quality_database.fetch_all(
                query=query, 
                values= {"bank_name": bank_name}
            )
        except Exception as e:
            print(e)
            return {"message": "some error occured"}
    
    result = []
    for i in range(len(query_result)):
        item = dict(query_result[i])
        if template_type not in [None, ''] and item.get("template_type")!=template_type:
            continue
        result.append(item)
        result[-1]["template_json"]=json.loads(result[-1]["template_json"])
    
    return result

async def simulate_invalid_pdf_addition(template_json, template_uuid):
    quality_query = """
                    select statement_id, bank_name from cc_statement_quality where (
                        cc_null=true or cc_payment_due_date_null=true or cc_total_dues_null=true or cc_statement_date_null=true or cc_min_amt_due_null=true or
                        cc_purchase_debits_null=true or cc_name_null=true or cc_credit_limit_null=true or cc_opening_balance_null=true or cc_avl_credit_limit_null=true or cc_avl_cash_limit_null=true or
                        cc_address_null=true or cc_payment_credits_null=true
                    )
                    AND (
                        cc_null_maker_status=false and cc_payment_due_date_null_maker_status=false and cc_total_dues_null_maker_status=false and cc_statement_date_null_maker_status=false and
                        cc_min_amt_due_null_maker_status=false and cc_purchase_debits_null_maker_status=false and cc_name_null_maker_status=false and cc_credit_limit_null_maker_status=false and cc_avl_credit_limit_null_maker_status=false and cc_avl_cash_limit_null_maker_status=false and cc_address_null_maker_status=false and
                        cc_payment_credits_null_maker_status=false
                    )
                    """

    quality_data = await quality_database.fetch_all(
        query = quality_query
    )

    for data in quality_data:
        data_dict = dict(data)
        statement_id=data_dict.get('statement_id')
        bank_name = data_dict.get('bank_name')

        key = f"cc_pdfs/{statement_id}_{bank_name}.pdf"
        try:
            pdf_bucket_response = s3.get_object(Bucket=CC_PDF_BUCKET, Key=key)
            print(f"fround key {statement_id}")
            response_data = check_all_text_logo_less_bbox(template_json,pdf_bucket_response,statement_id)
            if response_data:
                parked_data = json.dumps({template_uuid:[{"data":['INVALID_PDF']}]})
                quality_query_to_ignore = """
                                        UPDATE cc_statement_quality
                                        set cc_null_maker_status=true, cc_payment_due_date_null_maker_status=true, cc_total_dues_null_maker_status=true, cc_statement_date_null_maker_status=true,
                        cc_min_amt_due_null_maker_status=true, cc_purchase_debits_null_maker_status=true, cc_name_null_maker_status=true, cc_credit_limit_null_maker_status=true, cc_avl_credit_limit_null_maker_status=true, cc_avl_cash_limit_null_maker_status=true, cc_address_null_maker_status=true,
                        cc_payment_credits_null_maker_status=true, cc_null_maker_parked_data=:present_parked_data, cc_payment_due_date_null_maker_parked_data=:present_parked_data, cc_total_dues_null_maker_parked_data=:present_parked_data, 
                        cc_statement_date_null_maker_parked_data=:present_parked_data, cc_min_amt_due_null_maker_parked_data=:present_parked_data, cc_purchase_debits_null_maker_parked_data=:present_parked_data, cc_name_null_maker_parked_data=:present_parked_data, cc_credit_limit_null_maker_parked_data=:present_parked_data,
                        cc_opening_balance_null_maker_parked_data=:present_parked_data, cc_avl_credit_limit_null_maker_parked_data=:present_parked_data, cc_avl_cash_limit_null_maker_parked_data=:present_parked_data, cc_address_null_maker_parked_data=:present_parked_data, cc_payment_credits_null_maker_parked_data=:present_parked_data
                        where statement_id=:statement_id
                                            """
                await quality_database.execute(query=quality_query_to_ignore, values={"present_parked_data":parked_data,"statement_id":statement_id})
        except Exception as e:
            print(e)

async def simulate_invalid_pdf_approval(template_uuid, approval, template_json, user):
    quality_query = """
                    select statement_id, bank_name, cc_null_maker_parked_data from cc_statement_quality where (
                        cc_null=true or cc_payment_due_date_null=true or cc_total_dues_null=true or cc_statement_date_null=true or cc_min_amt_due_null=true or
                        cc_purchase_debits_null=true or cc_name_null=true or cc_credit_limit_null=true or cc_opening_balance_null=true or cc_avl_credit_limit_null=true or cc_avl_cash_limit_null=true or
                        cc_address_null=true or cc_payment_credits_null=true
                    )
                    AND (
                        cc_null_maker_status=true and cc_payment_due_date_null_maker_status=true and cc_total_dues_null_maker_status=true and cc_statement_date_null_maker_status=true and
                        cc_min_amt_due_null_maker_status=true and cc_purchase_debits_null_maker_status=true and cc_name_null_maker_status=true and cc_credit_limit_null_maker_status=true and cc_avl_credit_limit_null_maker_status=true and cc_avl_cash_limit_null_maker_status=true and cc_address_null_maker_status=true and
                        cc_payment_credits_null_maker_status=true and cc_null_checker_status=false and cc_payment_due_date_null_checker_status=false and cc_total_dues_null_checker_status=false and cc_statement_date_null_checker_status=false and
                        cc_min_amt_due_null_checker_status=false and cc_purchase_debits_null_checker_status=false and cc_name_null_checker_status=false and cc_credit_limit_null_checker_status=false and cc_opening_balance_null_checker_status=false and
                        cc_avl_credit_limit_null_checker_status=false and cc_avl_cash_limit_null_checker_status=false and cc_address_null_checker_status=false and cc_payment_credits_null_checker_status=false
                    )
                    """

    quality_data = await quality_database.fetch_all(
        query = quality_query
    )

    statement_ids = []
    for data in quality_data:
        data_dict = dict(data)
        parked_data = data_dict.get('cc_null_maker_parked_data')
        if parked_data==None:
            continue

        parked_data = json.loads(parked_data)
        temp_data = parked_data.get(template_uuid)
        if temp_data==None or len(temp_data)==0:
            continue

        temp_data=temp_data[0]
        if len(temp_data.get('data'))!=0 and temp_data.get('data')[0]!=None and temp_data.get('data')[0]=='INVALID_PDF':
            statement_ids.append(data_dict.get('statement_id'))
    
    approval_status = 0
    if approval:
        quality_query_for_update = """
                                UPDATE cc_statement_quality set cc_null_checker_status=true, cc_payment_due_date_null_checker_status=true, cc_total_dues_null_checker_status=true, cc_statement_date_null_checker_status=true,
                                cc_min_amt_due_null_checker_status=true, cc_purchase_debits_null_checker_status=true, cc_name_null_checker_status=true, cc_credit_limit_null_checker_status=true, cc_opening_balance_null_checker_status=true,
                                cc_avl_credit_limit_null_checker_status=true, cc_avl_cash_limit_null_checker_status=true, cc_address_null_checker_status=true,cc_payment_credits_null_checker_status=true
                                where statement_id=:statement_id
                                """
        approval_status=3
    else:
        quality_query_for_update = """
                                UPDATE cc_statement_quality set cc_null_maker_status=false, cc_payment_due_date_null_maker_status=false, cc_total_dues_null_maker_status=false, cc_statement_date_null_maker_status=false,
                                cc_min_amt_due_null_maker_status=false, cc_purchase_debits_null_maker_status=false, cc_name_null_maker_status=false, cc_credit_limit_null_maker_status=false, cc_avl_credit_limit_null_maker_status=false, cc_avl_cash_limit_null_maker_status=false, cc_address_null_maker_status=false,
                                cc_payment_credits_null_maker_status=false,
                                cc_null_maker_parked_data=null, cc_payment_due_date_null_maker_parked_data=null, cc_total_dues_null_maker_parked_data=null, 
                                cc_statement_date_null_maker_parked_data=null, cc_min_amt_due_null_maker_parked_data=null, cc_purchase_debits_null_maker_parked_data=null, cc_name_null_maker_parked_data=null, cc_credit_limit_null_maker_parked_data=null,
                                cc_opening_balance_null_maker_parked_data=null, cc_avl_credit_limit_null_maker_parked_data=null, cc_avl_cash_limit_null_maker_parked_data=null, cc_address_null_maker_parked_data=null, cc_payment_credits_null_maker_parked_data=null
                                where statement_id=:statement_id
                                """
    for statement_id in statement_ids:
        await quality_database.execute(query=quality_query_for_update,values={"statement_id":statement_id})
    
    if approval:
        body = {
            "template_uuid" : template_uuid,
            "template_json" : template_json,
            "template_type" : 'invalid_text_bbox',
            "bank_name" : 'generic',
            "priority" : None,
            "priority_to" : None,
            "approved_by" : user.username,
            "operation" : "create"
        }

        mock_active_status = 1
        if mock_active_status == 1:
            response = fb_dashboard_api_cc_create_or_update(body)

        if response.status_code!=200:
            return {"message": "api to update template in fb dashboard failed"}
        
    update_dict = {
        "status":approval_status,
        "template_uuid":template_uuid
    }
    query = """
            UPDATE cc_mocktemplates set active_status=:status where template_uuid=:template_uuid
            """
    
    try:
        await quality_database.execute(query = query, values = update_dict)
    except Exception as e:
        print(e)
        response.status_code = status.HTTP_500_INTERNAL_SERVER_ERROR
        return {"message": "some error occured while updating status in quality db"}
    if approval:
        return {"message": "template approved", "template_uuid": template_uuid}
    else:
        return {"message": "template rejected", "template_uuid": template_uuid}

# def check_all_text_ignore(page,template_json):


async def perform_simulation_ingestion_invalid(statement_id, pdf_bucket_response):
    # quality_query_to_fetch_invalid_mocktemplates = """
    #                                         select * from cc_mocktemplates where template_type='invalid_text_bbox' and active_status=1
    #                                             """
    

    response_metadata = pdf_bucket_response.get('Metadata')
    password = response_metadata.get('pdf_password')
    path = f'/tmp/temp_{statement_id}'
    with open(path,'wb') as theFile:
        theFile.write(pdf_bucket_response['Body'].read())

    doc = read_pdf(path,password)
    page = doc.load_page(0)

    # quality_data = await quality_database.fetch_all(query=quality_query_to_fetch_invalid_mocktemplates)
    quality_data = await get_credit_card_invalid_text_bbox_templates()
    for data in quality_data:
        # data_dict = dict(data)
        data_dict = data
        template_json = json.loads(data_dict.get('template_json'))
        template_uuid = data_dict.get('template_uuid')

        invalid_check = check_text_set(template_json,page)

        if invalid_check:
            if os.path.exists(path):
                os.remove(path)
            return template_uuid
    if os.path.exists(path):
        os.remove(path)
    doc.close()
    return None

@credit_router.post("/superuser_approval", tags=['credit_quality'])
async def superuser_approval(request: SuperUserApproval, response:Response, is_ticket: Optional[bool]=False, user = Depends(get_current_user)):
    if user.user_type != "superuser":
        response.status_code = status.HTTP_401_UNAUTHORIZED
        return {"message": "not authorised"}
    
    template_id = request.template_id
    approval = request.approval

    # getting the information of the current template_id
    quality_query = """
                        SELECT * from cc_mocktemplates where template_uuid = :template_id
                    """
    quality_query_data = await quality_database.fetch_one(query=quality_query, values={"template_id": template_id})
    if not quality_query_data:
        response.status_code = status.HTTP_404_NOT_FOUND
        return {"message": "template id not found"}
    
    quality_query_data = dict(quality_query_data)
    quality_query_data['template_json'] = json.loads(quality_query_data['template_json'])
    
    template_type = quality_query_data['template_type']
    bank_name = quality_query_data["bank_name"]

    if template_type=='invalid_text_bbox':
        return await simulate_invalid_pdf_approval(template_id, approval, quality_query_data['template_json'], user)

    if template_type in invalid_map and not is_ticket:
        null_type = invalid_map[template_type]

        statement_quality_query = f"""
                                    SELECT statement_id, bank_name, pdf_password, {null_type}, {null_type}_maker_status, {null_type}_checker_status, {null_type}_maker_parked_data
                                    FROM cc_statement_quality where {null_type} = TRUE AND
                                    {null_type}_maker_status = TRUE AND
                                    {null_type}_checker_status <> TRUE AND
                                    {null_type}_ignore_case <> TRUE AND
                                    bank_name = :bank_name
                                """
        statement_quality_query_data = await quality_database.fetch_all(query = statement_quality_query, values={"bank_name": bank_name})
    
        # now filtering and keeping only those objects that're related to this template_id
        start_time = datetime.now()
        print(f"CC_SUPERUSER started at {start_time}")
        statement_quality_query_data_final = []
        for items in statement_quality_query_data:
            statement_id = items.get('statement_id')

            temp_data = dict(items)
            parked_data = temp_data[f"{null_type}_maker_parked_data"]
            if parked_data==None:
                continue
            temp_data[f"{null_type}_maker_parked_data"] = json.loads(temp_data[f"{null_type}_maker_parked_data"])
            if template_id in temp_data[f"{null_type}_maker_parked_data"]:
                statement_quality_query_data_final.append(temp_data)
        
        print(f"CC_SUPERUSER parsing complete after {datetime.now()-start_time}")
    
        # prepare a list of all the statement_ids whose checker status needs to be changed
        short_statement_id_list = [_.get("statement_id") for _ in statement_quality_query_data_final]
        print("List of statement_ids that need to be updated --> ", short_statement_id_list)

        if approval:
            update_statement_quality_query = f"""
                                            UPDATE cc_statement_quality set {null_type}_checker_status = TRUE where statement_id = :statement_id
                                        """
        else:
            # print(f"rejecting template --> {template_id}, and defaulting null type --> {null_type}")
            update_statement_quality_query = f"""
                                            UPDATE cc_statement_quality set {null_type}_checker_status = FALSE , {null_type}_maker_status = FALSE , {null_type}_maker_parked_data = null where statement_id = :statement_id
                                        """
        for short_statement_item in short_statement_id_list:
            update_statement_quality_query_data = await quality_database.execute(query=update_statement_quality_query, values = {"statement_id": short_statement_item})
        
        print(f"CC_SUPERUSER updation complete after {datetime.now()-start_time}")
    
    update_quality_dict = {"template_uuid": template_id}
    if approval:
        print(f"Sending this template to dashboard db")
        body = {
            "template_uuid" : template_id,
            "template_json" : quality_query_data['template_json'],
            "template_type" : template_type,
            "bank_name" : bank_name,
            "priority" : quality_query_data['priority'],
            "priority_to" : quality_query_data['priority_to'],
            "approved_by" : user.username,
            "operation" : "create"
        }
            
        if template_type == 'card_type_hash':
            query = "SELECT * FROM bank_connect_fsmlibcctemplates where template_type='card_type_hash' and bank_name=:bank_name"
            try:
                query_response = await portal_db.fetch_one(query = query, values = {'bank_name':bank_name})
            except Exception as e:
                print(e)
                return Response(data={"message": "Error while searching for the template"}, status = 400)
            if query_response is not None:
                query_response = dict(query_response)
                body['operation'] = "update"
                body['template_uuid'] = query_response.get('template_uuid', None)

        response = fb_dashboard_api_cc_create_or_update(body)

        if response.status_code!=200:
            return {"message": "api to update template in fb dashboard failed"}
        
        # update the status in mocktemplates
    query = """
            UPDATE cc_mocktemplates set active_status=:status where template_uuid=:template_uuid
            """
    
    update_quality_dict["status"] = 3 if approval else 0

    try:
        await quality_database.execute(query = query, values = update_quality_dict)
    except Exception as e:
        print(e)
        response.status_code = status.HTTP_500_INTERNAL_SERVER_ERROR
        return {"message": "some error occured while updating status in quality db"}
    if approval:
        return {"message": "template approved", "template_uuid": template_id}
    else:
        return {"message": "template rejected", "template_uuid": template_id}
    

# @credit_router.post("/ingest",tags=['credit_quality'])
async def ingest_cc_statement(portalData: dict):
    statement_id = portalData.get('statement_id')

    startime = datetime.now()
    print(f"CC_INGEST :: {statement_id}, started at : {startime}")
    
    redis_response = redis_cli.get(f'{STATEMENT_TYPE}_ingest_{statement_id}')
    if redis_response != None:
        return {"message": "This statement is already ingested"}
    

    statement_status = portalData.get('statement_status')
    if statement_status==2:
        print(f"statement_id: {statement_id}, is an image, not doing ingestion.")
        redis_cli.set(f'{STATEMENT_TYPE}_ingest_{statement_id}', statement_id, ex=172800)
        return {"message":f"statement_id: {statement_id}, is an image, not doing ingestion."}

    bank_name = portalData.get("bank_name")
    if statement_status==1:
        print(f"statement_id: {statement_id}, password is incorrect, not doing ingestion.")
        # Using password_simulator to find password 5% of the time
        random_number = random.uniform(0, 1)
        check_probablity = 1 if bank_name in ['rbl','yesbnk','baroda','canara','pnbbnk','boi'] else 0.05
        if random_number > check_probablity:
            redis_cli.set(f'{STATEMENT_TYPE}_ingest_{statement_id}', statement_id, ex=172800)
            return {"message":f"statement_id: {statement_id}, password is incorrect, not doing ingestion."}
        name = portalData.get("name", "")
        pan_number = portalData.get("pan_number", "")
        cc_number = portalData.get("cc_number_digits", "")
        phone_number = portalData.get("registered_mobile_number", "")
        dob = portalData.get("dob")
        detected_passwword, detected_password_type = await simulate_different_password_types(statement_id, name, pan_number, cc_number, phone_number, dob, bank_name)
        if detected_passwword:
            qualitydb_insert_query = """
                INSERT INTO cc_statement_quality (statement_id, bank_name, pdf_password, password_type) 
                VALUES (%(statement_id)s, %(bank_name)s, %(pdf_password)s, %(password_type)s)
                """
            values = {
                "statement_id": statement_id,
                "bank_name": bank_name,
                "pdf_password": detected_passwword,
                "password_type": detected_password_type
            }
            DBConnection(QUALITY_DATABASE_NAME).execute_query(
                query=qualitydb_insert_query,
                values=values
            )

            await create_or_update_password_types(detected_password_type, bank_name)
        redis_cli.set(f'{STATEMENT_TYPE}_ingest_{statement_id}', statement_id, ex=172800)
        return {"message":f"statement_id: {statement_id}, password is incorrect, not doing ingestion."}
    
    if bank_name == 'rbl':
        redis_cli.set(f'{STATEMENT_TYPE}_ingest_{statement_id}', statement_id, ex=172800)
        return {"message": "Not Ingesting now"}

    pdf_password = portalData.get("pdf_password")

    value_map = {"bank_name" : bank_name}
    value_map["statement_id"] = statement_id
    value_map["pdf_password"] = pdf_password

    key = f"cc_pdfs/{statement_id}_{bank_name}.pdf"
    try:
        pdf_bucket_response = s3.get_object(Bucket=CC_PDF_BUCKET, Key=key)
    except Exception as e:
        print(e)
        redis_cli.set(f'{STATEMENT_TYPE}_ingest_{statement_id}', statement_id, ex=172800)
        return {"message":f"statement_id: {statement_id}, not in pdf bucket, not doing ingestion."} 
    
    invalid_template = await perform_simulation_ingestion_invalid(statement_id, pdf_bucket_response)

    for template_type in invalid_map.keys():
        is_extracted = (portalData is not None) and (portalData.get(identity_map[template_type], None) is not None)
        null_type = invalid_map[template_type]
        value_map[null_type] = not is_extracted
        if invalid_template!=None:
            value_map[f"{null_type}_maker_status"] = True
            value_map[f"{null_type}_maker_parked_data"] = json.dumps({invalid_template:[{"data":['INVALID_PDF']}]})
            value_map[f"{null_type}_checker_status"] = False
            value_map[f"{null_type}_ignore_case"] = False
        else:
            value_map[f"{null_type}_maker_status"] = False
            value_map[f"{null_type}_maker_parked_data"] = None
            value_map[f"{null_type}_checker_status"] = False
            value_map[f"{null_type}_ignore_case"] = False
            if value_map[null_type] and value_map[f"{null_type}_ignore_case"]==False:
                data_from_template_handler, template_id = await perform_simulation_ingestion(statement_id, bank_name, template_type)
                if data_from_template_handler and template_id:
                    if len(data_from_template_handler['data'])!=0 and data_from_template_handler['data'][0] not in [None,'']: 
                        value_map[f"{null_type}_maker_parked_data"] = json.dumps({template_id : [data_from_template_handler]})
                        value_map[f"{null_type}_maker_status"] = True

    value_map["pdf_ignore_reason"] = ""

    query_column = ""
    query_value = ""
    for template_type in invalid_map.keys():
        null_type = invalid_map[template_type]
        query_column = query_column + f"{null_type}, {null_type}_maker_status, {null_type}_maker_parked_data, {null_type}_checker_status, {null_type}_ignore_case,"
        query_value = query_value + f"%({null_type})s, %({null_type}_maker_status)s, %({null_type}_maker_parked_data)s, %({null_type}_checker_status)s, %({null_type}_ignore_case)s,"


    qualityInsertQuery = f"""
                            Insert into cc_statement_quality (
                                statement_id, bank_name, pdf_password,
                                {query_column} pdf_ignore_reason
                            ) VALUES (
                                %(statement_id)s, %(bank_name)s, %(pdf_password)s,
                                {query_value} %(pdf_ignore_reason)s
                            );
                        """
    values = value_map
    insertData = DBConnection(QUALITY_DATABASE_NAME).execute_query(query=qualityInsertQuery, values=values)
    
    print(f'CC_INGEST :: COMPLETED AFTER {datetime.now()-startime} for statement : {statement_id}')
    redis_cli.set(f'{STATEMENT_TYPE}_ingest_{statement_id}', statement_id, ex=172800)
    return {"message": "success"}

@credit_router.post("/request_addition",tags=['credit_quality'])
async def request_addition_cc_template(request:TemplateAddition, response:Response, is_ticket: Optional[bool]=False, user= Depends(get_current_user)):
    template_type = request.template_type
    template_json = request.template_json
    statement_id = request.statement_id
    bank_name = request.bank_name

    starttime = datetime.now()
    print(f"CC_REQUEST_ADDITION :: {statement_id}, started at {starttime}")

    if template_type in [None, ""] or template_json in [None, ""] or bank_name in [None, ""]:
        response.status_code = status.HTTP_400_BAD_REQUEST
        return {"message": "invalid body"}
    
    eligible_template_type = ['rewards_opening_balance_bbox', 'rewards_closing_balance_bbox','rewards_points_expired_bbox','rewards_points_claimed_bbox','rewards_points_credited_bbox', 'card_type_bbox','address_bbox','name_bbox','card_number_bbox','payment_due_date','total_dues','min_amt_due','credit_limit',
                          'avl_credit_limit','avl_cash_limit','opening_balance','payment/credits','purchase/debits',
                        'statement_date','date_bbox','trans_bbox', 'logo_hash', 'invalid_text_bbox', 'card_type_hash']
    
    if template_type not in eligible_template_type:
        response.status_code = status.HTTP_400_BAD_REQUEST
        return {"message": "Invalid template type requested, not supported."}
    
    if statement_id in [None, ""]:
        response.status_code = status.HTTP_400_BAD_REQUEST
        return {"message": "please enter a valid statement id"}
    
    if template_type == 'card_type_hash':
        logo_hash_list = list(template_json.keys())
        if len(logo_hash_list) != 1:
            response.status_code = status.HTTP_400_BAD_REQUEST
            return {"message":"only one hash must be provded"}

        selected_hash = logo_hash_list[0]
        await upload_selected_hash(selected_hash, statement_id, bank_name)
    
    template_uuid = f"{template_type}_{str(uuid4())}"
    values = {
        "template_uuid": template_uuid,
        "template_type": template_type,
        "template_json": json.dumps(template_json),
        "bank_name": bank_name,
        "priority": None,
        "priority_to": None,
        "request_by": user.username,
        "active_status": 1,
        "statement_id": statement_id
    }

    insertion_response = await insert_into_cc_mocktemplates(values)

    redis_key = f'credit_card_template_{template_type}_{bank_name}'
    redis_cli.delete(redis_key)

    if insertion_response.get("message")=="some error occured":
        response.status_code = status.HTTP_500_INTERNAL_SERVER_ERROR
        return insertion_response
    
    if template_type == 'invalid_text_bbox':
        await simulate_invalid_pdf_addition(template_json, template_uuid)
        return {"message": "simulation successful", "template_uuid": template_uuid}
    
    if template_type not in invalid_map.keys() or is_ticket:
        return {"message": "new template request has been made", "template_uuid": template_uuid}
    
    print(f'{template_uuid} added to mocktemplates, performing simulation')

    bucket = CC_PDF_BUCKET

    null_type = invalid_map[template_type]
    print("Null Type --> ", null_type)

    query_to_get_all_not_done_templates_for_this_type = f"""
                            SELECT statement_id, bank_name, pdf_password,
                                {null_type}, {null_type}_maker_status, {null_type}_maker_parked_data
                            FROM cc_statement_quality 
                            WHERE {null_type}=TRUE
                            AND {null_type}_maker_status <> TRUE
                            AND {null_type}_checker_status <> TRUE
                            AND {null_type}_ignore_case <> TRUE
                            AND bank_name=:bank_name order by created_at desc
                    """
    
    data_from_the_query = await quality_database.fetch_all(
                                query = query_to_get_all_not_done_templates_for_this_type, 
                                values = {
                                    "bank_name": bank_name
                                }
                        )
    
    for i in range(len(data_from_the_query)):
        data_from_the_query[i] = dict(data_from_the_query[i])

    for items in data_from_the_query:
        if items.get(null_type) and not items.get(f"{null_type}_marker_status"):
            statement_id = items.get("statement_id")

            key = f"cc_pdfs/{statement_id}_{bank_name}.pdf"
            try:
                pdf_bucket_response = s3.get_object(Bucket=bucket, Key=key)
            except Exception as e:
                print(e)
                continue

            # get the data from the template handler lambda for this 
            invocation_payload = {
                "transaction_flag": False,
                "bucket": bucket,
                "key": key,
                "template": template_json,
                "template_type": template_type,
                "new_flow": True,
                "bank" : bank_name
            }

            print("Invocation Payload ", invocation_payload)
            lambda_response_for_this_template = lambda_client.invoke(
                FunctionName = TEMPLATE_HANDLER_LAMBDA_FUNCTION_NAME, 
                Payload = json.dumps(invocation_payload), 
                InvocationType='RequestResponse'
            )
            http_status = lambda_response_for_this_template.get('ResponseMetadata', {}).get('HTTPStatusCode')
            headers = lambda_response_for_this_template.get('ResponseMetadata', {}).get('HTTPHeaders', {})
            if http_status != 200 or headers.get('x-amz-function-error') is not None:
                response.status_code=500
                return {'message': 'something went wrong in getting for single template'}
            response_data = lambda_response_for_this_template['Payload']._raw_stream.data.decode("utf-8")
            present_templates_response = json.loads(response_data)
            
            if present_templates_response:
                # check the contents of the data
                data = present_templates_response[0]
                if not data:
                    continue

                if (len(data['data'])==0) or (data['data'][0] in ['',None]):
                    continue
                
                print(f"need to update {null_type} for statement_id: {statement_id} with parked data: {present_templates_response}")
                # updating parked data with template response
                update_query = f"""
                                update cc_statement_quality set {null_type}_maker_parked_data = :parked_data,
                                {null_type}_maker_status = TRUE
                                where statement_id = :statement_id and {null_type}=TRUE
                            """
                update_query_result = await quality_database.execute(
                                            query=update_query, 
                                            values = {
                                                "parked_data": json.dumps({template_uuid : present_templates_response}),
                                                "statement_id": items.get("statement_id")
                                            }
                                        )
            print(f"Statement_id : {statement_id} is updated in bank_connect_statement quality.")
    
    print(f"CC_REQUEST_ADDITION :: {statement_id}, completed after {datetime.now() - starttime}")
    return {"message": "simulation successful", "template_uuid": template_uuid}

@credit_router.post("/get_extracted_data", tags=['credit_quality'])
async def get_extracted_data(request: GetExtractedData, response: Response, user=Depends(get_current_user)):
    statement_id = request.statement_id
    template_type = request.template_type
    template_json = request.template_json
    bank_name = request.bank_name

    if template_type not in invalid_map and template_type not in ['invalid_text_bbox','date_bbox', 'card_type_bbox', 'rewards_opening_balance_bbox', 'rewards_closing_balance_bbox','rewards_points_expired_bbox','rewards_points_claimed_bbox','rewards_points_credited_bbox', 'card_type_hash']:
        response.status_code=400
        return {'message': 'This template type is not supported yet'}

    bucket = CC_PDF_BUCKET
    key = f"cc_pdfs/{statement_id}_{bank_name}.pdf"

    # first check if this key exists in this bucket
    try:
        pdf_bucket_response = s3.get_object(Bucket=bucket, Key=key)
    except Exception as e:
        print(e)
        response.status_code = status.HTTP_404_NOT_FOUND
        return {"message": "key not found"}
    
    if template_type == 'card_type_hash':
        card_hash = list(template_json.keys())[0]
        return {
            'template_json':template_json,
            'presigned_url': viewable_presigned_url(f"quality_logo/cc_card_hashes/{card_hash}.png", QUALITY_BUCKET, "image/png")
        }
    
    if template_type == 'invalid_text_bbox':
        return {'data':check_all_text_logo_less_bbox(template_json,pdf_bucket_response,statement_id)}
    
    # get the data from the template handler lambda for this 
    invocation_payload = {
        "bucket": bucket,
        "key": key,
        "template": template_json,
        "template_type": template_type if template_type!='name_bbox' else 'cc_name_bbox',
        "new_flow": True,
        "bank" : bank_name
    }

    lambda_response_for_this_template = lambda_client.invoke(
        FunctionName = TEMPLATE_HANDLER_LAMBDA_FUNCTION_NAME, 
        Payload=json.dumps(invocation_payload), 
        InvocationType='RequestResponse'
    )

    http_status = lambda_response_for_this_template.get('ResponseMetadata', {}).get('HTTPStatusCode')
    headers = lambda_response_for_this_template.get('ResponseMetadata', {}).get('HTTPHeaders', {})
    if http_status != 200 or headers.get('x-amz-function-error') is not None:
        response.status_code=500
        return {'message': 'something went wrong in getting for single template'}
    response_data = lambda_response_for_this_template['Payload']._raw_stream.data.decode("utf-8")

    return {"data": json.loads(response_data)}

@credit_router.post("/ignore_case", tags=['credit_quality'])
async def get_null_cases(request: IgnoreType, response: Response, user= Depends(get_current_user)):
    statement_ids = request.statement_ids
    null_type = request.null_type

    if null_type not in reverse_invalid_map.keys():
        response.status_code = status.HTTP_400_BAD_REQUEST
        return {"message": "invalid null type"}
    
    if len(statement_ids)==0:
        return {"message": "ignored statements"}

    # now get the table from quality statement table
    statement_ids = str(statement_ids)
    statement_ids = statement_ids[1:]
    statement_ids = statement_ids[:-1]
    statement_ids = f"({statement_ids})"

    update_statement_quality_table_query = f"""
                                            update cc_statement_quality set {null_type}_ignore_case=True where statement_id in {statement_ids}
                                        """
    update_statement_quality_table_query_data = await quality_database.execute(query=update_statement_quality_table_query)

    return {"message": "ignored statements"}

@credit_router.get("/{null_type:str}_cases", tags=['credit_quality'])
async def get_null_cases(null_type:str, response: Response, current_page:Optional[int]=None, selected_bank:Optional[str]=None, max:Optional[int]=100, user= Depends(get_current_user)):
    # print("Request received for null type --> ", null_type)
    starttime = datetime.now()
    print(f'CC_NULL CASES :: started at {starttime}')
    if null_type not in reverse_invalid_map.keys():
        response.status_code = status.HTTP_400_BAD_REQUEST
        return {"message": "invalid null type"}
    quality_query_for_cc_statement_null_type = f"""
                    select bank_name, count(statement_id) as cases from cc_statement_quality where cc_null <> true and {null_type}=true and 
                    {null_type}_maker_status <> true and {null_type}_checker_status <> true and {null_type}_ignore_case <> TRUE 
                    group by bank_name order by cases desc
                    """
    quality_query_for_cc_null_type_only = f"""
                    select bank_name, count(statement_id) as cases from cc_statement_quality where {null_type}=true and 
                    {null_type}_maker_status <> true and {null_type}_checker_status <> true and {null_type}_ignore_case <> TRUE
                    group by bank_name order by cases desc
                    """
    quality_query = quality_query_for_cc_statement_null_type if null_type != 'cc_null' else quality_query_for_cc_null_type_only
    bank_cases_list = await quality_database.fetch_all(query=quality_query)
    total_cases = 0
    for cases in bank_cases_list:
        total_cases += cases.get('cases',0)
    if selected_bank is None or current_page is None:
        return {
            "bank_list":bank_cases_list,
            "all_data":[],
            "total_cases":total_cases
        }

    query_for_all_data = f"""
                            SELECT statement_id, bank_name, pdf_password,
                                {null_type}, {null_type}_maker_status, {null_type}_maker_parked_data
                            FROM cc_statement_quality
                            WHERE {null_type}=TRUE
                            AND {null_type}_maker_status <> TRUE
                            AND {null_type}_checker_status <> TRUE
                            AND {null_type}_ignore_case <> TRUE
                            AND bank_name=:bank_name
                    """
    
    if null_type!='cc_null':
        query_for_all_data += 'AND cc_null = FALSE'
    
    all_data = await quality_database.fetch_all(query=query_for_all_data,values={
        'bank_name':selected_bank
    })
    offset_val = (current_page-1)*max

    data_to_return = []
    for i in range(offset_val,min(len(all_data),offset_val+max)):
        current_data = dict(all_data[i])
        current_data['presigned_url'] = create_viewable_presigned_url(all_data[i]['statement_id'], all_data[i]['bank_name'], is_cc=True)
        data_to_return.append(current_data)

    print(f'CC_NULL_CASES :: Ended at {datetime.now()-starttime}')
    response.status_code = status.HTTP_200_OK
    return {"bank_list": bank_cases_list, "all_data": data_to_return, "total_cases": total_cases}

@credit_router.get("/get_parked_data", tags=['credit_quality'])
async def get_parked_data(template_id: str, response: Response, page: Optional[int]=1, maxi: Optional[int]=10, user=Depends(get_current_user)):
    # first check whether this template_id was actually requested
    quality_query = """
                        SELECT * from cc_mocktemplates where template_uuid = :template_id
                    """
    
    quality_query_data = await quality_database.fetch_one(query=quality_query, values={"template_id": template_id})
    if not quality_query_data:
        response.status_code = status.HTTP_404_NOT_FOUND
        return {"message": "template id not found"}
    
    quality_query_data = dict(quality_query_data)
    bank_name = quality_query_data["bank_name"]
    template_type = quality_query_data["template_type"]

    if template_type not in invalid_map and template_type not in ['invalid_text_bbox']:
        response.status_code = status.HTTP_404_NOT_FOUND
        return {"message": "this template type is not supported yet"}
    
    null_type = 'cc_null' if template_type=='invalid_text_bbox' else invalid_map[template_type]

    quality_query = f"""
                        SELECT statement_id, pdf_password, bank_name, {null_type}_maker_parked_data 
                        from cc_statement_quality
                        where bank_name = :bank_name and {null_type}=TRUE
                    """
    query_data = await quality_database.fetch_all(query=quality_query, values={"bank_name": bank_name})
    response_data = []
    for i in range(len(query_data)):
        query_data[i] = dict(query_data[i])
        parked_data = query_data[i][f"{null_type}_maker_parked_data"]
        if not parked_data:
            continue

        temp_data = json.loads(parked_data).get(template_id)
        if not temp_data or not isinstance(temp_data, list):
            continue
        temp_data = temp_data[0]
        if template_type=='invalid_text_bbox' and temp_data.get('data') != ['INVALID_PDF']:
            continue
        query_data[i]["parked_data"] = temp_data
        del query_data[i][f"{null_type}_maker_parked_data"]

        if query_data[i].get('pdf_password','') is None:
            query_data[i]['pdf_password'] = ""

        query_data[i]['template_json'] = quality_query_data['template_json']

        # if template_type in ['logo_hash']:
        #     # also add the presigned url for the logo
              # portal_query_data[i]['png_presigned_url'] = viewable_presigned_url(f"quality_logo/{portal_query_data[i]['parked_data']['selected_hash']}.png", QUALITY_BUCKET, "image/png")
        response_data.append(query_data[i])

    offset_val = (page-1)*maxi
    return_list = []
    for i in range(offset_val,min(len(response_data),offset_val+maxi)):
        return_list.append(response_data[i])

    return {
        "template_type": template_type,
        "data": return_list,
        "total_cases": len(response_data)
    }

@credit_router.get("/requested_templates", tags=['credit_quality'])
async def get_requested_templates(response: Response, bank_name: Optional[str] = None, template_type:str = None, user= Depends(get_current_user)):
    templates = await get_all_requested_cc_templates(bank_name, template_type)
    if isinstance(templates, dict) and templates.get("message")=="some error occured":
        response.status_code = status.HTTP_500_INTERNAL_SERVER_ERROR
        return templates
    
    return {"templates": templates}

def check_both_cases(path, possible_password):
    if not isinstance(possible_password, str):
        return None
    
    possible_password = possible_password.upper()
    doc = read_pdf(path, possible_password)
    if doc!=0:
        return possible_password
    
    possible_password = possible_password.lower()
    doc = read_pdf(path, possible_password)
    if doc!=0:
        return possible_password
    return None

async def simulate_different_password_types(statement_id, name, pan_number, cc_number, phone_number, dob, bank_name):
    try:
        dob_object = datetime.strptime(dob, '%Y-%m-%d')
    except Exception as e:
        dob_object = datetime.now()
        print("dob not in desired format")

    s3_bucket_key = f"cc_pdfs/{statement_id}_{bank_name}.pdf"
    try:
        pdf_bucket_response = s3.get_object(Bucket=CC_PDF_BUCKET, Key=s3_bucket_key)
    except Exception as e:
        print(e)
        return {"message": f"statement_id: {statement_id}, not in pdf bucket, not doing ingestion."} 
    
    pdf_tmp_path = f'/tmp/{statement_id}'
    with open(pdf_tmp_path,'wb') as pdf_file:
        pdf_file.write(pdf_bucket_response['Body'].read())

    ## Variations of Name
    names_list = name.split(" ")
    clean_name = re.sub(r'[^a-zA-Z0-9\s]', '', name)         ## A name without any special characters
    clean_name_without_spaces = clean_name.replace(" ", "")
    name_without_spaces = name.replace(" ", "")

    password_keys = {
        "NS4":          name[:4],                                           ## First 4 letters of Name
        "NS5":          name[:5],                                           ## First 4 letters of Name
        "NE4":          name[-4:],                                          ## Last 4 letters of Name
        "NE5":          name[-5:],                                          ## Last 5 letters of Name
        "NWSS4":        name_without_spaces[:4],                            ## First 4 letters of Name without space
        "NWSS5":        name_without_spaces[:5],                            ## First 4 letters of Name without space
        "NWSE4":        name_without_spaces[-4:],                           ## Last 4 letters of Name without space
        "NWSE5":        name_without_spaces[-5:],                           ## Last 5 letters of Name without space
        "CNS4":         clean_name_without_spaces[:4],                      ## First 4 letters of Clean Name (a name containing letters and digit)
        "CNE4":         clean_name_without_spaces[-4:],                     ## Last 4 letters of Clean Name (a name containing letters and digit)
        "MNS4":         names_list[1][:4] if len(names_list)>1 else "",     ## start 4 letters of middle name
        "LNS4":         names_list[-1][:4],                                 ## start 4 letters of last name
        "BNS4":         bank_name[:4],                                      ## start 4 letters of Bank name
        "DOByyyy":      dob_object.strftime("%Y"),                          ## YYYY of DOB
        "DOBddmm":      dob_object.strftime("%d%m"),                        ## DDMM of DOB
        "DOBmmdd":      dob_object.strftime("%m%d"),                        ## MMDD of DOB
        "DOBmmyy":      dob_object.strftime("%m%y"),                        ## MMYY of DOB
        "DOByymm":      dob_object.strftime("%y%m"),                        ## YYMM of DOB
        "DOBddmmyy":    dob_object.strftime("%d%m%y"),                      ## DDMMYY of DOB
        "DOBddmmyyyy":  dob_object.strftime("%d%m%Y"),                      ## DDMMYYYY of DOB
        "DOBddbb":      dob_object.strftime("%d%b"),                        ## DDmonth of DOB, month is a word rather than a number
        "CCE4":         cc_number[-4:],                                     ## last 4 digits of Credit Card Number
        "PANS4":        pan_number[:4],                                     ## start 4 chars of PAN Card Number
        "PANE4":        pan_number[-4:],                                    ## last 4 chars of PAN Card Number
        "PHNS4":        phone_number[:4],                                   ## start 4 digit of Phone Number
        "PHNE4":        phone_number[-4:],                                  ## last 4 digit of Phone Number
        "PHNS5":        phone_number[:5],                                   ## start 5 digit of Phone Number
        "PHNE5":        phone_number[-5:],                                  ## last 5 digit of Phone Number
    }

    for key, value in password_keys.items():
        try:
            possible_password = check_both_cases(pdf_tmp_path, value)
            if possible_password:
                print(f"Password: {possible_password} of type: {key} for statement with ID: {statement_id}")
                return possible_password, key
        except Exception as e:
            print(f"{key}: ", e)
    
    for pair in combinations(password_keys.items(), 2):
        possible_password = pair[0][1] + pair[1][1]
        password_type = pair[0][0] + pair[1][0]
        try:
            possible_password = check_both_cases(pdf_tmp_path, possible_password)
            if possible_password:
                print(f"Password: {possible_password} of type: {password_type} for statement with ID: {statement_id}")
                return possible_password, password_type
        except Exception as e:
            print(f"{password_type}: ", e)

        ##  Check with reverse order of pair
        possible_password = pair[1][1] + pair[0][1]
        password_type = pair[1][0] + pair[0][0]
        try:
            possible_password = check_both_cases(pdf_tmp_path, possible_password)
            if possible_password:
                print(f"Password: {possible_password} of type: {password_type} for statement with ID: {statement_id}")
                return possible_password, password_type
        except Exception as e:
            print(f"{password_type}: ", e)
    
    return None, None

async def upload_selected_hash(hash_to_upload, statement_id, bank_name):
    try:
        pdf_key = f'cc_pdfs/{statement_id}_{bank_name}.pdf'
        pdf_bucket_response = s3.get_object(Bucket=CC_PDF_BUCKET, Key=pdf_key)
    except Exception as e:
        print(e)
        return
    
    response_metadata = pdf_bucket_response.get('Metadata')
    password = response_metadata.get('pdf_password')
    tmp_file_path = f"/tmp/cc_{statement_id}_{bank_name}.pdf"
    with open(tmp_file_path, 'wb') as file_obj:
        file_obj.write(pdf_bucket_response['Body'].read())
    doc = read_pdf(tmp_file_path, password)
    images = doc.get_page_images(0)
    if os.path.exists(tmp_file_path):
        os.remove(tmp_file_path)
    
    images = images[:100]
    for img in images:
        try:
            xref = img[0]
            pix = fitz.Pixmap(doc, xref)
            png_name = f"/tmp/{statement_id}_{bank_name}-{hash_to_upload}.png"
            if pix.n == 0:
                pix.save(png_name)
            else:
                pix1 = fitz.Pixmap(fitz.csRGB, pix)
                pix1.save(png_name)
            # upload this png to s3 and save the link
            # the name of this s3 file is the image hash
            
            with open(png_name,"rb") as f:
                bytes = f.read() # read entire file as bytes
                readable_hash = hashlib.sha256(bytes).hexdigest()

            if readable_hash == hash_to_upload:
                s3_file_path = f"quality_logo/cc_card_hashes/{readable_hash}.png"

                s3_resource.Bucket(QUALITY_BUCKET).upload_file(
                    png_name, 
                    s3_file_path, 
                    ExtraArgs={
                        'Metadata': {
                                "statement_id": statement_id,
                                "bank_name": bank_name
                            }
                        }
                )
                if os.path.exists(png_name):
                    os.remove(png_name)
                doc.close()
                return

            if os.path.exists(png_name):
                os.remove(png_name)
        except Exception as e:
            print(e)
            continue
    doc.close()        