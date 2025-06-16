from fastapi import APIRouter, Depends, Response, status
from app.template_solutioning.request_models import *
from app.template_solutioning.dashboard_calls import fb_dashboard_api_create_or_update
from app.dependencies import get_current_user
from app.pdf_utils import read_pdf
from app.conf import *
from uuid import uuid4
from app.database_utils import portal_db, quality_database
from copy import deepcopy
import json
import os
import copy
from app.template_solutioning.redis import delete_vanilla_templates
from app.template_solutioning.copy_transactions import copy_transactions_between_statements;

solutioning_router = APIRouter()

"""
    mock template active status understanding
    0 -> request closed
    1 -> pending
    2 -> updation pending
    3 -> request approved
    4 -> priority updation pending
"""

@solutioning_router.get("/")
def health():
    return {"message": "i'm up"}

def is_logo_less_bbox_template_valid(template_json):
    if not isinstance(template_json,dict):
        return False 
    
    template_json_copy = copy.deepcopy(template_json)
    template_json_copy.pop('is_rotated',None)
    for bbox in template_json_copy.keys():
        if not isinstance(template_json_copy[bbox],list) and len(template_json_copy[bbox])!=4:
            return False
        for coordinate in template_json_copy[bbox]:
            coordinate = int(coordinate)
    return True

async def get_all_templates_for_a_bank(bank_name: str, template_type: Optional[str] = None, plus_inactive: Optional[bool] = False) -> dict:
    query = """    
                SELECT template_uuid, template_type, template_json, bank_name, is_active, created_at, approved_by, priority
                FROM bank_connect_fsmlibtemplates where bank_name=:bank_name order by priority
            """
    try:
        query_result = await portal_db.fetch_all( query=query, values= {"bank_name": bank_name})
    except Exception as e:
        print(e)
        return {"message": "some error occured"}
    
    query_result = [_ for _ in query_result if _.get('template_type')==template_type] if template_type not in [None, ""] else query_result
    if not plus_inactive:
        query_result = [_ for _ in query_result if _.get('is_active')]

    result = []
    for i in range(len(query_result)):
        result.append(dict(query_result[i]))
        result[-1]["template_json"]=json.loads(result[-1]["template_json"])

    return result

def get_unused_templates_for_a_bank(bank_name: str) -> dict:
    return dict
    # query = """    
    #             with ftl as
    #             (select split_part(fsmlib_template_uuid, '_', 1) as template_type,
    #             date(created_at::timestamptz at time zone 'Asia/Calcutta') as dt_template,
    #             fsmlib_template_uuid as template_id,
    #             statement_id, fsmlib_template_uuid, fsmlib_template
    #             from bank_connect_templatelogs
    #             where date(created_at::timestamptz at time zone 'Asia/Calcutta') >= current_date - 60),

    #             t as
    #             (select distinct statement_id, lower(bank_name) bank_name from 
    #             bank_connect_transactions
    #             where date(created_at::timestamptz at time zone 'Asia/Calcutta') >= current_date - 60
    #             )

    #             select fsmlib_template
    #             from ftl join t on ftl.statement_id = t.statement_id
    #             where bank_name=%(bank_name)s
    #             group by 1
    #         """
    # try:
    #     global redshift_db
    #     if redshift_db.closed:
    #         print("Reopening Redshift Connection")
    #         redshift_db = connect_to_redshift()
    #     cursor = redshift_db.cursor()
    #     cursor.execute(query, vars={"bank_name": bank_name})
    #     query_result = cursor.fetchall()
    # except Exception as e:
    #     print(e)
    #     redshift_db.rollback()
    #     return {"message": "some error occured"}
    
    # result = []
    # for _, in query_result:
    #     template = json.loads(_)
    #     result.append({
    #         'template_uuid': template.pop('uuid'),
    #         'template_json': template
    #     })
    
    # return result

async def get_all_cc_templates_for_bank(bank_name:str, template_type:Optional[str]=None, plus_inactive: Optional[bool] = False) -> dict:
    query = """
            SELECT template_uuid, template_type, template_json, bank_name, is_active, created_at, approved_by, priority
            FROM bank_connect_fsmlibcctemplates where bank_name=:bank_name order by priority
        """
    try:
        query_result = await portal_db.fetch_all( query=query, values= {"bank_name": bank_name})
    except Exception as e:
        print(e)
        return {"message": "some error occured"}
    
    query_result = [_ for _ in query_result if _.get('template_type')==template_type] if template_type not in [None, ""] else query_result
    if not plus_inactive:
        query_result = [_ for _ in query_result if _.get('is_active')]

    result = []
    for i in range(len(query_result)):
        result.append(dict(query_result[i]))
        result[-1]["template_json"]=json.loads(result[-1]["template_json"])

    return result

async def insert_into_mock_templates(values: dict):
    query = """ 
                INSERT INTO mocktemplates (template_uuid, template_type, template_json, bank_name, request_by, active_status, statement_id, priority, priority_to) 
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

async def get_all_requested_templates(bank_name: Optional[str] = None, template_type: Optional[str] = None):
    
    if bank_name in [None, '']:
        query = """    
                SELECT template_uuid, template_type, template_json, bank_name, active_status, created_at, request_by, statement_id, priority, priority_to
                FROM mocktemplates where active_status in (1,2,4)
            """
        try:
            query_result = await quality_database.fetch_all(query=query)
        except Exception as e:
            print(e)
            return {"message": "some error occured"}
    else:
        query = """    
                SELECT template_uuid, template_type, template_json, bank_name, active_status, created_at, request_by, statement_id, priority, priority_to
                FROM mocktemplates where bank_name=:bank_name and active_status in (1,2,4)
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

@solutioning_router.get("/available_templates", tags=['template_solicitor'])
async def get_available_templates(response: Response, bank_name: str, template_type:str = None, plus_inactive:Optional[bool] = False, user= Depends(get_current_user)):

    templates = await get_all_templates_for_a_bank(bank_name, template_type, plus_inactive)
    if isinstance(templates, dict) and templates.get("message") == "some error occured":
        response.status_code = status.HTTP_500_INTERNAL_SERVER_ERROR
        return templates
    
    return {"templates": templates}


async def get_bank_metadata():
    portal_db_query = "select * from bank_connect_bankmeta"
    query_response = await portal_db.fetch_all(query=portal_db_query)
    for resp in query_response:
        resp = dict(resp)
    return query_response

async def get_bank_master_data():
    portal_db_query = "select * from bank_connect_bankmaster"
    query_response = await portal_db.fetch_all(query=portal_db_query)
    for resp in query_response:
        resp = dict(resp)
    return query_response

@solutioning_router.get("/bank_meta", tags=['template_solicitor'])
async def get_available_templates(response: Response, user= Depends(get_current_user)):
    metadata = await get_bank_metadata()
    return {"bank_metadata": metadata}

@solutioning_router.get("/bank_master", tags=['template_solicitor'])
async def get_available_templates(response: Response, user= Depends(get_current_user)):
    bank_master_data = await get_bank_master_data()
    return {"bank_master_data": bank_master_data}


@solutioning_router.get('/available_cc_templates',tags=['template_solicitor'])
async def get_available_cc_templates(response:Response, bank_name:str, template_type:str=None, plus_inactive:Optional[bool] = False, user=Depends(get_current_user)):
    templates = await get_all_cc_templates_for_bank(bank_name, template_type, plus_inactive)
    if isinstance(templates, dict) and templates.get("message") == "some error occured":
        response.status_code = status.HTTP_500_INTERNAL_SERVER_ERROR
        return templates
    
    return {"templates": templates} 

async def get_distinct_banks_of_existing_templates() -> dict:
    query = """    
                SELECT DISTINCT(bank_name) FROM bank_connect_fsmlibtemplates
            """
    try:
        query_result = await portal_db.fetch_all(query=query)
    except Exception as e:
        print(e)
        return {"message": "some error occured"}

    result = []
    for i in range(len(query_result)):
        result.append(query_result[i]['bank_name'])
    return result

@solutioning_router.get('/transaction_channel', tags=['template_solicitor'])
async def get_transaction_channel(response:Response, bank_name:str=None, user=Depends(get_current_user)):
    fetch_query = 'select * from bank_connect_fsmlibtransactionchannels'
    if bank_name!=None:
        fetch_query = f"""
            select * from bank_connect_fsmlibtransactionchannels where bank_name='{bank_name}'
            """
    
    portal_data = await portal_db.fetch_all(query=fetch_query)
    if portal_data!=None:
        for i in range(len(portal_data)):
            portal_data[i] = dict(portal_data[i])
            portal_data[i]['regex_list'] = json.loads(portal_data[i]['regex_list'])

    return {'transaction_channel':portal_data}

@solutioning_router.get('/merchant_category', tags=['template_solicitor'])
async def get_merchant_category(response:Response, merchant_category:str=None, user=Depends(get_current_user)):
    fetch_query = 'select * from bank_connect_fsmlibmerchantcategory'
    if merchant_category!=None:
        fetch_query = f"""
                    select * from bank_connect_fsmlibmerchantcategory where merchant_category='{merchant_category}'
                    """
    
    portal_data = await portal_db.fetch_all(query=fetch_query)
    if portal_data!=None:
        for i in range(len(portal_data)):
            portal_data[i] = dict(portal_data[i])
            portal_data[i]['tag_list'] = json.loads(portal_data[i]['tag_list'])
    
    return {'merchant_category':portal_data}

@solutioning_router.get('/general_data', tags=['template_solicitor'])
async def get_general_data(response:Response, type:str=None, tag:str=None, user=Depends(get_current_user)):
    base_query = 'select * from bank_connect_fsmlibgeneraldata'
    conditions = []

    if type is not None:
        conditions.append(f"type='{type}'")

    if tag is not None:
        conditions.append(f"tag='{tag}'")

    if conditions:
        fetch_query = f"{base_query} where {' and '.join(conditions)}"
    else:
        fetch_query = base_query

    
    portal_data = await portal_db.fetch_all(query=fetch_query)
    if portal_data!=None:
        for i in range(len(portal_data)):
            portal_data[i] = dict(portal_data[i])
            portal_data[i]['regex_list'] = json.loads(portal_data[i]['regex_list'])
    
    return {'general_data':portal_data}

@solutioning_router.get('/uncleanmerchants', tags=['template_solicitor'])
async def get_general_data(response:Response, bank_name:str=None, user=Depends(get_current_user)):
    base_query = 'select * from bank_connect_fsmlibuncleanmerchants'
    conditions = []

    if bank_name is not None:
        conditions.append(f"bank_name='{bank_name}'")

    if conditions:
        fetch_query = f"{base_query} where {' and '.join(conditions)}"
    else:
        fetch_query = base_query

    
    portal_data = await portal_db.fetch_all(query=fetch_query)
    if portal_data!=None:
        for i in range(len(portal_data)):
            portal_data[i] = dict(portal_data[i])
            portal_data[i]['regex_list'] = json.loads(portal_data[i]['regex_list'])
    
    return {'uncleanmerchants':portal_data}

@solutioning_router.get('/fraud_data', tags=['template_solicitor'])
async def get_merchant_category(response:Response, type:str=None, bank_name:str=None, country:str=None, user=Depends(get_current_user)):
    base_query = 'select * from bank_connect_fsmlibfrauddata'
    conditions = []

    if type is not None:
        conditions.append(f"type='{type}'")

    if bank_name is not None:
        conditions.append(f"bank_name='{bank_name}'")

    if country is not None:
        conditions.append(f"country='{country}'")

    if conditions:
        fetch_query = f"{base_query} where {' and '.join(conditions)}"
    else:
        fetch_query = base_query

    
    portal_data = await portal_db.fetch_all(query=fetch_query)
    if portal_data!=None:
        for i in range(len(portal_data)):
            portal_data[i] = dict(portal_data[i])
            portal_data[i]['data_list'] = json.loads(portal_data[i]['data_list'])
    
    return {'fraud_data':portal_data}

@solutioning_router.get("/available_banks")
async def get_available_banks(response: Response, user= Depends(get_current_user)):
    
    banks = await get_distinct_banks_of_existing_templates()
    if isinstance(banks, dict) and banks.get("message") == "some error occured":
        response.status_code = status.HTTP_500_INTERNAL_SERVER_ERROR
        return banks
    
    return {"banks": banks}


@solutioning_router.get("/requested_templates", tags=['template_solicitor'])
async def get_requested_templates(response: Response, bank_name: Optional[str] = None, template_type:str = None, user= Depends(get_current_user)):
    templates = await get_all_requested_templates(bank_name, template_type)
    if isinstance(templates, dict) and templates.get("message")=="some error occured":
        response.status_code = status.HTTP_500_INTERNAL_SERVER_ERROR
        return templates
    
    return {"templates": templates}

@solutioning_router.post("/request_addition", tags=['template_solicitor'])
async def request_template_addition(request: TemplateAddition, response: Response, user= Depends(get_current_user)):
    template_type = request.template_type
    template_json = request.template_json
    statement_id = request.statement_id
    bank_name = request.bank_name

    if template_type in [None, ""] or template_json in [None, ""] or bank_name in [None, ""]:
        response.status_code = status.HTTP_400_BAD_REQUEST
        return {"message": "invalid body"}
    
    if bank_name in ["federal", "india_post"] and template_type=='trans_bbox':
        bank_name=bank_name+'1'

    # validation: check template_type from the list
    eligible_template_type = ["accnt_bbox", "account_category_bbox", "account_category_mapping", "address_bbox", "date_bbox", "ifsc_bbox", "last_page_regex", "account_delimiter_regex",
                              "limit_bbox", "micr_bbox", "name_bbox", "trans_bbox", "is_od_account_bbox", "od_limit_bbox", "logo_hash", "logo_less_bbox", "currency_bbox", "ignore_logo_hash",
                              "opening_bal_bbox", "closing_bal_bbox", "opening_date_bbox", 'invalid_text_bbox', 'email_bbox', 'phone_number_bbox', 'pan_number_bbox', 'joint_account_holders_regex']
    if template_type not in eligible_template_type:
        response.status_code = status.HTTP_400_BAD_REQUEST
        return {"message": "Invalid template type requested, not supported."}

    if statement_id in [None, ""]:
        response.status_code = status.HTTP_400_BAD_REQUEST
        return {"message": "please enter a valid statement id"}

    if template_type == "logo_hash" and (not isinstance(template_json, list) or len(template_json) < 1):
        if len(template_json)==0:
            response.status_code = status.HTTP_400_BAD_REQUEST
            return {"message": "Invalid template, empty list provided"}
        if not isinstance(template_json[0], (int, str)):
            response.status_code = status.HTTP_400_BAD_REQUEST
            return {"message": "Invalid logo hash type."}
        
    if template_type == "logo_hash" and  isinstance(template_json[0], str):
        # this is because there is a weird conversion issue in ecma script where they are rounding off long ints
        print("type casting logo hash to int.")
        template_json[0] = int(template_json[0])

    if template_type == 'ignore_logo_hash':
        if not isinstance(template_json,dict):
            response.status_code = status.HTTP_400_BAD_REQUEST
            return {"message": "Invalid Template Type"}
        
        ignore_hash_list = template_json.get('hash_list',[])
        if not isinstance(ignore_hash_list,list) or len(ignore_hash_list)==0:
            response.status_code = status.HTTP_400_BAD_REQUEST
            return {"message": "Ignore has list not found"}
        
        for i in range(len(ignore_hash_list)):
            ignore_hash_list[i] = int(ignore_hash_list[i])

        template_json['hash_list']=ignore_hash_list

    if template_type in ['logo_less_bbox','invalid_text_bbox']:
        is_valid = is_logo_less_bbox_template_valid(template_json)
        if not is_valid:
            return {'message':'invalid template json'}
    
    # Validation for new template for logoless_bank_logo
        
    template_uuid = template_type+"_"+str(uuid4())

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
    
    if template_type in ['invalid_text_bbox']:
        values['bank_name']='generic'

    delete_vanilla_templates()
    insertion_response = await insert_into_mock_templates(values)
    if insertion_response.get("message")=="some error occured":
        response.status_code = status.HTTP_500_INTERNAL_SERVER_ERROR
        return insertion_response
    
    return {"message": "new template request has been made", "template_uuid": template_uuid}

@solutioning_router.post("/request_updation", tags=['template_solicitor'])
async def request_template_updation(request: TemplateUpdation, response: Response, user=Depends(get_current_user)):
    bank_name = request.bank_name
    template_type = request.template_type
    template_json = request.template_json
    template_uuid = request.template_uuid
    statement_id = request.statement_id 

    templates = await get_all_templates_for_a_bank(bank_name, template_type)
    relevant_template = [_ for _ in templates if _.get("template_uuid")==template_uuid]
    if len(relevant_template)>0:
        relevant_template = relevant_template[0]
    else:
        response.status_code = status.HTTP_404_NOT_FOUND
        return {"message": "template not found"}
    
    
    template_type = relevant_template.get("template_type") if template_type in ["", None] else template_type
    template_json = relevant_template.get("template_json") if template_json in ["", None] else template_json
    
    values = {
                "template_uuid": template_uuid,
                "template_type": template_type,
                "template_json": json.dumps(template_json),
                "bank_name": bank_name,
                "priority": None,
                "priority_to": None,
                "request_by": user.username,
                "statement_id": statement_id,
                "active_status": 2
        }
    
    insertion_response = await insert_into_mock_templates(values)
    if insertion_response.get("message")=="some error occured":
        response.status_code = status.HTTP_500_INTERNAL_SERVER_ERROR
        return insertion_response
    
    return {"message": "template update request has been made", "template_uuid": template_uuid}

@solutioning_router.post("/perform_template_validation", tags=['template_solicitor'])
async def perform_template_validation(request: TemplateValidation, response: Response, user=Depends(get_current_user)):
    key = f"pdf/{request.key}"
    bank_name = request.bank_name
    template = request.template
    template_type = request.template_type
    bucket = PDF_BUCKET
    tmp_file_path = f"/tmp/{key}"

    # check if the object is present in the bucket
    try:
        pdf_bucket_response = s3.get_object(Bucket=bucket, Key=key)
        # write a temporary file with content
        with open(tmp_file_path, 'wb') as file_obj:
            file_obj.write(pdf_bucket_response['Body'].read())
    except Exception as e:
        print(e)
        response.status_code=404
        if os.path.exists(tmp_file_path):
            os.remove(tmp_file_path)
        return {"message": "file not found"}
    
    response_metadata = pdf_bucket_response.get('Metadata')
    password = response_metadata.get('pdf_password')

    doc = read_pdf(tmp_file_path, password)
    num_pages = doc.page_count
    doc.close()
    response_data_dict = {}
    transaction_flag = template_type == "trans_bbox"

    for page_number in range(min(3, num_pages)):
        invocation_payload = {
                "transaction_flag": transaction_flag,
                "bucket": bucket,
                "key": key,
                "page_num": page_number,
                "template": template,
                "bank" : bank_name
            }

        # invoke `get_data_for_template_handler` lambda to get the data extracted from this template
        lambda_response = lambda_client.invoke(
            FunctionName = TEMPLATE_HANDLER_LAMBDA_FUNCTION_NAME, 
            Payload=json.dumps(invocation_payload), 
            InvocationType='RequestResponse'
        )

        http_status = lambda_response.get('ResponseMetadata', {}).get('HTTPStatusCode')
        headers = lambda_response.get('ResponseMetadata', {}).get('HTTPHeaders', {})
        if http_status != 200 or headers.get('x-amz-function-error') is not None:
            response.status_code=500
            if os.path.exists(tmp_file_path):
                os.remove(tmp_file_path)
            return {'message': 'something went wrong in getting for single template'}
        response_data = lambda_response['Payload']._raw_stream.data.decode("utf-8")
        present_templates_response = json.loads(response_data)

        # get_all_templates_for_this_template_type
        present_templates = await get_all_templates_for_a_bank(bank_name, template_type)
        # present_templates should be presentable as a list of template jsons

        final_templates = []
        for i in present_templates:
            template_json = i.get("template_json")
            template_json['uuid'] = i.get("template_uuid")
            final_templates.append(template_json)

        present_templates = deepcopy(final_templates)

        # add the new template in this list at the last position
        present_templates += [template]

        invocation_payload["template"] = present_templates

        # print("Invocation payload for all templates is : ", invocation_payload)

        # invoke `get_data_for_template_handler` lambda to get the data extracted from this template
        lambda_response_for_all_templates = lambda_client.invoke(
            FunctionName = TEMPLATE_HANDLER_LAMBDA_FUNCTION_NAME, 
            Payload=json.dumps(invocation_payload), 
            InvocationType='RequestResponse'
        )

        http_status = lambda_response_for_all_templates.get('ResponseMetadata', {}).get('HTTPStatusCode')
        headers = lambda_response_for_all_templates.get('ResponseMetadata', {}).get('HTTPHeaders', {})
        if http_status != 200 or headers.get('x-amz-function-error') is not None:
            response.status_code=500
            if os.path.exists(tmp_file_path):
                os.remove(tmp_file_path)
            return {'message': 'something went wrong in getting for all templates'}
        lambda_response_for_all_templates_response_data = lambda_response_for_all_templates['Payload']._raw_stream.data.decode("utf-8")
        all_template_response = json.loads(lambda_response_for_all_templates_response_data)

        to_append = {
            "data_from_present_template": present_templates_response, 
            "data_from_all_templates": all_template_response,
        }
        
        response_data_dict[f"page_{page_number}"] = to_append
        if os.path.exists(tmp_file_path):
            os.remove(tmp_file_path)
    return {
        "message": "success", 
        "health": response_data_dict
    }

@solutioning_router.post("/approve_template", tags=['template_solicitor'])
async def approve_template(request: ApproveTemplate, response: Response, user=Depends(get_current_user)):
    if user.user_type!="superuser":
        response.status_code=status.HTTP_401_UNAUTHORIZED
        return {"message": "not authorised"}
    
    template_uuid = request.template_uuid
    bank_name = request.bank_name
    approval = request.approval
    update_quality_dict = {"template_uuid": template_uuid}

    # check whether this template actually exists in mock templates
    requested_templates = await get_all_requested_templates(bank_name=bank_name)
    
    requested_templates = [_ for _ in requested_templates if _.get("template_uuid")==template_uuid]

    if len(requested_templates)>0:
        requested_templates = requested_templates[0]
    else:
        response.status_code = status.HTTP_404_NOT_FOUND
        return {"message": "template not found"}
    
    mock_active_status = requested_templates.get("active_status", 1)
    if requested_templates.get('template_type') == 'logo_hash' and approval:
        query = """    SELECT *
                FROM bank_connect_fsmlibtemplates where bank_name=:bank_name and template_type=:template_type
            """
        try:
            query_result = await portal_db.fetch_all(query=query, values= {"bank_name": bank_name, "template_type": "logo_hash"})
        except Exception as e:
            print(e)
            return {"message": "some error occured"}
        if len(query_result) > 0:
            existing_logo_hash = dict(query_result[0])
            template_uuid = existing_logo_hash.get("template_uuid")
            mock_active_status = 2
    if requested_templates.get('template_type') == 'ignore_logo_hash' and approval:
        hash_list = requested_templates.get('template_json',{}).get('hash_list',[])
        requested_templates['template_json'] = hash_list

    template_type = requested_templates.get('template_type')
    template_json = requested_templates.get('template_json')
    if template_type in ['logo_less_bbox','invalid_text_bbox']:
        template_json.pop('is_rotated',None)
    body = {
        "template_uuid" : template_uuid,
        "template_json" : template_json,
        "template_type" : template_type,
        "bank_name" : requested_templates.get('bank_name'),
        "priority" : requested_templates.get('priority'),
        "priority_to" : requested_templates.get('priority_to'),
        "approved_by" : user.username,
        "operation" : "create"
    }

    if template_type == 'account_category_bbox':
        added_mapping = template_json.get('added_mapping',{})
        template_json.pop('added_mapping',None)
        body['template_json'] = template_json
        if added_mapping not in [{},None]:
            query = """
                    select * from bank_connect_fsmlibtemplates where template_type = 'account_category_mapping' and bank_name = :bank_name
                    """
            query_data = await portal_db.fetch_one(query=query,values={'bank_name':bank_name})
            mapping_body = {
                "template_uuid":f"account_category_mapping_{str(uuid4())}",
                "template_json":added_mapping,
                "template_type":"account_category_mapping",
                "bank_name" : requested_templates.get('bank_name'),
                "priority" : requested_templates.get('priority'),
                "priority_to" : requested_templates.get('priority_to'),
                "approved_by" : user.username,
                "operation" : "update"
            }
            if query_data is None:
                mapping_body['operation'] = 'create'
            else:
                mapping_body['template_uuid'] = query_data.get('template_uuid')

            print(mapping_body)

            response = fb_dashboard_api_create_or_update(mapping_body)
            if response.status_code!=200 and approval==True:
                return {"message": "api to update template in fb dashboard failed"}   
            
    print(body)

    if mock_active_status == 1 and approval == True:
        # call finbox dashboard prod api and create this entry
        body["operation"]="create"
        response = fb_dashboard_api_create_or_update(body)
    elif mock_active_status == 2 and approval == True:
        # call finbox dashboard prod api and update this entry
        body["operation"]="update"
        response = fb_dashboard_api_create_or_update(body)
    elif mock_active_status == 4 and approval == True:
        # call finbox dashboard prod api and update this entry
        body["operation"]="update_priority"
        response = fb_dashboard_api_create_or_update(body)
    
    if approval==True:
        response_json = response.json()
        if response.status_code!=200 and (response_json.get('message', None) != 'template_uuid already exists, cannot add new'):
            return {"message": "api to update template in fb dashboard failed"}
    
    # update the status in mocktemplates
    query = """
                UPDATE mocktemplates set active_status=:status where template_uuid=:template_uuid
            """
    
    delete_vanilla_templates()

    update_quality_dict ["status"] = 3 if approval else 0

    try:
        await quality_database.execute(query = query, values = update_quality_dict)
    except Exception as e:
        print(e)
        response.status_code = status.HTTP_500_INTERNAL_SERVER_ERROR
        return {"message": "some error occured while updating status in quality db"}
    if approval:
        return {"message": "template approved", "template_uuid": template_uuid}
    
    return {"message": "template rejected", "template_uuid": template_uuid}

@solutioning_router.post("/request_shifting", tags=['template_solicitor'])
async def request_template_shifting(request: TemplateShifting, response: Response, user=Depends(get_current_user)):
    bank_name = request.bank_name
    template_type = request.template_type
    template_uuid = request.template_uuid
    priority_from = request.priority_from
    priority_to = request.priority_to

    templates = await get_all_templates_for_a_bank(bank_name, template_type)
    relevant_template = [_ for _ in templates if _.get("template_type")==template_type and _.get("template_uuid")==template_uuid and _.get("priority")==priority_from]
    if len(relevant_template)>0:
        relevant_template = relevant_template[0]
    else:
        response.status_code = status.HTTP_404_NOT_FOUND
        return {"message": "template not found"}
    
    if priority_to is None or priority_to < 0 or priority_to >= len(templates) or priority_from == priority_to:
        response.status_code = status.HTTP_400_BAD_REQUEST
        return {"message": "not a valid priority"}
    
    
    template_json = relevant_template.get("template_json")

    values = {
                "template_uuid": template_uuid,
                "template_type": template_type,
                "template_json": json.dumps(template_json),
                "bank_name": bank_name,
                "priority": priority_from,
                "priority_to": priority_to,
                "request_by": user.username,
                "active_status": 4
        }
    
    insertion_response = await insert_into_mock_templates(values)
    if insertion_response.get("message")=="some error occured":
        response.status_code = status.HTTP_500_INTERNAL_SERVER_ERROR
        return insertion_response
    
    return {"message": "template update request has been made", "template_uuid": template_uuid}

@solutioning_router.get("/fetch_templates", tags=['template_solicitor'])
async def fetch_templates(response: Response, bank_name: str, request_type: str, frequency:Optional[str] = None, user= Depends(get_current_user)):
    template_type, plus_inactive = None, True

    templates = await get_all_templates_for_a_bank(bank_name, template_type, plus_inactive)
    if isinstance(templates, dict) and templates.get("message") == "some error occured":
        response.status_code = status.HTTP_500_INTERNAL_SERVER_ERROR
        return templates
    
    if request_type=='active' and not frequency:
        templates = [{"template_uuid": _['template_uuid'], "template_json": _['template_json']} for _ in templates if _['is_active']]
    if request_type=='inactive':
        templates = [{"template_uuid": _['template_uuid'], "template_json": _['template_json']} for _ in templates if not _['is_active']]
    if request_type=='active' and frequency=='used':
        templates = get_unused_templates_for_a_bank(bank_name)
    if request_type=='active' and frequency=='unused':
        active_templates = [{"template_uuid": _['template_uuid'], "template_json": _['template_json']} for _ in templates if _['is_active']]
        used_templates = get_unused_templates_for_a_bank(bank_name)
        if isinstance(used_templates, dict) and used_templates.get("message") == "some error occured":
            response.status_code = status.HTTP_500_INTERNAL_SERVER_ERROR
            return used_templates
        used_template_uuids = [_['template_uuid'] for _ in used_templates]
        templates = [_ for _ in active_templates if _['template_uuid'] not in used_template_uuids]
    
    return {"templates": templates}

@solutioning_router.post("/request_activation", tags=['template_solicitor'])
async def request_template_activation(request: TemplateActivation, response: Response, user=Depends(get_current_user)):
    if user.user_type!="superuser":
        response.status_code=status.HTTP_401_UNAUTHORIZED
        return {"message": "not authorised"}
    bank_name = request.bank_name
    template_type = request.template_type
    template_uuid = request.template_uuid
    operation = request.operation
    plus_inactive = True
    
    if operation not in ['activate_template', 'deactivate_template']:
        response.status_code = status.HTTP_400_BAD_REQUEST
        return {"message": f"{operation} operation not found"}

    templates = await get_all_templates_for_a_bank(bank_name, template_type, plus_inactive)
    relevant_template = [_ for _ in templates if _.get("template_uuid")==template_uuid]
    if len(relevant_template)>0:
        relevant_template = relevant_template[0]
    else:
        response.status_code = status.HTTP_404_NOT_FOUND
        return {"message": "template not found"}
    
    
    template_type = relevant_template.get("template_type")
    template_json = relevant_template.get("template_json")
    template_is_active = relevant_template.get('is_active')
    if operation=='activate_template' and template_is_active:
        response.status_code = status.HTTP_400_BAD_REQUEST
        return {"message": "template is already active"}
    elif operation=='deactivate_template' and not template_is_active:
        response.status_code = status.HTTP_400_BAD_REQUEST
        return {"message": "template is already inactive"}
    values = {
        "template_uuid" : template_uuid,
        "template_json" : template_json,
        "template_type" : template_type,
        "bank_name" : bank_name,
        "approved_by" : user.username,
        "operation" : operation
    }

    response = fb_dashboard_api_create_or_update(values)
    if response.status_code!=200:
        return {"message": "api to update template in fb dashboard failed"}
    
    return {"message": f"Success, {operation} perfomed on {template_uuid}"}

@solutioning_router.post("/copy_transactions/")
def copy_transactions(request: CopyTransactions, response: Response):
    response_data = {}
    from_statement_id = request.from_statement_id
    to_statement_ids = request.to_statement_ids
    
    print(f"from_statement_id: {from_statement_id}, to_statement_ids: {to_statement_ids}")

    if not all([from_statement_id, to_statement_ids]):
        return {
            "error": True,
            "message": "from_statement_id and to_statement_ids required",
            "data": ""
        }
    try:
        
        for to_statement_id in to_statement_ids:
            copy_transactions_between_statements(from_statement_id, to_statement_id)

        response_data  = {
            "error": False,
            "message": "update successful",
            "data": ""
        }

    except Exception as e:

        response_data = {
            "error": True,
            "message" : str(e),
            "data": ""
        }

    return response_data
