from fastapi import APIRouter, Depends, Response, status
from app.dependencies import get_current_user
from app.database_utils import quality_database, DBConnection
from typing import Optional, Union
from app.template_dashboard.utils import create_viewable_presigned_url
from app.template_solutioning.redis import redis_cli
import json
import re
from app.pdf_utils import get_text_in_box
from pydantic import BaseModel
from app.conf import s3, PDF_BUCKET
from app.pdf_utils import read_pdf
from uuid import uuid4
from app.constants import IDENTITY_NULL_TEMPLATE_REDIS_KEY, QUALITY_DATABASE_NAME
import os
from datetime import datetime
import fitz
class GetExtractedData(BaseModel):
    statement_id: str
    template_json: dict
    bank_name: str
    template_type: str

class RequestAddition(BaseModel):
    template_json: dict
    bank_name: str
    template_type: str
    country: Optional[str] = 'IN'

class ApproveTemplate(BaseModel):
    template_id: str
    approval:bool

class GetParkedData(BaseModel):
    template_uuid: str

null_identity_router = APIRouter()

supported_null_types = {
    'name_null':'name_null_regex',
    'account_null':'account_null_regex',
    'date_null':'date_null_regex',
    'ac_category_null':'ac_category_null_regex',
    'ifsc_null':'ifsc_null_regex',
    'micr_null':'micr_null_regex',
    'address_null':'address_null_regex'
}

identity_to_null_type_map = {
    'name':'name_null',
    'account_number':'account_null',
    'date':'date_null',
    'account_category': 'ac_category_null',
    'ifsc': 'ifsc_null',
    'micr': 'micr_null',
    'address': 'address_null'
}

reverse_supported_null_types = {v:k for k,v in supported_null_types.items()}

@null_identity_router.get("/{null_type:str}_cases")
async def get_cases(null_type:str, response: Response,current_page:Optional[int]=None, selected_bank:Optional[str]=None, max:Optional[int]=100,user= Depends(get_current_user)):
    if null_type not in supported_null_types.keys():
        response.status_code = status.HTTP_404_NOT_FOUND
        return {'message':'invalid null type'}
    
    # statement_id, bank_name, pdf_password, {null_type}_ignore_regex, {null_type}_ignore_statement
    quality_query = f"""
                        select bank_name, count(statement_id) as cases
                        from null_identity
                        where {null_type}_ignore_regex_id is not null and {null_type}_ignore_case = false and {null_type}_updated_at > NOW() - INTERVAL '30 days'
                        group by bank_name order by cases desc
                    """
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
    
    offset_val = (current_page-1)*max
    quality_query_to_fetch_cases = f"""
                                    select bank_name, statement_id, pdf_password, {null_type}_ignore_regex_id 
                                    from null_identity
                                    where {null_type}_ignore_regex_id is not null and {null_type}_ignore_case = false and {null_type}_updated_at > NOW() - INTERVAL '30 days'
                                    order by {null_type}_updated_at desc offset {offset_val} limit {max}
                                    """
    all_data = await quality_database.fetch_all(query=quality_query_to_fetch_cases)
    data_to_return = []
    for data in all_data:
        current_data = dict(data)
        current_data['presigned_url'] = create_viewable_presigned_url(current_data['statement_id'], current_data['bank_name'])
        data_to_return.append(current_data)
    
    response.status_code = status.HTTP_200_OK
    return {"bank_list": bank_cases_list, "all_data": data_to_return, "total_cases":total_cases}

async def get_identity_regexes_from_redis(bank_name, regex_type, country):
    redis_key = IDENTITY_NULL_TEMPLATE_REDIS_KEY.format(regex_type, bank_name, country)
    redis_response = redis_cli.get(redis_key)
    if redis_response is not None:
        return json.loads(redis_response)
    
    quality_query = f"""
                    select * from general_data where bank_name=%(bank_name)s and type=%(type)s and country=%(country)s and is_active=%(is_active)s and active_status in (1,3)
                    """
    
    quality_data = DBConnection(QUALITY_DATABASE_NAME).execute_query(
        query=quality_query,
        values={
            'bank_name':bank_name,
            'type':regex_type,
            'country':country,
            'is_active':True
        }
    )

    data_to_return = []
    for data in quality_data:
        data = dict(data)
        data_to_return.append({'template_uuid':data.get('template_uuid'),'template_json':data.get('data_list')})
    
    redis_cli.set(redis_key, json.dumps(data_to_return), ex=86400)
    return data_to_return

def get_text_and_find_match(template_json, template_uuid, doc, get_only_all_text = False):
    bbox = template_json.get('bbox')
    regex = template_json.get('regex')
    pages_to_see = [*range(min(doc.page_count, 3))]

    for page in pages_to_see:
        all_text = get_text_in_box(doc[page], bbox)
        all_text = all_text.replace('\n', ' ').replace('(cid:9)', '')
        if get_only_all_text:
            return all_text, False
        
        regex_match = re.match(regex, all_text)
        if regex_match is not None:
            return template_uuid, True
    
    return None, False

def get_doc_from_statement_id(statement_id, bank_name) -> Union[fitz.Document, int, None]:
    key = f"pdf/{statement_id}_{bank_name}.pdf"
    try:
        pdf_bucket_response = s3.get_object(Bucket=PDF_BUCKET, Key=key)
    except Exception as e:
        return None
    
    response_metadata = pdf_bucket_response.get('Metadata')
    password = response_metadata.get('pdf_password')
    temp_file_path = f'/tmp/temp_{statement_id}'
    with open(temp_file_path,'wb') as theFile:
        theFile.write(pdf_bucket_response['Body'].read())

    doc = read_pdf(temp_file_path,password)
    if os.path.exists(temp_file_path):
        os.remove(temp_file_path)
    return doc

# @null_identity_router.
async def simulate_ingest_identity_null(statement_id, portal_data, doc):
    bank_name = portal_data.get('bank_name')
    name = portal_data.get('name')
    account_number = portal_data.get('account_number')
    from_date = portal_data.get("from_date")
    to_date = portal_data.get("to_date")
    predicted_bank = portal_data.get("predicted_bank")
    detected_category = portal_data.get('account_category')
    ifsc = portal_data.get('ifsc')
    micr = portal_data.get('micr')
    address = portal_data.get('address')

    is_predicted_bank = predicted_bank is not None and predicted_bank.lower()
    pdf_password = portal_data.get('pdf_password')

    null_type_map = {
        'name': name in [None, ''] and is_predicted_bank,
        'account_number': account_number in [None, ''] and is_predicted_bank,
        'date': from_date is None or to_date is None and is_predicted_bank,
        'account_category': detected_category in [None, ''] and portal_data.get('has_ac_category_keyword'),
        'ifsc': ifsc in [None, ''] and portal_data.get('has_ifsc_keyword'),
        'micr': micr in [None, ''] and portal_data.get('has_micr_keyword'),
        'address': address in [None, '']
    }

    value_map = {
        'statement_id':statement_id,
        'bank_name':bank_name,
        'pdf_password':pdf_password
    }

    for identity, is_null in null_type_map.items():
        null_type = identity_to_null_type_map.get(identity)
        value_map.update({
            f'{null_type}_ignore_case': False,
            f'{null_type}_ignore_regex_id': None,
            f'{null_type}_updated_at': datetime.now()
        })

        if is_null:
            null_regex = supported_null_types.get(null_type)
            null_regexes = await get_identity_regexes_from_redis(bank_name, null_regex, 'IN')
            for null_regexe in null_regexes:
                regex_json = null_regexe.get('template_json')
                template_uuid = null_regexe.get('template_uuid')
                matched_template_uuid, is_match = get_text_and_find_match(regex_json, template_uuid, doc)
                value_map[f'{null_type}_ignore_regex_id'] = matched_template_uuid
                if is_match:
                    break

    quality_query = f"""
            insert into null_identity (bank_name, statement_id, pdf_password, 
                name_null_ignore_case, name_null_ignore_regex_id, name_null_updated_at,
                account_null_ignore_case, account_null_ignore_regex_id, account_null_updated_at,
                date_null_ignore_case, date_null_ignore_regex_id, date_null_updated_at,
                ac_category_null_ignore_case, ac_category_null_ignore_regex_id, ac_category_null_updated_at,
                ifsc_null_ignore_case, ifsc_null_ignore_regex_id, ifsc_null_updated_at,
                micr_null_ignore_case, micr_null_ignore_regex_id, micr_null_updated_at,
                address_null_ignore_case, address_null_ignore_regex_id, address_null_updated_at
            )
            values (%(bank_name)s, %(statement_id)s, %(pdf_password)s, 
                %(name_null_ignore_case)s, %(name_null_ignore_regex_id)s, %(name_null_updated_at)s,
                %(account_null_ignore_case)s, %(account_null_ignore_regex_id)s, %(account_null_updated_at)s,
                %(date_null_ignore_case)s, %(date_null_ignore_regex_id)s, %(date_null_updated_at)s,
                %(ac_category_null_ignore_case)s, %(ac_category_null_ignore_regex_id)s, %(ac_category_null_updated_at)s,
                %(ifsc_null_ignore_case)s, %(ifsc_null_ignore_regex_id)s, %(ifsc_null_updated_at)s,
                %(micr_null_ignore_case)s, %(micr_null_ignore_regex_id)s, %(micr_null_updated_at)s,
                %(address_null_ignore_case)s, %(address_null_ignore_regex_id)s, %(address_null_updated_at)s
            )
            """

    DBConnection(QUALITY_DATABASE_NAME).execute_query(query=quality_query, values=value_map)
    return value_map

@null_identity_router.post('/get_extracted_data')
async def get_extracted_data(request:GetExtractedData,response:Response, user=Depends(get_current_user)):
    template_type = request.template_type
    statement_id = request.statement_id
    bank_name = request.bank_name
    template_json = request.template_json

    if template_type not in reverse_supported_null_types.keys():
        response.status_code=status.HTTP_400_BAD_REQUEST
        return {"message":"template type not supported"}

    doc = get_doc_from_statement_id(statement_id, bank_name)
    if doc is None:
        return {"all_text":"", 'is_match':False, 'message':"PDF_NOT_FOUND"}
    _, is_match = get_text_and_find_match(template_json, '', doc)
    all_text, _ = get_text_and_find_match(template_json, '', doc, get_only_all_text=True)
    doc.close()
    return {'data':[{"data":[is_match],"all_text":[all_text]}]}

@null_identity_router.post('/request_addition')
async def request_addition(request: RequestAddition, response:Response, user=Depends(get_current_user)):
    template_type = request.template_type
    bank_name = request.bank_name
    template_json = request.template_json
    country = request.country

    if template_type not in reverse_supported_null_types.keys():
        response.status_code=status.HTTP_400_BAD_REQUEST
        return {"message":"template type not supported"}

    quality_query = f"""
                insert into general_data (bank_name, template_uuid, data_list, active_status, type, country)
                values (:bank_name, :template_uuid, :data_list, :active_status, :type, :country)
                """

    template_uuid = f'{template_type}_{str(uuid4())}'
    await quality_database.execute(query = quality_query, values = {
        'bank_name':bank_name,
        'template_uuid':template_uuid,
        'data_list':json.dumps(template_json),
        'active_status':1,
        'type':template_type,
        'country':country
    })

    null_type = reverse_supported_null_types.get(template_type)

    quality_query_for_not_done_statements = f"""
                                        SELECT statement_id, bank_name, pdf_password
                                        FROM statement_quality 
                                        WHERE {null_type}=TRUE
                                        AND {null_type}_maker_status <> TRUE
                                        AND {null_type}_checker_status <> TRUE
                                        AND {null_type}_ignore_case <> TRUE
                                        AND bank_name='{bank_name}' order by created_at desc
                                                    """
    quality_response = await quality_database.fetch_all(query=quality_query_for_not_done_statements)
    statements_to_update = []
    for quality_data in quality_response:
        quality_data = dict(quality_data)
        statement_id = quality_data.get('statement_id')
        doc = get_doc_from_statement_id(statement_id, bank_name)
        if doc is None:
            continue
        _, is_match = get_text_and_find_match(template_json, template_uuid, doc)
        doc.close()
        if is_match:
            statements_to_update.append(statement_id)

    if len(statements_to_update)>0:
        statement_string = ''
        for statement in statements_to_update:
            statement_string += f",'{statement}'"
        
        statement_string = statement_string[1:]
        quality_update_query = f"update null_identity set {null_type}_ignore_regex_id='{template_uuid}', {null_type}_updated_at=now() where statement_id in ({statement_string})"
        await quality_database.execute(query = quality_update_query)

        statement_quality_ignore_query = f"update statement_quality set {null_type}_ignore_case = TRUE where statement_id in ({statement_string})"
        await quality_database.execute(query = statement_quality_ignore_query)

    redis_key = IDENTITY_NULL_TEMPLATE_REDIS_KEY.format(template_type,bank_name,country)
    redis_cli.delete(redis_key)
    return {'message':'successfully done'}
    
@null_identity_router.post('/superuser_approval')
async def approve_template(request: ApproveTemplate, response:Response, user=Depends(get_current_user)):
    template_uuid = request.template_id
    approval = request.approval

    if user.user_type!='superuser':
        response.status_code = status.HTTP_400_BAD_REQUEST
        return {"message":"only superuser allowed"}

    quality_query = f"""
                    select * from general_data where template_uuid=:template_uuid
                    """
    quality_respose = await quality_database.fetch_one(query = quality_query, values = {'template_uuid':template_uuid})
    if quality_respose is None:
        response.status_code=status.HTTP_400_BAD_REQUEST
        return {"message":"template not found"}
    quality_respose = dict(quality_respose)
    template_type = quality_respose.get('type')
    bank_name = quality_respose.get('bank_name')
    country = quality_respose.get('country')
    
    quality_query = f"""
                update general_data set active_status=:active_status where template_uuid=:template_uuid
                """
    quality_query = await quality_database.execute(query = quality_query, values = {
        'template_uuid':template_uuid,
        'active_status': 3 if approval else 0
    })

    null_type = reverse_supported_null_types.get(template_type)
    if not approval:
        statements_that_were_sim_query = f"select * from null_identity where {null_type}_ignore_regex_id='{template_uuid}'"
        simulated_statement_data = await quality_database.fetch_all(query = statements_that_were_sim_query)
        statement_ids = []
        for sim_statement in simulated_statement_data:
            sim_statement = dict(sim_statement)
            statement_id = sim_statement.get('statement_id')
            statement_ids.append(statement_id)
        
        statement_string = ''
        if len(statement_ids)>0:
            for cur_statement_id in statement_ids:
                statement_string += f",'{cur_statement_id}'"
            
            statement_string = statement_string[1:]
            quality_query_to_update = f"update statement_quality set {null_type}_ignore_case = FALSE where statement_id in ({statement_string})"
            await quality_database.execute(query = quality_query_to_update)

            quality_query_to_update = f"update null_identity set {null_type}_ignore_regex_id=null, {null_type}_updated_at=now() where statement_id in ({statement_string})"
            await quality_database.execute(query=quality_query_to_update)

    redis_key = IDENTITY_NULL_TEMPLATE_REDIS_KEY.format(template_type,bank_name,country)
    redis_cli.delete(redis_key)

    return {"message":"approved regex, please add monitoring setup for this"}

@null_identity_router.get('/get_parked_data')
async def get_parked_data(template_id: str, response:Response,page: Optional[int]=1, maxi: Optional[int]=10, user=Depends(get_current_user)):
    template_uuid = template_id

    quality_query_to_fetch_template = f"select * from general_data where template_uuid='{template_uuid}'"
    quality_data = await quality_database.fetch_one(query=quality_query_to_fetch_template)
    if quality_data is None:
        response.status_code = status.HTTP_404_NOT_FOUND
        return {"message":"template not found"}
    
    quality_data = dict(quality_data)
    template_type = quality_data.get('type')
    null_type = reverse_supported_null_types.get(template_type)

    offset_val = (page-1)*maxi
    quality_query_to_fetch = f"select count(*) OVER() AS count, statement_id, bank_name, pdf_password, {null_type}_ignore_regex_id from null_identity where {null_type}_ignore_regex_id='{template_uuid}' and {null_type}_ignore_case=false order by created_at desc limit {maxi} offset {offset_val}"
    statements = await quality_database.fetch_all(query=quality_query_to_fetch)

    total_count = 0
    data_to_return = []
    for statement in statements:
        statement = dict(statement)
        if total_count==0:
            total_count = statement.get('count')
        
        statement.pop('count', None)
        statement['presigned_url'] = create_viewable_presigned_url(statement['statement_id'], statement['bank_name'])
        data_to_return.append(statement)
    
    response.status_code = status.HTTP_200_OK
    return {
        "template_type": template_type,
        "data": data_to_return,
        "total_cases": total_count
    }

# async def get_all_templates_for_a_bank(bank_name: str, template_type: Optional[str] = None, active_status: Optional[int] = 3) -> dict:
#     query = """    
#             select * from general_data where bank_name=:bank_name and type=:template_type and active_status=:active_status
#             """
#     quality_data = await quality_database.fetch_all(query=query, values={
#         'active_status':active_status,
#         'bank_name':bank_name,
#         'template_type':template_type
#     })

#     templates = []
#     for template in quality_data:
#         template = dict(template)
#         temp = {
#             'template_type':template.get('type'),
#             'template_json':template.get('data_list'),
#             'template_uuid':template.get('template_uuid'),
#             'bank_name':template.get('bank_name')
#         }
#         templates.append(temp)
    
#     return templates

async def get_all_requested_templates(bank_name: Optional[str] = None, template_type: Optional[str] = None, is_active = False):

    active_status = 3 if is_active else 1
    
    if bank_name in [None, '']:
        query = f"""    
                SELECT template_uuid, type, data_list, bank_name, active_status
                FROM general_data where active_status in ({active_status})
            """
        try:
            query_result = await quality_database.fetch_all(query=query)
        except Exception as e:
            print(e)
            return {"message": "some error occured"}
    else:
        query = f"""    
                SELECT template_uuid, type, data_list, bank_name, active_status
                FROM mocktemplates where bank_name=:bank_name and active_status in ({active_status})
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
        if template_type not in [None, ''] and item.get("type")!=template_type:
            continue
        template_type_cur = item.pop('type', None)
        template_json = item.pop('data_list', None)
        item['template_type'] = template_type_cur
        item['template_json'] = template_json
        result.append(item)
        result[-1]["template_json"]=json.loads(result[-1]["template_json"])
    
    return result

@null_identity_router.get("/requested_templates", tags=['template_solicitor'])
async def get_requested_templates(response: Response, bank_name: Optional[str] = None, template_type:str = None, active_status:bool=False, user= Depends(get_current_user)):
    templates = await get_all_requested_templates(bank_name, template_type, active_status)
    if isinstance(templates, dict) and templates.get("message")=="some error occured":
        response.status_code = status.HTTP_500_INTERNAL_SERVER_ERROR
        return templates
    
    return {"templates": templates}