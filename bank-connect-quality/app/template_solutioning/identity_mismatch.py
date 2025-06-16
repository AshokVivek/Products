from fastapi import APIRouter, Depends, Response, status

from app.constants import QUALITY_DATABASE_NAME
from app.template_solutioning.request_models import UpdatePatterns, MismatchTemplateAddition, ParkedData, MismatchSuperuserApproval, MismatchIgnore
from app.dependencies import get_current_user
from app.template_solutioning.redis import get_identity_patterns
import json
from uuid import uuid4
from app.database_utils import quality_database, DBConnection
from app.conf import redis_cli, PDF_BUCKET, lambda_client, TEMPLATE_HANDLER_LAMBDA_FUNCTION_NAME
from app.template_solutioning.redis import get_identity_mismatch_mocktemplates, delete_vanilla_templates
from app.template_dashboard.utils import create_viewable_presigned_url
from app.template_solutioning.dashboard_calls import fb_dashboard_api_create_or_update
from typing import Optional
import re
import math

identity_mismatch_router = APIRouter()

template_type_mapping = {
    "name_bbox":"name",
    "accnt_bbox":"account_number"
}

def invoke_template_handler_lambda(invocation_payload):
    lambda_response_for_this_template = lambda_client.invoke(
        FunctionName = TEMPLATE_HANDLER_LAMBDA_FUNCTION_NAME, 
        Payload = json.dumps(invocation_payload), 
        InvocationType='RequestResponse'
    )

    http_status = lambda_response_for_this_template.get('ResponseMetadata', {}).get('HTTPStatusCode')
    headers = lambda_response_for_this_template.get('ResponseMetadata', {}).get('HTTPHeaders', {})
    if http_status != 200 or headers.get('x-amz-function-error') is not None:
        return {'message': 'something went wrong in getting for single template'}
    response_data = lambda_response_for_this_template['Payload']._raw_stream.data.decode("utf-8")

    return json.loads(response_data)

@identity_mismatch_router.post('/update_patterns')
async def update_patterns(request: UpdatePatterns, response: Response, user = Depends(get_current_user)):
    if user.user_type != 'superuser':
        response.status_code=status.HTTP_400_BAD_REQUEST
        return {"message":"Route not allowed"}
     
    pattern_type = request.pattern_type
    identity_type = request.identity_type
    operation = request.operation
    country = request.country
    pattern = request.pattern

    if pattern_type not in ['name_pattern','account_pattern_not','account_pattern_is']:
        response.status_code=status.HTTP_400_BAD_REQUEST
        return {"message":"pattern_type not supported yet"}
    
    if pattern_type == 'name_pattern':
        if not isinstance(pattern, str):
            response.status_code = status.HTTP_400_BAD_REQUEST
            return {"message":"Only regexes are suppored in name_pattern"}
    
    if pattern_type in ['account_pattern_not','account_pattern_is']:
        if not isinstance(pattern, dict):
            response.status_code = status.HTTP_400_BAD_REQUEST
            return {"message":"disctionaries are supported in account_patterns"}
        for key in pattern.keys():
            if key not in ['bank_list','regex','length_check', 'length_gt', 'length_lt']:
                response.status_code = status.HTTP_400_BAD_REQUEST
                return {"message":"only bank_list, regex, length_check, length_gt and length_lt keys are supported by this template"}
            if key == 'length_check':
                if not isinstance(pattern.get('length_check'), int):
                    response.status_code = status.HTTP_400_BAD_REQUEST
                    return {"message":"length should be an integer"}
                if 'length_gt' in pattern.keys() and 'length_lt' in pattern.keys():
                    response.status_code = status.HTTP_400_BAD_REQUEST
                    return {"message":"length_gt and length_lt both cannot be present"}
                if 'length_gt' not in pattern.keys() and 'length_lt' not in pattern.keys():
                    response.status_code = status.HTTP_400_BAD_REQUEST
                    return {"message":"length_gt or length_lt should be present"}
            if key in ['length_gt','length_lt']:
                if not isinstance(pattern.get(key), bool):
                    response.status_code = status.HTTP_400_BAD_REQUEST
                    return {"message":"length_gt, length_lt should be of bool instance"}
            if key == "bank_list":
                if not isinstance(pattern.get('bank_list'),list):
                    response.status_code = status.HTTP_400_BAD_REQUEST
                    return {"message":"bank_list should be a list"}

    if identity_type not in ['name','account_number']:
        response.status_code=status.HTTP_400_BAD_REQUEST
        return {"message":"feature not enabled for this identity"}

    general_data_type = f'{pattern_type}_{country}'
    pattern_list = await get_identity_patterns(pattern_type, country)
    if operation == 'create':
        if pattern_list in [None, []]:
            pattern_list = [pattern]
            quality_query = f"""
                            insert into general_data (type, data_list, country, is_active)
                            values ('{pattern_type}', '{json.dumps(pattern_list)}', '{country}', true)
                            """
            await quality_database.execute(query=quality_query)
            redis_cli.delete(pattern_type)
            return {"message":"Successfully, Done"}
        else:
            response.status_code=status.HTTP_400_BAD_REQUEST
            return {"message":"Cannot create this, already present"}
    elif operation == 'update':
        if pattern_list is None:
            response.status_code=status.HTTP_400_BAD_REQUEST
            return {"message":"entry not present, could not update"}
        
        pattern_list.append(pattern)
        quality_query = f"""
                        update general_data set data_list = '{json.dumps(pattern_list)}' where type = '{pattern_type}' and country = '{country}' and is_active=true
                        """
        await quality_database.execute(query = quality_query)
        redis_cli.delete(general_data_type)
        return {"message":"Successfully, Done"}
    elif operation == 'remove':
        if pattern not in pattern_list:
            response.status_code=status.HTTP_400_BAD_REQUEST
            return {"message":"This is not present"}

        pattern_list.remove(pattern)
        quality_query = f"""
                        update general_data set data_list = '{json.dumps(pattern_list)}' where type = '{pattern_type}' and country = '{country}' and is_active=true
                        """
        await quality_database.execute(query = quality_query)
        redis_cli.delete(general_data_type)
        return {"message":"Successfully, Done"}
    else:
        response.status_code=status.HTTP_400_BAD_REQUEST
        return {"message":"Operation not defined yet"}
    
def check_pattern_match(account_pattern, account_number, bank_name):
    regex_check, length_check, bank_check = False, False, False
    if 'regex' in account_pattern.keys():
        regex_match = re.match(account_pattern.get('regex'), account_number)
        if regex_match is not None:
            regex_check = True
    else:
        regex_check=True

    if 'bank_list' in account_pattern.keys():
        bank_name_template = account_pattern.get('bank_list')
        bank_check = (bank_name_template is not None and bank_name in bank_name_template)
    else:
        bank_check = True

    if 'length_check' in account_pattern.keys():
        length_number = account_pattern.get('length_check')
        gt = account_pattern.get('length_gt')
        lt = account_pattern.get('length_lt')

        if gt and len(account_number)>length_number:
            length_check = True
        elif lt and len(account_number)<length_number:
            length_check = True
    else:
        length_check=True
    
    # print(account_pattern, regex_check, length_check, bank_check)
    if regex_check and length_check and bank_check:
        return True
        
    return False

async def trigger_identity_mismatch_on_ingestion(portal_data, country):
    name = portal_data.get('name')
    account_number = portal_data.get('account_number')
    name_pattern_key = "name_pattern"
    name_patterns = await get_identity_patterns(name_pattern_key, country)
    statement_id = portal_data.get('statement_id')
    bank_name = portal_data.get('bank_name')
    pdf_password = portal_data.get('pdf_password')

    value_map = {
        'statement_id':statement_id,
        'bank_name':bank_name,
        'pdf_password':pdf_password,
        'name':name,
        'name_mismatch_case':False,
        'name_mismatch_ignore_case':False,
        'name_mismatch_maker_parked_data':None,
        'name_mismatch_checker_status':False,
        'name_mismatch_maker_status':False,
        'name_matched_pattern':None
    }

    if name is not None:
        if name_patterns is not None:
            for pattern in name_patterns:
                regex_match = re.match(pattern, name)
                # print(pattern, regex_match, '-------------------------------------------------------')
                if regex_match is not None:
                    value_map['name_mismatch_case'] = True
                    value_map['name_matched_pattern'] = pattern
                    break

    if not value_map['name_mismatch_case']:
        metadata_analysis = portal_data.get('metadata_analysis', dict())
        if isinstance(metadata_analysis, str):
            metadata_analysis = json.loads(metadata_analysis)
        name_matches = metadata_analysis.get('name_matches', list())

        maximum_score = -10000
        for name_match in name_matches:
            score = name_match.get('score', 100)
            score = float(score)
            maximum_score = max(maximum_score, score)
        
        if maximum_score!=-10000 and maximum_score < 70:
            value_map['name_mismatch_case'] = True
            value_map['name_matched_pattern'] = json.dumps(name_matches)
    
    if value_map['name_mismatch_case']:
        templates = await get_identity_mismatch_mocktemplates(1, 'name_bbox', bank_name)
        for template in templates:
            template_uuid = template.get('template_uuid')
            invocation_payload = {
                "transaction_flag": False,
                "bucket": PDF_BUCKET,
                "key": f'pdf/{statement_id}_{bank_name}.pdf',
                "template": template.get('template_json', dict()),
                "template_type": 'name_bbox',
                "new_flow": True,
                "bank" : bank_name
            }

            data_from_template_handler = invoke_template_handler_lambda(invocation_payload)
            data_from_template_handler = data_from_template_handler[0]
            if not data_from_template_handler:
                continue

            value_map['name_mismatch_maker_parked_data'] = json.dumps({template_uuid:[data_from_template_handler]})
            value_map['name_mismatch_maker_status'] = True

    value_map['account_number'] = account_number
    value_map['account_number_mismatch_case'] = False
    value_map['account_number_mismatch_ignore_case'] = False
    value_map['account_number_mismatch_maker_parked_data'] = None
    value_map['account_number_mismatch_checker_status'] = False
    value_map['account_number_mismatch_maker_status'] = False

    is_account_invalid, hasChecked = False, False
    if account_number is not None:
        account_patterns = await get_identity_patterns('account_pattern_not', country)
        if account_patterns:
            # In this condition each on the pattern should not match
            for account_pattern in account_patterns:
                hasChecked = True
                is_pattern_match = check_pattern_match(account_pattern, account_number, bank_name)
                is_account_invalid = not is_pattern_match
                if is_pattern_match:
                    break
    
    if is_account_invalid:
        atleast_one_match = False
        account_patterns = await get_identity_patterns('account_pattern_is', country)
        if account_patterns:
            # In this if either one matches then its fine
            for account_pattrn in account_patterns:
                hasChecked = True
                is_pattern_match = check_pattern_match(account_pattrn, account_number, bank_name)
                if is_pattern_match:
                    atleast_one_match = True
                    break
        
        is_account_invalid = atleast_one_match

    # Marking this true to prevent account_number ingest, work needs to be done on the account_template 
    # is_account_number_valid = True
    if is_account_invalid and hasChecked:
        value_map['account_number_mismatch_case'] = True
        templates = await get_identity_mismatch_mocktemplates(1, 'accnt_bbox', bank_name)
        for template in templates:
            template_uuid = template.get('template_uuid')
            invocation_payload = {
                "transaction_flag": False,
                "bucket": PDF_BUCKET,
                "key": f'pdf/{statement_id}_{bank_name}.pdf',
                "template": template.get('template_json', dict()),
                "template_type": 'name_bbox',
                "new_flow": True,
                "bank" : bank_name
            }

            data_from_template_handler = invoke_template_handler_lambda(invocation_payload)
            data_from_template_handler = data_from_template_handler[0]
            if not data_from_template_handler:
                continue

            value_map['account_number_mismatch_maker_parked_data'] = json.dumps({template_uuid:[data_from_template_handler]})
            value_map['account_number_mismatch_maker_status'] = True

    insert_query = f"""
                insert into identity_mismatch_statement (
                    statement_id, bank_name, pdf_password, name_mismatch_case, name_mismatch_ignore_case, name_mismatch_maker_parked_data, name_mismatch_checker_status, name_mismatch_maker_status, name_matched_pattern, name,
                    account_number, account_number_mismatch_case, account_number_mismatch_ignore_case, account_number_mismatch_maker_parked_data, account_number_mismatch_checker_status, account_number_mismatch_maker_status
                )
                values (
                    %(statement_id)s, %(bank_name)s, %(pdf_password)s, %(name_mismatch_case)s, %(name_mismatch_ignore_case)s, %(name_mismatch_maker_parked_data)s, %(name_mismatch_checker_status)s, %(name_mismatch_maker_status)s, %(name_matched_pattern)s, %(name)s,
                    %(account_number)s, %(account_number_mismatch_case)s, %(account_number_mismatch_ignore_case)s, %(account_number_mismatch_maker_parked_data)s, %(account_number_mismatch_checker_status)s, %(account_number_mismatch_maker_status)s
                )
                    """
    
    DBConnection(QUALITY_DATABASE_NAME).execute_query(query=insert_query, values=value_map)
    return []

@identity_mismatch_router.post('/superuser_approval')
async def mismatch_superuser_approval(request: MismatchSuperuserApproval, response:Response, user = Depends(get_current_user)):
    template_uuid = request.template_id
    approval = request.approval

    if user.user_type != 'superuser':
        response.status_code = status.HTTP_400_BAD_REQUEST
        return {"message":"Route not allowed"}

    quality_query = f"""
                    select * from identity_mismatch_mocktemplates where template_uuid = '{template_uuid}'
                    """
    quality_template_data = await quality_database.fetch_one(query = quality_query)
    if quality_template_data is None:
        response.status_code = status.HTTP_404_NOT_FOUND
        return {"message":"tempalate not found"}
    
    quality_template_data = dict(quality_template_data)
    template_type = quality_template_data.get('template_type')
    bank_name = quality_template_data.get('bank_name')
    identity_type = template_type_mapping.get(template_type)
    template_json = quality_template_data.get('template_json')

    quality_query = f"""
                    select * from identity_mismatch_statement where {identity_type}_mismatch_case=true and {identity_type}_mismatch_ignore_case=false and 
                    {identity_type}_mismatch_maker_status=true and {identity_type}_mismatch_maker_parked_data is not null and {identity_type}_mismatch_checker_status=false and bank_name = '{bank_name}'
                    """
    quality_data = await quality_database.fetch_all(query = quality_query)
    for data in quality_data:
        data = dict(data)
        statement_id = data.get('statement_id')
        parked_data = json.loads(data.get(f'{identity_type}_mismatch_maker_parked_data'))
        if template_uuid in parked_data.keys():
            quality_query_to_update = f"""
                    update identity_mismatch_statement set {identity_type}_mismatch_checker_status=true where statement_id = '{statement_id}'
                    """
            if not approval:
                quality_query_to_update = f"""
                    update identity_mismatch_statement set {identity_type}_mismatch_checker_status=false, {identity_type}_mismatch_maker_status=false, {identity_type}_mismatch_maker_parked_data=null where statement_id = '{statement_id}'
                    """
            await quality_database.execute(query = quality_query_to_update)
    
    value_map = {}
    value_map['active_status'] = 3
    value_map['template_uuid'] = template_uuid

    quality_query_to_update_mocktemplate = f"update identity_mismatch_mocktemplates set active_status=:active_status where template_uuid=:template_uuid"
    if approval:
        mapping_body = {
            "template_uuid":template_uuid,
            "template_json":json.loads(template_json),
            "template_type":template_type,
            "bank_name" : bank_name,
            "priority" : None,
            "priority_to" : None,
            "approved_by" : user.username,
            "operation" : "create"
        }
        response = fb_dashboard_api_create_or_update(mapping_body)
        if response.status_code!=200:
            response.status_code = status.HTTP_400_BAD_REQUEST
            return {"message":"Could not approve template"}
    else:
        value_map['active_status'] = 0

    await quality_database.execute(query = quality_query_to_update_mocktemplate, values = value_map)
    delete_vanilla_templates()
    return {"message":"Successfully Done"}

@identity_mismatch_router.post('/request_addition')
async def request_addition(request: MismatchTemplateAddition, response: Response, user = Depends(get_current_user)):
    template_type = request.template_type
    template_json = request.template_json
    bank_name = request.bank_name
    statement_id = request.statement_id

    identity_type = template_type_mapping.get(template_type)
    template_uuid = f'{template_type}_{str(uuid4())}'

    quality_query = f"""insert into identity_mismatch_mocktemplates (template_uuid, active_status, bank_name, template_json, statement_id, template_type, request_by)
                        values (:template_uuid,:active_status,:bank_name,:template_json,:statement_id,:template_type,:request_by)
                    """
    await quality_database.execute(query = quality_query,values={
        'template_uuid':template_uuid,
        'active_status':1,
        'bank_name':bank_name,
        'template_json':json.dumps(template_json),
        'statement_id':statement_id,
        'template_type':template_type,
        'request_by':user.username
    })

    quality_query = f"""
                    select * from identity_mismatch_statement where {identity_type}_mismatch_case=true and 
                    {identity_type}_mismatch_ignore_case = false and {identity_type}_mismatch_maker_status = false and 
                    {identity_type}_mismatch_checker_status = false and bank_name = '{bank_name}'
                    """
    quality_data = await quality_database.fetch_all(query = quality_query)
    for data in quality_data:
        data = dict(data)
        current_statement_id = data.get('statement_id')
        invocation_payload = {
            "transaction_flag": False,
            "bucket": PDF_BUCKET,
            "key": f'pdf/{current_statement_id}_{bank_name}.pdf',
            "template": template_json,
            "template_type": template_type,
            "new_flow": True,
            "bank" : bank_name
        }

        data_from_template_handler = invoke_template_handler_lambda(invocation_payload)
        data_from_template_handler = data_from_template_handler[0]
        if not data_from_template_handler:
            continue

        update_query = f"update identity_mismatch_statement set {identity_type}_mismatch_maker_status=:maker_status, {identity_type}_mismatch_maker_parked_data=:parked_data where statement_id=:statement_id" 
        await quality_database.execute(query = update_query, values = {
            'maker_status':True,
            'parked_data':json.dumps({template_uuid:[data_from_template_handler]}),
            'statement_id':current_statement_id
        })
    
    delete_vanilla_templates()
    return {"message":"Successfully Done"}

@identity_mismatch_router.get('/get_parked_data')
async def get_parked_data(template_id: str, response: Response,page: Optional[int]=1, maxi: Optional[int]=10, user = Depends(get_current_user)):
    template_uuid = template_id
    
    fetch_bank_from_template = f"""
                        select * from identity_mismatch_mocktemplates where template_uuid = '{template_uuid}'
                            """
    template_data = await quality_database.fetch_one(query = fetch_bank_from_template)
    if template_data is None:
        response = status.HTTP_404_NOT_FOUND
        return {"message":"template not found"}
    
    template_data = dict(template_data)
    bank_name = template_data.get('bank_name')
    template_type = template_data.get('template_type')
    template_json = template_data.get('template_json')
    identity_type = template_type_mapping.get(template_type)

    quality_query = f"""
                    select * from identity_mismatch_statement where {identity_type}_mismatch_case=true and {identity_type}_mismatch_ignore_case=false and 
                    {identity_type}_mismatch_maker_status=true and {identity_type}_mismatch_maker_parked_data is not null and {identity_type}_mismatch_checker_status=false and bank_name = '{bank_name}'
                    """
    
    data_to_return = []
    quality_data = await quality_database.fetch_all(query=quality_query)
    for data in quality_data:
        data = dict(data)
        statement_id = data.get('statement_id')
        pdf_password = data.get('pdf_password')
        maker_parked_data = json.loads(data.get(f'{identity_type}_mismatch_maker_parked_data'))
        if template_uuid in maker_parked_data.keys():
            parked_data = {"all_text":[""],"data":maker_parked_data.get(template_uuid)}
            data_to_return.append({"template_json":template_json,"statement_id":statement_id, "bank_name":bank_name, 'parked_data':parked_data, 'pdf_password':pdf_password})
    
    offset_val = (page-1)*maxi
    return_list = []
    for i in range(offset_val,min(len(data_to_return),offset_val+maxi)):
        return_list.append(data_to_return[i])

    return {
        "template_type": template_type,
        "data": return_list,
        "total_cases": len(data_to_return)
    }

@identity_mismatch_router.post('/ignore_case')
async def ignore_cases(request:MismatchIgnore, response:Response, user = Depends(get_current_user)):
    statement_ids = request.statement_ids
    identity_type = request.identity_type

    if len(statement_ids)==0:
        return {"message": "ignored statements"} 

    statement_ids = str(statement_ids)
    statement_ids = statement_ids[1:]
    statement_ids = statement_ids[:-1]
    statement_ids = f"({statement_ids})"

    update_statement_quality_table_query = f"""
                                            update identity_mismatch_statement set {identity_type}_mismatch_ignore_case=True, ignore_by_user='{user.username}' where statement_id in {statement_ids}
                                        """
    update_statement_quality_table_query_data = await quality_database.execute(query=update_statement_quality_table_query)

    return {"message": "ignored statements"}

@identity_mismatch_router.get("/{mismatch_type:str}_cases")
async def get_mismatch_cases(mismatch_type:str, response: Response,current_page:Optional[int]=None, selected_bank:Optional[str]=None, max:Optional[int]=100,user= Depends(get_current_user)):
    if mismatch_type not in template_type_mapping.values():
        response.status_code = status.HTTP_400_BAD_REQUEST
        return {"message": "invalid null type"}

    quality_query = f"""
                    select bank_name, count(statement_id) as cases from identity_mismatch_statement where {mismatch_type}_mismatch_case=true and 
                    {mismatch_type}_mismatch_ignore_case = false and {mismatch_type}_mismatch_maker_status = false and 
                    {mismatch_type}_mismatch_checker_status = false
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

    query_for_all_data = f"""
                            SELECT statement_id, bank_name, pdf_password, {mismatch_type}
                            FROM identity_mismatch_statement
                            WHERE {mismatch_type}_mismatch_case = true and 
                            {mismatch_type}_mismatch_ignore_case = false and 
                            {mismatch_type}_mismatch_maker_status = false and 
                            {mismatch_type}_mismatch_checker_status = false
                            AND bank_name=:bank_name order by created_at desc offset {offset_val} limit {max}
                        """
    
    all_data = await quality_database.fetch_all(query=query_for_all_data,values={
        'bank_name':selected_bank
    })


    data_to_return = []
    for data in all_data:
        current_data = dict(data)
        current_data['presigned_url'] = create_viewable_presigned_url(current_data['statement_id'], current_data['bank_name'])
        data_to_return.append(current_data)

    response.status_code = status.HTTP_200_OK
    return {"bank_list": bank_cases_list, "all_data": data_to_return, "total_cases":total_cases}

async def get_all_requested_templates(bank_name: Optional[str] = None, template_type: Optional[str] = None):
    
    if bank_name in [None, '']:
        query = """    
                SELECT template_uuid, template_type, template_json, bank_name, active_status, created_at, request_by, statement_id
                FROM identity_mismatch_mocktemplates where active_status in (1,2,4)
            """
        try:
            query_result = await quality_database.fetch_all(query=query)
        except Exception as e:
            print(e)
            return {"message": "some error occured"}
    else:
        query = """    
                SELECT template_uuid, template_type, template_json, bank_name, active_status, created_at, request_by, statement_id
                FROM identity_mismatch_mocktemplates where bank_name=:bank_name and active_status in (1,2,4)
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

@identity_mismatch_router.get('/requested_templates')
async def requested_templates(response: Response, bank_name: Optional[str] = None, template_type:str = None, user = Depends(get_current_user)):
    templates = await get_all_requested_templates(bank_name, template_type)
    if isinstance(templates, dict) and templates.get("message")=="some error occured":
        response.status_code = status.HTTP_500_INTERNAL_SERVER_ERROR
        return templates
    
    return {"templates": templates}