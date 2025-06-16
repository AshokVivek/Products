from fastapi import APIRouter, Depends, Response, status
from app.database_utils import quality_database, portal_db
import json
from app.dependencies import get_current_user
from app.template_solutioning.request_models import UpdateKeywords
from app.conf import redis_cli
from app.constants import REDIS_KEY_INVALID_TEXT_BBOX_VANILLA

from app.database_utils import DBConnection
from app.constants import QUALITY_DATABASE_NAME, PORTAL_DATABASE_NAME

redis_router = APIRouter()

def delete_vanilla_templates():
    keys = redis_cli.keys()
    for key in keys:
        if key not in ['job_executed', 'categorisation_job','freshdesk'] and 'ingest' not in key and 'credit_card' not in key and 'null_regex' not in key:
            redis_cli.delete(key)

def invalidate_helper():
    keys = redis_cli.keys()
    for key in keys:
        if key not in ['job_executed', 'categorisation_job','freshdesk'] and 'ingest' not in key:
            redis_cli.delete(key)

@redis_router.post('/invalidate_cache')
async def invalidate_cache(user=Depends(get_current_user)):
    invalidate_helper()

    return {
        "success":True
    }

@redis_router.post('/update_keywords')
async def update_keywords(request: UpdateKeywords,response: Response,user=Depends(get_current_user)):
    keyword_type = request.keyword_type
    country = request.country
    keyword_list = request.keyword_list
    operation = request.operation

    quality_query = """
                        select * from general_data where country=:country and is_active=true and type=:keyword_type
                        """
    quality_data = await quality_database.fetch_one(query=quality_query, values={
        'country': country,
        'keyword_type': keyword_type
    })

    if not quality_data:
        response.status_code = status.HTTP_400_BAD_REQUEST
        return {"message":"Could not find data"}
    quality_data = dict(quality_data)
    current_list = json.loads(quality_data.get('data_list',[]))

    if operation=='add':
        for keyword in keyword_list:
            if keyword in current_list:
                continue
            current_list.append(keyword)
    elif operation=='remove':
        for keyword in keyword_list:
            if keyword not in current_list:
                continue
            current_list.remove(keyword)
    else:
        response.status_code = status.HTTP_400_BAD_REQUEST
        return {"message":"Not a valid operation"}

    update_query="""
            update general_data set data_list=:data_list where type=:type and country=:country and is_active=true
                """
    await quality_database.execute(query=update_query, values={
        'type':keyword_type,
        'country':country,
        'data_list':json.dumps(current_list)
    })

    redis_cli.delete(keyword_type)
    return {"message":"successfully done"}

async def get_or_put_keywords_quality(keyword_type):
    redis_response = redis_cli.get(keyword_type)
    if redis_response is not None:
        redis_response = json.loads(redis_response)
        return redis_response
    
    data_query = """
                select data_list from general_data where type=%(keyword_type)s
                """
    quality_data = DBConnection(QUALITY_DATABASE_NAME).execute_query(
        query=data_query,
        values={
            'keyword_type': keyword_type
        }
    )
    if quality_data is None:
        return None
    
    quality_data = dict(quality_data[0])
    keyword_json = json.dumps(quality_data.get('data_list'))
    # keyword_list = json.loads(keyword_json)
    redis_cli.set(keyword_type,keyword_json)
    return quality_data.get('data_list')

async def get_or_put_od_keywords():
    redis_response = redis_cli.get('OD_KEYWORDS')
    if redis_response is not None:
        redis_response = json.loads(redis_response)
        return redis_response
    
    od_keywords_fetch_query = """
                            select regex_list from bank_connect_fsmlibgeneraldata where tag='od_keywords' and country=%(country)s
                        """
    
    data_from_query = DBConnection(PORTAL_DATABASE_NAME).execute_query(query=od_keywords_fetch_query, values={'country':'IN'})
    if data_from_query is None:
        return None
    
    data_from_query = dict(data_from_query)
    data_from_query_json = data_from_query.get('regex_list')
    data_from_query = json.loads(data_from_query_json)
    redis_cli.set('OD_KEYWORDS',data_from_query_json)
    return data_from_query

async def get_credit_card_invalid_text_bbox_templates():
    redis_response = redis_cli.get('credit_card_invalid_text_bbox')
    if redis_response!=None:
        redis_response = json.loads(redis_response)
        return redis_response
    
    quality_query = """
                select * from cc_mocktemplates where template_type='invalid_text_bbox' and active_status=1
                """
    quality_data = DBConnection(QUALITY_DATABASE_NAME).execute_query(query=quality_query)
    if quality_data==None:
        return None

    templates = []
    for data in quality_data:
        data = dict(data)
        templates.append({'template_uuid':data.get("template_uuid"), 'template_json':data.get('template_json')})
    
    redis_cli.set('credit_card_invalid_text_bbox',json.dumps(templates), ex=900)
    return templates

async def get_credit_card_template_with_template_type(bank_name, template_type):
    redis_key = f'credit_card_template_{template_type}_{bank_name}'
    redis_response = redis_cli.get(redis_key)

    if redis_response!=None:
        redis_response = json.loads(redis_response)
        return redis_response
    
    query = """
            Select * from cc_mocktemplates where bank_name=%(bank_name)s and template_type=%(template_type)s and active_status=1
            """
    values = {
        "bank_name": bank_name,
        "template_type": template_type
    }
    quality_data = DBConnection(QUALITY_DATABASE_NAME).execute_query(query=query, values=values)

    if quality_data==None:
        return None
    
    templates = []
    for data in quality_data:
        data = dict(data)
        templates.append({'template_uuid':data.get("template_uuid"), 'template_json':data.get('template_json')})
    
    redis_cli.set(redis_key,json.dumps(templates), ex=900)
    return templates

async def get_od_templates_vanilla(bank_name):
    redis_key = f'vanilla_od_templates_{bank_name}'
    redis_response = redis_cli.get(redis_key)

    if redis_response!=None:
        redis_response = json.loads(redis_response)
        return redis_response
    
    query = """
            select * from mocktemplates where bank_name=%(bank_name)s and template_type in ('is_od_account_bbox','limit_bbox','od_limit_bbox') and active_status=1 order by created_at
            """
    values = {
        'bank_name': bank_name
    }
    quality_data = DBConnection(QUALITY_DATABASE_NAME).execute_query(
        query=query,
        values=values
    )

    if quality_data==None:
        return None
    
    templates = []
    for data in quality_data:
        data = dict(data)
        templates.append({'template_uuid':data.get("template_uuid"), 'template_type':data.get('template_type'), 'template_json':data.get('template_json')})
    
    redis_cli.set(redis_key,json.dumps(templates), ex=900)
    return templates

async def get_identity_patterns(pattern_type, country):
    redis_key = f'{pattern_type}_{country}'
    redis_response = redis_cli.get(redis_key)
    if redis_response is not None:
        return json.loads(redis_response)
    
    general_data_query = f"""
                        select * from general_data where type='{pattern_type}' and country='{country}' and is_active=true
                        """
    db_data = DBConnection(QUALITY_DATABASE_NAME).execute_query(query=general_data_query)
    if db_data is None:
        return None
    
    db_data = dict(db_data[0])
    data_list = db_data.get('data_list')
    redis_cli.set(redis_key, json.dumps(data_list), ex=86400)
    return data_list

async def get_identity_mismatch_mocktemplates(active_status, template_type, bank_name):
    redis_key = f'identity_mismatch_{template_type}_{bank_name}_{active_status}'
    redis_response = redis_cli.get(redis_key)
    if redis_response is not None:
        return json.loads(redis_response)
    
    identity_mismatch_mocktemple_query = f"""
                                select * from identity_mismatch_mocktemplates where template_type = '{template_type}' and bank_name='{bank_name}' and active_status='{active_status}' order by created_at desc
                                    """
    quality_db_response = DBConnection(QUALITY_DATABASE_NAME).execute_query(query=identity_mismatch_mocktemple_query)

    templates = []
    for template in quality_db_response:
        template = dict(template)
        template_json = template.get('template_json')
        template_uuid = template.get('template_uuid')
        templates.append({'template_json':template_json, 'template_uuid':template_uuid})
    
    redis_cli.set(redis_key,json.dumps(templates),ex=86400)
    return templates


async def get_vanilla_statement_invalid_text_bbox(bank_name):
    redis_key = REDIS_KEY_INVALID_TEXT_BBOX_VANILLA.format(bank_name)
    redis_response = redis_cli.get(redis_key)
    if redis_response is not None:
        return json.loads(redis_response)
    quality_query_for_fetching = f"select * from mocktemplates where template_type='invalid_text_bbox' and bank_name=%(bank_name)s and created_at > NOW() - INTERVAL '30 days' and active_status!=0 order by created_at desc"
    values = {
        "bank_name": bank_name
    }
    # quality_data = await quality_database.fetch_all(query = quality_query_for_fetching
    quality_data = DBConnection(QUALITY_DATABASE_NAME).execute_query(quality_query_for_fetching, values)

    templates = []
    for data in quality_data:
        data = dict(data)
        templates.append({
            'template_uuid':data.get('template_uuid'),
            'template_json':json.loads(data.get('template_json'))
        })
    
    redis_cli.set(redis_key, json.dumps(templates), ex=86400)
    return templates