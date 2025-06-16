from fastapi import APIRouter, Depends, Response, status
from app.dependencies import get_current_user
from app.database_utils import portal_db, quality_database, DBConnection
from app.template_solutioning.request_models import PerformSimulation, SuperUserApproval, IgnoreType, GetExtractedData, IgnoreAccount, InconsistencyCheck
import json
from app.template_dashboard.utils import create_viewable_presigned_url, viewable_presigned_url, check_text_set
from app.conf import *
from app.template_solutioning.logo_hash import get_images
from app.template_solutioning.account_quality import account_ingestion, get_inconsistency_cases, get_inconsistent_details
from app.pdf_utils import read_pdf
from app.template_solutioning.router import is_logo_less_bbox_template_valid
from typing import Optional
from app.template_solutioning.redis import get_or_put_od_keywords, get_or_put_keywords_quality
import copy
from datetime import datetime
import traceback
from app.template_solutioning.redis import get_od_templates_vanilla, get_vanilla_statement_invalid_text_bbox
from app.template_solutioning.identity_mismatch import trigger_identity_mismatch_on_ingestion
from app.template_solutioning.null_identity_quality import simulate_ingest_identity_null
from app.template_dashboard.utils import check_text_set
from app.constants import QUALITY_DATABASE_NAME, PORTAL_DATABASE_NAME
from app.ddb_utils import get_transactions_for_statement
from fsm_lambdas.library.fraud import transaction_balance_check, optimise_transaction_type
import fitz

quality_router = APIRouter()

IGNORE_HASHES = [2961526273525307393, 16940700616394954761, 1073402114634496, 11073402114634496, 7717328579540178953, 11073402114634496 ,506593695379491079 ,12130784746107491347 ,12238871137164383315 ,434361141553883946,
                608345227072000, 2276366221312, 45826056154382338, 57175410999296, 4556548824109100, 19247132005532160, 35346674876416, 72275907838607370]
STATEMENT_TYPE = 'vanilla'

invalid_map = {
    "name_bbox": "name_null",
    "accnt_bbox": "account_null",
    "date_bbox": "date_null",
    "logo_hash": "logo_null",
    "ignore_logo_hash":"ignore_logo_hash",
    "logo_less_bbox":"logo_less_bbox",
    "account_category_bbox":"ac_category_null",
    "ifsc_bbox":"ifsc_null",
    "micr_bbox":"micr_null",
    "limit_bbox":"limit_bbox",
    "od_limit_bbox":"od_limit_bbox",
    "is_od_account_bbox":"is_od_account_bbox",
    "address_bbox":"address_null",
    "invalid_text_bbox":"invalid_text_bbox"
}

reverse_invalid_map = {v:k for k,v in invalid_map.items()}

async def get_od_account_keywords():
    od_keywords_fetch_query = """
                            select regex_list from bank_connect_fsmlibgeneraldata where tag='od_keywords' and country=%(country)s
                        """
    
    # TODO handle for other countries
    data_from_query = DBConnection(PORTAL_DATABASE_NAME).execute_query(
        query=od_keywords_fetch_query,
        values={
            'country':'IN'
        }
    )
    data_from_query = dict(data_from_query[0])
    json_data = data_from_query.get('regex_list')
    return json_data

async def get_ignore_logo_hash():
    query = """
            select * from bank_connect_fsmlibgeneraldata where tag=%(tag)s and country=%(country)s
            """
    # TODO handle for other countries
    data_from_query = DBConnection(PORTAL_DATABASE_NAME).execute_query(
        query,
        values={'tag': 'ignore_logo_hash',
                'country': 'IN'
                }
    )
    data_from_query = dict(data_from_query[0])
    json_data = data_from_query.get('regex_list')
    return json_data

async def simulate_ignore_logo_hash_on_template(template_json,template_id,items,statement_id,approval=False):
    if len(template_json)==0:
        return False
    ignored_logo = template_json[0]
    if items.get("logo_null_maker_parked_data") is None:
        return
    
    present_parked_data = json.loads(items.get("logo_null_maker_parked_data"))
    
    if not approval:
        if ignored_logo not in present_parked_data['hash_list']:
            return False
        
        present_parked_data['selected_hash']=ignored_logo
        parked_data = {template_id:[present_parked_data]}
        query = """
                update statement_quality set logo_null_maker_parked_data = %(parked_data)s,
                logo_null_maker_status = TRUE
                where statement_id = %(statement_id)s and logo_null=TRUE
            """
        data_from_query = DBConnection(QUALITY_DATABASE_NAME).execute_query(
            query,
            values={
                'parked_data': json.dumps(parked_data),
                'statement_id': statement_id}
        )
    else:
        parked_data = present_parked_data.get(template_id,None)
        if not parked_data:
            return False
        
        query = """
                update statement_quality set pdf_ignore_reason = %(pdf_ignore_reason)s, logo_null_ignore_case = TRUE
                where statement_id = %(statement_id)s and logo_null=TRUE
            """
        data_from_query = DBConnection(QUALITY_DATABASE_NAME).execute_query(
            query,
            values={'statement_id': statement_id,
                    'pdf_ignore_reason': 'ignore logo detected'}
        )
    return True

def check_all_text_logo_less_bbox(template_json,pdf_bucket_response,key):
    template_json_copy = copy.deepcopy(template_json)
    template_json_copy.pop('is_rotated',None)
    
    response_metadata = pdf_bucket_response.get('Metadata')
    password = response_metadata.get('pdf_password')
    path = f'/tmp/temp_{key}'
    with open(path,'wb') as theFile:
        theFile.write(pdf_bucket_response['Body'].read())

    doc = read_pdf(path,password)
    logoless_check = False

    for i in range(min(2,doc.page_count)):
        page = doc.load_page(i)
        logoless_check = check_text_set(template_json_copy,page)
        if logoless_check:
            break
    doc.close()
    if os.path.exists(path):
        os.remove(path)
    return logoless_check

async def simulate_logo_less_bbox(statement_id,items,template_json,pdf_bucket_response,key,template_id,approval=False):
    if approval:
        present_parked_data = json.loads(items.get("logo_null_maker_parked_data",{}))
        if present_parked_data.get(template_id,None):
            return True
    else:
        is_all_text_present = check_all_text_logo_less_bbox(template_json,pdf_bucket_response,key)
        if is_all_text_present:
            if items.get("logo_null_maker_parked_data",{})==None:
                present_parked_data={}
            else:
                present_parked_data = json.loads(items.get("logo_null_maker_parked_data",{}))
            present_parked_data['selected_hash'] = 'LOGO_LESS'
            parked_data = {template_id:[present_parked_data]}
            query = """
                update statement_quality set logo_null_maker_parked_data = %(parked_data)s,
                logo_null_maker_status = TRUE
                where statement_id = %(statement_id)s and logo_null=TRUE
            """
            data_from_query = DBConnection(QUALITY_DATABASE_NAME).execute_query(
                query,
                values={'parked_data': json.dumps(parked_data),
                        'statement_id': statement_id}
            )
            return True
    return False

async def simulate_ac_category_ingestion(statement_id,bank_name,mapping):
    quality_query = """
                Select * from mocktemplates where bank_name=%(bank_name)s and template_type='account_category_bbox' and active_status=1
                    """
    values = {
        "bank_name": bank_name
    }
    quality_data = DBConnection(QUALITY_DATABASE_NAME).execute_query(
        query=quality_query,
        values=values
    )

    template_id = None
    data_from_template_handler = None
    for i in range(len(quality_data)):
        quality_data[i] = dict(quality_data[i])
        template_id = quality_data[i].get('template_uuid')
        data_from_template_handler = invoke_template_handler_lambda({
            "bucket": PDF_BUCKET,
            "key": f"pdf/{statement_id}_{bank_name}.pdf",
            "template": quality_data[i].get("template_json"),
            "template_type":"account_category_bbox",
            "new_flow":True,
            "mapping":json.loads(mapping),
            "bank":bank_name
        })

        if len(data_from_template_handler)>0:
            data_from_template_handler = data_from_template_handler[0]
            # print(data_from_template_handler)
            if not data_from_template_handler:
                continue
            if len(data_from_template_handler['data'])==0:
                continue
            if not data_from_template_handler['data'][0]:
                continue

        return data_from_template_handler, template_id
    return data_from_template_handler, template_id

async def get_mapping_acc_category(bank_name):
    get_mapping_query = """
                        Select template_json from bank_connect_fsmlibtemplates where template_type=%(template_type)s and bank_name=%(bank_name)s
                        """
    values = {
        'bank_name': bank_name,
        'template_type': 'account_category_mapping'
    }
    mapping_query_data = DBConnection(PORTAL_DATABASE_NAME).execute_query(
        query=get_mapping_query,
        values=values
        )

    mapping_query_data = dict(mapping_query_data[0]) if len(mapping_query_data)>0 else None

    if mapping_query_data is None:
        return {}
    
    return mapping_query_data.get('template_json',{})

async def perform_simulation_ingestion(statement_id, bank_name, template_type):
    print("inside perform_simulation_ingestion")
    quality_query = """
                    select * from mocktemplates where bank_name=%(bank_name)s and template_type=%(template_type)s and active_status=1
                    """
    values = {
            'bank_name': bank_name,
            'template_type': template_type,
        }
    quality_data = DBConnection(QUALITY_DATABASE_NAME).execute_query(
        query=quality_query,
        values=values
    )

    template_id = None
    data_from_template_handler = None
    for i in range(len(quality_data)):
        quality_data[i] = dict(quality_data[i])
        template_id = quality_data[i].get('template_uuid')
        data_from_template_handler = invoke_template_handler_lambda({
            "bucket": PDF_BUCKET,
            "key": f"pdf/{statement_id}_{bank_name}.pdf",
            "template": json.loads(quality_data[i].get("template_json")),
            "template_type":template_type,
            "new_flow":True,
            "bank":bank_name
        })

        if len(data_from_template_handler)>0:
            data_from_template_handler = data_from_template_handler[0]
            # print(data_from_template_handler)
            if not data_from_template_handler:
                continue
            if len(data_from_template_handler['data'])==0:
                continue
            if not data_from_template_handler['data'][0]:
                continue
        print("exiting perform_simulation_ingestion")
        return data_from_template_handler, template_id
    print("exiting perform_simulation_ingestion")
    return data_from_template_handler, template_id

async def perform_simulation_ingestion_od_or_limit(statement_id, bank_name):
    quality_data = await get_od_templates_vanilla(bank_name)

    template_id=None
    template_type=None
    data_from_template_handler=None
    for i in range(len(quality_data)):
        # quality_data[i] = dict(quality_data[i])
        template_id = quality_data[i].get('template_uuid')
        template_type = quality_data[i].get('template_type')

        data_from_template_handler = invoke_template_handler_lambda({
            "bucket": PDF_BUCKET,
            "key": f"pdf/{statement_id}_{bank_name}.pdf",
            "template": json.loads(quality_data[i].get("template_json")),
            "template_type":template_type,
            "new_flow":True,
            "bank":bank_name
        })

        if len(data_from_template_handler)>0:
            data_from_template_handler = data_from_template_handler[0]
            # print(data_from_template_handler)
            if not data_from_template_handler:
                continue
            if len(data_from_template_handler['data'])==0:
                continue
            if not data_from_template_handler['data'][0]:
                continue
        return data_from_template_handler, template_id, template_type
    return data_from_template_handler, template_id, template_type

async def has_keyword_for_category_ifsc_micr_ccod_limit(statement_id, bank_name):
    key = f"pdf/{statement_id}_{bank_name}.pdf"
    ac_category_ingest_keyword = None
    ifsc_ingest_keyword = None
    micr_ingest_keyword = None
    od_limit_ingest_keyword = None 
    credit_limit_ingest_keyword = None 
    ccod_ingest_keyword = None
    ACCOUNT_CATEGORY_KEYWORDS = await get_or_put_keywords_quality('ACCOUNT_CATEGORY_KEYWORDS')
    IFSC_KEYWORDS = await get_or_put_keywords_quality('IFSC_KEYWORDS')
    MICR_KEYWORDS = await get_or_put_keywords_quality('MICR_KEYWORDS')

    print('INGEST KEYWORD :: START')
    doc: fitz.Document = None
    try:
        pdf_bucket_response = s3.get_object(Bucket=PDF_BUCKET, Key=key)

        response_metadata = pdf_bucket_response.get('Metadata')
        password = response_metadata.get('pdf_password')
        path = f'/tmp/temp_{statement_id}'
        with open(path,'wb') as theFile:
            theFile.write(pdf_bucket_response['Body'].read())
        
        doc = read_pdf(path,password)
        for page_number in range(0,min(doc.page_count, 5)):
            page = doc[page_number]

            if ac_category_ingest_keyword==None and page_number<=3:
                for word in ACCOUNT_CATEGORY_KEYWORDS:
                    areas = page.search_for(word,hit_max=1)
                    has_ac_category=len(areas)>0
                    if has_ac_category:
                        ac_category_ingest_keyword = word
                        break

            if ifsc_ingest_keyword==None:
                for word in IFSC_KEYWORDS:
                    areas = page.search_for(word,hit_max=1)
                    has_ifsc = len(areas)>0
                    if has_ifsc:
                        ifsc_ingest_keyword = word
                        break

            if micr_ingest_keyword==None:
                for word in MICR_KEYWORDS:
                    areas = page.search_for(word,hit_max=1)
                    has_micr = len(areas)>0
                    if has_micr:
                        micr_ingest_keyword = word
                        break
                    
            print("INGEST KEYWORD :: COMPLETE")
            if ac_category_ingest_keyword!=None and micr_ingest_keyword!=None and ifsc_ingest_keyword!=None:
                return [ac_category_ingest_keyword, ifsc_ingest_keyword, micr_ingest_keyword]
        
        if os.path.exists(path):
            os.remove(path)
        print("INGEST KEYWORD :: COMPLETE")
        doc.close()
        return [ac_category_ingest_keyword, ifsc_ingest_keyword, micr_ingest_keyword]
    except Exception as e:
        print(e)
        if isinstance(doc, fitz.Document):
            doc.close()
        return ["Ingesting : Some error in ingest keyword" for i in range(0,3)]

@quality_router.get("/")
def health():
    return {"message": "i'm up"}

async def perform_invalid_text_bbox_simulation(template_json, bank_name, template_id, null_type):
    quality_query_to_get_not_done = f"""
                select statement_id, bank_name from statement_quality 
                where {null_type}_ignore_case=FALSE and {null_type}=TRUE and {null_type}_maker_status=FALSE
                    """
    
    statements = []
    quality_data = DBConnection(QUALITY_DATABASE_NAME).execute_query(
        query=quality_query_to_get_not_done
    )
    length = len(quality_data)
    cnt =0

    print(f'PERFORM SIMULATION :: CHECKING :: {length}')
    for data in quality_data:
        data = dict(data)
        statement_id = data.get('statement_id')
        bank_name_cur = data.get('bank_name')

        print(f'PERFORM SIMULATION PERMANENT IGNORE:: COMPLETED {cnt} OUT OF {length}')

        key = f"pdf/{statement_id}_{bank_name_cur}.pdf"
        try:
            pdf_bucket_response = s3.get_object(Bucket=PDF_BUCKET, Key=key)
        except Exception as e:
            print(e, "PDF_NOT_FOUND")
            continue

        response_metadata = pdf_bucket_response.get('Metadata')
        password = response_metadata.get('pdf_password')
        temp_file_path = f'/tmp/temp_{statement_id}'
        with open(temp_file_path,'wb') as theFile:
            theFile.write(pdf_bucket_response['Body'].read())

        doc = read_pdf(temp_file_path,password)
        invalid_check = check_text_set(template_json, doc[0])
        if invalid_check:
            statements.append(statement_id)

        cnt+=1
        if os.path.exists(temp_file_path):
            os.remove(temp_file_path)


    if len(statements)>0:
        statement_string = ""
        for statement in statements:
            statement_string += f",'{statement}'"
        
        statement_string=statement_string[1:]
        quality_update_query = f"""
                            update statement_quality set name_null_maker_status=true, account_null_maker_status=true, date_null_maker_status=true, logo_null_maker_status = true, ac_category_null_maker_status=true, ifsc_null_maker_status=true, micr_null_maker_status=true, od_or_limit_null_maker_status=true, address_null_maker_status=true, 
                            name_null_maker_parked_data=%(parked_data)s, account_null_maker_parked_data=%(parked_data)s, date_null_maker_parked_data=%(parked_data)s, logo_null_maker_parked_data=%(parked_data)s, ac_category_null_maker_parked_data=%(parked_data)s, ifsc_null_maker_parked_data=%(parked_data)s, micr_null_maker_parked_data=%(parked_data)s, od_or_limit_null_maker_parked_data=%(parked_data)s, address_null_maker_parked_data=%(parked_data)s,
                            name_null_ignore_case=false, account_null_ignore_case=false, date_null_ignore_case=false, logo_null_ignore_case = false, ac_category_null_ignore_case=false, ifsc_null_ignore_case=false, micr_null_ignore_case=false, od_or_limit_null_ignore_case=false, address_null_ignore_case=false
                            where statement_id in ({statement_string})
                                """
        quality_data = DBConnection(QUALITY_DATABASE_NAME).execute_query(
            query=quality_update_query,
            values={
                'parked_data': json.dumps({template_id:["INVALID_STATEMENT"]})
            }
        )

@quality_router.post("/perform_simulation", tags=['quality'])
async def perform_simulation(request: PerformSimulation, response:Response, user = Depends(get_current_user)):
    bank_name = request.bank_name
    template_type = request.template_type
    template_id = request.template_id
    bucket = PDF_BUCKET
    parent_module = request.parent_module
    
    time_now = datetime.now()

    print("PERFORM_SIMULATION :: Template ID requested --> ", template_id)
    if not template_id:
        response.status_code = status.HTTP_404_NOT_FOUND
        return {"message": "invalid template id"}

    # go and search the template json from mock templates
    # first get all the templates that may have been applied through this template_id
    quality_query = """
                        SELECT * from mocktemplates where template_uuid = :template_id
                    """
    quality_query_data = await quality_database.fetch_one(query=quality_query, values={"template_id": template_id})
    if not quality_query_data:
        response.status_code = status.HTTP_404_NOT_FOUND
        return {"message": "template id not found"}
    
    quality_query_data = dict(quality_query_data)
    added_through_statement_id = quality_query_data.get('statement_id')
    if template_type in ['is_od_account_bbox','limit_bbox','od_limit_bbox']:
        obj = await get_details_add_od_or_limit(added_through_statement_id)
        if not obj.get(f'can_simulate',True):
            response.status_code = status.HTTP_400_BAD_REQUEST
            return {"message": "This case was simulated already, skipping the simulation"}

    quality_query_data['template_json'] = json.loads(quality_query_data['template_json'])
    template_json = quality_query_data['template_json']

    if template_type in ['invalid_text_bbox']:
        null_type = invalid_map[parent_module]
        if null_type in ['logo_less_bbox']:
            null_type = 'logo_null'
        elif null_type in ['is_od_account_bbox']:
            null_type = 'od_or_limit_null'
        await perform_invalid_text_bbox_simulation(template_json, bank_name, template_id, null_type)
        return {"message": "simulation successful"}

    null_type = invalid_map[template_type]
    if template_type in ['ignore_logo_hash','logo_less_bbox']:
        null_type='logo_null'
    if template_type in ['limit_bbox','od_limit_bbox','is_od_account_bbox']:
        null_type='od_or_limit_null'
    print("Null Type --> ", null_type)

    query_to_get_all_not_done_templates_for_this_type = f"""
                            SELECT statement_id, bank_name, pdf_password,
                                {null_type}, {null_type}_maker_status, {null_type}_maker_parked_data
                            FROM statement_quality 
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
    
    transaction_flag = template_type == "trans_bbox"
    if template_type == 'account_category_bbox':
        added_mapping = template_json.get('added_mapping',{})
        template_json.pop('added_mapping',None)
        if added_mapping in [{},None]:
            prod_mappings = await get_mapping_acc_category(bank_name)
            added_mapping = prod_mappings
    
    for i in range(len(data_from_the_query)):
        data_from_the_query[i] = dict(data_from_the_query[i])
    # now for all these templates whose parked data is None or blank, and they have not been approved or made, get the data and store in the table
    for items in data_from_the_query:
        if items.get(null_type) and not items.get(f"{null_type}_marker_status"):
            if template_type not in ['ignore_logo_hash','logo_less_bbox','logo_hash','account_category_bbox'] and items.get(f"{null_type}_maker_parked_data"):
                continue
            
            statement_id = items.get("statement_id")
            # check if the object is present in the bucket
            key = f"pdf/{statement_id}_{bank_name}.pdf"
            try:
                pdf_bucket_response = s3.get_object(Bucket=bucket, Key=key)
                # write a temporary file with content
                # with open(tmp_file_path, 'wb') as file_obj:
                #     file_obj.write(pdf_bucket_response['Body'].read())
            except Exception as e:
                print(e)
                continue

            if template_type == "logo_hash":
                # here we need to get the images for this statement, and check if this logo is present in the images list
                required_logo = template_json[0]
                logo_null_maker_parked_data = items.get("logo_null_maker_parked_data")
                
                if logo_null_maker_parked_data is None:
                    continue
                
                present_parked_data = json.loads(items.get("logo_null_maker_parked_data"))
                hash_list = present_parked_data.get("hash_list")
                
                if required_logo not in hash_list:
                    continue
                
                present_parked_data['selected_hash'] = required_logo
                present_templates_response = [present_parked_data]
            elif template_type == 'ignore_logo_hash':
                await simulate_ignore_logo_hash_on_template(template_json.get('hash_list',[]),template_id,items,statement_id,False)
                continue
            elif template_type == 'logo_less_bbox':
                await simulate_logo_less_bbox(statement_id,items,template_json,pdf_bucket_response,statement_id,template_id,False)
                continue
            else:
                # get the data from the template handler lambda for this 
                invocation_payload = {
                    "transaction_flag": transaction_flag,
                    "bucket": bucket,
                    "key": key,
                    "template": template_json,
                    "template_type": template_type,
                    "new_flow": True,
                    "bank" : bank_name
                }
                if template_type == 'account_category_bbox':
                    invocation_payload['mapping'] = added_mapping
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

                if template_type in ['ifsc_bbox','micr_bbox','limit_bbox','od_limit_bbox','is_od_account_bbox','address_bbox']:
                    if (len(data['data'])==0) or (data['data'][0] in ['',None]):
                        continue
                
                if template_type=='date_bbox':
                    data_present = data['to_data'][0] and data['from_data'][0]
                    if not data_present:
                        continue
                if template_type=='account_category_bbox':
                    if (len(data['data'])==0) or (data['data'][0] in ['',None]):
                        continue
                
                print(f"need to update {null_type} for statement_id: {statement_id} with parked data: {present_templates_response}")
                # updating parked data with template response
                update_query = f"""
                                update statement_quality set {null_type}_maker_parked_data = :parked_data,
                                {null_type}_maker_status = TRUE
                                where statement_id = :statement_id and {null_type}=TRUE
                            """
                if template_type in ['limit_bbox','od_limit_bbox','is_od_account_bbox']:
                    update_query = f"""
                                update statement_quality set {null_type}_maker_parked_data = :parked_data,
                                {null_type}_maker_status = TRUE, {template_type}_simulated = TRUE
                                where statement_id = :statement_id and {null_type}=TRUE
                            """
                update_query_result = await quality_database.execute(
                                            query=update_query, 
                                            values = {
                                                "parked_data": json.dumps({template_id : present_templates_response}),
                                                "statement_id": items.get("statement_id")
                                            }
                                        )
            print(f"PERFORM SIMULATION :: Statement_id : {statement_id} is updated in bank_connect_statement quality, started {datetime.now()-time_now}")
    
    print("PERFORM_SIMULATION :: PROCESS_COMPLETE --> ", template_id)
    return {"message": "simulation successful"}

async def get_details_add_od_or_limit(statement_id):
    query = """
            select is_od_account_detected, is_credit_limit_detected, is_od_limit_detected, is_od_account_bbox_simulated, od_limit_bbox_simulated, limit_bbox_simulated from statement_quality where statement_id=:statement_id
            """
    query_data = await quality_database.fetch_one(query=query,values={'statement_id':statement_id})
    if query_data!=None:
        query_data = dict(query_data)

        response_object = {
            "can_add_is_od_account_bbox": not query_data.get('is_od_account_detected') and not query_data.get('is_od_account_bbox_simulated'),
            "can_add_od_limit_bbox": not query_data.get('is_od_limit_detected') and not query_data.get('od_limit_bbox_simulated'),
            "can_add_limit_bbox": not query_data.get('is_credit_limit_detected') and not query_data.get('limit_bbox_simulated')
        }
        response_object["can_simulate"] = not query_data.get('is_od_account_bbox_simulated') and not query_data.get('od_limit_bbox_simulated') and not query_data.get('limit_bbox_simulated')
        return response_object
    else:
        return {
            "can_simulate":True,
            "can_add_is_od_account_bbox":True,
            "can_add_od_limit_bbox":True,
            "can_add_limit_bbox":True
        }

@quality_router.post("/superuser_approval", tags=['quality'])
async def superuser_approval(request: SuperUserApproval, response:Response, user = Depends(get_current_user)):
    if user.user_type != "superuser":
        response.status_code = status.HTTP_401_UNAUTHORIZED
        return {"message": "not authorised"}
    
    template_id = request.template_id
    approval = request.approval

    # first get all the templates that may have been applied through this template_id
    quality_query = """
                        SELECT * from mocktemplates where template_uuid = :template_id
                    """
    quality_query_data = await quality_database.fetch_one(query=quality_query, values={"template_id": template_id})
    if not quality_query_data:
        response.status_code = status.HTTP_404_NOT_FOUND
        return {"message": "template id not found"}
    
    quality_query_data = dict(quality_query_data)
    quality_query_data['template_json'] = json.loads(quality_query_data['template_json'])
    
    template_type = quality_query_data['template_type']
    null_type = invalid_map[template_type]
    if template_type in ['ignore_logo_hash','logo_less_bbox']:
        null_type='logo_null'
    if template_type in ['limit_bbox','od_limit_bbox','is_od_account_bbox']:
        null_type='od_or_limit_null'
    if template_type in ['invalid_text_bbox']:
        null_type='logo_null'

    if template_type in ['invalid_text_bbox']:
        statement_quality_query = f"""
                                    SELECT statement_id, bank_name, pdf_password, {null_type}, {null_type}_maker_status, {null_type}_checker_status, {null_type}_maker_parked_data
                                    FROM statement_quality where {null_type} = TRUE AND
                                    {null_type}_maker_status = TRUE AND
                                    {null_type}_checker_status <> TRUE AND
                                    {null_type}_ignore_case <> TRUE
                                """
        statement_quality_query_data = await quality_database.fetch_all(query = statement_quality_query)
    else:
        statement_quality_query = f"""
                                    SELECT statement_id, bank_name, pdf_password, {null_type}, {null_type}_maker_status, {null_type}_checker_status, {null_type}_maker_parked_data
                                    FROM statement_quality where {null_type} = TRUE AND
                                    {null_type}_maker_status = TRUE AND
                                    {null_type}_checker_status <> TRUE AND
                                    {null_type}_ignore_case <> TRUE AND
                                    bank_name = :bank_name
                                """
        statement_quality_query_data = await quality_database.fetch_all(query = statement_quality_query, values={"bank_name": quality_query_data["bank_name"]})
    
    # now filtering and keeping only those objects that're related to this template_id
    statement_quality_query_data_final = []
    for items in statement_quality_query_data:
        statement_id = items.get('statement_id')
        bank_name = quality_query_data["bank_name"]
        if template_type == 'ignore_logo_hash':
            status = await simulate_ignore_logo_hash_on_template(quality_query_data.get('template_json',{}).get('hash_list',[]),template_id,items,items.get('statement_id'),True)
            if status:
                statement_quality_query_data_final.append(items)
        elif template_type == 'logo_less_bbox':
            status = await simulate_logo_less_bbox(statement_id,items,quality_query_data['template_json'],None,statement_id,template_id,True)
            if status:
                statement_quality_query_data_final.append(items)
        else:
            temp_data = dict(items)
            if temp_data[f"{null_type}_maker_parked_data"] is not None:
                temp_data[f"{null_type}_maker_parked_data"] = json.loads(temp_data[f"{null_type}_maker_parked_data"])
                if template_id in temp_data[f"{null_type}_maker_parked_data"]:
                    statement_quality_query_data_final.append(temp_data)
        
        print(f"SUPERUSER APPROVAL :: statement_id {statement_id}")
    
    # prepare a list of all the statement_ids whose checker status needs to be changed
    short_statement_id_list = [_.get("statement_id") for _ in statement_quality_query_data_final]
    print("List of statement_ids that need to be updated --> ", short_statement_id_list)

    update_query = None

    if template_type in ['invalid_text_bbox']:
        if len(short_statement_id_list)>0:
            statement_string = ""
            for statement in short_statement_id_list:
                statement_string+=f",'{statement}'"
            statement_string = statement_string[1:]

            update_query = f"""
                        update statement_quality set name_null_maker_status=false, account_null_maker_status=false, date_null_maker_status=false, logo_null_maker_status = false, ac_category_null_maker_status=false, ifsc_null_maker_status=false, micr_null_maker_status=false, od_or_limit_null_maker_status=false, address_null_maker_status=false,
                            name_null_maker_parked_data=null, account_null_maker_parked_data=null, date_null_maker_parked_data=null, logo_null_maker_parked_data=null, ac_category_null_maker_parked_data=null, ifsc_null_maker_parked_data=null, micr_null_maker_parked_data=null, od_or_limit_null_maker_parked_data=null, address_null_maker_parked_data=null
                        where statement_id in ({statement_string})
                        """
            if approval:
                update_query = f"""
                            update statement_quality set name_null_checker_status=true, account_null_checker_status=true, date_null_checker_status=true, logo_null_checker_status = true, ac_category_null_checker_status=true, ifsc_null_checker_status=true, micr_null_checker_status=true, od_or_limit_null_checker_status=true, address_null_checker_status=true
                        where statement_id in ({statement_string})
                            """
            await quality_database.execute(query = update_query)
            
        return {"message": "approval changes done"}

    if approval:
        update_query = f"""
                        UPDATE statement_quality set {null_type}_checker_status = TRUE where statement_id = :statement_id
                        """
    else:
        if template_type in ['logo_less_bbox', 'logo_hash', 'ignore_logo_hash']:
            update_statement_quality_query = f"""
                                                UPDATE statement_quality set {null_type}_checker_status = FALSE , {null_type}_maker_status = FALSE , {null_type}_maker_parked_data =:present_parked_data where statement_id = :statement_id
                                            """
            for statement_item in statement_quality_query_data_final:
                present_parked_data = statement_item.get('logo_null_maker_parked_data',{})
                if isinstance(present_parked_data,str):
                    present_parked_data = json.loads(present_parked_data)
                wrt_template_id = present_parked_data.get(template_id,None)
                if wrt_template_id is not None:
                    wrt_template_id = wrt_template_id[0]
                    updated_parked_data = {}
                    updated_parked_data['hash_list'] = wrt_template_id.get('hash_list')
                    updated_parked_data['hash_count'] = wrt_template_id.get('hash_count')
                else:
                    updated_parked_data = None

                update_statement_quality_query_data = await quality_database.execute(query=update_statement_quality_query, values = {"statement_id": statement_item.get('statement_id'), "present_parked_data": json.dumps(updated_parked_data) if updated_parked_data!=None else None})
        else:
            update_query = f"""
                            UPDATE statement_quality set {null_type}_checker_status = FALSE , {null_type}_maker_status = FALSE , {null_type}_maker_parked_data = null where statement_id = :statement_id
                            """
            if template_type in ['limit_bbox','od_limit_bbox','is_od_account_bbox']:
                update_query = f"""
                            UPDATE statement_quality set {template_type}_simulated=FALSE ,{null_type}_checker_status = FALSE , {null_type}_maker_status = FALSE , {null_type}_maker_parked_data = null where statement_id = :statement_id
                            """
    
    if update_query is not None:    
        for short_statement_item in short_statement_id_list:
            update_statement_quality_query_data = await quality_database.execute(query=update_query, values = {"statement_id": short_statement_item})

    if template_type == "logo_hash" and approval:
        # since this is getting approved , we can safely put this in production quality logo s3 bucket for this bank_name
        concerned_hash = quality_query_data['template_json'][0]
        quality_bucket_file_path = f"main_logos/{quality_query_data.get('bank_name')}/{concerned_hash}.png"
        s3_resource.meta.client.copy({
            'Bucket': QUALITY_BUCKET,
            'Key': f'quality_logo/{concerned_hash}.png'
        }, QUALITY_BUCKET, quality_bucket_file_path)

    return {"message": "approval changes done"}

@quality_router.post("/ignore_case", tags=['quality'])
async def get_null_cases(request: IgnoreType, response: Response, user= Depends(get_current_user)):
    statement_ids = request.statement_ids
    null_type = request.null_type

    if null_type not in reverse_invalid_map.keys():
        response.status_code = status.HTTP_400_BAD_REQUEST
        return {"message": "invalid null type"}
    
    if null_type in ['ignore_logo_hash','logo_less_bbox']:
        null_type = 'logo_null'
    if null_type in ['limit_bbox','od_limit_bbox','is_od_account_bbox']:
        null_type = 'od_or_limit_null'

    if len(statement_ids)==0:
        return {"message": "ignored statements"}

    # now get the table from quality statement table
    statement_ids = str(statement_ids)
    statement_ids = statement_ids[1:]
    statement_ids = statement_ids[:-1]
    statement_ids = f"({statement_ids})"

    update_statement_quality_table_query = f"""
                                            update statement_quality set {null_type}_ignore_case=True where statement_id in {statement_ids}
                                        """
    update_statement_quality_table_query_data = await quality_database.execute(query=update_statement_quality_table_query)

    return {"message": "ignored statements"}

@quality_router.get("/get_parked_data")
async def get_parked_data(template_id: str, response: Response, page: Optional[int]=1, maxi: Optional[int]=10, user=Depends(get_current_user)):
    # first check whether this template_id was actually requested
    quality_query = """
                        SELECT * from mocktemplates where template_uuid = :template_id
                    """
    
    quality_query_data = await quality_database.fetch_one(query=quality_query, values={"template_id": template_id})
    if not quality_query_data:
        response.status_code = status.HTTP_404_NOT_FOUND
        return {"message": "template id not found"}
    
    quality_query_data = dict(quality_query_data)
    bank_name = quality_query_data["bank_name"]
    template_type = quality_query_data["template_type"]

    if template_type not in invalid_map:
        response.status_code = status.HTTP_404_NOT_FOUND
        return {"message": "this template type is not supported yet"}
    
    null_type = invalid_map[template_type]
    if template_type in ['ignore_logo_hash','logo_less_bbox']:
        null_type='logo_null'
    if template_type in ['is_od_account_bbox','limit_bbox','od_limit_bbox']:
        null_type='od_or_limit_null'
    if template_type == 'invalid_text_bbox':
        null_type='logo_null'

    if template_type in ['invalid_text_bbox']:
        portal_query = f"""
                        SELECT statement_id, pdf_password, bank_name, logo_null_maker_parked_data 
                        from statement_quality
                        where( logo_null=TRUE or name_null=true or account_null=true)and (logo_null_maker_status=true or account_null_maker_status=true or name_null_maker_status=true)
                    """
        portal_query_data = await quality_database.fetch_all(query=portal_query)
    else:
        portal_query = f"""
                        SELECT statement_id, pdf_password, bank_name, {null_type}_maker_parked_data 
                        from statement_quality
                        where bank_name = :bank_name and {null_type}=TRUE and {null_type}_maker_status=true
                    """
        portal_query_data = await quality_database.fetch_all(query=portal_query, values={"bank_name": bank_name})
    response_data = []
    for i in range(len(portal_query_data)):
        portal_query_data[i] = dict(portal_query_data[i])
        if template_type!='trans_bbox':
            parked_data = portal_query_data[i][f"{null_type}_maker_parked_data"]
            if not parked_data:
                continue

            temp_data = json.loads(parked_data).get(template_id)
            if not temp_data or not isinstance(temp_data, list):
                continue
            temp_data = temp_data[0]
            if isinstance(temp_data,str):
                temp_data={"all_text":[""],"data":[temp_data]}
            portal_query_data[i]["parked_data"] = temp_data
            del portal_query_data[i][f"{null_type}_maker_parked_data"]

            if portal_query_data[i].get('pdf_password','') is None:
                portal_query_data[i]['pdf_password'] = ""

            portal_query_data[i]['template_json'] = quality_query_data['template_json']

            if template_type in ['ignore_logo_hash','logo_hash']:
                # also add the presigned url for the logo
                portal_query_data[i]['png_presigned_url'] = viewable_presigned_url(f"quality_logo/{portal_query_data[i]['parked_data']['selected_hash']}.png", QUALITY_BUCKET, "image/png")
            response_data.append(portal_query_data[i])
    
    offset_val = (page-1)*maxi
    return_list = []
    for i in range(offset_val,min(len(response_data),offset_val+maxi)):
        return_list.append(response_data[i])

    return {
        "template_type": template_type,
        "data": return_list,
        "total_cases": len(response_data)
    }

@quality_router.get("/{null_type:str}_cases", tags=['quality'])
async def get_null_cases(null_type:str, response: Response,current_page:Optional[int]=None, selected_bank:Optional[str]=None, max:Optional[int]=100,org_name:Optional[str]=None, organization_id:Optional[int]=None, client_id:Optional[int]=None,user= Depends(get_current_user)):
    # print("Request received for null type --> ", null_type)
    if null_type not in reverse_invalid_map.keys():
        response.status_code = status.HTTP_400_BAD_REQUEST
        return {"message": "invalid null type"}
    
    starttime = datetime.now()
    print(f'CC_NULL CASES :: started at {starttime}')
    
    if null_type in ['ignore_logo_hash','logo_less_bbox']:
        null_type='logo_null'
    
    if null_type in ['is_od_account_bbox','limit_bbox','od_limit_bbox']:
        null_type='od_or_limit_null'

    filter_query = ""
    if organization_id is not None:
        filter_query += f" and organization_id = {organization_id}"
    if client_id is not None:
        filter_query += f" and client_id = {client_id}"
    if org_name is not None:
        filter_query += f" and org_name = '{org_name}'"

    quality_query = f"""
                    select bank_name, count(statement_id) as cases from statement_quality where {null_type}=true and 
                    {null_type}_maker_status <> true and {null_type}_checker_status <> true and {null_type}_ignore_case <> TRUE {filter_query}
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

    query_for_all_data = f"""
                            SELECT statement_id, bank_name, pdf_password, client_id, organization_id, org_name
                                {null_type}, {null_type}_maker_status, {null_type}_maker_parked_data
                            FROM statement_quality
                            WHERE {null_type}=TRUE
                            AND {null_type}_maker_status <> TRUE
                            AND {null_type}_checker_status <> TRUE
                            AND {null_type}_ignore_case <> TRUE
                            AND bank_name=:bank_name
                            {filter_query}
                    """
    
    all_data = await quality_database.fetch_all(query=query_for_all_data,values={
        'bank_name':selected_bank
    })

    offset_val = (current_page-1)*max

    data_to_return = []
    for i in range(offset_val,min(len(all_data),offset_val+max)):
        current_data = dict(all_data[i])
        current_data['presigned_url'] = create_viewable_presigned_url(current_data['statement_id'], current_data['bank_name'])

        # if null type is logo null we need to create presigned urls for all the pngs that are stored in hash dict
        if null_type == 'logo_null':
            current_data['hash_dict'] = {}
            
            if not current_data['logo_null_maker_parked_data']:
                continue
            current_data['logo_null_maker_parked_data'] = json.loads(current_data['logo_null_maker_parked_data'])
            
            if current_data['logo_null_maker_parked_data'].get('selected_hash'):
                continue

            parked_data = current_data['logo_null_maker_parked_data']['hash_list']

            if parked_data is not None:
                for hash in parked_data:
                    current_data['hash_dict'][hash] = viewable_presigned_url(f"quality_logo/{hash}.png", QUALITY_BUCKET, "image/png")
            
                for i in range(len(parked_data)):
                    parked_data[i] = str(parked_data[i])
        data_to_return.append(current_data)
    
    print(f'NULL_CASES :: Ended at {datetime.now()-starttime}')
    response.status_code = status.HTTP_200_OK
    return {"bank_list": bank_cases_list, "all_data": data_to_return, "total_cases":total_cases}

# to think: what do with the cases which we want to ignore
# to think: how to deal with update requests

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

async def perform_simulation_for_statement(statement_id, template_type, bank_name):
    # get all the pending templates from mocktemplates for this statement id
    quality_query = """
                        SELECT * from mocktemplates where bank_name = %(bank_name)s and template_type = %(template_type)s and active_status = 1
                    """
    values = {
        "bank_name": bank_name,
        "template_type": template_type
    }
    quality_data = DBConnection(QUALITY_DATABASE_NAME).execute_query(query=quality_query,values=values)
    
    template_id = None
    data_from_template_handler = None
    for i in range(len(quality_data)):
        quality_data[i] = dict(quality_data[i])
        template_id = quality_data[i].get("template_uuid")
        data_from_template_handler = invoke_template_handler_lambda({
            "bucket": PDF_BUCKET,
            "key": f"pdf/{statement_id}_{bank_name}.pdf",
            "template": quality_data[i].get("template_json"),
            "template_type": template_type,
            "new_flow": True
        })
        data_from_template_handler = data_from_template_handler[0]
        if not data_from_template_handler:
            continue
        
        if template_type=='date_bbox':
            data_present = data_from_template_handler['to_data'][0] and data_from_template_handler['from_data'][0]
            if not data_present:
                continue
        
        return data_from_template_handler, template_id
    return data_from_template_handler, template_id


async def perform_logo_simulation_for_statement(statement_id, bank_name, my_hashes):
    quality_query = """
                        SELECT * from mocktemplates where bank_name = %(bank_name)s and template_type = %(template_type)s and active_status = 1
                    """
    values = {
        "bank_name": bank_name,
        "template_type": "logo_hash"
    }
    quality_data = DBConnection(QUALITY_DATABASE_NAME).execute_query(query=quality_query, values=values)

    for i in range(len(quality_data)):
        quality_data[i] = dict(quality_data[i])
        concerned_hash = json.loads(quality_data[i].get("template_json"))[0]
        template_uuid = quality_data[i].get('template_uuid')
        if concerned_hash in my_hashes:
            return concerned_hash, template_uuid
    return None, None

# @quality_router.post("/ingest", tags=['quality'])
async def ingest_into_quality_vanilla(portalData: dict):
    statement_id = portalData.get('statement_id')
    org_metadata = portalData.get('org_metadata', dict())
    client_id = org_metadata.get('client_id')
    organization_id = org_metadata.get('organization_id')
    org_name = org_metadata.get('org_name')
    
    redis_response = redis_cli.get(f'{STATEMENT_TYPE}_ingest_{statement_id}')
    if redis_response != None:
        return {"message": "This statement is already ingested"}
    
    start_time = datetime.now()
    print(f"INGEST :: statement_id {statement_id}")
    
    statement_status = portalData.get("statement_status")

    # in case of extracted by perfios, return
    if portalData.get("is_extracted_by_perfios"):
        print(f"statement_id: {statement_id}, is either extracted by perfios")
        redis_cli.set(f'{STATEMENT_TYPE}_ingest_{statement_id}', statement_id, ex=172800)
        return
    
    # if keyword all != True, no need to do this operation
    if not portalData.get("keyword_all") and statement_status!=12:
        print(f"statement_id: {statement_id}, does not have all keywords, not doing ingestion.")
        redis_cli.set(f'{STATEMENT_TYPE}_ingest_{statement_id}', statement_id, ex=172800)
        return
    
    # if attempt type is aa or external aa data don't do this
    if portalData.get("is_external_aa_data") or portalData.get("attempt_type") == "aa":
        print(f"Statement {statement_id} was rejected because it is either uploaded via aa or is external aa data.")
        redis_cli.set(f'{STATEMENT_TYPE}_ingest_{statement_id}', statement_id, ex=172800)
        return

    # check whether bank_name and predicted bank are same, if not don't make an entry
    predicted_bank = portalData.get("predicted_bank")
    bank_name = portalData.get("bank_name")

    key = f"pdf/{statement_id}_{bank_name}.pdf"
    try:
        pdf_bucket_response = s3.get_object(Bucket=PDF_BUCKET, Key=key)
    except Exception as e:
        print(e)
        redis_cli.set(f'{STATEMENT_TYPE}_ingest_{statement_id}', statement_id, ex=172800)
        return {"message":f"statement_id: {statement_id}, not in pdf bucket, not doing ingestion."} 

    response_metadata = pdf_bucket_response.get('Metadata')
    password = response_metadata.get('pdf_password')
    temp_file_path = f'/tmp/temp_{statement_id}'
    with open(temp_file_path,'wb') as theFile:
        theFile.write(pdf_bucket_response['Body'].read())

    doc = read_pdf(temp_file_path,password)
    
    pdf_password = portalData.get("pdf_password")

    logo_hash = portalData.get("logo_hash")
    value_map = {"bank_name" : portalData.get("bank_name")}
    value_map["statement_id"] = statement_id
    value_map["pdf_password"] = pdf_password

    is_statement_invalid, invalid_template_id = False, None
    invalid_text_bbox_templates = await get_vanilla_statement_invalid_text_bbox('generic')
    page = doc.load_page(0)
    for invalid_text_bbox_template in invalid_text_bbox_templates:
        invalid_template_json = invalid_text_bbox_template.get('template_json')
        invalid_template_uuid = invalid_text_bbox_template.get('template_uuid')
        invalid_check = check_text_set(invalid_template_json, page)
        if invalid_check:
            invalid_template_id = invalid_template_uuid
            is_statement_invalid = True
            break
    value_map["logo_null"] = logo_hash is None
    value_map["logo_null_maker_status"] = is_statement_invalid
    value_map["logo_null_maker_parked_data"] = None
    value_map["logo_null_checker_status"] = False
    value_map["logo_null_ignore_case"] = is_statement_invalid

    hash_dict = get_images(statement_id, bank_name, pdf_password)
    if value_map["logo_null"]:
        value_map["logo_null_maker_parked_data"] = {}
        value_map["logo_null_maker_parked_data"]["hash_count"] = len(hash_dict)
        value_map["logo_null_maker_parked_data"]["hash_list"] = list(hash_dict.values())
        if is_statement_invalid:
            value_map['logo_null_maker_parked_data'][invalid_template_id] = ["INVALID_STATEMENT"]
        else:
            # perform simulation for this statement
            simulation_logo, logo_template_simulated = await perform_logo_simulation_for_statement(statement_id, bank_name, value_map["logo_null_maker_parked_data"]["hash_list"])
            print('INGESTION :: performed simulation logo_null')
            if simulation_logo:
                value_map["logo_null_maker_parked_data"]['selected_hash'] = simulation_logo
                value_map["logo_null_maker_status"] = True
                value_map['logo_null_maker_parked_data'] = {logo_template_simulated:value_map['logo_null_maker_parked_data']} 
        value_map["logo_null_maker_parked_data"] = json.dumps(value_map["logo_null_maker_parked_data"])

    # if predicted_bank is not None:
    is_predicted_bank = predicted_bank is not None and predicted_bank.lower()
    bank_name = bank_name.lower()

    # if predicted_bank!=bank_name:
    #     print(f"BankName and Predicted Bank do not match, not doing for {statement_id}")
    #     redis_cli.set(f'{STATEMENT_TYPE}_ingest_{statement_id}', statement_id, ex=172800)
    #     return

    name = portalData.get("name")
    value_map["name_null"] = name in [None, ""]
    value_map["name_null_maker_status"] = is_statement_invalid
    value_map["name_null_maker_parked_data"] = None
    value_map["name_null_checker_status"] = False
    value_map["name_null_ignore_case"] = False

    if value_map["name_null"] and is_predicted_bank:
        if is_statement_invalid:
            value_map['logo_null_maker_parked_data'] = json.dumps({invalid_template_id:["INVALID_STATEMENT"]})
        else:
            try:
                parked_data, template_uuid = await perform_simulation_for_statement(statement_id, "name_bbox", bank_name)
                print('INGESTION :: performed simulation name_bbox')
                if parked_data and template_uuid:
                    value_map["name_null_maker_parked_data"] = json.dumps({template_uuid : parked_data}) or None
                    value_map["name_null_maker_status"] = True
            except Exception as e:
                print(f"Exception while performing simulation for name_null for statement id {statement_id} : {e}")

    value_map["account_null"] = statement_status == 12
    value_map["account_null_maker_status"] = is_statement_invalid
    value_map["account_null_maker_parked_data"] = None
    value_map["account_null_checker_status"] = False
    value_map["account_null_ignore_case"] = False

    if value_map["account_null"]:
        if is_statement_invalid:
            value_map["account_null_maker_parked_data"] = json.dumps({invalid_template_id:["INVALID_STATEMENT"]})
        else:
            try:
                parked_data, template_uuid = await perform_simulation_for_statement(statement_id, "accnt_bbox", bank_name)
                print('INGESTION :: performed simulation accnt_bbox')
                if parked_data and template_uuid:
                    value_map["account_null_maker_parked_data"] = json.dumps({template_uuid : parked_data}) or None
                    value_map["account_null_maker_status"] = True
            except Exception as e:
                print(f"Exception while performing simulation for account null for statement id {statement_id} : {e}")

    from_date = portalData.get("from_date")
    to_date = portalData.get("to_date")
    value_map["date_null"] = from_date is None or to_date is None and is_predicted_bank
    value_map["date_null_maker_status"] = is_statement_invalid
    value_map["date_null_maker_parked_data"] = None
    value_map["date_null_checker_status"] = False
    value_map["date_null_ignore_case"] = False

    if value_map["date_null"]:
        if is_statement_invalid:
            value_map["date_null_maker_parked_data"] = json.dumps({invalid_template_id:["INVALID_STATEMENT"]})
        else:
            try:
                parked_data, template_uuid = await perform_simulation_for_statement(statement_id, "date_bbox", bank_name)
                print('INGESTION :: performed simulation date_bbox')
                if parked_data and template_uuid:
                    value_map["date_null_maker_parked_data"] = json.dumps({template_uuid : parked_data}) or None
                    value_map["date_null_maker_status"] = True
            except Exception as e:
                print(f"Exception while performing simulation for date_null for statement id {statement_id} : {e}")
    
    has_keyword_result = await has_keyword_for_category_ifsc_micr_ccod_limit(statement_id, bank_name)

    detected_category = portalData.get('account_category')
    value_map["ac_category_null"] =  detected_category in [None,""] and has_keyword_result[0]!=None
    value_map["ac_category_null_maker_status"] = is_statement_invalid
    value_map["ac_category_null_maker_parked_data"] = None
    value_map["ac_category_null_checker_status"] = False
    value_map["ac_category_null_ignore_case"] = False
    value_map['ac_category_ingest_keyword'] = has_keyword_result[0]
    portalData['has_ac_category_keyword'] = has_keyword_result[0]

    if value_map['ac_category_null']:
        if is_statement_invalid:
            value_map['ac_category_null_maker_parked_data'] = json.dumps({invalid_template_id:["INVALID_STATEMENT"]})
        else:
            try:
                mapping = await get_mapping_acc_category(bank_name)
                parked_data, template_uuid = await simulate_ac_category_ingestion(statement_id,bank_name,mapping)
                print('INGESTION :: performed simulation account_category')
                if parked_data and template_uuid:
                    if len(parked_data['data'])!=0 and parked_data['data'][0] not in [None,'']: 
                        value_map["ac_category_null_maker_parked_data"] = json.dumps({template_uuid : [parked_data]})
                        value_map["ac_category_null_maker_status"] = True
            except Exception as e:
                print(f"Exception while performing simulation for ac_category null for statement id {statement_id} : {e}")
    
    detected_ifsc = portalData.get('ifsc')
    value_map["ifsc_null"] = detected_ifsc in [None,""] and has_keyword_result[1]!=None
    value_map["ifsc_null_maker_status"] = is_statement_invalid
    value_map["ifsc_null_maker_parked_data"] = None
    value_map["ifsc_null_checker_status"] = False 
    value_map["ifsc_null_ignore_case"] = False
    value_map['ifsc_ingest_keyword'] = has_keyword_result[1]
    portalData['has_ifsc_keyword'] = has_keyword_result[1]

    if value_map['ifsc_null']:
        if is_statement_invalid:
            value_map["ifsc_null_maker_status"] = json.dumps({is_statement_invalid:["INVALID_STATEMENT"]})
        else:
            try:
                parked_data, template_uuid = await perform_simulation_ingestion(statement_id, bank_name, 'ifsc_bbox')
                print('INGESTION :: performed simulation ifsc_bbox')
                if parked_data and template_uuid:
                    if len(parked_data['data'])!=0 and parked_data['data'][0] not in [None,'']: 
                        value_map[f"ifsc_null_maker_parked_data"] = json.dumps({template_uuid : [parked_data]})
                        value_map[f"ifsc_null_maker_status"] = True
            except Exception as e:
                print(f"Exception while performing simulation for ifsc null for statement id {statement_id} : {e}")

    detected_micr = portalData.get('micr')
    value_map["micr_null"] = detected_micr in [None,""] and has_keyword_result[2]!=None
    value_map["micr_null_maker_status"] = is_statement_invalid
    value_map["micr_null_maker_parked_data"] = None
    value_map["micr_null_checker_status"] = False 
    value_map["micr_null_ignore_case"] = False
    value_map['micr_ingest_keyword'] = has_keyword_result[2]
    portalData['has_micr_keyword'] = has_keyword_result[1]

    if value_map['micr_null']:
        if is_statement_invalid:
            value_map['micr_null_maker_parked_data'] = json.dumps({invalid_template_id:"INVALID_STATEMENT"})
        else:
            try:
                parked_data, template_uuid = await perform_simulation_ingestion(statement_id, bank_name, 'micr_bbox')
                print('INGESTION :: performed simulation micr_bbox')
                if parked_data and template_uuid:
                    if len(parked_data['data'])!=0 and parked_data['data'][0] not in [None,'']: 
                        value_map[f"micr_null_maker_parked_data"] = json.dumps({template_uuid : [parked_data]})
                        value_map[f"micr_null_maker_status"] = True
            except Exception as e:
                print(f"Exception while performing simulation for micr null for statement id {statement_id} : {e}")

    value_map['is_od_account_detected'] = portalData.get('is_od_account') not in [None,""]
    value_map['is_credit_limit_detected'] = portalData.get('credit_limit') not in [None,""]
    value_map['is_od_limit_detected'] = portalData.get('od_limit') not in [None,""]
    value_map['is_od_account_bbox_simulated'] = False
    value_map['limit_bbox_simulated'] = False
    value_map['od_limit_bbox_simulated'] = False
    is_od_or_limit_null = (not value_map['is_od_account_detected'] or not value_map['is_credit_limit_detected'] or not value_map['is_od_limit_detected'])
    if is_od_or_limit_null:
        if (value_map['is_credit_limit_detected'] and portalData.get('credit_limit')==0) and (value_map['is_od_limit_detected'] and portalData.get('od_limit')==0):
            is_od_or_limit_null = False

    value_map['od_or_limit_null'] = is_od_or_limit_null
    value_map['od_or_limit_null_maker_status'] = is_statement_invalid
    value_map['od_or_limit_null_maker_parked_data'] = None
    value_map['od_or_limit_null_checker_status'] = False
    value_map['od_or_limit_null_ignore_case'] = False

    if value_map['od_or_limit_null']:
        if is_statement_invalid:
            value_map['od_or_limit_null_maker_parked_data'] = json.dumps({invalid_template_id:["INVALID_STATEMENT"]})
        else:
            try:
                parked_data, template_uuid, template_type = await perform_simulation_ingestion_od_or_limit(statement_id, bank_name)
                print('INGESTION :: performed simulation od_bbox')
                if parked_data and template_uuid and template_type:
                    if len(parked_data['data'])!=0 and parked_data['data'][0] not in [None,'']: 
                        value_map[f"od_or_limit_null_maker_parked_data"] = json.dumps({template_uuid : [parked_data]})
                        value_map[f"od_or_limit_null_maker_status"] = True
                        value_map[f'{template_type}_simulated'] = True
            except Exception as e:
                print(f"Exception while performing simulation for od_or_limit null for statement id {statement_id} : {e}")


    value_map['address_null'] = portalData.get('address') is None
    value_map['address_null_maker_status'] = is_statement_invalid
    value_map['address_null_maker_parked_data'] = None
    value_map['address_null_checker_status'] = False
    value_map['address_null_ignore_case'] = False

    if value_map['address_null']:
        if is_statement_invalid:
            value_map['address_null_maker_parked_data'] = json.dumps({invalid_template_id:["INVALID_STATEMENT"]})
        else:
            try:
                parked_data, template_uuid = await perform_simulation_ingestion(statement_id, bank_name, 'address_bbox')
                print('INGESTION :: performed simulation address_bbox')
                if parked_data and template_uuid and template_type:
                    if len(parked_data['data'])!=0 and parked_data['data'][0] not in [None,'']: 
                        value_map[f"address_null_maker_parked_data"] = json.dumps({template_uuid : [parked_data]})
                        value_map[f"address_null_maker_status"] = True
            except Exception as e:
                print(f"Exception while performing simulation for od_or_limit null for statement id {statement_id} : {e}")
                print(traceback.format_exc())

    #ignoring random cases by using logo hashes 
    value_map["pdf_ignore_reason"] = ""

    ignore_hashes = await get_ignore_logo_hash()
    hashes_list = list(hash_dict.values())
    value_map['organization_id'] = organization_id
    value_map['client_id'] = client_id
    value_map['org_name'] = org_name
    
    if hashes_list != None and len(hashes_list) > 0:
        for hash in hashes_list:
            if hash in ignore_hashes:
                value_map["logo_null_ignore_case"] = True
                value_map["date_null_ignore_case"] = True
                value_map["account_null_ignore_case"] = True
                value_map["name_null_ignore_case"] = True
                value_map["pdf_ignore_reason"] = "auto_logo_hash " + str(hash)

    # bank connect quality query to insert this data
    try:
        null_identity_simulate_data = await simulate_ingest_identity_null(statement_id, portalData, doc)
        if null_identity_simulate_data.get('name_null_ignore_regex_id'):
            value_map['name_null_ignore_case'] = True
        if null_identity_simulate_data.get('account_null_ignore_regex_id'):
            value_map['account_null_ignore_case'] = True
        if null_identity_simulate_data.get('date_null_ignore_regex_id'):
            value_map['date_null_ignore_case'] = True
        if null_identity_simulate_data.get('ac_category_null_ignore_regex_id'):
            value_map['ac_category_null_ignore_case'] = True
        if null_identity_simulate_data.get('ifsc_null_ignore_regex_id'):
            value_map['ifsc_null_ignore_case'] = True
        if null_identity_simulate_data.get('micr_null_ignore_regex_id'):
            value_map['micr_null_ignore_case'] = True
        if null_identity_simulate_data.get('address_null_ignore_regex_id'):
            value_map['address_null_ignore_case'] = True
        
        qualityInsertQuery = """
                                Insert into statement_quality (
                                    statement_id, bank_name, pdf_password,
                                    name_null, name_null_maker_status, name_null_maker_parked_data, name_null_checker_status, name_null_ignore_case,
                                    account_null, account_null_maker_status, account_null_maker_parked_data, account_null_checker_status, account_null_ignore_case,
                                    date_null, date_null_maker_status, date_null_maker_parked_data, date_null_checker_status, date_null_ignore_case,
                                    logo_null, logo_null_maker_status, logo_null_maker_parked_data, logo_null_checker_status, logo_null_ignore_case, 
                                    pdf_ignore_reason, ac_category_null, ac_category_null_maker_status, ac_category_null_maker_parked_data, ac_category_null_checker_status, ac_category_null_ignore_case,
                                    ifsc_null, ifsc_null_maker_status, ifsc_null_maker_parked_data, ifsc_null_checker_status, ifsc_null_ignore_case, 
                                    micr_null, micr_null_maker_status, micr_null_maker_parked_data, micr_null_checker_status, micr_null_ignore_case,
                                    od_or_limit_null, od_or_limit_null_maker_status, od_or_limit_null_maker_parked_data, od_or_limit_null_checker_status, od_or_limit_null_ignore_case,
                                    is_od_account_detected, is_credit_limit_detected, is_od_limit_detected, is_od_account_bbox_simulated, limit_bbox_simulated, od_limit_bbox_simulated,
                                    ac_category_ingest_keyword, ifsc_ingest_keyword, micr_ingest_keyword,
                                    address_null, address_null_maker_status, address_null_maker_parked_data, address_null_checker_status, address_null_ignore_case,
                                    client_id, organization_id, org_name
                                ) VALUES (
                                    %(statement_id)s, %(bank_name)s, %(pdf_password)s,
                                    %(name_null)s, %(name_null_maker_status)s, %(name_null_maker_parked_data)s, %(name_null_checker_status)s, %(name_null_ignore_case)s,
                                    %(account_null)s, %(account_null_maker_status)s, %(account_null_maker_parked_data)s, %(account_null_checker_status)s, %(account_null_ignore_case)s,
                                    %(date_null)s, %(date_null_maker_status)s, %(date_null_maker_parked_data)s, %(date_null_checker_status)s, %(date_null_ignore_case)s,
                                    %(logo_null)s, %(logo_null_maker_status)s, %(logo_null_maker_parked_data)s, %(logo_null_checker_status)s, %(logo_null_ignore_case)s,
                                    %(pdf_ignore_reason)s, %(ac_category_null)s, %(ac_category_null_maker_status)s, %(ac_category_null_maker_parked_data)s, %(ac_category_null_checker_status)s, %(ac_category_null_ignore_case)s,
                                    %(ifsc_null)s, %(ifsc_null_maker_status)s, %(ifsc_null_maker_parked_data)s, %(ifsc_null_checker_status)s, %(ifsc_null_ignore_case)s, 
                                    %(micr_null)s, %(micr_null_maker_status)s, %(micr_null_maker_parked_data)s, %(micr_null_checker_status)s, %(micr_null_ignore_case)s,
                                    %(od_or_limit_null)s, %(od_or_limit_null_maker_status)s, %(od_or_limit_null_maker_parked_data)s, %(od_or_limit_null_checker_status)s, %(od_or_limit_null_ignore_case)s,
                                    %(is_od_account_detected)s, %(is_credit_limit_detected)s, %(is_od_limit_detected)s, %(is_od_account_bbox_simulated)s, %(limit_bbox_simulated)s, %(od_limit_bbox_simulated)s,
                                    %(ac_category_ingest_keyword)s, %(ifsc_ingest_keyword)s, %(micr_ingest_keyword)s,
                                    %(address_null)s, %(address_null_maker_status)s, %(address_null_maker_parked_data)s, %(address_null_checker_status)s, %(address_null_ignore_case)s,
                                    %(client_id)s, %(organization_id)s, %(org_name)s
                                );
                            """
        
        insertData = DBConnection(QUALITY_DATABASE_NAME).execute_query(
                                query=qualityInsertQuery,
                                values=value_map
                            )
        
        short_entity_id = portalData.get("entity_id")
        entity_id_query = """
                                SELECT entity_id from bank_connect_entity where id=%(short_entity_id)s
                            """
        portalEntityData = DBConnection(PORTAL_DATABASE_NAME).execute_query(
                                        query=entity_id_query, 
                                        values={
                                            "short_entity_id": short_entity_id
                                        }
                                    )
        portalEntityData = dict(portalEntityData[0])
        entity_id = portalEntityData.get("entity_id")
        account_id = portalData.get("account_id")
        
        await account_ingestion(entity_id, account_id, statement_id, bank_name)

        print(f"INGEST :: statement_id {statement_id}, completed after {datetime.now()-start_time}")
        redis_cli.set(f'{STATEMENT_TYPE}_ingest_{statement_id}', statement_id, ex=172800)
        
        identity_mismatch_data = await trigger_identity_mismatch_on_ingestion(portalData, 'IN')
        identity_mismatch_data = str(tuple(identity_mismatch_data))
    except Exception as e:
        print(traceback.format_exc(),e)

    if os.path.exists(temp_file_path):
        os.remove(temp_file_path)
    doc.close()
    return None

@quality_router.post("/get_extracted_data", tags=['quality'])
async def get_extracted_data(request: GetExtractedData, response: Response ,is_ticket: Optional[bool]=False, user=Depends(get_current_user)):
    statement_id = request.statement_id
    template_type = request.template_type
    template_json = request.template_json
    bank_name = request.bank_name
    page_number = request.page_number

    if template_type in ['is_od_account_bbox','limit_bbox','od_limit_bbox'] and not is_ticket:
        obj = await get_details_add_od_or_limit(statement_id)
        if not obj.get(f'can_add_{template_type}',True):
            response.status_code = status.HTTP_400_BAD_REQUEST
            return {"message": "This case has been solved already"}
    
    if template_type == 'account_category_bbox':
        if not isinstance(template_json,dict) or ("bbox" not in template_json) or ("regex" not in template_json) or len(template_json.get('bbox',[]))!=4:
            response.status_code = status.HTTP_400_BAD_REQUEST
            return {"message": "invalid template_json"}

    if template_type in ['logo_less_bbox','invalid_text_bbox']:
        if isinstance(bank_name,str) and 'fi' in bank_name.split('$'):
            bank_name='federal'
        if not is_logo_less_bbox_template_valid(template_json):
            response.status_code = status.HTTP_400_BAD_REQUEST
            return {"message": "invalid template_json"}

    bucket = PDF_BUCKET
    key = f"pdf/{statement_id}_{bank_name}.pdf"

    # first check if this key exists in this bucket
    try:
        pdf_bucket_response = s3.get_object(Bucket=bucket, Key=key)
    except Exception as e:
        print(e)
        response.status_code = status.HTTP_404_NOT_FOUND
        return {"message": "key not found"}
    
    if template_type in ['logo_less_bbox','invalid_text_bbox']:
        return {'data':check_all_text_logo_less_bbox(template_json,pdf_bucket_response,statement_id)}
    
    # get the data from the template handler lambda for this 
    invocation_payload = {
        "bucket": bucket,
        "key": key,
        "template": template_json,
        "template_type": template_type,
        "new_flow": True,
        "bank" : bank_name,
        "page_num": page_number
    }

    if template_type == 'is_od_account_bbox':
        od_account_keywords = await get_or_put_od_keywords()
        invocation_payload['od_keywords']=od_account_keywords

    if(template_type=='account_category_bbox'):
        # added_mapping is optional
        # template_json:{
        #     "bbox":[],
        #     "regex":"",
        #     "added_mapping":{"x":"y"}
        # }
        added_mapping = template_json.get('added_mapping',{})
        template_json.pop('added_mapping',None)
        invocation_payload['template']=template_json
        if added_mapping not in [None,{}]:
            invocation_payload['mapping']=added_mapping
        else:
            invocation_payload['mapping']=await get_mapping_acc_category(bank_name)

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

@quality_router.get("/get_all_inconsistent_data", tags=['quality'])
async def get_inconsistent_cases(from_date: str, to_date: str, response: Response, user=Depends(get_current_user)):
    data = await get_inconsistency_cases(from_date, to_date)
    if "message" in data:
        response.status_code = status.HTTP_400_BAD_REQUEST
    return data

@quality_router.get("/detailed_inconsistent_info", tags=['quality'])
async def detailed_inconsistent(entity_id: str, account_id: str, response: Response, user=Depends(get_current_user)):
    data, statement_list = await get_inconsistent_details(entity_id, account_id)
    if data.get("message")=="does not exist":
        response.status_code = status.HTTP_404_NOT_FOUND
    data["statements"] = []
    
    portal_query = """
                SELECT statement_id, pdf_password, from_date, to_date, bank_name FROM bank_connect_statement
                WHERE statement_id = :statement_id
            """
    for statement_id in statement_list:
        portal_data = await portal_db.fetch_one(portal_query, {
                            "statement_id": statement_id
                        })
        portal_data = dict(portal_data)

        # also get the presigned url for this statement
        portal_data['presigned_url'] = create_viewable_presigned_url(statement_id, portal_data['bank_name'])
        
        data["statements"].append(portal_data)

    return data

@quality_router.post("/ignore_account", tags=['quality'])
async def ignore_account(request: IgnoreAccount, response: Response, user=Depends(get_current_user)):
    entity_id = request.entity_id
    account_id = request.account_id
    ignore_bool = request.ignore_bool
    ignore_field = request.ignore_field
    ignore_reason = request.ignore_reason

    if ignore_field not in ['inconsistent']:
        response.status_code = status.HTTP_400_BAD_REQUEST
        return {"message": "invalid null type for account quality"}
    
    update_query = f"""
                        UPDATE account_quality SET {ignore_field}_ignore_case = :ignore_bool, {ignore_field}_remarks = :ignore_reason
                        WHERE entity_id=:entity_id AND account_id = :account_id
                    """
    
    values = {
        "ignore_bool": ignore_bool,
        "entity_id": entity_id,
        "account_id": account_id,
        "ignore_reason": ignore_reason
    }

    await quality_database.execute(query=update_query, values=values)

    return {
        "message": "success",
        "entity_id": entity_id,
        "account_id": account_id
    }


@quality_router.get("/check_inconsistency")
async def check_inconsistency(request: InconsistencyCheck, response: Response, user=Depends(get_current_user)):
    """
    Checks inconsistency in the statement and returns the inconsistent hash after performing optimizations
    """
    
    statement_id = request.statement_id
    bank_name = request.bank_name
    attempt_type = request.attempt_type

    
    transactions, _ = get_transactions_for_statement(
                        statement_id=statement_id
                    )
    
    transactions, _, _, _ = optimise_transaction_type(
                        transactions_dict=transactions, 
                        bank=bank_name, 
                        statement_attempt_type=attempt_type
                    )

    transaction_hash = transaction_balance_check(
                            transaction_list=transactions,
                            bank=bank_name,
                            statement_attempt_type=attempt_type
                        )
    return {
        "message": "success",
        "inconsistent_hash": transaction_hash
    }