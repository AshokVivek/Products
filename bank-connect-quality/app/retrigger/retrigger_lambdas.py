from fastapi import APIRouter, Depends, Response, status
import json
from app.dependencies import get_current_user
from app.retrigger.models import *
from app.retrigger.models import InvokeUpdateState
from app.database_utils import portal_db, quality_database
from app.retrigger.ddb_status import update_ddb_status
from app.conf import *
from app.conf import ACCOUNT_TABLE, ENRICHMENTS_TABLE
from app.template_solutioning.redis import redis_cli
from app.utils import trigger_update_state
from boto3.dynamodb.conditions import Key
from app.ddb_utils import collect_results
import traceback
import time

invocation_router = APIRouter()

pdf_bucket = PDF_BUCKET

@invocation_router.get("/")
def health():
    return {"message": "i'm up"}

async def get_templates_from_rds(required_templates, bank):
    # check if templates are cached
    redis_key = f"templates_{bank}"
    templates = redis_cli.get(redis_key)
    if templates!=None:
        print("templates are present in bank, serving from here")
        templates = json.loads(templates)
        return templates
    templates = {}
    for template_type in required_templates:
        result = await get_template_for_bank(bank, template_type)

        templates[template_type] = result
        if bank in ['federal', 'india_post'] and template_type == 'trans_bbox':
            tmp_bank = bank + "1"
            result = await get_template_for_bank(tmp_bank, template_type)
            templates[template_type].extend(result)

    redis_cli.set(redis_key, json.dumps(templates))
    return templates

async def get_template_for_bank(bank, template_type):
    
    all_templates_query = """
                            SELECT template_uuid, template_type, template_json, bank_name, is_active, priority
                            FROM bank_connect_fsmlibtemplates
                            WHERE bank_name=:bank_name
                            AND template_type=:template_type
                            AND is_active=true
                            ORDER BY priority
                        """
    all_templates = await portal_db.fetch_all( 
                                all_templates_query, values={
                                    "bank_name": bank, 
                                    "template_type": template_type
                                }
                            )
        
    result = []

    for i in range(len(all_templates)):
        dict_template = dict(all_templates[i])
        result.append(dict_template["template_json"])
        result[-1]=json.loads(result[-1])
        result[-1]["uuid"] = dict_template.get("template_uuid", None)
    
    return result

async def get_account_id_and_delete_report(statement_id, account_id="", entity_id="", invoked_by = ''):
    if not account_id:
        portal_query='select account_id from bank_connect_statement where statement_id=:statement_id'
        portal_data = await portal_db.fetch_one(query=portal_query, values={'statement_id': statement_id})
        account_id = None
        if portal_data!=None:
            portal_data=dict(portal_data)
            account_id = portal_data.get('account_id', None)
    
    if invoked_by == 'transactions' and entity_id!='':
        try:
            response = s3.list_objects_v2(
                Bucket = ENRICHMENTS_BUCKET,
                Prefix = f'cache/entity_{entity_id}'
            )
            
            response_content = response['Contents']
            for response_obj in response_content:
                s3_resource.Object(ENRICHMENTS_BUCKET, response_obj.get('Key')).delete()
        except Exception as e:
            print(f"Could not delete execell file --> {e}")
    
    if account_id != None:
        try:
            file_name = 'account_report_{}.xlsx'.format(account_id)
            s3_resource.Object(REPORT_BUCKET, file_name).delete()
        except Exception as e:
            print(f"Could not delete execell file --> {e}")

@invocation_router.post("/invoke_identity", tags=['invocation_lambdas'])
async def invoke_identity(request: InvokeLambda, response: Response, user= Depends(get_current_user)):
    key = f"pdf/{request.key}"
    preshared_names = request.preshared_names

    try:
        pdf_bucket_response = s3.get_object(Bucket=pdf_bucket, Key=key)
    except Exception as e:
        print(e)
        response.status_code=404
        return {"message": "file not found"}
    
    response_metadata = pdf_bucket_response.get('Metadata')
    statement_id = response_metadata.get('statement_id')
    bank = response_metadata.get('bank_name')
    password = response_metadata.get('pdf_password')
    entity_id = response_metadata.get('entity_id')

    identity_query = f"""select is_active from bank_connect_statement where statement_id = '{statement_id}'"""
    identity_data = await portal_db.fetch_one(query=identity_query)
    is_active = False
    if identity_data!=None:
        identity_data = dict(identity_data)
        is_active = identity_data.get('is_active')
    
    if not is_active:
        return {"message": "Failed as statement is deactivated"}

    await get_account_id_and_delete_report(statement_id)

    required_templates = ["micr_bbox", "account_category_bbox", "name_bbox", "ifsc_bbox", "limit_bbox", "name_quality", "account_category_mapping", "address_bbox", "accnt_bbox", "date_bbox", "is_od_account_bbox", "od_limit_bbox", "currency_bbox", "opening_date_bbox", "opening_bal_bbox", "closing_bal_bbox", 'trans_bbox', 'last_page_regex', 'joint_account_holders_regex', 'email_bbox', 'phone_number_bbox', 'pan_number_bbox']
    identity_templates = await get_templates_from_rds(required_templates, bank)

    lambda_payload = {
        "bucket": pdf_bucket,
        "key": key,
        "preshared_names": preshared_names,
        "template": identity_templates,
        "re_extraction": True
    }

    try:
        lambda_response = lambda_client.invoke(
                    FunctionName = IDENTITY_LAMBDA_FUNCTION_NAME, 
                    Payload = json.dumps(lambda_payload), 
                    InvocationType = "RequestResponse"
                )
        query = """
            INSERT INTO retrigger_logs (
                retriggered_by,
                entity_id,
                statement_id,
                retrigger_type
                )
            VALUES (
                :retriggered_by,
                :entity_id,
                :statement_id,
                :retrigger_type
            )
        """
        await quality_database.execute(query=query, values={
            "retriggered_by": user.username,
            "entity_id": entity_id,
            "statement_id": statement_id,
            "retrigger_type": "Identity"
        })
    except Exception as e:
        print(e)
        response.status_code=500
        return {"message": "something went wrong"}
    
    return {"message": "success", "logs": "success"}


@invocation_router.post("/invoke_update_state", tags=['invocation_lambdas'])
async def invoke_update_state(request: InvokeUpdateState, response: Response, user= Depends(get_current_user)):
    bank_name = request.bank_name
    statement_id = request.statement_id
    entity_id = request.entity_id
    attempt_type = request.attempt_type

    key = f'pdf/{statement_id}_{bank_name}.pdf'
    if attempt_type=='aa':
        key = f'aa/{statement_id}_{bank_name}.json'

    try:
        pdf_bucket_response = s3.get_object(Bucket=pdf_bucket, Key=key)
    except Exception as e:
        print(e)
        response.status_code=404
        return {"message": "file not found"}
    
    account_id = None
    portal_query='select account_id from bank_connect_statement where statement_id=:statement_id'
    portal_data = await portal_db.fetch_one(query=portal_query, values={'statement_id': statement_id})
    account_id = None
    if portal_data!=None:
        portal_data=dict(portal_data)
        account_id = portal_data.get('account_id', None)

    await get_account_id_and_delete_report(statement_id, entity_id=entity_id, invoked_by='transactions', account_id=account_id)

    ddb_update_resp = await update_ddb_status(statement_id, "processing", "processing", None, None, None, response, to_reject_statement=False, message=None, update_message=True)
    if ddb_update_resp != {"message": "status updated", "statement_id": statement_id}:
        return ddb_update_resp
    
    quality_query = f"""
            select 
                org.id as organization_id, 
                org.client_id as client_id, 
                org.name as org_name,
                e.entity_id as entity_id,
                e.is_processing_requested as is_processing_requested
            from 
                bank_connect_entity e, 
                finbox_dashboard_organization org 
            where 
                org.id=e.organization_id and 
                e.entity_id='{entity_id}' 
            limit 1
        """
    data = await portal_db.fetch_one(query=quality_query)
    data = dict(data)
    entity_id = data.get("entity_id")
    is_processing_requested = data.get("is_processing_requested", False)
    if entity_id and is_processing_requested is True:
        # This is required, when there is no entry in enrihments table,
        # Since at upload time insights API wasn't part of api_subscriptions list.
        ENRICHMENTS_TABLE.update_item(
            Key={'entity_id': entity_id},
            UpdateExpression="SET is_processing_requested = :p",
            ExpressionAttributeValues={
                ':p': is_processing_requested
            }
        )

    lambda_payload = {
        "statement_id" : statement_id,
        "entity_id" : entity_id,
        'org_metadata': {
            'organization_id': data.get('organization_id'),
            'client_id': data.get('client_id'),
            'org_name': data.get('org_name')
        }
    }

    try:
        await update_retrigger_status_in_account(account_id, entity_id)
        lambda_response = await trigger_update_state(lambda_payload, user.username,entity_id,statement_id)
    except Exception as e:
        response.status_code = 500
        print(traceback.format_exc())
        return {"message":"Something went wrong", "logs":str(lambda_response)}
        
    return {"message": "success", "logs": str(lambda_response)}

@invocation_router.post("/invoke_analyze_pdf", tags=['invocation_lambdas'])
async def invoke_analyze_pdf(request: InvokeLambda, response: Response, user= Depends(get_current_user)):
    key = f"pdf/{request.key}"

    try:
        pdf_bucket_response = s3.get_object(Bucket=pdf_bucket, Key=key)
    except Exception as e:
        print(e)
        response.status_code=404
        return {"message": "file not found"}
    
    response_metadata = pdf_bucket_response.get('Metadata')
    statement_id = response_metadata.get('statement_id')
    bank = response_metadata.get('bank_name')
    password = response_metadata.get('pdf_password')
    entity_id = response_metadata.get('entity_id')
    # enrichment_regexes = await get_enrichment_regexes(bank, 'IN')

    identity_query = f"""select i.account_number, 
                                i.name, 
                                s.account_id, 
                                s.attempt_type,
                                e.entity_id,
                                e.link_id,
                                o.id as org_id,
                                o.name as org_name
                                from bank_connect_statement s, bank_connect_identity i, bank_connect_entity e, finbox_dashboard_organization o
                                where s.entity_id=e.id
                                and e.organization_id=o.id
                                and i.statement_id=s.id 
                                and s.statement_id='{statement_id}'"""
    identity_data = await portal_db.fetch_one(query=identity_query)
    account_id, name, account_number, entity_id = None, None, None, None
    statement_meta_data_for_warehousing = dict()

    if identity_data!=None:
        identity_data = dict(identity_data)
        account_id = identity_data.get('account_id', None)
        name = identity_data.get('name', None)
        account_number = identity_data.get('account_number', None)
        entity_id = identity_data.get('entity_id', None)
        statement_meta_data_for_warehousing["link_id"] = identity_data.get("link_id", None)
        statement_meta_data_for_warehousing["org_id"] = identity_data.get("org_id", None)
        statement_meta_data_for_warehousing["org_name"] = identity_data.get("org_name", None)
        statement_meta_data_for_warehousing["attempt_type"] = identity_data.get("attempt_type", "pdf")
    else:
        response.status_code=404
        return {"message": "Could not find identity for this statement"}

    session_query = f"""select s.from_date, s.to_date, o.session_flow from bank_connect_session s, bank_connect_entity e, bank_connect_orgbankconfig o where e.organization_id=o.organization_id and e.entity_id::uuid=s.session_id and e.entity_id='{entity_id}'"""
    session_data = await portal_db.fetch_one(query=session_query)
    from_date, to_date, session_flow = None, None, False
    
    if session_data!=None:
        session_data = dict(session_data)
        from_date = session_data.get('from_date', None)
        to_date = session_data.get('to_date', None)
        session_flow = session_data.get('session_flow', False)
    
    extract_multiple_accounts = False
    extract_multiple_accounts_query = f"""select o.extract_multiple_accounts from bank_connect_entity e, bank_connect_orgbankconfig o where e.organization_id=o.organization_id and e.entity_id='{entity_id}'"""
    extract_multiple_accounts_data = await portal_db.fetch_one(query=extract_multiple_accounts_query)
    if extract_multiple_accounts_data:
        extract_multiple_accounts = extract_multiple_accounts_data.get('extract_multiple_accounts', False)
    
    session_date_range = {
        "from_date": None,
        "to_date": None
    }
    
    await get_account_id_and_delete_report(statement_id, account_id, entity_id=entity_id, invoked_by='transactions')
    if session_flow:
        session_date_range["from_date"] = from_date
        session_date_range["to_date"] = to_date
        statement_meta_data_for_warehousing["session_id"] = entity_id
    
    templates = await get_templates_from_rds(['trans_bbox', 'last_page_regex', 'account_delimiter_regex'], bank)

    #handling case when we just want to extarct transactions from a particular statement_id
    particular_template_uuid = request.template_uuid
    trans_bboxes = templates.get('trans_bbox', [])
    if particular_template_uuid is not None and particular_template_uuid != '':
        new_trans_bboxes = []
        for tmp_trans_bbox in trans_bboxes:
            if tmp_trans_bbox['uuid'] == particular_template_uuid:
                new_trans_bboxes.append(tmp_trans_bbox)
        trans_bboxes = new_trans_bboxes
        print("new trans_bboxes ", trans_bboxes)

    lambda_payload = {
        "bucket" : pdf_bucket,
        "key" : key,
        "name" : name,
        "account_number" : account_number,
        "trans_bbox" : trans_bboxes,
        "last_page_regex" : templates.get('last_page_regex', []),
        "account_delimiter_regex": templates.get('account_delimiter_regex', []),
        "session_date_range": session_date_range,
        "account_id": account_id,
        "country": COUNTRY,
        "extract_multiple_accounts": extract_multiple_accounts,
        "statement_meta_data_for_warehousing": statement_meta_data_for_warehousing
    }
    STATEMENT_TABLE.update_item(
        Key={"statement_id": statement_id},
        UpdateExpression = "set last_page_index = :l, updated_at = :u",
        ExpressionAttributeValues={
            ':l' : -1,
            ':u': time.time_ns()
        }
    )
    try:
        await update_retrigger_status_in_account(account_id, entity_id)
        lambda_response = lambda_client.invoke(
                        FunctionName = ANALYZE_PDF_LAMBDA_FUNCTION_NAME, 
                        Payload = json.dumps(lambda_payload), 
                        InvocationType = "RequestResponse"
                    )
        query = """
            INSERT INTO retrigger_logs (
                retriggered_by,
                entity_id,
                statement_id,
                template_uuid,
                retrigger_type
                )
            VALUES (
                :retriggered_by,
                :entity_id,
                :statement_id,
                :template_uuid,
                :retrigger_type
            )
        """
        await quality_database.execute(query=query, values={
            "retriggered_by": user.username,
            "entity_id": entity_id,
            "statement_id": statement_id,
            "template_uuid": particular_template_uuid,
            "retrigger_type": "Transaction"
        })
    except Exception as e:
        print(e)
        response.status_code=500
        return {"message": "something went wrong"}

    return {"message": "success", "logs": lambda_response}

@invocation_router.post('/invoke_analyze_aa', tags=['invocation_lambdas'])
async def invoke_analyze_aa(request: InvokeAnalyzeAA, response: Response, user=Depends(get_current_user)):
    statement_id = request.statement_id
    bank_name = request.bank_name

    file_key = 'aa/{}_{}.json'.format(statement_id, bank_name)

    try:
        pdf_bucket_response = s3.get_object(Bucket=pdf_bucket, Key=file_key)
    except Exception as e:
        print(e)
        response.status_code=404
        return {"message": "file not found"}
    
    portal_query = f"""
                    select e.entity_id as entity_id,
                    e.link_id as link_id,
                    i.name as name, 
                    s.account_id as account_id,
                    s.attempt_type,
                    o.id as org_id,
                    o.name as org_name
                    from bank_connect_identity i, bank_connect_statement s, bank_connect_entity e, finbox_dashboard_organization o
                    where s.entity_id=e.id 
                    and i.statement_id=s.id 
                    and e.organization_id=o.id
                    and s.statement_id=:statement_id
                """
    
    portal_data = await portal_db.fetch_one(query=portal_query, values={'statement_id':statement_id})
    if portal_data==None:
        response.status_code=404
        return {"message": "Data not found"}
    
    portal_data = dict(portal_data)
    name = portal_data.get('name', None)
    entity_id = portal_data.get('entity_id', None)
    account_id = portal_data.get('account_id', None)

    statement_meta_data_for_warehousing = {
        "link_id": portal_data.get("link_id", None),
        "org_id": portal_data.get("org_id", None),
        "org_name": portal_data.get("org_name", None),
        "attempt_type": portal_data.get("attempt_type", "aa")
    }

    if name==None or account_id==None or entity_id==None:
        response.status_code=404
        return {"message": "Name or entity_id or account_id not found"}
    

    session_query = f"""select s.from_date, s.to_date, o.session_flow from bank_connect_session s, bank_connect_entity e, bank_connect_orgbankconfig o where e.organization_id=o.organization_id and e.entity_id::uuid=s.session_id and e.entity_id='{entity_id}'"""
    session_data = await portal_db.fetch_one(query=session_query)
    from_date, to_date, session_flow = None, None, False
    if session_data!=None:
        session_data = dict(session_data)
        from_date = session_data.get('from_date', None)
        to_date = session_data.get('to_date', None)
        session_flow = session_data.get('session_flow', False)
    
    session_date_range = {
        "from_date": None,
        "to_date": None
    }
    if session_flow:
        session_date_range["from_date"] = from_date
        session_date_range["to_date"] = to_date
        statement_meta_data_for_warehousing["session_id"] = entity_id
    
    await get_account_id_and_delete_report(statement_id, account_id, entity_id=entity_id, invoked_by='transactions')

    payload = {
        "statement_id": statement_id,
        "entity_id": entity_id,
        "name": name,
        "bank_name": bank_name,
        "aa_data_file_key": file_key,
        "bucket_name": pdf_bucket,
        "country": COUNTRY,
        "session_date_range": session_date_range,
        "statement_meta_data_for_warehousing": statement_meta_data_for_warehousing
    }

    try:
        await update_retrigger_status_in_account(account_id, entity_id)
        lambda_response = lambda_client.invoke(
                        FunctionName = ANALYZE_TRANSACTIONS_LAMBDA_FINVU_AA, 
                        Payload = json.dumps(payload), 
                        InvocationType = "RequestResponse"
                    )
        query = """
            INSERT INTO retrigger_logs (
                retriggered_by,
                entity_id,
                statement_id,
                retrigger_type
                )
            VALUES (
                :retriggered_by,
                :entity_id,
                :statement_id,
                :retrigger_type
            )
        """
        await quality_database.execute(query=query, values={
            "retriggered_by": user.username,
            "entity_id": entity_id,
            "statement_id": statement_id,
            "retrigger_type": "AA Transaction"
        })
    except Exception as e:
        print(e)
        response.status_code=404
        return {"message": "something went wrong, while triggering lambda"}

    return {"message": "success", "logs": lambda_response}

async def get_required_enrichment_regex(regex_type, bank_name, country):
    if regex_type == 'merchant_category':
        portab_query =f"""select * from bank_connect_fsmlibmerchantcategory where country='{country}' and is_active=true order by priority desc"""
        portal_data = await portal_db.fetch_all(query=portab_query)
        data_dict = {}
        for portal_item in portal_data:
            portal_item = dict(portal_item)
            portal_item['tag_list'] = json.loads(portal_item['tag_list'])
            data_dict[portal_item.get('merchant_category')] = portal_item.get('tag_list')

        return data_dict
    elif regex_type == 'transaction_channel':
        transaction_channel = {
            'debit': {},
            'credit': {}
        }

        available_bank_query = """select distinct(bank_name) from bank_connect_fsmlibtransactionchannels"""
        available_banks = await portal_db.fetch_all(available_bank_query)
        banks = []
        for bank_item in available_banks:
            bank_item = dict(bank_item)
            banks.append(bank_item.get('bank_name', None))
        
        if bank_name not in banks:
            bank_name='generic'
        
        debit_data_query = f"""select * from bank_connect_fsmlibtransactionchannels where bank_name='{bank_name}' and country='{country}' and transaction_type='debit' and is_active=true order by priority desc"""
        debit_data = await portal_db.fetch_all(query=debit_data_query)
        debit_data_list = []
        for debit_data_item in debit_data:
            debit_data_item = dict(debit_data_item)
            channel = debit_data_item.get('transaction_channel')
            debit_data_item['regex_list'] = json.loads(debit_data_item.get('regex_list'))
            transaction_channel['debit'][channel] = debit_data_item['regex_list']
        
        credit_data_query = f"""select * from bank_connect_fsmlibtransactionchannels where bank_name='{bank_name}' and country='{country}' and transaction_type='credit' and is_active=true order by priority desc"""
        credit_data = await portal_db.fetch_all(query=credit_data_query)
        credit_data_list = []
        for credit_data_item in credit_data:
            credit_data_item = dict(credit_data_item)
            channel = credit_data_item.get('transaction_channel')
            credit_data_item['regex_list'] = json.loads(credit_data_item.get('regex_list'))
            transaction_channel['credit'][channel] = credit_data_item['regex_list']

        return transaction_channel
    elif regex_type == 'unclean_merchant':
        unclean_merchant = {
            'debit': {},
            'credit': {}
        }
        available_bank_query = """select distinct(bank_name) from bank_connect_fsmlibuncleanmerchants"""
        available_banks = await portal_db.fetch_all(available_bank_query)
        banks = []
        for bank_item in available_banks:
            bank_item = dict(bank_item)
            banks.append(bank_item.get('bank_name', None))
        
        if bank_name not in banks:
            bank_name='generic'

        debit_data_query = f"""select * from bank_connect_fsmlibuncleanmerchants where bank_name='{bank_name}' and country='{country}' and transaction_type='debit' and is_active=true"""
        debit_data = await portal_db.fetch_all(query=debit_data_query)
        debit_data_list = []
        for debit_data_item in debit_data:
            debit_data_item = dict(debit_data_item)
            unclean_merchant['debit'] = json.loads(debit_data_item.get('regex_list'))
        
        credit_data_query = f"""select * from bank_connect_fsmlibuncleanmerchants where bank_name='{bank_name}' and country='{country}' and transaction_type='credit' and is_active=true"""
        credit_data = await portal_db.fetch_all(query=credit_data_query)
        credit_data_list = []
        for credit_data_item in credit_data:
            credit_data_item = dict(credit_data_item)
            unclean_merchant['credit'] = json.loads(credit_data_item.get('regex_list'))
        
        return unclean_merchant
    elif regex_type == 'lender_list':
        lender_list = {
            'lenders':[]
        }
        portab_query =f"""select * from bank_connect_fsmlibgeneraldata where country='{country}' and tag='lender_list' and type='lender_list'"""
        portal_data = await portal_db.fetch_all(query=portab_query)
        for portal_item in portal_data:
            portal_item = dict(portal_item)
            lender_list['lenders'] = json.loads(portal_item['regex_list'])
            return lender_list

        return lender_list
    elif regex_type == 'description':
        description = {}
        portab_query =f"""select * from bank_connect_fsmlibgeneraldata where country='{country}' and tag='description'"""
        portal_data = await portal_db.fetch_all(query=portab_query)
        for portal_item in portal_data:
            portal_item = dict(portal_item)
            description_type = portal_item.get('type')
            description[description_type] = json.loads(portal_item['regex_list'])
        return description
    return []

async def get_enrichment_regexes(bank_name, country):
    enrichment_regexes = {}
    required_regexes = ['merchant_category','transaction_channel','unclean_merchant','lender_list','description']
    for required_regex_type in required_regexes:
        enrichment_regexes[required_regex_type] = await get_required_enrichment_regex(required_regex_type, bank_name, country)
    
    return enrichment_regexes


async def update_retrigger_status_in_account(account_id, entity_id):
    qp = {
        'KeyConditionExpression': Key('entity_id').eq(
        entity_id) & Key('account_id').eq(account_id),
        'ConsistentRead': True
    }

    account = collect_results(ACCOUNT_TABLE.query, qp)
    if len(account) > 0:
        account = account[0]
        webhook_metadata = account.get('webhook_metadata', dict())
        webhook_metadata.update({
            'is_retrigger': True
        })
        
        ACCOUNT_TABLE.update_item(
            Key={
                'entity_id': entity_id,
                'account_id': account_id
            },
            UpdateExpression="SET webhook_metadata = :meta",
            ExpressionAttributeValues={
                ':meta': webhook_metadata
            }
        )