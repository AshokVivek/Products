from fastapi import APIRouter, Depends, Response, status
from app.template_solutioning.request_models import TestMetadata, AdditionFraudMetadata
from app.dependencies import get_current_user
from app.database_utils import portal_db
from app.conf import lambda_client, METADATA_FRAUDS_FUNCTION, PDF_BUCKET
import json
from concurrent.futures import ThreadPoolExecutor
from app.template_solutioning.dashboard_calls import fb_dashboard_api_update_or_delete_metadata

fraud_metadata_router = APIRouter()

def get_fraud_code_for_type(fraud_type):
    if fraud_type == 'author_fraud':
        return 3
    elif fraud_type == 'date_fraud':
        return 4
    elif fraud_type == "font_and_encryption_fraud":
        return 9
    elif fraud_type == "page_hash_fraud":
        return 10
    elif fraud_type == "identity_name_fraud":
        return 11
    elif fraud_type== "rgb_fraud":
        return 13
    elif fraud_type== "good_author_fraud":
        return 14
    elif fraud_type== "tag_hex_fraud":
        return 15
    elif fraud_type== "flag_000rg_50_fraud":
        return 16
    elif fraud_type== "tag_hex_on_page_cnt_fraud":
        return 17
    elif fraud_type== "TD_cnt_fraud":
        return 18
    elif fraud_type== "TJ_cnt_fraud":
        return 19
    elif fraud_type== "touchup_textedit_fraud":
        return 20
    elif fraud_type== "cnt_of_pagefonts_not_equal_fraud":
        return 21
    elif fraud_type=='good_font_type_size_fraud':
        return 22
    elif fraud_type=='pikepdf_exception':
        return 24
    return None

async def fetch_metadata_for_bank(bank_name):
    portal_query = """
                select * from bank_connect_fsmlibfrauddata where bank_name=:bank_name
                """
    
    portal_data = await portal_db.fetch_all(query = portal_query, values={
        'bank_name': bank_name
    })

    metadata_dict = {
        'good_font_list': [],
        'encryption_algo_list': [],
        'strict_metadata_fraud_list': [],
        'stream_font_list': []
    }

    if portal_data!=None:
        for i in range(len(portal_data)):
            current_data = dict(portal_data[i])
            metadata_type = current_data.get('type')
            if metadata_type in metadata_dict.keys():
                metadata_list = current_data.get('data_list')
                metadata_dict[metadata_type] = json.loads(metadata_list)
    
    return metadata_dict

def check_is_already_present(metadata_dict, new_metadata, metadata_type):
    data_list = metadata_dict[metadata_type]

    if metadata_type in ['good_font_list','encryption_algo_list']:
        if new_metadata in data_list:
            return True
    elif metadata_type in ['strict_metadata_fraud_list']:
        for item in data_list:
            if item.get('pdf_version') == new_metadata.get('pdf_version') and item.get('cleaned_creator') == new_metadata.get('cleaned_creator') and item.get('cleaned_author') == new_metadata.get('cleaned_author') and item.get('cleaned_producer') == new_metadata.get('cleaned_producer'):
                return True
    elif metadata_type in ['stream_font_list']:
        for item in data_list:
            if len(item) == len(new_metadata):
                all_present = True
                for new_meta_item in new_metadata:
                    if new_meta_item not in item:
                        all_present = False
                        break
                
                if all_present:
                    return True
    
    return False

def multithreading_helper(params):
    statement_id = params.pop('statement_id', None)
    response = lambda_client.invoke(
        FunctionName = METADATA_FRAUDS_FUNCTION,
        Payload = json.dumps(params),
        InvocationType='RequestResponse'
    )

    payload = json.loads(response['Payload'].read().decode('utf-8'))
    if payload==None:
        return {
            'statement_id': statement_id,
            'all_fraud_list': None
        }
    payload['statement_id'] = statement_id

    fraud_code_list = []
    for fraud_type in payload.get('all_fraud_list', []):
        fraud_code_list.append(get_fraud_code_for_type(fraud_type))

    
    payload['all_fraud_list'] = fraud_code_list
    return payload

async def get_fraud_list_for_statement_ids_from_prod(statement_ids):
    statement_id_string = ""
    for i in range(len(statement_ids)):
        statement_id_string += f"'{statement_ids[i]}',"
    
    statement_id_string = statement_id_string[:-1]
    portal_query = f"""
                select statement_id, statement_status, fraud_list, bank_name from bank_connect_statement where statement_id in ({statement_id_string})
                """
    
    portal_data = await portal_db.fetch_all(query=portal_query)
    result_dict = {}
    if portal_data!=None:
        for i in range(len(portal_data)):
            portal_data[i] = dict(portal_data[i])
            fraud_list = portal_data[i].get('fraud_list')

            result_dict[portal_data[i].get('statement_id')] = {
                'statement_status': portal_data[i].get('statement_status'),
                'fraud_list': None if fraud_list==None else json.loads(fraud_list),
                'bank_name': portal_data[i].get('bank_name')
            }
    
    return result_dict

def validate_metadata(new_metadata, metadata_type):
    if metadata_type == 'strict_metadata_fraud_list':
        if not isinstance(new_metadata, dict):
            return False
        for key in ['pdf_version', 'cleaned_creator', 'cleaned_author', 'cleaned_producer']:
            if key not in new_metadata.keys():
                return False
    elif metadata_type in ['good_font_list','encryption_algo_list']:
        if not isinstance(new_metadata, str):
            return False
    elif metadata_type == 'stream_font_list':
        if not isinstance(new_metadata, list):
            return False
        for font_item in new_metadata:
            if not isinstance(font_item, str):
                return False
    return True
    
def remove_metadata(data_list, metadata_type, new_metadata):
    if metadata_type in ['good_font_list','encryption_algo_list']:
        if new_metadata in data_list:
            data_list.remove(new_metadata)

    elif metadata_type == 'strict_metadata_fraud_list':
        for i in range(len(data_list)):
            if data_list[i].get('pdf_version') == new_metadata.get('pdf_version') and data_list[i].get('cleaned_creator') == new_metadata.get('cleaned_creator') and data_list[i].get('cleaned_author') == new_metadata.get('cleaned_author') and data_list[i].get('cleaned_producer') == new_metadata.get('cleaned_producer'):
                data_list.pop(i)
                break

    elif metadata_type == 'stream_font_list':
        for i in range(len(data_list)):
            if len(data_list[i]) == len(new_metadata):
                all_present = True
                for new_meta_item in new_metadata:
                    if new_meta_item not in data_list[i]:
                        all_present = False
                        break
                
                if all_present:
                    data_list.pop(i)
                    break
    
    return data_list

@fraud_metadata_router.post('/test_addition')
async def test_metadata_addition(request: TestMetadata, response: Response, user = Depends(get_current_user)):
    bank_name = request.bank_name
    metadata_type = request.metadata_type
    metadata = request.metadata
    statement_list = request.statement_list
    operation = request.operation

    if user.user_type != "superuser":
        response.status_code = status.HTTP_401_UNAUTHORIZED
        return {"message": "not authorised"}

    if len(statement_list)>50:
        response.status_code = status.HTTP_400_BAD_REQUEST
        return {"message": "Too many statement ids passed"}

    if metadata_type not in ['strict_metadata_fraud_list', 'good_font_list', 'encryption_algo_list', 'stream_font_list']:
        response.status_code = status.HTTP_400_BAD_REQUEST
        return {"message": "This metadata type is not supported yet"}
    
    is_new_metadata_valid = validate_metadata(metadata, metadata_type)
    if not is_new_metadata_valid:
        response.status_code = status.HTTP_400_BAD_REQUEST
        return {"message": "Metadata not valid, please check"}

    prod_results = await get_fraud_list_for_statement_ids_from_prod(statement_list)
    if(prod_results==None or len(prod_results)!=len(statement_list)):
        response.status_code = status.HTTP_400_BAD_REQUEST
        return {"message": "All or some data for statement_id is not present"}
    
    metadata_dict_from_prod = await fetch_metadata_for_bank(bank_name)
    is_already_present = check_is_already_present(metadata_dict_from_prod, metadata, metadata_type)
    if operation == 'update':
        if is_already_present:
            response.status_code = status.HTTP_400_BAD_REQUEST
            return {"message": "This metadata is already present"}

        metadata_dict_from_prod[metadata_type].append(metadata)
    elif operation == 'delete':
        if not is_already_present:
            response.status_code = status.HTTP_400_BAD_REQUEST
            return {"message": "Cannot delete, metadata not present"}
        
        metadata_dict_from_prod[metadata_type] = remove_metadata(metadata_dict_from_prod[metadata_type], metadata_type, metadata)
    else:
        response.status_code = status.HTTP_400_BAD_REQUEST
        return {"message": "Not a valid operation"}

    process_list = []
    for statement_id in statement_list:
        statement_bank_name = prod_results.get(statement_id).get('bank_name')
        if statement_bank_name != bank_name:
            response.status_code = status.HTTP_400_BAD_REQUEST
            return {"message": "All Statements should be of the same bank"}

        process_list.append({
            'bucket': PDF_BUCKET,
            'key': f'pdf/{statement_id}_{bank_name}.pdf',
            'attempt_type': 'pdf',
            'is_retrigger': False,
            'stream_font_list': metadata_dict_from_prod.get('stream_font_list'),
            'encryption_algo_list': metadata_dict_from_prod.get('encryption_algo_list'),
            'good_font_list': metadata_dict_from_prod.get('good_font_list'),
            'strict_metadata_fraud_list': metadata_dict_from_prod.get('strict_metadata_fraud_list'),
            'is_metadata_update_testing': True,
            'statement_id': statement_id
        })
        
    fraud_iterable = []
    with ThreadPoolExecutor(max_workers=2) as executor:
        fraud_iterable = executor.map(multithreading_helper, process_list)
    
    for fraud_test_result in fraud_iterable:
        test_statement_id = fraud_test_result.get('statement_id')
        test_fraud_list = fraud_test_result.get('all_fraud_list')
        if isinstance(test_fraud_list, str):
            test_fraud_list = json.loads(test_fraud_list)
        if test_fraud_list == None:
            prod_results[test_statement_id].update({'test_fraud_list':None})
        else:
            prod_results[test_statement_id].update({'test_fraud_list':test_fraud_list})
    
    return prod_results

@fraud_metadata_router.post('/request_addition')
async def request_addition(request: AdditionFraudMetadata, response: Response, user = Depends(get_current_user)):
    bank_name = request.bank_name
    operation = request.operation
    metadata_type = request.metadata_type
    metadata = request.metadata
    if user.user_type != "superuser":
        response.status_code = status.HTTP_401_UNAUTHORIZED
        return {"message": "not authorised"}

    request_body = {
        'data_type': metadata_type,
        'bank_name': bank_name,
        'new_meta_data': metadata,
        'country': 'IN',
        'operation': operation
    }

    response = fb_dashboard_api_update_or_delete_metadata(request_body)
    if response.status_code!=200:
        response.status_code = status.HTTP_400_BAD_REQUEST
        return {"message": "API to update hashboard failed"}

    return {"message": "Successfully Done"}