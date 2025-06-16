from fastapi import APIRouter, Depends, Response, status
from app.database_utils import DBConnection
from app.dependencies import get_current_user
from app.inconsistency.statement.models import MarkCompleted
from app.constants import DATE_FORMAT
import traceback
from app.constants import QUALITY_DATABASE_NAME
from typing import Optional
from datetime import datetime, timedelta
from app.ddb_utils import get_transactions_for_statement
from app.template_dashboard.utils import create_viewable_presigned_url
from app.conf import PDF_BUCKET

statement_level_inconsistency = APIRouter()

@statement_level_inconsistency.get("/get", tags=['statement_level_inconsistency'])
async def get_statement_level_inconsistency_data(response: Response, 
                                                statement_id:Optional[str]=None, 
                                                from_date:Optional[str]=None, 
                                                to_date:Optional[str]=None,
                                                user=Depends(get_current_user),
                                                bank_name:Optional[str]=None, 
                                                organization_id:Optional[int]=None,
                                                organization_name:Optional[str]=None):
    quality_query = "select * from statement_level_inconsistency where inconsistent_hash!='' and inconsistent_hash is not null and status='PENDING'"
    if from_date is not None:
        try:
            formatted_from_date = datetime.strptime(from_date, '%Y-%m-%d')
            formatted_from_date = formatted_from_date - timedelta(days=1)
        except Exception:
            response.status_code = status.HTTP_400_BAD_REQUEST
            return {
                'message': 'could not format from_date: YYYY-MM-DD format expected',
                'data': {}
            }
    
        quality_query += f" and created_at>='{formatted_from_date.strftime('%Y-%m-%d')} 18:30:00'"
    
    if to_date is not None:
        try:
            formatted_to_date = datetime.strptime(to_date, '%Y-%m-%d')
            # formatted_to_date = formatted_to_date + timedelta(days=1)
        except Exception:
            response.status_code = status.HTTP_400_BAD_REQUEST
            return {
                'message': 'could not format to_date: YYYY-MM-DD format expected',
                'data': {}
            }
    
        quality_query += f" and created_at<='{formatted_to_date.strftime('%Y-%m-%d')} 18:30:00'"
    
    if statement_id is not None:
        quality_query += f" and statement_id = '{statement_id}'"
    
    if bank_name is not None:
        quality_query += f" and bank_name = '{bank_name}'"
    
    if organization_id is not None:
        quality_query += f" and organization_id = {organization_id}"
    
    if organization_name is not None:
        quality_query += f" and organization_name = '{organization_name}'"

    quality_data = DBConnection(QUALITY_DATABASE_NAME).execute_query(
        query=quality_query
    )

    return {
        'message': "",
        'data': quality_data
    }

@statement_level_inconsistency.post('/mark_completed', tags=['statement_level_inconsistency'])
async def mark_completed(request: MarkCompleted, response: Response, user=Depends(get_current_user)):
    quality_query = f"update statement_level_inconsistency set status='{request.status}', inconsistent_remarks='{request.inconsistent_remarks}', reason='{request.reason}', type='{request.type}', updated_at=%(updated_at)s where statement_id='{request.statement_id}'"

    DBConnection(QUALITY_DATABASE_NAME).execute_query(
        query=quality_query,
        values={
            'updated_at': datetime.now()
        }
    )

    return {
        'message': "successfully done",
        'data': {}
    }

@statement_level_inconsistency.get('/get_detailed_inconsistency_information', tags=['statement_level_inconsistency'])
async def get_detailed_inconsistency_information(response: Response, statement_id:str, user=Depends(get_current_user)):
    quality_query = f"select inc.inconsistent_hash as inconsistent_hash, sq.bank_name as bank_name, sq.pdf_password as pdf_password, sq.inconsistency_due_to_extraction as inconsistency_due_to_extraction, sq.inconsistent_statement_data as inconsistent_statement_data from statement_level_inconsistency inc, statement_quality sq where sq.statement_id=inc.statement_id and inc.statement_id='{statement_id}'"

    query_response = DBConnection(QUALITY_DATABASE_NAME).execute_query(
        query=quality_query
    )

    if len(query_response)==0:
        return {
            'message':'statement not found',
            'data': {}
        }
    
    bank_name = query_response[0].get('bank_name')
    pdf_password = query_response[0].get('pdf_password')
    inconsistent_hash = query_response[0].get('inconsistent_hash')
    inconsistency_due_to_extraction = query_response[0].get('inconsistency_due_to_extraction')
    inconsistent_statement_data = query_response[0].get('inconsistent_statement_data')
    inconsistent_statement_data = [] if not isinstance(inconsistent_statement_data, list) else inconsistent_statement_data
    transactions, _  = get_transactions_for_statement(statement_id)

    index = -1
    inconsistent_transactions = []
    for i in range(len(transactions)):
        if transactions[i].get('hash') == inconsistent_hash:
            index = i
            break
    
    if index!=-1:
        inconsistent_transactions = transactions[max(index-5,0):min(index+5,len(transactions)-1)]
    
    presigned_url = create_viewable_presigned_url(statement_id, bank_name)
    return {
        'message':'',
        'data':{
            'presigned_url': presigned_url,
            'transactions': inconsistent_transactions,
            'inconsistent_hash': inconsistent_hash,
            'bank_name': bank_name,
            'pdf_password': pdf_password,
            'inconsistency_due_to_extraction': inconsistency_due_to_extraction,
            'inconsistent_statement_data': inconsistent_statement_data
        }
    }


async def ingest_statement(kafka_message, is_retry = False):
    try:
        deduped_dict = {}
        for data_dict in kafka_message:
            statement_id = data_dict.get('statement_id')
            inconsistent_hash = data_dict.get('inconsistent_hash')
            is_inconsistent = data_dict.get('is_inconsistent')
            attempt_type = data_dict.get('attempt_type')
            pdf_hash = data_dict.get('pdf_hash')
            bank_name = data_dict.get('bank_name')
            entity_id = data_dict.get('entity_id')
            account_id = data_dict.get('account_id')
            organization_id = data_dict.get('organization_id')
            organization_name = data_dict.get('organization_name')

            if attempt_type == 'aa':
                continue

            if statement_id in deduped_dict.keys():
                previous_incon_hash = deduped_dict[statement_id].get('inconsistent_hash')
                inconsistent_hash = inconsistent_hash if inconsistent_hash else previous_incon_hash
                deduped_dict[statement_id] = {'is_inconsistent': inconsistent_hash is not None, 
                                              'inconsistent_hash': inconsistent_hash, 
                                              'pdf_hash': pdf_hash, 
                                              'bank_name': bank_name, 
                                              'entity_id': entity_id, 
                                              'account_id': account_id, 
                                              'organization_id': organization_id, 
                                              'organization_name': organization_name}
            else:
                deduped_dict[statement_id] = {'is_inconsistent': is_inconsistent, 
                                              'inconsistent_hash': inconsistent_hash, 
                                              'pdf_hash': pdf_hash, 
                                              'bank_name': bank_name, 
                                              'entity_id': entity_id, 
                                              'account_id': account_id, 
                                              'organization_id': organization_id, 
                                              'organization_name': organization_name}
        
        deduped_kafka_message = []
        for key, value in deduped_dict.items():
            value['statement_id'] = key
            deduped_kafka_message.append(value)

        if len(deduped_kafka_message)==0:
            return

        statement_ids = ''
        statement_to_info_dict = {}
        for data_dict in deduped_kafka_message:
            statement_id = data_dict.get('statement_id')
            statement_ids += f"'{statement_id}',"
            statement_to_info_dict[statement_id] = data_dict
        
        statement_ids = statement_ids[:-1]
        statement_ids = f"{statement_ids}"
            
        quality_query = f'select * from statement_level_inconsistency where statement_id in ({statement_ids})'
        quality_data = DBConnection(QUALITY_DATABASE_NAME).execute_query(
            query=quality_query
        )

        done_statements = {}
        for quality_data_dict in quality_data:
            statement_id = quality_data_dict.get('statement_id')
            is_inconsistent = quality_data_dict.get('is_inconsistent')
            inconsistent_hash = quality_data_dict.get('inconsistent_hash')
            account_id = quality_data_dict.get('account_id')

            done_statements[statement_id] = True
            quality_query = f"update statement_level_inconsistency set inconsistent_hash='{inconsistent_hash}' where statement_id='{statement_id}'"
            DBConnection(QUALITY_DATABASE_NAME).execute_query(
                query=quality_query
            )

        for data_dict in deduped_kafka_message:
            statement_id = data_dict.get('statement_id')
            inconsistent_hash = data_dict.get('inconsistent_hash')
            pdf_hash = data_dict.get('pdf_hash')
            bank_name = data_dict.get('bank_name')
            entity_id = data_dict.get('entity_id')
            account_id = data_dict.get('account_id')
            organization_id = data_dict.get('organization_id')
            organization_name = data_dict.get('organization_name')

            if not done_statements.get(statement_id):
                if pdf_hash is not None:
                    quality_query_to_check_pdf_hash = f"select pdf_hash, statement_id from statement_level_inconsistency where pdf_hash=%(pdf_hash)s"
                    quality_pdf_hash_data = DBConnection(QUALITY_DATABASE_NAME).execute_query(
                        query=quality_query_to_check_pdf_hash,
                        values={
                            'pdf_hash':pdf_hash
                        }
                    )
                    if len(quality_pdf_hash_data)>0:
                        print(f"Rejecting statement : {statement_id}, as pdf_hash already ingested", quality_pdf_hash_data)
                        continue

                quality_query = f"""insert into statement_level_inconsistency (statement_id,inconsistent_hash,created_at,updated_at,status,inconsistent_remarks,pdf_hash,bank_name, entity_id, account_id, organization_id, organization_name)
                                    VALUES (%(statement_id)s,%(inconsistent_hash)s,%(created_at)s,%(updated_at)s,%(status)s,%(inconsistent_remarks)s,%(pdf_hash)s,%(bank_name)s,%(entity_id)s,%(account_id)s,%(organization_id)s, %(organization_name)s)
                                """
                DBConnection(QUALITY_DATABASE_NAME).execute_query(
                    query=quality_query,
                    values={
                        'statement_id': statement_id,
                        'inconsistent_hash': inconsistent_hash,
                        'created_at': datetime.now(),
                        'updated_at': datetime.now(),
                        'status': "PENDING",
                        'inconsistent_remarks': None,
                        'bank_name': bank_name,
                        'pdf_hash': pdf_hash,
                        'entity_id': entity_id,
                        'account_id': account_id,
                        'organization_id': organization_id,
                        'organization_name': organization_name
                    }
                )

    except Exception as e:
        print(traceback.format_exc())