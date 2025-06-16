from fastapi import APIRouter, Depends, Response, status
from app.database_utils import DBConnection
from app.dependencies import get_current_user
from app.fsmlib_data_update.models import RequestDataUpdate, ApprovalStatus, RequestApproval
from app.constants import QUALITY_DATABASE_NAME, PORTAL_DATABASE_NAME
from app.template_solutioning.dashboard_calls import fb_dashboard_api_fsmlib_data
from app.conf import COUNTRY
from uuid import uuid4
from typing import Optional
from datetime import datetime
from app.conf import s3, PDF_BUCKET
from app.template_dashboard.utils import create_presigned_url_by_bucket
import json

fsmlib_update_router = APIRouter()

@fsmlib_update_router.post("/request_data_update", tags=['fsmlib_update_data'])
async def request_data_update(request: RequestDataUpdate, response: Response, user=Depends(get_current_user)):
    request_type = request.request_type
    requested_by = user.username
    requested_data = request.requested_data
    statement_id = request.statement_id
    operation_type = request.operation_type

    if operation_type not in ['update', 'delete']:
        response.status_code = status.HTTP_400_BAD_REQUEST
        return {
            "message": "unsupported operation",
            "data": {}
        }

    if request_type not in ['company_end_keywords']:
        response.status_code = status.HTTP_400_BAD_REQUEST
        return {
            "message": "unsupported request_type",
            "data": {}
        }
    

    if not statement_id:
        response.status_code = status.HTTP_400_BAD_REQUEST
        return {
            "message": "Provide a sample statement_id for which this request is being made",
            "data": {}
        }
    
    data_from_query = DBConnection(PORTAL_DATABASE_NAME).execute_query(
        query='select statement_id, bank_name from bank_connect_statement where statement_id=%(statement_id)s',
        values={
            'statement_id': statement_id
        }
    )

    if len(data_from_query)==0:
        response.status_code = status.HTTP_400_BAD_REQUEST
        return {
            "message": "Statement not found in RDS",
            "data": {}
        }
    
    bank_name = data_from_query[0].get('bank_name')
    
    try:
        pdf_bucket_response = s3.get_object(Bucket=PDF_BUCKET, Key=f'pdf/{statement_id}_{bank_name}.pdf')
    except Exception as e:
        print(e)
        response.status_code = status.HTTP_400_BAD_REQUEST
        return {
            "message": "Statement not found in S3",
            "data": {}
        }
    
    if request_type == 'company_end_keywords' and not requested_data.get('requested_string'):
        response.status_code = status.HTTP_400_BAD_REQUEST
        return {
            "message": "requested_string not found",
            "data": {}
        }
    
    quality_query = '''
                        insert into fsmlib_data_update_requests (request_id, requested_data, request_type, requested_by, statement_id, approval_status, operation_type, created_at, updated_at) 
                        VALUES (%(request_id)s, %(requested_data)s, %(request_type)s, %(requested_by)s, %(statement_id)s, %(approval_status)s, %(operation_type)s, %(created_at)s, %(updated_at)s)
                    '''
    
    request_id = str(uuid4())
    DBConnection(QUALITY_DATABASE_NAME).execute_query(
        query=quality_query,
        values={
            'requested_data': json.dumps(requested_data),
            'request_type': request_type,
            'requested_by': requested_by,
            'statement_id': statement_id,
            'approval_status': ApprovalStatus.PENDING,
            'request_id': request_id,
            'operation_type': operation_type,
            'created_at': datetime.now(),
            'updated_at': datetime.now()
        }
    )

    return {
        "message": "successfully requested",
            "data": {
                'request_id': request_id
            }
    }

@fsmlib_update_router.get("/get_all_requests", tags=['fsmlib_update_data'])
async def request_data_update(response: Response, request_type:Optional[str]=None, approval_status:Optional[str] = "PENDING", user=Depends(get_current_user)):
    quality_query = "select * from fsmlib_data_update_requests where approval_status=%(approval_status)s"
    value_dict = {
        'approval_status': approval_status
    }

    if request_type:
        quality_query += ' and request_type=%(request_type)s'
        value_dict.update({
            'request_type': request_type
        })

    data_from_query = DBConnection(QUALITY_DATABASE_NAME).execute_query(
        query=quality_query,
        values=value_dict
    )

    return  {
        "message": "unsupported operation",
        "data": data_from_query
    }

@fsmlib_update_router.get("/view_request", tags=['fsmlib_update_data'])
async def request_data_update(response: Response, request_id:str, user=Depends(get_current_user)):
    quality_query = 'select * from fsmlib_data_update_requests where request_id=%(request_id)s'

    data_from_query = DBConnection(QUALITY_DATABASE_NAME).execute_query(
        query=quality_query,
        values={
            'request_id': request_id
        }
    )

    if len(data_from_query) == 0:
        response.status_code = status.HTTP_404_NOT_FOUND
        return {
            "message": "request not found",
            "data":{}
        }
    
    statement_id = data_from_query[0].get('statement_id')
    portal_data = DBConnection(PORTAL_DATABASE_NAME).execute_query(
        query='select statement_id, bank_name, pdf_password from bank_connect_statement where statement_id=%(statement_id)s',
        values={
            'statement_id': statement_id
        }
    )

    bank_name = portal_data[0].get('bank_name')
    data_dict = dict(data_from_query[0])
    data_dict.update({
        'presigned_url': create_presigned_url_by_bucket(PDF_BUCKET,f'pdf/{statement_id}_{bank_name}.pdf',4000),
        'pdf_password': portal_data[0].get('pdf_password')
    })

    return {
        "message": "",
        "data": data_dict
    }

@fsmlib_update_router.post("/approve_request", tags=['fsmlib_update_data'])
async def request_data_update(request: RequestApproval, response: Response, user=Depends(get_current_user)):
    if user.user_type != 'superuser':
        response.status_code = status.HTTP_401_UNAUTHORIZED
        return {
            "message": "logged-in user not superuser",
            "data": {}
        }
    
    approval_status = request.approval_status
    request_id = request.request_id
    
    quality_query = 'select * from fsmlib_data_update_requests where request_id=%(request_id)s'

    data_from_query = DBConnection(QUALITY_DATABASE_NAME).execute_query(
        query=quality_query,
        values={
            'request_id': request_id
        }
    )

    if len(data_from_query) == 0:
        response.status_code = status.HTTP_404_NOT_FOUND
        return {
            "message": "request not found",
            "data": {}
        }
    
    data_from_query = data_from_query[0]
    request_operation_type = data_from_query.get('operation_type')
    request_type = data_from_query.get('request_type')
    requested_data = data_from_query.get('requested_data')

    if approval_status:
        response = fb_dashboard_api_fsmlib_data({
            'operation': request_operation_type,
            'country': COUNTRY,
            'request_type': request_type,
            'requested_data': requested_data
        })
        
        if response.status_code != 200:
            response_json = response.json()
            return {'message': "Something went wrong on dashboard", 'data': response_json}
    
    quality_query = """
                    update fsmlib_data_update_requests set approved_by=%(approved_by)s, approval_status=%(approval_status)s, updated_at=%(updated_at)s where request_id=%(request_id)s
                    """
    
    data_from_query = DBConnection(QUALITY_DATABASE_NAME).execute_query(
        query=quality_query,
        values={
            'request_id': request_id,
            'approved_by': user.username,
            'approval_status': "APPROVED" if approval_status else "REJECTED",
            'updated_at': datetime.now()
        }
    )

    return {
        "message": "Approved Successfully" if approval_status else "Rejected Successfully",
        "data": {}
    }