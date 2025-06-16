import warnings
import pandas as pd
from copy import deepcopy
import sentry_sdk
import json
import traceback
from fastapi import Depends, Request, Response, APIRouter
from app.dependencies import get_current_user
from app.database_utils import clickhouse_client, portal_db
from.models import FaultyTransactionUpdateData
from datetime import datetime, timedelta
from app.conf import redis_cli
from app.constants import DATE_FORMAT

warnings.simplefilter(action = "ignore", category = FutureWarning)
pd.options.mode.chained_assignment = None

extraction_issue_router = APIRouter()

@extraction_issue_router.get("/get-filters")
def get_filters(request: Request, response: Response):

    response_data = {}
    final_data = {}

    query_params = request.query_params
    bank_name = query_params.get('bank_name', '')
    issue_type = query_params.get('issue_type', '')
    from_date = query_params.get('from_date', None)
    to_date = query_params.get('to_date', None)
    try:
        if from_date:
            from_date = datetime.strptime(from_date, DATE_FORMAT)
        else:
            from_date = (datetime.now() - timedelta(1)).strftime(DATE_FORMAT)

        if to_date:
            to_date = datetime.strptime(to_date, DATE_FORMAT)
        else:
            to_date = datetime.now().strftime(DATE_FORMAT)
    except Exception as e:
        return {
            "error": True,
            "message": f"Invalid Date format. Expected : {DATE_FORMAT}",
            "data": ""
        }

    # Query Construction
    select_part = 'SELECT COUNTDistinct(statement_id) as statement_count, '
    table_name_part = ' FROM bank_connect.extractionIssue '
    where_part = f" WHERE created_at >= '{from_date}' AND created_at < '{to_date}' "
    order_by_part = ' ORDER BY statement_count '
    group_by_part = ' GROUP BY '

    if not bank_name and not issue_type:  # bank_names and issues_type, both are needed
        select_part += 'extraction_issue_type as issue_type, bank_name as bank_name '
        group_by_part += 'issue_type, bank_name '
    elif issue_type:  # bank_names needed while issues_type is choosen
        select_part += ' bank_name as bank_name '
        where_part += f" AND extraction_issue_type = '{issue_type}' "
        group_by_part += 'bank_name'
    elif bank_name:  # issues_types needed while bank_name is choosen
        select_part += ' extraction_issue_type as issue_type '
        where_part += f"AND bank_name = '{bank_name}' "
        group_by_part += f" issue_type "

    final_query = select_part + table_name_part + where_part + group_by_part + order_by_part
    
    try:
        result_df = clickhouse_client.query_df(final_query)
        
        if not result_df.empty:
            if 'issue_type' in result_df.columns:
                issue_df = deepcopy(result_df.groupby(['issue_type'])['statement_count'].sum())
                issue_df = pd.DataFrame({'issue_type': issue_df.index, 'statement_count': issue_df.values})
                final_data['issue_type'] = issue_df.to_dict("records")

            if 'bank_name' in result_df.columns:
                bank_name_df = deepcopy(result_df.groupby(['bank_name'])['statement_count'].sum())
                bank_name_df = pd.DataFrame({'bank_name': bank_name_df.index, 'statement_count': bank_name_df.values})
                final_data['bank_name'] = bank_name_df.to_dict("records")

        response_data = {
            "error": False,
            "message" : "success",
            "data": final_data
        }
    except Exception as e:
        print(traceback.format_exc())
        sentry_sdk.capture_exception(e)
        response_data = {
            "error": True,
            "message" : "Internal Sever Error",
            "data": ""
        }
    
    return response_data

@extraction_issue_router.get("/get-statements/")
def get_statements_with_issues(request: Request, response: Response):
    statement_ids = []
    response_data = {}

    query_params_dict = request.query_params
    issue_type = query_params_dict.get('issue_type', None)
    bank_name = query_params_dict.get('bank_name', None)
    statement_id = query_params_dict.get('statement_id', None)
    from_date = query_params_dict.get('from_date', None)
    to_date = query_params_dict.get('to_date', None)

    try:
        if from_date:
            from_date = datetime.strptime(from_date, DATE_FORMAT)
        else:
            from_date = (datetime.now() - timedelta(1)).strftime(DATE_FORMAT)

        if to_date:
            to_date = datetime.strptime(to_date, DATE_FORMAT)
        else:
            to_date = datetime.now().strftime(DATE_FORMAT)
    except Exception as e:
        return {
            "error": True,
            "message": f"Invalid Date format. Expected : {DATE_FORMAT}",
            "data": ""
        }

    if not any ([issue_type, bank_name, statement_id]):
        return {
            "error": True,
            "message": "Issue type, Bank Name or Statement id required",
            "data": ""
        }
    
     # Query Construction
    select_part = 'SELECT DISTINCT(statement_id) as statement_id, count(*) as transaction_count '
    table_name_part = ' FROM bank_connect.extractionIssue '
    where_part = f" WHERE is_extraction_problem_confirmed is NULL AND created_at >= '{from_date}' AND created_at < '{to_date}' "
    order_by_part = ''
    group_by_part = ' GROUP BY statement_id'

    if statement_id:
        where_part += f" AND statement_id = '{statement_id}' "
    else:
        if bank_name and issue_type:
            where_part += f" AND bank_name = '{bank_name}' AND extraction_issue_type = '{issue_type}' "
        elif bank_name:
            where_part += f" AND bank_name = '{bank_name}' "
        elif issue_type:
            where_part += f" AND extraction_issue_type = '{issue_type}' "

    
    final_query = select_part + table_name_part + where_part + group_by_part + order_by_part
    try:
        result_df = clickhouse_client.query_df(final_query)
        if not result_df.empty:
            statement_ids = result_df.to_dict("records")

        response_data =  {
            "error": False,
            "message": "success",
            "data": statement_ids
        }
    
    except Exception as e:
        print(traceback.format_exc())
        sentry_sdk.capture_exception(e)
        response_data = {
            "error": True,
            "message" : "Internal Sever Error",
            "data": ""
        }

    return response_data

@extraction_issue_router.get("/get-faulty-transactions/")
def get_faulty_transactions(request: Request, response: Response):
    grouped_dict = {}
    query_params_dict = request.query_params
    issue_type = query_params_dict.get('issue_type', None)
    statement_id = query_params_dict.get('statement_id', None)

    response_data = {}

    if not statement_id:
        return {
            "error": True,
            "message": "statement_id recquired",
            "data": ""
        }
    
    select_part = 'SELECT * '
    table_name_part = ' FROM bank_connect.extractionIssue '
    where_part = f" WHERE statement_id = '{statement_id}' AND is_extraction_problem_confirmed is NULL "
    order_by_part = 'ORDER BY page_number, sequence_number'

    if issue_type:
        where_part += f" AND extraction_issue_type = '{issue_type}' "

    final_query = select_part + table_name_part + where_part + order_by_part    
    
    try:
        result_df = clickhouse_client.query_df(final_query)

        if not result_df.empty:
            result_df['amount'] = result_df['amount'].astype(str)
            result_df['balance'] = result_df['balance'].astype(str)
            result_df['amount'] = result_df['amount'].astype(float)
            result_df['balance'] = result_df['balance'].astype(float)
            grouped_dict = result_df.groupby('page_number').apply(lambda x: x.drop('page_number', axis=1).to_dict(orient='records')).to_dict()

        response_data =  {
            "error": False,
            "message": "success",
            "data": grouped_dict
        }
    except Exception as e:
        print(traceback.format_exc())
        sentry_sdk.capture_exception(e)
        response_data = {
            "error": True,
            "message" : "Internal Sever Error",
            "data": ""
        }

    return response_data

@extraction_issue_router.patch("/update-faulty-transactions/{transaction_unique_id}")
def update_faulty_transactions(transaction_unique_id: str, faulty_transaction_updated_data: FaultyTransactionUpdateData, request: Request, response: Response, user=Depends(get_current_user)):

    is_extraction_problem_confirmed = faulty_transaction_updated_data.is_extraction_problem_confirmed
    is_issue_solved = faulty_transaction_updated_data.is_issue_solved
    technique_used_to_solve = faulty_transaction_updated_data.technique_used_to_solve

    response_data = {}

    update_command = f"ALTER TABLE extractionIssue UPDATE is_extraction_problem_confirmed = {is_extraction_problem_confirmed}, is_issue_solved = {is_issue_solved}, technique_used_to_solve= '{technique_used_to_solve}' WHERE unique_id = '{transaction_unique_id}'"
    
    try:
        result = clickhouse_client.command(update_command)

        response_data  = {
            "error": False,
            "message": "update successful",
            "data": ""
        }

    except Exception as e:
        print(traceback.format_exc())
        sentry_sdk.capture_exception(e)
        response_data = {
            "error": True,
            "message" : "Internal Sever Error",
            "data": ""
        }

    return response_data

@extraction_issue_router.get("/get-template/{template_uuid}")
async def get_template_from_uuid(template_uuid: str, request: Request, response: Response, user=Depends(get_current_user)):
    try:
        template_data = {}
        template_data = redis_cli.get(template_uuid)
        if template_data:
            template_data = json.loads(template_data)
        else:
            template_data = await fetch_template_from_uuid(template_uuid=template_uuid)
            redis_cli.set(template_uuid, json.dumps(template_data), 86400)

        response_data = {
            "error":False,
            "message":'successful',
            "data":template_data
        }

    except Exception as e:
        sentry_sdk.capture_exception(e)
        print(traceback.format_exc())
        response_data = {
            "error":True,
            "message":'',
            "data":""
        }

    return response_data
    
async def fetch_template_from_uuid(template_uuid):
    template = {}
    try:
        print("querying database")
        query = f"SELECT template_uuid, template_type, template_json, bank_name, is_active, approved_by, priority FROM bank_connect_fsmlibtemplates where template_uuid=:template_uuid"
        template = await portal_db.fetch_one(
                            query, 
                            values={
                                "template_uuid": template_uuid
                            }
    
                        )
    except Exception as e:
         sentry_sdk.capture_exception(e)
    return dict(template)