from fastapi import APIRouter, Depends, Response, status
from app.template_solutioning.request_models import CommonIngest
from app.database_utils import portal_db
from app.dependencies import get_current_user
from app.conf import *
from app.template_solutioning.credit_data import ingest_cc_statement
from app.template_solutioning.quality_data import ingest_into_quality_vanilla
import sentry_sdk
from app.database_utils import DBConnection
from app.constants import QUALITY_DATABASE_NAME, PORTAL_DATABASE_NAME

ingest_router = APIRouter()

@ingest_router.post('/ingest')
async def ingest_into_quality(request: CommonIngest):
    statements = request.statements
    # access_code = request.access_code

    # if access_code != QUALITY_SECRET:
    #     response.status_code = status.HTTP_401_UNAUTHORIZED
    #     return {"message": "unauthorized"}
    return await ingest_helper(statements)


async def ingest_helper(statements_list):
    """
    Also used by kafka consumers to ingest data
    """
    statements = statements_list

    credit_card_identity = {}
    vanilla_identity = {}

    credit_card_statements = [x for x in statements if x.get('is_credit_card', False)]
    vanilla_statements = [x for x in statements if not x.get('is_credit_card', False)]

    credit_card_statements_stringified_format, vanilla_statements_stringified_format = "", ""
    for statement in credit_card_statements:
        statement_id = statement.get('statement_id')
        identity = statement.get('identity')
        credit_card_statements_stringified_format += f"'{statement_id}',"
        credit_card_identity[statement_id] = identity

    credit_card_statements_stringified_format = credit_card_statements_stringified_format[:-1]

    for statement in vanilla_statements:
        statement_id = statement.get('statement_id')
        identity = statement.get('identity')
        vanilla_statements_stringified_format += f"'{statement_id}',"
        vanilla_identity[statement_id] = identity
    vanilla_statements_stringified_format = vanilla_statements_stringified_format[:-1]

    if len(credit_card_statements_stringified_format) > 0:
        credit_card_data_query = f"""
                                select * from bank_connect_creditcardstatement where statement_id in ({credit_card_statements_stringified_format})
                                """
        credit_card_ingest_data = DBConnection(PORTAL_DATABASE_NAME).execute_query(credit_card_data_query)
        for credit_card_data in credit_card_ingest_data:
            credit_card_data = dict(credit_card_data)
            statement_status = credit_card_data.get('statement_status', None)
            name_from_upload = credit_card_data.get('name', None)
            statement_id = credit_card_data.get('statement_id')
            credit_card_data.update(credit_card_identity.get(statement_id, dict()))
            if statement_status == 1:
                credit_card_data['name'] = name_from_upload

            try:
                await ingest_cc_statement(credit_card_data)
            except Exception as e:
                sentry_sdk.capture_exception(e)

    identity_mismatch_data = []
    if len(vanilla_statements_stringified_format) > 0:
        vanilla_data_query = f""" SELECT * from bank_connect_statement where statement_id in ({vanilla_statements_stringified_format})"""
        vanilla_ingest_data = DBConnection(PORTAL_DATABASE_NAME).execute_query(vanilla_data_query)
        for vanilla_data in vanilla_ingest_data:
            vanilla_data = dict(vanilla_data)
            statement_id = vanilla_data.get('statement_id')
            vanilla_data.update(vanilla_identity.get(statement_id, dict()))
            try:
                statement_identity_mismatch_data = await ingest_into_quality_vanilla(vanilla_data)
                identity_mismatch_data.append(statement_identity_mismatch_data)
            except Exception as e:
                print(e)
                sentry_sdk.capture_exception(e)

    return {"message": "Successfully Done"}
