from fastapi import APIRouter, Depends, Response, status
from app.dependencies import get_current_user
from app.retrigger.models import BsaStatus
from app.conf import STATEMENT_TABLE
from app.database_utils import portal_db

ddb_router = APIRouter()


async def update_ddb_status(statement_id, transactions_status, processing_status, identity_status, metadata_fraud_status, page_identity_fraud_status, response: Response, to_reject_statement=None, message=None, update_message=None):
    # check if record exists in the bank connect transactions table for this statement-id
    ddb_response = STATEMENT_TABLE.get_item(
                            Key={
                                    "statement_id": statement_id
                                }
                            )
    all_items = ddb_response.get('Item', [])

    print(all_items)
    if len(all_items)==0:
        print("This statement does not exist in the table, serving 404")
        response.status_code=status.HTTP_404_NOT_FOUND
        return {"message": "statement_id not available in bsa-table"}

    eligible_input_status_types = ["processing", "completed", "failed"]

    if (transactions_status not in eligible_input_status_types) or (processing_status not in eligible_input_status_types):
        response.status_code=status.HTTP_404_NOT_FOUND
        return {"message": f"status should be in {eligible_input_status_types}"}

    if identity_status not in [None, ""] and identity_status not in ["processing", "completed", "failed"]:
        response.status_code=status.HTTP_404_NOT_FOUND
        return {"message": f"status should be in {eligible_input_status_types}"}
        
    if metadata_fraud_status not in [None, ""] and metadata_fraud_status not in ["processing", "completed", "failed"]:
        response.status_code=status.HTTP_404_NOT_FOUND
        return {"message": f"status should be in {eligible_input_status_types}"}
        
    if page_identity_fraud_status not in [None, ""] and page_identity_fraud_status not in ["processing", "completed", "failed"]:
        response.status_code=status.HTTP_404_NOT_FOUND
        return {"message": f"status should be in {eligible_input_status_types}"}

    # update the status in dynamodb
    update_expression = "set transactions_status = :transactions_status, processing_status = :processing_status"
    update_expression_values = {
        ":transactions_status": transactions_status,
        ":processing_status": processing_status,
    }

    if identity_status not in [None,""]:
        update_expression += ", identity_status=:identity_status"
        update_expression_values.update({
            ":identity_status": identity_status
        })
    
    if metadata_fraud_status not in [None, ""]:
        update_expression += ", metadata_fraud_status=:metadata_fraud_status"
        update_expression_values.update({
            ":metadata_fraud_status": metadata_fraud_status
        })
    
    if page_identity_fraud_status not in [None, ""]:
        update_expression += ", page_identity_fraud_status=:page_identity_fraud_status"
        update_expression_values.update({
            ":page_identity_fraud_status": page_identity_fraud_status
        })

    if to_reject_statement not in [None, ""]:
        update_expression += ", to_reject_statement=:to_reject_statement"
        update_expression_values.update({
            ":to_reject_statement": to_reject_statement
        })
    
    if update_message:
        update_expression += ", message=:message"
        update_expression_values.update({
            ":message": message
        })

    STATEMENT_TABLE.update_item(
        Key={"statement_id": statement_id},
        UpdateExpression = update_expression,
        ExpressionAttributeValues=update_expression_values
    )


    return {"message": "status updated", "statement_id": statement_id}

@ddb_router.post('/update_bsa_status', tags=['update_ddb'])
async def update_bsa_status(request: BsaStatus, response: Response, user= Depends(get_current_user)):
    statement_id = request.statement_id
    transactions_status = request.transactions_status
    processing_status = request.processing_status
    identity_status = request.identity_status
    metadata_fraud_status = request.metadata_fraud_status
    page_identity_fraud_status = request.page_identity_fraud_status
    to_reject_statement = request.to_reject_statement
    message = request.message
    update_message = request.update_message
    
    if processing_status == 'processing':
        to_reject_statement = False
        message = None
        update_message = True

    pg_query = "SELECT is_active FROM bank_connect_statement WHERE statement_id = :statement_id"
    query_result = dict(await portal_db.fetch_one(query=pg_query, values={"statement_id": statement_id}))
    if not query_result.get('is_active', True):
        response.status_code=status.HTTP_400_BAD_REQUEST
        return {"message": "statement has false is_active status"}

    return await update_ddb_status(statement_id, transactions_status, processing_status, identity_status, metadata_fraud_status, page_identity_fraud_status, response, to_reject_statement, message, update_message)