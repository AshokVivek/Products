import os
from app.dependencies import get_current_user
from fastapi import APIRouter, Depends, Response, status
from app.database_utils import portal_db
from app.proxy.proxy_models import XlsxReportRequestPayload
import requests
from app.conf import BANK_CONNECT_BASE_URL

proxy_router = APIRouter()

async def get_xlsx_report(x_api_key, server_hash, entity_id):
        
    url = f'{BANK_CONNECT_BASE_URL}/bank-connect/v1/entity/{entity_id}/xlsx_report/'
    headers = {
        'x-api-key': x_api_key,
        'server-hash': server_hash
    } 

    url_response = requests.request("GET", url, headers=headers, data=entity_id)
    return url_response.json()

@proxy_router.get("/xlsx_report_from_entityid")
async def xlsx_report_from_entityid(payload: XlsxReportRequestPayload, response: Response, user=Depends(get_current_user)):
    entity_id = payload.entity_id
    if entity_id is None:
        response.status_code = status.HTTP_400_BAD_REQUEST
        return {
            "message": "entity_id is required",
            "data": {}
        }
    if entity_id not in [None, ""]:
        pg_query = """
                    SELECT api_key,
                    server_hash
                    FROM users_user
                    JOIN finbox_dashboard_organization ON finbox_dashboard_organization.id = users_user.organization_id
                    JOIN bank_connect_entity ON finbox_dashboard_organization.id = bank_connect_entity.organization_id
                    WHERE bank_connect_entity.entity_id = :entity_id
                """                
        pg_query_values = {
            "entity_id": entity_id
        }
        try:
            query_result = dict(await portal_db.fetch_one(query=pg_query, values=pg_query_values))
            if not query_result:
                response.status_code = status.HTTP_404_NOT_FOUND
                return {
                    "message": "Invalid entity_id",
                    "data": {}
                }
            
        except Exception as e:
            print(f"Exception occured while getting db values: {e}")
            response.status_code = status.HTTP_404_NOT_FOUND
            return {
                "message": "Failed to get DB values",
                "data": {}
            }  
        
        x_api_key = query_result.get("api_key")
        server_hash = query_result.get("server_hash")
                
        xlsx_report_response = await get_xlsx_report(x_api_key, server_hash, entity_id)
        return {
            "message": "Success",
            "data": xlsx_report_response
        }