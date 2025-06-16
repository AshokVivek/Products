from fastapi import APIRouter, Depends
from app.database_utils import portal_db
from app.conf import redis_cli
from app.dependencies import get_current_user
import json
general_router = APIRouter()

@general_router.get("/client_org_data", tags=['general'])
async def get_client_information(user=Depends(get_current_user)):
    REDIS_KEY = 'client_information'
    if redis_cli.get(REDIS_KEY) is not None:
        required_data = json.loads(redis_cli.get(REDIS_KEY))
        return {
            "organization_info": required_data
        }
    
    portal_data_query = """
                        select client_id, id as organization_id, name as org_name from finbox_dashboard_organization
                        """
    portal_data = await portal_db.fetch_all(query=portal_data_query)
    
    data_list = []
    for data_obj in portal_data:
        data_list.append(dict(data_obj))
    
    redis_cli.set(REDIS_KEY,json.dumps(data_list))
    return {
        "organization_info": data_list
    } 