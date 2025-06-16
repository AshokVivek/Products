import hashlib, json
from app.database_utils import portal_db
from app.conf import STAGE, redis_cli

async def get_hash_of_tables(table_name, database):
    query = f"""
                SELECT * FROM {table_name}
            """
    if table_name in ["bank_connect_fsmlibgeneraldata", "bank_connect_fsmlibtransactionchannels", 
        "bank_connect_fsmlibuncleanmerchants", "bank_connect_fsmlibmerchantcategory"]:
        query += "WHERE country='IN' ORDER BY id"
    data = await database.fetch_all(
        query = query
    )
    data = [dict(_) for _ in data]
    hash_object = hashlib.sha256()
    for items in data:
        hash_object.update(json.dumps(items).encode())
    return hash_object.hexdigest()

async def get_bank_connect_table_hash(table_name):
    key = f'{STAGE}_{table_name}_hash'
    value = redis_cli.get(key)
    if value!=None:
        return value
    hash = await get_hash_of_tables(table_name, portal_db)
    redis_cli.set(key, value, 72000)
    return hash