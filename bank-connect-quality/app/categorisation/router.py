from fastapi import APIRouter, Depends, Response, status
from app.dependencies import get_current_user
from app.categorisation.request_models import AddRegexToTempDB
from app.conf import *
from app.categorisation.utils import get_bank_connect_table_hash, get_hash_of_tables
from app.database_utils import clickhouse_client, quality_database
from fastapi_utils.tasks import repeat_every
import json, pandas as pd, threading

categorisation_router = APIRouter()


@categorisation_router.get("/", tags=["Categorisation Router"])
def health():
    return {"message": "categorisation router is up"}


@categorisation_router.post("/add_regex", tags=["Categorisation Router"])
async def approve_template(
    request: AddRegexToTempDB, response: Response, user=Depends(get_current_user)
):
    enrichment_type = request.enrichment_type
    regex = request.regex
    operation = request.operation

    if not (enrichment_type and regex and operation):
        response.status_code = status.HTTP_400_BAD_REQUEST
        return {"message": "invalid request"}


@repeat_every(seconds=60)
async def categorisation_cron_checker():
    tables_to_check = {
        "bank_connect_fsmlibgeneraldata": "fsmlibgeneraldata",
        "bank_connect_fsmlibtransactionchannels": "fsmlibtransactionchannels",
        "bank_connect_fsmlibuncleanmerchants": "fsmlibuncleanmerchants",
        "bank_connect_fsmlibmerchantcategory": "fsmlibmerchantcategory",
    }
    change_found = False
    for bc_table, quality_table in tables_to_check:
        bank_connect_hash = get_bank_connect_table_hash(bc_table)
        quality_hash = get_hash_of_tables(quality_table, quality_database)
        if bank_connect_hash != quality_hash:
            change_found = True
            break

    if not change_found:
        return {"message": "no change in hashes"}


def process_transactions(entity_id):
    query = f"""
        SELECT * from bank_connect.transactions
        WHERE entity_id='{entity_id}'
        ORDER by page_number, sequence_number
    """
    transactions_data = clickhouse_client.query_df(query)
    transactions_data.fillna("", inplace=True)
    transactions_data["created_at"] = transactions_data["created_at"].astype("str")
    transactions = transactions_data.to_dict("records")
    if not transactions:
        return
    print("total number of transactions to process: ", len(transactions), ", entity_id: ", entity_id)
    bank_name = transactions[0]["bank_name"]
    # print(transactions)
    lambda_payload = {"bank_name": bank_name, "transactions": transactions}
    lambda_response = lambda_client.invoke(
        FunctionName=CATEGORISATION_LAMBDA_FUNCTION_NAME,
        Payload=json.dumps(lambda_payload),
        InvocationType="RequestResponse",
    )
    data = json.loads(lambda_response["Payload"].read().decode("utf-8"))
    print("transactions fetched : ", len(data))
    new_transaction_df = pd.DataFrame(data)
    clickhouse_client.insert_df(
        "bank_connect.transactions_quality_run", new_transaction_df
    )
    print("transactions processed for entity_id : ", entity_id)
    clickhouse_client.close()


async def perform_new_simultaion():
    clickhouse_client = clickhouse_connect.get_client(
        host=CLICKHOUSE_HOST, 
        port=CLICKHOUSE_PORT, 
        username=CLICKHOUSE_USER, 
        password=CLICKHOUSE_PASSWORD, 
        database='bank_connect',
        secure=True
    )
    unique_entity_ids_query = """
                SELECT entity_id, count(*) FROM bank_connect.transactions group by entity_id
            """
    unique_data = clickhouse_client.query_df(unique_entity_ids_query)
    unique_data.fillna("", inplace=True)
    total_count = len(unique_data)
    print("Total Entity IDs to classify : ", total_count)

    THREAD_COUNT = 5
    for i in range(0, total_count, THREAD_COUNT):
        threads_list = []
        for j in range(0, THREAD_COUNT):
            item_index = (i * THREAD_COUNT) + j
            if item_index >= total_count:
                break
            df_row = unique_data.loc[item_index].to_list()
            entity_id = df_row[0]
            t = threading.Thread(target=process_transactions, args=(entity_id,))
            t.start()
            threads_list.append(t)
        for t in threads_list:
            t.join()
    clickhouse_client.close()