from app.database_utils import clickhouse_client
from app.database_utils import portal_db
from app.conf import COMP_SCHEDULING_INTERVAL
from datetime import datetime, timedelta
import pandas as pd
import requests
import os


def get_transactions(entity_id, api_key):
    url = f"https://apis.bankconnect.finbox.in/bank-connect/v1/entity/{entity_id}/transactions/"
    headers = {
        'x-api-key': api_key
    }
    response = requests.request("GET", url, headers=headers)
    return response.json().get("transactions", []), response.json().get("accounts", [{}])

async def get_mapping_data_from_portal_db(from_time_stamp, to_time_stamp):
    query = """
            select distinct b_entity_id, p_entity_id from bank_connect_entitymappingcomp
            where created_at>:from_time_stamp and created_at<:to_time_stamp
            """
    data = await portal_db.fetch_all(
        query = query,
        values = {
            "from_time_stamp": from_time_stamp,
            "to_time_stamp": to_time_stamp
        }
    )
    mapping_data = []
    entity_ids = []
    for i in data:
        i = dict(i)
        mapping_data.append(i)
        entity_ids.append(i['b_entity_id'])
        entity_ids.append(i['p_entity_id'])
    
    if len(entity_ids) == 0:
        return pd.DataFrame([]), pd.DataFrame([])
    
    api_key_query = f"""
            select distinct entity_id, api_key from bank_connect_entity e join users_user u
            on e.organization_id=u.organization_id
            where api_key!='' and api_key not ilike '5ErqPvTJ%' and entity_id in {tuple(entity_ids)}
        """
    api_key_data = await portal_db.fetch_all(
        query = api_key_query
    )
    for i in range(0, len(api_key_data)):
        api_key_data[i] = dict(api_key_data[i])
    
    return pd.DataFrame(mapping_data), pd.DataFrame(api_key_data)

PERF_COMP_MAPPING_CRON_LOCKFILE = '/tmp/perfios_comp_mapping_cron_job.lock'
async def perfios_bankconnect_comp_mapping():
    if os.path.exists(PERF_COMP_MAPPING_CRON_LOCKFILE):
        print("Another instance is already running, skipping for this worker")
        return
    
    with open(PERF_COMP_MAPPING_CRON_LOCKFILE, "w") as f:
        pass
    from_time_stamp = datetime.now() - timedelta(minutes=60)
    to_time_stamp = from_time_stamp + timedelta(minutes=COMP_SCHEDULING_INTERVAL)

    mapped_entity_ids_df, api_key_data_df = await get_mapping_data_from_portal_db(from_time_stamp, to_time_stamp)
    print("number of entity ids : ", len(mapped_entity_ids_df))

    all_transactions = []
    created_at = datetime.now()
    seen_p_entity_ids = []
    for index, items in mapped_entity_ids_df.iterrows():
        p_entity_id = items["p_entity_id"]
        p_api_key = list(api_key_data_df[api_key_data_df['entity_id']==p_entity_id]['api_key'])[0]

        if p_entity_id in seen_p_entity_ids:
            continue
        seen_p_entity_ids.append(p_entity_id)
        
        b_entity_id = items["b_entity_id"]
        b_api_key = list(api_key_data_df[api_key_data_df['entity_id']==b_entity_id]['api_key'])[0]
        
        # print("p_entity_id", p_entity_id)
        # print("b_entity_id", b_entity_id)

        p_transactions, p_accounts = get_transactions(p_entity_id, p_api_key)
        b_transactions, b_accounts = get_transactions(b_entity_id, b_api_key)

        # print("length of p_transactions: ", len(p_transactions))
        # print("length of b_transactions: ", len(b_transactions))

        b_account_id_map = {}
        for b_acc in b_accounts:
            aid = b_acc["account_id"]
            bname = b_acc["bank"]
            b_account_id_map[aid] = bname
        
        # print(b_account_id_map)

        common_keys = ["amount", "balance", "date", "transaction_type"]
        p_transactions_map = {}
        b_transactions_map = {}
        final_set = []

        _p_duplicate_keys = {}
        for txn in p_transactions:
            key = tuple(txn[i] for i in common_keys)
            if key in p_transactions_map:
                _counter_value = _p_duplicate_keys.get(key, 0)
                _p_duplicate_keys[key] = _counter_value + 1
                _new_key = f"{key}-{_counter_value}"
                print(_new_key)
                p_transactions_map[_new_key] = txn
            else:
                p_transactions_map[key] = txn

        _b_duplicate_keys = {}
        for txn in b_transactions:
            key = tuple(txn[i] for i in common_keys)
            if key in b_transactions_map:
                _counter_value = _b_duplicate_keys.get(key, 0)
                _b_duplicate_keys[key] = _counter_value + 1
                _new_key = f"{key}-{_counter_value}"
                b_transactions_map[_new_key] = txn
            else:
                b_transactions_map[key] = txn
        
        # print("length of p_transactions_map: ", len(p_transactions_map))
        # print("length of b_transactions_map: ", len(b_transactions_map))

        p_counter = 0
        for tup_key, p_transaction in p_transactions_map.items():
            b_transaction = b_transactions_map.get(tup_key, {})
            account_id = b_transaction.get("account_id")
            p_account_id = p_transaction.get("account_id")
            p_counter += 1
            if b_transaction:
                if "perfios_txn_category" not in p_transaction:
                    continue
                final_set.append({
                    "p_entity_id": p_entity_id,
                    "b_entity_id": b_entity_id,
                    "p_account_id": p_account_id,
                    "b_account_id": account_id,
                    "b_bank_name": b_account_id_map[account_id],
                    "p_transaction_note": p_transaction['transaction_note'],
                    "p_transaction_type": p_transaction["transaction_type"],
                    "p_amount": p_transaction["amount"],
                    "p_balance": p_transaction["balance"],
                    "p_date": p_transaction["date"],
                    "p_hash": p_transaction["hash"],
                    "b_transaction_note": b_transaction['transaction_note'],
                    "b_transaction_type": b_transaction["transaction_type"],
                    "b_amount": b_transaction["amount"],
                    "b_balance": b_transaction["balance"],
                    "b_date": b_transaction["date"],
                    "b_hash": b_transaction["hash"],
                    "perfios_txn_category": p_transaction["perfios_txn_category"],
                    "category": b_transaction["category"],
                    "transaction_channel": b_transaction["transaction_channel"],
                    "description": b_transaction["description"],
                    "merchant_category": b_transaction["merchant_category"],
                    "p_counter": p_counter,
                    "created_at": created_at
                })
        all_transactions += final_set
    
    all_transactions_df = pd.DataFrame(all_transactions)
    clickhouse_client.insert_df(
        "bank_connect.mapped_comp_transactions",
        all_transactions_df
    )
    print(f"inserted {len(all_transactions)} in bank_connect.mapped_comp_transactions")
    os.remove(PERF_COMP_MAPPING_CRON_LOCKFILE)