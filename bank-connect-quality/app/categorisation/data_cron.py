import pandas as pd
import requests
import json
import random
from app.conf import s3, SLACK_TOKEN, redis_cli, SLACK_CHANNEL
from app.database_utils import portal_db
from datetime import datetime
import time
import os
from slack_sdk import WebClient
from app.retrigger.models import BsaStatus, InvokeLambda
from app.retrigger.ddb_status import update_bsa_status
from app.retrigger.retrigger_lambdas import invoke_analyze_pdf

# SLACK_CHANNEL = "C072PE0AR3M"

NUM_USERS = 1000

API_KEYS_S3_PATH = "data/api_keys.xlsx"
REFERENCE_DATA_S3_URI = "data/reference_data_2.xlsx"

def download_files_from_s3(key, to_save_file_path):
    response = s3.get_object(Bucket = "bank-connect-quality-prod", Key = key)
    with open(to_save_file_path, 'wb') as file_obj:
        file_obj.write(response['Body'].read())

async def retrigger_transactions_for_statement(statement_id, bank_name):
    status_obj = BsaStatus(
        statement_id=statement_id,
        transactions_status="processing",
        processing_status="processing",
        to_reject_statement=False,
        message=None,
        update_message=True
    )
    await update_bsa_status(status_obj, None)

    invoke_obj = InvokeLambda(
        key = f"{statement_id}_{bank_name}.pdf"
    )
    await invoke_analyze_pdf(invoke_obj, None)

def get_transactions(entity_id, api_key):
    url = f"https://apis.bankconnect.finbox.in/bank-connect/v1/entity/{entity_id}/transactions/"
    headers = {
        'x-api-key': api_key
    }
    response = requests.request("GET", url, headers=headers)
    return response.json().get("transactions", []), response.json().get("accounts", [{}])[0].get("bank", "")

CATEGORISATION_CRON_LOCKFILE = '/tmp/categorisation_cron_job.lock'
async def perform_categorisation_check():
    if os.path.exists(CATEGORISATION_CRON_LOCKFILE):
        print("Another instance is already running, skipping for this worker")
        return
    
    with open(CATEGORISATION_CRON_LOCKFILE, "w") as f:
        pass

    time.sleep(random.randint(1, 100))
    job_executed = redis_cli.get("categorisation_job")
    if job_executed == 1:
        print("Categorisation Job is already scheduled")
        os.remove(CATEGORISATION_CRON_LOCKFILE)
        return
    print("-- Performing Categorisation JOB --")
    redis_cli.set("categorisation_job", 1, 600)
    download_files_from_s3(API_KEYS_S3_PATH, "api_keys.xlsx")
    download_files_from_s3(REFERENCE_DATA_S3_URI, "reference_data.xlsx")
    print("-- DATA FILES DOWNLOADED FROM S3 --")   

    df = pd.read_excel("api_keys.xlsx")
    reference_data = pd.read_excel("reference_data.xlsx")

    print("-- DATA FILES READ AS DATAFRAMES -- ")
    
    bank_connect_entity_ids = list(df['b_entity_id'])
    bank_connect_entity_ids = sorted(bank_connect_entity_ids)
    bank_connect_entity_ids = bank_connect_entity_ids[:NUM_USERS]
    await portal_db.disconnect()
    await portal_db.connect()
    query = f"""
        select e.entity_id, s.statement_id, s.bank_name
        from
            bank_connect_statement s
            join bank_connect_entity e on s.entity_id = e.id
        where
            e.entity_id in {tuple(bank_connect_entity_ids)}
            and e.created_at>'2023-12-31'
            and bank_name in ( 'sbi', 'hdfc', 'kotak', 'baroda', 'canara', 'ubi', 'pnbbnk', 'axis', 'icici', 'boi')
            order by e.created_at desc
    """
    query_data = await portal_db.fetch_all(query=query)
    res = []
    for i in range(len(query_data)):
        res.append(dict(query_data[i]))
    res = pd.DataFrame(res)

    print(f"-- Retrieved Data from prod about statements list, count is {len(res)} --")

    for index, items in res.iterrows():
        statement_id = items['statement_id']
        bank_name = items['bank_name']
        try:
            await retrigger_transactions_for_statement(statement_id, bank_name)
        except Exception:
            print("exception ", statement_id)
        if index % 2 == 0:
            print(f"Retriggered statements count : {index}")
    
    print("-- Retriggering statements completed for all statements --")
    bc_entity_id = list(set(res['entity_id']))
    filtered_df = df[df['b_entity_id'].isin(bc_entity_id)]

    print("-- Sleeping for 180 seconds --")
    time.sleep(180)

    files_to_delete = [
        "api_keys.xlsx",
        "reference_data.xlsx"
    ]
    
    print("-- Starting to Retrieve for all users --")

    for index, items in filtered_df.iterrows():
        try:
            p_entity_id = items['p_entity_id']
            api_key = items['api_key']
            p_transactions = get_transactions(p_entity_id, api_key)

            b_entity_id = items['b_entity_id']
            api_key = items['b_api_key']
            b_transactions = get_transactions(b_entity_id, api_key)
        except Exception as e:
            print(e)
            continue
        
        with open(f"{p_entity_id}.json", "w") as f:
            f.write(json.dumps(p_transactions))
            files_to_delete.append(f"{p_entity_id}.json")

        with open(f"{b_entity_id}.json", "w") as f:
            f.write(json.dumps(b_transactions))
            files_to_delete.append(f"{b_entity_id}.json")
        
        if index % 100 == 0:
            print(f"Fetch Transactions Complete for index : {index}")
    
    print("-- Retrieved transactions for all users, Starting to analyse --")

    final_set = []
    common_keys = ["amount", "balance", "date"]
    for index, items in filtered_df.iterrows():
        p_entity_id = items['p_entity_id']
        b_entity_id = items['b_entity_id']

        try:
            p_transactions = json.load(open(f"{p_entity_id}.json", "r"))
            b_transactions = json.load(open(f"{b_entity_id}.json", "r"))
        except Exception:
            continue
        
        p_transactions, bank_name = p_transactions
        b_transactions, bank_name = b_transactions

        p_transactions_map = {}
        b_transactions_map = {}
        for txn in p_transactions:
            key = tuple(txn[i] for i in common_keys)
            p_transactions_map[key] = txn

        for txn in b_transactions:
            key = tuple(txn[i] for i in common_keys)
            b_transactions_map[key] = txn


        for tup_key, d1 in p_transactions_map.items():
            d2 = b_transactions_map.get(tup_key, {})
            if d2:
                if "category" not in d2 or "perfios_txn_category" not in d1:
                    continue
                if "transfer" in d1["perfios_txn_category"].lower():
                    continue
                if "transfer" in d2.get("category").lower():
                    category = "transfer"
                else:
                    category = d2.get("category")
                final_set.append({
                    "bank_name": bank_name,
                    "perfios_txn_category": d1["perfios_txn_category"],
                    "category": category,
                    "transaction_note": d1['transaction_note'],
                    "transaction_type": d1["transaction_type"],
                    "b_transaction_note": d2['transaction_note'],
                    "p_entity_id": p_entity_id,
                    "transaction_channel": d2["transaction_channel"],
                    "description": d2["description"],
                    "merchant_category": d2["merchant_category"],
                    "amount": d2["amount"],
                    "balance": d2["balance"],
                    "date": d2["date"],
                    "hash": d2["hash"]
                })
    
    print("-- Analysis Completed Now trying to Generate Confusion Matrix --")
    confusion_matrix = pd.DataFrame(final_set)
    tag = datetime.now().strftime("%Y-%m-%d %H:%M")
    pd.crosstab(confusion_matrix["perfios_txn_category"], confusion_matrix["category"]).to_excel(f"report_{tag}.xlsx")
    confusion_matrix.to_excel(f"data_{tag}.xlsx")

    unique_categories = list(reference_data["perfios_txn_category"].unique())
    match_matrix = []
    for i in unique_categories:
        category_dict = {
            "category": i,
            "total_count": None,
            "matched": None,
            "mis-matched": None,
            "mis-matched-pct": None
        }
        filtered_ref_txns = reference_data[reference_data["correct_perfios_txn_category"]==i]
        category_dict["total_count"] = len(filtered_ref_txns)
        if category_dict["total_count"] == 0:
            print("Category with no data : ", i)
            continue
        hashes = list(filtered_ref_txns["hash"])
        reference_data_filtered_hash = confusion_matrix[confusion_matrix["hash"].isin(hashes)]
        category_dict["matched"] = len(reference_data_filtered_hash[reference_data_filtered_hash["category"]==i])
        category_dict["mis-matched"] = len(reference_data_filtered_hash[reference_data_filtered_hash["category"]!=i])
        category_dict["mis-matched-pct"] = round((category_dict["mis-matched"]/category_dict["total_count"])*100, 2)
        match_matrix.append(category_dict)
    
    match_df = pd.DataFrame(match_matrix)
    match_df = match_df.sort_values(by=['mis-matched-pct'], ascending=False)
    match_df = match_df.loc[:, ~match_df.columns.str.contains('^Unnamed')]
    match_df.to_excel("mis-matched_data.xlsx")
    print("-- analysis completed trying to send via the slack message --")

    MSME_CATEGORIES_LIST = [
        "Advance Salary",
        "Auto Loan",
        "Auto Loan Disbursed",
        "Bank Charges",
        "Below Min Balance",
        "Bounced I/W Cheque",
        "Bounced I/W Cheque Charges",
        "Bounced I/W ECS",
        "Bounced I/W ECS Charges",
        "Bounced I/W Payment",
        "Bounced O/W Cheque",
        "Bounced O/W Cheque Charges",
        "Bounced O/W ECS",
        "Card Settlement",
        "Cash Back",
        "Cash Deposit",
        "Cash Withdrawal",
        "Credit Card Payment",
        "Dividend",
        "EMI Payment",
        "Fixed Deposit",
        "Gold Loan",
        "Gold Loan Disbursed",
        "Home Loan",
        "Home Loan Disbursed",
        "Insurance",
        "Interest",
        "Investment Expense",
        "Investment Income",
        "Loan",
        "Loan Disbursed",
        "MF Purchase",
        "MF Redemption",
        "Penal Charges",
        "Personal Loan",
        "Salary",
        "Salary Paid",
        "Reversal",
        "Tax"
    ]
    
    filtered_msme = match_df[match_df["category"].isin(MSME_CATEGORIES_LIST)]
    message = filtered_msme.head(40).to_string(index=False)

    slack_client = WebClient(token=SLACK_TOKEN)
    response = slack_client.chat_postMessage(
        channel = SLACK_CHANNEL,
        text = f"```Single Categorisation Analysis (Top MSME Categories) ON ({NUM_USERS} users)\n {'-'*30}\n" + message +"\n```"
    )

    ts = response["ts"]

    message = match_df.head(40).to_string(index=False)
    slack_client.chat_postMessage(
        channel = SLACK_CHANNEL,
        thread_ts = ts,
        text = f"```Top Mismatch Categories\n {'-'*50}\n" + message +"\n```"
    )
    slack_client.files_upload_v2(
        channel= SLACK_CHANNEL,
        initial_comment = "Mis-Matched Matrix",
        file = "mis-matched_data.xlsx",
        thread_ts = ts
    )

    slack_client.files_upload_v2(
        channel= SLACK_CHANNEL,
        initial_comment = "Confusion Matrix for all categories",
        file = f"report_{tag}.xlsx",
        thread_ts = ts
    )

    slack_client.files_upload_v2(
        channel= SLACK_CHANNEL,
        initial_comment = "Data for all categories",
        file = f"data_{tag}.xlsx",
        thread_ts = ts
    )
    
    print("--analysis completed deleting all files--")
    files_to_delete += [
        f"report_{tag}.xlsx",
        f"data_{tag}.xlsx",
        "mis-matched_data.xlsx"
    ]
    for files in files_to_delete:
        try:
            os.remove(files)
        except Exception:
            continue
    
    os.remove(CATEGORISATION_CRON_LOCKFILE)