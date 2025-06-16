import pandas as pd
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.cluster import KMeans
import os, string, random
from app.conf import s3_resource, QUALITY_BUCKET, s3, CATEGORIZE_RS_SERVER_PRIVATE_IP
from app.database_utils import quality_database, clickhouse_client
import requests
import json
from copy import deepcopy

TRANSACTIONS_LIMIT = 20000

def id_generator(size=30, chars=string.ascii_uppercase + string.digits):
    return ''.join(random.choice(chars) for _ in range(size))

def put_file_to_s3(file_path, bucket, key, metadata = None):
    if metadata is None:
        s3_resource.Bucket(bucket).upload_file(file_path, key)
    else:
        s3_resource.Bucket(bucket).upload_file(file_path, key, ExtraArgs={'Metadata': metadata})
    return "s3://{}/{}".format(bucket, key)

async def categorise_transactions(data, transaction_type, bank_name):
    print(f"categorising transactions for {transaction_type} of {bank_name}")
    documents = data["transaction_note"].values.astype("U")
    vectorizer = TfidfVectorizer(stop_words='english')
    features = vectorizer.fit_transform(documents)
    k = 10
    model = KMeans(n_clusters=k, init='k-means++', max_iter=100, n_init=1)
    model.fit(features)
    data['cluster'] = model.labels_
    clusters = data.groupby('cluster')
    for cluster in clusters.groups:
        data_index = clusters.get_group(cluster)
        if not len(data_index):
            continue
        if not data_index["transaction_note"].to_list():
            continue
        cluster_id = id_generator()
        file_name = f"/tmp/{cluster_id}.csv"
        key = f"quality_clusters/{cluster_id}.csv"
        f = open(file_name, 'w')
        f.write(data_index.to_csv())
        f.close()
        uri_link = put_file_to_s3(
            file_name,
            QUALITY_BUCKET,
            key
        )
        insert_query = """
                        INSERT INTO transactions_quality (cluster_id, bank_name, transaction_type, cluster_link, cluster_sample_transaction_note)
                        VALUES (:cluster_id, :bank_name, :transaction_type, :cluster_link, :cluster_sample_transaction_note)
                    """
        values = {
            "cluster_id": cluster_id,
            "bank_name": bank_name,
            "transaction_type": transaction_type,
            "cluster_link": uri_link,
            "cluster_sample_transaction_note": data_index["transaction_note"].to_list()[0]
        }
        await quality_database.execute(
            insert_query,
            values=values
        )
        os.remove(file_name)

async def categorise_transactions_into_clusters(limit=20):
    query = f"""
                Select distinct bank_name, count(*)
                from bank_connect.transactions_new
                group by bank_name
                limit {limit}
            """
    bank_names = clickhouse_client.query_df(query)
    bank_names.fillna("", inplace=True)
    
    for index, items in bank_names.iterrows():
        bank_name = items["bank_name"]
        # check if any cluster of this particular is not solved yet
        check_status = f"""
                SELECT * from transactions_quality
                WHERE bank_name='{bank_name}'
                and requested_at is null
            """
        data = await quality_database.fetch_all(
            query=check_status
        )
        if data:
            print("pending clusters for this bank, not categorising anymore")
            continue
        get_uncategorised_transactions = f"""
                            Select * from bank_connect.transactions_new
                            where bank_name='{bank_name}' and category_regex is null
                            limit 2000000
                        """
        bank_transactions = clickhouse_client.query_df(get_uncategorised_transactions)
        bank_transactions.fillna("", inplace=True)
        debit_transactions = bank_transactions[bank_transactions["transaction_type"]=="debit"]
        credit_transactions = bank_transactions[bank_transactions["transaction_type"]=="credit"]
        await categorise_transactions(debit_transactions, "debit", bank_name)
        await categorise_transactions(credit_transactions, "credit", bank_name)

async def run_through_regexes_for_cluster(cluster_id, regex, capturing_group_details):
    cluster_data_query = f"""
            SELECT * from transactions_quality
            WHERE cluster_id = '{cluster_id}' and approved_at is null limit 1
        """
    cluster_query_data = await quality_database.fetch_one(
        query=cluster_data_query
    )

    response_data = {
        "message": None,
        "summary": {},
        "data": []
    }
    if cluster_query_data is None:
        response_data["message"] = "invalid cluster id"
        return response_data
    
    cluster_query_data = dict(cluster_query_data)
    key = cluster_query_data["cluster_link"].replace("s3://bank-connect-quality-prod/", "")
    pdf_bucket_response = s3.get_object(Bucket=QUALITY_BUCKET, Key=key)
    
    file_path = f"/tmp/{cluster_id}.csv"
    with open(file_path, 'wb') as file_obj:
        file_obj.write(pdf_bucket_response['Body'].read())
    
    data = pd.read_csv(file_path)
    data.fillna('', inplace=True)
    number_of_capturing_groups = len(capturing_group_details)
    req_metadata = list(capturing_group_details.values())

    # final_data = []
    # for index, items in data.iterrows():
    #     transaction_note = r'{}'.format(items["transaction_note"])
    #     data = {
    #         "transaction_note": transaction_note
    #     }
    #     match = re.search(regex, transaction_note)
    #     for x in range(1, number_of_capturing_groups+1):
    #         match_data = None
    #         try:
    #             match_data = match.group(x)
    #         except Exception:
    #             pass
    #         data[capturing_group_details[str(x)]] = match_data
    #     final_data.append(data)

    # response_data["data"] = final_data

    payload = {
        "regex": regex,
        "bank_name": cluster_query_data["bank_name"],
        "transaction_type": cluster_query_data["transaction_type"],
        "transactions": data.to_dict("records")[:TRANSACTIONS_LIMIT],
        "account_type": None,
        "creditor_name": None,
        "creditor_ifsc": None,
        "creditor_upi_handle": None,
        "creditor_bank": None,
        "creditor_account_number": None,
        "receiver_name": None,
        "receiver_ifsc": None,
        "receiver_upi_handle": None,
        "receiver_bank": None,
        "reciever_account_number": None,
        "merchant_name": None,
        "merchant_ifsc": None,
        "merchant_upi_handle": None,
        "merchant_bank": None,
        "cheque_number": None,
        "transaction_reference_1": None,
        "transaction_reference_2": None,
        "primary_channel": None,
        "secondary_channel": None,
        "tertiary_channel": None,
        "raw_location": None,
        "transaction_timestamp": None,
        "transaction_amount": None,
        "currency": None
    }
    for key, value in capturing_group_details.items():
        if value in payload:
            if not isinstance(key, int):
                payload[value] = int(key)
    
    url = f"http://{CATEGORIZE_RS_SERVER_PRIVATE_IP}/test_regex"
    payload = json.dumps(payload)
    headers = {
        'Content-Type': 'application/json'
    }
    response = requests.request("POST", url, headers=headers, data=payload)
    final_data = []
    try:
        data = response.json()
        data = data["transactions"]
        required_keys = ["transaction_note"] + list(capturing_group_details.values())
        for d in data:
            new_obj = deepcopy(d)
            for i in d.keys():
                if i not in required_keys:
                    new_obj.pop(i)
            final_data.append(new_obj)
        response_data["data"] = final_data
    except Exception as e:
        print(e)
        response_data["message"] = "invalid regex"
    os.remove(file_path)
    return response_data