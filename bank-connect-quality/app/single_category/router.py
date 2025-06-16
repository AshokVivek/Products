from app.template_solutioning.dashboard_calls import fb_dashboard_api_create_or_update_category_regex
from fastapi import APIRouter, Depends, Response, status, BackgroundTasks
from app.dependencies import get_current_user
from app.conf import s3, QUALITY_BUCKET, redis_cli, COUNTRY
from app.database_utils import quality_database, portal_db, clickhouse_client
import json
import pandas as pd
import requests
import os
from typing import Optional
from uuid import uuid4
from app.single_category.models import RequestRegexAddition, RemoveFromCluster, AddMetadata, ApproveRegex, AssignCluster, SuperUserApproval, TestRegex, StartCategorisation, CreateSpecificCluster
from app.single_category.single_category_helper import put_file_to_s3, run_through_regexes_for_cluster, categorise_transactions_into_clusters, id_generator, TRANSACTIONS_LIMIT
from app.template_dashboard.utils import create_presigned_url_by_bucket
from datetime import datetime

single_category_router = APIRouter()


@single_category_router.get("/metadata_list", tags=["single_category"])
async def get_metadata_list(response: Response, user=Depends(get_current_user)):
    query = """
                SELECT category, category_description from metadata_categories
                WHERE is_active=true
            """
    data = await quality_database.fetch_all(query)
    for i in range(len(data)):
        data[i] = dict(data[i])
    return {
        "message": None,
        "data": data
    }

@single_category_router.get("/inferred_category_list", tags=["single_category"])
async def get_inferred_category_list(response: Response, user=Depends(get_current_user)):
    query = """
                SELECT category, category_description from inferred_categories
                WHERE is_active=true
            """
    data = await quality_database.fetch_all(query)
    for i in range(len(data)):
        data[i] = dict(data[i])
    return {
        "message": None,
        "data": data
    }


@single_category_router.post("/add_metadata", tags=["single_category"])
async def add_metadata(
    response: Response,
    request: AddMetadata,
    user = Depends(get_current_user)
):
    if user.user_type != "superuser":
        response.status_code = status.HTTP_401_UNAUTHORIZED
        return {"message": "not authorised"}
    
    category = request.category
    category_description = request.category_description
    insert_query = """
                    INSERT INTO metadata_categories (category, category_description, added_by)
                    VALUES (:category, :category_description, :added_by)
                """
    await quality_database.execute(
        insert_query,
        values={
            "category": category,
            "category_description": category_description,
            "added_by": user.username
        }
    )
    return {
        "message": "added successfully"
    }

@single_category_router.post("/remove_from_cluster", tags=["single_category"])
async def remove_from_cluster(
    response: Response,
    request: RemoveFromCluster,
    user = Depends(get_current_user)
):
    cluster_id = request.cluster_id
    hash_list = request.hash_list
    cluster_query = f"""
                        SELECT cluster_id, cluster_link
                        FROM transactions_quality
                        WHERE cluster_id='{cluster_id}'
                    """
    data = await quality_database.fetch_one(
        query=cluster_query
    )
    if not data:
        return {
            "message": "invalid cluster id"
        }
    data = dict(data)
    key = data["cluster_link"].replace("s3://bank-connect-quality-prod/", "")
    pdf_bucket_response = s3.get_object(Bucket=QUALITY_BUCKET, Key=key)
    
    file_path = f"/tmp/{cluster_id}.csv"
    with open(file_path, 'wb') as file_obj:
        file_obj.write(pdf_bucket_response['Body'].read())
    
    data = pd.read_csv(file_path)
    data = data[~data["hash"].isin(hash_list)]
    data.to_csv(file_path)
    key = f"quality_clusters/{cluster_id}.csv"
    put_file_to_s3(
        file_path,
        QUALITY_BUCKET,
        key
    )
    return {
        "message": "removed transactions from hash",
        "cluster_id": cluster_id
    }



@single_category_router.post("/request_regexes", tags=["single_category"])
async def request_regexes(
    response: Response, 
    request: RequestRegexAddition, 
    user=Depends(get_current_user)
):
    cluster_id = request.cluster_id
    regex = request.regex
    capturing_group_details = request.capturing_group_details
    inferred_category = request.inferred_category
    transaction_channel_tag = request.transaction_channel_tag
    merchant_category_tag = request.merchant_category_tag
    description_tag = request.description_tag

    get_details_from_cluster_query = f"""
                SELECT * from transactions_quality
                WHERE cluster_id='{cluster_id}' and approved_at is null limit 1
            """
    cluster_query_data = await quality_database.fetch_one(
        query=get_details_from_cluster_query
    )
    if cluster_query_data is None:
        response.status_code=status.HTTP_400_BAD_REQUEST
        return {
            "cluster_id": cluster_id,
            "message": "invalid cluster id"
        }
    cluster_query_data = dict(cluster_query_data)
    bank_name = cluster_query_data["bank_name"]
    transaction_type = cluster_query_data["transaction_type"]
    capturing_group_details["inferred_category"] = inferred_category
    old_category_regex = None

    if transaction_channel_tag:
        # check if the transaction_channel_tag is valid
        # checking in redis first
        redis_key = f"transaction_channel_list_{COUNTRY}_{bank_name}_{transaction_type}"
        tcr = redis_cli.get(redis_key)
        if tcr is not None:
            tcr = json.loads(tcr)
        else:
            portal_query = """
                    select distinct transaction_channel from bank_connect_fsmlibtransactionchannels
                    where country=:country and bank_name=:bank_name and transaction_type=:transaction_type
                """
            tcd = await portal_db.fetch_all(query=portal_query, values={
                    "country": COUNTRY,
                    "bank_name": bank_name,
                    "transaction_type": transaction_type
                })
            tcr = []
            for i in range(0, len(tcd)):
                tcd[i] = dict(tcd[i])
                tcr.append(tcd[i]["transaction_channel"])
            redis_cli.set(redis_key, json.dumps(tcr), 86400)
        if transaction_channel_tag is not None and transaction_channel_tag not in tcr:
            response.status_code = status.HTTP_400_BAD_REQUEST
            return {
                "cluster_id": cluster_id,
                "message": "unrecognized transaction channel tag"
            }
        old_category_regex = {
            "type": "transaction_channel",
            "bank_name": bank_name,
            "transaction_type": transaction_type,
            "tag": transaction_channel_tag
        }
    elif merchant_category_tag:
        redis_key = f"transaction_channel_list_{COUNTRY}"
        mcr = redis_cli.get(redis_key)
        if mcr is not None:
            mcr = json.loads(mcr)
        else:
            portal_query = """
                    select distinct merchant_category from bank_connect_fsmlibmerchantcategory
                    where country = :country and merchant_category not ilike '%regex%'
                """
            mcd = await portal_db.fetch_all(query=portal_query, values={
                "country": COUNTRY
            })
            mcr = []
            for i in mcd:
                i = dict(i)
                mcr.append(i["merchant_category"])
            redis_cli.set(redis_key, json.dumps(mcr), 86400)
        if merchant_category_tag is not None and merchant_category_tag not in mcr:
            response.status_code = status.HTTP_400_BAD_REQUEST
            return {
                "cluster_id": cluster_id,
                "message": "unrecognized merchant category tag"
            }
        old_category_regex = {
            "type": "merchant_category",
            "bank_name": bank_name,
            "transaction_type": transaction_type,
            "tag": merchant_category_tag+"_regex"
        }
    
    elif description_tag:
        redis_key = f"description_list_{COUNTRY}"
        dtr = redis_cli.get(redis_key)
        if dtr is not None:
            dtr = json.loads(dtr)
        else:
            portal_query = """
                select distinct type from bank_connect_fsmlibgeneraldata
                where country = :country and tag='description'
            """
            dtd = await portal_db.fetch_all(query=portal_query, values={
                "country": COUNTRY
            })
            dtr = []
            for i in dtd:
                dtr.append(dict(i)["type"])
            redis_cli.set(redis_key, json.dumps(dtr), 86400)
        if description_tag is not None and description_tag not in dtr:
            response.status_code = status.HTTP_400_BAD_REQUEST
            return {
                "cluster_id": cluster_id,
                "message": "unrecognized description tag"
            }
        old_category_regex = {
            "type": "description",
            "tag": description_tag,
            "bank_name": bank_name,
            "transaction_type": transaction_type
        }

    if isinstance(old_category_regex, dict):
        old_category_regex = json.dumps(old_category_regex)
    
    insert_query_into_category_regex_table = """
                        INSERT INTO category_regex (bank_name, transaction_type, regex, capturing_group_details, requested_by, cluster_id, old_category_regex)
                        VALUES (:bank_name, :transaction_type, :regex, :capturing_group_details, :requested_by, :cluster_id, :old_category_regex)
                    """
    await quality_database.execute(
        insert_query_into_category_regex_table,
        values={
            "bank_name": bank_name,
            "transaction_type": transaction_type,
            "regex": regex,
            "capturing_group_details": json.dumps(capturing_group_details),
            "requested_by": user.username,
            "cluster_id": cluster_id,
            "old_category_regex": old_category_regex
        }
    )

    update_cluster_requested_at_query = """
                        UPDATE transactions_quality SET requested_at=:requested_at, cluster_regex=:cluster_regex,
                        cluster_capturing_group_details=:capturing_group_details
                        WHERE cluster_id=:cluster_id
                    """
    await quality_database.execute(
        update_cluster_requested_at_query,
        values={
            "cluster_id": cluster_id,
            "cluster_regex": regex,
            "capturing_group_details": json.dumps(capturing_group_details),
            "requested_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }
    )

    return {
        "cluster_id": cluster_id,
        "message": "request successfully submitted"
    }


@single_category_router.post("/approve_regexes", tags=["single_category"])
async def approve_regexes(
    response: Response, 
    request: ApproveRegex, 
    user=Depends(get_current_user)
):
    if user.user_type != "superuser":
        response.status_code = status.HTTP_401_UNAUTHORIZED
        return {"message": "not authorised"}
    pass


@single_category_router.post("/test_regex", tags=["single_category"])
async def test_regex(
    response: Response,
    request: TestRegex,
    user=Depends(get_current_user)
):
    cluster_id = request.cluster_id
    regex = request.regex
    capturing_group_details = request.capturing_group_details
    print("Capturing group details : ", capturing_group_details)
    simulated_data = await run_through_regexes_for_cluster(cluster_id, regex, capturing_group_details)
    if simulated_data["message"] is not None:
        response.status_code=status.HTTP_400_BAD_REQUEST
    return simulated_data

@single_category_router.post("/start_categorisation", tags=["single_category"])
async def start_categorisation(
    request: StartCategorisation,
    response: Response,
    background_tasks: BackgroundTasks,
    user=Depends(get_current_user)
):
    if user.user_type!="superuser":
        response.status_code=status.HTTP_401_UNAUTHORIZED
        return {"message": "not authorised"}
    limit = request.limit
    is_categorisation_underway = redis_cli.get("categorisation_process")
    if is_categorisation_underway == "1":
        return {
            "message": "categorisation already in progress.",
            "data": []
        }
    redis_cli.set("categorisation_process", 1, ex=7200)
    background_tasks.add_task(categorise_transactions_into_clusters, limit)
    redis_cli.set("categorisation_process", 0, ex=7200)
    response.status_code = 200
    return {
        "message": "categorisation in progress"
    }


@single_category_router.post("/create_specific_cluster", tags=["single_category"])
async def create_specific_cluster(
    request: CreateSpecificCluster,
    response: Response,
    user=Depends(get_current_user)
):
    bank_name = request.bank_name
    transaction_type = request.transaction_type
    sample_transaction_note = request.sample_transaction_note
    regex = request.regex
    import re
    try:
        re.compile(regex)
    except Exception as e:
        response.status_code = status.HTTP_400_BAD_REQUEST
        return {
            "message": e,
            "cluster_id": None
        }
    query_to_get_transactions = f"""
                SELECT *, match(transaction_note, '{regex}') as m
                FROM bank_connect.transactions
                WHERE m=1 and bank_name='{bank_name}' and transaction_type='{transaction_type}'
                LIMIT 5000
            """
    bank_transactions = clickhouse_client.query_df(query_to_get_transactions)
    bank_transactions.fillna("", inplace=True)
    cluster_id = id_generator()
    file_name = f"/tmp/{cluster_id}.csv"
    key = f"quality_clusters/{cluster_id}.csv"
    f = open(file_name, 'w')
    f.write(bank_transactions.to_csv())
    f.close()
    uri_link = put_file_to_s3(
        file_name,
        QUALITY_BUCKET,
        key
    )
    insert_query = """
                    INSERT INTO transactions_quality (cluster_id, bank_name, transaction_type, cluster_link, cluster_sample_transaction_note, cluster_allotted_to)
                    VALUES (:cluster_id, :bank_name, :transaction_type, :cluster_link, :cluster_sample_transaction_note, :cluster_allotted_to)
                """
    values = {
        "cluster_id": cluster_id,
        "bank_name": bank_name,
        "transaction_type": transaction_type,
        "cluster_link": uri_link,
        "cluster_sample_transaction_note": sample_transaction_note,
        "cluster_allotted_to": user.username
    }
    await quality_database.execute(
        insert_query,
        values=values
    )
    os.remove(file_name)
    return {
        "message": "cluster created",
        "cluster_id": cluster_id
    }


@single_category_router.get("/get_clusters_info", tags=["single_category"])
async def get_clusters_info(
    response: Response,
    user = Depends(get_current_user)
):
    is_superuser = user.user_type=="superuser"
    query = """
                SELECT distinct bank_name, transaction_type, count(*) as count
                FROM transactions_quality
                WHERE approved_at IS null
                GROUP BY  bank_name, transaction_type
                ORDER BY count DESC
            """
    if not is_superuser:
        query = f"""
                SELECT distinct bank_name, transaction_type, count(*) as count
                FROM transactions_quality
                WHERE approved_at IS null
                AND cluster_allotted_to='{user.username}'
                GROUP BY  bank_name, transaction_type
                ORDER BY count DESC
            """
    query_result = await quality_database.fetch_all(
        query=query,
        values={}
    )
    data = []
    for i in query_result:
        data.append(dict(i))
    
    return {
        "message": None,
        "data": data
    }

@single_category_router.get("/get_clusters", tags=["single_category"])
async def get_clusters(
    response: Response,
    cluster_id: Optional[str]=None,
    bank_name: Optional[str]=None,
    transaction_type: Optional[str]=None,
    user=Depends(get_current_user)
):
    is_superuser = user.user_type=="superuser"
    if not (cluster_id or (bank_name and transaction_type)):
        if not is_superuser:
            response.status_code = status.HTTP_400_BAD_REQUEST
            return {
                "message": "please either provide cluster id or a combination of bank name and transaction type"
            }
    query = """
                SELECT * from transactions_quality
                WHERE approved_at is null
                and requested_at is null
            """
    if cluster_id:
        query += f" and cluster_id='{cluster_id}'"
    elif bank_name:
        query += f" and bank_name='{bank_name}' and transaction_type='{transaction_type}'"
    
    if not is_superuser:
        query += f" and cluster_allotted_to='{user.username}'"

    query_result = await quality_database.fetch_all(
        query=query, 
        values= {}
    )
    data = []
    for i in query_result:
        item = dict(i)
        if "cluster_capturing_group_details" in item and item["cluster_capturing_group_details"] is not None and not isinstance(item["cluster_capturing_group_details"], dict):
            item["cluster_capturing_group_details"] = json.loads(item["cluster_capturing_group_details"])
        
        # change the s3_link to the presigned url at this path
        item["cluster_link"] = create_presigned_url_by_bucket(QUALITY_BUCKET, item["cluster_link"].replace("s3://bank-connect-quality-prod/", ""), 3600)

        # response = requests.get(item["cluster_link"])
        # tmp_file_name = f"{str(uuid4())}.csv"
        # with open(tmp_file_name, mode="wb") as file:
        #     file.write(response.content)
        # df = pd.read_csv(tmp_file_name)
        # df = df[['statement_id', 'transaction_note']]
        # df.fillna('', inplace=True)
        # item["transactions"] = df.to_dict("records")
        # os.remove(tmp_file_name)

        data.append(item)
    response.status_code = status.HTTP_200_OK
    return {
        "message": None,
        "data": data
    }

@single_category_router.get("/get_cluster_transactions", tags=["single_category"])
async def get_cluster_transactions(
    response: Response,
    cluster_id: Optional[str]=None,
    user=Depends(get_current_user)
):
    query = f"""
                SELECT * from transactions_quality
                WHERE approved_at is null
                AND cluster_id = '{cluster_id}'
            """
    data = await quality_database.fetch_one(
        query=query
    )
    if not data:
        return {
            "message": "invalid cluster id",
            "requested_at": None,
            "transactions": []
        }
    data = dict(data)
    cluster_link = data["cluster_link"]
    cluster_link = create_presigned_url_by_bucket(
        QUALITY_BUCKET, 
        cluster_link.replace("s3://bank-connect-quality-prod/", ""), 
        3600
    )
    response = requests.get(cluster_link)
    tmp_file_name = f"{str(uuid4())}.csv"
    with open(tmp_file_name, mode="wb") as file:
        file.write(response.content)
    df = pd.read_csv(tmp_file_name)
    df = df[['statement_id', 'transaction_note', 'hash']]
    df.fillna('', inplace=True)
    transactions = df.to_dict("records")
    os.remove(tmp_file_name)
    return {
        "message": None,
        "requested_at": data["requested_at"],
        "transactions": transactions[:TRANSACTIONS_LIMIT]
    }

@single_category_router.post("/assign_cluster", tags=["single_category"])
async def assign_cluster(
    response: Response,
    request: AssignCluster,
    user=Depends(get_current_user)
):
    if user.user_type != "superuser":
        response.status_code = status.HTTP_401_UNAUTHORIZED
        return {"message": "not authorised"}
    update_cluster_alloted_at_query = """
                update transactions_quality
                set cluster_allotted_to=:cluster_allotted_to
                where cluster_id = :cluster_id
            """
    await quality_database.execute(
        update_cluster_alloted_at_query,
        values={
            "cluster_id": request.cluster_id,
            "cluster_allotted_to": request.username
        }
    )
    return {
        "message": "assigned successfully",
    }

@single_category_router.get("/requested_clusters", tags=["single_category"])
async def requested_clusters(
    response: Response,
    user=Depends(get_current_user)
):
    if user.user_type != "superuser":
        response.status_code = status.HTTP_401_UNAUTHORIZED
        return {"message": "not authorised"}
    
    query = """
                SELECT cr.regex, cr.capturing_group_details, cr.requested_by, cr.old_category_regex
                tq.cluster_sample_transaction_note, tq.cluster_link, tq.cluster_id, 
                FROM category_regex as cr
                JOIN transactions_quality as tq ON tq.cluster_id=cr.cluster_id
                where requested_at is not null;
            """
    query_result = await quality_database.fetch_all(
        query=query
    )

    if not query_result:
        return {
            "message": "no requested clusters",
            "data": []
        }
    
    clusters = []
    for i in query_result:
        item = dict(i)
        item["cluster_link"] = create_presigned_url_by_bucket(
            QUALITY_BUCKET, 
            item["cluster_link"].replace("s3://bank-connect-quality-prod/", ""), 
            3600
        )
        if item["old_category_regex"]:
            item["old_category_regex"] = json.loads(item["old_category_regex"])
        
        clusters.append(item)
    
    response.status_code = status.HTTP_200_OK
    return {
        "data": clusters,
        "message": None,
    }

@single_category_router.post("/superuser_approval", tags=["single_category"])
async def superuser_approval(
    response: Response,
    request: SuperUserApproval,
    user=Depends(get_current_user)
):
    if user.user_type != "superuser":
        response.status_code = status.HTTP_401_UNAUTHORIZED
        return {"message": "not authorised"}
    
    cluster_id = request.cluster_id
    approval = request.approval

    category_regex_query = """SELECT tq.id, tq.bank_name, tq.cluster_regex, tq.cluster_capturing_group_details, tq.transaction_type,
                tq.requested_at, tq.approved_at, cr.old_category_regex from transactions_quality tq 
                join cluster_regex cr on tq.cluster_id=cr.cluster_id 
                where tq.cluster_id = :cluster_id"""
    cluster_data = await quality_database.fetch_one(query=category_regex_query, values={"cluster_id": cluster_id})

    if not cluster_data:
        response.status_code = status.HTTP_404_NOT_FOUND
        return {"message": "cluster not found"}
    
    if cluster_data.get('approved_at', None):
        response.status_code = status.HTTP_404_NOT_FOUND
        return {"message": "cluster is already approved"}
    
    if not approval:
        update_cluster_requested_at_query = """
            update transactions_quality
            set requested_at=:requested_at
            where cluster_id=:cluster_id
        """
        await quality_database.execute(
            update_cluster_requested_at_query,
            values={
                "cluster_id": cluster_id,
                "requested_at": None
            }
        )
        return {
            "message": "approval rejected"
        }

    if not cluster_data.get('requested_at', None):
        response.status_code = status.HTTP_404_NOT_FOUND
        return {"message": "cluster is not requested for approval"}
    
    if not cluster_data.get('cluster_regex', None):
        response.status_code = status.HTTP_404_NOT_FOUND
        return {"message": "regex not found"}
    
    cluster_data = dict(cluster_data)
    capturing_group_details = cluster_data.get("cluster_capturing_group_details", None)
    if not capturing_group_details:
        return {
            "error": "capturing_group_details is empty"
        }
    capturing_group_details = json.loads(capturing_group_details)
    inferred_category = None
    if "inferred_category" in capturing_group_details:
        inferred_category = capturing_group_details.pop("inferred_category")
        if 'null' in inferred_category:
            inferred_category = None
    
    old_category_regex = None
    old_category_regex = cluster_data.get("old_category_regex")
    if old_category_regex is not None:
        old_category_regex = json.loads(old_category_regex)
    
    reformatted_capturing_group_details = {v:k for k,v in capturing_group_details.items()}
    mapping_body = {
        "regex_id": cluster_data.get('id') if cluster_data else f"account_category_mapping_{str(uuid4())}",
        "regex": cluster_data.get('cluster_regex'),
        "template_json": reformatted_capturing_group_details,
        "transaction_type": cluster_data.get('transaction_type'),
        "bank_name": cluster_data.get('bank_name'),
        "operation": 'create',
        "inferred_category": inferred_category,
        "old_category_regex": old_category_regex
    }

    fb_dashboard_api_create_or_update_category_regex(mapping_body)

    update_cluster_approved_at_query = """
        update transactions_quality
        set approved_at=:approved_at, cluster_allotted_to=:cluster_allotted_to, requested_at=:requested_at
        where cluster_id = :cluster_id
    """
    await quality_database.execute(
        update_cluster_approved_at_query,
        values={
            "cluster_id": cluster_id,
            "approved_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "requested_at": None,
            "cluster_allotted_to": None
        }
    )
    response.status_code = 200
    return {
        "message": "regex approved"
    }
