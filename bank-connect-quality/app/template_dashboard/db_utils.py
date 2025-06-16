import datetime
import json
from typing import List, Optional
from ..database_utils import portal_db
from datetime import datetime
from .models.request import StatementEntityData, StatementFromToData
from fastapi.encoders import jsonable_encoder


async def get_all_templates(bank_name: str, template_type: str):
    #function to get all templates for a bank and corresponding template_type

    redash_query = """
    SELECT
        template_uuid,template_json
        FROM 
        bank_connect_fsmlibtemplates
        WHERE
            template_type = :template_type
            AND
            bank_name = :bank_name
    """

    redash_query_values = {
        "template_type": template_type,
        "bank_name": bank_name
    }
    #querying to pg
    try:
        query_result = await portal_db.fetch_all(query=redash_query, values=redash_query_values)
        if not query_result:
            return None
        else:
            return query_result
    except Exception as e:
        print("Failed to get db values")
        return None


async def add_template_to_db(templateUUID: str, template: dict, bank_name: str, template_type: str):
    #function to add template to pg

    redash_query = """
        INSERT INTO bank_connect_fsmlibtemplates
        (template_uuid, template_type, template_json, bank_name, is_active, created_at)
        VALUES('{}','{}','{}','{}',{},'{}')
    """.format(templateUUID, template_type, json.dumps(jsonable_encoder(template)), bank_name.upper(), True, datetime.now())
    try:
        await portal_db.execute(redash_query)
        return True
    except Exception as e:
        print("Failed to Update DB: " + str(e))
        return False


async def get_identity_data_from_statement(statement_id: str = "") -> Optional[List[StatementEntityData]]:
    #function to get full statement info from pg 
    if statement_id == "":
        return None

    pg_query = """
       SELECT bcs.statement_id, bcs.pdf_password, bcs.bank_name, bcs.from_date, bcs.to_date, bcs.is_extracted,
       bci.name, bci.account_number, bci.address, bci.ifsc, bci.micr  FROM bank_connect_statement as bcs 
       LEFT JOIN bank_connect_identity as bci ON bcs.id = bci.statement_id WHERE bcs.statement_id = :statement_id
       """

    pg_query_values = {
        "statement_id": statement_id
    }
    #fetching data from postgress
    query_result = await portal_db.fetch_one(query=pg_query, values=pg_query_values)

    if query_result is None:
        return None

    statement_identity_data = StatementEntityData(**dict(query_result.items()))

    print(statement_identity_data)
    return statement_identity_data


async def get_statements_postgress_data_time_range(from_date: datetime, to_date: datetime, bbox_type: str = "") -> Optional[List[StatementFromToData]]:
    # function to get unextracted statement data between date range
    if bbox_type == 'date':
        table_alias = 'bcs'
        bbox_type = 'from_date'
        condition1 = "{table_alias}.{bbox_type} is null".format(table_alias=table_alias,bbox_type=bbox_type)
    else:
        table_alias = 'bci'
        condition1 = "({table_alias}.{bbox_type} is null OR {table_alias}.{bbox_type} = '')".format(table_alias=table_alias,bbox_type=bbox_type)

    pg_query = """
        SELECT
            bcs.statement_id,
            bcs.pdf_password,
            bcs.bank_name,
            {table_alias}.{bbox_type}
        FROM
            bank_connect_statement bcs
        LEFT JOIN
            bank_connect_identity bci
        ON
            bcs.id = bci.statement_id
        WHERE
            bcs.created_at::timestamptz at time zone 'Asia/Calcutta'
        BETWEEN
            :from_date AND :to_date
        AND
            {condition1} 
        AND
            bcs.is_extracted = True
        AND
            bcs.is_extracted_by_perfios = False
        AND
            bcs.attempt_type != 'aa'
        AND
            bcs.statement_status not in (1, 2)

    """.format(table_alias=table_alias, bbox_type=bbox_type, condition1=condition1)

    #print("pg_quesy: ", pg_query)
    pg_query_values = {
        "from_date": from_date,
        "to_date": to_date,
    }

    query_result = await portal_db.fetch_all(query=pg_query, values=pg_query_values)

    if query_result is None:
        return None

    return query_result

async def get_transactions_data_time_range(from_date: datetime, to_date: datetime, bbox_type: str = "") -> Optional[List[StatementFromToData]]:
    # function to get unextracted statement data between date range
    
    pg_query = """
        SELECT
            statement_id,
            pdf_password,
            bank_name,
            is_extracted 
        FROM
            bank_connect_statement 
        WHERE
            created_at::timestamptz at time zone 'Asia/Calcutta'
        BETWEEN
            :from_date AND :to_date
        AND
            is_extracted is false
    """

    #print("pg_quesy: ", pg_query)
    pg_query_values = {
        "from_date": from_date,
        "to_date": to_date,
    }

    query_result = await portal_db.fetch_all(query=pg_query, values=pg_query_values)

    if query_result is None:
        return None

    return query_result



async def get_bank_name_from_statement_id(statement_id: str, is_cc=False):
    # fetch bank_name from postgress against a statement_id
    pg_query = f"SELECT bank_name FROM {'bank_connect_creditcardstatement' if is_cc else 'bank_connect_statement'} WHERE statement_id = :statement_id"

    pg_query_values = {
        "statement_id": statement_id
    }

    query_result = await portal_db.fetch_one(query=pg_query, values=pg_query_values)
    if query_result is None:
        return None
    
    result = dict(query_result.items())

    return result


async def get_new_template_data_from_db(template_id: str):
    # fetch newly added template

    pg_query = "SELECT template_uuid, template_type, template_json FROM bank_connect_fsmlibtemplates WHERE template_uuid = :template_id"

    pg_query_values = {
        "template_id": template_id
    }
    query_result = await portal_db.fetch_one(query=pg_query, values=pg_query_values)
    result = dict(query_result.items())

    if query_result is None:
        return None

    return result

async def get_logo_test_cases_from_pg(from_date : datetime,to_date: datetime):
    pg_query = "SELECT concat(bank_name,'-',predicted_bank) , STATEMENT_ID FROM bank_connect_statement WHERE is_logo_mismatch = TRUE  AND created_at >= :from_date AND created_at <= :to_date "

    pg_query_values = {
        "from_date": from_date,
        "to_date" : to_date
    }

    query_result = await portal_db.fetch_all(query=pg_query, values=pg_query_values)
    if query_result is None:
        return None
    return query_result

async def get_logo_null_test_cases_from_pg(from_date : datetime,to_date: datetime):
    pg_query = """SELECT bank_name, statement_id
FROM bank_connect_statement join bank_connect_entity on bank_connect_statement.entity_id = bank_connect_entity.id WHERE predicted_bank IS NULL
  AND is_extracted_by_perfios = FALSE
  AND is_external_aa_data = FALSE
  AND keyword_all = TRUE
  and attempt_type != 'aa'
  AND bank_connect_statement.created_at >= :from_date and bank_connect_statement.created_at <= :to_date"""

    pg_query_values = {
        "from_date": from_date,
        "to_date" : to_date
    }

    query_result = await portal_db.fetch_all(query=pg_query, values=pg_query_values)
    if query_result is None:
        return None
    return query_result

async def get_date_null_test_cases_from_pg(from_d : datetime,to_d: datetime):
    pg_query="""SELECT bank_name, statement_id,pdf_password
    FROM bank_connect_statement
    WHERE created_at >= :from_d
    AND created_at <= :to_d
    AND statement_status=0
    AND from_date ISNULL
    AND is_extracted_by_perfios=FALSE
    AND attempt_type='pdf'
    and keyword_all=true"""
    pg_query_values = {
        "from_d": from_d,
        "to_d" : to_d
    }

    query_result = await portal_db.fetch_all(query=pg_query, values=pg_query_values)
    if query_result is None:
        return None
    return query_result

async def get_Account_null_test_cases_from_pg(from_date : datetime,to_date: datetime):
    pg_query = """SELECT *
FROM bank_connect_statement
WHERE statement_status=12
  AND created_at>= :from_date
  AND created_at<= :to_date
  AND is_extracted_by_perfios = FALSE
  AND is_external_aa_data = FALSE
  AND attempt_type != 'aa'"""

    pg_query_values = {
        "from_date": from_date,
        "to_date" : to_date
    }

    query_result = await portal_db.fetch_all(query=pg_query, values=pg_query_values)
    if query_result is None:
        return None
    return query_result

async def get_Name_null_test_cases_from_pg(from_date : datetime,to_date: datetime):
    pg_query = """SELECT bank_name, bank_connect_statement.statement_id
FROM bank_connect_statement join bank_connect_identity on bank_connect_statement.id = bank_connect_identity.statement_id WHERE 
is_logo_mismatch = FALSE
and predicted_bank is not null
and name is null
  AND is_extracted_by_perfios = FALSE
  AND is_external_aa_data = FALSE
  AND keyword_all = TRUE
  and attempt_type != 'aa'
  AND bank_connect_statement.created_at >= :from_date and bank_connect_statement.created_at <= :to_date"""

    pg_query_values = {
        "from_date": from_date,
        "to_date" : to_date
    }

    query_result = await portal_db.fetch_all(query=pg_query, values=pg_query_values)
    if query_result is None:
        return None
    return query_result

async def get_statement_id_password_from_pg(statement_id : str, is_cc=False):
    pg_query = "SELECT pdf_password from bank_connect_statement WHERE statement_id = :statement_id"
    if is_cc:
        pg_query = "SELECT pdf_password from bank_connect_creditcardstatement WHERE statement_id = :statement_id"

    pg_query_values = {
        "statement_id": statement_id,
    }

    query_result = await portal_db.fetch_one(query=pg_query, values=pg_query_values)
    if query_result is None:
        return None
    result = dict(query_result.items())
    
    return result

async def get_unextracted_statements_from_pg(from_date : datetime,to_date: datetime):
    pg_query = """SELECT bank_name, bank_connect_statement.statement_id
FROM bank_connect_statement  WHERE 
is_logo_mismatch = FALSE
and predicted_bank is not null
and is_extracted = FALSE
  AND is_extracted_by_perfios = FALSE
  AND is_external_aa_data = FALSE
  and attempt_type != 'aa'
  and page_count > 3
  AND bank_connect_statement.created_at >= :from_date and bank_connect_statement.created_at <= :to_date"""

    pg_query_values = {
        "from_date": from_date,
        "to_date" : to_date
    }

    query_result = await portal_db.fetch_all(query=pg_query, values=pg_query_values)
    if query_result is None:
        return None
    return query_result

async def get_low_transaction_count_from_pg(from_date : datetime,to_date: datetime):
    pg_query = """SELECT STATEMENT_ID,bank_name,page_count,transaction_count, transaction_count/page_count as ratio
FROM bank_connect_statement
WHERE is_extracted_by_perfios = FALSE
  AND attempt_type != 'aa'
  AND is_logo_mismatch = FALSE
  AND is_extracted = TRUE
  AND transaction_count IS NOT NULL
  AND page_count > 4
  AND created_at >= :from_date AND created_at <= :to_date
  AND transaction_count/page_count < 6
  order by page_count desc"""

    pg_query_values = {
        "from_date": from_date,
        "to_date" : to_date
    }
    
    query_result = await portal_db.fetch_all(query=pg_query, values=pg_query_values)
    if query_result is None:
        return None
    return query_result

async def get_name_mismatch_test_cases_from_pg(from_date : datetime,to_date: datetime):
    pg_query = """SELECT Date(bank_connect_statement.created_at) AS "Date",
       bank_connect_statement.statement_id,
       bank_connect_statement.bank_name AS "bank_name",
       bank_connect_statement.predicted_bank AS "predicted_bank",
       CASE
           WHEN bank_connect_statement.bank_name = bank_connect_statement.predicted_bank THEN 'Matched'
           ELSE 'Mismatched'
       END AS "Bank Match Status",
       bank_connect_statement.statement_status AS "Statement Status",
       CASE
           WHEN bank_connect_identity.name IS NULL THEN 'Blank'
           ELSE bank_connect_identity.name
       END AS "User Name"
FROM bank_connect_statement
LEFT JOIN bank_connect_identity ON bank_connect_statement.id = bank_connect_identity.statement_id
LEFT JOIN bank_connect_entity ON bank_connect_entity.id = bank_connect_statement.entity_id
LEFT JOIN finbox_dashboard_organization ON finbox_dashboard_organization.id = bank_connect_entity.organization_id
WHERE bank_connect_statement.is_extracted_by_perfios='false'
  AND keyword_all=TRUE
  AND bank_connect_statement.attempt_type!='aa'
  AND (bank_connect_statement.created_at) >= :from_date AND (bank_connect_statement.created_at) <= :to_date
  AND finbox_dashboard_organization.id != 1"""

    pg_query_values = {
        "from_date": from_date,
        "to_date" : to_date
    }

    query_result = await portal_db.fetch_all(query=pg_query, values=pg_query_values)
    if query_result is None:
        return None
    return query_result

async def get_account_mismatch_test_cases_from_pg(from_date : datetime,to_date: datetime):
    pg_query = """SELECT DISTINCT bank_connect_statement.statement_id,
                bank_connect_statement.bank_name AS "bank_name",
                CASE
                    WHEN bank_connect_identity.name IS NULL THEN 'Blank'
                    ELSE bank_connect_identity.name
                END AS "User Name",
                CASE
                    WHEN account_number IS NULL THEN 'Blank'
                    ELSE account_number
                END AS "Account Number"
FROM bank_connect_statement
JOIN bank_connect_entity ON bank_connect_statement.entity_id=bank_connect_entity.id
JOIN finbox_dashboard_organization ON finbox_dashboard_organization.id=bank_connect_entity.organization_id
JOIN bank_connect_identity ON bank_connect_statement.id=bank_connect_identity.statement_id
WHERE bank_connect_statement.is_extracted_by_perfios='false'
  AND keyword_all=TRUE
  AND bank_connect_statement.attempt_type!='aa'
  AND (bank_connect_statement.created_at) >= :from_date AND (bank_connect_statement.created_at) <= :to_date
  AND finbox_dashboard_organization.id != 1"""

    pg_query_values = {
        "from_date": from_date,
        "to_date" : to_date
    }

    query_result = await portal_db.fetch_all(query=pg_query, values=pg_query_values)
    if query_result is None:
        return None
    return query_result