from app.template_solutioning.quality_data import get_ignore_logo_hash
from app.database_utils import quality_database
from app.template_solutioning.logo_hash import get_images
from app.conf import *

async def retrigger_ignore_cases():
    # get all the cases that are pending to be solved from quality
    case_query = """
                    SELECT * FROM statement_quality WHERE
                    (name_null = true AND name_null_maker_status = false AND name_null_ignore_case = false) OR
                    (account_null = true AND account_null_maker_status = false AND account_null_ignore_case = false) OR
                    (date_null = true AND date_null_maker_status = false AND date_null_ignore_case = false)
                """
    
    case_query_data = await quality_database.fetch_all(query=case_query)
    
    for items in case_query_data:
        data_item = dict(items)
        bank_name = data_item['bank_name']
        statement_id = data_item['statement_id']
        pdf_password = data_item['pdf_password']
        print("doing for statement id : ", statement_id)
        hash_dict = get_images(statement_id, bank_name, pdf_password)
        hashes_values = list(hash_dict.values())

        ignore_hash_list = await get_ignore_logo_hash()
        for ignore_hash in ignore_hash_list:
            if ignore_hash in hashes_values:
                # update `pdf_ignore_reason` and all the other ignores
                update_query = """
                                    UPDATE statement_quality SET name_null_ignore_case = true,
                                    account_null_ignore_case = true, date_null_ignore_case = true, pdf_ignore_reason = :pdf_ignore_reason
                                    where statement_id = :statement_id
                                """
                values = {
                    "statement_id": statement_id,
                    "pdf_ignore_reason": f"auto_logo_hash {str(ignore_hash)}"
                }
                await quality_database.execute(update_query, values)
                print(f"Ignored statement : {statement_id}.")
                break
