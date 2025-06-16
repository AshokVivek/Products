import json
import time
import os
from uuid import uuid4

import requests
import sys
from datetime import datetime
from library.excel_report.constants import DEFAULT_REQUIRED_TRANSACTIONS_COUNT
from python.aws_utils import get_json_from_s3_file

from library.enrichment_regexes import get_transaction_templates
from library.date_utils import get_months_from_periods, convert_date_range_to_datetime
from library.fitz_functions import read_pdf, check_if_image, get_pdf_page_hashes_with_page_text, get_generic_text_from_bank_pdf
from library.transactions import get_transaction, get_transactions_finvu_aa
from library.excel_report.metrics_helper import get_metrics, top_debit_credit
from library.excel_report.rolling_monthly.rolling_monthly_analysis import rolling_month_analysis_func
from library.extract_txns_karur import get_transactions_for_karur
from library.internal_tool_utils import get_template_data_for_bbox
from python.aggregates import get_account_id_for_statement, get_progress, get_accounts_for_entity, get_complete_progress, get_final_account_category, \
    get_bank_name_for_statement, get_country_for_statement, get_currency_for_statement, get_upload_status, get_enrichment_for_entity, update_progress, update_progress_on_dashboard
from python.aggregates import get_complete_identity_for_statement, get_identity_for_statement, map_session_account_status
from python.aggregates import get_transactions_for_entity, get_transactions_for_statement, get_page_level_transactions_for_statement
from python.aggregates import get_transactions_for_account, get_date_discontinuity, get_account_for_entity, get_transactions_for_statement_page
from python.aggregates import get_salary_transactions_from_ddb, get_recurring_transactions_list_from_ddb, get_account_wise_months, redundant_keys, \
        get_link_id_overview, get_non_metadata_frauds, update_last_page, get_recurring_raw_from_ddb, get_extracted_frauds_list, get_statement_ids_for_account_id, \
        get_recurring_lender_debit_transactions, update_bounce_transactions_for_account_transactions, mark_refund_on_basis_of_same_balance, keep_specific_keys, fill_transactions_na_key
from python.utils import (
    is_raw_aa_transactions_inconsistent,
    get_transactions_list_of_lists_finvu_aa,
    remove_local_file,
    get_data_for_template_handler_util, move_clickhouse_data
)
from library.extract_txns_csv import get_transactions_list_of_lists_csv
from sentry_sdk import set_context, set_tag
from python.enrichment_regexes import check_and_get_everything
from python.store_data import store_data_from_enrichment_regexes
from python.configs import LAMBDA_LOGGER, CATEGORIZE_RS_PRIVATE_IP, PRIMARY_EXTRACTION_QUEUE_URL, SECONDARY_EXTRACTION_QUEUE_URL, EXTRACT_SYNC_TRANSACTIONS_FUNCTION, \
        ANALYZE_PDF_PAGE_FUNCTION, ANALYZE_PDF_PAGE_SECONDARY_FUNCTION, ANALYZE_PDF_PAGE_TERNARY_FUNCTION, TERNARY_LAMBDA_QUEUE_URL, KARUR_EXTRACTION_FUNCTION, NUMBER_OF_RETRIES, FINVU_AA_PAGE_FUNCTION, \
        AA_TRANSACTIONS_PAGE_QUEUE_URL, bank_connect_transactions_table, dynamodb, bank_connect_statement_table
from python.update_state_handlers import update_state_fan_out_handler
from python.configs import sqs_client, s3, bank_connect_statement_table_name, lambda_client, IS_SERVER
from python.configs import *
from category import SingleCategory
from library.fitz_functions import get_name, get_account_num, get_date_range, get_account_category, get_credit_limit, get_od_limit, is_od_account_check, \
                                get_account_num, get_micr, get_ifsc, get_opening_closing_bal, get_opening_date, \
                                get_joint_account_holders_name
from library.fraud import fraud_category
from python.utils import send_event_to_update_state_queue
from library.credit_card_extraction_with_ocr import get_cc_transactions_using_fitz
import warnings
import pandas as pd
from library.transaction_description import get_transaction_description
from library.transaction_channel import get_transaction_channel
from concurrent.futures import ThreadPoolExecutor
from library.salary import get_salary_transactions
import pdf2image
import ocrmypdf
from library.utils import get_ocr_condition_for_credit_card_statement, get_pages
from python.context.logging import LoggingContext
import traceback
import calendar
from library.get_edges_test import get_last_page_regex_simulation
from python.utils import update_transactions_on_session_date_range, update_field_for_statement
from python.api_utils import call_api_with_session
warnings.simplefilter(action = "ignore", category = FutureWarning)
pd.options.mode.chained_assignment = None


def lambda_execution_plan(payload, statement_id, doc, file_path, is_ocr_extracted, local_logging_context):
    LAMBDA_LOGGER.debug("Executing via lambda execution plan", extra=local_logging_context.store)
    
    bank_name = payload["bank"]
    number_of_pages = payload["number_of_pages"]
    
    payload["extraction_pipeline"] = "LAMBDA"

    function_name = ANALYZE_PDF_PAGE_FUNCTION
    # primary lambda for statements with page count < 60
    if number_of_pages <60:
        function_name = ANALYZE_PDF_PAGE_FUNCTION
    # secondary lambda for statements with page count >= 60 and <120
    elif number_of_pages in range(60, 120):
        function_name = ANALYZE_PDF_PAGE_SECONDARY_FUNCTION
    # ternary lambda without sqs for statements with page count >= 120 and <220
    elif number_of_pages in range(120, 220):
        function_name = ANALYZE_PDF_PAGE_TERNARY_FUNCTION
    
    else:
        if bank_name not in ['karur']:
            function_name = ANALYZE_PDF_PAGE_TERNARY_FUNCTION
            print("This statement should be extracted via ternary lambda, invoking accordingly.")
            LAMBDA_LOGGER.debug(f"Bank found to not be Karur, function name finalized to be {function_name}. Attempting to invoke the same per page via SQS accordingly.", extra=local_logging_context.store)
            
            for page_number in range(number_of_pages):
                payload['page_number'] = page_number    
                print("Invoking ternary_lambda for page number {} of {}".format(page_number, statement_id))
                random_uuid = str(uuid4())
                try:    
                    _ = sqs_client.send_message(
                        QueueUrl = TERNARY_LAMBDA_QUEUE_URL,
                        MessageBody = json.dumps(payload),
                        MessageDeduplicationId = '{}_{}'.format(random_uuid, page_number),
                        MessageGroupId = 'tertiary_lambda_invocation_{}'.format(random_uuid)
                    )
                    # print("Written to ternary sqs: ",sqs_push_response)
                except Exception as exception:
                    print("Failed to push into ternary lambda's sqs")
                    local_logging_context.upsert(exception=str(exception),trace=traceback.format_exc())
                    LAMBDA_LOGGER.error(f"Exception observed while pushing message to the ternary lambda queue {TERNARY_LAMBDA_QUEUE_URL} for UUID {random_uuid} and page number {page_number}.",extra=local_logging_context.store)
                    local_logging_context.remove_keys(["exception", "trace"])
                    return
            if os.path.exists(file_path):
                os.remove(file_path)
            return {"message": "success"}
    
    if bank_name in ['karur', 'hsbc']:
        search_text = doc[0].search_for("�")
        if len(search_text) > 5 or is_ocr_extracted:
            function_name = KARUR_EXTRACTION_FUNCTION
    
    LAMBDA_LOGGER.debug(f"Function name finalized as {function_name}. Invoking the {function_name} lambda per page for {number_of_pages} total pages", extra=local_logging_context.store)

    print("Finally function_name is : ", function_name)
    # invoke analyze pdf page lambda in async
    for page_number in range(number_of_pages):
        payload['page_number'] = page_number
        lambda_client.invoke(FunctionName=function_name, Payload = json.dumps(payload), InvocationType='Event')
    
    # delete file after usage
    if os.path.exists(file_path):
        os.remove(file_path)
    
    LAMBDA_LOGGER.info(f"{function_name} lambda invocation completed for {number_of_pages} total pages and analyse_pdf_handler lambda flow successfully completed, returning response.", extra=local_logging_context.store)


def server_execution_plan(payload, statement_id, local_logging_context):
    LAMBDA_LOGGER.debug("Executing via server execution plan", extra=local_logging_context.store)
    number_of_pages = payload["number_of_pages"]

    LAMBDA_LOGGER.info("Popping Trans BBox, Last page, Account Delimiter, Enrichment Regex as they may increase the size of the send_message_batch payload", extra=local_logging_context.store)

    payload.pop("trans_bbox")
    payload.pop("last_page_regex")
    payload.pop("account_delimiter_regex")
    payload.pop("enrichment_regexes")

    queue_url = None
    cut_off_page_count = os.environ.get("cut_off_page_count", 61)
    if number_of_pages in range(0, cut_off_page_count):
        queue_url = PRIMARY_EXTRACTION_QUEUE_URL
    else:
        queue_url = SECONDARY_EXTRACTION_QUEUE_URL
    
    LAMBDA_LOGGER.info(f"Queue URL {queue_url}", extra=local_logging_context.store)

    MESSAGE_BATCH_SIZE = 10
    PAGES_TOP_LIMIT = 1000
    
    # if number_of_pages > 500:
    #     MESSAGE_BATCH_SIZE = number_of_pages // MESSAGE_BATCH_SIZE

    all_messages = []
    current_batch = []
    current_batch_count = 0
    batches_encountered = 0

    for page_number in range(number_of_pages):
        payload["page_number"] = page_number
        payload["extraction_pipeline"] = "SERVER"
        message_group_id = f"{statement_id}_{page_number}"
        
        if number_of_pages in range(200, PAGES_TOP_LIMIT):
            # this will distribute the load by 10. so if the number of pages are max 1000, there will 100 inflight groups and thus extraction will be faster
            message_group_id = f"{statement_id}_{batches_encountered}"
        elif number_of_pages >= PAGES_TOP_LIMIT:
            # if the number of pages are greater than 1000, the max number of inflight groups would be 10, to give priority to the smaller pdfs.
            message_group_id = f"{statement_id}_{current_batch_count}"

        message = {
            "Id": f"{statement_id}_{page_number}",
            "MessageBody": json.dumps(payload),
            "MessageDeduplicationId": f"{statement_id}_{page_number}",
            "MessageGroupId": message_group_id
        }
        current_batch.append(message)
        current_batch_count += 1
        if current_batch_count == MESSAGE_BATCH_SIZE:
            LAMBDA_LOGGER.info(f"Adding batch to the set {current_batch_count}", extra=local_logging_context.store)
            all_messages.append(current_batch)
            current_batch = []
            current_batch_count = 0
            batches_encountered += 1
    
    if current_batch_count:
        LAMBDA_LOGGER.info("Appending Last batch out of the loop", extra=local_logging_context.store)
        all_messages.append(current_batch)
    
    for i in range(0, len(all_messages)):
        LAMBDA_LOGGER.info(f"Processing message count {i}", extra=local_logging_context.store)
        response = sqs_client.send_message_batch(
            QueueUrl = queue_url,
            Entries = all_messages[i]
        )
        LAMBDA_LOGGER.info(f"SQS Batch Send response : {response}", extra=local_logging_context.store)



def analyze_pdf_handler(event, context):
    bucket = event.get('bucket')
    key = event.get('key')
    name = event.get('name', '')
    account_number = event.get('account_number','')
    trans_bbox = event.get('trans_bbox', [])
    last_page_regex = event.get('last_page_regex', [])
    account_delimiter_regex = event.get('account_delimiter_regex', [])
    enrichment_regexes = event.get("enrichment_regexes", {})
    country = event.get("country", "IN")
    account_id = event.get("account_id", None)
    session_date_range = event.get("session_date_range", {'from_date':None, 'to_date':None})
    statement_meta_data_for_warehousing =  event.get('statement_meta_data_for_warehousing', {})
    extract_multiple_accounts = event.get('extract_multiple_accounts', False)
    org_metadata = event.get("org_metadata", dict())
    extraction_pipeline = event.get("extraction_pipeline", "LAMBDA")
    local_logging_context = event.get("local_logging_context")

    if not local_logging_context:
        local_logging_context: LoggingContext = LoggingContext(source="analyze_pdf_handler")
        LAMBDA_LOGGER.info("Initiating the extraction (analyse PDF) handler flow.", extra=local_logging_context.store)

    response = s3.get_object(Bucket=bucket, Key=key)

    response_metadata = response.get('Metadata')
    statement_id = response_metadata.get('statement_id')
    bank = response_metadata.get('bank_name')
    password = response_metadata.get('pdf_password')
    entity_id = response_metadata.get('entity_id')
    set_tag("entity_id", entity_id)
    set_tag("account_id", account_id)
    set_tag("statement_id", statement_id)
    set_context("analyze_pdf_event_payload", event)

    local_logging_context.upsert(entity_id=entity_id, statement_id=statement_id, account_id=account_id, bank=bank)
    LAMBDA_LOGGER.info("Parameters successfully extracted from the event and S3", extra=local_logging_context.store)
    if not statement_id:
        # no need to process if can't get statement id
        LAMBDA_LOGGER.warning("statement_id not found, ignoring this event", extra=local_logging_context.store)
        return {"message": "ignored, statement_id not found in metadata"}

    extracted_identity = get_complete_identity_for_statement(statement_id)

    LAMBDA_LOGGER.info("Successfully extracted statement identity", extra=local_logging_context.store)
    is_ocr_extracted = extracted_identity.get('is_ocr_extracted', False)

    identity = {}
    identity['opening_bal'] = extracted_identity.get('opening_bal', None)
    identity['closing_bal'] = extracted_identity.get('closing_bal', None)
    identity['opening_date'] = extracted_identity.get('opening_date', None)
    identity['date_range'] = extracted_identity.get('extracted_date_range', {})
    identity['is_ocr_extracted'] = is_ocr_extracted

    account_dict = None
    account_category = None

    if account_id is not None:
        account_dict = get_account_for_entity(entity_id, account_id)
        LAMBDA_LOGGER.debug("account_id found and successfully extracted account information for specified entity_id",extra=local_logging_context.store)
        account_dict = account_dict.get('item_data')

    if account_dict is not None:
        account_category, _ = get_final_account_category(account_dict.get('account_category', None), account_dict.pop('is_od_account', None), account_dict.pop('input_account_category', None), account_dict.pop('input_is_od_account', None))
        LAMBDA_LOGGER.debug("Final account_category successfully extracted.", extra=local_logging_context.store)

    # write a temporary file with content
    file_path = f"/tmp/{statement_id}_{bank}.pdf"
    if IS_SERVER:
        file_path = f"/efs/{statement_id}_{bank}.pdf"
        LAMBDA_LOGGER.info(f"Since this is server, writing the file in mounted path {file_path}", extra=local_logging_context.store)

    with open(file_path, 'wb') as file_obj:
        file_obj.write(response['Body'].read())

    LAMBDA_LOGGER.debug("Successfully wrote the contents of the statement to a temporary file. Attempting to read the PDF now.", extra=local_logging_context.store)

    # do basic checks
    doc = read_pdf(file_path, password)
    if isinstance(doc, int):
        if doc == -1:
            # file doesn't exists or is not a valid pdf file
            LAMBDA_LOGGER.warning("PDF found to not be parseable, returning response.",extra=local_logging_context.store)
            return {"message": "PDF is not parsable"}
        else:
            # password is incorrect
            LAMBDA_LOGGER.warning("PDF password found to be incorrect, returning response.", extra=local_logging_context.store)
            return {"message": "Password is incorrect"}
        
    # for now do detailed image check for non federal bank statements
    if bank not in ['federal', 'hsbc'] and check_if_image(doc):
        LAMBDA_LOGGER.warning("PDF found to not be parseable for a non- Federal bank after checking if it is a scanned image or not. Returning response.", extra=local_logging_context.store)
        return {"message": "PDF is not parsable"}

    number_of_pages = doc.page_count  # get the page count
    
    # make entry for number of pages and instantiate the pages_done key in dynamodb
    bank_connect_statement_table.update_item(
        Key={'statement_id': statement_id},
        UpdateExpression="set {} = :s, created_at = :c, pages_done = :d".format('page_count'),
        ExpressionAttributeValues={
                ':s': number_of_pages,
                ':c': str(int(time.time())),
                ':d': 0
        })
    
    LAMBDA_LOGGER.debug(f"Successfully inserted total and completed page number count in DDB, number of pages: {number_of_pages}",extra=local_logging_context.store)

    payload = {
        'key': key,
        'bucket': bucket,
        'bank': bank,
        'name': name,
        'account_number':account_number,
        'password': password,
        'trans_bbox': trans_bbox,
        'last_page_regex': last_page_regex,
        'account_delimiter_regex': account_delimiter_regex,
        'number_of_pages': number_of_pages,
        'enrichment_regexes': enrichment_regexes,
        'country': country,
        'account_id': account_id,
        'account_category': account_category ,
        'session_date_range': session_date_range,
        'identity': identity,
        'statement_meta_data_for_warehousing': statement_meta_data_for_warehousing,
        'extract_multiple_accounts': extract_multiple_accounts,
        'org_metadata': org_metadata,
        "file_path": file_path,
        "entity_id": entity_id,
        "statement_id": statement_id
    }

    if( 
        bank in [ "hsbc", "solapur_siddheshwar", "jnkbnk", "bcabnk", "megabnk", "abhinav_sahakari", "kurla_nagrik", "agrasen_urban", "rajarshi_shahu"] 
        and not is_ocr_extracted
    ):
        LAMBDA_LOGGER.debug(f"Attempting to invoke the {EXTRACT_SYNC_TRANSACTIONS_FUNCTION} lambda.",extra=local_logging_context.store)
        response = lambda_client.invoke(
            FunctionName=EXTRACT_SYNC_TRANSACTIONS_FUNCTION, 
            Payload=json.dumps(payload), 
            InvocationType="Event"
        )
        LAMBDA_LOGGER.info(f"Successfully invoked the {EXTRACT_SYNC_TRANSACTIONS_FUNCTION} lambda, returning response.",extra=local_logging_context.store)
        return {"message": "success"}

    if extraction_pipeline == "LAMBDA" or bank in ["hsbc", "karur"]:
        LAMBDA_LOGGER.info("Executing via lambda execution plan", extra=local_logging_context.store)
        lambda_execution_plan(
            payload=payload,
            statement_id=statement_id,
            doc=doc,
            file_path=file_path,
            is_ocr_extracted=is_ocr_extracted,
            local_logging_context=local_logging_context
        )
    else:
        LAMBDA_LOGGER.info("Executing via server execution plan", extra=local_logging_context.store)
        server_execution_plan(
            payload=payload,
            statement_id=statement_id,
            local_logging_context=local_logging_context
        )

    # local_logging_context.clear()
    return {"message": "success"}

def analyze_pdf_page_ternary_handler(event, context):
    
    local_logging_context: LoggingContext = LoggingContext(
        source="analyze_pdf_page_ternary_handler"
    )

    LAMBDA_LOGGER.info(
        "Initiating the analyse PDF page ternary handler flow.",
        extra=local_logging_context.store
    )

    if event.get("Records",None):
        print("Extracting from SQS")
        records = event.get("Records", None)
        
        if records is None or len(records) == 0:

            LAMBDA_LOGGER.warning(
                "No records found while extracting message from SQS.",
                extra=local_logging_context.store
            )

            print("no records were found")
            return
        
        record = records[0]
        body = record.get("body", None)
        
        if body is None:

            LAMBDA_LOGGER.warning(
                "Record body was found to be None after extracting message from SQS.",
                extra=local_logging_context.store
            )

            print("record body was none")
            return
        
        event=json.loads(body)
    
    extraction_helper(event, local_logging_context=local_logging_context)


def analyze_pdf_page_handler(event, context):
    local_logging_context: LoggingContext = LoggingContext(source="analyze_pdf_page_handler")
    LAMBDA_LOGGER.info("Initiating the analyse PDF page handler flow.", extra=local_logging_context.store)
    extraction_helper(event, local_logging_context=local_logging_context)


def update_bsa_extracted_count(entity_id, statement_id, page_number, number_of_pages,
                               statement_meta_data_for_warehousing=None, org_metadata:dict = dict(), file_path=""):
    if statement_meta_data_for_warehousing is None:
        statement_meta_data_for_warehousing = {}

    # statement_id is the statement_id here
    print("Number of pages: ", number_of_pages)
    for i in range(NUMBER_OF_RETRIES):
        try:
            if i>0:
                print("Retry #{} to update increment count for page number {} of {}".format(i, page_number, statement_id))

            response = dynamodb.update_item(    
                TableName=bank_connect_statement_table_name,     
                Key={'statement_id':{'S': statement_id}},    
                UpdateExpression='SET pages_done = pages_done + :inc',    
                ExpressionAttributeValues={':inc': {'N': '1'}},  
                ReturnValues="UPDATED_NEW"
            )

            break
        except dynamodb.exceptions.ConditionalCheckFailedException:
            print("ConditionalCheckFailedException occured, retrying with backoff factor")
            time.sleep(0.1 * (2 ** i))

    pages_done = response.get("Attributes", {}).get("pages_done", {}).get("N")
    
    print(f'Pages Done for statement : {statement_id} --> {pages_done}, page_number : {page_number}, number_of_pages : {number_of_pages}, entity_id : {entity_id}, condition_for_update_state : {int(pages_done)==number_of_pages}')
    if int(pages_done)==number_of_pages:
        if IS_SERVER:
            if os.path.exists(file_path):
                os.remove(file_path)
        send_event_to_update_state_queue(entity_id, statement_id, statement_meta_data_for_warehousing, org_metadata=org_metadata)

    return {"message": "success"}



def extraction_helper(event, local_logging_context: LoggingContext = None):
    """
    This function handles the extraction of transactions from a PDF statement. It processes each page of the statement,
    extracts transaction data, and updates ddb for that page.

    Parameters:
    - event (dict): The event payload containing the following keys:
        - 'bucket' (str): The S3 bucket name where the PDF is stored.
        - 'key' (str): The S3 key (path) of the PDF file.
        - 'page_number' (int): The page number to process.
        - 'number_of_pages' (int): The total number of pages in the PDF.
        - 'enrichment_regexes' (dict): Regex patterns for data enrichment.
        - 'country' (str): The country code (default is "IN").
        - 'session_date_range' (dict): The date range for the session with 'from_date' and 'to_date'.
        - 'statement_meta_data_for_warehousing' (dict): Metadata for warehousing.
        - 'org_metadata' (dict): Organization metadata.
    - local_logging_context (LoggingContext, optional): The logging context for capturing logs.

    Output:
    - dict: A response dictionary containing the update status from the `update_bsa_extracted_count` function.
    """
    if not local_logging_context:
        local_logging_context: LoggingContext = LoggingContext(source="extraction_helper")
        
    LAMBDA_LOGGER.info("Initiating the extraction helper flow.", extra=local_logging_context.store)
    
    bucket = str(event.get('bucket'))
    key = str(event.get('key'))
    page_number = int(event.get('page_number'))
    number_of_pages = event.get('number_of_pages', 0)
    enrichment_regexes = event.get("enrichment_regexes", {})
    country = event.get("country", "IN")        # <- this value is hardcoded to IN for now
    session_date_range = event.get('session_date_range', {'from_date':None, 'to_date':None})
    statement_meta_data_for_warehousing = event.get("statement_meta_data_for_warehousing", {})
    org_metadata = event.get('org_metadata', dict())
    extraction_pipeline = event.get("extraction_pipeline", "LAMBDA")

    # if this is SERVER, then the file is available mounted at EFS volume
    # no need to download the file again 
    if not IS_SERVER:
        response = s3.get_object(Bucket=bucket, Key=key)
        response_metadata = response.get('Metadata')
        entity_id = response_metadata.get('entity_id')
        bank = response_metadata.get('bank_name')
        statement_id = response_metadata.get('statement_id')
        
        file_path = "/tmp/{}.pdf".format(statement_id)
        with open(file_path, 'wb') as file_obj:
            file_obj.write(response['Body'].read())
    else:
        entity_id = event.get("entity_id")
        bank = event.get("bank")
        statement_id = event.get("statement_id")
        file_path = event.get("file_path")

    set_tag("entity_id", entity_id)
    set_tag("statement_id", statement_id)
    set_context("analyze_pdf_page_event_payload", event)
    local_logging_context.upsert(entity_id=entity_id, statement_id=statement_id, bank=bank, page_number=page_number, number_of_pages=number_of_pages)
    LAMBDA_LOGGER.debug(msg="Parameters successfully extracted from the event and S3. Storing data from enrichment regexes.",extra=local_logging_context.store)

    if not IS_SERVER:
        LAMBDA_LOGGER.debug("Attempting to extract all regexes for the specified bank + country combination.",extra=local_logging_context.store)
        store_data_from_enrichment_regexes(enrichment_regexes, bank, country) # check if data is retrieved from server and populate the files
        check_and_get_everything(bank, country) # check and get all regexes for this bank and country

    if IS_SERVER:
        # here trans bbox etc is not available in event payload, to reduce the size. 
        # getting them from the redis
        trans_bbox, last_page_regex, account_delimiter_regex = get_transaction_templates(bank)
        event["trans_bbox"] = trans_bbox
        event["last_page_regex"] = last_page_regex
        event["account_delimiter_regex"] = account_delimiter_regex

    # print('currently processing page {} of uuid {}'.format(page_number, statement_id))
    LAMBDA_LOGGER.info(msg = f"Total number of pages in this statement: {number_of_pages}",extra = local_logging_context.store)
    LAMBDA_LOGGER.info(msg = f"currently processing page {page_number} of uuid {statement_id}", extra = local_logging_context.store)
    LAMBDA_LOGGER.info("Attepting to extract transactions via Plumber and Fitz.",extra=local_logging_context.store)
    
    event['path'] = file_path
    
    t1 = time.time()
    transactions_output_dict = get_transaction(event, local_logging_context, LAMBDA_LOGGER)
    
    transactions = transactions_output_dict.get('transactions', [])
    extraction_template_uuid = transactions_output_dict.get('extraction_template_uuid', '')
    last_page_flag = transactions_output_dict.get('last_page_flag')
    removed_date_opening_balance = transactions_output_dict.get('removed_opening_balance_date')
    removed_date_closing_balance = transactions_output_dict.get('removed_closing_balance_date')
    
    number_of_transactions = len(transactions)

    LAMBDA_LOGGER.debug(f"Successfully extracted {number_of_transactions} transactions.",extra=local_logging_context.store)

    if removed_date_opening_balance is not None:
        update_field_for_statement(statement_id, f'removed_date_opening_balance_{page_number}', removed_date_opening_balance)

    if removed_date_closing_balance is not None:
        update_field_for_statement(statement_id, f'removed_date_closing_balance_{page_number}', removed_date_closing_balance)
    
    LAMBDA_LOGGER.info(f"""
        Extraction Pipeline : {extraction_pipeline}, 
        Bank Name: {bank}
        Number of Transactions: {number_of_transactions} 
        Page Number: {page_number} 
        Time took {time.time() - t1}""",
    extra=local_logging_context.store)

    LAMBDA_LOGGER.debug("Updating transactions based on session date range.",extra=local_logging_context.store)

    transactions = update_transactions_on_session_date_range(session_date_range, transactions, statement_id, page_number)

    LAMBDA_LOGGER.debug("Triggering transaction forward mapper.",extra=local_logging_context.store)
    categorizer = SingleCategory(bank_name=bank, transactions=transactions, categorize_server_ip=CATEGORIZE_RS_PRIVATE_IP)
    transactions = categorizer.categorize_from_forward_mapper()

    time_stamp_in_mlilliseconds = time.time_ns()
    dynamo_object = {
        'statement_id': statement_id,
        'page_number': page_number,
        'item_data': json.dumps(transactions, default=str),
        'template_id': extraction_template_uuid,
        'transaction_count': number_of_transactions,
        'created_at': time_stamp_in_mlilliseconds,
        'updated_at': time_stamp_in_mlilliseconds
    }

    if last_page_flag:
        update_last_page(statement_id, page_number)

    # delete file after usage
    if not IS_SERVER and os.path.exists(file_path):
        os.remove(file_path)

    LAMBDA_LOGGER.debug("Updating transactions within DDB.",extra=local_logging_context.store)
    bank_connect_transactions_table.put_item(Item=dynamo_object)
    
    LAMBDA_LOGGER.debug("Updating Bank Statement Analyser extracted count within DDB and attempting to send update state event post processing.",extra=local_logging_context.store)
    update_bsa_response = update_bsa_extracted_count(entity_id, statement_id, page_number, number_of_pages, statement_meta_data_for_warehousing, org_metadata=org_metadata, file_path=file_path)

    LAMBDA_LOGGER.info("Update state event sent successfully and execution of the extraction helper lambda completed.",extra=local_logging_context.store)
    # local_logging_context.clear()

    return update_bsa_response

def link_id_overview(event, context):
    link_id = event.get('link_id')
    entity_ids = event.get('entity_ids')

    progress = get_link_id_overview(entity_ids)

    return_dict = dict()
    return_dict['link_id'] = link_id
    return_dict['progress_data'] = progress

    return return_dict

def transactions_sanity_checker(transactions):
    """
    Validates if the transactions list are json load-able
    """
    final_transactions = []
    
    for transaction in transactions:
        amount = transaction.get("amount", None)
        balance = transaction.get("balance",None)
        date = transaction.get("date",None)
        if isinstance(amount, float) and amount != float("inf") and amount != float("-inf") \
            and isinstance(balance, float) and balance != float("inf") and balance != float("-inf") \
            and isinstance(date, str) and date != float("inf") and date != float("-inf"):
            final_transactions.append(transaction)
    
    return final_transactions

def get_complete_entity_progress_for_statement(entity_id, statement_id):
    response= get_complete_progress(statement_id)
    identity_message = response.pop('identity_message', None)
    transaction_message = response.pop('transaction_message', None)
    processing_message = response.pop('processing_message', None)
    message = None
    if identity_message != None:
        message = identity_message
    elif transaction_message != None:
        message = transaction_message
    elif processing_message != None:
        message = processing_message
    response['statement_id']=statement_id
    response['message'] = message
    return response

def access_handler(event, context):
    print("Event details: {}".format(event))
    access_type = event.get('access_type')
    entity_id = event.get('entity_id')
    to_reject_account = event.get('to_reject_account', False)
    set_tag("entity_id", entity_id)
    set_context("access_handler_event_payload", event)
    
    ##########################################################################################################
    # this gets the accounts for the entity and stores relevant enrichment regexes associated to it
    accounts = get_accounts_for_entity(entity_id, to_reject_account)
    # print("These are the accounts fetched ======== {}".format(accounts))
    for account in accounts:
        account_id = account.get('account_id')
        account_statements = get_statement_ids_for_account_id(entity_id, account_id)
        if len(account_statements) > 0:
            account_bank_name = get_bank_name_for_statement(account_statements[0])
            account_country = get_country_for_statement(account_statements[0])
            check_and_get_everything(account_bank_name, account_country)
    ##########################################################################################################

    if access_type == 'ENTITY_IDENTITY':
        entity_id = event.get('entity_id')
        accounts = get_accounts_for_entity(entity_id, to_reject_account)
        account_wise_latest_stmt = event.get('account_wise_latest_stmt', dict())

        identity_list = list()
        name = False
        statement_id = False
        identity = False
        for account in accounts:
            account = account.get('item_data', dict())
            account_id = account.get('account_id')
            if account_id in account_wise_latest_stmt.keys():
                statement_id = account_wise_latest_stmt[account_id]
                identity = get_identity_for_statement(statement_id)
                name = identity.get('name')

                identity['account_category'], _ = get_final_account_category(account.get('account_category', None), account.pop('is_od_account', None), account.pop('input_account_category', None), 
                                                                account.pop('input_is_od_account', None))
                #did not pop credit_limit because it was already present, we can remove it from identity in future
                identity['credit_limit'] = account.get('credit_limit', None)
                identity['od_limit'] = account.get('od_limit', None)
                if identity.get('credit_limit', None) == None:
                    identity['credit_limit'] = identity.get('od_limit',None)
                credit_limit = identity.get('credit_limit', None)
                if credit_limit == '':
                    identity['credit_limit'] = None
                if credit_limit != None and credit_limit != '':
                    identity['credit_limit'] = int(float(credit_limit))
                if not identity['credit_limit']:
                    identity['credit_limit'] = identity.get('od_limit', None)
                identity.pop('od_limit', None)
                identity.pop('is_od_account', None)
                identity.pop('currency', None)
                identity.pop('raw_account_category', None)
                identity.pop('input_account_category', None)
                identity.pop('input_is_od_account', None)
                identity.pop('updated_od_paramters_by', None)
                identity.pop('od_metadata', None)
                if statement_id and identity and name:
                    identity_list.append(identity)
                    continue

            account_statements = account.get('statements')
            if len(account_statements) == 0:
                continue

            identity_candidates = list()

            for sample_statement_id in account_statements:
                identity = get_identity_for_statement(sample_statement_id)

                if identity != dict():
                    score = int(identity.get('name') not in ['', None]) + \
                            int(identity.get('address') not in ['', None]) + \
                            int(identity.get('account') not in ['', None])

                    identity['account_category'], _ = get_final_account_category(account.get('account_category', None), account.pop('is_od_account', None), account.pop('input_account_category', None), 
                                                                account.pop('input_is_od_account', None))
                    
                    identity['credit_limit'] = account.get('credit_limit', None)
                    identity['od_limit'] = account.get('od_limit', None)
                    if identity.get('credit_limit', None) == None:
                        identity['credit_limit'] = identity.get('od_limit',None)
                    credit_limit = identity.get('credit_limit', None)
                    if credit_limit == '':
                        identity['credit_limit'] = None
                    if credit_limit != None and credit_limit != '':
                        identity['credit_limit'] = int(float(credit_limit))
                    if identity['account_category'] != 'overdraft':
                        identity.pop('od_limit_input_by_customer', None)
                    if not identity['credit_limit']:
                        identity['credit_limit'] = identity.get('od_limit', None)
                    identity.pop('od_limit', None)
                    identity.pop('is_od_account', None)
                    identity.pop('currency', None)
                    identity.pop('raw_account_category', None)
                    identity.pop('input_account_category', None)
                    identity.pop('input_is_od_account', None)
                    identity.pop('updated_od_paramters_by', None)
                    identity.pop('od_metadata', None)
                    identity_candidates.append(
                        {'identity': identity, 'score': score})

            if len(identity_candidates) > 0:
                identity_candidates.sort(
                    key=lambda x: x['score'], reverse=True)
                identity_list.append(identity_candidates[0].get('identity', dict()))
        # print("Response for access_type = {} and entity_id ={} is {}".format(access_type,entity_id,identity_list))
        return identity_list

    elif access_type == 'ENTITY_ACCOUNTS':
        entity_id = event.get('entity_id')
        salary_confidence_flag = event.get('salary_confidence_flag',False)
        last_updated_date_required = event.get('last_updated_date_required', False)
        account_id = event.get('account_id', None)
        is_streaming = event.get('is_streaming', False)
        accounts = list()
        if account_id:
            temp_account = get_account_for_entity(entity_id, account_id, to_reject_account)
            if temp_account:
                accounts = [temp_account]
        else:
            accounts = get_accounts_for_entity(entity_id, to_reject_account)

        accounts_list = list()
        for account in accounts:
            account_dict = account.get('item_data')
            account_item_status = account.get('item_status')
            statements = account_dict.get('statements')

            date_ranges = list()  # init
            identity = {}  # init
            for stmt_id in statements:
                identity = get_complete_identity_for_statement(stmt_id)
                date_range = identity.get('date_range')
                if date_range:
                    date_ranges.append(date_range)

            account_dict['months'] = get_months_from_periods(date_ranges)
            account_dict['name'] = identity.get('identity',{}).get("name", '')
            account_dict['address'] = identity.get('identity',{}).get("address", '')
            account_dict['account_category'], _ = get_final_account_category(account_dict.get('account_category', None), account_dict.pop('is_od_account', None), account_dict.pop('input_account_category', None), 
                                                            account_dict.pop('input_is_od_account', None))
            if account_dict.get('credit_limit', None) is None:
                account_dict['credit_limit'] = account_dict.get('od_limit',None)
            if account_dict.get('od_limit', None) is None:
                account_dict['od_limit'] = account_dict.get('credit_limit',None)

            if salary_confidence_flag is False and 'salary_confidence' in account_dict.keys():
                del account_dict['salary_confidence']

            # also get the country and currency from ddb and write in accounts_list
            country_code = get_country_for_statement(statements[0])
            currency_code = get_currency_for_statement(statements[0])

            # default country and currency to IN and INR respectively incase of None values
            country_code = country_code if country_code is not None else "IN"
            currency_code = currency_code if currency_code is not None else "INR"

            account_dict["country_code"] = country_code
            account_dict["currency_code"] = currency_code
            account_dict.pop('neg_txn_od', None)
            account_dict.pop('od_limit_input_by_client', None)

            # get the date discontinuity remarks if any
            account_dict["missing_data"] = get_date_discontinuity(entity_id, account_dict['account_id'])
            if not is_streaming:
                account_dict.pop('is_inconsistent', None)
                account_dict.pop('inconsistent_hash', None)
            elif account_item_status:
                account_dict['is_rejected_account'] = True if account_item_status['account_status']!='completed' else False

            if last_updated_date_required:
                account_dict['last_updated'] = account.get('updated_at', None)

            accounts_list.append(account_dict)

        # print("Response for access_type = {} and entity_id ={} is {}".format(access_type,entity_id,accounts_list))
        return accounts_list

    elif access_type == 'ENTITY_TRANSACTIONS':
        entity_id = event.get('entity_id')
        account_id = event.get('account_id', None)
        is_sme = event.get('is_sme', False)
        show_rejected_transactions = event.get('show_rejected_transactions', False)
        transactions = []
        if account_id:
            temp_account = get_account_for_entity(entity_id, account_id, to_reject_account)
            if temp_account:
                transactions, _ = get_transactions_for_account(entity_id, account_id, show_rejected_transactions=show_rejected_transactions)
            object_key = "transactions/account_{}".format(account_id)
        else:
            transactions = get_transactions_for_entity(entity_id, is_sme, to_reject_account, show_rejected_transactions)
            object_key = "transactions/entity_{}".format(entity_id)
        # transactions = remove_redundant_keys(transactions)
        transactions = keep_specific_keys(transactions)
        transactions = fill_transactions_na_key(transactions)
        transactions = transactions_sanity_checker(transactions)
        print("response size:", sys.getsizeof(transactions))
        response = None 
        if sys.getsizeof(transactions) < 80000:
            response = transactions
        else:
            transactions = json.dumps(transactions)
            s3_object = s3_resource.Object(BANK_CONNECT_DDB_FAILOVER_BUCKET, object_key)
            s3_object.put(Body=bytes(transactions, encoding='utf-8'))
            response = {"s3_object_key": object_key}
        # print("Response for access_type = {} and entity_id ={} is {}".format(access_type,entity_id,transactions))
        return response

    elif access_type == 'ENTITY_FRAUD':
        # gets both author identity and transaction fault and returns
        # a list of statements with frauds types

        entity_id = event.get('entity_id')
        account_id = event.get('account_id', None)
        include_inconsistent_transactions = event.get('include_inconsistent_transactions', False)
        accounts = list()
        if account_id:
            temp_account = get_account_for_entity(entity_id, account_id, to_reject_account)
            if temp_account:
                accounts = [temp_account]
        else:
            accounts = get_accounts_for_entity(entity_id, to_reject_account)

        fraud_statements = list()  # stores statement ids
        fraud_reasons = list()  # stores fraud dictionary

        for account in accounts:
            account_dict = account.get('item_data')
            statements = account_dict.get('statements')
            account_id = account_dict.get('account_id')
            is_od_account = account_dict.get('is_od_account', False)
            account_category = account_dict.get('account_category', False)
            input_is_od_account = account_dict.get('input_is_od_account', False)
            input_account_category = account_dict.get('input_account_category', False)
            account_category, _ = get_final_account_category(account_category, is_od_account, input_account_category, input_is_od_account)

            for stmt_id in statements:
                identity = get_complete_identity_for_statement(stmt_id)

                # masking every metadata fraud with author_fraud
                metadata_frauds = [_[0] for _ in fraud_category.items() if _[1]=='metadata']
                if identity.get("fraud_type", None) in metadata_frauds:
                    identity["fraud_type"] = "author_fraud"

                if identity.get('is_fraud'):
                    # metadata fraud present
                    fraud_statements.append(stmt_id)
                    fraud_reasons.append({
                        'statement_id': stmt_id,
                        'fraud_type': identity.get('fraud_type'),
                        'account_id': account_id,
                        'transaction_hash': None,
                        'fraud_category': 'metadata'
                    })
            # get non metadata frauds in required format
            disparities = get_non_metadata_frauds(account_id, include_inconsistent_transactions)
    
            for disparity in disparities:
                fraud_type = disparity.get('fraud_type', None)
                if account_category in ["CURRENT", "corporate", "overdraft"] and fraud_type == 'negative_balance':
                    continue
                
                fraud_statement_id = disparity.get('statement_id')
                if fraud_statement_id and (fraud_statement_id not in fraud_statements):
                    fraud_statements.append(fraud_statement_id)
    
                fraud_reasons.append(disparity)

        response = {
            'fraudulent_statements': fraud_statements,
            'fraud_type': fraud_reasons 
        }
        # print("Response for access_type = {} and entity_id ={} is {}".format(access_type,entity_id,response))
        return response

    elif access_type == 'ENTITY_BEHAVIOURAL_INDICATORS':
        entity_id = event.get('entity_id')
        behavioural_fraud_types = [_[0] for _ in fraud_category.items() if _[1] not in ('metadata', 'accounting')]
        behavioural_indicator = []
        
        for account in accounts:
            account_dict = account.get('item_data')
            account_id = account_dict.get('account_id')
            indicators = get_non_metadata_frauds(account_id)
            for disparity in indicators:
                if isinstance(disparity, dict) and disparity.get("fraud_type") in behavioural_fraud_types:
                    behavioural_indicator.append(disparity)
        
        return behavioural_indicator
    
    elif access_type == 'COMPLETE_ENTITY_PROGRESS':
        entity_id = event.get('entity_id')
        account_id = event.get('account_id', None)
        accounts = list()
        if account_id:
            temp_account = get_account_for_entity(entity_id, account_id, to_reject_account)
            if temp_account:
                accounts = [temp_account]
        else:
            accounts = get_accounts_for_entity(entity_id, to_reject_account)
        scanned_and_aa_statement_ids = event.get('scanned_and_aa_statement_ids', [])

        progress = list()
        statements_added = list()
        for account in accounts:
            account_statements = account.get('item_data').get('statements')
            for statement_id in account_statements:
                statements_added.append(statement_id)
                response = get_complete_entity_progress_for_statement(entity_id, statement_id)
                progress.append(response)
                
        #adding progress for statemnt_ids in case of nanonets because account id is not generated initially
        for statement_id in scanned_and_aa_statement_ids:
            if statement_id not in statements_added:
                response = get_complete_entity_progress_for_statement(entity_id, statement_id)
                progress.append(response)
        
        entity_enirchment = get_enrichment_for_entity(entity_id)
        if 'caching_status' in entity_enirchment.keys() and entity_enirchment['caching_status']=='processing':
            for index in range(len(progress)):
                if progress[index]['processing_status']=='completed':
                    progress[index]['processing_status'] = 'processing'
        
        # print("Response for access_type = {} and entity_id ={} is {}".format(access_type,entity_id,progress))
        return progress

    elif access_type == 'ENTITY_PROGRESS':
        entity_id = event.get('entity_id')
        account_id = event.get('account_id', None)
        scanned_and_aa_statement_ids = event.get('scanned_and_aa_statement_ids', [])
        if scanned_and_aa_statement_ids is None:
            scanned_and_aa_statement_ids = []

        accounts = list()
        if account_id:
            temp_account = get_account_for_entity(entity_id, account_id, to_reject_account)
            if temp_account:
                accounts = [temp_account]
        else:
            accounts = get_accounts_for_entity(entity_id, to_reject_account)

        progress = list()
        statements_added = list()
        for account in accounts:
            account_statements = account.get('item_data').get('statements')

            for statement_id in account_statements:
                statements_added.append(statement_id)
                statement_status, statement_message = get_progress(statement_id, 'processing_status')
                progress.append(
                    {
                        'statement_id': statement_id,
                        'status': statement_status,
                        'message': statement_message
                    })
        
        #adding progress for statement_ids in case of nanonets and aa because account id is not generated initially
        for statement_id in scanned_and_aa_statement_ids:
            if statement_id not in statements_added:
                statement_status, statement_message = get_progress(statement_id, 'processing_status')
                progress.append(
                    {
                        'statement_id': statement_id,
                        'status': statement_status,
                        'message': statement_message
                    })

        # print("Response for access_type = {} and entity_id ={} is {}".format(access_type,entity_id,progress))
        return progress

    elif access_type == 'ENTITY_TRANSACTIONS_PROGRESS':
        entity_id = event.get('entity_id')
        account_id = event.get('account_id', None)
        scanned_and_aa_statement_ids = event.get('scanned_and_aa_statement_ids', [])
        if scanned_and_aa_statement_ids is None:
            scanned_and_aa_statement_ids = []

        accounts = list()
        if account_id:
            temp_account = get_account_for_entity(entity_id, account_id, to_reject_account)
            if temp_account:
                accounts = [temp_account]
        else:
            accounts = get_accounts_for_entity(entity_id, to_reject_account)

        progress = list()
        statements_added = list()
        for account in accounts:
            account_statements = account.get('item_data').get('statements')

            for statement_id in account_statements:
                statements_added.append(statement_id)
                statement_status, statement_message = get_progress(statement_id, 'transactions_status')
                progress.append(
                    {
                        'statement_id': statement_id,
                        'status': statement_status,
                        'message': statement_message
                    })

        #adding progress for statemnt_ids in case of nanonets because account id is not generated initially
        for statement_id in scanned_and_aa_statement_ids:
            if statement_id not in statements_added:
                statement_status, statement_message = get_progress(statement_id, 'transactions_status')
                progress.append(
                    {
                        'statement_id': statement_id,
                        'status': statement_status,
                        'message': statement_message
                    })
        
        # print("Response for access_type = {} and entity_id = {} is {}".format(access_type,entity_id,progress))
        return progress

    elif access_type == 'ENTITY_IDENTITY_PROGRESS':
        entity_id = event.get('entity_id')
        account_id = event.get('account_id', None)
        scanned_and_aa_statement_ids = event.get('scanned_and_aa_statement_ids', [])
        if scanned_and_aa_statement_ids is None:
            scanned_and_aa_statement_ids = []

        accounts = list()
        if account_id:
            temp_account = get_account_for_entity(entity_id, account_id, to_reject_account)
            if temp_account:
                accounts = [temp_account]
        else:
            accounts = get_accounts_for_entity(entity_id, to_reject_account)

        progress = list()
        statements_added = list()
        for account in accounts:
            account_statements = account.get('item_data').get('statements')

            for statement_id in account_statements:
                statements_added.append(statement_id)
                # statement_status, statement_message = get_progress(entity_id, statement_id, 'identity_status')
                # transaction_status, _ = get_progress(entity_id, statement_id, 'transactions_status')
                response = get_complete_progress(statement_id)
                progress.append(
                    {
                        'statement_id': statement_id,
                        'status': response['identity_status'],
                        'message': response['identity_message'],
                        'transaction_status': response['transaction_status']
                    })
        
        #adding progress for statement_ids in case of nanonets and aa because account id is not generated initially
        for statement_id in scanned_and_aa_statement_ids:
            if statement_id not in statements_added:
                response = get_complete_progress(statement_id)
                progress.append(
                    {
                        'statement_id': statement_id,
                        'status': response['identity_status'],
                        'message': response['identity_message'],
                        'transaction_status': response['transaction_status']
                    })
        
        # print("Response for access_type = {} and entity_id ={} is {}".format(access_type,entity_id,progress))
        return progress

    elif access_type == 'ENTITY_SALARY_TRANSACTIONS':
        entity_id = event.get('entity_id')
        account_id = event.get('account_id', None)
        account_id_list = list()
        accounts = get_accounts_for_entity(entity_id, to_reject_account)

        all_salary_transactions = list()

        for account in accounts:
            accnt_id = account.get('account_id')
            account_id_list.append(accnt_id)
        if account_id:
            if account_id in account_id_list:
                account_id_list = [account_id]
            else:
                account_id_list = list()
        for account_id in account_id_list:
            salary_transactions = get_salary_transactions_from_ddb(account_id)
            # salary_transactions = remove_redundant_keys(salary_transactions)
            salary_transactions = keep_specific_keys(salary_transactions)
            salary_transactions = fill_transactions_na_key(salary_transactions)
            for salary_transaction in salary_transactions:
                salary_transaction['account_id'] = account_id
            all_salary_transactions += salary_transactions

        all_salary_transactions = transactions_sanity_checker(all_salary_transactions)

        # To prevent the lambda_client from viewing the salary calculation method key in the transactions
        for transaction in all_salary_transactions:
            if isinstance(transaction, dict) and 'calculation_method' in transaction:
                print("popping calculation method")
                transaction.pop('calculation_method', None)
        # print("Response for access_type = {} and entity_id ={} is {}".format(access_type,entity_id,all_salary_transactions))
        return all_salary_transactions

    elif access_type == 'ENTITY_RECURRING_TRANSACTIONS':
        entity_id = event.get('entity_id')
        account_id = event.get('account_id', None)
        account_id_list = list()
        accounts = get_accounts_for_entity(entity_id, to_reject_account)

        recurring_debit_transactions_list = list()
        recurring_credit_transactions_list = list()

        for account in accounts:
            accnt_id = account.get('account_id')
            account_id_list.append(accnt_id)
        if account_id:
            if account_id in account_id_list:
                account_id_list = [account_id]
                object_key = "recurring_transactions/account_{}".format(account_id)
            else:
                account_id_list = list()
        else:
            object_key = "recurring_transactions/entity_{}".format(entity_id)
        for account_id in account_id_list:
            debit_transactions, credit_transactions = get_recurring_transactions_list_from_ddb(account_id)
            for i in range(0, len(debit_transactions)):
                sanitized_transaction_list = keep_specific_keys(debit_transactions[i]['transactions'])
                debit_transactions[i]['transactions'] = fill_transactions_na_key(sanitized_transaction_list)
    
            for i in range(0, len(credit_transactions)):
                sanitized_transaction_list = keep_specific_keys(credit_transactions[i]["transactions"])
                credit_transactions[i]['transactions'] = fill_transactions_na_key(sanitized_transaction_list)

            recurring_debit_transactions_list += debit_transactions
            recurring_credit_transactions_list += credit_transactions
        
        # before returning the lists, also sort them on the basis of median amount in a descending order
        recurring_debit_transactions_list = sorted(recurring_debit_transactions_list, key=lambda x: x["median"], reverse=True)
        recurring_credit_transactions_list = sorted(recurring_credit_transactions_list, key=lambda x: x["median"], reverse=True)

        resultant_recurring_transactions = {
            'debit_transactions': recurring_debit_transactions_list,
            'credit_transactions': recurring_credit_transactions_list
        }
        
        response = None
        if (len(json.dumps(resultant_recurring_transactions))) < 120000: 
            response = resultant_recurring_transactions 
        else:
            resultant_recurring_transactions = json.dumps(resultant_recurring_transactions)
            s3_object = s3_resource.Object(BANK_CONNECT_DDB_FAILOVER_BUCKET, object_key)
            s3_object.put(Body=bytes(resultant_recurring_transactions, encoding='utf-8'))
            response = {"s3_object_key": object_key}
        # print("Response for access_type = {} and entity_id ={} is {}".format(access_type,entity_id,response))
        return response
    
    elif access_type == 'SESSION_STATUS':
        entity_id = event.get('entity_id')
        session_date_range = event.get('session_date_range')
        acceptance_criteria = event.get('acceptance_criteria', [])
        date_range_approval_criteria = event.get('date_range_approval_criteria', 0)
        is_missing_date_range_enabled = event.get('is_missing_date_range_enabled', False)
        accept_anything = event.get('accept_anything', False)
        accounts = get_accounts_for_entity(entity_id)

        status_response = map_session_account_status(entity_id, accounts, session_date_range, acceptance_criteria, date_range_approval_criteria, is_missing_date_range_enabled, accept_anything)

        return status_response

    elif access_type == 'ENTITY_DASHBOARD_METRICS':
        entity_id = event.get('entity_id')
        account_id = event.get('account_id', None)
        account_id_list = list()
        accounts = get_accounts_for_entity(entity_id, to_reject_account)
        predictors = get_predictors_for_entity(entity_id, to_reject_account)
        metrics_list = list()

        for account in accounts:
            accnt_id = account.get('account_id')
            account_id_list.append(accnt_id)
        if account_id:
            if account_id in account_id_list:
                account_id_list = [account_id]
            else:
                account_id_list = list()
        for account_id in account_id_list:
            transactions, hash_dict = get_transactions_for_account(entity_id, account_id)
            account_metrics = get_metrics(transactions)
            corresponding_predictor_obj = [_ for _ in predictors if _.get('account_id')==account_id]
            if len(corresponding_predictor_obj)>0:
                account_metrics['avg_balance'] = corresponding_predictor_obj[0].get("predictors", {}).get('avg_daily_closing_balance', 0.0)
            account_metrics.update({'account_id': account_id})
            metrics_list.append(account_metrics)

        response = {"account_wise_metrics": metrics_list}
        # print("Response for access_type = {} and entity_id ={} is {}".format(access_type,entity_id,response))
        return response

    elif access_type == 'ENTITY_ALL_MONTHS_UPDATED':
        """
            Response Format :
            {
                '6e21e38d-f5a5-415a-8c04-25c96fa32e4e': 
                {
                    'bank': 'kotak', 
                    'months_on_txn': [], 
                    'months_on_extraction': ['2024-03', '2024-04', '2024-05', '2024-06', '2024-07', '2024-08'], 
                    'missing_date_range_on_extraction': {}, 
                    'missing_months_on_extraction': ['2024-09']
                }
            }
        """

        entity_id = event.get('entity_id')
        account_id = event.get('account_id', None)
        is_missing_date_range_enabled = event.get('is_missing_date_range_enabled', False)
        session_date_range = event.get('session_date_range', None)
        acceptance_criteria = event.get('acceptance_criteria', list())
        session_date_range = convert_date_range_to_datetime(session_date_range, "%d/%m/%Y")

        response = get_account_wise_months(entity_id, account_id, is_missing_date_range_enabled, session_date_range)
        account_data = response.get(account_id, dict())
        missing_months_on_extraction = account_data.get('missing_months_on_extraction', list())
        months_on_extraction = account_data.get('months_on_extraction', list())

        if 'ignore_last_month' in acceptance_criteria and response is not None:
            session_to_date = session_date_range.get('to_date', None) if isinstance(session_date_range, dict) else None
            current_datetime = datetime.now()
            current_day = int(current_datetime.strftime('%d'))

            _,total_days = calendar.monthrange(int(current_datetime.strftime('%Y')),int(current_datetime.strftime('%m')))
            current_yy_mm = current_datetime.strftime('%Y-%m')
            session_to_date_yy_mm = session_to_date.strftime('%Y-%m') if session_to_date is not None else None

            if session_to_date_yy_mm == current_yy_mm and total_days != current_day and account_id in list(response.keys()):
                if current_yy_mm in missing_months_on_extraction:
                    missing_months_on_extraction.remove(current_yy_mm)
                    response[account_id]['missing_months_on_extraction'] = missing_months_on_extraction
                if current_yy_mm not in months_on_extraction:
                    months_on_extraction.append(current_yy_mm)
                    response[account_id]['months_on_extraction'] = months_on_extraction
        
        return response
    

    elif access_type == 'ENTITY_TOP_CREDITS_DEBITS':
        entity_id = event.get('entity_id')
        is_sme = event.get('is_sme', False)
        correction = event.get('correction', False)
        # get all transactions for an entity
        transactions = get_transactions_for_entity(entity_id, is_sme, to_reject_account)
        transactions = transactions_sanity_checker(transactions)
        if len(transactions) > 0:
            top_5_debit, top_5_credit = top_debit_credit(transactions, correction)
            top_5_credits = {}
            top_5_debits = {}
            top_5_debits['top_5_debit'] = top_5_debit
            top_5_credits['top_5_credit'] = top_5_credit
            top_5_credits.update(top_5_debits)
        # print("Response for access_type = {} and entity_id ={} is {}".format(access_type,entity_id,top_5_credits))
        return top_5_credits

    elif access_type == 'ENTITY_ROLLING_MONTHLY_ANALYSIS':
        entity_id = event.get('entity_id')
        is_sme = event.get('is_sme', False)
        # get all transactions for an entity
        transactions = get_transactions_for_entity(entity_id, is_sme, to_reject_account)
        transactions = transactions_sanity_checker(transactions)
        
        accounts = get_accounts_for_entity(entity_id, to_reject_account)
        all_salary_transactions = list()
        for account in accounts:
            account_id = account.get('account_id')
            salary_transactions = get_salary_transactions_from_ddb(account_id)
            all_salary_transactions += salary_transactions

        response_dict = dict()
        if len(transactions) > 0:
            # get the monthly analysis
            all_salary_transactions = transactions_sanity_checker(all_salary_transactions)
            response_dict = rolling_month_analysis_func(transactions, all_salary_transactions)
        # return the dictionary
        # print("Response for access_type = {} and entity_id ={} is {}".format(access_type,entity_id,response_dict))
        return response_dict
    
    elif access_type == 'ACCOUNT_IDENTITY':
        entity_id = event.get('entity_id')
        accounts = get_accounts_for_entity(entity_id, to_reject_account)

        accounts_list = list()
        for account in accounts:
            account_dict = account.get('item_data')
            statements = account_dict.get('statements')
            account_number = account_dict.get('account_number', None)
            if account_number == None:
                continue
    
            if statements:
                identity = get_identity_for_statement(statements[0])
                identity['bank'] = account_dict.get('bank')
                identity['ifsc'] = account_dict.get('ifsc')
                identity['micr'] = account_dict.get('micr')

                identity['account_category'], identity['is_od_account'] = get_final_account_category(account_dict.get('account_category', None), account_dict.get('is_od_account', None), account_dict.get('input_account_category', None),
                                            account_dict.get('input_is_od_account', None))
                identity['od_limit'] = account_dict.get('od_limit', None)
                identity['credit_limit'] = account_dict.get('credit_limit', None)
                if identity['od_limit'] == None:
                    identity['od_limit'] = identity['credit_limit']
                if identity['credit_limit'] == None:
                    identity['credit_limit'] = identity['od_limit']

            accounts_list.append(identity)
        # print("Response for access_type = {} and entity_id ={} is {}".format(access_type,entity_id,accounts_list))
        return accounts_list

    elif access_type == 'ACCOUNT_TRANSACTIONS':
        entity_id = event.get('entity_id')
        account_id = event.get('account_id')

        transactions = []
        temp_account = get_account_for_entity(entity_id, account_id, to_reject_account)
        if temp_account:
            transactions, _ = get_transactions_for_account(entity_id, account_id)
        # transactions = remove_redundant_keys(transactions)
        transactions = keep_specific_keys(transactions)
        transactions = fill_transactions_na_key(transactions)
        transactions = transactions_sanity_checker(transactions)
        print("response size:", sys.getsizeof(transactions))

        # TODO: If response size > 120000, serve from S3.

        # print("Response for access_type = {} and entity_id ={} is {}".format(access_type,entity_id,transactions))
        return transactions

    elif access_type == 'ACCOUNT_TOP_CREDITS_DEBITS':
        entity_id = event.get('entity_id')
        account_id = event.get('account_id')
        correction = event.get('correction', False)
        req_transactions_count = event.get("req_transactions_count", DEFAULT_REQUIRED_TRANSACTIONS_COUNT)

        transactions, hash_dict = get_transactions_for_account(entity_id, account_id)
        transactions = transactions_sanity_checker(transactions)
        
        response_dict = {f'top_{req_transactions_count}_debit': {}, f'top_{req_transactions_count}_credit': {}}
        if len(transactions) > 0:
            top_debit, top_credit = top_debit_credit(transactions, correction, req_transactions_count)
            response_dict[f'top_{req_transactions_count}_debit'] = top_debit
            response_dict[f'top_{req_transactions_count}_credit'] = top_credit

        # print("Response for access_type = {} and entity_id ={} is {}".format(access_type,entity_id,response_dict))
        return response_dict
    
    elif access_type == 'ACCOUNT_MONTHLY_ANALYSIS':
        entity_id = event.get('entity_id')
        account_id = event.get('account_id')

        response_list = get_monthly_analysis_for_entity_updated(entity_id, to_reject_account)

        for account_data in response_list:
            if account_id in account_data.keys():
                return account_data[account_id].get('monthly_analysis', dict())
        return dict()
    
    elif access_type == 'ACCOUNT_RECURRING_TRANSACTIONS':
        entity_id = event.get('entity_id')
        account_id = event.get('account_id')

        debit_transactions, credit_transactions = [], []
        temp_account = get_account_for_entity(entity_id, account_id, to_reject_account)
        if temp_account:
            debit_transactions, credit_transactions = get_recurring_transactions_list_from_ddb(account_id)
        for i in range(0, len(debit_transactions)):
            for j in range(0, len(debit_transactions[i]['transactions'])):
                for key in redundant_keys:
                    debit_transactions[i]['transactions'][j].pop(key, None)
            debit_transactions[i]['transactions'] = fill_transactions_na_key(debit_transactions[i]['transactions'])
        for i in range(0, len(credit_transactions)):
            for j in range(0, len(credit_transactions[i]['transactions'])):
                for key in redundant_keys:
                    credit_transactions[i]['transactions'][j].pop(key, None)
            credit_transactions[i]['transactions'] = fill_transactions_na_key(credit_transactions[i]["transactions"])
        
        resultant_recurring_transactions = {
            'debit_transactions': debit_transactions,
            'credit_transactions': credit_transactions
        }
        
        #TODO for all access type in access lambda send a flag for which we can directly send dump without storing in s3
        #NOTE before adding data into s3 do not use this access type for django

        # print("Response for access_type = {} and entity_id ={} is {}".format(access_type,entity_id,response))
        return resultant_recurring_transactions

    elif access_type == 'STATEMENT_STATS':
        entity_id = event.get('entity_id')
        accounts = get_accounts_for_entity(entity_id)

        response = []
        for account in accounts:
            account_id = account.get('account_id')
            statements = get_statement_ids_for_account_id(entity_id, account_id)
            for statement_id in statements:
                to_insert_obj = {
                    "entity_id": entity_id,
                    "account_id": account_id,
                    "statement_id": statement_id,
                    "first_transaction_timestamp": None,
                    "last_transaction_timestamp": None,
                    "bank": None
                }
                statement_transactions, hash_page_number_map = get_transactions_for_statement(statement_id, False, False)
                if len(statement_transactions)>0:
                    to_insert_obj["first_transaction_timestamp"] = statement_transactions[0].get("date")
                    to_insert_obj["last_transaction_timestamp"] = statement_transactions[-1].get("date")
                
                to_insert_obj["bank"] = get_bank_name_for_statement(statement_id)

                response.append(to_insert_obj)
        return response

    elif access_type == 'SESSION_UPLOAD_STATUS':
        session_date_range = event.get('session_date_range')
        is_missing_date_range_enabled = event.get('is_missing_date_range_enabled', False)
        need_account_status = event.get("need_account_status", False)
        acceptance_criteria = event.get('acceptance_criteria', [])
        date_range_approval_criteria = event.get('date_range_approval_criteria', 0)
        accept_anything = event.get('accept_anything', False)

        response = get_upload_status(entity_id, is_missing_date_range_enabled, session_date_range, to_reject_account,
                                     need_account_status, acceptance_criteria, date_range_approval_criteria,
                                     accept_anything)
        return response


    elif access_type == 'MOVE_CLICKHOUSE_DATA':
        print("Just intering the access handler MOVE_CLICKHOUSE_DATA================================")
        local_logging_context: LoggingContext = LoggingContext(source="access_handler", entity_id=entity_id)
        source_table = event.get("source_table")
        action = event.get("action")
        fanout_data_map = event.get("fanout_data_map")
        print(f"This is the event inside the MOVE_CLICKHOUSE_DATA = {event}")
        print(f"THis is the local_logging_context = {local_logging_context}")

        return move_clickhouse_data(source_table, action, fanout_data_map, local_logging_context)


def get_monthly_analysis_for_entity_updated(entity_id, to_reject_account=False):
    payload = {
        'entity_id': entity_id,
        'is_updated_requested': True,
        'to_reject_account': to_reject_account
    }
    print(f"\n\nTriggering enrichments monthly analysis updated Lambda for entity_id: {entity_id}\n\n")
    response = lambda_client.invoke(
            FunctionName = ENRICHMENT_MONTHLY_ANALYSIS_FUNCTION, 
            Payload = json.dumps(payload)
        )

    http_status = response.get('ResponseMetadata', dict()).get('HTTPStatusCode')
    headers = response.get('ResponseMetadata', dict()).get('HTTPHeaders', dict())

    if http_status != 200 or headers.get('x-amz-function-error') is not None:
        return dict()
    response_payload = response['Payload']._raw_stream.data.decode("utf-8")

    return json.loads(response_payload)

def get_predictors_for_entity(entity_id, to_reject_account=False):
    payload = {
        'entity_id': entity_id,
        'to_reject_account': to_reject_account
    }
    
    response = lambda_client.invoke(
            FunctionName = ENRICHMENT_PREDICTORS_FUNCTION, 
            Payload = json.dumps(payload)
        )

    http_status = response.get('ResponseMetadata', dict()).get('HTTPStatusCode')
    headers = response.get('ResponseMetadata', dict()).get('HTTPHeaders', dict())

    if http_status != 200 or headers.get('x-amz-function-error') is not None:
        return dict()
    
    response_payload = response['Payload']._raw_stream.data.decode("utf-8")
    return json.loads(response_payload)

def analyze_csv_handler(event, context):
    if "re_extraction" in event.keys() and event.get("re_extraction", False):
        bucket = event.get("bucket")
        key = event.get("key")
    else:
        record = event["Records"][0]
        bucket = record["s3"]["bucket"]["name"]
        key = record["s3"]["object"]["key"]

    response = s3.get_object(Bucket=bucket, Key=key)

    response_metadata = response.get("Metadata")

    statement_id = response_metadata.get("statement_id")
    bank = response_metadata.get("bank")
    entity_id = response_metadata.get("entity_id")

    if not statement_id:
        # no need to process if cannot get statement id
        return {
            "message": "ignored, statement_id not found in metadata"
        }

    # save csv to a temporary file
    file_path = "/tmp/{}.csv".format(statement_id)
    with open(file_path, "wb") as file_object:
        file_object.write(response["Body"].read())

    # get all the extracted transactions using fsmlib
    all_txns_list_of_lists = get_transactions_list_of_lists_csv(file_path, bank)

    # get the number of pages in which csv is divided
    number_of_pages = len(all_txns_list_of_lists)

    # make an entry for number of pages in dynamodb
    bank_connect_statement_table.update_item(
        Key={"statement_id": statement_id},
        UpdateExpression="set {} = :s, created_at = :c".format("page_count"),
        ExpressionAttributeValues={
            ":s": number_of_pages,
            ":c": str(int(time.time()))
        }
    )

    for page_number, txns_list in enumerate(all_txns_list_of_lists):
        # create a DynamoDB object
        time_stamp_in_mlilliseconds = time.time_ns()
        dynamo_object = {
            "statement_id": statement_id,
            "page_number": page_number,
            "item_data": json.dumps(txns_list, default=str),
            "transaction_count": len(txns_list),
            'created_at': time_stamp_in_mlilliseconds,
            'updated_at': time_stamp_in_mlilliseconds
        }

        bank_connect_transactions_table.put_item(Item=dynamo_object)

    return {"message": "success"}

def analyze_transactions_finvu_aa(event, context):
    
    statement_id = event.get("statement_id", "")
    entity_id = event.get("entity_id", "")
    name = event.get("name", "")
    bank_name = event.get("bank_name", "")
    bucket_name = event.get("bucket_name", "")
    enrichment_regexes = event.get("enrichment_regexes", {})
    session_date_range = event.get('session_date_range', {'from_date': None, 'to_date': None})
    statement_meta_data_for_warehousing = event.get('statement_meta_data_for_warehousing', {})
    local_logging_context = event.get("local_logging_context")

    if not local_logging_context:
        local_logging_context: LoggingContext = LoggingContext(source="analyze_transactions_finvu_aa")
        LAMBDA_LOGGER.info("Initiating the extraction (AA) handler flow.", extra=local_logging_context.store)

    set_tag("entity_id", entity_id)
    set_tag("statement_id", statement_id)
    set_context("analyze_transactions_finvu_aa_event_payload", event)
    
    if not statement_id:
        LAMBDA_LOGGER.info("Ignored, Statement ID not found in event", extra=local_logging_context.store)
        return {
            "message": "ignored, statement_id not found in event"
        }
    
    if not entity_id:
        LAMBDA_LOGGER.info("Ignored, Entity ID not found in event", extra=local_logging_context.store)
        return {
            "message": "ignored, entity_id not found in event"
        }

    if not bucket_name:
        LAMBDA_LOGGER.info("Ignored, Entity ID not found in event", extra=local_logging_context.store)
        return {
            "message": "ignored, bucket name not found in event"
        }

    aa_data_file_key = event.get("aa_data_file_key", None)

    if aa_data_file_key is None:
        LAMBDA_LOGGER.info("ignored, aa data file key not found in event", extra=local_logging_context.store)
        return {
            "message": "ignored, aa data file key not found in event"
        }
    
    # getting aa data from s3 bucket
    aa_data = get_json_from_s3_file(bucket_name, aa_data_file_key)

    # getting body from aa data -> array
    body = aa_data.get("body", dict())

    # getting financial info objects -> array
    fiObjects = body[0]["fiObjects"]

    # getting the first fiObject
    firstFiObject = fiObjects[0]

    # getting the transactions list from aa
    aa_transactions_list = firstFiObject.get("Transactions", dict()).get("Transaction", [])

    print("number of aa transactions: {}".format(len(aa_transactions_list)))

    does_raw_trxns_contains_inconsistency = is_raw_aa_transactions_inconsistent(aa_transactions_list, bank_name)
    if does_raw_trxns_contains_inconsistency:
        # sort the transactions in ascending order
        aa_transactions_list = sorted(aa_transactions_list, key=lambda d: d["valueDate"])
        aa_transactions_list = sorted(aa_transactions_list, key=lambda d: d["transactionTimestamp"])

    # getting transactions list of list for page count
    txns_list_of_lists = get_transactions_list_of_lists_finvu_aa(aa_transactions_list)

    number_of_pages = len(txns_list_of_lists)

    print("number of pages: {}".format(number_of_pages))

    # make entry for number of pages in ddb
    bank_connect_statement_table.update_item(
        Key={"statement_id": statement_id},
        UpdateExpression="set {} = :s, created_at = :c, pages_done = :d".format('page_count'),
        ExpressionAttributeValues={':s': number_of_pages, ':c': str(int(time.time())), ':d': 0}
    )

    number_of_pages = len(txns_list_of_lists)
    
    params = {
        "entity_id": entity_id,
        "statement_id": statement_id,
        "name": name,
        "bank_name": bank_name,
        "number_of_pages": number_of_pages,
        "enrichment_regexes": enrichment_regexes,
        "session_date_range": session_date_range,
        "statement_meta_data_for_warehousing": statement_meta_data_for_warehousing
    }

    if not IS_SERVER:
        function_name = FINVU_AA_PAGE_FUNCTION
        # invoking analyze lambda for every page
        for page_number in range(number_of_pages):
            params["page_number"] = page_number
            params["aa_transactions_page"] = txns_list_of_lists[page_number]
            payload = json.dumps(params)
            lambda_client.invoke(FunctionName=function_name, Payload=payload, InvocationType="Event")
    else:
        params.pop("enrichment_regexes")

        all_messages = []
        current_batch = []
        MESSAGE_BATCH_SIZE = 10
        current_batch_count = 0
        for page_number in range(0, number_of_pages):
            params["page_number"] = page_number
            params["aa_transactions_page"] = txns_list_of_lists[page_number]
            message_group_id = f"{statement_id}_{page_number}"
            message = {
                "Id": message_group_id,
                "MessageBody": json.dumps(params),
                "MessageDeduplicationId": message_group_id,
                "MessageGroupId": message_group_id
            }
            current_batch.append(message)
            current_batch_count += 1
            if current_batch_count == MESSAGE_BATCH_SIZE:
                all_messages.append(current_batch)
                current_batch = []
                current_batch_count = 0
        
        if current_batch_count:
            all_messages.append(current_batch)
        
        for message_batch in all_messages:
            response = sqs_client.send_message_batch(
                QueueUrl = AA_TRANSACTIONS_PAGE_QUEUE_URL,
                Entries = message_batch
            )
            print(response)
    return {
        "message": "success"
    }

def analyze_transactions_finvu_aa_page(event, context):
    page_number = int(event.get("page_number"))
    entity_id = event.get("entity_id")
    statement_id = event.get("statement_id")
    aa_transactions_page = event.get("aa_transactions_page", [])
    name = event.get("name", "")
    bank_name = event.get("bank_name", "")
    number_of_pages = event.get("number_of_pages")
    enrichment_regexes = event.get("enrichment_regexes", {})
    session_date_range = event.get("session_date_range", {"from_date": None, "to_date": None})
    statement_meta_data_for_warehousing = event.get('statement_meta_data_for_warehousing', {})
    org_metadata = event.get('org_metadata', dict())
    local_logging_context = event.get("local_logging_context")

    if not local_logging_context:
        local_logging_context: LoggingContext = LoggingContext(source="analyze_transactions_finvu_aa")
        LAMBDA_LOGGER.info("Initiating the extraction (AA) handler flow.", extra=local_logging_context.store)

    set_tag("entity_id", entity_id)
    set_tag("statement_id", statement_id)
    set_context("analyze_transactions_finvu_aa_page_event_payload", event)

    LAMBDA_LOGGER.info(f"currently processing page {page_number} of statement_id {statement_id}", extra=local_logging_context.store)
    LAMBDA_LOGGER.info(f"Number of transactions in this page are {len(aa_transactions_page)}", extra=local_logging_context.store)

    country = get_country_for_statement(statement_id)

    if not IS_SERVER:
        # check if data is retrieved from server and populate the files
        store_data_from_enrichment_regexes(enrichment_regexes, bank_name, country)
        check_and_get_everything(bank_name, country)

    transactions, error_message = get_transactions_finvu_aa(aa_transactions_page, bank_name, name, session_date_range)
    if error_message is not None:
        update_progress(statement_id, 'transactions_status', 'failed', "Data could not be retrieved from the Account Aggregator")
        update_progress(statement_id, 'processing_status', 'failed', "Data could not be retrieved from the Account Aggregator")
        update_progress_payload = {
            'is_extracted': False,
            'is_complete': False,
            'account_id': get_account_id_for_statement(statement_id)
        }
        update_progress_on_dashboard(statement_id, update_progress_payload, entity_id)
        return {
            "message": "failed",
        }
    
    transactions = update_transactions_on_session_date_range(session_date_range, transactions, statement_id, page_number)
    
    categorizer = SingleCategory(bank_name=bank_name, transactions=transactions, categorize_server_ip=CATEGORIZE_RS_PRIVATE_IP)
    transactions = categorizer.categorize_from_forward_mapper()
    number_of_transactions = len(transactions)

    LAMBDA_LOGGER.info(f"Number of Transactions for statement id {statement_id} is {number_of_transactions}", extra=local_logging_context.store)

    time_stamp_in_mlilliseconds = time.time_ns()
    ddb_object = {
        "statement_id": statement_id,
        "page_number": page_number,
        "item_data": json.dumps(transactions, default=str),
        "transaction_count": number_of_transactions,
        'created_at': time_stamp_in_mlilliseconds,
        'updated_at': time_stamp_in_mlilliseconds
    }
    bank_connect_transactions_table.put_item(Item=ddb_object)
    update_bsa_extracted_count(entity_id, statement_id, page_number, number_of_pages,statement_meta_data_for_warehousing, org_metadata=org_metadata)


def karur_data_fsmlib_transformation_handler(event, context):

    data = event.get('transaction_data')
    table = json.loads(data)
    page_num = int(event.get('page_num'))
    bucket = str(event.get('bucket'))
    key = str(event.get('key'))
    name = event.get('name', '')
    number_of_pages = event.get('number_of_pages', 0)
    country = event.get("country", "IN")
    enrichment_regexes = event.get("enrichment_regexes", {})
    account_category = event.get("account_category", None)
    identity = event.get("identity", {})
    session_date_range = event.get("session_date_range", {'from_date':None, 'to_date':None})
    response = s3.get_object(Bucket=bucket, Key=key)

    response_metadata = response.get('Metadata')

    entity_id = response_metadata.get('entity_id')
    bank = response_metadata.get('bank_name')
    password = response_metadata.get('pdf_password')
    statement_id = response_metadata.get('statement_id')
    org_metadata = event.get('org_metadata', dict())
    
    # check if data is retrieved from server and populate the files
    store_data_from_enrichment_regexes(enrichment_regexes, bank, country)

    # check and get all regexes for this bank and country
    check_and_get_everything(bank, country)

    file_path = "/tmp/{}.pdf".format(statement_id)
    with open(file_path, 'wb') as file_obj:
        file_obj.write(response['Body'].read())

    final_transactions, removed_opening_balance_date = get_transactions_for_karur(file_path, bank, password, page_num, name, table, account_category)
    if removed_opening_balance_date!=None:
        update_field_for_statement(statement_id, f'removed_date_opening_balance_{page_num}', removed_opening_balance_date)

    final_transactions = update_transactions_on_session_date_range(session_date_range, final_transactions, statement_id, page_num)

    number_of_transactions = len(final_transactions)
    
    print('Found {} transactions'.format(number_of_transactions))
    time_stamp_in_mlilliseconds = time.time_ns()
    dynamo_object = {
        "statement_id": statement_id,
        "page_number": page_num,
        "item_data": json.dumps(final_transactions, default=str),
        "transaction_count": number_of_transactions,
        'created_at': time_stamp_in_mlilliseconds,
        'updated_at': time_stamp_in_mlilliseconds
    }

    update_bsa_extracted_count(entity_id, statement_id, page_num, number_of_pages, org_metadata=org_metadata)

    # delete file after usage
    if os.path.exists(file_path):
        os.remove(file_path)

    bank_connect_transactions_table.put_item(Item=dynamo_object)


def generate_statement_page_hashes(event, context):
    """
    This lambda fn downloads a pdf
    and generates pdf pages hashes with text using 
    utility functions written in fsmlib
    """
    print("event recieved: {}".format(event))

    records = event.get("Records")

    for record in records:

        start_time = time.time()

        try:
            record_body = json.loads(record.get("body", ""))
        except Exception as e:
            print("could not parse body, record: {}, exception: {}".format(record, e))
            continue

        account_number = record_body.get("account_number", None)
        name = record_body.get("name", None)
        s3_file_key = record_body.get("s3_file_key", None)
        s3_file_bucket = record_body.get("s3_file_bucket", None)

        if account_number is None or account_number == "":
            return {
                "message": "account_number is requried"
            }

        if s3_file_key is None or s3_file_key == "":
            return {
                "message": "s3_file_key is required"
            }

        if s3_file_bucket is None or s3_file_bucket == "":
            return {
                "message": "s3_file_bucket is required"
            }

        # download the file
        file_response = s3.get_object(Bucket=s3_file_bucket, Key=s3_file_key)

        file_metadata = file_response.get('Metadata')

        statement_id = file_metadata.get('statement_id')
        # bank = file_metadata.get('bank_name')
        pdf_password = file_metadata.get('pdf_password')

        print("file_metadata: {}".format(file_metadata))

        if not statement_id:
            # no need to process if can't get statement id
            return {"message": "ignored, statement_id not found in metadata"}

        # write a temporary file with content
        file_path = "/tmp/{}.pdf".format(statement_id)
        with open(file_path, 'wb') as file_obj:
            file_obj.write(file_response['Body'].read())

        try:
            pdf_page_hashes_with_text = get_pdf_page_hashes_with_page_text(file_path, pdf_password)
        except Exception as e:
            # TODO: send a notification that hash generation failed
            print("\nCould not generate pdf hashes, exception: {}\n".format(e))
            return

        hash_end_time = time.time()
        print("it took {} seconds to create pdf hashes".format(hash_end_time-start_time))

        print('calling dashboard API')
        api_url = '{}/bank-connect/v1/internal/check_page_hash_fraud/'.format(DJANGO_BASE_URL)

        payload = {
            "statement_id": statement_id,
            "account_number": account_number,
            "name": name,
            "statement_page_hashes_with_text": pdf_page_hashes_with_text
        }

        headers = {
            'x-api-key': API_KEY,
            'Content-Type': "application/json",
        }

        payload = json.dumps(payload, default=str)

        retries = 3
        sleep_duration = 5  # in seconds
        while retries:
            response = call_api_with_session(api_url,"POST", payload, headers)
            if response.status_code == 200:
                break
            retries -= 1
            time.sleep(sleep_duration)
        
        if retries == 0:
            print("could not call dashboard api for page hash fraud check")
        else:
            print("successfully called dashboard api for page hash fraud check")

        end_time = time.time()
        print("it took {} seconds to make api call".format(end_time-hash_end_time))


def convert_table_data_to_trans(table_data, identity, template):
    from library.utils import get_date_format
    from library.statement_plumber import amount_to_float, get_transaction_type, get_amount
    if len(table_data) == 0:
        return []
    columns = template['column'][0]
    bank_name = identity.get('identity', {}).get('bank_name', None)

    transactions = list()
    for i in range(0, len(table_data)):
        tmp_trans = dict()
        data = table_data[i]
        if len(data) != len(columns):
            continue
        for j in range(len(data)):
            tmp_trans[columns[j]] = data[j]

        trans = dict()
        trans['transaction_type'] = get_transaction_type({'transaction_type':tmp_trans.get('transaction_type', None),'credit':tmp_trans.get('credit',None),'debit':tmp_trans.get('debit',None),'amount': tmp_trans.get('amount',None)}, identity.get('country_code', "IN"))
        amount = get_amount({'credit':tmp_trans.get('credit',None),'debit':tmp_trans.get('debit',None),'amount': tmp_trans.get('amount',None)}, bank_name)

        amount = amount_to_float(amount)
        trans["amount"] = amount
        balance = amount_to_float(tmp_trans.get('balance', None))
        trans["balance"] = balance
        date = get_date_format(tmp_trans.get('date', None))
        trans["date"] = date
        trans["transaction_note"] = tmp_trans.get('transaction_note', None) 
        trans["chq_num"] = tmp_trans.get('chq_num', None) 

        if amount == None or balance == None or date == False or tmp_trans.get('transaction_note', None) == None:
            continue
        transactions.append(trans)
    return transactions

def get_data_for_template_handler(event, context):
    #bank_connect internal tool handller , to extract data from a perticular template 
    local_logging_context: LoggingContext = LoggingContext(source="get_data_for_template_handler")
    print("event was: {}".format(event))
    transaction_flag = event.get("transaction_flag", None)
    bucket = str(event.get('bucket'))
    key = str(event.get('key'))
    page_num = event.get('page_num', None)
    template = event.get('template')
    bank = event.get("bank", None)
    new_flow = event.get("new_flow", False)
    template_type = event.get("template_type", False)
    is_credit_card = key!=None and key.split('/')[0]=='cc_pdfs'

    if key == '' or key is None:
        return {"message": "Key cannot be empty or null"}

    if bucket == '' or bucket is None:
        return {"message": "bucket cannot be empty or null"}

    response = s3.get_object(Bucket=bucket, Key=key)

    response_metadata = response.get('Metadata')

    #bank_name = response_metadata.get('bank_name')
    password = response_metadata.get('pdf_password')
    statement_id = response_metadata.get('statement_id')


    # write a temporary file with content
    file_path = "/tmp/{}.pdf".format(statement_id)
    with open(file_path, 'wb') as file_obj:
        file_obj.write(response['Body'].read())
    #template = json.loads(template)
    
    doc = read_pdf(file_path, password)

    if template_type == 'get_table_data':
        if page_num == None:
            remove_local_file(file_path)
            return {"message": "page_num cannot be empty or null"}
        
        all_pages = get_pages(file_path, password)
        p_page = all_pages[page_num]
        edges = p_page.edges
        
        vertical_lines = template.get("vertical_lines", [])
        horizontal_lines = template.get("horizontal_lines", [])
        from library.get_edges_test import get_df_graphical_lines
        data, _ = get_df_graphical_lines(file_path, password, page_num, horizontal_lines, vertical_lines, plumber_page_edges=edges)
        remove_local_file(file_path)
        return data
    
    if template_type == 'get_processed_ddb_data':
        if page_num == None:
            remove_local_file(file_path)
            return {"message": "page_num cannot be empty or null"}
        trans_dict = get_transactions_for_statement_page(statement_id, page_num)
        for txn in trans_dict:
            txn['date'] = str(txn['date'])
            txn.pop('salary_confidence_percentage', None)
            txn.pop('salary_calculation_method', None)
        
        remove_local_file(file_path)
        return trans_dict

    if template_type == 'update_table_data' or template_type == 'update_processed_table_data':
        
        import warnings
        import pandas as pd
        from library.transaction_channel import get_transaction_channel
        from library.transaction_description import get_transaction_description
        from library.utils import add_hash_to_transactions_df
        from library.extract_txns_fitz import remove_closing_balance, remove_opening_balance


        warnings.simplefilter(action = "ignore", category = FutureWarning)
        pd.options.mode.chained_assignment = None


        table_data = event.get("table_data", None)
        if page_num == None:
            remove_local_file(file_path)
            return {"message": "page_num cannot be empty or null"}
        if table_data == None:
            remove_local_file(file_path)
            return {"message": "table_data cannot be empty or null"}

        identity = get_complete_identity_for_statement(statement_id)

        if template_type == 'update_table_data':
            transactions = convert_table_data_to_trans(table_data, identity, template)
        else:
            tmp_transactions = table_data
            transactions = list()
            account_number = identity.get('identity', {}).get('account_number', '')
            account_category = ""
            for txn in tmp_transactions:
                if txn.get('account_category'):
                    account_category = txn.get('account_category')
                    break

            for trans in tmp_transactions:
                from library.utils import get_date_format
                from library.statement_plumber import amount_to_float
                trans['amount'] = amount_to_float(trans.get('amount',None))
                trans['balance'] = amount_to_float(trans.get('balance',None))
                trans['date'] = get_date_format(trans.get('date', None))

                if 'category' not in  trans.keys():
                    trans['category'] = ""
                if 'page_number' not in trans.keys():
                    trans['page_number'] = page_num
                if 'optimizations' not in trans.keys():
                    trans['optimizations'] = []
                if 'account_number' not in trans.keys():
                    trans['account_number'] = account_number
                if 'account_category' not in trans.keys():
                    trans['account_category'] = account_category
                if 'is_in_session_date_range' not in trans.keys():
                    trans['is_in_session_date_range'] = True

                if trans['date'] != False and trans['amount'] != None and trans['balance'] != None:
                    transactions.append(trans)
                
                account_number = trans['account_number']
                account_category = trans['account_category']
        
        # check and get all regexes for this bank and country
        check_and_get_everything(identity.get('identity', {}).get('bank_name', None), identity.get('country_code', "IN"))

        trans_df = get_transaction_channel(pd.DataFrame(transactions), identity.get('identity', {}).get('bank_name', None))
        trans_df = get_transaction_description(trans_df,identity.get('name',None))
        trans_df = add_hash_to_transactions_df(trans_df)

        transaction_list = trans_df.to_dict('records')
        categorizer = SingleCategory(bank_name=bank, transactions=transaction_list, categorize_server_ip=CATEGORIZE_RS_PRIVATE_IP)
        transaction_list = categorizer.categorize_from_forward_mapper()
        
        trans_df = pd.DataFrame(transaction_list)

        trans_dict, removed_date_opening_balance = remove_opening_balance(trans_df.to_dict('records'))
        trans_dict, removed_date_closing_balance = remove_closing_balance(trans_dict)

        time_stamp_in_mlilliseconds = time.time_ns()
        dynamo_object = {
            'statement_id': statement_id,
            'page_number': page_num,
            'item_data': json.dumps(trans_dict, default=str),
            'transaction_count': len(trans_dict),
            'created_at': time_stamp_in_mlilliseconds,
            'updated_at': time_stamp_in_mlilliseconds
        }
        bank_connect_transactions_table.put_item(Item=dynamo_object)
        for i in range(len(trans_dict)):
            trans_dict[i]['date'] = str(trans_dict[i]['date'])
        
        remove_local_file(file_path)
        return trans_dict

    if new_flow:
        try:
            print("Extracting from the new flow")
            template_json = [template]
            
            # calling fsmlib function toextract identity
            if template_type == "name_bbox":
                template_data, _, _ = get_name(doc, template_json, bank, file_path)
            elif template_type == "accnt_bbox":
                template_data, _, _ = get_account_num(doc, template_json, bank, file_path)
            elif template_type == "date_bbox":
                template_data, _ = get_date_range(doc, bank, template_json)
                template_data_all_text, _ = get_date_range(doc, bank, template_json, True)
                if len(template_data)>0 and len(template_data_all_text)>0:
                    template_data['from_data'] = [template_data.pop('from_date')]
                    template_data['to_data'] = [template_data.pop('to_date')]
                    template_data['from_data_all_text'] = [template_data_all_text.pop("from_date")]
                    template_data['to_data_all_text'] = [template_data_all_text.pop("to_date")]
            elif template_type == 'account_category_bbox':
                mapping = event.get('mapping', {})
                if isinstance(mapping,str):
                    mapping=json.loads(mapping)
                template_data, template_data_regex_data, _ = get_account_category(doc, bank, template_json, mapping, get_only_all_text=False)
                template_data_all_text, template_data_regex_data2, _ = get_account_category(doc, bank, template_json, mapping, get_only_all_text=True)
                template_data = {'data':[template_data],'all_text':[template_data_all_text]}
            elif template_type == 'limit_bbox':
                data, data_response, _ = get_credit_limit(doc, template_json, bank,get_only_all_text=False)
                all_text, all_text_resp, _ = get_credit_limit(doc, template_json, bank,get_only_all_text=True)
                template_data={'data':[data],'all_text':[all_text]}
            elif template_type == 'od_limit_bbox':
                data, data_response, _ = get_od_limit(doc, template_json, get_only_all_text= False)
                all_text, all_text_resp, _ = get_od_limit(doc, template_json,get_only_all_text= True)
                template_data={'data':[data],'all_text':[all_text]}
            elif template_type == 'is_od_account_bbox':
                od_keywords = event.get('od_keywords', [])
                if isinstance(od_keywords,str):
                    od_keywords=json.loads(od_keywords)
                template_data, _ = is_od_account_check(doc, template_json,get_only_all_text= False,od_keywords=od_keywords)
                template_data_all_text, _ = is_od_account_check(doc, template_json,get_only_all_text= True,od_keywords=od_keywords)
                template_data={'data':[template_data],'all_text':[template_data_all_text]}
            elif template_type in ['email_bbox', 'phone_number_bbox', 'pan_number_bbox']:
                extracted_text, _, _ = get_generic_text_from_bank_pdf(doc, template_json, False, template_type)
                extracted_text_all_text, _, _ = get_generic_text_from_bank_pdf(doc, template_json, True, template_type)
                template_data = {'data':[extracted_text], 'all_text':[extracted_text_all_text]}
            elif template_type in ['address_bbox', 'cc_name_bbox', 'total_dues', 'min_amt_due', 'purchase/debits', 'credit_limit', 'avl_credit_limit', 'opening_balance', 'avl_cash_limit', 'payment/credits', 'card_type_bbox', 'rewards_opening_balance_bbox', 'rewards_closing_balance_bbox','rewards_points_expired_bbox','rewards_points_claimed_bbox','rewards_points_credited_bbox','payment_due_date', 'statement_date','card_number_bbox']:
                template_data = get_data_for_template_handler_util(template_type, doc, template_json, bank, file_path)
                if len(doc[0].get_text()) == 0 or get_ocr_condition_for_credit_card_statement(doc, page_number=0):
                    ocr_template_data = {'data':[None], 'all_text':[None]}
                    output_file_path = '/tmp/'
                    total_pages = doc.page_count
                    pages_to_create = [i for i in range(min(total_pages, 2))]
                    if template_type in ['card_number_bbox']:
                        if total_pages>=3:
                            pages_to_create.append(2)
                    if template_type in ['payment_due_date', 'statement_date','total_dues', 'min_amt_due', 'purchase/debits', 'credit_limit', 'avl_credit_limit', 'opening_balance', 'avl_cash_limit', 'payment/credits', 'card_type_bbox', 'rewards_opening_balance_bbox', 'rewards_closing_balance_bbox','rewards_points_expired_bbox','rewards_points_claimed_bbox','rewards_points_credited_bbox']:
                        pages_to_create += [*range(min(total_pages, 3))]
                        pages_to_create += [*range(total_pages-3,total_pages)]
                    pages_to_create = [page_num for page_num in pages_to_create if page_num>=0]
                    pages_to_create = set(pages_to_create)
                    for page_number in pages_to_create:
                        path_to_images = pdf2image.convert_from_path(
                            file_path, dpi=250, userpw=password, output_folder=output_file_path, 
                            paths_only=True, fmt="jpeg", first_page=page_number+1, last_page=page_number+1,
                            grayscale=True, transparent=True, ownerpw=password
                        )

                        if len(path_to_images)>0:
                            ocr_file_path = path_to_images[0].replace('.jpg', '.pdf')
                            ocrmypdf.ocr(
                                path_to_images[0],
                                ocr_file_path,
                                deskew=True,
                                force_ocr=True,
                                progress_bar=False
                            )
                            page_doc = read_pdf(ocr_file_path,password)
                            ocr_template_data = get_data_for_template_handler_util(template_type, page_doc, template_json, bank, file_path)
                            if os.path.exists(ocr_file_path):
                                os.remove(ocr_file_path)
                            if os.path.exists(path_to_images[0]):
                                os.remove(path_to_images[0])
                            if ocr_template_data.get('data',[None])[0] is not None:
                                return [ocr_template_data]
                
                return [template_data]
            elif template_type == 'micr_bbox':
                template_data, _ = get_micr(doc, bank, template_json, path=file_path, get_only_all_text=False)
                template_data_all_text, _ = get_micr(doc, bank, template_json, path=file_path, get_only_all_text=True)
                template_data={'data':[template_data], 'all_text':[template_data_all_text]}
            elif template_type == 'ifsc_bbox':
                template_data,_ = get_ifsc(doc,bank,template_json, path=file_path, get_only_all_text=False)
                template_data_all_text,_ = get_ifsc(doc,bank,template_json, path=file_path, get_only_all_text=True)
                template_data={'data':[template_data], 'all_text':[template_data_all_text]}
            elif template_type == 'opening_bal_bbox':
                template_data, _ = get_opening_closing_bal(doc, template_json, path=file_path, get_only_all_text=False, bank=bank)
                template_data_all_text, _ = get_opening_closing_bal(doc, template_json, path=file_path, get_only_all_text=True, bank=bank)
                template_data={'data':[template_data], 'all_text':[template_data_all_text]}
            elif template_type == 'closing_bal_bbox':
                template_data, _ = get_opening_closing_bal(doc, template_json, path=file_path, get_only_all_text=False, bank=bank)
                template_data_all_text, _ = get_opening_closing_bal(doc, template_json, path=file_path, get_only_all_text=True, bank=bank)
                template_data={'data':[template_data], 'all_text':[template_data_all_text]}
            elif template_type == 'opening_date_bbox':
                template_data, _ = get_opening_date(doc, bank, template_json, path=file_path, get_only_all_text=False)
                template_data_all_text, _ = get_opening_date(doc, bank, template_json, path=file_path, get_only_all_text=True)
                template_data={'data':[template_data], 'all_text':[template_data_all_text]}
            elif template_type == 'joint_account_holders_regex':
                template_data, _ = get_joint_account_holders_name(doc, template_json, bank, get_only_all_text=False, extract_from_page_number=page_num)
                template_data_all_text, _ = get_joint_account_holders_name(doc, template_json, bank, get_only_all_text=True, extract_from_page_number=page_num)
                template_data={'data':[template_data], 'all_text':[template_data_all_text]}
            elif template_type in ['last_page_regex', 'account_delimiter_regex']:
                last_page_coordinate, captured_data, text_strings, combined_text_strings = get_last_page_regex_simulation(doc, page_num, template_json)
                template_data={'data':[last_page_coordinate, captured_data], 'all_text':[text_strings, combined_text_strings]}
            else:
                template_data = None
        
            remove_local_file(file_path)
            return [template_data]
        except Exception as e:
            remove_local_file(file_path)
            return [None]

    #extract data for identity
    if not transaction_flag:
        bbox = template["bbox"]
        regex = template["regex"]

        if bbox == '' or bbox is None:
            remove_local_file(file_path)
            return {"message": "bounding boxes cannot be none"}

        if regex == '' or regex is None:
            remove_local_file(file_path)
            return {"message": " regex cannot be empty or null"}
        
        if page_num == None:
            print("inside no page number specified flow...")
            template_data_list = []
            
            for i in range(3):
                template_data = get_template_data_for_bbox(bbox, file_path, password, i, regex)
                print("template_data: ", template_data)
                if template_data == "":
                    template_data_list.append(None)
                else:
                    template_data_list.append(template_data)
            
            remove_local_file(file_path)
            if all([elem == None for elem in template_data_list]):
                return []
            return template_data_list
        
        # calling fsmlib function toextract identity
        template_data = get_template_data_for_bbox(bbox, file_path, password, page_num, regex)
        
        if template_data == "":
            template_data = None
        
        remove_local_file(file_path)
        return [template_data]
    else:
        if is_credit_card:
            template_param = {
                "date_bbox":[],
                "trans_bbox":[template]
            }

            if len(doc[page_num].get_text()) == 0 or get_ocr_condition_for_credit_card_statement(doc, page_number=page_num):
                transactions = []
                output_file_path = '/tmp/'
                path_to_images = pdf2image.convert_from_path(
                    file_path, dpi=300, userpw=password, output_folder=output_file_path, 
                    paths_only=True, fmt="jpeg", first_page=page_num+1, last_page=page_num+1,
                    grayscale=True, transparent=True, ownerpw=password
                )

                if len(path_to_images)>0:
                    ocr_file_path = path_to_images[0].replace('.jpg', '.pdf')
                    ocrmypdf.ocr(
                        path_to_images[0],
                        ocr_file_path,
                        deskew=True,
                        force_ocr=True,
                        progress_bar=False
                    )
                    page_doc = read_pdf(ocr_file_path,password)
                    transactions, template_id = get_cc_transactions_using_fitz(ocr_file_path, password, bank, 0, template_param)
                    if os.path.exists(ocr_file_path):
                        os.remove(ocr_file_path)
                    if os.path.exists(path_to_images[0]):
                        os.remove(path_to_images[0])
            else:
                transactions, template_id = get_cc_transactions_using_fitz(file_path, password, bank, page_num, template_param)
            transactions=json.dumps(transactions, default=str)
            remove_local_file(file_path)
            return transactions
        else:
            print("Extracting transactions from the template")
            check_and_get_everything(bank)
            country = event.get('country', 'IN')
            identity = event.get('identity', {})
            if template is None or len(template)<2:
                remove_local_file(file_path)
                return {"message": "template is none or does not contain required values"}
            
            if isinstance(template, dict):
                template['uuid'] = "test_quality_template"
                template = [template]
            
            try:
                transaction_input_payload = {
                    'path': file_path,
                    'bank': bank,
                    'password': password,
                    'page_number': page_num,
                    'key': key,
                    'number_of_pages': doc.page_count,
                    'bucket': bucket,
                    'trans_bbox': template,
                    'country': country,
                    'identity': identity
                }
                transaction_output_dict = get_transaction(transaction_input_payload, local_logging_context, LAMBDA_LOGGER)
                
                transactions = transaction_output_dict.get('transactions', [])
                
                for i in range(len(transactions)):
                    transactions[i]['date'] = str(transactions[i]['date'])
                print(transactions)
            except Exception as e:
                remove_local_file(file_path)
                print(traceback.format_exc())
                return e
            
            remove_local_file(file_path)
            return transactions
        
def cache_access(event, context):

    # this lambda can be used by anyone who does not want to interact with any access lambda.
    # this is primarily built for bank-connect enrichment apis which is going to be used by DS team.
    # this lambda will return cache urls of fsm-arbiter s3 bucket which can be used by anyone who has access.

    entity_id = event.get('entity_id')
    account_id = event.get('account_id')
    items_needed = event.get('items_needed', [])
    to_reject_account = event.get('to_reject_account', False)
    account_ids_list = get_accounts_for_entity(entity_id, to_reject_account)
    is_sme = event.get('is_sme', False)

    if not account_id:
        # this means that I'll have to do the operation for all the account_ids
        account_ids = []
        for account in account_ids_list:
            if account.get('item_data').get('account_number')==None:
                print(f"Account id {account} was not extracted")
                continue

            account_item_data = account.get('item_data', dict())
            obj = {
                "account_id": account_item_data.get('account_id'),
                "statements": account_item_data.get('statements'),
                "salary_confidence": account_item_data.get("salary_confidence"),
            }
            account_ids.append(obj)
    else:
        account_ids = [
            {
                "account_id": account_id,
                "statements": get_statement_ids_for_account_id(entity_id, account_id)
            }
        ]
        single_account = get_account_for_entity(entity_id, account_id, to_reject_account)
        if not isinstance(single_account, dict) or single_account.get('account_id', None) != account_id:
            account_ids = []
        else:
            account_ids[0]["salary_confidence"] = single_account.get("item_data", dict()).get("salary_confidence")

    response_dict = {}

    # since entity_transactions are global, no need to calculate them again and again
    if 'entity_transactions' in items_needed:
        entity_transactions = get_transactions_for_entity(entity_id, is_sme, to_reject_account)
        entity_transactions_s3_key = f"entity_{entity_id}/entity_{entity_id}_transactions.json"
        payload = {
            "entity_id": entity_id,
            "transactions": entity_transactions
        }
        put_object_in_s3(entity_transactions_s3_key, payload)
        response_dict['entity_transactions'] = entity_transactions_s3_key
    
    if 'entity_salary_transactions' in items_needed:
        entity_salary_transactions = []
        response_dict['entity_salary_transactions'] = None
    

    if 'entity_fraud' in items_needed:
        entity_fraud = access_handler({
                            "access_type": "ENTITY_FRAUD",
                            "entity_id": entity_id,
                            "to_reject_account": to_reject_account
                        }, None)
        entity_fraud_s3_key = f"entity_{entity_id}/entity_{entity_id}_entity_fraud.json"
        payload = {
            "entity_id": entity_id,
            "entity_fraud": entity_fraud
        }
        put_object_in_s3(entity_fraud_s3_key, payload)
        response_dict['entity_fraud'] = entity_fraud_s3_key

    for account in account_ids:
        account_id = account.get('account_id')
        statements = account.get('statements')
        account_dict = [_ for _ in account_ids_list if _.get("account_id") == account_id][0].get('item_data')
        print(f"The account dict for account_id {account_id} is {account_dict}.")

        if account_id is None:
            continue
        
        response_dict[account_id] = {}

        if 'identity' in items_needed and statements:
            # but before requesting identity for statements[0], we need to determine whether or not the statement was extracted
            # simply passing statements[0] can fail because the statement may not have been extracted for multiple reasons
            # thus ideally instead of randomly invoking only the first statement check in a loop for all the statements of that account
            statement_for_which_identity_extracted = None
            for statement in statements:
                identity = get_complete_identity_for_statement(statement)
                if isinstance(identity, dict) and len(identity)==0:
                    continue
                if isinstance(identity, dict) and identity.get("identity", {}).get("account_number") not in [None, ""]:
                    statement_for_which_identity_extracted = statement

                    account_dict['account_category'], _ = get_final_account_category(account_dict.get('account_category', None), account_dict.pop('is_od_account', None), account_dict.pop('input_account_category', None), account_dict.pop('input_is_od_account', None))
                    
                    if account_dict.get('credit_limit', None) == None:
                        account_dict['credit_limit'] = account_dict.get('od_limit',None)
                    if account_dict.get('od_limit', None) == None:
                        account_dict['od_limit'] = account_dict.get('credit_limit',None)

                    identity['identity']['account_category'] = account_dict['account_category']
                    identity['identity']['od_limit'] = account_dict['od_limit']
                    identity['identity']['credit_limit'] = account_dict['credit_limit']
                    
                    # now re-writing this retrieved file in s3
                    payload = {
                        "entity_id": entity_id,
                        "account_id": account_id,
                        "statement_id": statement_for_which_identity_extracted,
                        "identity": identity
                    }
                    s3.put_object(Bucket=BANK_CONNECT_CACHEBOX_BUCKET, Key="entity_{}/account_{}/statement_{}_identity.json".format(entity_id, account_id, statement_for_which_identity_extracted), Body=json.dumps(payload, default=str).encode('utf-8'))
                    break

            if statement_for_which_identity_extracted!=None:
                response_dict[account_id]['identity'] = "entity_{}/account_{}/statement_{}_identity.json".format(entity_id, account_id, statement_for_which_identity_extracted)
            else:
                # since identity is not extracted for this account at all, there is no point to give any data.
                # pop out the account id from the response_dict and continue ahead
                response_dict.pop(account_id)
                continue
        
        if 'account_transactions' in items_needed:
            account_transactions, hash_dict = get_transactions_for_account(entity_id, account_id)
            account_s3_key = f"entity_{entity_id}/account_{account_id}/account_{account_id}_transactions.json"
            put_object_in_s3(
                account_s3_key, {
                    "entity_id": entity_id,
                    "transactions": account_transactions
            })
            print(f"put account id {account_id} transactions in s3")
            response_dict[account_id]['account_transactions'] = account_s3_key

        if 'salary_transactions' in items_needed:
            salary_transactions = get_salary_transactions_from_ddb(account_id)
            salary_s3_key = f"entity_{entity_id}/account_{account_id}/account_{account_id}_salary_transactions.json"
            put_object_in_s3(salary_s3_key, {
                    "entity_id": entity_id,
                    "account_id": account_id,
                    "salary_transactions": salary_transactions
                })
            print(f"put account id {account_id} salary transactions in s3")
            response_dict[account_id]['salary_transactions'] = salary_s3_key
            
            ## Put salary_confidence_percentage in response.
            response_dict[account_id]['salary_confidence_percentage'] = account.get("salary_confidence")
        
        if 'entity_salary_transactions' in items_needed:
            salary_transactions = get_salary_transactions_from_ddb(account_id)
            entity_salary_transactions += salary_transactions

        if 'recurring_transactions' in items_needed:
            recurring_transactions = get_recurring_raw_from_ddb(account_id)
            recurring_transactions_s3_key = f"entity_{entity_id}/account_{account_id}/account_{account_id}_advanced_features.json"
            payload = {
                "entity_id": entity_id,
                "account_id": account_id,
                "advanced_features": recurring_transactions
            }
            put_object_in_s3(recurring_transactions_s3_key, payload)
            print(f"put account id {account_id} recurring transactions in s3")
            response_dict[account_id]['recurring_transactions'] = recurring_transactions_s3_key

        if 'frauds_list' in items_needed:
            frauds_list, response_dict['is_extracted_by_perfios'] = get_extracted_frauds_list(entity_id, account_id)
            fraud_list_s3_key = f"entity_{entity_id}/account_{account_id}/account_{account_id}_fraud_list.json"
            payload = {
                "entity_id": entity_id,
                "account_id": account_id,
                "fraud_list": frauds_list
            }
            put_object_in_s3(fraud_list_s3_key, payload)
            print(f"put account id {account_id} frauds list in s3")
            response_dict[account_id]['frauds_list'] = fraud_list_s3_key

        if 'page_level_transactions' in items_needed:
            response_dict[account_id]['page_level_transactions'] = {}
            for statement_id in statements:
                page_level_transactions, page_transaction_values_dict = get_page_level_transactions_for_statement(entity_id, account_id, statement_id)
                response_dict[account_id]['page_level_transactions'][statement_id] = page_level_transactions
                print(f"Page level transactions for statement id : {statement_id} is: ", page_level_transactions)

    if 'entity_salary_transactions' in items_needed:
        # need to upload this accumulated set in fsm-arbiter
        payload = {
            "entity_id": entity_id,
            "entity_salary_transactions": entity_salary_transactions
        }
        entity_salary_transactions_s3_key = f"entity_{entity_id}/entity_{entity_id}_salary_transactions.json"
        put_object_in_s3(entity_salary_transactions_s3_key, payload)
        print(f"put entity id {entity_id} frauds list in s3")
        response_dict['entity_salary_transactions'] = entity_salary_transactions_s3_key
    
    if 'missing_data' in items_needed:
        missing_data_dict = {}
        for account in account_ids_list:
            if account.get('item_data').get('account_number') is not None:
                account_id = account.get('item_data').get('account_id')
                missing_data_dict[account_id] = account.get('item_data').get('missing_data', [])
        response_dict['missing_data'] = missing_data_dict

    if 'account_date_range_map' in items_needed:
        account_date_range_map = {}
        for account in account_ids_list:
            account_item_data = account.get("item_data", {})
            account_date_range_map[account.get("account_id")] = account_item_data.get('account_date_range', {})
        response_dict['account_date_range_map'] = account_date_range_map

    print("Response for entity_id: {} is {}".format(entity_id, response_dict))
    return response_dict

def put_object_in_s3(key, payload):
    s3.put_object(
        Bucket = BANK_CONNECT_CACHEBOX_BUCKET,
        Key = key,
        Body = json.dumps(payload).encode('utf-8')
    )

def categorisation_multithreading_helper(params):
    response = lambda_client.invoke(
        FunctionName = CATEGORISATION_PAGE_FUNCTION, 
        Payload = json.dumps(params), 
        InvocationType = 'RequestResponse'
    )
    payload = json.loads(response['Payload'].read().decode('utf-8'))
    return payload

def categorisation_handler_page(event, context):
    transactions = event.get("transactions", [])
    bank_name = event.get("bank_name", None)
    country = event.get("country", "IN")
    account_category = event.get("account_category", "")

    check_and_get_everything(bank_name, country)
    df = pd.DataFrame(transactions)
    df = get_transaction_channel(df, bank_name, country, account_category)
    df = get_transaction_description(df, country)
    return df.to_dict("records")

def categorisation_handler(event, context):
    transactions = event.get("transactions", [])
    transactions_url = event.get("transactions_url", None)
    bank_name = event.get("bank_name", None)
    country = event.get("country", "IN")
    account_category = event.get("account_category", "")

    if (not transactions and not transactions_url) or (not bank_name):
        return []
    
    check_and_get_everything(bank_name, country)

    random_uuid = str(uuid4())
    if transactions_url and not transactions:
        try:
            file_name = f"transactions_{random_uuid}.json"
            result = requests.get(transactions_url)
            with open(file_name, mode="wb") as file:
                file.write(result.content)
            transactions = json.load(open(file_name))
            os.remove(file_name)
        except Exception as e:
            print(e)
            return {"error": e}
    
    txn_list_of_lists = get_transactions_list_of_lists_finvu_aa(transactions)
    process_list = []
    for l in txn_list_of_lists:
        payload = {
            "transactions": l,
            "bank_name": bank_name,
            "country": "IN",
            "account_category": account_category
        }
        process_list.append(payload)
    with ThreadPoolExecutor(max_workers=10) as executor:
        transacton_iterable = executor.map(categorisation_multithreading_helper, process_list)
    final_transactions = []
    for transactions in transacton_iterable:
        final_transactions += transactions
    
    salary_data = get_salary_transactions(final_transactions, [], True)
    
    salary_hashes = []
    for tx in salary_data.get("salary_transactions", []):
        salary_hashes.append(tx.get("hash"))

    df = pd.DataFrame(final_transactions)
    # print(df.head(10))
    
    hash_dict_map = {}
    for index, items in df.iterrows():
        hash = items.get("hash")
        if hash not in hash_dict_map:
            hash_dict_map[hash] = [[random_uuid, 0]]
        else:
            hash_dict_map[hash].append([random_uuid, 0])
    
    lender_transaction_hashes = get_recurring_lender_debit_transactions(df)
    print(f"recurring lender transaction detected for account_id : {random_uuid}, for following hashes : {lender_transaction_hashes}\n\n")

    auto_debit_payment_hashes, auto_debit_payment_bounce_hashes = update_bounce_transactions_for_account_transactions(df)
    print(f"auto debit payment bounce detected for account_id : {random_uuid}, for following hashes : {auto_debit_payment_bounce_hashes}\n\n")

    refund_hash_set = mark_refund_on_basis_of_same_balance(df, auto_debit_payment_bounce_hashes)
    print(f"refund transactions detected for account_id : {random_uuid}, for following hashes : {refund_hash_set}\n\n")

    records = df.to_dict("records")

    keys_to_delete = ["date_obj"]

    for txns in records:
        if txns.get("hash") in lender_transaction_hashes:
            txns["merchant_category"] = "loans"
            txns["description"] = "lender_transaction"
            txns["transaction_channel_regex"] = "lender recurring"
        if txns.get("hash") in auto_debit_payment_hashes:
            txns["transaction_channel"] = "auto_debit_payment"
            txns["transaction_channel_regex"] = "auto debit recurring"
        if txns.get("hash") in auto_debit_payment_bounce_hashes:
            txns["transaction_channel"] = "auto_debit_payment_bounce"
            txns["transaction_channel_regex"] = "auto debit payment bounce recurring"
        if txns.get("hash") in refund_hash_set:
            txns["transaction_channel"] = "refund"
            txns["transaction_channel_regex"] = "refund recurring"
        if txns.get("hash") in salary_hashes:
            txns["transaction_channel"] = "salary"
            txns["transaction_channel_regex"] = ""
        for key in keys_to_delete:
            if key in txns:
                del txns[key]
    
    # forward mapper
    t1 = time.time()
    categorizer = SingleCategory(bank_name=bank_name, transactions=records, categorize_server_ip=CATEGORIZE_RS_PRIVATE_IP)
    records = categorizer.categorize_from_forward_mapper()
    print(
        f"Forward Mapper took {time.time()-t1} seconds for {len(records)} transactions"
    )

    if transactions_url:
        file_path = f"transactions_{random_uuid}.json"
        with open(file_path, "wb") as w:
            w.write(json.dump(records))
        s3_resource.Bucket(BANK_CONNECT_CACHEBOX_BUCKET).upload_file(file_path, file_path)
    
    return records