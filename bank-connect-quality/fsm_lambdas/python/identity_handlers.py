import json
import os
from typing import Any, Union
import pdf2image, ocrmypdf
from uuid import uuid4, UUID
import requests
import time
from decimal import Decimal
import re
from boto3.dynamodb.conditions import Key, Attr
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from sentry_sdk import set_context, set_tag, capture_exception

from library.fitz_functions import extract_essential_identity, get_metadata_fraud, read_pdf
from library.extract_txns_csv import extract_identity_csv
from library.fitz_functions import get_stream_fraud_data_page, add_stream_fraud_data_all_pages
from library.date_utils import convert_date_range_to_datetime
from library.utils import check_date
from python.aggregates import AccountDict, transform_identity, update_progress, get_account_for_entity, get_final_account_category, update_progress_fraud_status
from python.aggregates import get_country_for_statement, send_event_to_quality
from python.aws_utils import collect_results, get_json_from_s3_file
from python.configs import LAMBDA_LOGGER, StatementType, METADATA_STREAM_NAME, bank_connect_account_table, lambda_client, STREAM_FRAUD_DATA_PAGE_FUNCTION, s3, bank_connect_identity_table, bank_connect_tmp_identity_table, DJANGO_BASE_URL, API_KEY
from python.utils import create_identity_object_for_quality, prepare_statement_metadata_warehouse, json_serial, get_date_of_format, get_account, add_statement_to_account, configure_od_limit_after_identity_extraction, \
    update_account_table_multiple_keys, create_new_account, create_or_update_account_details_for_pdf
from python.context.logging import LoggingContext
import traceback
import pypdf
from python.clickhouse.firehose import send_data_to_firehose
from python.api_utils import call_api_with_session

def add_input_account_category_to_account(entity_id, account_id,input_account_category, input_is_od_account):
    bank_connect_account_table.update_item(
        Key={
            'entity_id': entity_id,
            'account_id': account_id,
        },
        UpdateExpression="SET item_data.input_account_category = :i, item_data.input_is_od_account = :o, updated_at = :u",
        ExpressionAttributeValues={
            ':i': input_account_category,
            ':o': input_is_od_account,
            ':u': time.time_ns()
        }
    )


def update_credit_od_limits(entity_id, account_id,field_name, field_value):
    bank_connect_account_table.update_item(
        Key={
            'entity_id': entity_id,
            'account_id': account_id
        },
        UpdateExpression="SET item_data.#data = :i, updated_at = :u",
        ExpressionAttributeNames={
            '#data': field_name,
        },
        ExpressionAttributeValues={
            ':i': field_value,
            ':u': time.time_ns()
        }
    )


def get_count_of_accounts(entity_id):
    qp = {
        'KeyConditionExpression': Key('entity_id').eq(entity_id),
        'ConsistentRead': True,
        'ProjectionExpression': 'account_id'}
    accounts = collect_results(bank_connect_account_table.query, qp)
    return len(accounts)


def get_account_aa(entity_id, linked_account_ref_number) -> Union[AccountDict, None]:
    """
    This method returns existing account id from ddb.
    """
    local_logging_context: LoggingContext = LoggingContext(
        source="get_account_aa",
        linked_account_ref_number=linked_account_ref_number,
        entity_id=entity_id
    )
    if linked_account_ref_number is None:
        return None

    qp = {
        'KeyConditionExpression': Key('entity_id').eq(entity_id),
        'ConsistentRead': True,
        'ProjectionExpression': 'entity_id,account_id,item_data'}
    accounts = collect_results(bank_connect_account_table.query, qp)

    for account in accounts:
        curr_linked_account_ref_number = account.get(
            'item_data', dict()).get('linked_account_ref_number', None)

        if curr_linked_account_ref_number is None:
            continue

        try:
            if str(UUID(curr_linked_account_ref_number)) == str(UUID(linked_account_ref_number)):
                local_logging_context.upsert(curr_linked_account_ref_number=curr_linked_account_ref_number)
                LAMBDA_LOGGER.debug(
                    f"get_account_aa: account matched with UUID check",
                    extra=local_logging_context.store
                )
                return account
            # If the valid UUID's doesn't match move to the next account for comparison
            continue

        except (ValueError, AttributeError, TypeError) as e:
            # Handling all expected exceptions
            pass

        ref_number_check = re.compile("[a-zA-Z]+", re.IGNORECASE)
        # Check if ref numbers contains alphabets, then only check last 4 digits
        if ref_number_check.findall(linked_account_ref_number) or ref_number_check.findall(curr_linked_account_ref_number):
            if linked_account_ref_number[-4:] == curr_linked_account_ref_number[-4:]:
                LAMBDA_LOGGER.debug(
                    f"get_account_aa: account matched with last 4 char check with {linked_account_ref_number[-4:]}",
                    extra=local_logging_context.store
                )
                return account
        else:
            if linked_account_ref_number == curr_linked_account_ref_number:
                return account 

    return None


# TODO: Make the function signature more readable using a pydantic model
def create_new_account_aa(
    entity_id,
    bank,
    masked_account_number,
    linked_account_ref_number,
    statement_id,
    ifsc=None,
    micr=None,
    account_category=None,
    account_opening_date=None,
    is_od_account=False,
    od_limit=None,
    credit_limit=None,
    pan_number="",
    phone_number="",
    email="",
    dob="",
    account_status="",
    holder_type="",
    joint_account_holders=[]
):
    """
    This method returns a new account id for an account number.
    This method is specific to AA flow, because we have masked account number
    and linked account ref number available.
    NOTE: Check for existing account number cannot be done on the basis of masked account number
    so for AA flow the check happens for account ref number.
    """
    account_id = str(uuid4())
    time_stamp_in_mlilliseconds = time.time_ns()
    dynamo_object = {
        'entity_id': entity_id,
        'account_id': account_id,
        'item_data': {
            'bank': bank,
            'linked_account_ref_number': linked_account_ref_number,
            'account_number': masked_account_number,
            'statements': [statement_id],
            'account_id': account_id,
            'ifsc': ifsc,
            'micr': micr,
            'account_opening_date': account_opening_date,
            'account_category': account_category,
            'is_od_account': is_od_account,
            'credit_limit': credit_limit,
            'od_limit': od_limit,
            'pan_number': pan_number,
            'phone_number': phone_number,
            'email': email,
            "dob": dob,
            "account_status": account_status,
            "holder_type": holder_type,
            "joint_account_holders": joint_account_holders
        },
        'created_at': time_stamp_in_mlilliseconds,
        'updated_at': time_stamp_in_mlilliseconds
    }

    bank_connect_account_table.put_item(Item=dynamo_object)
    return account_id


def update_failed_pdf_status(statement_id, message):
    print('updating failed status for st id {}'.format(statement_id))
    update_progress(statement_id, 'processing_status', 'failed', message)
    update_progress(statement_id, 'identity_status', 'failed', message)
    update_progress(statement_id, 'transactions_status', 'failed', message)
    update_progress(statement_id, 'metadata_fraud_status', 'failed', message)
    update_progress(statement_id, 'page_identity_fraud_status', 'failed', message)


def json_serial(obj):
    from decimal import Decimal
    from datetime import date, datetime

    if isinstance(obj, (datetime, date)):
        return obj.isoformat()

    if isinstance(obj, (Decimal)):
        return str(round(obj, 3))

    raise TypeError ("Type %s not serializable" % type(obj))


def fraud_multithreading_helper(params):
    print(f"Trying to get stream fraud data for page_number {params['page']}.")
    response = lambda_client.invoke(
        FunctionName=STREAM_FRAUD_DATA_PAGE_FUNCTION, 
        Payload=json.dumps(params), 
        InvocationType='RequestResponse'
    )
    payload = json.loads(response['Payload'].read().decode('utf-8'))
    result = json.loads(payload['body'])['result']
    return result


def stream_fraud_data_page(event, context):
    bucket = event['bucket']
    key = event['key']
    page_no = event['page']

    response = s3.get_object(Bucket=bucket, Key=key)
    pdf_metadata = response.get('Metadata')

    bank = pdf_metadata.get('bank_name')
    password = pdf_metadata.get('pdf_password')
    statement_id = pdf_metadata.get('statement_id')

    path = "/tmp/{}.pdf".format(statement_id)
    with open(path, 'wb') as file_obj:
        file_obj.write(response['Body'].read())
    
    exception_in_fraud_logic = False
    final_dict = {}
    try:
        final_dict = get_stream_fraud_data_page(path, password, bank, page_no)
    except Exception as e:
        print("exception occured as {} on page_no {} for statment_id {}".format(e, page_no, statement_id))
        exception_in_fraud_logic = True
    
    # delete the file after the work is over
    if os.path.exists(path):
        os.remove(path)
    
    final_dict['exception_in_fraud_logic'] = exception_in_fraud_logic
    response = {
        'statusCode': 200,
        'body': json.dumps({'result': final_dict}, default=str)
    }
    return response


def get_new_content_stream_data_using_thread(total_pages, bucket, key):
    process_list = [None]*total_pages
    fraud_iterable = []
    for page in range(total_pages):
        params = {
            'bucket': bucket, 
            'key': key, 
            'page': page
        }
        process_list[page] = params
        
    with ThreadPoolExecutor(max_workers=10) as executor:
        fraud_iterable = executor.map(fraud_multithreading_helper, process_list)

    stream_pages_dict_data = {}
    page = 0
    for page_fraud_data in fraud_iterable:
        stream_pages_dict_data[page] = page_fraud_data
        page += 1
    
    stream_fraud_data = add_stream_fraud_data_all_pages(stream_pages_dict_data, total_pages)
    return stream_fraud_data


def metadata_frauds(event, context):
    start_time=time.time()
    bucket = event['bucket']
    key = event['key']
    attempt_type = event.get('attempt_type',None)
    is_retrigger = event.get('is_retrigger',False)
    stream_font_list = event.get('stream_font_list',[])
    encryption_algo_list = event.get('encryption_algo_list',[])
    good_font_list = event.get('good_font_list',[])
    strict_metadata_fraud_list = event.get('strict_metadata_fraud_list', [])
    is_metadata_update_testing = event.get('is_metadata_update_testing', False)
    allowed_frauds = event.get('allowed_frauds', [])

    response = s3.get_object(Bucket=bucket, Key=key)

    pdf_metadata = response.get('Metadata')

    bank = pdf_metadata.get('bank_name')
    password = pdf_metadata.get('pdf_password')
    entity_id = pdf_metadata.get('entity_id')
    statement_id = pdf_metadata.get('statement_id')
    print("calculating metadata frauds for statement_id: {} and attempt_type = {}".format(statement_id,attempt_type))

    path = "/tmp/{}.pdf".format(statement_id)
    if key.endswith(".csv"):
        #it's a csv so no fraud check
        return None

    with open(path, 'wb') as file_obj:
        file_obj.write(response['Body'].read())

    doc = read_pdf(path, password)
    if isinstance(doc, int):
        print("Could not read pdf for statement_id = {}".format(statement_id))
        return
    country = get_country_for_statement(statement_id)

    num_pages = doc.page_count
    new_content_stream_data = get_new_content_stream_data_using_thread(num_pages, bucket, key)

    is_fraud, fraud_type, doc_metadata_dict, all_fraud_list = get_metadata_fraud(new_content_stream_data, doc, bank, path, password, country, stream_font_list, encryption_algo_list, good_font_list, strict_metadata_fraud_list)
    if os.path.exists(path):
        os.remove(path)

    if 'metadata' not in allowed_frauds:
        is_fraud=False
        fraud_type=None

    if is_metadata_update_testing:
        return {
            'all_fraud_list': all_fraud_list
        }

    table_name = bank_connect_identity_table
    #only for iifl_fraud_flow, need to be removed in future
    is_iifl_fraud_flow = event.get("is_iifl_fraud_flow", False)
    if is_iifl_fraud_flow == True:
        print("Inside iifl fraud flow for statement_id= {}".format(statement_id))
        table_name = bank_connect_tmp_identity_table
    #--------------------------------------------------

    time_taken= round(float(time.time()-start_time),4)
    doc_metadata_dict['time_taken']=Decimal(str(time_taken))
    if (is_fraud == True or is_retrigger==True) and attempt_type == 'pdf':
        table_name.update_item(
            Key={'statement_id': statement_id},
            UpdateExpression = """  set item_data.is_fraud = :f, 
                                    item_data.fraud_type = :t ,
                                    item_data.doc_metadata = :d,
                                    updated_at = :u""",
            ExpressionAttributeValues={
                ':f': is_fraud,
                ':t': fraud_type,
                ':d': doc_metadata_dict,
                ':u': time.time_ns()
            }
        )
    else :
        table_name.update_item(
            Key={'statement_id': statement_id},
            UpdateExpression = """set item_data.doc_metadata = :d, updated_at = :u""",
            ExpressionAttributeValues = {':d': doc_metadata_dict, ':u': time.time_ns() }
        )
        
    #marking matadata fraud calculationa as completed in bsa page count table
    update_progress(statement_id, "metadata_fraud_status", "completed")

    statement_metadata_clickhouse_event = prepare_statement_metadata_warehouse(doc_metadata_dict, statement_id, bank)
    send_data_to_firehose([statement_metadata_clickhouse_event], METADATA_STREAM_NAME)

    print('calling dashboard API for statement id: {}'.format(statement_id))
    url = '{}/bank-connect/v1/internal/{}/update_metadata_frauds/'.format(
        DJANGO_BASE_URL, statement_id)

    headers = {
        'x-api-key': API_KEY,
        'Content-Type': "application/json",
    }
    payload = {
        "is_fraud":is_fraud,
        "fraud_type":fraud_type,
        "doc_metadata":doc_metadata_dict,
        "is_retrigger":is_retrigger,
        "all_fraud_list":all_fraud_list
    }

    retries = 3
    sleep_duration = 5  # in seconds
    while retries:
        response = call_api_with_session(url,"POST", json.dumps(payload,default=json_serial), headers)
        print('retries {} and response status {} and response {} and payload {}'.format(retries, response.status_code, response.text, payload))
        if response.status_code == 200:
            break
        retries -= 1
        time.sleep(sleep_duration)
    return (retries != 0)


def ocr_identity_extraction(path, bank, password, preshared_names, template, page_count):

    dpi = 300 if bank == 'karur' else 330 
    fmt = 'jpeg' if bank == 'karur' else 'png' 
    last_page = 1 if bank == 'karur' else min(2, page_count) 
    grayscale = True if bank == 'karur' else False 
    transparent = True if bank == 'karur' else False 
    file_suffix = 'jpg' if bank == 'karur' else 'png'

    output_folder_path = "/tmp/"
    
    paths_to_images = pdf2image.convert_from_path(
        path, dpi=dpi, userpw=password, output_folder=output_folder_path, 
        paths_only=True, fmt=fmt, first_page=1, last_page=last_page,
        grayscale=grayscale, transparent=transparent, ownerpw=password
    )
    
    if len(paths_to_images)==0:
        return {}
    
    ocr_pdf_path_list = []
    
    for i in range(last_page):
        ocr_file_path = paths_to_images[i].replace(f'.{file_suffix}', '.pdf')
        ocrmypdf.ocr(
            paths_to_images[i],
            ocr_file_path,
            deskew=True,
            force_ocr=True,
            progress_bar=False
        )
        ocr_pdf_path_list.append(ocr_file_path)
    
    uuid = str(uuid4())
    final_path = f'/tmp/{uuid}.pdf'
    
    if bank == 'hsbc':
        merge_pdfs(ocr_pdf_path_list,final_path)
    
    if bank == 'karur':
        final_path = ocr_pdf_path_list[0]
    
    identity = extract_essential_identity(final_path, bank, password, preshared_names, template)
    
    ocr_pdf_path_list.append(final_path)
    for path in ocr_pdf_path_list:
        if os.path.exists(path):
            os.remove(path)
    
    return identity


def merge_pdfs(pdf_paths, output_path):
    pdf_merger = pypdf.PdfWriter()
    
    for path in pdf_paths:
        pdf_merger.append(path)
    
    pdf_merger.write(output_path)
    pdf_merger.close()


def identity_handler(event, context):

    """
    Structure and content of the context object:
    LambdaContext(
        [
            aws_request_id=86708aa3-dc45-4fb1-8647-5683b419be59,
            log_group_name=/aws/lambda/bank-connect-dev-identity_enhanced,
            log_stream_name=2024/03/11/bank-connect-dev-identity_enhanced[$LATEST]9a434ef520c44ffc933286fd92c1064d,
            function_name=bank-connect-dev-identity_enhanced,
            memory_limit_in_mb=3072,
            function_version=$LATEST,
            invoked_function_arn=arn:aws:lambda:ap-south-1: 909798297030:function:bank-connect-dev-identity_enhanced,
            client_context=ClientContext(
                [
                    custom={
                        'x-datadog-trace-id': '11651177946432769054', 
                        'x-datadog-parent-id': '6575605521443441272', 
                        'x-datadog-sampling-priority': '1', 
                        'x-datadog-tags': '_dd.p.dm=-0'
                    },
                    env=None,
                    client=None
                ]
            ),
            identity=CognitoIdentity(
                [
                    cognito_identity_id=None,
                    cognito_identity_pool_id=None
                ]
            )
        ]
    )
    Usage:
    context.x
    """

    local_logging_context: LoggingContext = LoggingContext(
        source="identity_handler"
    )

    LAMBDA_LOGGER.info(
        "Initiating the identity handler flow.",
        extra=local_logging_context.store
    )

    bucket = event['bucket']
    key = event['key']
    preshared_names = event.get("preshared_names", [])
    template = event.get("template", [])
    ask_od_limit_flag = event.get("ask_od_limit_flag", False)
    user_inputs = event.get("user_inputs", {})
    input_account_category = user_inputs.get("input_account_category", None)
    input_is_od_account = user_inputs.get("input_is_od_account", None)
    currency_mapping = event.get("currency_mapping", {})
    scanned_pdf_support_flag = event.get("scanned_pdf_support_flag", False)
    session_date_range = event.get("session_date_range", None)
    session_flow = event.get("session_flow", False)
    re_extraction = event.get("re_extraction", False)
    org_metadata = event.get('org_metadata', dict())

    session_date_range = convert_date_range_to_datetime(session_date_range, "%d/%m/%Y")

    print("trying to extract identity for statement_id: {}".format(key))
    print("preshared names: {} - {}".format(preshared_names, type(preshared_names)))

    response = s3.get_object(Bucket=bucket, Key=key)

    pdf_metadata = response.get('Metadata')

    bank = pdf_metadata.get('bank_name')
    password = pdf_metadata.get('pdf_password')
    entity_id = pdf_metadata.get('entity_id')
    statement_id = pdf_metadata.get('statement_id')
    set_tag("entity_id", entity_id)
    set_tag("statement_id", statement_id)
    set_context("identity_event_payload", event)

    local_logging_context.upsert(
        entity_id=entity_id,
        statement_id=statement_id,
        statement_type=StatementType.VANILLA.value,
        bank=bank
    )

    LAMBDA_LOGGER.debug(
        "Parameters successfully extracted from the event and S3",
        extra=local_logging_context.store
    )

    #TODO shift to detected country to be safe
    country = currency_mapping.get('default_country', None)

    is_csv = False

    path = "/tmp/{}.pdf".format(statement_id)
    if key.endswith(".csv"):
        is_csv = True
        path = "/tmp/{}.csv".format(statement_id)
    
    with open(path, 'wb') as file_obj:
        file_obj.write(response['Body'].read())
    
    identity = dict()

    t1 = time.time()
    if is_csv:

        LAMBDA_LOGGER.info(
            f"Attempting to extract Identity CSV via path {path}",
            extra=local_logging_context.store
        )

        identity = extract_identity_csv(path, bank)

        LAMBDA_LOGGER.debug(
            "Identity CSV extracted successfully",
            extra=local_logging_context.store
        )

    else:
        LAMBDA_LOGGER.info(
            f"Attempting to extract essential identity for path {path}",
            extra=local_logging_context.store
        )

        identity = extract_essential_identity(path, bank, password, preshared_names, template, country)

        LAMBDA_LOGGER.debug(
            f"Essential identity successfully extracted. Attempting to read PDF {path}",
            extra=local_logging_context.store
        )

        doc = read_pdf(path, password)
        if bank == 'karur' and not identity.get('identity', dict()).get('account_category', None):
            if not isinstance(doc, int) and len(doc[0].search_for("�")) > 5:
                
                LAMBDA_LOGGER.info(
                    "Bank detected to be Karur post reading PDF, with account category existing with the identity object. Performing OCR Identity extraction now.",
                    extra=local_logging_context.store
                )
                
                new_identity = ocr_identity_extraction(path, bank, password, preshared_names, template, doc.page_count)
                if identity and new_identity:
                    necessary_keys_to_check = ['name', 'ifsc', 'micr', 'account_category', 'is_od_account', 'od_limit', 'credit_limit', 'templates_used']
                    for key_to_check in necessary_keys_to_check:
                        identity['identity'][key_to_check] = identity.get('identity', dict()).get(key_to_check) or new_identity.get('identity', dict()).get(key_to_check)
                    identity['is_ocr_extracted'] = True
        if bank == 'hsbc' and identity.get('is_image', False) and not isinstance(doc, int):
            identity = ocr_identity_extraction(path, bank, password, preshared_names, template, doc.page_count)
            if identity.get('identity', dict()).get('account_number') not in (None, ''):
                identity['is_ocr_extracted'] = True
                identity['page_count'] = doc.page_count

    t2 = time.time()

    print("Identity extraction from library for {} took {}".format(key, (t2-t1)))

    # delete the file after the work is over
    if os.path.exists(path):
        os.remove(path)

    LAMBDA_LOGGER.debug(
        f"New identity extraction completed via OCR. File {path} removed post processing.",
        extra=local_logging_context.store
    )
    
    #only for iifl_fraud_flow, need to be removed in future
    is_iifl_fraud_flow = event.get("is_iifl_fraud_flow", False)
    if is_iifl_fraud_flow == True:

        LAMBDA_LOGGER.debug(
            "IIFL fraud flow detected. Attempting to update progress.",
            extra=local_logging_context.store
        )

        update_progress(statement_id, "metadata_fraud_status", "processing")

        LAMBDA_LOGGER.info(
            f"IIFL fraud flow detected. Statement {statement_id} metadata fraud status successfully updated to PROCESSING, responding with user name and account number.",
            extra=local_logging_context.store
        )

        user_account_number = identity.get('identity').get('account_number')
        user_name = identity.get('identity').get('name')
        return {
            "account_number":user_account_number,
            "name":user_name
        }
    #--------------------------------------------------

    if bank == 'NA':

        LAMBDA_LOGGER.info(
            "Bank detected to be NA. Returning with the identity object.",
            extra=local_logging_context.store
        )

        return identity

    # pop keywords (not to save in ddb)
    keywords = identity.get('keywords', dict())
    keywords_in_line = identity.get('keywords_in_line', True)

    if country != 'IN':
        keywords_in_line = True
        identity['keywords_in_line'] = True

    if identity.get('password_incorrect'):

        LAMBDA_LOGGER.debug(
            "Password found to be incorrect. Attempting to update statement ID status to password_incorrect and creating a new account.",
            extra=local_logging_context.store
        )

        update_failed_pdf_status(statement_id, 'password_incorrect')
        create_new_account(entity_id, bank, None, statement_id)

        LAMBDA_LOGGER.info(
            f"Password found to be incorrect. Statement {statement_id} status updated to password_incorrect, and new account successfully created. Responding with the identity object.",
            extra=local_logging_context.store
        )

        return identity

    if identity is None or identity.get('identity') is None:
        #if pdf is image and org is support for scanned pdf then extract it through nanonets
        if scanned_pdf_support_flag:
            identity['extract_using_nanonets'] = True

            LAMBDA_LOGGER.info(
                "Identity not found. Marking extract_using_nanonets as True since organization supports PDF scanning. Returning with the identity object.",
                extra=local_logging_context.store
            )

            return identity
            
        update_failed_pdf_status(statement_id, 'Scanned images are not supported')
        create_new_account(entity_id, bank, None, statement_id)
        identity['is_image'] = True

        LAMBDA_LOGGER.info(
            f"Statement {statement_id} status updated to Scanned images are not supported, and new account successfully created. Responding with the identity object.",
            extra=local_logging_context.store
        )

        return identity

    account_number = identity.get('identity').get('account_number')
    account = get_account(entity_id, account_number, bank)

    if not keywords.get('all_present', True) or not keywords_in_line:
        print("Keywords not present, aborting!")

        LAMBDA_LOGGER.debug(
            "Keywords found to be absent. Attempting to update statement ID status to Not a valid statement and creating a new account.",
            extra=local_logging_context.store
        )

        update_failed_pdf_status(statement_id,'Not a Valid Statement')
        create_new_account(entity_id, bank, None, statement_id)

        LAMBDA_LOGGER.info(
            f"Keywords found to be absent. Statement {statement_id} status updated to Not a valid statement, and new account successfully created. Responding with the identity object.",
            extra=local_logging_context.store
        )

        return identity

    LAMBDA_LOGGER.debug(
        "Attempting to deduce currency from the identity information.",
        extra=local_logging_context.store
    )

    # as of now, identification of currency format from statement is not present
    # thus, default value prescribed is None
    detected_currency_code = identity.get('identity').get('currency')
    detected_country_code = None

    mapping_dict = currency_mapping.get('mapping_dict', {})

    if detected_currency_code == None or detected_currency_code not in mapping_dict.keys():
        # this means that the currency format was not detected
        # or detected currency format not exit in eligible coutries of organization.
        # thus default to the value at `country_of_origin`
        print("we were not able to detect currency for this bank, switching to defaults")
        detected_currency_code = currency_mapping.get('default_currency')
        detected_country_code = currency_mapping.get('default_country')
    else:
        # since we predicted the currency, let's get the country code from the `mapping_dict`
        detected_country_code = mapping_dict[detected_currency_code]

    account_id = account.get('account_id', None) if account is not None else None
    if not account_id and pdf_metadata.get('is_single_account') == 'True' and get_count_of_accounts(entity_id) > 0:
        identity['single_acc_mismatch'] = True

        LAMBDA_LOGGER.info(
            "Single account found from PDF metadata but number of accounts is greater than 0. Mismatch scenario, returning the identity object.",
            extra=local_logging_context.store
        )

        return identity
    
    LAMBDA_LOGGER.debug(
        "Attempting to create account with current context and identity information.",
        extra=local_logging_context.store
    )
    
    account_id = create_or_update_account_details_for_pdf(entity_id, statement_id, bank, account, identity_with_extra_params=identity, identity_lambda_input=event)

    local_logging_context.upsert(
        account_id=account_id
    )

    LAMBDA_LOGGER.info(
        "Successfully created/ updated account with current context and identity information. Attempting to update identity with extracted parameters and storing in DDB.",
        extra=local_logging_context.store
    )

    identity['identity'].update({'account_id': account_id})
    identity['identity']['input_is_od_account'] = input_is_od_account
    identity['identity']['input_account_category'] = input_account_category
    identity['country_code'] = detected_country_code
    identity['currency_code'] = detected_currency_code
    identity['identity']['bank_name'] = bank
    identity['preshared_names'] = preshared_names

    # storing an absolute key called abs_date_range which will not be overridden by update state fan out
    # TODO : Need to check whether adding this key will impact in any new key in the response of identity api
    identity['extracted_date_range'] = identity['date_range']
    identity['date_range'] = {'from_date': None, 'to_date': None}
    identity['identity']['od_metadata'] = {
        'od_limit_by_extraction': identity['identity'].get('od_limit',None), 'is_od_account_by_extraction': identity['identity'].get('is_od_account',None)
    }

    time_stamp_in_mlilliseconds = time.time_ns()
    dynamo_object = {
        'statement_id': statement_id,
        'item_data': identity,
        'created_at': time_stamp_in_mlilliseconds,
        'updated_at': time_stamp_in_mlilliseconds
    }

    bank_connect_identity_table.put_item(Item=dynamo_object)

    #adding it to identity block for response to user
    #NOTE: always append this after data addition to identity table IMP
    identity['date_range'] = identity['extracted_date_range']

    LAMBDA_LOGGER.info(
        "Identity successfully updated with extracted parameters and stored in DDB. Updating progress and fraud status.",
        extra=local_logging_context.store
    )

    update_progress(statement_id, 'identity_status', 'completed')
    if not re_extraction:
        update_progress_fraud_status(statement_id, "processing")

    LAMBDA_LOGGER.debug(
        "Identity progress and fraud status updated successfully.",
        extra=local_logging_context.store
    )

    if account_number in [None,'']:
        print("Account Number check failed here, data saved in ddb for quality purposes")

        LAMBDA_LOGGER.debug(
            "Account number not found. Marking statement as invalid and sending event to Quality.",
            extra=local_logging_context.store
        )

        identity_object_for_quality = create_identity_object_for_quality(identity.get('identity', dict()), metadata_analysis=identity.get('metadata_analysis', dict()), statement_id=statement_id, org_metadata=org_metadata)

        update_failed_pdf_status(statement_id,'Not a Valid Statement')
        # create_new_account(entity_id, bank, None, statement_id)
        send_event_to_quality(statement_id, entity_id, identity_object_for_quality, is_credit_card = False)

        LAMBDA_LOGGER.info(
            "Account number not found. Statement successfully marked as invalid and event sent to Quality.",
            extra=local_logging_context.store
        )

        return identity

    tmp_identity_date_range = convert_date_range_to_datetime(identity['extracted_date_range'], "%Y-%m-%d")
    if session_flow and session_date_range and tmp_identity_date_range:
        if tmp_identity_date_range['from_date'] > session_date_range['to_date'] or tmp_identity_date_range['to_date'] < session_date_range['from_date']:
            update_failed_pdf_status(statement_id, 'No transactions in expected date range')
            identity['OUT_OF_DATE_RANGE']=True
            return identity

    identity['identity'].pop('od_metadata', None)
    identity['identity']['account_category'], is_od_account = get_final_account_category(identity['identity'].get('account_category', None), identity['identity'].pop('is_od_account', None),
                                    identity['identity'].pop('input_account_category', None), identity['identity'].pop('input_is_od_account', None))

    if ask_od_limit_flag and is_od_account and identity['identity'].get('od_limit',None) == None:
        account_data = get_account_for_entity(entity_id,account_id)
        od_limit_input_by_customer = account_data['item_data'].get('od_limit_input_by_customer', None)
        if not od_limit_input_by_customer:
            identity['ask_od_limit'] = True

    if identity['identity'].get('credit_limit', None) == None:
        identity['identity']['credit_limit'] = identity['identity'].get('credit_limit', None)
    if identity['identity'].get('od_limit', None) == None:
        identity['identity']['od_limit'] = identity['identity'].get('od_limit', None)
    
    LAMBDA_LOGGER.info(
        "Identity object updated and successful execution of the lambda completed.",
        extra=local_logging_context.store
    )

    local_logging_context.clear()

    return identity

def identity_handler_finvu_aa(event, context):
    print("event recieved: {}".format(event))

    local_logging_context: LoggingContext = LoggingContext(
        source="identity_handler_finvu_aa"
    )

    LAMBDA_LOGGER.info(
        "Initiating the identity Finvu AA handler flow.",
        extra=local_logging_context.store
    )

    # here we'll be getting data
    # which we recieved from AA in the event
    aa_data_file_key = event.get("aa_data_file_key", "")
    bucket_name = event.get("bucket_name", "")
    entity_id = event.get("entity_id", "")
    statement_id = event.get("statement_id", "")
    bank = event.get("bank_name", "")
    country = event.get("country", "IN")
    set_tag("entity_id", entity_id)
    set_tag("statement_id", statement_id)
    set_context("identity_event_payload", event)

    local_logging_context.upsert(
        entity_id=entity_id,
        statement_id=statement_id,
        statement_type=StatementType.ACCOUNT_AGGREGATOR.value,
        bank=bank
    )

    LAMBDA_LOGGER.debug(
        "Parameters successfully extracted from the event",
        extra=local_logging_context.store
    )

    # getting json aa data from s3 file
    aa_data = get_json_from_s3_file(bucket_name, aa_data_file_key)

    identity_with_extra_params = dict()
    account_id = None

    local_logging_context.upsert(
        account_id=account_id
    )

    try:
        # we will consider only first object of the aa data -> array
        body = aa_data.get("body", dict())

        if not body:
            LAMBDA_LOGGER.info(
                "AA body is null",
                extra=local_logging_context.store
            )
            return identity_with_extra_params

        # getting financial info objects -> Array
        fiObjects = body[0]["fiObjects"]

        # getting the first fi object
        firstFiObject = fiObjects[0]

        # getting the profile
        profile_obj = firstFiObject.get("Profile", dict())

        # getting holder object
        holders = profile_obj.get("Holders", dict())
        
        holder_obj = holders.get("Holder", dict())

        # getting the summary obj
        summary_obj = firstFiObject.get("Summary", dict())

        # getting transactions object
        transactions_obj = firstFiObject.get("Transactions", dict())

        # getting the transactions list
        transactions_list = transactions_obj.get("Transaction", [])

        # getting from and to date from transactions obj
        from_date = transactions_obj.get("startDate", None) # transactions_list[0].get("valueDate", None)
        to_date = transactions_obj.get("endDate", None) # transactions_list[-1].get("valueDate", None)

        # handle case when AA's send blank string as from_date and to_date
        from_date = get_date_of_format(from_date, "%Y-%m-%d")
        to_date = get_date_of_format(to_date, "%Y-%m-%d")

        # identifies that we knew the from and to date for transactions/statement
        is_date_extracted = False if from_date == None or to_date == None else True

        # keywords availability values
        is_amount_present = True
        is_balance_present = True
        is_date_present = True
        are_all_present = is_amount_present and is_balance_present and is_date_present

        # NOTE:
        # here for identity we are using masked account number
        # but for creating account_id we'll use the linkedAccRef
        linked_account_ref_number = firstFiObject.get("linkedAccRef", "")
        masked_account_number = firstFiObject.get("maskedAccNumber", "")

        # Verify account opening date format and try to format it to %Y-%m-%d
        account_opening_date = summary_obj.get('openingDate')
        if account_opening_date:
            date_or_error, format_used = check_date(account_opening_date)
            if isinstance(date_or_error, datetime):
                account_opening_date = date_or_error.strftime("%Y-%m-%d")
            else:
                # If the account opening date is NOT convertible to a standard format,
                # Raise this as a validation error
                set_context("identity_handler_finvu_aa", {
                    "error": "INVALID_ACCOUNT_OPENING_DATE_FORMAT",
                    "openingDate": summary_obj.get("openingDate")
                })
                local_logging_context.upsert(
                    error_code="INVALID_ACCOUNT_OPENING_DATE_FORMAT",
                    error_message=f"Invalid account opening date format sent by AA, {summary_obj.get('openingDate')}."
                )

                LAMBDA_LOGGER.warning(f"Invalid account opening date format found on AA identity",
                                      extra=local_logging_context.store)
                raise ValueError(f"""
                    Validation error encountered on account opening date format sent by AA. "
                    New format: {account_opening_date}, expected format: '%Y-%m-%d'"
                    Check CW on this entity id: {entity_id} for more details
                """)

        ### CREATING IDENTITY with extra params OBJ ###
        identity_with_extra_params["is_image"] = False
        identity_with_extra_params["password_incorrect"] = False
        identity_with_extra_params["identity"] = {}
        identity_with_extra_params["identity"]["account_number"] = masked_account_number
        identity_with_extra_params["identity"]["name"] = holder_obj.get("name", "")
        identity_with_extra_params["identity"]["address"] = holder_obj.get("address", "")

        identity_with_extra_params["identity"]["phone_number"] = holder_obj.get("mobile", "") or ""
        identity_with_extra_params["identity"]["pan_number"] = holder_obj.get("pan", "") or ""
        identity_with_extra_params["identity"]["dob"] = holder_obj.get("dob", "") or ""
        identity_with_extra_params["identity"]["email"] = holder_obj.get("email", "") or ""
        identity_with_extra_params["identity"]["holder_type"] = holders.get("type", "") or ""
        identity_with_extra_params["identity"]["joint_account_holders"] = holder_obj.get("joint_account_holders", []) or []

        identity_with_extra_params["identity"]["account_opening_date"] = account_opening_date
        identity_with_extra_params["identity"]["account_status"] = summary_obj.get("status", "") or ""
        identity_with_extra_params["identity"]["ifsc"] = summary_obj.get("ifscCode", "")
        identity_with_extra_params["identity"]["micr"] = summary_obj.get("micrCode", "")
        identity_with_extra_params["identity"]["account_category"] = summary_obj.get("type", "")
        identity_with_extra_params["identity"]["bank_name"] = bank
        try:
            identity_with_extra_params["identity"]["credit_limit"] = abs(round(float(summary_obj.get("drawingLimit", 0))))
        except ValueError:
            identity_with_extra_params["identity"]["credit_limit"] = 0
        except TypeError:
            identity_with_extra_params["identity"]["credit_limit"] = 0

        try:
            identity_with_extra_params["identity"]["od_limit"] = abs(round(float(summary_obj.get("currentODLimit", 0))))
        except ValueError:
            identity_with_extra_params["identity"]["od_limit"] = 0
        except TypeError:
            identity_with_extra_params["identity"]["od_limit"] = 0

        identity_with_extra_params['identity']['raw_account_category'] = summary_obj.get("facility", None)

        identity_with_extra_params['identity']['is_od_account'] = False

        identity_with_extra_params["keywords"] = {
            "amount_present": is_amount_present,
            "balance_present": is_balance_present,
            "date_present": is_date_extracted,
            "all_present": are_all_present
        }
        identity_with_extra_params["date_range"] = {'from_date': from_date, 'to_date': to_date}
        identity_with_extra_params["is_fraud"] = False
        identity_with_extra_params["fraud_type"] = None
        identity_with_extra_params["no_transactions_from_finsense"] = len(transactions_list) == 0

        LAMBDA_LOGGER.debug(
            "Parameters successfully extracted from S3/ AA object.",
            extra=local_logging_context.store
        )

        # no password incorrect check
        # no identity none check

        account = get_account_aa(entity_id, linked_account_ref_number)
        account_id = create_or_update_account_details_for_aa(entity_id, statement_id, bank, account, identity_with_extra_params, firstFiObject)

        local_logging_context.upsert(
            account_id=account_id
        )

        LAMBDA_LOGGER.info(
            "Successfully initialized account and extracted account_id. Attempting to update identity with extracted parameters and storing in DDB.",
            extra=local_logging_context.store
        )

        identity_with_extra_params["identity"].update({
            "account_id": account_id
        })

        # we dont store ifsc, micr and keywords in ddb
        identity_with_extra_params.get("identity", dict()).pop("ifsc", None)
        identity_with_extra_params.get("identity", dict()).pop("micr", None)
        keywords = identity_with_extra_params.pop("keywords", dict())

        identity_with_extra_params["country_code"] = "IN"
        identity_with_extra_params["currency_code"] = "INR"

        identity_with_extra_params['date_range'] = {'from_date': None, 'to_date': None}
        identity_with_extra_params['extracted_date_range'] = {'from_date': from_date, 'to_date': to_date}
        identity_with_extra_params['identity']['od_metadata'] = {
            'od_limit_by_extraction': identity_with_extra_params["identity"]["od_limit"], 'is_od_account_by_extraction': None
        }

        time_stamp_in_mlilliseconds = time.time_ns()
        ddb_object = {
            'statement_id': statement_id,
            'item_data': identity_with_extra_params,
            'created_at': time_stamp_in_mlilliseconds,
            'updated_at': time_stamp_in_mlilliseconds
        }

        bank_connect_identity_table.put_item(Item=ddb_object)

        # adding it to identity block for response to user
        # NOTE: always append this after data addition to identity table IMP
        identity_with_extra_params['date_range'] = identity_with_extra_params['extracted_date_range']

        identity_with_extra_params.get('identity',dict()).pop('raw_account_category', None)
        identity_with_extra_params.get('identity',dict()).pop('od_metadata', None)

        LAMBDA_LOGGER.info(
            "Identity successfully updated with extracted parameters and stored in DDB. Updating progress and fraud status.",
            extra=local_logging_context.store
        )

        update_progress(statement_id, "identity_status", "completed")
        update_progress_fraud_status(statement_id, "completed")

        # we send keywords too in response
        identity_with_extra_params["keywords"] = keywords
        identity_with_extra_params['identity']['account_category'], _ = get_final_account_category(identity_with_extra_params['identity']['account_category'], identity_with_extra_params['identity']['is_od_account'], None, None)

        LAMBDA_LOGGER.debug(
            "Identity progress and fraud status updated successfully.",
            extra=local_logging_context.store
        )
        LAMBDA_LOGGER.info(
            "Identity object updated and successful execution of the Finvu AA Identity lambda completed.",
            extra=local_logging_context.store
        )

    except ValueError as ve:
        capture_exception(ve)

    except Exception as e:
        print("exception occurred in aa identity: {}".format(e))

        local_logging_context.upsert(
            exception=str(e),
            trace=traceback.format_exc()
        )

        LAMBDA_LOGGER.error(
            "Exception observed while performing AA identity.",
            extra=local_logging_context.store
        )

    local_logging_context.clear()

    return identity_with_extra_params


def create_or_update_account_details_for_aa(
        entity_id: str, 
        statement_id: str,
        bank: str, 
        account: Union[AccountDict, None], 
        identity_with_extra_params: dict, 
        firstFiObject: dict
    ) -> Union[str, None]:
    identity_from_identity_item_data = identity_with_extra_params.get("identity", dict())

    identity_od_limit = identity_from_identity_item_data.get("od_limit")
    identity_credit_limit = identity_from_identity_item_data.get("credit_limit")
    ifsc = identity_from_identity_item_data.get("ifsc")
    micr = identity_from_identity_item_data.get("micr")
    account_category = identity_from_identity_item_data.get("account_category")
    is_od_account = identity_from_identity_item_data.get("is_od_account")
    pan_number = identity_from_identity_item_data.get("pan_number")
    phone_number = identity_from_identity_item_data.get("phone_number")
    email = identity_from_identity_item_data.get('email')
    dob = identity_from_identity_item_data.get("dob", "")
    account_status = identity_from_identity_item_data.get("account_status", "")
    holder_type = identity_from_identity_item_data.get("holder_type", "")
    joint_account_holders = identity_from_identity_item_data.get("joint_account_holders", [])


    # Fetch AA Account Number
    linked_account_ref_number = firstFiObject.get("linkedAccRef", "")
    masked_account_number = firstFiObject.get("maskedAccNumber", "")
    account_opening_date = firstFiObject.get("Summary", dict()).get("openingDate", None)

    if isinstance(account_opening_date, str):
        account_opening_date = account_opening_date.replace('/', '-')

    if account:
        account_id = account.get("account_id", None)
        add_statement_to_account(entity_id, account_id, statement_id)

        account_data_to_update: list[tuple[str, Any]] = []

        if identity_credit_limit:
            account_data_to_update.append(('credit_limit', identity_credit_limit))

        # Update account OD Limits
        # NOTE: we dont ask ask_od_limit_flag in case of AA hence passed False
        account_item_data = account.get('item_data')
        updated_od_limit_details = configure_od_limit_after_identity_extraction(account_item_data, identity_from_identity_item_data, 'aa', ask_od_limit_flag=False)
        if updated_od_limit_details:
            account_data_to_update.append(('neg_txn_od', updated_od_limit_details.get('neg_txn_od')))
            account_data_to_update.append(('od_limit', updated_od_limit_details.get('od_limit')))

        # Update DDB
        update_account_table_multiple_keys(entity_id, account_id, account_data_to_update)
        return account_id
    
    print("Creating account with linked_account_ref_number ", linked_account_ref_number)
    return create_new_account_aa(
        entity_id,
        bank,
        masked_account_number,
        linked_account_ref_number,
        statement_id,
        ifsc,
        micr,
        account_category,
        account_opening_date,
        is_od_account,
        identity_od_limit,
        identity_credit_limit,
        pan_number,
        phone_number,
        email,
        dob,
        account_status,
        holder_type,
        joint_account_holders
    )


def push_identity_to_portaldb(event, context):
    """
    This lambda is triggered by a Dynamo DB Stream on fsm-results table.
    It extracts the identity from ddb and pushes it into finboxdashboard database 
    through an API call.

    Both INSERT and MODIFY cases are handled here.
    """
    # print("event: {}".format(event))

    event_records = event.get("Records", [])

    if len(event_records) == 0:
        print("no records were found, exiting...")
        return

    for record in event_records:
        ddb_event_name = record["eventName"]
        if ddb_event_name not in ['INSERT', 'MODIFY']:
            continue

        # get the relevant identity data
        statement_id = record["dynamodb"]["NewImage"]["statement_id"]["S"]

        # identity item data
        item_data = record.get("dynamodb", dict()).get("NewImage", dict()).get("item_data", dict()).get("M", dict())

        transformed_identity = transform_identity(item_data)

        # send this identity info to dashboard
        print("calling dashboard api to push identity info for statement_id={} and event={}".format(statement_id, ddb_event_name))

        url = '{}/bank-connect/v1/internal/save_identity_postgres/'.format(DJANGO_BASE_URL)

        request_headers = {
            'x-api-key': API_KEY,
            'Content-Type': "application/json",
        }

        request_payload = {
            "ddb_event_name": ddb_event_name,
            "statement_id": statement_id,
            **transformed_identity
        }

        # print("request_payload: {}".format(request_payload))

        retries = 3
        sleep_duration = 5

        while retries:
            response = call_api_with_session(url,"POST", json.dumps(request_payload), request_headers)

            if response.status_code == 200:
                break
            retries -= 1
            time.sleep(sleep_duration)
