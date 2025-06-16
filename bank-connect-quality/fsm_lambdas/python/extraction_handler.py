import json, time, pdf2image, ocrmypdf
from sentry_sdk import set_context, set_tag
from library.fitz_functions import read_pdf
from library.transactions import get_transaction
from library.validations import update_epoch_date, front_fill_balance, correct_transaction_type, connect_transaction, correct_2row_transaction_notes
from library.hsbc_ocr import process_transactions
from library.utils import EPOCH_DATE
from python.handlers import update_bsa_extracted_count, update_transactions_on_session_date_range, update_last_page, update_field_for_statement
from python.enrichment_regexes import check_and_get_everything
from python.store_data import store_data_from_enrichment_regexes
from python.configs import LAMBDA_LOGGER, CATEGORIZE_RS_PRIVATE_IP
from python.configs import *
from concurrent.futures import ThreadPoolExecutor
from python.utils import check_and_distribute_transactions_in_pages, update_transactions_on_session_date_range, update_field_for_statement
from python.context.logging import LoggingContext
from category import SingleCategory
from library.fraud import transaction_balance_check, optimise_transaction_type

def multithreading_helper(params):
    print(f"Trying to extract for page_number {params['page_number']}.")
    response = lambda_client.invoke(
        FunctionName=EXTRACT_TRANSACTIONS_PAGE_FUNCTION, 
        Payload=json.dumps(params), 
        InvocationType='RequestResponse'
    )
    payload = json.loads(response['Payload'].read().decode('utf-8'))
    result = json.loads(payload['body'])['result']
    extraction_template_uuid = json.loads(payload['body'])['template_id']
    print('Found {} transactions in page {} for {} with template_id {}'.format(len(result), params['page_number'], params['key'], extraction_template_uuid))
    return {"result": result, "extraction_template_uuid": extraction_template_uuid}

def extraction_handler(event, context):

    local_logging_context: LoggingContext = LoggingContext(
        source="extraction_handler"
    )

    LAMBDA_LOGGER.info(
        "Initiating the extraction handler flow.",
        extra=local_logging_context.store
    )

    key = event.get('key', '')
    bucket = event.get('bucket', '')
    name = event.get('name', '')
    account_number  = event.get('account_number', '')
    trans_bbox = event.get('trans_bbox', [])
    last_page_regex = event.get('last_page_regex', [])
    account_delimiter_regex = event.get('account_delimiter_regex', [])
    number_of_pages = event.get('number_of_pages', 0)
    enrichment_regexes = event.get('enrichment_regexes', {})
    country = event.get("country", "IN")
    account_category = event.get("account_category", None)
    identity = event.get('identity', {})
    opening_balance = identity.get('opening_bal', None)
    closing_balance = identity.get('closing_bal', None)
    session_date_range = event.get("session_date_range", {'from_date':None, 'to_date':None})
    statement_meta_data_for_warehousing = event.get("statement_meta_data_for_warehousing", {})
    extract_multiple_accounts = event.get('extract_multiple_accounts', False)
    org_metadata = event.get('org_metadata', dict())

    if opening_balance is not None and opening_balance != '':
        opening_balance = float(opening_balance)
    if closing_balance is not None and closing_balance != '':
        closing_balance = float(closing_balance)

    print("Invoking extraction for {}".format(key))

    response = s3.get_object(Bucket=bucket, Key=key)
    response_metadata = response.get('Metadata')
    statement_id = response_metadata.get('statement_id')
    bank = response_metadata.get('bank_name')
    password = response_metadata.get('pdf_password')
    entity_id = response_metadata.get('entity_id')
    set_tag("entity_id", entity_id)
    set_tag("statement_id", statement_id)
    set_context("extraction_handler_event_payload", event)

    local_logging_context.upsert(
        entity_id=entity_id,
        statement_id=statement_id,
        bank=bank,
        number_of_pages=number_of_pages
    )

    LAMBDA_LOGGER.debug(
        "Parameters successfully extracted from the event and S3. Attempting to store data extracted from enrichment regexes.",
        extra=local_logging_context.store
    )

    # check if data is retrieved from server and populate the files
    store_data_from_enrichment_regexes(enrichment_regexes, bank, country)
    
    LAMBDA_LOGGER.debug(
        "Attempting to extract all regexes for the specified bank and country.",
        extra=local_logging_context.store
    )

    # check and get all regexes for this bank and country
    check_and_get_everything(bank, country)

    # write a temporary file with content
    path = "/tmp/{}.pdf".format(statement_id)
    with open(path, 'wb') as file_obj:
        file_obj.write(response['Body'].read())

    if bank in ["solapur_siddheshwar", "hsbc", "jnkbnk", "bcabnk", "megabnk", 'abhinav_sahakari', 'kurla_nagrik', 'agrasen_urban', 'rajarshi_shahu']:# for all generic date time issue cases
        
        all_transactions = []
        
        final_set_transactions_hsbc = []
        page_breaks_hsbc = []    
        
        LAMBDA_LOGGER.debug(
            "Fitz document created. Attempting to invoke the page specific lambda in a multi- threaded fashion.",
            extra=local_logging_context.store
        )

        # Introducing multi-threading and lambda call for extraction of pages.
        process_list = [None]*number_of_pages
        transacton_iterable = []
        page_wise_extraction_templates = {}
        for page in range(number_of_pages):
            params = {
                'path': path, 
                'bank': bank, 
                'password': password, 
                'page_number': page, 
                'name': name, 
                'key': key, 
                'bucket': bucket, 
                'account_number': account_number, 
                'trans_bbox': trans_bbox, 
                'last_page_regex': last_page_regex,
                'enrichment_regexes': enrichment_regexes,
                'country': country,
                'identity': identity,
                'account_category': account_category,
                'number_of_pages': number_of_pages,
                'entity_id': entity_id,
                'account_delimiter_regex': account_delimiter_regex,
                'extract_multiple_accounts': extract_multiple_accounts 
            }
            process_list[page] = params
            
        with ThreadPoolExecutor(max_workers=10) as executor:
            transacton_iterable = executor.map(multithreading_helper, process_list)

        for page_number, transaction_ in enumerate(transacton_iterable):
            transaction = transaction_.get('result', [])
            all_transactions += [transaction]

            extraction_templated_id = transaction_.get('extraction_template_uuid', '')
            page_wise_extraction_templates[page_number] = extraction_templated_id

            if bank in ['hsbc','jnkbnk']:
                final_set_transactions_hsbc+=transaction
                page_breaks_hsbc.append(len(transaction))
        
        LAMBDA_LOGGER.debug(
            "Page specific lambda triggered successfully. Performing bank specific data cleanup.",
            extra=local_logging_context.store
        )
        
        if bank in ['abhinav_sahakari', 'kurla_nagrik', 'agrasen_urban', 'rajarshi_shahu']:
            all_transactions = check_and_distribute_transactions_in_pages(all_transactions)
        if bank in ["bcabnk"]:
            all_transactions = connect_transaction(all_transactions)
        if bank in ["bcabnk", 'hsbc']:
            all_transactions, _ = front_fill_balance(all_transactions, opening_balance, closing_balance, -1, bank)
        elif bank in ["hsbc", "jnkbnk"]:
            all_transactions = process_transactions(final_set_transactions_hsbc, page_breaks_hsbc, bank, name)
        elif bank in ['megabnk']:
            str_epoch_date = EPOCH_DATE.strftime("%Y-%m-%d %H:%M:%S")
            all_transactions = update_epoch_date(all_transactions, str_epoch_date)
        if bank in ['hsbc']:
            all_transactions = correct_2row_transaction_notes(all_transactions)
        
        LAMBDA_LOGGER.debug(
            "Attemping to update transactions as per session date range, inserting into DDB and attempting to send update state event post processing.",
            extra=local_logging_context.store
        )

        page_number = 0
        for page in all_transactions:
            page = update_transactions_on_session_date_range(session_date_range, page, statement_id, page_number)
            print('Found {} transactions in page {} for {}'.format(len(page), page_number, statement_id))
            time_stamp_in_mlilliseconds = time.time_ns()
            dynamo_object = {
                'statement_id': statement_id,
                'page_number': page_number,
                'item_data': json.dumps(page, default=str),
                'template_id': page_wise_extraction_templates[page_number],
                'transaction_count': len(page),
                'created_at': time_stamp_in_mlilliseconds,
                'updated_at': time_stamp_in_mlilliseconds
            }
            bank_connect_transactions_table.put_item(Item=dynamo_object)
            update_bsa_extracted_count(entity_id, statement_id, page_number, number_of_pages, statement_meta_data_for_warehousing, org_metadata=org_metadata)
            page_number += 1

    LAMBDA_LOGGER.info(
        "Update state event sent successfully per page and execution of the extraction handler lambda completed.",
        extra=local_logging_context.store
    )

    local_logging_context.clear()
            
def extraction_page_handler(event, context):

    local_logging_context: LoggingContext = LoggingContext(
        source="extraction_page_handler"
    )

    LAMBDA_LOGGER.info(
        "Initiating the extraction page handler flow.",
        extra=local_logging_context.store
    )

    path = event['path']
    page_number = event['page_number']
    key = event['key']
    bucket = event['bucket']
    enrichment_regexes = event.get('enrichment_regexes', {})
    country = event.get("country", "IN")
    number_of_pages = event.get('number_of_pages', 0)
    entity_id = event.get("entity_id")

    print("Invoking extraction page for {}".format(key))

    response = s3.get_object(Bucket=bucket, Key=key)
    response_metadata = response.get('Metadata')
    request_id = statement_id = response_metadata.get('statement_id')
    bank = response_metadata.get('bank_name')

    local_logging_context.upsert(
        entity_id=entity_id,
        statement_id=statement_id,
        bank=bank,
        page_number=page_number,
        number_of_pages=number_of_pages
    )

    LAMBDA_LOGGER.debug(
        "Parameters successfully extracted from the event and S3. Attempting to store data extracted from enrichment regexes.",
        extra=local_logging_context.store
    )

    # check if data is retrieved from server and populate the files
    store_data_from_enrichment_regexes(enrichment_regexes, bank, country)
    
    LAMBDA_LOGGER.debug(
        "Attempting to extract all regexes for the specified bank and country.",
        extra=local_logging_context.store
    )

    # check and get all regexes for this bank and country
    check_and_get_everything(bank, country)

    # write a temporary file with content
    path = "/tmp/{}.pdf".format(request_id)
    with open(path, 'wb') as file_obj:
        file_obj.write(response['Body'].read())
    event['path'] = path

    LAMBDA_LOGGER.debug(
        "Attepting to extract transactions via Plumber and Fitz.",
        extra=local_logging_context.store
    )

    transactions_ouput_dict = get_transaction(event, local_logging_context, LAMBDA_LOGGER)
    transaction = transactions_ouput_dict.get('transactions', [])
    extraction_template_uuid = transactions_ouput_dict.get('extraction_template_uuid')
    last_page_flag = transactions_ouput_dict.get('last_page_flag')
    removed_date_opening_balance = transactions_ouput_dict.get('removed_opening_balance_date')
    removed_date_closing_balance = transactions_ouput_dict.get('removed_closing_balance_date')
    
    LAMBDA_LOGGER.debug(
        "Transactions successfully extracted.",
        extra=local_logging_context.store
    )
    
    if removed_date_opening_balance is not None:
        update_field_for_statement(request_id, f'removed_date_opening_balance_{page_number}', removed_date_opening_balance)
    if removed_date_closing_balance is not None:
        update_field_for_statement(request_id, f'removed_date_closing_balance_{page_number}', removed_date_closing_balance)

    LAMBDA_LOGGER.debug(
        "Triggering transaction forward mapper and updating last page.",
        extra=local_logging_context.store
    )

    categorizer = SingleCategory(bank_name=bank, transactions=transaction, categorize_server_ip=CATEGORIZE_RS_PRIVATE_IP)
    transaction = categorizer.categorize_from_forward_mapper()

    if last_page_flag:
        update_last_page(request_id, page_number)

    LAMBDA_LOGGER.info(
        "Execution of the extraction page handler lambda completed.",
        extra=local_logging_context.store
    )

    local_logging_context.clear()

    response = {
        'statusCode': 200,
        'body': json.dumps({'result': transaction, 'template_id': extraction_template_uuid}, default=str)
    }
    return response

def karur_ocr_extraction_enhanced_handler(event, context):
    DPI_LIST_KARUR = [300, 290, 275]
    DPI_LIST_HSBC = [330, 300, 290, 275, 345]

    local_logging_context: LoggingContext = LoggingContext(
        source="karur_ocr_extraction_enhanced_handler"
    )

    LAMBDA_LOGGER.info(
        "Initiating the Karur OCR extraction enhanced handler flow.",
        extra=local_logging_context.store
    )

    bucket = str(event.get('bucket'))
    key = str(event.get('key'))
    page_number = int(event.get('page_number')) 
    number_of_pages = event.get('number_of_pages', 0)
    enrichment_regexes = event.get("enrichment_regexes", {})
    country = event.get("country", "IN")        # <- this value is hardcoded to IN for now
    session_date_range = event.get('session_date_range', {'from_date':None, 'to_date':None})
    statement_meta_data_for_warehousing = event.get("statement_meta_data_for_warehousing", {})
    org_metadata = event.get('org_metadata', dict())

    response = s3.get_object(Bucket=bucket, Key=key)
    response_metadata = response.get('Metadata')

    entity_id = response_metadata.get('entity_id')
    bank = response_metadata.get('bank_name')
    password = response_metadata.get('pdf_password')
    statement_id = response_metadata.get('statement_id')
    statement_meta_data_for_warehousing['session_date_range'] = session_date_range
    
    DPI_LIST = DPI_LIST_KARUR if bank == 'karur' else DPI_LIST_HSBC 
    fmt = 'jpeg' if bank == 'karur' else 'png' 
    file_suffix = 'jpg' if bank == 'karur' else 'png'
    grayscale = True if bank == 'karur' else False
    transparent = True if bank == 'karur' else False

    set_tag("entity_id", entity_id)
    set_tag("statement_id", statement_id)
    set_context("analyze_pdf_event_payload", event)

    local_logging_context.upsert(
        entity_id=entity_id,
        statement_id=statement_id,
        bank=bank,
        page_number=page_number,
        number_of_pages=number_of_pages
    )

    LAMBDA_LOGGER.debug(
        "Parameters successfully extracted from the event and S3. Attempting to store data extracted from enrichment regexes.",
        extra=local_logging_context.store
    )
    
    # check if data is retrieved from server and populate the files
    store_data_from_enrichment_regexes(enrichment_regexes, bank, country)
    
    LAMBDA_LOGGER.debug(
        "Attempting to extract all regexes for the specified bank and country.",
        extra=local_logging_context.store
    )

    # check and get all regexes for this bank and country
    check_and_get_everything(bank, country)
    
    # write a temporary file with content
    file_path = "/tmp/{}.pdf".format(statement_id)
    with open(file_path, 'wb') as file_obj:
        file_obj.write(response['Body'].read())
        
    print('Total number of pages in this statement: ', number_of_pages)
    print('currently processing page {} of uuid {}'.format(page_number, statement_id))

    extraction_template_uuid = ''
    output_folder_path = "/tmp/"

    LAMBDA_LOGGER.debug(
        f"Attempting to convert {file_path} to an image with jpeg format, output to {output_folder_path} via pdf2image.",
        extra=local_logging_context.store
    )
    
    final_transactions = []
    final_last_page_flag = False
    final_extraction_template_uuid = ''
    final_removed_date_opening_balance = None
    final_removed_date_closing_balance = None
    dpi_used = None
    
    for dpi in DPI_LIST:
        
        paths_to_images = pdf2image.convert_from_path(
            file_path, dpi=dpi, userpw=password, output_folder=output_folder_path, 
            paths_only=True, fmt=fmt, first_page=page_number+1, last_page=page_number+1,
            grayscale=grayscale, transparent=transparent, ownerpw=password
        )
        
        ocr_file_path = ""
        if len(paths_to_images)==0:

            LAMBDA_LOGGER.debug(
                "No images created as length of path_to_images is 0. Setting transactions as an empty list.",
                extra=local_logging_context.store
            )

            transactions, last_page_flag = [], False

        else:
            ocr_file_path = paths_to_images[0].replace(f'.{file_suffix}', '.pdf')

            LAMBDA_LOGGER.debug(
                f"Attempting to perform OCR on {ocr_file_path} and convert it into a PDF via ocrmypdf",
                extra=local_logging_context.store
            )

            ocrmypdf.ocr(
                paths_to_images[0],
                ocr_file_path,
                deskew=True,
                force_ocr=True,
                progress_bar=False
            )
            # Flag is passed in identity to fix the inconsistency in karur that occurs due to wrong detection of amounts by OCR
            event['identity']['fix_numericals'] = True
            
            LAMBDA_LOGGER.debug(
                "Attepting to extract transactions via Plumber and Fitz.",
                extra=local_logging_context.store
            )
                        
            event['path'] = ocr_file_path
            event['page_number'] = 0
            event['bank'] = bank

            transactions_ouput_dict = get_transaction(event, local_logging_context, LAMBDA_LOGGER)
            transactions= transactions_ouput_dict.get('transactions', [])
            extraction_template_uuid = transactions_ouput_dict.get('extraction_template_uuid')
            last_page_flag = transactions_ouput_dict.get('last_page_flag')
            removed_date_opening_balance = transactions_ouput_dict.get('removed_opening_balance_date')
            removed_date_closing_balance = transactions_ouput_dict.get('removed_closing_balance_date')
                        
            transactions_to_check = [_ for _ in transactions if (_['amount'] != -1.0 and _['balance'] != -1.0)]
            transactions_to_check, _, _, _ = optimise_transaction_type(transactions_to_check)
            
            if len(transactions) >= len(final_transactions) and transaction_balance_check(transactions_to_check) is None:
                final_transactions = transactions
                final_last_page_flag = last_page_flag
                final_extraction_template_uuid = extraction_template_uuid
                final_removed_date_opening_balance = removed_date_opening_balance
                final_removed_date_closing_balance = removed_date_closing_balance
                dpi_used = dpi
                break
            elif len(transactions) > len(final_transactions):
                final_transactions = transactions
                final_last_page_flag = last_page_flag
                final_extraction_template_uuid = extraction_template_uuid
                final_removed_date_opening_balance = removed_date_opening_balance
                final_removed_date_closing_balance = removed_date_closing_balance
                dpi_used = dpi
    
    transactions = final_transactions
    last_page_flag = final_last_page_flag
    extraction_template_uuid = final_extraction_template_uuid
    removed_date_opening_balance = final_removed_date_opening_balance
    removed_date_closing_balance = final_removed_date_closing_balance
   
    LAMBDA_LOGGER.debug(
        "Transactions successfully extracted.",
        extra=local_logging_context.store
    )
    
    if removed_date_opening_balance is not None:
        update_field_for_statement(statement_id, f'removed_date_opening_balance_{page_number}', removed_date_opening_balance)
    
    if removed_date_closing_balance is not None:
        update_field_for_statement(statement_id, f'removed_date_closing_balance_{page_number}', removed_date_closing_balance)
    
    number_of_transactions = len(transactions)
    print('Found {} transactions in page {} for {}'.format(number_of_transactions, page_number, statement_id))
    
    LAMBDA_LOGGER.debug(
        f"Triggering transaction forward mapper and updating last page for {number_of_transactions} transactions.",
        extra=local_logging_context.store
    )
    
    categorizer = SingleCategory(bank_name=bank, transactions=transactions, categorize_server_ip=CATEGORIZE_RS_PRIVATE_IP)
    transactions = categorizer.categorize_from_forward_mapper()  

    LAMBDA_LOGGER.debug(
        "Attemping to update transactions as per session date range, inserting into DDB and attempting to send update state event post processing.",
        extra=local_logging_context.store
    )

    transactions = update_transactions_on_session_date_range(session_date_range, transactions, statement_id, page_number)
    
    time_stamp_in_mlilliseconds = time.time_ns()
    dynamo_object = {
        'statement_id': statement_id,
        'page_number': page_number,
        'item_data': json.dumps(transactions, default=str),
        'template_id': extraction_template_uuid,
        'transaction_count': number_of_transactions,
        'created_at': time_stamp_in_mlilliseconds,
        'updated_at': time_stamp_in_mlilliseconds,
        'dpi': dpi_used
    }

    if last_page_flag:
        update_last_page(statement_id, page_number)

    # delete file after usage
    if os.path.exists(file_path):
        os.remove(file_path)
    if os.path.exists(ocr_file_path):
        os.remove(ocr_file_path)

    bank_connect_transactions_table.put_item(Item=dynamo_object)

    update_bsa_response = update_bsa_extracted_count(entity_id, statement_id, page_number, number_of_pages, statement_meta_data_for_warehousing, org_metadata=org_metadata)

    LAMBDA_LOGGER.info(
        f"Update state event sent successfully per page and execution of the Karur OCR extraction handler lambda completed with dpi = {dpi_used}",
        extra=local_logging_context.store
    )

    local_logging_context.clear()

    return update_bsa_response

def put_file_to_s3(file_path, bucket, key):
    s3_resource.Bucket(bucket).upload_file(file_path, key)

# def identity_extraction_handler(event, context):
#     key = event.get('key', '')
#     bucket = event.get('bucket', '')
#     preshared_names = event.get('preshared_names',[])

#     response = s3.get_object(Bucket=bucket, Key=key)
#     response_metadata = response.get('Metadata')
#     statement_id = response_metadata.get('statement_id')
#     bank = response_metadata.get('bank_name')
#     password = response_metadata.get('pdf_password')
#     entity_id = response_metadata.get('entity_id')

#     # write a temporary file with content
#     path = "/tmp/{}.pdf".format(statement_id)
#     with open(path, 'wb') as file_obj:
#         file_obj.write(response['Body'].read())
    
#     doc = read_pdf(path, password)
#     is_image = check_if_image(doc)

#     # if bank in ocr_supported_banks and is_image:
#     #     print("Extracting from ocr")
#     #     identity = extract_essential_identity_ocr(path, bank, password, preshared_names)
#     # else:
#     #     print("Not image, extracting from fitz")
#     identity = extract_essential_identity(path, bank, password, preshared_names)
    
#     return identity