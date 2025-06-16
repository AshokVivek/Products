import os, json, requests
from library.credit_card_extraction_with_ocr import extract_cc_identity, get_cc_transactions_using_fitz, generate_cc_report
from library.fitz_functions import read_pdf
from python.aws_utils import collect_results
from python.enrichment_regexes import check_and_get_everything
from python.store_data import store_data_from_enrichment_regexes
from sentry_sdk import capture_exception
from datetime import datetime
import time
from boto3.dynamodb.conditions import Key
from python.configs import LAMBDA_LOGGER, StatementType, CC_TRANSACTIONS_STREAM_NAME, CC_IDENTITY_STREAM_NAME, IS_SERVER, DJANGO_BASE_URL, API_KEY, bank_connect_cc_statement_table, bank_connect_cc_identity_table, s3
from python.configs import *
from python.aggregates import send_event_to_quality
import pdf2image, ocrmypdf
from library.utils import get_ocr_condition_for_credit_card_statement
from python.utils import create_identity_object_for_credit_card_quality, prepare_ware_house_identity_data_for_credit_card
from python.context.logging import LoggingContext
from python.utils import cc_prepare_warehouse_data
from python.clickhouse.firehose import send_data_to_firehose
import traceback
from library.enrichment_regexes import get_cc_transaction_templates
from python.api_utils import call_api_with_session


def cc_identity_handler(event, context):

    local_logging_context: LoggingContext = LoggingContext(
        source="cc_identity_handler"
    )

    LAMBDA_LOGGER.info(
        "Initiating the Credit Card identity handler flow.",
        extra=local_logging_context.store
    )

    bucket = event['bucket']
    key = event['key']
    template = event.get("template", {})
    image_hash_list = event.get('image_hash_list', [])
    statement_meta_data_for_warehousing = event.get('statement_meta_data_for_warehousing', dict())

    response = s3.get_object(Bucket=bucket, Key=key)

    pdf_metadata = response.get('Metadata')

    bank = pdf_metadata.get('bank_name')
    password = pdf_metadata.get('pdf_password')
    statement_id = pdf_metadata.get('statement_id')
    entity_id = pdf_metadata.get('entity_id')

    local_logging_context.upsert(
        entity_id=entity_id,
        statement_id=statement_id,
        statement_type=StatementType.CREDIT_CARD.value,
        bank=bank
    )

    statement_meta_data_for_warehousing['entity_id'] = entity_id
    statement_meta_data_for_warehousing['statement_id'] = statement_id

    LAMBDA_LOGGER.debug(
        "Parameters successfully extracted from the event and S3",
        extra=local_logging_context.store
    )

    path = f"/tmp/{statement_id}.pdf"
    with open(path, "wb+") as file_obj:
        file_obj.write(response['Body'].read())

    LAMBDA_LOGGER.debug(
        "Attempting to extract Credit Card identity",
        extra=local_logging_context.store
    )

    identity = extract_cc_identity(path, password, bank, template=template, image_hash_list=image_hash_list, statement_meta_data_for_warehousing=statement_meta_data_for_warehousing)
    warehouse_data = prepare_ware_house_identity_data_for_credit_card(statement_meta_data_for_warehousing)

    LAMBDA_LOGGER.debug(
        "Successfully extracted Credit Card Identity information.",
        extra=local_logging_context.store
    )

    print(f"identity received for statement id : {statement_id} is {identity}")
    meta = identity.get("meta")
    core_identity = identity.get("identity")

    # typecast datetime objects to str
    for key in core_identity:
        if isinstance(core_identity[key], datetime):
            core_identity[key] = core_identity[key].strftime("%d-%m-%Y")

    credit_card_number = core_identity.get('credit_card_number', None)
    payment_due_date = core_identity.get('payment_due_date', None)
    statement_date = core_identity.get('statement_date', None)
    total_dues = core_identity.get('total_dues', None)

    is_cc_number_extracted = credit_card_number not in ['', None]
    # if identity is failed due to is_image or password_incorrect, mark transaction status as failed else processing
    transaction_status = "failed" if meta.get("is_image") or meta.get("password_incorrect") or not is_cc_number_extracted else "processing"
    
    # Marking transactions as failed if payment_due_date and statement_date and total_dues all are not extracted
    if payment_due_date in [None, ''] and statement_date in [None, ''] and total_dues in [None, '']:
        transaction_status = "failed"

    message = None
    if meta.get("is_image"):
        message = "s: image statement"
    elif meta.get("password_incorrect"):
        message = "s: incorrect password was entered"
    elif not is_cc_number_extracted:
        message = "ds: credit card number was not extracted"

    if credit_card_number in [None,'']:

        LAMBDA_LOGGER.debug(
            "Credit card number not found. Sending event to Quality.",
            extra=local_logging_context.store
        )

        credit_card_identity_for_quality = create_identity_object_for_credit_card_quality(core_identity)
        send_event_to_quality(statement_id, entity_id, credit_card_identity_for_quality, is_credit_card = True)
        
    LAMBDA_LOGGER.debug(
        f"Attempting to update bank_connect_cc_statement entry with transaction status as {transaction_status}.",
        extra=local_logging_context.store
    )

    ## Making an entry in BankConnect Credit Card Statement Table
    bank_connect_cc_statement_table.update_item(
        Key = {
            "statement_id": statement_id
        },
        UpdateExpression = "set identity_status = :s, transaction_status = :t",
        ExpressionAttributeValues = {
            ':s': "completed", 
            ':t': transaction_status
        }
    )

    LAMBDA_LOGGER.debug(
        f"Successfully updated bank_connect_cc_statement entry with transaction status as {transaction_status}. Attempting to add entry into the bank_connect_cc_identity_table DDB table.",
        extra=local_logging_context.store
    )

    ## Making an entry in BankConnect Credit Card Identity Table
    ts = time.time_ns()
    dynamo_object = {
        'statement_id': statement_id,
        'item_data': core_identity,
        'message': message,
        'created_at': ts,
        'updated_at': ts
    }

    bank_connect_cc_identity_table.put_item(Item=dynamo_object)

    LAMBDA_LOGGER.debug(
        "Successfully added entry into the bank_connect_cc_identity_table DDB table.",
        extra=local_logging_context.store
    )

    ## Making an entry in BankConnect Credit Card Entity Table or updating if the entry is already present

    # check if the entry exists in the `bank_connect_cc_entity_mapping_table`

    LAMBDA_LOGGER.debug(
        "Attempting to extract entity mapping data.",
        extra=local_logging_context.store
    )

    qp = {
        'KeyConditionExpression': Key('entity_id').eq(entity_id), 
        'ConsistentRead': True
    }
    entity_mapping_data =  collect_results(bank_connect_cc_entity_mapping_table.query, qp)
    print("Entity Mapping Data : ", entity_mapping_data)
    if entity_mapping_data:
        print(f"Entity id : {entity_id} mapping exists, updating statements list")

        LAMBDA_LOGGER.debug(
            "Entity mapping data successfully found. Updating statement list.",
            extra=local_logging_context.store
        )

        bank_connect_cc_entity_mapping_table.update_item(
        Key={
            'entity_id': entity_id
        },
        UpdateExpression="SET item_data.statement_ids = list_append(item_data.statement_ids, :i), updated_at = :u",
        ExpressionAttributeValues={
            ':i': [statement_id],
            ':u': time.time_ns()
        })

        LAMBDA_LOGGER.debug(
            "Entity mapping data successfully found and statement list updated successfully.",
            extra=local_logging_context.store
        )

    else:
        print(f"For Entity Id: {entity_id}, making a new mapping entry")

        LAMBDA_LOGGER.debug(
            "Entity mapping data not found. Creating a new mapping entry in DDB.",
            extra=local_logging_context.store
        )

        dynamo_object = {
            'entity_id': entity_id,
            'item_data': {
                "statement_ids" : [statement_id]
            },
            'created_at': ts,
            'updated_at': ts
        }
        bank_connect_cc_entity_mapping_table.put_item(Item = dynamo_object)

        LAMBDA_LOGGER.debug(
            "Successfully added entry into the bank_connect_cc_entity_mapping_table DDB table.",
            extra=local_logging_context.store
        )

    os.remove(path)

    LAMBDA_LOGGER.info(
        "Successful execution of the Credit Card Identity lambda completed.",
        extra=local_logging_context.store
    )

    local_logging_context.clear()

    send_data_to_firehose([warehouse_data], CC_IDENTITY_STREAM_NAME)

    return identity

def extract_cc_transactions(event, context):
    local_logging_context = event.get("local_logging_context")
    if not local_logging_context:
        local_logging_context: LoggingContext = LoggingContext(source="extract_cc_transactions")

    LAMBDA_LOGGER.info("Initiating the Credit Card extract transaction flow.",extra=local_logging_context.store)

    bucket = event['bucket']
    key = event['key']
    enrichment_regexes = event.get('enrichment_regexes', {})
    template = event.get('template',{})
    statement_meta_data_for_warehousing =  event.get('statement_meta_data_for_warehousing', {})

    response = s3.get_object(Bucket=bucket, Key=key)

    pdf_metadata = response.get('Metadata')

    bank = pdf_metadata.get('bank_name')
    password = pdf_metadata.get('pdf_password')
    statement_id = pdf_metadata.get('statement_id')
    entity_id = pdf_metadata.get('entity_id')

    local_logging_context.upsert(
        entity_id=entity_id,
        statement_id=statement_id,
        statement_type=StatementType.CREDIT_CARD.value,
        bank=bank
    )

    LAMBDA_LOGGER.debug("Parameters successfully extracted from the event and S3. Attempting to prepare Fitz document.",extra=local_logging_context.store)

    path = f"/tmp/{statement_id}_{bank}.pdf"
    if IS_SERVER:
        path = f"/efs/cc/{statement_id}_{bank}.pdf"
    
    with open(path, "wb+") as file_obj:
        file_obj.write(response['Body'].read())
    
    doc = read_pdf(path, password)
    number_of_pages = doc.page_count

    bank_connect_cc_statement_table.update_item(
        Key = {
            'statement_id': statement_id
        },
        UpdateExpression = "set page_count = :s, pages_done = :d",
        ExpressionAttributeValues = {
            ':s': number_of_pages, 
            ':d': 0
        }
    )

    payload = {
        "key": key,
        "bucket": bucket,
        "page_count": number_of_pages,
        "enrichment_regexes": enrichment_regexes,
        "template": template,
        "entity_id": entity_id,
        "statement_id": statement_id,
        "pdf_password": password,
        "path": path,
        "bank_name": bank,
        "statement_meta_data_for_warehousing": statement_meta_data_for_warehousing
    }
    
    LAMBDA_LOGGER.debug(f"Successfully prepared Fitz document, inserted details into DDB and attempting to trigger per page extraction lambda for {number_of_pages} pages.", extra=local_logging_context.store)

    if IS_SERVER:
        # enrichment regexes will only bloat the request, popping it
        payload.pop("enrichment_regexes")
        payload.pop("template")
        
        LAMBDA_LOGGER.info(f"Keys present in payload : {payload.keys()}", extra=local_logging_context.store)
        # send events in batch
        LAMBDA_LOGGER.debug("Server case, Preparing messages in batch and sending them", extra=local_logging_context.store)
        MESSAGE_BATCH_SIZE = 10
        all_messages = []
        current_batch = []
        current_batch_count = 0
        
        for page_number in range(number_of_pages):
            payload["page_number"] = page_number
            message_group_id = f"{statement_id}_{page_number}"
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
            
        if current_batch_count:
            all_messages.append(current_batch)
            
        for i in range(len(all_messages)):
            LAMBDA_LOGGER.info(f"Processing message count {i}", extra=local_logging_context.store)
            response = sqs_client.send_message_batch(
                QueueUrl = CREDIT_CARD_EXTRACTOR_QUEUE_URL,
                Entries = all_messages[i]
            )
            LAMBDA_LOGGER.info(f"SQS Batch Send response : {response}", extra=local_logging_context.store)
    else:
        for i in range(number_of_pages):
            payload['page_number']=i
            response = lambda_client.invoke(
                FunctionName = CC_TRANSACTIONS_PAGE_LAMBDA_OCR, 
                Payload=json.dumps(payload), 
                InvocationType='Event'
            )
        os.remove(path)

    LAMBDA_LOGGER.info(f"Successful execution of the Credit Card Extraction lambda completed after triggering page specific lambdas for {number_of_pages} pages.",extra=local_logging_context.store)
    if not IS_SERVER:
        local_logging_context.clear()
    return {"message": "success"}

def extract_cc_transactions_page(event, context):
    local_logging_context = event.get("local_logging_context")
    
    if not local_logging_context:
        local_logging_context: LoggingContext = LoggingContext(source="extract_cc_transactions_page")
    
    LAMBDA_LOGGER.info("Initiating the Credit Card extract page transaction flow.", extra=local_logging_context.store)

    bucket = event['bucket']
    key = event['key']
    page_number = event['page_number']
    page_count = event['page_count']
    enrichment_regexes = event.get('enrichment_regexes', {})
    template = event.get('template',{})
    entity_id = event.get('entity_id')
    statement_meta_data_for_warehousing =  event.get('statement_meta_data_for_warehousing', {})
    bank = event.get("bank_name")
    password = event.get("pdf_password")
    statement_id = event.get("statement_id")
    country = event.get("country", "IN")

    if IS_SERVER:
        path = event.get("path")
        LAMBDA_LOGGER.info(f"Server Extraction Module, Using path {path}", extra=local_logging_context.store)
        # get the templates from the redis as message in batch will only bloat the network data
        template = get_cc_transaction_templates(bank_name = bank)
        LAMBDA_LOGGER.info("Got templates from redis since attempt type is Server", extra=local_logging_context.store)
    else:
        response = s3.get_object(Bucket=bucket, Key=key)
        pdf_metadata = response.get('Metadata')

    local_logging_context.upsert(
        entity_id=entity_id,
        statement_id=statement_id,
        statement_type=StatementType.CREDIT_CARD.value,
        bank=bank,
        page_number=page_number,
        number_of_pages=page_count
    )

    LAMBDA_LOGGER.debug("Parameters successfully extracted from the event and S3. Attempting to store data extracted from enrichment regexes.",extra=local_logging_context.store)

    # check if data is retrieved from server and populate the files
    if not IS_SERVER:
        store_data_from_enrichment_regexes(enrichment_regexes, bank, country)
        # check and get all regexes for this bank and country
        check_and_get_everything(bank, country)
    
    LAMBDA_LOGGER.debug("Attempting to extract all regexes for the specified bank and country.",extra=local_logging_context.store)

    if not IS_SERVER:
        path = f"/tmp/{statement_id}_{bank}.pdf"
        # here path is present in the event, use it directly no need to download file
        with open(path, "wb+") as file_obj:
            file_obj.write(response['Body'].read())

    LAMBDA_LOGGER.debug("Attepting to extract transactions via Fitz.",extra=local_logging_context.store)

    doc = read_pdf(path, password)
    
    if isinstance(doc, int):
        LAMBDA_LOGGER.info(f"Error while reading the PDF file, Error Code: {doc} for path: {path}", extra=local_logging_context.store)
        return

    LAMBDA_LOGGER.debug("Attepting to extract transactions via Fitz.",extra=local_logging_context.store)
    all_text_in_page = doc[page_number].get_text()
    
    LAMBDA_LOGGER.info(f"Length of All Text In Page = {len(all_text_in_page)}", extra = local_logging_context.store)

    template_id = None
    try:
        # TODO: REMOVE THIS TO ORCHESTRATOR LEVEL. This is a worthless condition on page level. A statement with text on 2 pages and ads on the others will be delayed due to OCR
        if(len(all_text_in_page))==0 or get_ocr_condition_for_credit_card_statement(doc, page_number=page_number):
            LAMBDA_LOGGER.debug(f"Either no text found in the page by Fitz or OCR condition for Credit Card Statement is not satisfied. Attempting to perform OCR via pdf2image for {path}.",extra=local_logging_context.store)
            output_folder_path = "/tmp/"
            
            paths_to_images = pdf2image.convert_from_path(
                path, dpi=300, userpw=password, output_folder=output_folder_path, 
                paths_only=True, fmt="jpeg", first_page=page_number+1, last_page=page_number+1,
                grayscale=True, transparent=True, ownerpw=password
            )

            if len(paths_to_images)>0:
                ocr_file_path = paths_to_images[0].replace('.jpg', '.pdf')
                LAMBDA_LOGGER.debug(f"Attempting to perform OCR on {ocr_file_path} and convert it into a PDF via ocrmypdf",extra=local_logging_context.store)
                ocrmypdf.ocr(
                    paths_to_images[0],
                    ocr_file_path,
                    deskew=True,
                    force_ocr=True,
                    progress_bar=False
                )
                LAMBDA_LOGGER.debug("Attepting to extract CC transactions via Fitz.",extra=local_logging_context.store)

                transactions, template_id = get_cc_transactions_using_fitz(ocr_file_path, password, bank, 0, template)
                # TODO: Send transactions directly to Kafka Topic Here, OCR Flag should be true to identify later or just update a key called OCR = true
                if os.path.exists(ocr_file_path):
                    os.remove(ocr_file_path)
                if os.path.exists(paths_to_images[0]):
                    os.remove(paths_to_images[0])
        else:
            LAMBDA_LOGGER.debug("Data found after preparing Fitz document, attempting to extract CC transactions via Fitz.",extra=local_logging_context.store)
            # TODO: Send transactions directly to Kafka Topic Here
            transactions, template_id = get_cc_transactions_using_fitz(path, password, bank, page_number, template)

    except Exception as e:
        local_logging_context.upsert(exception=str(e),trace=traceback.format_exc())
        LAMBDA_LOGGER.error("Exception observed while extracting transactions via Fitz or format conversion using pdf2image OR ocrmypdf.",extra=local_logging_context.store)
        local_logging_context.remove_keys(["exception", "trace"])
        print(e)
        capture_exception(e)
        transactions = []
        # TODO: Send transactions directly to Kafka Topic Here, With transactions 0

    LAMBDA_LOGGER.debug(f"{len(transactions)} transactions extracted. Attempting to store the same within DDB.",extra=local_logging_context.store)

    ts = time.time_ns()
    dynamo_object = {
        'statement_id': statement_id,
        'page_number': page_number,
        'item_data': json.dumps(transactions, default=str),
        'transaction_count': len(transactions),
        'template_id': template_id,
        "created_at": ts,
        "updated_at": ts
    }

    bank_connect_cc_transactions_table.put_item(Item=dynamo_object)
    LAMBDA_LOGGER.debug(f"Found {len(transactions)} transactions in page {page_number} of statement_id {statement_id}", extra=local_logging_context.store)
    LAMBDA_LOGGER.debug("Attempting to update the CC statement record within DDB.",extra=local_logging_context.store)

    response = dynamodb.update_item(    
        TableName = bank_connect_cc_statement_table_name,     
        Key={
            "statement_id" : {
                "S": statement_id
            }
        },    
        UpdateExpression="SET pages_done = pages_done + :inc",    
        ExpressionAttributeValues={
            ":inc": {
                "N": "1"
            }
        },  
        ReturnValues="UPDATED_NEW"
    )

    pages_done = response.get("Attributes", {}).get("pages_done", {}).get("N")
    LAMBDA_LOGGER.debug(f"Pages Done: {pages_done}, Page Count: {page_count}", extra=local_logging_context.store)

    if int(pages_done) == page_count:
        LAMBDA_LOGGER.debug("Current page detected to be the last page, attempting to update the status.",extra=local_logging_context.store)
        # get all transactions from dynamo db
        qp = {
            'KeyConditionExpression': Key('statement_id').eq(statement_id),
            'ConsistentRead': True, 
            'ProjectionExpression': 'entity_id, transaction_count'
        }
        all_transactions = collect_results(bank_connect_cc_transactions_table.query, qp)
        transaction_count = int(sum([item['transaction_count'] for item in all_transactions]))
        LAMBDA_LOGGER.debug(f"Transaction Count for statement id - {statement_id} is {transaction_count}",extra=local_logging_context.store)
        transaction_status = 'failed' if len(all_transactions)==0 else 'completed'
        
        bank_connect_cc_statement_table.update_item(
            Key = {'statement_id': statement_id},
            UpdateExpression = "set transaction_status = :s",
            ExpressionAttributeValues={':s': transaction_status}
        )
        LAMBDA_LOGGER.debug("CC Statement table updated. Attempting to invoke the /bank-connect/v1/credit_card/update_progress/ API.", extra=local_logging_context.store)

        url = f"{DJANGO_BASE_URL}/bank-connect/v1/credit_card/update_progress/"
        payload={
            "statement_id": statement_id,
            "page_count": page_count,
            "transaction_count": transaction_count
        }

        payload = json.dumps(payload, default=str)
        response = call_api_with_session(
            url = url,
            payload = payload,
            headers = {
                "x-api-key": API_KEY,
                "Content-Type": "application/json",
            },
            method="POST",
            params=None
        )
        LAMBDA_LOGGER.info(f"Response from Server - {response.text}", extra=local_logging_context.store)
        LAMBDA_LOGGER.debug("Attempting to extract the CC identity and sending event to Quality.",extra=local_logging_context.store)

        identity_from_ddb = get_identity_for_statement_id(statement_id)
        identity_from_ddb = identity_from_ddb[0] if identity_from_ddb else dict()
        identity_item_data = identity_from_ddb.get('item_data', dict())
        credit_card_identity_for_quality = create_identity_object_for_credit_card_quality(identity_item_data)
        send_event_to_quality(statement_id, entity_id, credit_card_identity_for_quality, is_credit_card = True)

        statement_meta_data_for_warehousing['entity_id'] = entity_id
        statement_meta_data_for_warehousing['statement_id'] = statement_id
        statement_meta_data_for_warehousing['bank_name'] = identity_item_data.get('bank')
        statement_meta_data_for_warehousing['credit_card_number'] = identity_item_data.get('credit_card_number')

        # TODO: Deprecate this as the pages have already been sent, we will save on read requests here
        stream_all_transactions = get_all_transactions_for_statement_id(statement_id)
        warehouse_data = cc_prepare_warehouse_data(statement_meta_data_for_warehousing, stream_all_transactions)
        send_data_to_firehose(warehouse_data, CC_TRANSACTIONS_STREAM_NAME)

        # since all the pages are done, the file can be removed from EFS here
        if IS_SERVER:
            os.remove(path)
    else:
        print(f"did not enter into the condition for {pages_done} / {page_count}")
    
    LAMBDA_LOGGER.info("Successful execution of the Credit Card Page Extraction lambda completed.", extra=local_logging_context.store)
    
    if not IS_SERVER:
        os.remove(path)
        local_logging_context.clear()

def get_identity_for_statement_id(statement_id):
    qp = {
        'KeyConditionExpression': Key('statement_id').eq(statement_id), 
        'ConsistentRead': True
    }
    return collect_results(bank_connect_cc_identity_table.query, qp)

def get_identity_for_all_statements_in_identity(progress_block, req_statement_id):
    identity = {}
    for statement in progress_block:
        statement_id = statement.get("statement_id")
        if req_statement_id and req_statement_id != statement_id:
            continue
        identity_data_block = get_identity_for_statement_id(statement_id)
        identity_data_block = identity_data_block[0] if identity_data_block else None

        if identity_data_block is None:
            continue

        message = identity_data_block.get("message")
        item_data = identity_data_block.get("item_data")
        if not message:
            if item_data.get('card_type', None) == None:
                item_data['card_type'] = None
            if item_data.get('rewards', None) == None:
                item_data['rewards'] = {
                    "closing_balance":None,
                    "opening_balance":None,
                    "points_claimed":None,
                    "points_credited":None,
                    "points_expired":None
                }
            identity[statement_id] = item_data
    return identity

def get_transactions_for_statement_id(statement_id):
    qp = {
        'KeyConditionExpression': Key('statement_id').eq(statement_id), 
        'ConsistentRead': True
    }
    return collect_results(bank_connect_cc_transactions_table.query, qp)


#this function sends all txns irrespective of transaction_type for streaming txn to CH
def get_all_transactions_for_statement_id(statement_id):
    transactions_blocks = get_transactions_for_statement_id(statement_id)
    statement_transactions = []
    for block in transactions_blocks:
        item_data = block.get("item_data", '[]')
        item_data = json.loads(item_data)
        template_id = block.get("template_id")
        for sequence_number, transaction in enumerate(item_data):
            transaction['template_id'] = template_id
            transaction['page_number'] = int(block.get('page_number'))
            transaction['sequence_number'] = int(sequence_number)
            statement_transactions.append(transaction)
    return statement_transactions

def get_transactions_for_all_statements(progress_block, req_statement_id):
    transactions = {}
    for statement in progress_block:
        statement_id = statement.get("statement_id")
        if req_statement_id and req_statement_id != statement_id:
            continue
        transactions_blocks = get_transactions_for_statement_id(statement_id)
        statement_transactions = []
        for block in transactions_blocks:
            item_data = block.get("item_data")
            item_data = json.loads(item_data)
            credit_debit_transactions = []
            for transaction in item_data:
                if transaction.get('transaction_type') in ['credit', 'debit']:
                    credit_debit_transactions.append(transaction)
            statement_transactions.extend(credit_debit_transactions)
        transactions[statement_id] = statement_transactions
    return transactions


def cc_access(event, context):
    access_type = event.get('access_type')
    entity_id = event.get('entity_id')
    req_statement_id = event.get('statement_id', None)
    req_statement_id_valid = False

    print(f"Event details: {event}")
    qp = {
        'KeyConditionExpression': Key('entity_id').eq(entity_id), 
        'ConsistentRead': True
    }

    entity_mapping_data = collect_results(bank_connect_cc_entity_mapping_table.query, qp)
    progress_block = []

    if entity_mapping_data:
        statement_list = entity_mapping_data[0].get("item_data", {}).get("statement_ids", [])
        for statement in statement_list:
            req_statement_id_valid = req_statement_id_valid or True if statement == req_statement_id else False
            
            qp = {
                'KeyConditionExpression' : Key('statement_id').eq(statement),
                'ConsistentRead' : True
            }

            statement_table_data = collect_results(bank_connect_cc_statement_table.query, qp)
            transaction_progress = statement_table_data[0]['transaction_status']
            
            progress_block.append({
                "statement_id" : statement,
                "entity_id": entity_id,
                "transaction_progress": transaction_progress
            })


    result_dict = {'progress': progress_block}
    print("Intermediate result dict with progress block: ", result_dict)

    if access_type=="identity":
        result_dict['identity'] = {}

        if req_statement_id and not req_statement_id_valid:
            return result_dict

        result_dict['identity'] = get_identity_for_all_statements_in_identity(progress_block, req_statement_id)

    elif access_type=="transactions":
        result_dict['transactions'] = get_transactions_for_all_statements(progress_block, req_statement_id)

    elif access_type=="report":

        all_identity = get_identity_for_all_statements_in_identity(progress_block, req_statement_id)
        all_transactions = get_transactions_for_all_statements(progress_block, req_statement_id)

        eligible_st_ids = [
            items.get("statement_id")
            for items in progress_block
            if items.get("transaction_progress") in ["completed"]
        ]
        result_dict['reports']=[]

        for statement_id in eligible_st_ids:
            identity_var = all_identity.get(statement_id)
            transactions_var = all_transactions.get(statement_id)

            file_name = f'{statement_id}_cc_excel_report.xlsx'
            file_path = f'/tmp/{file_name}'

            try:
                generate_cc_report(identity_var, transactions_var, file_path)
                s3_resource.Bucket(BANK_CONNECT_REPORTS_BUCKET).upload_file(file_path, file_name)
                s3_path = s3.generate_presigned_url( 'get_object', Params={'Bucket': BANK_CONNECT_REPORTS_BUCKET, 'Key': file_name})
                result_dict['reports'].append({
                    "statement_id": statement_id,
                    "report_url": s3_path
                })
                os.remove(file_path)
            except Exception as e:
                print(
                    f"couldn't generate excel report for statement id {statement_id} because of ",
                    e,
                )

    return result_dict