import boto3
import time
import os
import pandas as pd
import json
from python.textract import Textract
from python.aggregates import update_progress, get_statement_ids_for_account_id, update_last_page
from datetime import datetime
from python.configs import CATEGORIZE_RS_PRIVATE_IP, s3, bank_connect_identity_table, bank_connect_statement_table, bank_connect_transactions_table
from library.extract_txns_fitz import get_tables_each_page
from library.utils import get_date_format
from python.identity_handlers import get_account, create_new_account, add_statement_to_account
from python.handlers import update_field_for_statement, update_transactions_on_session_date_range, update_bsa_extracted_count
from library.extract_txns_finvu_aa import get_transaction_channel_description_hash
from category import SingleCategory
from pypdf import PdfReader, PdfWriter
from python.configs import REGION


s3_resource = boto3.resource('s3', region_name=REGION)

class TextractExtractor:
    """
        Textract Extractor - extracts useful identity, transactions & other useful information
    """
    def __init__(
        self,
        entity_id="",
        statement_id="",
        bucket_name="",
        job_id="",
        destination_bucket_name="",
        bank_name="",
        page_numbers=None,
        template=None,
        page_count=None,
        textract_job_id_map=None,
        pdf_key="",
        account_number=None,
        ):
            """
            Initialize an instance with attributes related to a statement processing job.

            Args:
                entity_id (str): Unique identifier for the entity.
                statement_id (str): Identifier for the bank statement.
                bucket_name (str): Source bucket name where the statement resides.
                job_id (str): Unique identifier for the processing job.
                destination_bucket_name (str): Bucket name for storing processed outputs.
                bank_name (str): Name of the bank.
                page_numbers (list): List of page numbers to process. Default is an empty list.
                template (dict): Template details for processing. Default is an empty dictionary.
                page_count (int): Total number of pages in the document.
                textract_job_id_map (dict): Mapping of Textract job IDs. Default is an empty dictionary.
                pdf_key (str): Key for accessing the PDF in the bucket.
                account_number (str): Account number associated with the statement.
            """
            self.entity_id = entity_id
            self.statement_id = statement_id
            self.bucket_name = bucket_name
            self.job_id = job_id
            self.destination_bucket_name = destination_bucket_name
            self.bank_name = bank_name
            self.page_numbers = page_numbers
            self.template = template
            self.page_count = page_count
            self.textract_job_id_map = textract_job_id_map
            self.pdf_key = pdf_key
            self.account_number = account_number
    
    
    def identity_for_textract(self, item_data, statement_id, entity_id, bank_name, page_count, file_key, bucket):
        identity = item_data.get("identity", {})
        extracted_date_range = item_data.get("extracted_date_range", {})
        account_number = identity.get("account_number", None)
        ifsc = identity.get("ifsc", None)
        micr = identity.get("micr", None)
        address = identity.get("address", None)
        name = identity.get("name", None)
        re_extraction = identity.get("re_extraction", False)

        self.account_number = account_number

        update_progress(statement_id, "identity_status", "processing", "")
        update_progress(statement_id, "processing_status", "processing", "")
        update_progress(statement_id, "transactions_status", "processing", "")

        from_date = extracted_date_range.get("from_date")
        to_date = extracted_date_range.get("to_date")

        from_date = get_date_format(from_date)
        to_date = get_date_format(to_date)

        if isinstance(from_date, datetime) and isinstance(to_date, datetime):
            from_date = from_date.strftime("%Y-%m-%d")
            to_date = to_date.strftime("%Y-%m-%d")
        else:
            from_date, to_date = None, None

        extracted_date_range['from_date'] = from_date
        extracted_date_range['to_date'] = to_date
        
        account = get_account(entity_id, account_number)
        account_id = account.get("account_id", None) if account else None
        if not account_id:
            account_id = create_new_account(
                entity_id=entity_id,
                bank=bank_name,
                account_number=account_number,
                statement_id=statement_id,
                ifsc=ifsc,
                micr=micr
            )
        else:
            account_statement_ids = get_statement_ids_for_account_id(entity_id=entity_id, account_id=account_id)
            if statement_id in account_statement_ids:
                re_extraction = True
            if not re_extraction:
                add_statement_to_account(entity_id, account_id, statement_id)

        identity["bank_name"] = bank_name
        identity["account_id"] = account_id
        item_data["date_range"] = {
            "from_date": None,
            "to_date": None
        }
        item_data["page_count"] = page_count

        time_stamp_in_mlilliseconds = time.time_ns()
        dynamo_db_object = {
            "statement_id": statement_id,
            "item_data": item_data,
            "created_at": time_stamp_in_mlilliseconds,
            "updated_at": time_stamp_in_mlilliseconds
        }
        bank_connect_identity_table.put_item(Item=dynamo_db_object)

        print("BankConnect Identity Table Insertion Completed")

        # split the pdf into individual pages
        self.split_pdf()

        # begin textract extraction for this pdf
        textract_job_id_map = self.invoke_textract()

        # save job id map for future reference
        bank_connect_statement_table.update_item(
            Key={'statement_id': statement_id},
            UpdateExpression="""set textract_job_id_map = :j, identity_status = :i, updated_at = :u, pages_done = :pd, page_count = :pc""",
            ExpressionAttributeValues={
                ":j": json.dumps(textract_job_id_map),
                ":u": time.time_ns(),
                ":i": 'completed',
                ":pd": 0,
                ":pc": page_count
            }
        )
        print("Statement Table updated with Textract Job Id Map")
        return
    
    def invoke_textract(self):
        
        page_count = self.page_count
        statement_id = self.statement_id
        bucket = self.bucket_name
        textract_job_id_map = {}
        
        for page in range(page_count):
            textract = Textract(pdf_key=f'textract/{statement_id}/{statement_id}_{page}.pdf', bucket_name=bucket, page_num=page)
            textract_job_id_map[page] = textract.start_textract_analysis()
        
        self.textract_job_id_map = textract_job_id_map

        return textract_job_id_map
    
    def split_pdf(self):
        bucket = self.bucket_name
        key = self.pdf_key
        statement_id = self.statement_id

        response = s3.get_object(Bucket=bucket, Key=key)
        # write a temporary file with content
        file_path = "/tmp/{}.pdf".format(statement_id)
        with open(file_path, 'wb') as file_obj:
            file_obj.write(response['Body'].read())
        
        reader = PdfReader(file_path)

        for page_number, page in enumerate(reader.pages):
            writer = PdfWriter()
            writer.add_page(page)
            
            output_pdf_path = os.path.join('/tmp/', f"{statement_id}_page_{page_number}.pdf")
            with open(output_pdf_path, "wb") as output_pdf:
                writer.write(output_pdf)
            
            self.upload_file_to_s3_bucket(output_pdf_path, bucket, f"textract/{statement_id}/{statement_id}_{page_number}.pdf")
            os.remove(output_pdf_path)
    
    def upload_file_to_s3_bucket(self, path, destination_bucket_name, destination_bucket_path):
        s3_resource.Bucket(destination_bucket_name).upload_file(path, destination_bucket_path)

    def wait_for_textract_job(self, get_paths=True, page_job_id=None, page_num=None):
        print(f"wait for textract job started for statement_id {self.statement_id} for page_num {page_num}")
        
        job_id = page_job_id if page_job_id else self.job_id

        textract = Textract(destination_bucket_name=self.destination_bucket_name, job_id=job_id, statement_id=self.statement_id, page_num=page_num)
        xlsx_paths = textract.get_csv_tables(get_paths)
        
        print(f"xlsx_paths returning here for statement: {self.statement_id} are: {xlsx_paths}")

        return xlsx_paths

    def __get_xlsx_file(self, page_num):

        print(f"fetching xlsx file from s3 for statement {self.statement_id} page_num: {page_num}")
        
        file_key = f"textract/{self.job_id}/{self.job_id}_extracted_table_page_{page_num}.xlsx"
        response = s3.get_object(Bucket=self.destination_bucket_name, Key=file_key)
        
        
        # file_path = "tmp/"
        # directory = os.path.dirname(file_path)
        
        # if directory and not os.path.exists(directory):
        #     os.makedirs(directory)
        
        file_path = f"/tmp/{self.job_id}_extracted_table_page_{page_num}.xlsx"
        
        with open(file_path, 'wb') as file_obj:
            file_obj.write(response['Body'].read())
        
        return file_path
    
    def get_textract_transaction_page(self, page_num, session_date_range={}, statement_meta_data_for_warehousing=None, org_metadata={}, xlsx_path=None):
        print(f"Trying to extract textract transactions for statement_id {self.statement_id} page {page_num} with xlsx_path {xlsx_path}")
        if not self.job_id and not self.textract_job_id_map:
            raise Exception("Job Id should be present to get Textract Results")
        
        if not self.destination_bucket_name:
            raise Exception("destination_bucket_name should be present to get Textract Results")
        
        xlsx_file_path = xlsx_path if xlsx_path else self.__get_xlsx_file(page_num)

        textract_df = pd.read_excel(xlsx_file_path, dtype=str)
        textract_df = textract_df.fillna(value='')
        textract_table = textract_df.to_dict('records')

        payload = self.__prepare_transctions_payload(page_num, textract_table)
        page_number = page_num
        transactions_output_dict = get_tables_each_page(payload, {}, None)
        transactions = transactions_output_dict.get('transactions', [])
        extraction_template_uuid = transactions_output_dict.get('extraction_template_uuid', '')
        last_page_flag = transactions_output_dict.get('last_page_flag')
        removed_date_opening_balance = transactions_output_dict.get('removed_opening_balance_date')
        removed_date_closing_balance = transactions_output_dict.get('removed_closing_balance_date')
        

        if removed_date_opening_balance is not None:
            update_field_for_statement(self.statement_id, f'removed_date_opening_balance_{page_number}', removed_date_opening_balance)

        if removed_date_closing_balance is not None:
            update_field_for_statement(self.statement_id, f'removed_date_closing_balance_{page_number}', removed_date_closing_balance)
        number_of_transactions = len(transactions)

        print('Found {} transactions in page {} for {}'.format(number_of_transactions, page_number, self.statement_id))

        transactions = update_transactions_on_session_date_range(session_date_range, transactions, page_number, self.statement_id)

        # now categorise the transactions
        transactions = get_transaction_channel_description_hash(
            transactions_list=transactions,
            bank=self.bank_name,
            name="",
            country="IN",
            account_category=""
        )

        categorizer = SingleCategory(bank_name=self.bank_name, transactions=transactions, categorize_server_ip=CATEGORIZE_RS_PRIVATE_IP)
        transactions = categorizer.categorize_from_forward_mapper()

        if last_page_flag:
            update_last_page(self.statement_id, page_number)

        os.remove(xlsx_file_path)
        
        time_stamp_in_mlilliseconds = time.time_ns()
        dynamo_object = {
            'statement_id': self.statement_id,
            'page_number': page_number,
            'item_data': json.dumps(transactions, default=str),
            'template_id': extraction_template_uuid,
            'transaction_count': number_of_transactions,
            'created_at': time_stamp_in_mlilliseconds,
            'updated_at': time_stamp_in_mlilliseconds
        }
        
        bank_connect_transactions_table.put_item(Item=dynamo_object)

        # print(f"updating bsa for statement_id {self.statement_id} for page_number {page_number} of {self.page_count}")

        # update_bsa_response = update_bsa_extracted_count(self.entity_id, self.statement_id, page_number, self.page_count, statement_meta_data_for_warehousing, org_metadata=org_metadata)
        
        # return update_bsa_response
    
    def get_all_transactions(self, xlsx_paths=[]):

        print(f"all textract transactions called for statement_id: {self.statement_id}")
        
        if not self.page_count:
            raise Exception("page_count should be present to get Textract Results")
        
        if not self.template:
            raise Exception("template should be present to get Textract Results")

        for i in range(self.page_count):
            xlsx_path = None
            if isinstance(xlsx_paths, list) and len(xlsx_paths) > i:
                xlsx_path = xlsx_paths[i]
            self.get_textract_transaction_page(i, xlsx_path=xlsx_path)
    
    def __prepare_transctions_payload(self, page_num, textract_table):
        
        payload = {
            "path": None,
            "password": None,
            "bank": self.bank_name,
            "page_number": page_num,
            "account_number": self.account_number,
            "key": None,
            "last_page_regex": [],
            "number_of_pages": self.page_count,
            "page": None,
            "account_delimiter_regex": [],
            "extract_multiple_accounts": False,
            "trans_bbox": [self.template],
            "original_page_num": None,
            "unused_raw_txn_rows_from_second_page": {'raw_rows':[],'transaction_rows':[]},
            "country": 'IN',
            "identity": {},
            "session_date_range": {},
            "account_category": None,
            "textract_table": textract_table
        }

        return payload