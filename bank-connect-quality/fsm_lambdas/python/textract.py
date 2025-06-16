import boto3
import os
import shutil
import time
import csv
from openpyxl import Workbook
from python.configs import REGION

textract_client = boto3.client('textract', region_name=REGION)
s3_resource = boto3.resource('s3', region_name=REGION)

class Textract:
    def __init__(self, pdf_key="", bucket_name="", job_id="", destination_bucket_name="", statement_id="", page_num=None):
        self.pdf_key = pdf_key
        self.bucket_name = bucket_name
        self.job_id = job_id
        self.destination_bucket_name = destination_bucket_name
        self.POLL_TIME_INTERVAL = 3 # in seconds
        self.FETCH_NEXT_TIME_INTERVAL = 0.5 # in seconds
        self.statement_id = statement_id
        self.page_num = page_num
    
    def start_textract_analysis(self):
        if self.pdf_key is None or self.bucket_name is None:
            raise Exception("PDF Key and Bucket Name should be present to start Textract Analysis")
        response = textract_client.start_document_analysis(
            DocumentLocation={
                "S3Object": {
                    "Bucket": self.bucket_name, 
                    "Name": self.pdf_key
                }
            },
            FeatureTypes=["TABLES"]
        )
        return response["JobId"]

    def __wait_for_textract_job(self):
        if not self.job_id:
            raise Exception("Job Id should be present for polling")
        while True:
            response = textract_client.get_document_analysis(JobId=self.job_id)
            status = response['JobStatus']
            if status in ['SUCCEEDED', 'FAILED']:
                return status
            print(f"Job {self.job_id} is {status}. Waiting for completion...")
            time.sleep(self.POLL_TIME_INTERVAL)
    
    def extract_table_data(self, blocks):
        # Extract table data for each page
        page_tables = {}
        # Map IDs to block data
        block_map = {block['Id']: block for block in blocks}
        for block in blocks:
            if block['BlockType'] == 'TABLE':
                page_number = block['Page']  # Get the page number of the table
                if page_number not in page_tables:
                    page_tables[page_number] = []
                rows = []
                # Iterate over child relationships
                for relationship in block.get('Relationships', []):
                    if relationship['Type'] == 'CHILD':
                        for child_id in relationship['Ids']:
                            cell_block = block_map[child_id]
                            if cell_block['BlockType'] == 'CELL':
                                row_index = cell_block['RowIndex'] - 1
                                column_index = cell_block['ColumnIndex'] - 1
                                # Ensure rows list is large enough
                                while len(rows) <= row_index:
                                    rows.append([])
                                # Ensure row has enough columns
                                while len(rows[row_index]) <= column_index:
                                    rows[row_index].append("")
                                # Add cell text
                                cell_text = ""
                                for cell_relationship in cell_block.get('Relationships', []):
                                    if cell_relationship['Type'] == 'CHILD':
                                        for child_id in cell_relationship['Ids']:
                                            word_block = block_map[child_id]
                                            if word_block['BlockType'] == 'WORD':
                                                cell_text += word_block['Text'] + " "
                                rows[row_index][column_index] = cell_text.strip()
                page_tables[page_number].extend(rows)
        return page_tables
    
    def save_page_tables_as_csv(self, page_tables, file_path):
        # Save each page's table data as a separate CSV file
        for page_number, table_data in page_tables.items():
            file_name = f'{file_path}/extracted_table_page_{page_number}.csv'
            with open(file_name, mode='w', newline='', encoding='utf-8') as file:
                writer = csv.writer(file)
                writer.writerows(table_data)
            print(f"Page {page_number} table saved to '{file_name}'")
    
    def save_page_tables_as_excel(self, page_tables, job_id):
        
        if not page_tables and self.page_num:
            page_tables = {self.page_num:[[]]}
        
        # Save each page's table data as a separate Excel file
        for page_number, table_data in page_tables.items():
            file_name = f'/tmp/{self.statement_id}_extracted_table_page_{self.page_num}.xlsx'
            workbook = Workbook()
            sheet = workbook.active
            sheet.title = f'Page {page_number}'

            # Write table data to the Excel sheet
            for row in table_data:
                sheet.append(row)

            workbook.save(file_name)
            print(f"Page {self.page_num} table saved to '{file_name}'")

    
    def __get_textract_results(self):
        if not self.job_id:
            raise Exception("Job Id should be present to get Textract Results")
        
        all_blocks = []
        next_token = None
        counter = 0
        while True:
            if next_token:
                response = textract_client.get_document_analysis(
                        JobId = self.job_id, 
                        NextToken=next_token
                    )
            else:
                response = textract_client.get_document_analysis(
                    JobId = self.job_id
                )
            # Add blocks from the current page
            all_blocks.extend(response['Blocks'])
            # Check for more pages
            next_token = response.get('NextToken')
            if not next_token:
                break
            print(f"Fetching more results, current token count {counter}...")
            counter += 1
            time.sleep(self.FETCH_NEXT_TIME_INTERVAL)
        return all_blocks
    
    def upload_file_to_s3_bucket(self, path, destination_bucket_name, destination_bucket_path):
        s3_resource.Bucket(destination_bucket_name).upload_file(path, destination_bucket_path)
    
    def get_csv_tables(self, get_paths=False):
        if not (self.job_id and self.destination_bucket_name and self.statement_id):
            raise Exception("Job Id & Destination Bucket Name should be present to get Textract Results")
        
        response = {
            "message": "SUCCESS",
            "destination_bucket": self.destination_bucket_name,
            "destination_bucket_paths": []
        }
        
        job_status = self.__wait_for_textract_job()
        if job_status != "SUCCEEDED":
            response["message"] = "FAILED"
            return response
        
        blocks = self.__get_textract_results()
        page_tables = self.extract_table_data(blocks)

        # saving as excel because comma separated values are interpreted as different columns in pd.read_csv downstream
        # self.save_page_tables_as_csv(page_tables, file_path=f"/tmp/{self.job_id}")
        self.save_page_tables_as_excel(page_tables=page_tables, job_id=self.job_id)
        
        all_files_in_dir = [
            f for f in os.listdir("/tmp")
            if f"{self.statement_id}_extracted_table_page_{self.page_num}" in f
        ]

        print("All files in DIR : ", all_files_in_dir)
        local_paths = []
        for file_name in all_files_in_dir:
            destination_path = f"textract/{self.statement_id}/{self.statement_id}_{self.page_num}.xlsx"
            path = f"/tmp/{file_name}"
            response["destination_bucket_paths"].append(destination_path)
            self.upload_file_to_s3_bucket(
                path = f"/tmp/{file_name}",
                destination_bucket_name=self.destination_bucket_name,
                destination_bucket_path=destination_path
            )
            local_paths.append(path)
            
            if not get_paths:
                os.remove(path)
            
        return local_paths if get_paths else response