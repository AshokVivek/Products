from app.template_dashboard.utils import check_and_migrate
from app.database_utils import quality_database
from app.conf import s3, s3_client_old, OLD_QUALITY_BUCKET, QUALITY_BUCKET
import os
import threading

async def migrate_pdfs():
    query_for_all_pdfs = """
                            SELECT distinct(statement_id), bank_name, created_at from statement_quality order by created_at desc
                        """
    statement_ids = await quality_database.fetch_all(query=query_for_all_pdfs)

    # for items in statement_ids:
    #     item = dict(items)
    #     statement_id = item.get("statement_id")
    #     bank_name = item.get("bank_name")
    #     check_and_migrate(statement_id, bank_name)
    NUM_THREADS = 25
    for i in range(0, len(statement_ids), NUM_THREADS):
        thread_pool = []
        for j in range(NUM_THREADS):
            index = (i*NUM_THREADS) + j
            
            if index >= len(statement_ids):
                break

            items = statement_ids[index]
            item = dict(items)
            statement_id = item.get("statement_id")
            bank_name = item.get("bank_name")
            
            t = threading.Thread(target=check_and_migrate, args=(statement_id, bank_name))
            t.start()
            thread_pool.append(t)

        for threads in thread_pool:
            threads.join()
        print("Items done : ", i)

def download_file_and_upload(key, source_bucket, destination_bucket):
    response = s3_client_old.get_object(Bucket=source_bucket, Key=key)
    response_metadata = response.get('Metadata')
    
    file_path = f"/tmp/{key}"
    with open(file_path, 'wb') as file_obj:
        file_obj.write(response['Body'].read())
        
    s3.Bucket(destination_bucket).upload_file(
            file_path, 
            key, 
            ExtraArgs = 
                {
                    'Metadata': response_metadata
                }
        )
    if os.path.exists(file_path):
        os.remove(file_path)

async def migrate_quality_bucket_logos():
    paginator = s3_client_old.get_paginator('list_objects')
    source_bucket = OLD_QUALITY_BUCKET
    destination_bucket = QUALITY_BUCKET
    NUM_THREADS_SPAWN = 2
    
    operation_parameters = {
        'Bucket': source_bucket
    }

    page_iterator = paginator.paginate(**operation_parameters)
    pagination_count = 0
    
    for page in page_iterator:
        contents = page['Contents']
        print(f"Pagination Count : {pagination_count}, Total Content: {len(contents)}")
        pagination_count += 1
        items_done = 0
        total_count= len(contents)

        while items_done < total_count:
            num_of_threads_to_spawn = min(NUM_THREADS_SPAWN, total_count-items_done)
            threads = []
            
            for index in range(num_of_threads_to_spawn):
                intended_index = items_done+index
                key = contents[intended_index].get("Key")
                
                t = threading.Thread(
                        target=download_file_and_upload, 
                        args=(key, source_bucket, destination_bucket)
                    )
                t.start()
                threads.append(t)
            
            items_done += NUM_THREADS_SPAWN
            for thread in threads:
                thread.join()
            print("Total Items done : ", items_done)