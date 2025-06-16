import traceback
import fitz
from psycopg2.extras import Json

from operator import itemgetter
from app.pdf_utils import read_pdf

from app.conf import (
    s3,
    PDF_BUCKET
)

from app.database_utils import DBConnection
from app.constants import (
    QUALITY_DATABASE_NAME,
)

pdf_bucket = PDF_BUCKET


def inconsistency_solve_consumer(inconsistent_transaction_data):
    statement_id = inconsistent_transaction_data.get('statement_id', None)
    bank_name = inconsistent_transaction_data.get('bank_name', None)
    inconsistent_transaction = inconsistent_transaction_data.get('inconsistent_transaction', [])
    inconsistent_transaction = sorted(inconsistent_transaction, key=itemgetter('page_number', 'sequence_number'))

    if not statement_id or not bank_name:
        print("statement_id and bank_name cannot be none")
        return

    statement_pdf_file_key = f"pdf/{statement_id}_{bank_name}.pdf"

    try:
        pdf_bucket_response = s3.get_object(Bucket=pdf_bucket, Key=statement_pdf_file_key)
    except Exception as e:
        print(e, 'File Not Found')
        return

    response_metadata = pdf_bucket_response.get('Metadata')
    password = response_metadata.get('pdf_password')

    temp_file_path = f'/tmp/temp_{statement_id}'
    with open(temp_file_path, 'wb') as theFile:
        theFile.write(pdf_bucket_response['Body'].read())

    doc = read_pdf(temp_file_path, password)

    inconsistent_transaction_1 = inconsistent_transaction[0]
    inconsistent_transaction_2 = inconsistent_transaction[1]
    first_transaction_page_number = inconsistent_transaction_1.get('page_number', 0)
    second_transaction_page_number = inconsistent_transaction_2.get('page_number', 0)

    first_transaction_bbox = second_transaction_bbox = []

    # page rects
    first_page_rect = doc[first_transaction_page_number].rect
    second_page_rect = doc[second_transaction_page_number].rect

    if abs(first_transaction_page_number - second_transaction_page_number) >= 2:
        insert_status_into_db(True, bank_name, statement_id, inconsistent_transaction)
        return f"{statement_id}: Inconsistency due to extraction issue"

    transaction_details_to_search_in_pdf = [
        'balance',
        'transaction_note',
        'date',
    ]

    for transaction_detail in transaction_details_to_search_in_pdf:
        if len(first_transaction_bbox) and len(second_transaction_bbox):
            break

        if not len(first_transaction_bbox):
            search_string = str(inconsistent_transaction_1.get(transaction_detail, ''))

            if transaction_detail == 'transaction_note':
                search_string = " ".join(search_string.split(" ")[:2])
            first_transaction_bbox = doc[first_transaction_page_number].search_for(search_string)

        if not len(second_transaction_bbox):
            search_string = str(inconsistent_transaction_2.get(transaction_detail, ''))

            if transaction_detail == 'transaction_note':
                search_string = " ".join(search_string.split(" ")[:2])
            second_transaction_bbox = doc[second_transaction_page_number].search_for(search_string)
    if len(first_transaction_bbox) and len(second_transaction_bbox):
        first_transaction_bbox = first_transaction_bbox[0]
        second_transaction_bbox = second_transaction_bbox[0]
    else:
        return f"{statement_id}: Failed to Locate transactions on the pdf"
    all_transaction_numbers = []

    if first_transaction_page_number == second_transaction_page_number:

        search_area = (
            0,
            first_transaction_bbox.y0,
            second_page_rect.x1,
            second_transaction_bbox.y1
        )

        text_between_transactions = get_text_in_box(doc[first_transaction_page_number], search_area)

    else:

        first_search_area = (
            0,
            first_transaction_bbox.y0,
            first_page_rect.x1,  # last coordinate of that page
            first_page_rect.y1  # last coordinate of that page
        )

        second_search_area = (
            0,
            0,
            second_page_rect.x1,  # last coordinate of that page
            second_transaction_bbox.y1  # last coordinate of that page
        )

        text_between_transactions = get_text_in_box(doc[first_transaction_page_number], first_search_area)
        text_between_transactions.extend(get_text_in_box(doc[second_transaction_page_number], second_search_area))

    for text in text_between_transactions:
        try:
            text = text.replace('c', '').replace('r', '').replace('C', '').replace('R', '')
            all_transaction_numbers.append(float(text))
        except Exception:
            pass
    first_transaction_balance = inconsistent_transaction_1.get('balance')
    second_transaction_balance = inconsistent_transaction_2.get('balance')

    difference_between_inconsistent_balances = abs(second_transaction_balance - first_transaction_balance)

    if difference_between_inconsistent_balances in all_transaction_numbers:
        insert_status_into_db(True, bank_name, statement_id, inconsistent_transaction)
        return f"{statement_id}: Inconsistency due to extraction issue"

    for index, number in enumerate(all_transaction_numbers):
        if first_transaction_balance == number:
            continue
        if (first_transaction_balance + number) in all_transaction_numbers or (
                first_transaction_balance - number) in all_transaction_numbers:
            insert_status_into_db(True, bank_name, statement_id, inconsistent_transaction)
            return f"{statement_id}: Inconsistency due to extraction issue"

    insert_status_into_db(False, bank_name, statement_id, inconsistent_transaction)
    return f"{statement_id}: No extraction Issue Found"


def get_text_in_box(page, box):
    rect = fitz.Rect(box)
    words = page.get_textbox(rect)
    return words.split('\n')


def insert_status_into_db(is_extraction_issue, bank_name, statement_id, inconsistent_transaction):
    try:
        check_statement_exists_query = """SELECT 1 FROM statement_quality WHERE statement_id = %(statement_id)s"""
        values = {'statement_id': statement_id}

        statement_id_exists = DBConnection(QUALITY_DATABASE_NAME).execute_query(check_statement_exists_query, values)
        if not statement_id_exists:
            query = """
                INSERT INTO statement_quality (statement_id, bank_name, inconsistency_due_to_extraction, inconsistent_statement_data) 
                VALUES (%(statement_id)s, %(bank_name)s, %(inconsistency_due_to_extraction)s, %(inconsistent_transaction)s)
            """
        else:
            query = """
                         UPDATE statement_quality SET inconsistency_due_to_extraction = %(inconsistency_due_to_extraction)s, 
                              inconsistent_statement_data = %(inconsistent_transaction)s where statement_id = %(statement_id)s;
                    """
        values = {
            "inconsistency_due_to_extraction": is_extraction_issue,

            "inconsistent_transaction": Json(inconsistent_transaction),
            "statement_id": statement_id,
            "bank_name": bank_name
        }

        query_result = DBConnection(QUALITY_DATABASE_NAME).execute_query(query, values)

        return query_result
    except Exception as _:
        print(traceback.format_exc())
        return False
