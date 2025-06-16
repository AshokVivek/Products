import os
import json
import warnings
import pandas as pd

from library.fitz_functions import read_pdf
from library.fraud import transaction_balance_check
from library.statement_plumber import transaction_rows, map_correct_columns
from library.transaction_channel import get_transaction_channel
from library.transaction_description import get_transaction_description
from library.utils import add_hash_to_transactions_df, convert_pandas_timestamp_to_date_string
from library.extract_txns_fitz import balance_date_rows, remove_opening_balance


warnings.simplefilter(action = "ignore", category = FutureWarning)
pd.options.mode.chained_assignment = None


def get_transactions_for_karur(path, bank, password, page_num, name, table, account_category=''):
    """
    Get transactions for the OCR table data
    of karur bank statement.
    """
    doc = read_pdf(path, password)  # gets fitz document object
    removed_opening_balance_date = None
    if isinstance(doc, int):
        # password incorrect or file doesn't exist or file is not a pdf
        return [], removed_opening_balance_date
    num_pages = doc.page_count  # get the page count
    if page_num < num_pages:  # check whether page_num provided doesn't exceed num_pages
        file_path = 'library/bank_data/' + bank + '.json'
        if os.path.exists(file_path):
            with open(file_path, 'r') as data_file:
                try:
                    extraction_parameter = json.load(
                        data_file).get('trans_bbox', [])
                except ValueError:
                    print("Invalid JSON file\nPlease check")
                    extraction_parameter = []
                except Exception as e:
                    print(e)
                    extraction_parameter = []
                finally:
                    data_file.close()
        else:
            print("Incorrect bank name")
            extraction_parameter = []
        all_transactions, removed_date_opening_balance = get_advance_transactions_each_page(bank, extraction_parameter, table, account_category)
        removed_opening_balance_date = removed_date_opening_balance

        if all_transactions:
            transaction_df = pd.DataFrame(all_transactions)
            transaction_df = get_transaction_description(transaction_df, name)
            transaction_df = add_hash_to_transactions_df(transaction_df)
            transaction_list = transaction_df.to_dict('records')
            return transaction_list, removed_opening_balance_date

    return [], removed_opening_balance_date

def get_advance_transactions_each_page(bank, extraction_parameter, table, account_category=''):
    return_data_page = []
    for index, each_parameter in enumerate(extraction_parameter):
        table = table
        #print(table)
        columns = each_parameter['column']
        footer = each_parameter.get("footer", [False,None])
        special_symbol = each_parameter.get("special_symbol", False)
        actual_table = table
        txn_df = pd.DataFrame(actual_table)
        for each_column_list in columns:
            transaction_list = []
            if special_symbol:
                txn_df = txn_df.replace( '\|', '', regex=True)
            ######################
            # remove n arrays (footer) from actual_table
            if footer[0]:
                actual_table = actual_table[:len(actual_table) - footer[1]]
                # print('--- df after removeing footer -- ')
                # print(actual_table)
            #######################
            if txn_df.shape[1] == len(each_column_list):
                txn_df.columns = each_column_list
                balance_date_rows_page = balance_date_rows(txn_df)
                transaction_rows_page,_ = transaction_rows(
                    balance_date_rows_page, bank)
                transaction_list.extend(transaction_rows_page.apply(
                    lambda row: map_correct_columns(row, bank, "IN"), axis=1))
                transaction_df = pd.DataFrame(transaction_list)

                for col in transaction_df:
                    if col in ['amount', 'balance']:
                        transaction_df[col] = transaction_df[col].fillna(0)
                    else:
                        transaction_df[col] = transaction_df[col].fillna('')

                if transaction_df.shape[0] > 0:
                    transaction_df = transaction_df[((transaction_df['transaction_type'] == 'credit') | (
                        transaction_df['transaction_type'] == 'debit')) & (abs(transaction_df['amount']) > 0)]

                transaction_df = get_transaction_channel(transaction_df, bank, "IN", account_category) # since karur is based only on IN, the country flag is passed as IN

                if transaction_df.shape[0] > 0:
                    num_note_not_captured = transaction_df[transaction_df['transaction_note'] == ''].shape[0]
                    num_channel_not_captured = transaction_df[
                        transaction_df['transaction_channel'] == 'Other'].shape[0]
                else:
                    num_note_not_captured = 10000
                    num_channel_not_captured = 10000

                is_fraud = transaction_balance_check(transaction_df.to_dict('records'), 'karur')
                if transaction_df.shape[0] > len(return_data_page):
                    return_data_page = transaction_df.to_dict('records')
                    fraud_flag = is_fraud
                    num_note_not_captured_final = num_note_not_captured
                    num_channel_not_captured_final = num_channel_not_captured
                elif (transaction_df.shape[0] == len(return_data_page)) and (is_fraud is None):
                    if fraud_flag or num_note_not_captured < num_note_not_captured_final or (
                            num_channel_not_captured < num_channel_not_captured_final):
                        return_data_page = transaction_df.to_dict('records')
                        fraud_flag = is_fraud
                        num_note_not_captured_final = num_note_not_captured
                        num_channel_not_captured_final = num_channel_not_captured
            
    return_data_page, removed_date_opening_balance = remove_opening_balance(return_data_page)
    removed_date_opening_balance = convert_pandas_timestamp_to_date_string(removed_date_opening_balance)
    return return_data_page, removed_date_opening_balance