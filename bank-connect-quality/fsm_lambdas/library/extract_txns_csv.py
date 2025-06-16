import re
import csv
import warnings
import pandas as pd

from library.extract_txns_fitz import balance_date_rows
from library.statement_plumber import transaction_rows, map_correct_columns
from library.transaction_channel import get_transaction_channel
from library.fraud import transaction_balance_check
from library.transaction_description import get_transaction_description
from library.utils import add_hash_to_transactions_df


warnings.simplefilter(action = "ignore", category = FutureWarning)
pd.options.mode.chained_assignment = None


# regex which removes any special charcters and 
# keeps only characters and digits in any language
# it also keeps spaces
only_char_digit_regex = re.compile(r"[^\w]+", re.UNICODE)

# spaces and new lines regex
spaces_new_line_regex = re.compile("\\s*")

# possible `date` header texts
possible_date_headers = ["txndate", "date", "valuedate", "transactiondate"]

# possible `transaction_note` header texts
possible_transaction_note_headers = ["description", "narration", "transactionnote", "note", "particulars", "narrationtext"]

# possible `debit` header texts
possible_debit_headers = ["debit", "debitamount", "withdrawal", "withdrawalamount"]

# possible `credit` header texts
possible_credit_headers = ["credit", "creditamount", "deposit", "depositamount"]

# possible `balance` header texts
possible_balance_headers = ["balance", "balanceamount", "runningbalance"]

# possible `balance_type` header texts
possible_balance_type_headers = ["balancetype", "crdr", "drcr"]

# possible `amount` header texts
possible_amount_headers = ["amount", "transactionamount", "txnamount"]

# possible `transaction_type` header texts
possible_transaction_type_headers = ["txntype", "transactiontype", "crdr", "drcr"]

# possible `chq_num` header texts [Reference number is combined with this only]
possible_chq_num_headers = ["chequenumber", "chequenum", "referenceno", "refno", "refnum", "referencenumber", "chqrefnumber","cheque"]

possible_amount_number = ['AccountStatementfor']
def get_transactions_from_csv(path, bank):
    """
    this function return a list of transactions processed out of a csv file
    """

    # read csv file as list of list because of variable number of columns
    try:
        csv_data = None
        with open(path, "r") as csv_file:
            reader = csv.reader(csv_file)
            csv_data = list(reader)
    except:
        print("Could not open/read the CSV file")

    # print(csv_data)

    # `csv_data` is a list of lists we need to
    # predict the template with index of columns 
    predicted_template = None
    header_index = None
    for i, row in enumerate(csv_data[:20]):
        predicted_template = if_header_row_predict_template(row)
        if isinstance(predicted_template, dict):
            # we found a template
            header_index = i
            break

    # checking if we found the header
    if header_index == None:
        print("Invalid CSV, could not predict template")
        return []

    # here means we know the template
    # we can now map the columns and make the transactions dict
    
    final_transactions_list = []

    # iterate from header index to last to create a list of list
    transaction_matrix = []
    for row in csv_data[header_index + 1:]:
        temp_single_txn = []
        for attribute, index in predicted_template.items():
            temp_single_txn.append(row[index])
        transaction_matrix.append(temp_single_txn)

    # create dataframe from the transaction matrix
    txn_df = pd.DataFrame(transaction_matrix)
    txn_df.columns = predicted_template.keys()

    balance_date_rows_csv = balance_date_rows(txn_df)
    transaction_rows_csv, _ = transaction_rows(balance_date_rows_csv, bank)

    transaction_list = []
    transaction_list.extend(
        transaction_rows_csv.apply(
            lambda row: map_correct_columns(row, bank, "IN"), axis=1
        )
    )

    txn_df = pd.DataFrame(transaction_list)
    # print(txn_df.to_string())

    for column in txn_df:
        if column in ["amount", "balance"]:
            txn_df[column] = txn_df[column].fillna(0)
        else:
            txn_df[column] = txn_df[column].fillna("")

    if txn_df.shape[0] > 0:
        txn_df = txn_df[((txn_df["transaction_type"] == "credit") | (txn_df["transaction_type"] == "debit")) & (abs(txn_df["amount"]) > 0)]

    txn_df = get_transaction_channel(txn_df, bank)

    if txn_df.shape[0] > 0:
        num_note_not_captured = txn_df[txn_df["transaction_note"] == ""].shape[0]
        num_channel_not_captured = txn_df[txn_df["transaction_channel"] == "Other"].shape[0]
    else:
        num_note_not_captured = 10000
        num_channel_not_captured = 10000
    
    is_fraud = transaction_balance_check(txn_df.to_dict("records"), bank)

    if txn_df.shape[0] > len(final_transactions_list):
        final_transactions_list = txn_df.to_dict("records")
        fraud_flag = is_fraud
        num_note_not_captured_final = num_note_not_captured
        num_channel_not_captured_final = num_channel_not_captured

    txn_df = pd.DataFrame(final_transactions_list)
    txn_df = get_transaction_description(txn_df)
    txn_df = add_hash_to_transactions_df(txn_df)
    final_transactions_list = txn_df.to_dict("records")

    print("TOTAL TXNS FOUND -> ", len(final_transactions_list))
    return final_transactions_list


def if_header_row_predict_template(row):
    """
    row -> it is a list of values which might be the header
    header helps us to figure out which column represents what

    which columns are must to be present in csv:
        -> date
        -> transaction_note
        -> debit and credit OR amount and transaction_type OR amount with sign
        -> balance
    """

    columns_found_status = {
        "date": False,
        "transaction_note": False,
        "debit": False,
        "credit": False,
        "balance": False,
        "balance_type": False,
        "amount": False,
        "transaction_type": False,
        "chq_num": False
    }

    index_of_columns = {
        "date": None,
        "transaction_note": None,
        "debit": None,
        "credit": None,
        "balance": None,
        "balance_type": None,
        "amount": None,
        "transaction_type": None,
        "chq_num": None
    }

    # check presence of column headers
    for i, column in enumerate(row):
        # print(column)

        # date
        if not columns_found_status["date"] and is_that_column(possible_date_headers, column):
            columns_found_status["date"] = True
            index_of_columns["date"] = i
            continue

        # transaction_note
        if not columns_found_status["transaction_note"] and is_that_column(possible_transaction_note_headers, column):
            columns_found_status["transaction_note"] = True
            index_of_columns["transaction_note"] = i
            continue

        # debit
        if not columns_found_status["debit"] and is_that_column(possible_debit_headers, column):
            columns_found_status["debit"] = True
            index_of_columns["debit"] = i
            continue

        # credit
        if not columns_found_status["credit"] and is_that_column(possible_credit_headers, column):
            columns_found_status["credit"] = True
            index_of_columns["credit"] = i
            continue

        # balance
        if not columns_found_status["balance"] and is_that_column(possible_balance_headers, column):
            columns_found_status["balance"] = True
            index_of_columns["balance"] = i
            # balance_type should be the next column if present
            if (i+1) < len(row):
                if not columns_found_status["balance_type"] and is_that_column(possible_balance_type_headers, row[i+1]):
                    columns_found_status["balance_type"] = True
                    index_of_columns["balance_type"] = i+1
            continue

        # amount
        if not columns_found_status["amount"] and is_that_column(possible_amount_headers, column):
            columns_found_status["amount"] = True
            index_of_columns["amount"] = i
            # transaction_type should be the previous or next column
            if (i+1) < len(row):
                # check next column
                if not columns_found_status["transaction_type"] and is_that_column(possible_transaction_type_headers, row[i+1]):
                    columns_found_status["transaction_type"] = True
                    index_of_columns["transaction_type"] = i+1
                    continue
            if (i-1) >= 0:
                # check previous column
                if not columns_found_status["transaction_type"] and is_that_column(possible_transaction_type_headers, row[i-1]):
                    columns_found_status["transaction_type"] = True
                    index_of_columns["transaction_type"] = i-1
                    continue
            else:
                # there is no transaction type in columns
                # we need to figure out using sign of amount
                pass
            continue

        # chq_num
        if not columns_found_status["chq_num"] and is_that_column(possible_chq_num_headers, column):
            columns_found_status["chq_num"] = True
            index_of_columns["chq_num"] = i
            continue
        
    # print(columns_found_status, "\n", index_of_columns)

    predicted_template = {}
    date_check = False
    transaction_note_check = False
    credit_debit_check = False
    balance_check = False

    # date
    if columns_found_status["date"]:
        predicted_template["date"] = index_of_columns["date"]
        date_check = True

    # transaction_note
    if columns_found_status["transaction_note"]:
        predicted_template["transaction_note"] = index_of_columns["transaction_note"]
        transaction_note_check = True

    # debit and credit (separate colums)
    if columns_found_status["debit"] and columns_found_status["credit"]:
        # we have debit and credit
        predicted_template["debit"] = index_of_columns["debit"]
        predicted_template["credit"] = index_of_columns["credit"]
        credit_debit_check = True
    elif columns_found_status["amount"] and columns_found_status["transaction_type"]:
        # we have amount and transaction type
        predicted_template["amount"] = index_of_columns["amount"]
        predicted_template["transaction_type"] = index_of_columns["transaction_type"]
        credit_debit_check = True
    elif columns_found_status["amount"]:
        # only amount is present means we will check for (cr/dr OR +/-) to figure out type of transaction
        predicted_template["amount"] = index_of_columns["amount"]
        credit_debit_check = True
    else:
        # we did not found any way of figuring out the type of transaction
        return False

    # balance
    if columns_found_status["balance"] and columns_found_status["balance_type"]:
        # we have both balance and balance_type
        predicted_template["balance"] = index_of_columns["balance"]
        predicted_template["balance_type"] = index_of_columns["balance_type"]
        balance_check = True
    elif columns_found_status["balance"]:
        # we only have balance
        # we will check for cr/dr OR +/- to figure out type of balance
        predicted_template["balance"] = index_of_columns["balance"]
        balance_check = True
    else:
        # we did not found any way of figuring out balance 
        return False

    # chq_ref_num
    if columns_found_status["chq_num"]:
        # we also have a cheque num / ref num / both in one column
        predicted_template["chq_num"] = index_of_columns["chq_num"]

    print("PREDICTED TEMPLATE -> ", predicted_template)

    if date_check and transaction_note_check and credit_debit_check and balance_check:
        # means we have got all we need for a transaction
        # we return the predicted template
        return predicted_template

    return False

def is_that_column(possible_texts_list, column_to_check):
    temp_col = column_to_check
    temp_col = spaces_new_line_regex.sub("", temp_col)
    temp_col = only_char_digit_regex.sub("", temp_col)
    for text in possible_texts_list:
        if text.lower() == temp_col.lower():
            return True
    return False

def get_transactions_list_of_lists_csv(path, bank):
    """
    this function returns a list of lists read from the csv file
    basically dividing the whole long list into smaller chunks
    containing 25-30 transactions each
    """

    all_transactions = get_transactions_from_csv(path, bank)

    all_txns_list_of_lists = []

    if len(all_transactions) <= 30:
        all_txns_list_of_lists.append(all_transactions)
    else:
        all_txns_list_of_lists = [all_transactions[i:i+25] for i in range(0, len(all_transactions), 25)]
    return all_txns_list_of_lists

def extract_identity_csv(path,bankname):
    if(bankname == "yesbnk"):
        # read csv file as list of list because of variable number of columns
        try:
            csv_data = None
            with open(path, "r") as csv_file:
                reader = csv.reader(csv_file)
                csv_data = list(reader)
        except:
            print("Could not open/read the CSV file")

        for i, column in enumerate(csv_data[:20]):
            if(len(column)):
                temp = column[0].split(":")
                if is_that_column(possible_amount_number,temp[0]):
                    return {'identity': {'account_number':temp[1]}, 'date_range': {'from_date': None, 'to_date': None}}
                    break
        return {'identity': {}, 'date_range': {'from_date': None, 'to_date': None}}
    else:
        return {'identity': {}, 'date_range': {'from_date': None, 'to_date': None}}