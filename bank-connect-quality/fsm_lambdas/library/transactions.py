from typing import Union
from library.extract_txns_fitz import get_transactions_using_fitz
from library.statement_plumber import get_transactions_using_plumber
from library.extract_txns_finvu_aa import get_transaction_channel_description_hash
from library.fraud import change_transaction_type, optimise_transaction_type
from library.finvu_aa_inconsistency_removal import remove_finvu_aa_inconsistency
from library.utils import amount_to_float, check_date, log_data
from library.date_utils import supported_formats
from datetime import datetime
from datetime import timedelta
from library.helpers.constants import BANKS_WITH_TRANSACTIONS_SPLIT_ENABLED


def format_date_from_finvu_transaction(finvu_transaction):
    finvu_transaction_value_date = finvu_transaction.get('valueDate', '')

    datetime_obj, _ = check_date(finvu_transaction_value_date)
    if not datetime_obj:
        print(f'With valueDate: {finvu_transaction_value_date}, exception occured')
        datetime_obj = format_aa_transaction_date(finvu_transaction['transactionTimestamp'])
    return datetime_obj

def format_aa_transaction_date(txn_date_string):
    resultant_datetime_obj = None

    for format in supported_formats:
        try:
            resultant_datetime_obj = datetime.strptime(txn_date_string, format)
            break
        except ValueError:
            pass

    # to get notified when a new format is found
    if resultant_datetime_obj is None:
        raise ValueError("new date format found for AA transactions, txn_date_string: {}".format(txn_date_string))

    return resultant_datetime_obj

def get_transaction(transaction_input_payload, local_logging_context, LOGGER):
    
    country = transaction_input_payload.get('country', 'IN')
    bank = transaction_input_payload.get('bank')
    trans_bbox = transaction_input_payload.get('trans_bbox', [])
    page_number = transaction_input_payload.get('page_number')

    log_data(message=f"Received {len(trans_bbox)} number of templates for trans_bbox", LOGGER=LOGGER, local_logging_context=local_logging_context, log_type="debug")

    plumber_transactions_output_dict = {}
    
    # forcing country to be in upper case
    if isinstance(country, str):
        country = country.upper()
        log_data(message=f"Country for this statement is {country}", LOGGER=LOGGER, local_logging_context=local_logging_context, log_type="debug")
    
    last_page_regex = transaction_input_payload.get('last_page_regex', [])
    account_delimiter_regex = transaction_input_payload.get('account_delimiter_regex', [])
    for l_template in last_page_regex:
        is_present_in_account_regex = False
        for a_template in account_delimiter_regex:
            if l_template.get('regex') == a_template.get('regex'):
                is_present_in_account_regex = True
                break
        l_template['is_present_in_account_regex'] = is_present_in_account_regex
        
    plumber_banks_list = ["federal", "alrajhi", "ncb", "india_post"]
    plumber_trans_bbox = []
    if bank in plumber_banks_list:
        plumber_trans_bbox = [_ for _ in trans_bbox if _.get('table_setting')]
        trans_bbox = [_ for _ in trans_bbox if _ not in plumber_trans_bbox]

    # Do not change the order of this list
    page_number_list = [page_number]
    if bank in BANKS_WITH_TRANSACTIONS_SPLIT_ENABLED:
        page_number_list = [page_number+1,page_number]

    transaction_input_payload['original_page_num'] = page_number
    transaction_input_payload['unused_raw_txn_rows_from_second_page'] = {'raw_rows':[],'transaction_rows':[]}

    for current_page_number in page_number_list:
        transaction_input_payload['page_number'] = current_page_number
        if bank in plumber_banks_list:
            transaction_input_payload['trans_bbox'] = plumber_trans_bbox
            plumber_transactions_output_dict = get_transactions_using_plumber(transaction_input_payload)

            if bank in ("federal", "india_post"):
                transaction_input_payload['trans_bbox'] = trans_bbox
                transaction_output_dict = get_transactions_using_fitz(transaction_input_payload, local_logging_context, LOGGER)
                if current_page_number == page_number+1:
                    transaction_input_payload['unused_raw_txn_rows_from_second_page'] = transaction_output_dict.get('unused_raw_txn_rows_from_starting', {'raw_rows':[],'transaction_rows':[]})
            
            if len(plumber_transactions_output_dict.get('transactions', [])) > len(transaction_output_dict.get('transactions', [])):
                transaction_output_dict = plumber_transactions_output_dict

        else:
            transaction_output_dict = get_transactions_using_fitz(transaction_input_payload, local_logging_context, LOGGER)
            if current_page_number == page_number+1:
                transaction_input_payload['unused_raw_txn_rows_from_second_page'] = transaction_output_dict.get('unused_raw_txn_rows_from_starting', {'raw_rows':[],'transaction_rows':[]})
    
    return transaction_output_dict


def get_balance_from_finvu_aa_transaction(finvu_aa_txn) -> Union[float, None]:
    possible_balance_keywords = ["currentBalance", "balance"]
    resultant_balance = None

    for key in possible_balance_keywords:
        try:
            resultant_balance = finvu_aa_txn[key]
            break
        except Exception as e:
            pass
    
    if resultant_balance:
        resultant_balance = amount_to_float(resultant_balance)
    return resultant_balance


def get_amount_from_finvu_aa_transaction(finvu_aa_txn):
    possible_amount_keywords = ["amount"]
    resultant_amount = None

    for key in possible_amount_keywords:
        try:
            resultant_amount = float(finvu_aa_txn[key])
            break
        except:
            pass
    
    return resultant_amount



# --- Below are the two types of sample data
# {
#     amount: '10341.1', 
#     balance: '10341.1', 
#     mode: 'OTHERS', 
#     narration: 'Tran. For Principal Amt                           ', 
#     reference: '                ', 
#     transactionDateTime: '2022-11-18T18:30:00Z', 
#     txnId: ' M1250152', 
#     type: 'TDS', 
#     valueDate: '2022-11-19'
# }

# OR --- 

# {
#     "type": "DEBIT",
#     "mode": "OTHERS",
#     "amount": 389.0,
#     "currentBalance": "19943.98",
#     "transactionTimestamp": "2022-06-30T18:30:00Z",
#     "valueDate": "2022-07-01",
#     "txnId": "S93384957",
#     "narration": "UPI/AMAZON SELLER S/218254592112/UPI Collect",
#     "reference": "UPI-218263987882"
# }

# OR ---

# {
#     "amount": 11.0, 
#     "currentBalance": '1428.62', 
#     "mode": 'UPI', 
#     "narration": 'UPI/311621188685/141916/UPI/paytmqr281005050101', 
#     "reference": 'NA', 
#     "transactionTimestamp": '2023-04-26T08:49:16.000+00:00', 
#     "txnId": 'S79458351', 
#     "type": 'DEBIT', 
#     "valueDate": '2023-04-26'
# }

def calculate_balance_from_txns(transactions_list: list, count_of_txns_current_batch: int, index: int, transaction: dict) -> Union[float, None]:
    balance_from_txns = get_balance_from_finvu_aa_transaction(transaction)
    if isinstance(balance_from_txns, float):
        return balance_from_txns

    # handle this case by self calculating the missing balance based on the neighbor txns
    idx_to_compare = index + 1
    if count_of_txns_current_batch > 1:
        # decide the index to compare for last transaction
        if index == count_of_txns_current_batch-1:
            idx_to_compare = index - 1

        balance_from_neighbor_txns = get_balance_from_finvu_aa_transaction(transactions_list[idx_to_compare])
        if not isinstance(balance_from_neighbor_txns, float):
            return None

        # current txn time is greater than the next txn
        if format_aa_transaction_date(transaction["transactionTimestamp"]) > format_aa_transaction_date(transactions_list[idx_to_compare]["transactionTimestamp"]):
            # the current txn is newer than idx_to_compare txn
            current_txn_amount = get_amount_from_finvu_aa_transaction(transaction)
            if current_txn_amount is  None:
                return None
            if transaction.get("type") is None:
                return None
            if transaction["type"].lower() == "credit":
                return balance_from_neighbor_txns + current_txn_amount
            elif transaction["type"].lower() == "debit":
                return balance_from_neighbor_txns - current_txn_amount

        # current txn time is lesser than the next txn
        elif format_aa_transaction_date(transaction["transactionTimestamp"]) < format_aa_transaction_date(transactions_list[idx_to_compare]["transactionTimestamp"]):
            # the current txn is older than idx_to_compare txn
            neighbor_txn_amount = get_amount_from_finvu_aa_transaction(transactions_list[idx_to_compare])
            if neighbor_txn_amount is None:
                return None
            if transactions_list[idx_to_compare]["type"].lower() == "credit":
                return balance_from_neighbor_txns - neighbor_txn_amount
            elif transactions_list[idx_to_compare]["type"].lower() == "debit":
                return balance_from_neighbor_txns + neighbor_txn_amount

def get_transactions_finvu_aa(transactions_list, bank, name, session_date_range: dict):
    """
    This method takes in transactions list recieved from FinVu AA
    And returns back list of transactions accoring to Bank Connect Schema
    With extra parameters
    """

    # grabbing all the possible parameters from finvu aa data
    # according to bc schema
    transactions_list_finvu_data = []

    count_of_txns_current_batch = len(transactions_list)

    for idx, transaction in enumerate(transactions_list):
        finvu_transaction = {
            "transaction_type": transaction["type"].lower(),
            "transaction_note": transaction.get("narration", ""),
            "amount": get_amount_from_finvu_aa_transaction(transaction),
            "date": datetime.strftime(format_date_from_finvu_transaction(transaction), "%Y-%m-%d %H:%M:%S")
        }
        
        finvu_transaction["balance"] = calculate_balance_from_txns(transactions_list, count_of_txns_current_batch, idx, transaction)
        ## TODO: Improve the logic of extrapolation of balance when found empty.
        if finvu_transaction.get("balance") is None and isinstance(session_date_range, dict) and session_date_range.get('from_date', None) is not None and session_date_range.get('to_date', None) is not None:
            session_from_date = datetime.strptime(session_date_range.get('from_date'), '%d/%m/%Y')
            session_to_date = datetime.strptime(session_date_range.get('to_date'), '%d/%m/%Y') + timedelta(days=1)
            tmp_date = format_date_from_finvu_transaction(transaction)
            if isinstance(tmp_date, datetime) and (tmp_date < session_from_date or tmp_date >= session_to_date):
                finvu_transaction["balance"] = 0
        if finvu_transaction.get("balance") is None:
            return [], "empty balance"

        transactions_list_finvu_data.append(finvu_transaction)
    
    for i in range(len(transactions_list_finvu_data)):
        transactions_list_finvu_data[i]['optimizations'] = []
    
    if any(transaction.get('amount', 0) < 0 for transaction in transactions_list_finvu_data):
        transactions_list_finvu_data, _, _, _, _ = change_transaction_type(transactions_list_finvu_data)
    transactions_list_finvu_data, _, _, _ = optimise_transaction_type(transactions_list_finvu_data)

    # using this new list for further calculations
    final_transactions_list = get_transaction_channel_description_hash(transactions_list_finvu_data, bank, name)

    # remove unwanted inconsistency
    final_transactions_list = remove_finvu_aa_inconsistency(final_transactions_list, bank)

    # converting this list into df
    return final_transactions_list, None


def get_account_category_from_transactions(account_transactions:list) -> Union[str, None]:
    sorted_data = sorted(account_transactions, key=lambda x: datetime.strptime(x['date'], '%Y-%m-%d %H:%M:%S'), reverse=True)

    account_category = None
    for transaction in sorted_data:
        transaction_category = transaction.get('category')
        transaction_type = transaction.get('transaction_type')
        transaction_channel = transaction.get('transaction_channel')

        if transaction_category is not None:
            if 'salary'.lower() in transaction_category.lower() and transaction_channel != 'upi':
                if transaction_type == 'credit':
                    if account_category is None:
                        account_category = 'individual'
                else:
                    if account_category is None:
                        account_category = 'corporate'

            elif transaction_category == 'Interest' and transaction_type == 'credit':
                if account_category is None:
                    account_category = 'individual'
                
            elif transaction_category == 'Interest Charges' and transaction_type == 'debit':
                if account_category is None:
                    account_category = 'corporate'

            elif transaction_category == 'Below Min Balance':
                if account_category is None:
                    account_category = 'individual'
                
            elif transaction_category == 'Personal Loan':
                if account_category is None:
                    account_category = 'individual'
                
            elif transaction_category == 'Business Loan':
                if account_category is None:
                    account_category = 'corporate'
                
            elif transaction_category == 'Auto Loan':
                if account_category is None:
                    account_category = 'corporate'
        
        if account_category is not None:
            break
        
    return account_category
