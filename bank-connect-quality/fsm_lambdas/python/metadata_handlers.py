import re
import json, xlsxwriter, hashlib
from datetime import datetime

from python.aggregates import API_KEY
from python.aggregates import get_accounts_for_entity
from python.aggregates import get_transactions_for_account
from python.aggregates import get_identity_for_statement
from python.aggregates import generate_xlsx_report, get_country_for_statement, get_complete_identity_for_statement
from python.aggregates import get_salary_transactions_from_ddb
from python.aggregates import get_recurring_raw_from_ddb
from python.aggregates import get_extracted_frauds_list, get_final_account_category
from python.aggregates import get_statement_table_data
from python.bc_apis import get_bank_connect_predictors, get_bank_connect_score_from_lambda, get_bank_connect_monthly_analysis, get_bank_connect_eod_balances
from python.enrichment_regexes import check_and_get_everything
from library.excel_report.report_generator import generate_aggregated_overview
from library.excel_report.report_generator import calculate_monthwise_aggregated_balance
from library.excel_report.report_transactions import generate_aggregated_transactions
from library.excel_report.report_statement import statemenets_details
import threading
from python.configs import *
from python.configs import s3, BANK_CONNECT_REPORTS_BUCKET
from sentry_sdk import capture_exception
import copy
from python.formatted_data_handlers import get_frauds_separated

headers = {'Content-Type': 'application/json', 'x-api-key': API_KEY}
params = json.dumps({})

def check_necessity(key, bucket, data):
    try:
        result = s3.head_object(Bucket=bucket, Key=key)['Metadata']
        # timestamp with nanoseconds to datetime
        data_updated_at = datetime.fromtimestamp(int(data['updated_at'])/10**9)
        result_created_at = datetime.strptime(result['created_at'], "%Y-%m-%d %H:%M:%S.%f")
        if data_updated_at < result_created_at and result['statements_hash'] == data['statements_hash']:
            s3_path = s3.generate_presigned_url(
                'get_object', 
                Params={
                    'Bucket': bucket, 
                    'Key': key
                })
            print("Caching report url from s3.")
            return s3_path
    except:
        pass
    return None


def generate_file_name_with_given_pattern(file_name_string, data):
    """
        Replaces keys in the file_name_string with values from a dictionary, handling varying orders.
    """
    response = {
        "generated_file_name": "",
        "success": False
    }
    pattern = r"{([A-Za-z]+(?:_[A-Za-z0-9]+)*)}"  # Matches curly braces containing letters

    def replacer(match):
        key = match.group(1)
        return str(data.get(key, "&"))  # Use get with default

    file_name = re.sub(pattern, replacer, file_name_string)

    if "&" not in file_name:
        response["generated_file_name"] = "{}.xlsx".format(file_name)
        response["success"] = True
    return response


# New Report
def xlsx_report_handler(event, context):

    entity_id = str(event.get('entity_id'))
    excel_report_version = event.get('excel_report_version') or 'v1'
    excel_report_version = excel_report_version.lower()
    attempt_type_data = event.get('attempt_type_data', {})
    requested_account_id = event.get("account_id", None)
    adjusted_eod = event.get('adjusted_eod', False)
    is_sme = event.get('is_sme', False)
    to_remap_predictors = event.get('to_remap_predictors', False)
    ignore_self_transfer = event.get('ignore_self_transfer', False)
    to_reject_account = event.get('to_reject_account', False)
    caching_enabled = event.get("caching_enabled", False)
    excel_filename_format = event.get("excel_filename_format", None)
    session_dict = event.get('session_dict', {})
    statement_metadata = event.get('metadata', dict())

    print(f'recieved event  --> {event}')

    accounts = get_accounts_for_entity(entity_id, to_reject_account)

    scores = {}
    predictors = {}
    monthly_analysis = {}
    eod_balances = {}
    unadjusted_eod_balances = {}
    # bc_scores = get_bank_connect_score(entity_id)
    # computed_predictors = get_bank_connect_predictors(entity_id)

    print("generating report for entity id: {} with attempt type data: {}".format(entity_id, attempt_type_data))
    print(f"for this organisation, excel report version is {excel_report_version}")

    
    copied_event = copy.deepcopy(event)
    copied_event.pop('account_id', None)
    copied_event = dict(sorted(copied_event.items()))

    links = []
    for account in accounts:
        account_id = account.get('item_data').get('account_id')
        statements = account.get('item_data').get('statements')
        account_statement_metadata = statement_metadata.get(account_id, dict())
        statements.sort()
        statements_to_be_hashed = ", ".join(statements)+json.dumps(copied_event)
        statements_to_be_hashed = statements_to_be_hashed.encode('utf-8')

        hashed_statements = hashlib.md5(statements_to_be_hashed).hexdigest()
        if requested_account_id not in [None, ""] and requested_account_id!=account_id:
            continue
        print("Trying to generate xlsx report for account_id: ", account_id)
        # for perfios extracted pdf checks
        # we do not need to create the excel report for such cases
        # we'll be using the reports by perfios
        fraud_list, is_extracted_by_perfios = get_extracted_frauds_list(entity_id, account_id, statements)
        print("statements for entity id: {} and account id: {} are -> {}".format(entity_id, account_id, statements))
        
        # check if account is updated after excel generation
        s3_file_key = None
        if excel_filename_format:
            identity = account.get("item_data", {})
            if statements:
                identity = get_identity_for_statement(statements[0])
            file_name_response = generate_file_name_with_given_pattern(excel_filename_format, identity)
            if file_name_response.get("success", False):
                s3_file_key = file_name_response.get("generated_file_name")

        if not s3_file_key:
            s3_file_key = 'account_report_{}.xlsx'.format(account_id)
        updated_at = account.get('updated_at')
        account_data = {'updated_at': updated_at, 'statements_hash': hashed_statements}
        s3_path = check_necessity(s3_file_key, BANK_CONNECT_REPORTS_BUCKET, account_data)
        
        if s3_path is None:
            if not scores and not predictors and not monthly_analysis and not eod_balances:
                t1 = threading.Thread(target=get_bank_connect_score_from_lambda, args=(entity_id, scores, is_sme, to_reject_account))
                t1.start()
                t2 = threading.Thread(target=get_bank_connect_predictors, args=(entity_id, predictors, adjusted_eod, requested_account_id, to_remap_predictors, ignore_self_transfer, to_reject_account, caching_enabled))
                t2.start()
                t3 = threading.Thread(target=get_bank_connect_monthly_analysis, args=(entity_id, monthly_analysis, adjusted_eod, is_sme, ignore_self_transfer, to_reject_account, caching_enabled))
                t3.start()
                t4 = threading.Thread(target=get_bank_connect_eod_balances, args=(entity_id, eod_balances, adjusted_eod, is_sme, to_reject_account, caching_enabled, session_dict))
                t4.start()
                if adjusted_eod:
                    t5 = threading.Thread(target=get_bank_connect_eod_balances, args=(entity_id, unadjusted_eod_balances, not adjusted_eod, is_sme, to_reject_account, caching_enabled, session_dict))
                    t5.start()
                    t5.join()
                t1.join()
                t2.join()
                t3.join()
                t4.join()

            account_monthly_analysis = monthly_analysis.get(account_id, {})
            account_eod_balances = eod_balances.get(account_id, {})
            account_unadjusted_eod_balances = unadjusted_eod_balances.get(account_id, {})

            if statements:
                for statement_uuid in statements:
                    statement_identity = get_complete_identity_for_statement(statement_uuid)
                    txn_from_date = statement_identity.get('date_range', dict()).get('from_date', None)
                    if txn_from_date is not None:
                        txn_from_date = datetime.strptime(txn_from_date, '%Y-%m-%d').strftime('%d-%b-%y')
                    txn_to_date = statement_identity.get('date_range', dict()).get('to_date', None)
                    if txn_to_date is not None:
                        txn_to_date = datetime.strptime(txn_to_date, '%Y-%m-%d').strftime('%d-%b-%y')

                    if statement_uuid in account_statement_metadata.keys():
                        account_statement_metadata[statement_uuid]['txn_from_date'] = txn_from_date
                        account_statement_metadata[statement_uuid]['txn_to_date'] = txn_to_date
                    else:
                        account_statement_metadata[statement_uuid] = {
                            'txn_from_date':txn_from_date,
                            'txn_to_date':txn_to_date
                        }

                identity = get_identity_for_statement(statements[0])
                account_item_data = account.get('item_data', dict())
                identity['bank'] = account_item_data.get('bank')
                identity['ifsc'] = account_item_data.get('ifsc')
                identity['micr'] = account_item_data.get('micr')
                identity["account_opening_date"] = account_item_data.get("account_opening_date", None)

                identity['account_category'], _ = get_final_account_category(account_item_data.get('account_category', None), account_item_data.get('is_od_account', None), account_item_data.get('input_account_category', None),
                                            account_item_data.get('input_is_od_account', None))

                identity['salary_confidence'] = account_item_data.get('salary_confidence','')
                identity['account_id'] = account_id
                identity['od_limit'] = account_item_data.get('od_limit', None)
                identity['credit_limit'] = account_item_data.get('credit_limit', None)
                if identity['od_limit'] == None:
                    identity['od_limit'] = identity['credit_limit']
                if identity['credit_limit'] == None:
                    identity['credit_limit'] = identity['od_limit']

                identity['transaction_id'] = entity_id
                identity['applicant_id'] = session_dict.get('session_applicant_id', '')
                identity['mobile_number'] = account_item_data.get('phone_number')
                identity['pan'] = account_item_data.get('pan_number')
                identity['email'] = account_item_data.get('email')
                account_fraud_dict = get_frauds_separated(account_id, entity_id, to_reject_account=to_reject_account)
                fraud_status = 'VERIFIED'
                if account_fraud_dict.get('Metadata', False) or account_fraud_dict.get('Accounting', False):
                    fraud_status = 'FRAUD'
                elif account_fraud_dict.get('Behavioural', False) or account_fraud_dict.get('Transactional', False):
                    fraud_status = 'REFER'

                identity['fraud_status'] = fraud_status
                # adding the attempt types for this account id
                # also converting the list into string
                identity['attempt_types'] = ", ".join(attempt_type_data.get(account_id, []))
                transactions, hash_dict = get_transactions_for_account(entity_id, account_id)
                salary_transactions = get_salary_transactions_from_ddb(account_id)
                recurring_transactions = get_recurring_raw_from_ddb(account_id)
                
                identity['bc_score'] = scores.get(account_id, "")
                
                identity['missing_data'] = account.get('item_data', {}).get('missing_data', [])
                                
                country = get_country_for_statement(statements[0])

                if transactions:
                    # only create the excel report if this account was not extracted by perfios
                    check_and_get_everything(identity['bank'], country)
                    
                    s3_path = None
                    predictors_for_this_account = predictors.get(account_id, {})
                    if not is_extracted_by_perfios:
                        metadata = {'statements_hash': hashed_statements}
                        s3_path = generate_xlsx_report(account_id, transactions, identity, salary_transactions, recurring_transactions, fraud_list, predictors_for_this_account, account_monthly_analysis, account_eod_balances, excel_report_version, country, metadata=metadata, file_name=s3_file_key, unadjusted_eod_balances=account_unadjusted_eod_balances, account_statement_metadata=account_statement_metadata)
        if s3_path:
            links.append({'link': s3_path, 'account_id': account_id})
        
        # this also handles the case when some of the statements are extracted by Bank Connect - Probably by AA
        # for such cases both the reports Bank Connect's and Perfios will be showm
        if is_extracted_by_perfios:
            print("the account was perfios extracted - sending link as None")
            links.append({"link": None, "account_id": account_id})
        
        s3_path = None
        # but we need to create report when the same account was also extracted by aa or netbanking
        if ("AA" in attempt_type_data.get(account_id, []) or "Net Banking" in attempt_type_data.get(account_id, [])) and is_extracted_by_perfios and transactions:
            check_and_get_everything(identity['bank'], country)
            print("account id: {} had more attempts other than perfios".format(account_id))
            # Note: this will merge the fraud list from perfios too - can't do anything for that for now
            s3_path = generate_xlsx_report(account_id, transactions, identity, salary_transactions, recurring_transactions, fraud_list, predictors_for_this_account, account_monthly_analysis, account_eod_balances, excel_report_version, account_statement_metadata=account_statement_metadata)
            links.append({'link': s3_path, 'account_id': account_id})

    print("report links for entity id: {} -> {}".format(entity_id, links))
    return links

def aggregate_xlsx_report_handler(event, context):
    
    entity_id = str(event.get('entity_id'))
    aggregate_excel_report_version = event.get('aggregate_excel_report_version') or event.get('excel_report_version') or 'v1'
    aggregate_excel_report_version = aggregate_excel_report_version.lower()
    attempt_type_data = event.get('attempt_type_data', {})
    adjusted_eod = event.get('adjusted_eod', False)
    is_sme = event.get('is_sme', False)
    to_remap_predictors = event.get('to_remap_predictors', False)
    ignore_self_transfer = event.get('ignore_self_transfer', False)
    to_reject_account = event.get('to_reject_account', False)
    session_dict = event.get('session_dict', {})
    caching_enabled = event.get("caching_enabled", False)
    statement_metadata = event.get('metadata', dict())
    
    print(f'recieved event  --> {event}')
    
    accounts = get_accounts_for_entity(entity_id, to_reject_account)

    # we can do the below two things in an async fashion
    scores = {}
    predictors = {}
    monthly_analysis = {}
    eod_balances = {}
    unadjusted_eod_balances = {}
    t1 = threading.Thread(target=get_bank_connect_score_from_lambda, args=(entity_id, scores, is_sme, to_reject_account))
    t1.start()
    t2 = threading.Thread(target=get_bank_connect_predictors, args=(entity_id, predictors, adjusted_eod, None, to_remap_predictors, ignore_self_transfer, to_reject_account, caching_enabled))
    t2.start()
    t3 = threading.Thread(target=get_bank_connect_monthly_analysis, args=(entity_id, monthly_analysis, adjusted_eod, is_sme, ignore_self_transfer, to_reject_account, caching_enabled))
    t3.start()
    t4 = threading.Thread(target=get_bank_connect_eod_balances, args=(entity_id, eod_balances, adjusted_eod, is_sme, to_reject_account, caching_enabled, session_dict))
    t4.start()
    if adjusted_eod:
        t5 = threading.Thread(target=get_bank_connect_eod_balances, args=(entity_id, unadjusted_eod_balances, not adjusted_eod, is_sme, to_reject_account, caching_enabled, session_dict))
        t5.start()
        t5.join()
    t1.join()
    t2.join()
    t3.join()
    t4.join()
    # bc_scores = get_bank_connect_score(entity_id)
    # computed_predictors = get_bank_connect_predictors(entity_id)

    print("generating report for entity id: {} with attempt type data: {}".format(entity_id, attempt_type_data))
    print(f"for this organisation, aggregate excel report version is {aggregate_excel_report_version}")

    file_name = 'entity_report_{}.xlsx'.format(entity_id)
    file_path = '/tmp/{}'.format(file_name)
    aggregated_workbook = xlsxwriter.Workbook(file_path, {'strings_to_numbers': True})
    # Version v6 is for multi account report
    statement_details_sheet = None
    if aggregate_excel_report_version == 'v6':
        statement_details_sheet = aggregated_workbook.add_worksheet('Statement Details')
    aggregated_worksheet = aggregated_workbook.add_worksheet('Aggregated Overview')
    if aggregate_excel_report_version == 'v4':
        aggregated_xns_sheet = aggregated_workbook.add_worksheet('Aggregated Transactions')
        entity_transactions = []
    account_overview_dicts = []
    number = 0
    statement_details = {}
    for account in accounts:
        account_id = account.get('item_data').get('account_id')
        statements = account.get('item_data').get('statements')
        bank_name = account.get('item_data').get('bank')
        account_statement_metadata = statement_metadata.get(account_id, dict())

        if bank_name in ['phonepe_bnk']:
            continue
        
        print("Trying to generate xlsx report for account_id: ", account_id)
        # for perfios extracted pdf checks
        # we do not need to create the excel report for such cases
        # we'll be using the reports by perfios
        is_extracted_by_perfios = False

        print("statements for entity id: {} and account id: {} are -> {}".format(entity_id, account_id, statements))

        account_monthly_analysis = monthly_analysis.get(account_id, {})
        account_eod_balances = eod_balances.get(account_id, {})
        account_unadjusted_eod_balances = unadjusted_eod_balances.get(account_id, {})
        
        if statements:
            identity = {}
            for statement_uuid in statements:
                if not identity:
                    identity = get_identity_for_statement(statement_uuid)
                statement_identity = get_complete_identity_for_statement(statement_uuid)
                txn_from_date = statement_identity.get('date_range', dict()).get('from_date', None)
                if txn_from_date is not None:
                    txn_from_date = datetime.strptime(txn_from_date, '%Y-%m-%d').strftime('%d-%b-%y')
                txn_to_date = statement_identity.get('date_range', dict()).get('to_date', None)
                if txn_to_date is not None:
                    txn_to_date = datetime.strptime(txn_to_date, '%Y-%m-%d').strftime('%d-%b-%y')

                if statement_uuid in account_statement_metadata.keys():
                    statement_details[statement_uuid] = account_statement_metadata[statement_uuid]
                    statement_details[statement_uuid]['account_number'] = identity.get('account_number')
                    statement_details[statement_uuid]['bank_name'] = identity.get('bank_name')
                    statement_details[statement_uuid]['name'] = identity.get('name')
                    statement_details[statement_uuid]['txn_from_date'] = txn_from_date
                    statement_details[statement_uuid]['txn_to_date'] = txn_to_date
                    statement_details[statement_uuid]['status'] = 'success'
                    statement_details[statement_uuid]['failure_reason'] = None
                elif statement_uuid in statement_metadata.get('failed_account_id', {}):
                    statement_table_data = get_statement_table_data(statement_uuid)
                    statement_details[statement_uuid] = statement_metadata['failed_account_id'][statement_uuid]
                    statement_details[statement_uuid]['account_number'] = identity.get('account_number')
                    statement_details[statement_uuid]['bank_name'] = identity.get('bank_name')
                    statement_details[statement_uuid]['name'] = identity.get('name')
                    statement_details[statement_uuid]['txn_from_date'] = txn_from_date
                    statement_details[statement_uuid]['txn_to_date'] = txn_to_date
                    statement_details[statement_uuid]['status'] = 'failed'
                    statement_details[statement_uuid]['failure_reason'] = statement_table_data.get('message')
            
            identity['bank'] = account.get('item_data').get('bank')
            identity['ifsc'] = account.get('item_data').get('ifsc')
            identity['micr'] = account.get('item_data').get('micr')
            identity["account_opening_date"] = account.get('item_data').get("account_opening_date", None)

            identity['account_category'], _ = get_final_account_category(account.get('item_data').get('account_category', None), account.get('item_data').get('is_od_account', None), account.get('item_data').get('input_account_category', None),
                                        account.get('item_data').get('input_is_od_account', None))

            identity['salary_confidence'] = account.get('item_data').get('salary_confidence','')
            identity['account_id'] = account_id
            identity['od_limit'] = account.get('item_data').get('od_limit', None)
            identity['credit_limit'] = account.get('item_data').get('credit_limit', None)
            if identity['od_limit'] == None:
                identity['od_limit'] = identity['credit_limit']
            if identity['credit_limit'] == None:
                identity['credit_limit'] = identity['od_limit']

            # adding the attempt types for this account id
            # also converting the list into string
            identity['attempt_types'] = ", ".join(attempt_type_data.get(account_id, []))
            transactions, hash_dict = get_transactions_for_account(entity_id, account_id)
            if aggregate_excel_report_version == 'v4':
                entity_transactions.extend(transactions)
            salary_transactions = get_salary_transactions_from_ddb(account_id)
            recurring_transactions = get_recurring_raw_from_ddb(account_id)
            
            identity['bc_score'] = scores.get(account_id, "")
            
            identity['missing_data'] = account.get('item_data', {}).get('missing_data', [])
            
            country = get_country_for_statement(statements[0])

            if transactions:
                number += 1
                # only create the excel report if this account was not extracted by perfios
                check_and_get_everything(identity['bank'], country)
                
                fraud_list, is_extracted_by_perfios = get_extracted_frauds_list(entity_id, account_id, statements)
                predictors_for_this_account = predictors.get(account_id, {})
                if not is_extracted_by_perfios:
                    overview_dict = generate_xlsx_report(account_id, transactions, identity, salary_transactions, recurring_transactions, fraud_list, predictors_for_this_account, account_monthly_analysis, account_eod_balances, aggregate_excel_report_version, country, aggregated_workbook, str(number), unadjusted_eod_balances=account_unadjusted_eod_balances)
                    account_overview_dicts.append(overview_dict)
    link = []
    s3_path = None

    # If overview not extracted for the account or the account was perfios extracted
    if len(account_overview_dicts) == 0 or None in account_overview_dicts or is_extracted_by_perfios:
        return link
    
    data_in_vertical_format = True
    if aggregate_excel_report_version == 'v2':
        data_in_vertical_format = False

    # Create a aggregated overview of the overview of all the accounts
    try:
        aggregated_eod_balances = {}
        if aggregate_excel_report_version == 'v6':
            aggregated_eod_balances = calculate_monthwise_aggregated_balance(eod_balances)
        generate_aggregated_overview(aggregated_workbook, aggregated_worksheet, account_overview_dicts, data_in_vertical_format, aggregate_excel_report_version, aggregated_eod_balances)
        if aggregate_excel_report_version == 'v4':
            generate_aggregated_transactions(aggregated_workbook, aggregated_xns_sheet, entity_transactions, 'SME_REPORT', aggregate_excel_report_version)
    except Exception as e:
        capture_exception(e)
        print("Aggregated Overview failed due to -> ", e)
        
    if aggregate_excel_report_version == 'v6':
        statemenets_details(aggregated_workbook, aggregate_excel_report_version, {}, statement_details, statement_details_sheet)

    aggregated_workbook.close()

    try:
        s3_resource.Bucket(BANK_CONNECT_REPORTS_BUCKET).upload_file(file_path, file_name)

        if os.path.exists(file_path):
            os.remove(file_path)

        s3_path = s3.generate_presigned_url(
            'get_object', 
            Params={
                'Bucket': BANK_CONNECT_REPORTS_BUCKET, 
                'Key': file_name
            }
        )
        link=[{'link': s3_path}]
    except Exception as e:
        capture_exception(e)
        print("Aggregate Report failed due to -> ", e)

    print("Aggregate report link for entity id: {} -> {}".format(entity_id, link))
    return link