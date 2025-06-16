import os
import boto3
import json
import logging
from botocore.config import Config
from enum import Enum

class ImproperlyConfigured(Exception):
    """FSM-Lambdas is somehow improperly configured"""
    pass

def get_env_var(name):
    try:
        temp_val = os.environ.get(name)
        if temp_val in [None, ""]:
            raise KeyError
        return temp_val
    except KeyError:
        raise ImproperlyConfigured(f"Environment variable: `{name}` was not set.")

NUMBER_OF_RETRIES = 5

CURRENT_STAGE = get_env_var("CURRENT_STAGE")
CURRENT_STAGE = CURRENT_STAGE.lower()
REGION = get_env_var("REGION")
AWS_ACCOUNT_ID = get_env_var("AWS_ACCOUNT_ID")

##########################################################################################
    
##########################################################################################
# LOGGER INITIALIZATION
LAMBDA_LOGGER = logging.getLogger()

IS_RAMS_SERVER = os.environ.get("IS_RAMS_SERVER", "false") in ["true", "1", "t"] # this needs to be removed in next PR and is present to be backward compatible
IS_SERVER = os.environ.get("IS_SERVER", "false") in ["true", "1", "t"] # this will be transformed to a `get_env_var` function due to backward compaitibility issue

if IS_SERVER or IS_RAMS_SERVER:
    from context.log_config import LOGGER
    LAMBDA_LOGGER = LOGGER

##########################################################################################
# boto3 definitions and Imports
s3 = boto3.client('s3', region_name=REGION)

endpointURL = s3.meta.endpoint_url
s3 = boto3.client('s3', region_name=REGION, endpoint_url=endpointURL) 
# had to do this due to a very disapointing bug in boto3, s3 client.

s3_resource = boto3.resource('s3', region_name=REGION)
sqs_client = boto3.client("sqs", region_name=REGION)
lambda_client = boto3.client('lambda', region_name=REGION)
cloud_watch_client = boto3.client('cloudwatch', region_name=REGION)
dynamodb = boto3.client("dynamodb",region_name=REGION)
ddb_config = Config(
    retries = {
        'max_attempts': 50,
        'mode': 'adaptive'
    }
)
dynamo_db = boto3.resource('dynamodb', config=ddb_config, region_name=REGION)
streamClient = boto3.client("firehose", region_name=REGION) 
##########################################################################################

####################################################################################################################
# kinesis stream
TRANSACTIONS_STREAM_NAME = f"bank-connect-clickhouse-transactions-{CURRENT_STAGE}"
CC_TRANSACTIONS_STREAM_NAME = f"bank-connect-clickhouse-cc-transactions-{CURRENT_STAGE}"
CC_IDENTITY_STREAM_NAME = f"bank-connect-clickhouse-cc-identity-{CURRENT_STAGE}"
ACCOUNTS_STREAM_NAME = f"bank-connect-clickhouse-accounts-{CURRENT_STAGE}"
DISPARITIES_STREAM_NAME = f"bank-connect-clickhouse-disparities-{CURRENT_STAGE}"
METADATA_STREAM_NAME = f"bank-connect-clickhouse-statement-metadata-{CURRENT_STAGE}"
TCAP_CUSTOMERS_STREAM_NAME = f"bank-connect-clickhouse-tcap-customers-{CURRENT_STAGE}"
TCAP_CALL_DETAILS_STREAM_NAME = f"bank-connect-clickhouse-tcap-call-details-{CURRENT_STAGE}"
TCAP_RECURRING_AA_PULLS_STREAM_NAME = f"bank-connect-clickhouse-tcap-recurring-aa-pulls-{CURRENT_STAGE}"
IDENTITY_STREAM_NAME = f"bank-connect-clickhouse-identity-{CURRENT_STAGE}"
####################################################################################################################

####################################################################################################################
# constant error messages
SIZE_EXCEEDED_ERROR_MESSAGE = "Member must have length less than or equal to 1024000"
REQUEST_TOO_LARGE_ERROR_MESSAGE = "An error occurred (413) when calling the PutRecord operation"
####################################################################################################################

##########################################################################################
# URLs, API KEYs and SECRETs
DJANGO_BASE_URL = get_env_var('DJANGO_BASE_URL')
API_KEY = get_env_var('INTERNAL_API_KEY')

NANONETS_API_KEY = get_env_var('NANONETS_API_KEY')
NANONETS_MODEL_ID = get_env_var('NANONETS_MODEL_ID')

RECURRING_MICROSERVICE_TOKEN = get_env_var('RECURRING_MICROSERVICE_TOKEN')
RECURRING_MICROSERVICE_URL = get_env_var("RECURRING_MICROSERVICE_URL")

QUALITY_ACCESS_CODE = get_env_var("BANK_CONNECT_QUALITY_SECRET")
BANK_CONNECT_QUALITY_PRIVATE_IP = get_env_var("BANK_CONNECT_QUALITY_PRIVATE_IP")
CATEGORIZE_RS_PRIVATE_IP = get_env_var("CATEGORIZE_RS_PRIVATE_IP")

INTERNAL_QUALITY_CHECK_URL = get_env_var("INTERNAL_QUALITY_CHECK_URL")
UPDATE_STATE_FAN_OUT_INFO_URL = f"{DJANGO_BASE_URL}/bank-connect/v1/internal_admin/get_info_for_update_state_fan_out/"
##########################################################################################

##########################################################################################
# S3 Buckets

BANK_CONNECT_REPORTS_BUCKET = f"bank-connect-reports-{CURRENT_STAGE}"
BANK_CONNECT_DDB_FAILOVER_BUCKET = f"bank-connect-ddb-failover-{CURRENT_STAGE}"
BANK_CONNECT_CACHEBOX_BUCKET = f"bank-connect-cachebox-{CURRENT_STAGE}"
BANK_CONNECT_UPLOADS_BUCKET = f"bank-connect-uploads-{CURRENT_STAGE}"
BANK_CONNECT_DUMP_BUCKET = f"bank-connect-dump-{CURRENT_STAGE}"
BANK_CONNECT_CLICKHOUSE_BUCKET= f"bank-connect-clickhouse-{CURRENT_STAGE}"
BANK_CONNECT_ENRICHMENTS_BUCKET= f"bank-connect-enrichments-{CURRENT_STAGE}"
BANK_CONNECT_DMS_PUSH_LOGS_BUCKET= f"bank-connect-dms-push-logs-{CURRENT_STAGE}"
BANK_CONNECT_UPLOADS_REPLICA_BUCKET= f"bank-connect-uploads-replica-{CURRENT_STAGE}"

if REGION == "ap-south-1":
    BANK_CONNECT_REPORTS_BUCKET = f"bank-connect-reports-{CURRENT_STAGE}"
    BANK_CONNECT_DDB_FAILOVER_BUCKET = f"bank-connect-ddb-failover-{CURRENT_STAGE}"
    BANK_CONNECT_CACHEBOX_BUCKET = f"bank-connect-cachebox-{CURRENT_STAGE}"
    BANK_CONNECT_UPLOADS_BUCKET = f"bank-connect-uploads-{CURRENT_STAGE}"
    BANK_CONNECT_DUMP_BUCKET = f"bank-connect-dump-{CURRENT_STAGE}"
    BANK_CONNECT_CLICKHOUSE_BUCKET= f"bank-connect-clickhouse-{CURRENT_STAGE}"
    BANK_CONNECT_ENRICHMENTS_BUCKET= f"bank-connect-enrichments-{CURRENT_STAGE}"
    BANK_CONNECT_DMS_PUSH_LOGS_BUCKET= f"bank-connect-dms-push-logs-{CURRENT_STAGE}"
    BANK_CONNECT_UPLOADS_REPLICA_BUCKET= f"bank-connect-uploads-replica-{CURRENT_STAGE}"
else:
    BANK_CONNECT_REPORTS_BUCKET = f"bank-connect-id-reports-{CURRENT_STAGE}"
    BANK_CONNECT_DDB_FAILOVER_BUCKET = f"bank-connect-id-ddb-failover-{CURRENT_STAGE}"
    BANK_CONNECT_CACHEBOX_BUCKET = f"bank-connect-id-cachebox-{CURRENT_STAGE}"
    BANK_CONNECT_UPLOADS_BUCKET = f"bank-connect-id-uploads-{CURRENT_STAGE}"
    BANK_CONNECT_DUMP_BUCKET = f"bank-connect-id-dump-{CURRENT_STAGE}"
    BANK_CONNECT_CLICKHOUSE_BUCKET= f"bank-connect-id-clickhouse-{CURRENT_STAGE}"
    BANK_CONNECT_ENRICHMENTS_BUCKET= f"bank-connect-id-enrichments-{CURRENT_STAGE}"
    BANK_CONNECT_DMS_PUSH_LOGS_BUCKET= f"bank-connect-id-dms-push-logs-{CURRENT_STAGE}"
    BANK_CONNECT_UPLOADS_REPLICA_BUCKET= f"bank-connect-id-uploads-replica-{CURRENT_STAGE}"

BANK_CONNECT_CACHEBOX_RESOURCE = s3_resource.Bucket(BANK_CONNECT_CACHEBOX_BUCKET)
##########################################################################################

##########################################################################################
# DDB Table Descriptions
bank_connect_statement_table_name = f'bank-connect-statement-{CURRENT_STAGE}'
bank_connect_identity_table_name = f'bank-connect-identity-{CURRENT_STAGE}'
bank_connect_account_table_name = f'bank-connect-accounts-{CURRENT_STAGE}'
bank_connect_transactions_table_name = f'bank-connect-transactions-{CURRENT_STAGE}'
bank_connect_salary_table_name = f'bank-connect-salary-transactions-{CURRENT_STAGE}'
bank_connect_disparities_table_name = f'bank-connect-disparities-{CURRENT_STAGE}'
bank_connect_recurring_table_name = f'bank-connect-recurring-transactions-{CURRENT_STAGE}'
bank_connect_tmp_identity_table_name = f'bank-connect-tmp-identity-{CURRENT_STAGE}'
bank_connect_cc_statement_table_name = f'bank-connect-cc-statement-{CURRENT_STAGE}'
bank_connect_cc_transactions_table_name = f'bank-connect-cc-transactions-{CURRENT_STAGE}'
bank_connect_cc_identity_table_name = f'bank-connect-cc-identity-{CURRENT_STAGE}'
bank_connect_cc_entity_mapping_table_name = f'bank-connect-cc-entity-mapping-{CURRENT_STAGE}'
bank_connect_enrichments_table_name = f'bank-connect-enrichments-{CURRENT_STAGE}'

bank_connect_statement_table = dynamo_db.Table(bank_connect_statement_table_name)
bank_connect_identity_table = dynamo_db.Table(bank_connect_identity_table_name)
bank_connect_account_table = dynamo_db.Table(bank_connect_account_table_name)
bank_connect_transactions_table = dynamo_db.Table(bank_connect_transactions_table_name)
bank_connect_salary_table = dynamo_db.Table(bank_connect_salary_table_name)
bank_connect_disparities_table = dynamo_db.Table(bank_connect_disparities_table_name)
bank_connect_recurring_table = dynamo_db.Table(bank_connect_recurring_table_name)
bank_connect_tmp_identity_table = dynamo_db.Table(bank_connect_tmp_identity_table_name)
bank_connect_cc_statement_table = dynamo_db.Table(bank_connect_cc_statement_table_name)
bank_connect_cc_transactions_table = dynamo_db.Table(bank_connect_cc_transactions_table_name)
bank_connect_cc_identity_table = dynamo_db.Table(bank_connect_cc_identity_table_name)
bank_connect_cc_entity_mapping_table = dynamo_db.Table(bank_connect_cc_entity_mapping_table_name)
bank_connect_enrichments_table = dynamo_db.Table(bank_connect_enrichments_table_name)
##########################################################################################

##########################################################################################
# SQS Globals
TERNARY_LAMBDA_QUEUE_URL = f"https://sqs.{REGION}.amazonaws.com/{AWS_ACCOUNT_ID}/bank-connect-large-pdf-extraction-{CURRENT_STAGE}.fifo"
UPDATE_STATE_FAN_OUT_INVOCATION_QUEUE_URL = f"https://sqs.{REGION}.amazonaws.com/{AWS_ACCOUNT_ID}/bank-connect-advance-analysis-trigger-{CURRENT_STAGE}.fifo"
PERFIOS_REPORT_FETCH_TASK_QUEUE_URL = f"https://sqs.{REGION}.amazonaws.com/{AWS_ACCOUNT_ID}/bank-connect-perf-report-fetch-{CURRENT_STAGE}.fifo"
FINVU_AA_REQUEST_STATUS_POLLING_JOBS_QUEUE_URL = f"https://sqs.{REGION}.amazonaws.com/{AWS_ACCOUNT_ID}/bank-connect-aa-finvu-request-status-polling-{CURRENT_STAGE}"
QUALITY_QUEUE_URL = f"https://sqs.{REGION}.amazonaws.com/{AWS_ACCOUNT_ID}/bank-connect-quality-task-{CURRENT_STAGE}"
PDF_PAGES_HASH_GENERATION_TASKS_QUEUE_URL = f"https://sqs.{REGION}.amazonaws.com/{AWS_ACCOUNT_ID}/bank-connect-pdf-page-hash-generation-{CURRENT_STAGE}.fifo"
SESSION_EXPIRY_SQS_QUEUE_URL = f"https://sqs.{REGION}.amazonaws.com/{AWS_ACCOUNT_ID}/bank-connect-session-expiry-{CURRENT_STAGE}"
RAMS_POST_PROCESSING_QUEUE_URL = f"https://sqs.{REGION}.amazonaws.com/{AWS_ACCOUNT_ID}/bank-connect-rams-post-processing-{CURRENT_STAGE}.fifo"

##########################################################################################

##########################################################################################
# LAMBDA FUNCTION NAMES
ENRICHMENT_PREDICTORS_FUNCTION = f'bank-connect-enrichments-{CURRENT_STAGE}-predictors'
ENRICHMENT_MONTHLY_ANALYSIS_FUNCTION = f'bank-connect-enrichments-{CURRENT_STAGE}-monthly_analysis'
ENRICHMENT_EOD_FUNCTION = f'bank-connect-enrichments-{CURRENT_STAGE}-eod_balance'
EXTRACT_SYNC_TRANSACTIONS_FUNCTION = f'bank-connect-{CURRENT_STAGE}-extract_transactions'
ANALYZE_PDF_PAGE_FUNCTION = f'bank-connect-{CURRENT_STAGE}-analyze_pdf_page_enhanced'
ANALYZE_PDF_PAGE_SECONDARY_FUNCTION = f'bank-connect-{CURRENT_STAGE}-analyze_pdf_page_secondary_enhanced'
ANALYZE_PDF_PAGE_TERNARY_FUNCTION = f'bank-connect-{CURRENT_STAGE}-analyze_pdf_page_ternary_enhanced'
KARUR_EXTRACTION_FUNCTION = f'bank-connect-{CURRENT_STAGE}-analyze_pdf_page_karur_enhanced'
KARUR_EXTRACTION_PAGE_FUNCTION = f'bank-connect-{CURRENT_STAGE}-transform_transaction_fsmlib_karur_enhanced'
FINVU_AA_PAGE_FUNCTION = f'bank-connect-{CURRENT_STAGE}-analyze_transactions_finvu_aa_page_enhanced'
UPDATE_STATE_FAN_OUT_FUNCTION = f'bank-connect-{CURRENT_STAGE}-update_state_fan_out_enhanced'
ACCESS_FUNCTION = f'bank-connect-{CURRENT_STAGE}-access_enhanced'
METADATA_FRAUDS_FUNCTION = f'bank-connect-{CURRENT_STAGE}-metadata_frauds_enhanced'
SCORE_LAMBDA_FUNCTION = f'bank-connect-{CURRENT_STAGE}-score'
EXTRACT_TRANSACTIONS_PAGE_FUNCTION = f'bank-connect-{CURRENT_STAGE}-extract_transactions_page'
CATEGORISATION_PAGE_FUNCTION = f'bank-connect-{CURRENT_STAGE}-categorisation_handler_page'
STREAM_FRAUD_DATA_PAGE_FUNCTION = f'bank-connect-{CURRENT_STAGE}-stream_fraud_data_page_enhanced'
CC_TRANSACTIONS_PAGE_LAMBDA = f'bank-connect-{CURRENT_STAGE}-cc_transactions_page'
CC_TRANSACTIONS_PAGE_LAMBDA_OCR = f'bank-connect-{CURRENT_STAGE}-cc_transactions_page_with_ocr'
CACHE_SUBSCRIBED_DATA_LAMBDA = f'bank-connect-{CURRENT_STAGE}-cache_subscribed_data'
DMS_PUSH_LAMBDA = f'bank-connect-{CURRENT_STAGE}-dms_push_handler'
XLSX_REPORT_LAMBDA = f'bank-connect-{CURRENT_STAGE}-xlsx_report_enhanced'
AGGREGATE_XLSX_REPORT_LAMBDA = f'bank-connect-{CURRENT_STAGE}-aggregate_xlsx_report_enhanced'
XML_REPORT_LAMBDA = f'bank-connect-{CURRENT_STAGE}-xml_report_handler'
##########################################################################################

##########################################################################################
# STATEMENT TYPE ENUM


class StatementType(Enum):

    CREDIT_CARD = "credit_card"
    ACCOUNT_AGGREGATOR = "account_aggregator"
    VANILLA = "vanilla"


STATEMENT_TYPE_MAP = {
    "pdf": StatementType.VANILLA.value,
    "credit_card": StatementType.CREDIT_CARD.value,
    "aa": StatementType.ACCOUNT_AGGREGATOR.value
}


##########################################################################################

##########################################################################################
# KAFKA Configs
KAFKA_BROKERS = os.environ.get('KAFKA_BROKER_URL')

# Kafka Topics
KAFKA_TOPIC_INCONSISTENCY = os.environ.get('KAFKA_TOPIC_INCONSISTENCY')
KAFKA_TOPIC_QUALITY_EVENTS = os.environ.get('KAFKA_TOPIC_QUALITY_EVENTS')
KAFKA_TOPIC_WEBHOOK_SEND = 'webhook_sending_topic'
KAFKA_TOPIC_DMS_FAILURE_EMAIL = 'dms_failure'

##########################################################################################

# TCAP Variables
TCAP_DMS_ENDPOINT = os.environ.get('TCAP_DMS_ENDPOINT')
TCAP_DMS_AUTH_KEY = os.environ.get('TCAP_DMS_AUTH_KEY')


##########################################################################################
EXTRACTION_ISSUE_SLACK_TOKEN = get_env_var("EXTRACTION_ISSUE_SLACK_TOKEN")
EXTRACTION_ISSUE_SLACK_CHANNEL = get_env_var("EXTRACTION_ISSUE_SLACK_CHANNEL")

##########################################################################################
SUBSCRIPTION_TYPE = os.environ.get("SUBSCRIPTION_TYPE")
SUBSCRIBE_QUEUE_URL = os.environ.get("SUBSCRIBE_QUEUE_URL")
PRIMARY_EXTRACTION_QUEUE_URL = get_env_var("PRIMARY_EXTRACTION_QUEUE_URL")
SECONDARY_EXTRACTION_QUEUE_URL = get_env_var("SECONDARY_EXTRACTION_QUEUE_URL")

CREDIT_CARD_EXTRACTOR_QUEUE_URL = os.environ.get("CREDIT_CARD_EXTRACTOR_QUEUE_URL")
AA_TRANSACTIONS_PAGE_QUEUE_URL = os.environ.get("AA_TRANSACTIONS_PAGE_QUEUE_URL")
##########################################################################################
# SETUP Folder in EFS if not present

if IS_SERVER:
    if os.path.exists("/efs") and not os.path.exists("/efs/cc"):
        print("EFS Credit Card path does not exist, Creating /efs/cc")
        os.mkdir("/efs/cc")
    