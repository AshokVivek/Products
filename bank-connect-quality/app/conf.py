import boto3
from botocore.config import Config
import os
import redis
import clickhouse_connect
from app.constants import DEFAULT_AWS_REGION

ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 1440  # 1 day
AWS_REGION = os.environ.get("AWS_REGION", DEFAULT_AWS_REGION)
COUNTRY = "IN"
if AWS_REGION == "ap-southeast-3":
    COUNTRY = "ID"

STAGE = os.environ.get("STAGE", "dev")

LOCAL_DB_URL = "sqlite:///./quality.db"

##########################################################################################
# boto3 instantiations
ddb_config = Config(
    retries = {
        'max_attempts': 50,
        'mode': 'adaptive'
    }
)

s3_config = Config(
    retries = {
        "max_attempts": 5,
        "mode": "standard"
    }
)

dynamo_db = boto3.resource('dynamodb', region_name=AWS_REGION, config=ddb_config)
s3_resource = boto3.resource('s3', region_name=AWS_REGION)
s3 = boto3.client('s3', config=s3_config, region_name=AWS_REGION)
endpointURL = s3.meta.endpoint_url
s3 = boto3.client('s3', region_name=AWS_REGION, endpoint_url=endpointURL) 

sqs_client = boto3.client('sqs', region_name=AWS_REGION)

ssm = boto3.client('ssm', region_name=AWS_REGION)
lambda_client = boto3.client('lambda', region_name=AWS_REGION,config=boto3.session.Config(
    read_timeout=120,
    connect_timeout=120
))
##########################################################################################

##########################################################################################
# lambda function names
IDENTITY_LAMBDA_FUNCTION_NAME = f"bank-connect-{STAGE}-identity_enhanced"
UPDATE_STATE_FAN_OUT_FUNCTION_NAME = f"bank-connect-{STAGE}-update_state_fan_out_enhanced"
ANALYZE_PDF_LAMBDA_FUNCTION_NAME = f"bank-connect-{STAGE}-analyze_pdf_enhanced"
CACHE_ACCESS_LAMBDA_FUNCTION_NAME = f"bank-connect-{STAGE}-cache_access_enhanced"
TEMPLATE_HANDLER_LAMBDA_FUNCTION_NAME = f"bank-connect-{STAGE}-get_data_for_template_handler_enhanced"
METADATA_FRAUDS_FUNCTION = f"bank-connect-{STAGE}-metadata_frauds_enhanced"
ANALYZE_TRANSACTIONS_LAMBDA_FINVU_AA = f"bank-connect-{STAGE}-analyze_transactions_finvu_aa_enhanced"
CATEGORISATION_LAMBDA_FUNCTION_NAME = f"bank-connect-{STAGE}-categorisation_handler"
ACCESS_LAMBDA_FUNCTION_NAME = f"bank-connect-{STAGE}-access_enhanced"
##########################################################################################

#queue urls
RAMS_POST_PROCESSING_QUEUE_URL = ssm.get_parameter(Name=f'BANK_CONNECT_RAMS_POST_PROCESSING_QUEUE_URL_{STAGE.upper()}', WithDecryption=True)['Parameter']['Value']

##########################################################################################
# s3 buckets
QUALITY_BUCKET = f"bank-connect-quality-{STAGE}"
FSM_ARBITER_BUCKET = f"bank-connect-cachebox-{STAGE}"
PDF_BUCKET = f"bank-connect-uploads-replica-{STAGE}" if STAGE=='prod' else f'bank-connect-uploads-{STAGE}'
CC_PDF_BUCKET = f"bank-connect-uploads-replica-{STAGE}"
REPORT_BUCKET = f"bank-connect-reports-{STAGE}"
ENRICHMENTS_BUCKET = f"bank-connect-enrichments-{STAGE}"
BANK_CONNECT_DDB_FAILOVER_BUCKET = f"bank-connect-ddb-failcountryover-{STAGE}"
if AWS_REGION == 'ap-southeast-3':
    QUALITY_BUCKET =  f"bank-connect-id-quality-{STAGE}"
    FSM_ARBITER_BUCKET = f"bank-connect-id-cachebox-{STAGE}"
    PDF_BUCKET = f"bank-connect-id-uploads-replica-{STAGE}" if STAGE=='prod' else f"bank-connect-id-uploads-{STAGE}"
    CC_PDF_BUCKET = f"bank-connect-id-uploads-replica-{STAGE}"
    REPORT_BUCKET = f"bank-connect-id-reports-{STAGE}"
    BANK_CONNECT_DDB_FAILOVER_BUCKET = f"bank-connect-id-ddb-failover-{STAGE}"
    ENRICHMENTS_BUCKET = f"bank-connect-id-enrichments-{STAGE}"
##########################################################################################

##########################################################################################
# get variables from SSM
BANK_CONNECT_BASE_URL = ssm.get_parameter(Name=f'BANK_CONNECT_DJANGO_APIS_BASE_URL_{STAGE.upper()}', WithDecryption=True)['Parameter']['Value']
API_KEY = ssm.get_parameter(Name=f'BANK_CONNECT_FINBOX_API_KEY_{STAGE.upper()}', WithDecryption=True)['Parameter']['Value']
SERVER_HASH = ssm.get_parameter(Name=f'BANK_CONNECT_FINBOX_SERVER_HASH_{STAGE.upper()}', WithDecryption=True)['Parameter']['Value']
SUPERUSER_TOKEN = 'abcd' if STAGE=='dev' else ssm.get_parameter(Name=f'BANK_CONNECT_QUALITY_SUPERUSER_TOKEN_{STAGE.upper()}', WithDecryption=True)['Parameter']['Value']

BANK_CONNECT_DB_READ_REPLICA_HOST_URL = ssm.get_parameter(Name=f'BANK_CONNECT_DB_READ_REPLICA_HOST_URL_{STAGE.upper()}', WithDecryption=True)['Parameter']['Value']
BANK_CONNECT_USER = ssm.get_parameter(Name=f'BANK_CONNECT_READONLY_USER_{STAGE.upper()}', WithDecryption=True)['Parameter']['Value']
BANK_CONNECT_PASSWORD = ssm.get_parameter(Name=f'BANK_CONNECT_READONLY_PASSWORD_{STAGE.upper()}', WithDecryption=True)['Parameter']['Value']

DATABASE = ssm.get_parameter(Name = f'BANK_CONNECT_DJANGO_APIS_PG_DBNAME_{STAGE.upper()}', WithDecryption=True)['Parameter']['Value']

QUALITY_HOST = ssm.get_parameter(Name = f'BANK_CONNECT_QUALITY_RDS_PG_HOST_URL_{STAGE.upper()}', WithDecryption=True)['Parameter']['Value']
QUALITY_USER = ssm.get_parameter(Name = f'BANK_CONNECT_QUALITY_RDS_PG_USER_{STAGE.upper()}', WithDecryption=True)['Parameter']['Value']
QUALITY_DB = ssm.get_parameter(Name = f'BANK_CONNECT_QUALITY_RDS_PG_DBNAME_{STAGE.upper()}', WithDecryption=True)['Parameter']['Value']
QUALITY_PASSWORD = ssm.get_parameter(Name = f'BANK_CONNECT_QUALITY_RDS_PG_PASSWORD_{STAGE.upper()}', WithDecryption=True)['Parameter']['Value']
QUALITY_PORT = ssm.get_parameter(Name = f'BANK_CONNECT_QUALITY_RDS_PG_PORT_{STAGE.upper()}', WithDecryption=True)['Parameter']['Value']

QUALITY_SECRET = ssm.get_parameter(Name = f'BANK_CONNECT_QUALITY_SECRET_{STAGE.upper()}', WithDecryption=True)['Parameter']['Value']

MOSHPITTECH_ACCESS_KEY = None
MOSHPITTECH_SECRET_KEY = None
if AWS_REGION == 'ap-south-1' and STAGE.upper() == 'PROD':
    MOSHPITTECH_ACCESS_KEY = ssm.get_parameter(Name = 'MOSHPITTECH_ACCESS_KEY', WithDecryption=True)['Parameter']['Value']
    MOSHPITTECH_SECRET_KEY = ssm.get_parameter(Name = 'MOSHPITTECH_SECRET_KEY', WithDecryption=True)['Parameter']['Value']
# SECRET_KEY = ssm.get_parameter(Name='BANK_CONNECT_QUALITY_SECRET', WithDecryption=True)['Parameter']['Value']
# FB_DASHBOARD_BC_QUALITY_INTERNAL_SECRET = ssm.get_parameter(Name="FB_DASHBOARD_BC_QUALITY_INTERNAL_SECRET", WithDecryption=True)['Parameter']['Value']
##########################################################################################

if STAGE == "dev":
    DATABASE = "portaldb"
PORT = "5432"

PORTAL_DB_URL = f"postgres://{BANK_CONNECT_USER}:{BANK_CONNECT_PASSWORD}@{BANK_CONNECT_DB_READ_REPLICA_HOST_URL}:{PORT}/{DATABASE}?sslmode=disable"
if STAGE.upper() == "PROD":
    QUALITY_DB_URL = f"postgres://{QUALITY_USER}:{QUALITY_PASSWORD}@{QUALITY_HOST}:{QUALITY_PORT}/{QUALITY_DB}?sslmode=allow"
else:
    QUALITY_DB_URL = f"postgres://{QUALITY_USER}:{QUALITY_PASSWORD}@{QUALITY_HOST}:{PORT}/{QUALITY_DB}"

REDSHIFT_HOST = ssm.get_parameter(Name = f'BANK_CONNECT_REDSHIFT_CLUSTER_HOST_URL_WITHOUT_PORT_{STAGE.upper()}', WithDecryption=True)['Parameter']['Value']
REDSHIFT_USER = ssm.get_parameter(Name = f'BANK_CONNECT_REDSHIFT_CLUSTER_READ_ONLY_USERNAME_{STAGE.upper()}', WithDecryption=True)['Parameter']['Value']
REDSHIFT_PASSWORD = ssm.get_parameter(Name = f'BANK_CONNECT_REDSHIFT_CLUSTER_READ_ONLY_PASSWORD_{STAGE.upper()}', WithDecryption=True)['Parameter']['Value']
REDSHIFT_PORT = ssm.get_parameter(Name = f'BANK_CONNECT_REDSHIFT_CLUSTER_PORT_{STAGE.upper()}', WithDecryption=True)['Parameter']['Value']
REDSHIFT_DATABASE = ssm.get_parameter(Name = f'BANK_CONNECT_REDSHIFT_CLUSTER_DBNAME_{STAGE.upper()}', WithDecryption=True)['Parameter']['Value']

# if STAGE == "dev":
#     ssm_keys = ["dev_redshift_host", "dev_redshift_name", "dev_redshift_password", "dev_redshift_port", "dev_redshift_user"]
#     ssm_parameters = ssm.get_parameters(Names=ssm_keys, WithDecryption=True)['Parameters']
#     ssm_secrets = dict()
#     for ssm_parameter in ssm_parameters:
#         ssm_secrets[ssm_parameter['Name']] = ssm_parameter['Value']

#     REDSHIFT_HOST = ssm_secrets["dev_redshift_host"]
#     REDSHIFT_DATABASE = ssm_secrets["dev_redshift_name"]
#     REDSHIFT_PASSWORD = ssm_secrets["dev_redshift_password"]
#     REDSHIFT_PORT = ssm_secrets["dev_redshift_port"]
#     REDSHIFT_USER = ssm_secrets["dev_redshift_user"]
# else:
#     ssm_keys = ["prod_redshift_host", "prod_redshift_name", "prod_redshift_password", "prod_redshift_port", "prod_redshift_user"]
#     ssm_parameters = ssm.get_parameters(Names=ssm_keys, WithDecryption=True)['Parameters']
#     ssm_secrets = dict()
#     for ssm_parameter in ssm_parameters:
#         ssm_secrets[ssm_parameter['Name']] = ssm_parameter['Value']

#     REDSHIFT_HOST = ssm_secrets["prod_redshift_host"]
#     REDSHIFT_DATABASE = ssm_secrets["prod_redshift_name"]
#     REDSHIFT_PASSWORD = ssm_secrets["prod_redshift_password"]
#     REDSHIFT_PORT = ssm_secrets["prod_redshift_port"]
#     REDSHIFT_USER = ssm_secrets["prod_redshift_user"]

REDSHIFT_DB_URL = "postgresql://{}:{}@{}:{}/{}".format(REDSHIFT_USER, REDSHIFT_PASSWORD, REDSHIFT_HOST, REDSHIFT_PORT, REDSHIFT_DATABASE)


##########################################################################################
# DynamoDB Tables
STATEMENT_TABLE_NAME = f"bank-connect-statement-{STAGE}"
ACCOUNT_TABLE_NAME = f"bank-connect-accounts-{STAGE}"
TRANSACTIONS_TABLE_NAME = f"bank-connect-transactions-{STAGE}"
ENRICHMENTS_TABLE_NAME = f'bank-connect-enrichments-{STAGE}'

# boto3 resources of the above DynamoDB Tables
STATEMENT_TABLE = dynamo_db.Table(STATEMENT_TABLE_NAME)
ACCOUNT_TABLE = dynamo_db.Table(ACCOUNT_TABLE_NAME)
TRANSACTIONS_TABLE = dynamo_db.Table(TRANSACTIONS_TABLE_NAME)
ENRICHMENTS_TABLE = dynamo_db.Table(ENRICHMENTS_TABLE_NAME)
##########################################################################################

##########################################################################################
# Old Account resources
OLD_PDF_BUCKET = "fsmprodreplica"
OLD_QUALITY_BUCKET = "fsm-quality-prod"
s3_client_old = boto3.client("s3", aws_access_key_id=MOSHPITTECH_ACCESS_KEY, aws_secret_access_key=MOSHPITTECH_SECRET_KEY)
##########################################################################################

redis_cli = redis.Redis(
    host='redis',
    port=6379,
    charset="utf-8",
    decode_responses=True
)

##########################################################################################


##########################################################################################
SELF_HOSTED_CLICKHOUSE_HOST = ssm.get_parameter(Name = f'BANK_CONNECT_CLICKHOUSE_HOST_{STAGE.upper()}', WithDecryption=True)['Parameter']['Value']
SELF_HOSTED_CLICKHOUSE_PORT = ssm.get_parameter(Name = f'BANK_CONNECT_CLICKHOUSE_PORT_{STAGE.upper()}', WithDecryption=True)['Parameter']['Value']
SELF_HOSTED_CLICKHOUSE_USER = ssm.get_parameter(Name = f'BANK_CONNECT_CLICKHOUSE_USER_{STAGE.upper()}', WithDecryption=True)['Parameter']['Value']
SELF_HOSTED_CLICKHOUSE_PASSWORD = ssm.get_parameter(Name = f'BANK_CONNECT_CLICKHOUSE_PASSWORD_{STAGE.upper()}', WithDecryption=True)['Parameter']['Value']
SELF_HOSTED_CLICKHOUSE_DATABASE = ssm.get_parameter(Name = f'BANK_CONNECT_CLICKHOUSE_DATABASE_{STAGE.upper()}', WithDecryption=True)['Parameter']['Value']

##########################################################################################
SLACK_TOKEN = ssm.get_parameter(Name = f'BANK_CONNECT_INCONSISTENT_SLACK_TOKEN_{STAGE.upper()}', WithDecryption=True)['Parameter']['Value']
SLACK_CHANNEL = ssm.get_parameter(Name = f'BANK_CONNECT_INCONSISTENT_SLACK_CHANNEL_{STAGE.upper()}', WithDecryption=True)['Parameter']['Value']

EXTRACTION_ISSUE_SCHEDULING_INTERVAL = ssm.get_parameter(Name=f'BANK_CONNECT_EXTRACTION_ISSUE_SCHEDULING_INTERVAL_{STAGE.upper()}', WithDecryption=True)['Parameter']['Value']
EXTRACTION_ISSUE_SLACK_TOKEN = ssm.get_parameter(Name=f'BANK_CONNECT_EXTRACTION_ISSUE_SLACK_TOKEN_{STAGE.upper()}', WithDecryption=True)['Parameter']['Value']
EXTRACTION_ISSUE_SLACK_CHANNEL = ssm.get_parameter(Name = f'BANK_CONNECT_EXTRACTION_ISSUE_SLACK_CHANNEL_{STAGE.upper()}', WithDecryption=True)['Parameter']['Value']
##########################################################################################

##########################################################################################
#KAFKA SETTINGS

KAFKA_BROKER_URLS = ssm.get_parameter(Name=f'KAFKA_BROKER_URL_{STAGE.upper()}', WithDecryption=True)['Parameter']['Value'].split(',')

#KAFKA TOPICS
KAFKA_TOPIC_INCONSISTENCY = ssm.get_parameter(Name=f'KAFKA_TOPIC_INCONSISTENCY_{STAGE.upper()}', WithDecryption=True)['Parameter']['Value']

##########################################################################################

##########################################################################################
CATEGORIZE_RS_SERVER_PRIVATE_IP = ssm.get_parameter(Name = f'CATEGORIZE_RS_SERVER_PRIVATE_IP_{STAGE.upper()}', WithDecryption=True)['Parameter']['Value']
##########################################################################################

COMP_SCHEDULING_INTERVAL = 15
FRESHDESK_APIKEY = ""
FRESHDESK_URL = ""
if AWS_REGION=='ap-south-1':
    FRESHDESK_APIKEY = ssm.get_parameter(Name = f'FINBOX_FRESHDESK_API_KEY', WithDecryption=True)['Parameter']['Value']
    FRESHDESK_URL= ssm.get_parameter(Name = f'FINBOX_FRESHDESK_URL', WithDecryption=True)['Parameter']['Value']