import os
import pytest
from moto import mock_aws
import boto3

os.environ["CURRENT_STAGE"] = "dev"
os.environ["REGION"] = "ap-south-1"
os.environ["AWS_ACCOUNT_ID"] = "909798297030"
os.environ["IS_SERVER"] = "False"
os.environ["INTERNAL_API_KEY"] = "<INTERNAL_API_KEY>"
os.environ["DJANGO_BASE_URL"] = "https://apis-dev.bankconnect.finbox.in"

os.environ["ENABLE_SENTRY"] = "False"
os.environ["SENTRY_DSN"] = "https://de4d485be4c97897f72da546bf1050c1@o82232.ingest.sentry.io/4505781969485824"
os.environ["FINVU_AA_REQUEST_STATUS_POLLING_JOBS_QUEUE_URL"] = "https://sqs.ap-south-1.amazonaws.com/909798297030/bank-connect-aa-finvu-request-status-polling-dev"
os.environ["INTERNAL_QUALITY_CHECK_URL"] = "<INTERNAL_QUALITY_CHECK_URL>"
os.environ["PERFIOS_REPORT_FETCH_TASK_QUEUE_URL"] = "https://sqs.ap-south-1.amazonaws.com/909798297030/bank-connect-perf-report-fetch-dev.fifo"
os.environ["RECURRING_MICROSERVICE_TOKEN"] = "05de8576-4636-11ed-b878-0242ac120002"
os.environ["RECURRING_MICROSERVICE_URL"] = "https://apis-dev.bankconnect.finbox.in/microservices/app-rams/"
os.environ["UPDATE_STATE_FAN_OUT_INFO_URL"] = "https://apis-dev.bankconnect.finbox.in/bank-connect/v1/internal_admin/get_info_for_update_state_fan_out"
os.environ["UPDATE_STATE_FAN_OUT_INVOCATION_QUEUE_URL"] = "https://sqs.ap-south-1.amazonaws.com/909798297030/update_state_fan_out_invocation_queue_url.fifo"
os.environ["RAMS_POST_PROCESSING_QUEUE_URL"] = "https://sqs.ap-south-1.amazonaws.com/909798297030/bank-connect-rams-post-processing-dev.fifo"
os.environ["TERNARY_LAMBDA_QUEUE_URL"] = "https://sqs.ap-south-1.amazonaws.com/909798297030/bank-connect-ternary-lambda-queue.fifo"
os.environ["QUALITY_QUEUE_URL"] = "https://sqs.ap-south-1.amazonaws.com/909798297030/bank-connect-quality-task-dev"
os.environ["PDF_PAGES_HASH_GENERATION_TASKS_QUEUE_URL"] = "https://sqs.ap-south-1.amazonaws.com/909798297030/bank-connect-pdf-hash-queue.fifo"
os.environ["BANK_CONNECT_QUALITY_SECRET"] = "<BANK_CONNECT_QUALITY_SECRET>"
os.environ["BANK_CONNECT_QUALITY_PRIVATE_IP"] = "<BANK_CONNECT_QUALITY_PRIVATE_IP>"
os.environ["OLD_ACCOUNT_ACCESS_KEY"] = ""
os.environ["OLD_ACCOUNT_SECRET_KEY"] = ""
os.environ["BANK_CONNECT_REPORTS_BUCKET"] = "bank-connect-reports-dev"
os.environ["BANK_CONNECT_DDB_FAILOVER_BUCKET"] = "bank-connect-ddb-failover-dev"
os.environ["BANK_CONNECT_CACHEBOX_BUCKET"] = "bank-connect-cachebox-dev"
os.environ["BANK_CONNECT_UPLOADS_BUCKET"] = "bank-connect-uploads-dev"
os.environ["BANK_CONNECT_DUMP_BUCKET"] = "bank-connect-dump-dev"
os.environ["BANK_CONNECT_CLICKHOUSE_BUCKET"] = "bank-connect-clickhouse-dev"
os.environ["BANK_CONNECT_ENRICHMENTS_BUCKET"] = "bank-connect-enrichments-dev"
os.environ["BANK_CONNECT_DMS_PUSH_LOGS_BUCKET"] = "bank-connect-dms-push-logs-dev"
os.environ[
    "FSMLIB_CC_TEMPLATE_LOGGING_TASKS_QUEUE_URL"
] = "https://sqs.ap-south-1.amazonaws.com/909798297030/bank-connect-cc-template-logging-dev.fifo"
os.environ["SESSION_EXPIRY_SQS_QUEUE_URL"] = "https://sqs.ap-south-1.amazonaws.com/909798297030/bank-connect-session-expiry-dev"
os.environ["BANK_CONNECT_UPLOADS_REPLICA_BUCKET"] = "bank-connect-uploads-replica-dev"
os.environ["CATEGORIZE_RS_PRIVATE_IP"] = "<CATEGORIZE_RS_PRIVATE_IP>"
os.environ["NANONETS_API_KEY"] = "<NANONETS_API_KEY>"
os.environ["NANONETS_MODEL_ID"] = "e452fcfe-abfe-42af-b5b3-ee6f4015c00c"
os.environ["KAFKA_BROKER_URL"] = "<KAFKA_BROKER_URL>"
os.environ["KAFKA_BROKER_PORT"] = "29092"
os.environ["KAFKA_TOPIC_INCONSISTENCY"] = "inconsistency-solve"
os.environ["KAFKA_TOPIC_DMS_LOGS"] = "dms_push_logs"
os.environ["AWS_ACCESS_KEY_ID"] = "testing"
os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
os.environ["AWS_DEFAULT_REGION"] = "ap-south-1"
os.environ["KAFKA_BROKER_URL"] = "b-1.bankconnectkafkadev.obimkl.c2.kafka.ap-south-1.amazonaws.com:9094,b-3.bankconnectkafkadev.obimkl.c2.kafka.ap-south-1.amazonaws.com:9094,b-2.bankconnectkafkadev.obimkl.c2.kafka.ap-south-1.amazonaws.com:9094"
os.environ["KAFKA_BROKER_TOPIC"] = "inconsistency-solve"
os.environ["EXTRACTION_ISSUE_SLACK_TOKEN"] = "1234567890"
os.environ["EXTRACTION_ISSUE_SLACK_CHANNEL"] = "1234567890"
os.environ["PRIMARY_EXTRACTION_QUEUE_URL"] = "https://sqs.ap-south-1.amazonaws.com/909798297030/bank-connect-primary-extraction-dev.fifo"
os.environ["SECONDARY_EXTRACTION_QUEUE_URL"] = "https://sqs.ap-south-1.amazonaws.com/909798297030/bank-connect-secondary-extraction-dev.fifo"
os.environ["TERNARY_EXTRACTION_QUEUE_URL"] = "https://sqs.ap-south-1.amazonaws.com/909798297030/bank-connect-ternary-extraction-dev.fifo"
os.environ["CLOUDWATCH_LOG_GROUP"] = "bank-connect-unified-logs-dev"

@pytest.fixture
def aws_dynamodb():
    stage = os.environ["CURRENT_STAGE"]
    with mock_aws():
        dynamodb = boto3.resource("dynamodb", region_name="ap-south-1")
        dynamodb.create_table(
            TableName=f"bank-connect-accounts-{stage}",
            KeySchema=[{"AttributeName": "account_id", "KeyType": "HASH"}],
            AttributeDefinitions=[{"AttributeName": "account_id", "AttributeType": "S"}],
            ProvisionedThroughput={"ReadCapacityUnits": 5, "WriteCapacityUnits": 5},
        )
        dynamodb.create_table(
            TableName=f"bank-connect-identity-{stage}",
            KeySchema=[{"AttributeName": "statement_id", "KeyType": "HASH"}],
            AttributeDefinitions=[{"AttributeName": "statement_id", "AttributeType": "S"}],
            ProvisionedThroughput={"ReadCapacityUnits": 5, "WriteCapacityUnits": 5},
        )
        dynamodb.create_table(
            TableName=f"bank-connect-disparities-{stage}",
            KeySchema=[{"AttributeName": "account_id", "KeyType": "HASH"}],
            AttributeDefinitions=[{"AttributeName": "account_id", "AttributeType": "S"}],
            ProvisionedThroughput={"ReadCapacityUnits": 5, "WriteCapacityUnits": 5},
        )
        dynamodb.create_table(
            TableName=f"bank-connect-statement-{stage}",
            KeySchema=[{"AttributeName": "statement_id", "KeyType": "HASH"}],
            AttributeDefinitions=[{"AttributeName": "statement_id", "AttributeType": "S"}],
            ProvisionedThroughput={"ReadCapacityUnits": 5, "WriteCapacityUnits": 5},
        )
        dynamodb.create_table(
            TableName=f"bank-connect-transactions-{stage}",
            KeySchema=[{"AttributeName": "statement_id", "KeyType": "HASH"}],
            AttributeDefinitions=[{"AttributeName": "statement_id", "AttributeType": "S"}],
            ProvisionedThroughput={"ReadCapacityUnits": 5, "WriteCapacityUnits": 5},
        )
        dynamodb.create_table(
            TableName=f"bank-connect-salary-transactions-{stage}",
            KeySchema=[{"AttributeName": "account_id", "KeyType": "HASH"}],
            AttributeDefinitions=[{"AttributeName": "account_id", "AttributeType": "S"}],
            ProvisionedThroughput={"ReadCapacityUnits": 5, "WriteCapacityUnits": 5},
        )

        account_table = dynamodb.Table(f"bank-connect-accounts-{stage}")
        identity_table = dynamodb.Table(f"bank-connect-identity-{stage}")
        disparities_table = dynamodb.Table(f"bank-connect-disparities-{stage}")
        transactions_table = dynamodb.Table(f"bank-connect-transactions-{stage}")
        salary_transactions_table = dynamodb.Table(f"bank-connect-salary-transactions-{stage}")
        statement_table = dynamodb.Table(f"bank-connect-statement-{stage}")

        # Case 1
        account_table.put_item(
            Item={
                "entity_id": "6935038b-0c6e-4696-92b5-4325832654",
                "account_id": "fd4c00bc-45da-4b37-8a7d-843256324859",
                "created_at": 1699530367973822465,
                "item_data": {
                    "account_id": "fd4c00bc-45da-4b37-8a7d-843256324859",
                    "account_category": "SAVINGS",
                    "account_number": "XXXXXXXXXXX8105",
                    "bank": "axis",
                    "ifsc": "UTIB0002091",
                    "linked_account_ref_number": "7056748b-c30c-4a25-afa0-87f15655db4c",
                    "micr": "",
                    "missing_data": [],
                    "salary_confidence": None,
                    "statements": ["324a9f63-c644-4c77-b1a7-68ac8fdf9b03"],
                    "dob": "",
                    "email": "",
                    "pan_number": "",
                    "phone_number": "",
                    "account_status": "",
                    "holder_type": ""
                },
                "updated_at": 1699530370295855921,
            }
        )
        identity_table.put_item(
            Item={
                "statement_id": "324a9f63-c644-4c77-b1a7-68ac8fdf9b03",
                "created_at": 1699530367980042266,
                "item_data": {
                    "country_code": "IN",
                    "currency_code": "INR",
                    "date_range": {"from_date": "2023-08-10", "to_date": "2023-11-04"},
                    "fraud_type": None,
                    "identity": {
                        "account_category": "SAVINGS",
                        "account_id": "fd4c00bc-45da-4b37-8a7d-415c5c0052a4",
                        "account_number": "XXXXXXXXXXX8105",
                        "address": "GOKUL NAGAR ATAL VIHAR MATHPURENA,RAIPUR BEHIND GARDEN, ,RAIPUR,CHHATTISGARH,INDIA,492013",
                        "credit_limit": 0,
                        "name": "MOHAMMAD JASIM",
                    },
                    "is_fraud": False,
                    "is_image": False,
                    "no_transactions_from_finsense": False,
                    "password_incorrect": False,
                    "dob": "",
                    "email": "",
                    "pan_number": "",
                    "phone_number": "",
                    "account_status": "",
                    "holder_type": ""
                },
                "updated_at": 1699530369652657136,
            }
        )

        # Case 2
        account_table.put_item(
            Item={
                "entity_id": "c925471a-7910-416d-8c16-8eca0db98b21",
                "account_id": "13076613-2983-4f2d-9f12-8333484a979f",
                "created_at": 1707998460346000836,
                "item_data": {
                    "account_id": "13076613-2983-4f2d-9f12-8333484a979f",
                    "account_category": "individual",
                    "account_number": "00000041965448665",
                    "account_opening_date": None,
                    "bank": "sbi",
                    "credit_limit": 0,
                    "ifsc": "SBIN0040722",
                    "input_account_category": None,
                    "input_is_od_account": None,
                    "is_od_account": None,
                    "micr": "583002127",
                    "missing_data": [{"from_date": "2024-01-03", "to_date": "2024-01-31"}, {"from_date": "2023-10-30", "to_date": "2023-11-09"}],
                    "od_limit": 0,
                    "salary_confidence": None,
                    "statements": ["4be69fee-b290-444e-8ed3-56b41d13b7fa", "0d623233-1ed1-4d40-a182-3cd5b388ddcb"],
                    "dob": "",
                    "email": "",
                    "pan_number": "",
                    "phone_number": "",
                    "account_status": "",
                    "holder_type": ""
                },
                "updated_at": 1707998511137255947,
            }
        )
        identity_table.put_item(
            Item={
                "statement_id": "4be69fee-b290-444e-8ed3-56b41d13b7fa",
                "created_at": 1707998460352975039,
                "item_data": {
                    "closing_bal": None,
                    "country_code": "IN",
                    "currency_code": "INR",
                    "date_range": {"from_date": "2023-11-01", "to_date": "2024-01-03"},
                    "doc_metadata": {
                        "all_colour_codes": ["0 G"],
                        "author": "",
                        "cnt_negative_y": 32,
                    },
                    "extracted_date_range": {"from_date": "2023-11-01", "to_date": "2024-01-31"},
                    "fraud_type": None,
                    "identity": {
                        "account_category": "individual",
                        "account_id": "13076613-2983-4f2d-9f12-8333484a939f",
                        "account_number": "00000041965448665",
                        "address": "C/O: Md Mustafa Alam, #328 24th Ward, Ka wadi Street, Near Parvatamma Temple Bellary-583102 Bellary",
                        "bank_name": "sbi",
                        "credit_limit": 0,
                        "currency": None,
                        "ifsc": "SBIN0040722",
                        "input_account_category": None,
                        "input_is_od_account": None,
                        "is_od_account": None,
                        "micr": "583002127",
                        "name": "MR. MD MAHMUD ALAM",
                        "od_limit": 0,
                        "raw_account_category": "LOTUS SAVING BANK-ADHAR- CHQ",
                    },
                    "is_fraud": False,
                    "keywords": {"all_present": True, "amount_present": True, "balance_present": True, "date_present": True},
                    "keywords_in_line": True,
                    "metadata_analysis": {"name_matches": []},
                    "opening_bal": None,
                    "opening_date": None,
                    "page_count": 16,
                    "dob": "",
                    "email": "",
                    "pan_number": "",
                    "phone_number": "",
                    "account_status": "",
                    "holder_type": ""
                },
                "updated_at": 1707998488812958361,
            }
        )

        # Case 3: account_number is None
        account_table.put_item(
            Item={
                "entity_id": "c501cf3f-3dfc-4807-9e78-94d1e763e0b9",
                "account_id": "155fae13-94ed-462e-b7a8-b6666a1ccac9",
                "created_at": 1705423715708823105,
                "item_data": {
                    "account_id": "155fae13-94ed-462e-b7a8-b6666a1ccac9",
                    "account_category": None,
                    "account_number": None,
                    "account_opening_date": None,
                    "bank": "icici",
                    "credit_limit": None,
                    "ifsc": None,
                    "input_account_category": None,
                    "input_is_od_account": None,
                    "is_od_account": None,
                    "micr": None,
                    "od_limit": None,
                    "statements": ["dabd306f-3af1-4c06-a31c-0ef3fdcbf482"],
                    "dob": "",
                    "email": "",
                    "pan_number": "",
                    "phone_number": "",
                    "account_status": "",
                    "holder_type": ""
                },
                "updated_at": 1705423715708823105,
            }
        )

        # Case 4
        account_table.put_item(
            Item={
                "entity_id": "7cf941af-191b-4c79-adcb-bba5f29e3510",
                "account_id": "423b55fb-b2ef-4134-a93c-c8147f950167",
                "created_at": 1700916874408911868,
                "item_data": {
                    "account_id": "423b55fb-b2ef-4134-a93c-c8147f950167",
                    "account_category": "individual",
                    "account_number": "7147642094",
                    "bank": "kotak",
                    "ifsc": "",
                    "micr": "",
                    "statements": ["fbe05364-d287-45a8-989c-db7b4e2dee5a"],
                    "dob": "",
                    "email": "",
                    "pan_number": "",
                    "phone_number": "",
                    "account_status": "",
                    "holder_type": ""
                },
                "updated_at": 1700916880495728396,
            }
        )
        identity_table.put_item(
            Item={
                "statement_id": "fbe05364-d287-45a8-989c-db7b4e2dee5a",
                "created_at": 1700916880502231981,
                "item_data": {
                    "date_range": {"from_date": "2023-06-02", "to_date": "2023-11-25"},
                    "fraud_type": None,
                    "identity": {
                        "account_category": "individual",
                        "account_id": "423b55fb-b2ef-4134-a93c-c8147f950b67",
                        "account_number": "7147642094",
                        "address": "H N0-B-99 STREET N0-4 RAMA GAR DEN KARAWAL NAGAR NORTH EAST New Delhi - 110094",
                        "credit_limit": "",
                        "ifsc": "",
                        "micr": "",
                        "name": "Sanjay",
                        "perfios_account_category": "SAVINGS",
                        "perfios_statement_status": "VERIFIED",
                        "perfios_transaction_id": "Z9HL1700916857471",
                    },
                    "is_extracted_by_perfios": True,
                    "is_fraud": False,
                    "is_fraud_from_excel": False,
                    "is_fraud_from_perfios_data": "VERIFIED",
                    "is_image": False,
                    "keywords": {"all_present": True, "amount_present": True, "balance_present": True, "date_present": True},
                    "password_incorrect": False,
                    "account_status": "",
                    "holder_type": "",
                    "dob": "",
                        "email": "",
                        "pan_number": "",
                        "phone_number": "",
                },
                "updated_at": 1700916880502231981,
            }
        )

        # Case 5
        account_table.put_item(
            Item={
                "entity_id": "11e618c5-6869-487d-b999-74ffc0b481e1",
                "account_id": "ec9fde7b-eb06-4958-8e9b-ad7ca2c51397",
                "created_at": 1715886838427837375,
                "item_data": {
                    "account_id": "ec9fde7b-eb06-4958-8e9b-ad7ca2c51397",
                    "account_category": "individual",
                    "account_number": "157764016923",
                    "account_opening_date": None,
                    "bank": "indusind",
                    "credit_limit": None,
                    "ifsc": None,
                    "input_account_category": None,
                    "input_is_od_account": None,
                    "is_od_account": False,
                    "micr": None,
                    "missing_data": [],
                    "neg_txn_od": False,
                    "od_limit": 0,
                    "salary_confidence": 70,
                    "statements": [
                        "f1031160-ba89-4705-b103-fb3585eeadb4",
                        "d36556ad-e315-4789-85d0-3ed87c205165",
                        "61a7acd6-be6f-4ed5-aefa-eb881ded88b4",
                    ],
                    "dob": "",
                    "email": "",
                    "pan_number": "",
                    "phone_number": "",
                    "account_status": "",
                    "holder_type": ""
                },
                "updated_at": 1715886860852265725,
            }
        )
        identity_table.put_item(
            Item={
                "statement_id": "f1031160-ba89-4705-b103-fb3585eeadb4",
                "created_at": 1715886838434523044,
                "item_data": {
                    "closing_bal": None,
                    "country_code": "IN",
                    "currency_code": "INR",
                    "date_range": {"from_date": "2024-05-02", "to_date": "2024-05-16"},
                    "doc_metadata": {
                        "all_colour_codes": [
                            "0 0 0 RG",
                            "0 0 0 rg",
                            "0 0 1 rg",
                            "0 G",
                            "0 g",
                            "0.30196 0.30196 0.30196 rg",
                            "0.65882 0.06667 0.18039 RG",
                            "0.65882 0.06667 0.18039 rg",
                            "0.89804 0.89804 0.89804 RG",
                            "0.95686 0.89412 0.7098 rg",
                            "1 1 1 RG",
                            "1 1 1 rg",
                        ],
                    },
                    "extracted_date_range": {"from_date": "2024-05-01", "to_date": "2024-05-16"},
                    "fraud_type": None,
                    "identity": {
                        "account_category": "individual",
                        "account_id": "ec9fde7b-eb06-4958-8e9b-ad7ca2c55397",
                        "account_number": "157764016923",
                        "address": "BASTHA, BASTHA, MAINATAND, BASTHA,WEST CHAMPARAN, WARD NO 12, WEST CHAMPARAN,BIHAR, INDIA-845306",
                        "bank_name": "indusind",
                        "credit_limit": None,
                        "currency": None,
                        "ifsc": None,
                        "input_account_category": None,
                        "input_is_od_account": None,
                        "is_od_account": False,
                        "micr": None,
                        "name": "SHAHANAWAJ ALAM",
                        "od_limit": 0,
                        "od_metadata": {"initial_neg_txn_od": False, "is_od_account_by_extraction": None, "od_limit_by_extraction": None},
                        "raw_account_category": "SAVINGS ACCOUNT-INDUS CLASSIC",
                        "updated_od_paramters_by": "L50%_NEGATIVE_TXN_OD_False",
                    },
                    "is_fraud": False,
                    "keywords": {"all_present": True, "amount_present": True, "balance_present": True, "date_present": True},
                    "keywords_in_line": True,
                    "metadata_analysis": {"name_matches": []},
                    "opening_bal": None,
                    "opening_date": None,
                    "page_count": 5,
                    "dob": "",
                    "email": "",
                    "pan_number": "",
                    "phone_number": "",
                    "account_status": "",
                    "holder_type": ""
                },
                "updated_at": 1715886849236669809,
            }
        )

        # Case 6: Statements not present
        account_table.put_item(
            Item={
                "entity_id": "1b2e302c-a058-4fad-af42-20d4c786e1c3",
                "account_id": "e81101e6-2180-4e74-8d98-f5038e64e0b0",
                "created_at": 1715886805807521235,
                "item_data": {
                    "account_id": "e81101e6-2180-4e74-8d98-f5038e64e0b0",
                    "account_category": "SAVINGS",
                    "account_number": "XXXXXXXXXXX0446",
                    "account_opening_date": "2009-03-02",
                    "bank": "boi",
                    "credit_limit": 0,
                    "ifsc": "BKID0007801",
                    "is_od_account": False,
                    "linked_account_ref_number": "878124a3-7d56-4a17-9721-db996172e889",
                    "micr": "244013052",
                    "missing_data": [],
                    "neg_txn_od": False,
                    "od_limit": 0,
                    "salary_confidence": None,
                    "dob": "",
                    "email": "",
                    "pan_number": "",
                    "phone_number": "",
                    "account_status": "",
                    "holder_type": ""
                },
                "item_status": {"account_status": "completed", "error_code": None, "error_message": None},
                "updated_at": 1715886811747270633,
            }
        )
        identity_table.put_item(
            Item={
                "statement_id": "f8235c05-0568-43e6-ae44-12dcd3d587f7",
                "created_at": 1715886805813501166,
                "item_data": {
                    "country_code": "IN",
                    "currency_code": "INR",
                    "date_range": {"from_date": "2023-11-01", "to_date": "2024-04-30"},
                    "extracted_date_range": {"from_date": "2023-11-01", "to_date": "2024-04-30"},
                    "fraud_type": None,
                    "identity": {
                        "account_category": "SAVINGS",
                        "account_id": "e81101e6-2180-4e74-8d98-f5018e64e0b0",
                        "account_number": "XXXXXXXXXXX0446",
                        "address": "VILL GURAITHA POST PAKWADA,DISTT MORADABAD,,MORBA,UP,IN,244102",
                        "bank_name": "boi",
                        "credit_limit": 0,
                        "is_od_account": False,
                        "name": "SUNITA WO JASWANT",
                        "od_limit": 0,
                        "od_metadata": {"initial_neg_txn_od": None, "is_od_account_by_extraction": None, "od_limit_by_extraction": 0},
                        "raw_account_category": None,
                        "updated_od_paramters_by": "L50%_NEGATIVE_TXN_OD_FALSE",
                    },
                    "is_fraud": False,
                    "is_image": False,
                    "no_transactions_from_finsense": False,
                    "password_incorrect": False,
                    "dob": "",
                    "email": "",
                    "pan_number": "",
                    "phone_number": "",
                    "account_status": "",
                    "holder_type": ""
                },
                "updated_at": 1715886809500217598,
            }
        )

        # Case 7: Country and currency code not present
        account_table.put_item(
            Item={
                "entity_id": "bc870d10-b904-465c-8e11-ecb9f1825707",
                "account_id": "25e32c76-d2fc-4a6c-87df-d55e4a1b8455",
                "created_at": 1715925032986932312,
                "item_data": {
                    "account_id": "25e32c76-d2fc-4a6c-87df-d55e4a1b8455",
                    "account_category": "individual",
                    "account_number": "011310100291672",
                    "account_opening_date": None,
                    "bank": "ubi",
                    "credit_limit": None,
                    "ifsc": "UBIN0801135",
                    "input_account_category": None,
                    "input_is_od_account": None,
                    "is_od_account": False,
                    "micr": None,
                    "missing_data": [],
                    "neg_txn_od": False,
                    "od_limit": 0,
                    "salary_confidence": None,
                    "statements": [
                        "e50b64ce-c511-4c43-ba84-ab40f24476e5",
                        "f8de024e-6151-4342-9ef9-118505a62eb4",
                        "ff0082c0-34eb-4645-b2a0-d472ebbcc2bf",
                        "4419f0a7-a4da-4b5e-a15b-d87678467f14",
                    ],
                    "dob": "",
                    "email": "",
                    "pan_number": "",
                    "phone_number": "",
                    "account_status": "",
                    "holder_type": ""
                },
                "updated_at": 1715925100957217454,
            }
        )
        identity_table.put_item(
            Item={
                "statement_id": "f8de024e-6151-4342-9ef9-118505a62eb4",
                "created_at": 1715925051509227835,
                "item_data": {
                    "closing_bal": None,
                    "date_range": {"from_date": "2024-03-01", "to_date": "2024-03-31"},
                    "extracted_date_range": {"from_date": "2024-03-01", "to_date": "2024-03-31"},
                    "fraud_type": None,
                    "identity": {
                        "account_category": "individual",
                        "account_id": "25e32c76-d2fc-4a6c-87df-d55e4a5b8455",
                        "account_number": "011310100291672",
                        "address": "LAAVUDAIAH SORUPA 4-54, NALGONDA NEREDGOMMU, DEVERAKONDA",
                        "bank_name": "ubi",
                        "credit_limit": None,
                        "currency": None,
                        "ifsc": "UBIN0801135",
                        "input_account_category": None,
                        "input_is_od_account": None,
                        "is_od_account": False,
                        "micr": None,
                        "name": "LAAVUDAIAH SORUPA",
                        "od_limit": 0,
                        "od_metadata": {"initial_neg_txn_od": False, "is_od_account_by_extraction": None, "od_limit_by_extraction": None},
                        "raw_account_category": "Savings Account",
                        "updated_od_paramters_by": "L50%_NEGATIVE_TXN_OD_FALSE",
                    },
                    "is_fraud": False,
                    "keywords": {"all_present": True, "amount_present": True, "balance_present": True, "date_present": True},
                    "keywords_in_line": True,
                    "metadata_analysis": {"name_matches": []},
                    "opening_bal": None,
                    "opening_date": None,
                    "page_count": 12,
                    "dob": "",
                    "email": "",
                    "pan_number": "",
                    "phone_number": "",
                    "account_status": "",
                    "holder_type": ""
                },
                "updated_at": 1715925067356096151,
            }
        )

        # Case 8: date_range not present in identity
        account_table.put_item(
            Item={
                "entity_id": "e78f9776-2553-4ac5-90a7-04015d8a2046",
                "account_id": "24f4f3d6-4d3d-4f79-af70-8f68216c378f",
                "created_at": 1715925014987892302,
                "item_data": {
                    "account_id": "24f4f3d6-4d3d-4f79-af70-8f68216c378f",
                    "account_category": "individual",
                    "account_number": "00000062443248925",
                    "account_opening_date": None,
                    "bank": "sbi",
                    "credit_limit": 0,
                    "ifsc": "SBIN0021302",
                    "input_account_category": None,
                    "input_is_od_account": None,
                    "is_od_account": False,
                    "micr": "506002036",
                    "missing_data": [],
                    "neg_txn_od": False,
                    "od_limit": 0,
                    "salary_confidence": None,
                    "statements": [
                        "bcba53ab-a441-444a-92d0-57b76e7897c9",
                        "5a58f4b1-f53b-4ae2-801b-b9513966428e",
                        "7ac37aa3-418c-4b62-a382-d310300dcdc2",
                        "2fe0e70b-2655-441f-95b9-86d2825f5fe6",
                        "b88db121-5fbc-42e8-9b95-d989021d30ce",
                        "610b6038-18cf-42aa-b8e8-4d2e21cf052d",
                        "2a85ec4c-df9d-4b02-9d72-882b3d36dc27",
                    ],
                    "dob": "",
                    "email": "",
                    "pan_number": "",
                    "phone_number": "",
                    "account_status": "",
                    "holder_type": ""
                },
                "item_status": {
                    "account_status": "failed",
                    "error_code": "UNPARSABLE",
                    "error_message": "Failed to process because of an unparsable statement",
                },
                "updated_at": 1715925053343229683,
            }
        )
        identity_table.put_item(
            Item={
                "statement_id": "2fe0e70b-2655-441f-95b9-86d2825f5fe6",
                "created_at": 1715925021111326717,
                "item_data": {
                    "closing_bal": None,
                    "country_code": "IN",
                    "currency_code": "INR",
                    "extracted_date_range": {"from_date": "2024-01-30", "to_date": "2024-02-29"},
                    "fraud_type": None,
                    "identity": {
                        "account_category": "individual",
                        "account_id": "24f4f3d6-4d3d-4f79-af70-8f68266c378f",
                        "account_number": "00000062443248925",
                        "address": "HNO 1-18 KAPULA KANAPARTHY SANGEM MANDAL DIST WARANGAL-506001 WARANGAL",
                        "bank_name": "sbi",
                        "credit_limit": 0,
                        "currency": None,
                        "ifsc": "SBIN0021302",
                        "input_account_category": None,
                        "input_is_od_account": None,
                        "is_od_account": False,
                        "micr": "506002036",
                        "name": "MR. SOMARAJU NALLATHEEGALA",
                        "od_limit": 0,
                        "od_metadata": {"initial_neg_txn_od": False, "is_od_account_by_extraction": None, "od_limit_by_extraction": 0},
                        "raw_account_category": "REGULAR SB CHQ-INDIVIDUALS",
                        "updated_od_paramters_by": "L50%_NEGATIVE_TXN_OD_FALSE",
                    },
                    "is_fraud": False,
                    "keywords": {"all_present": True, "amount_present": True, "balance_present": True, "date_present": True},
                    "keywords_in_line": True,
                    "metadata_analysis": {
                        "name_matches": [
                            {
                                "matches": True,
                                "name": "SOMARAJU NALLATHEEGALA",
                                "score": 100,
                                "tokenized_matches": [
                                    {"matches": True, "score": 100, "token": "SOMARAJU"},
                                    {"matches": False, "score": 46, "token": "NALLATHEEGALA"},
                                ],
                            },
                            {
                                "matches": False,
                                "name": "SURYA PHARMACY",
                                "score": 50,
                                "tokenized_matches": [
                                    {"matches": False, "score": 60, "token": "SURYA"},
                                    {"matches": False, "score": 50, "token": "PHARMACY"},
                                ],
                            },
                        ]
                    },
                    "opening_bal": None,
                    "opening_date": None,
                    "page_count": 14,
                                        "dob": "",
                        "email": "",
                        "pan_number": "",
                        "phone_number": "",
                        "account_status": "",
                    "holder_type": ""
                },
                "updated_at": 1715925044043314307,
            }
        )

        # Case 9: account_status is failed
        account_table.put_item(
            Item={
                "entity_id": "bdc363de-fd2e-4ffd-a53f-7a8db8aff612",
                "account_id": "30b30d95-305f-4322-a378-28ed0cbeb4f1",
                "created_at": 1715924989270210423,
                "item_data": {
                    "account_id": "30b30d95-305f-4322-a378-28ed0cbeb4f1",
                    "account_category": "individual",
                    "account_number": "00000034249671065",
                    "account_opening_date": None,
                    "bank": "sbi",
                    "credit_limit": 0,
                    "ifsc": "SBIN0008307",
                    "input_account_category": None,
                    "input_is_od_account": None,
                    "is_od_account": False,
                    "micr": "281002050",
                    "neg_txn_od": False,
                    "od_limit": 0,
                    "salary_confidence": None,
                    "statements": [
                        "eaf5fe32-fd7b-4d38-90e5-038d4e2669af",
                        "e82a03dc-7c30-4ceb-87d1-7c2b240c4f4a",
                        "b820a3de-1922-40f0-a515-4232330af44f",
                    ],
                                            "dob": "",
                        "email": "",
                        "pan_number": "",
                        "phone_number": "",
                        "account_status": "",
                    "holder_type": ""
                },
                "item_status": {
                    "account_status": "failed",
                    "error_code": "UNPARSABLE",
                    "error_message": "Failed to process because of an unparsable statement",
                },
                "updated_at": 1715925030141851756,
            }
        )
        identity_table.put_item(
            Item={
                "statement_id": "e82a03dc-7c30-4ceb-87d1-7c2b240c4f4a",
                "created_at": 1715924997591978723,
                "item_data": {
                    "closing_bal": None,
                    "country_code": "IN",
                    "currency_code": "INR",
                    "date_range": {"from_date": "2023-11-02", "to_date": "2024-01-25"},
                    "extracted_date_range": {"from_date": "2023-11-02", "to_date": "2024-04-03"},
                    "fraud_type": None,
                    "identity": {
                        "account_category": "individual",
                        "account_id": "30b30d95-305f-4322-a378-28ed0cbeb4ff",
                        "account_number": "00000034249671065",
                        "address": "83, MOTI NAGAR EXTN BALAJIPURAM AURANGABAD MATHURA-281006 Mathura",
                        "bank_name": "sbi",
                        "credit_limit": 0,
                        "currency": None,
                        "ifsc": "SBIN0008307",
                        "input_account_category": None,
                        "input_is_od_account": None,
                        "is_od_account": False,
                        "micr": "281002050",
                        "name": "MR. MANOJ CHAUDHARY",
                        "od_limit": 0,
                        "od_metadata": {"initial_neg_txn_od": False, "is_od_account_by_extraction": None, "od_limit_by_extraction": 0},
                        "raw_account_category": "REGULAR SB CHQ-INDIVIDUALS",
                        "updated_od_paramters_by": "L50%_NEGATIVE_TXN_OD_FALSE",
                    },
                    "is_fraud": False,
                    "keywords": {"all_present": True, "amount_present": True, "balance_present": True, "date_present": True},
                    "keywords_in_line": True,
                    "metadata_analysis": {
                        "name_matches": [
                            {
                                "matches": True,
                                "name": "MANOJ CHAUDHARY",
                                "score": 100,
                                "tokenized_matches": [
                                    {"matches": True, "score": 100, "token": "MANOJ"},
                                    {"matches": True, "score": 100, "token": "CHAUDHARY"},
                                ],
                            },
                            {
                                "matches": False,
                                "name": "NAVDEEP TRADERS",
                                "score": 40,
                                "tokenized_matches": [
                                    {"matches": False, "score": 43, "token": "NAVDEEP"},
                                    {"matches": False, "score": 43, "token": "TRADERS"},
                                ],
                            },
                        ]
                    },
                    "opening_bal": None,
                    "opening_date": None,
                    "page_count": 15,
                                            "dob": "",
                        "email": "",
                        "pan_number": "",
                        "phone_number": "",
                        "account_status": "",
                    "holder_type": ""
                },
                "updated_at": 1715925019118909716,
            }
        )

        # Case 10: missing data not present
        account_table.put_item(
            Item={
                "entity_id": "bdc363de-fd2e-4ffd-a53f-7a8db81ff612",
                "account_id": "30b30d95-305f-4322-a378-28e10cbeb4f1",
                "created_at": 1715924989270210423,
                "item_data": {
                    "account_id": "30b30d95-305f-4322-a378-28e10cbeb4f1",
                    "account_category": "individual",
                    "account_number": "00000034249671065",
                    "account_opening_date": None,
                    "bank": "sbi",
                    "credit_limit": 0,
                    "ifsc": "SBIN0008307",
                    "input_account_category": None,
                    "input_is_od_account": None,
                    "is_od_account": False,
                    "micr": "281002050",
                    "neg_txn_od": False,
                    "od_limit": 0,
                    "salary_confidence": None,
                    "statements": [
                        "eaf5fe32-fd7b-4d38-90e5-038d4e2669af",
                        "e82a03dc-7c30-4ceb-87d1-7c2b240c4f4a",
                        "b820a3de-1922-40f0-a515-4232330af44f",
                    ],
                                            "dob": "",
                        "email": "",
                        "pan_number": "",
                        "phone_number": "",
                        "account_status": "",
                    "holder_type": ""
                },
                "item_status": {
                    "account_status": "completed",
                    "error_code": "UNPARSABLE",
                    "error_message": "Failed to process because of an unparsable statement",
                },
                "updated_at": 1715925030141851756,
            }
        )
        identity_table.put_item(
            Item={
                "statement_id": "e82a03dc-7c30-4ceb-87d1-7c2b240c4f4a",
                "created_at": 1715924997591978723,
                "item_data": {
                    "closing_bal": None,
                    "country_code": "IN",
                    "currency_code": "INR",
                    "date_range": {"from_date": "2023-11-02", "to_date": "2024-01-25"},
                    "extracted_date_range": {"from_date": "2023-11-02", "to_date": "2024-04-03"},
                    "fraud_type": None,
                    "identity": {
                        "account_category": "individual",
                        "account_id": "30b30d95-305f-4322-a378-28ed0cbeb4ff",
                        "account_number": "00000034249671065",
                        "address": "83, MOTI NAGAR EXTN BALAJIPURAM AURANGABAD MATHURA-281006 Mathura",
                        "bank_name": "sbi",
                        "credit_limit": 0,
                        "currency": None,
                        "ifsc": "SBIN0008307",
                        "input_account_category": None,
                        "input_is_od_account": None,
                        "is_od_account": False,
                        "micr": "281002050",
                        "name": "MR. MANOJ CHAUDHARY",
                        "od_limit": 0,
                        "od_metadata": {"initial_neg_txn_od": False, "is_od_account_by_extraction": None, "od_limit_by_extraction": 0},
                        "raw_account_category": "REGULAR SB CHQ-INDIVIDUALS",
                        "updated_od_paramters_by": "L50%_NEGATIVE_TXN_OD_FALSE",
                    },
                    "is_fraud": False,
                    "keywords": {"all_present": True, "amount_present": True, "balance_present": True, "date_present": True},
                    "keywords_in_line": True,
                    "metadata_analysis": {
                        "name_matches": [
                            {
                                "matches": True,
                                "name": "MANOJ CHAUDHARY",
                                "score": 100,
                                "tokenized_matches": [
                                    {"matches": True, "score": 100, "token": "MANOJ"},
                                    {"matches": True, "score": 100, "token": "CHAUDHARY"},
                                ],
                            },
                            {
                                "matches": False,
                                "name": "NAVDEEP TRADERS",
                                "score": 40,
                                "tokenized_matches": [
                                    {"matches": False, "score": 43, "token": "NAVDEEP"},
                                    {"matches": False, "score": 43, "token": "TRADERS"},
                                ],
                            },
                        ]
                    },
                    "opening_bal": None,
                    "opening_date": None,
                    "page_count": 15,
                                            "dob": "",
                        "email": "",
                        "pan_number": "",
                        "phone_number": "",
                        "account_status": "",
                    "holder_type": ""
                },
                "updated_at": 1715925019118909716,
            }
        )

        # 1: get_account_frauds_test data
        account_table.put_item(
            Item={
                "entity_id": "6f0586b1-e834-4d43-904e-f0615376b1ff",
                "account_id": "fd54e44c-9076-4ac7-abc9-5ca177fc1e2a",
                "created_at": 1715944845211885276,
                "item_data": {
                    "account_id": "fd54e44c-9076-4ac7-abc9-5ca177fc1e2a",
                    "account_category": None,
                    "account_number": "7491118105",
                    "account_opening_date": None,
                    "bank": "indbnk",
                    "credit_limit": None,
                    "ifsc": "IDIB000L574",
                    "input_account_category": None,
                    "input_is_od_account": None,
                    "is_od_account": True,
                    "micr": None,
                    "missing_data": [{"from_date": "2023-05-01", "to_date": "2023-05-11"}],
                    "neg_txn_od": False,
                    "od_limit": 200000000,
                    "od_limit_input_by_customer": True,
                    "salary_confidence": None,
                    "statements": [
                        "c180cf42-98f6-4e0e-ad09-d37382d5f705",
                        "d88e3b07-a881-4681-97f5-9b8ac6adac73",
                        "b4663e5a-a795-418c-8e1d-f2d15a015251",
                    ],
                                            "dob": "",
                        "email": "",
                        "pan_number": "",
                        "phone_number": "",
                        "account_status": "",
                    "holder_type": ""
                },
                "updated_at": 1715945803460459744,
            }
        )
        identity_table.put_item(
            Item={
                "statement_id": "b4663e5a-a795-418c-8e1d-f2d15a015251",
                "created_at": 1715945789386107112,
                "item_data": {
                    "closing_bal": None,
                    "country_code": "IN",
                    "currency_code": "INR",
                    "date_range": {"from_date": "2023-05-15", "to_date": "2024-04-30"},
                    "extracted_date_range": {"from_date": "2023-05-11", "to_date": "2023-10-31"},
                    "fraud_type": "author_fraud",
                    "identity": {
                        "account_category": None,
                        "account_id": "fd54e44c-9076-4ac7-abc9-5ca177fc1e2a",
                        "account_number": "7491118105",
                        "address": "House No B-125/661/26/1 Street No 03,Near Water Tank Basti Jodhewal,Ludhiana, Punjab",
                        "bank_name": "indbnk",
                        "credit_limit": None,
                        "currency": None,
                        "ifsc": "IDIB000L574",
                        "input_account_category": None,
                        "input_is_od_account": None,
                        "is_od_account": True,
                        "micr": None,
                        "name": "MUKUL MALHOTRA",
                        "od_limit": 200000000,
                        "od_metadata": {"initial_neg_txn_od": False, "is_od_account_by_extraction": True, "od_limit_by_extraction": None},
                        "raw_account_category": None,
                        "updated_od_paramters_by": "G50%_TXN_OD_TRUE",
                    },
                    "is_fraud": True,
                    "keywords": {"all_present": True, "amount_present": True, "balance_present": True, "date_present": True},
                    "keywords_in_line": True,
                    "metadata_analysis": {"name_matches": []},
                    "opening_bal": None,
                    "opening_date": None,
                    "page_count": 6,
                                            "dob": "",
                        "email": "",
                        "pan_number": "",
                        "phone_number": "",
                        "account_status": "",
                    "holder_type": ""
                },
                "updated_at": 1715945802833265161,
            }
        )
        disparities_table.put_item(
            Item={
                "account_id": "fd54e44c-9076-4ac7-abc9-5ca177fc1e2a",
                "created_at": 1715945803564938776,
                "item_data": '[{"fraud_type": "inconsistent_transaction", "transaction_hash": "184a599753d81b210dc2aa8a83228db0", "prev_date": "2023-10-31 00:00:00", "curr_date": "2023-10-31 00:00:00", "statement_id": "d88e3b07-a881-4681-97f5-9b8ac6adac73", "account_id": "fd54e44c-9076-4ac7-abc9-5ca177fc1e2a"}, {"fraud_type": "inconsistent_transaction", "transaction_hash": "184a599753d81b210dc2aa8a83228db0", "prev_date": "2023-10-31 00:00:00", "curr_date": "2023-10-31 00:00:00", "statement_id": "b4663e5a-a795-418c-8e1d-f2d15a015251", "account_id": "fd54e44c-9076-4ac7-abc9-5ca177fc1e2a"}, {"fraud_type": "inconsistent_transaction", "transaction_hash": "184a599753d81b210dc2aa8a83228db0", "prev_date": "2023-10-31 00:00:00", "curr_date": "2023-10-31 00:00:00", "account_id": "fd54e44c-9076-4ac7-abc9-5ca177fc1e2a"}, {"fraud_type": "min_rtgs_amount", "transaction_hash": "e0ab7842fc31fd7e84338ebc1ebe037c"}]',
                "updated_at": 1715945803564938776,
            }
        )

        # 1: get_account_transactions_test data
        account_table.put_item(
            Item={
                "entity_id": "672dd54b-d556-4315-8e12-0aa5337f147e",
                "account_id": "4cd9ebf5-be03-4172-941f-91b01aa3dfc0",
                "created_at": 1716185046026071287,
                "item_data": {
                    "account_id": "4cd9ebf5-be03-4172-941f-91b01aa3dfc0",
                    "account_category": "individual",
                    "account_number": "00000020163205719",
                    "account_opening_date": None,
                    "bank": "sbi",
                    "credit_limit": 0,
                    "ifsc": "SBIN0013760",
                    "input_account_category": None,
                    "input_is_od_account": None,
                    "is_od_account": False,
                    "micr": "380002166",
                    "missing_data": [{"from_date": "2024-03-19", "to_date": "2024-04-30"}],
                    "neg_txn_od": False,
                    "od_limit": 0,
                    "salary_confidence": None,
                    "statements": [
                        "990210b8-58bd-4e3b-a9be-46ee69b0f81e",
                        "e7affedd-3fe7-4124-bf3c-0b520bf4c5ab",
                    ],
                                            "dob": "",
                        "email": "",
                        "pan_number": "",
                        "phone_number": "",
                        "account_status": "",
                    "holder_type": ""
                },
                "item_status": {"account_status": "completed", "error_code": None, "error_message": None},
                "updated_at": 1716185152864518656,
            }
        )
        statement_table.put_item(
            Item={
                "statement_id": "990210b8-58bd-4e3b-a9be-46ee69b0f81e",
                "created_at": "1716185046",
                "cut_transactions_page_0": True,
                "cut_transactions_page_1": True,
                "cut_transactions_page_2": True,
                "cut_transactions_page_3": True,
                "cut_transactions_page_4": True,
                "cut_transactions_page_5": True,
                "cut_transactions_page_6": True,
                "cut_transactions_page_7": True,
                "cut_transactions_page_8": True,
                "identity_status": "completed",
                "message": None,
                "metadata_fraud_status": "completed",
                "pages_done": 12,
                "page_count": 12,
                "page_identity_fraud_status": "completed",
                "processing_status": "completed",
                "transactions_status": "completed",
            }
        )
        statement_table.put_item(
            Item={
                "statement_id": "e7affedd-3fe7-4124-bf3c-0b520bf4c5ab",
                "created_at": "1716185067",
                "identity_status": "completed",
                "message": None,
                "metadata_fraud_status": "completed",
                "pages_done": 4,
                "page_count": 4,
                "page_identity_fraud_status": "completed",
                "processing_status": "completed",
                "transactions_status": "completed",
            }
        )
        transactions_table.put_item(
            Item={
                "statement_id": "990210b8-58bd-4e3b-a9be-46ee69b0f81e",
                "page_number": 0,
                "created_at": 1716185062951482347,
                "item_data": "[]",
                "template_id": "trans_bbox_a6561a13-a12a-48d9-94a7-20a5055dd63c",
                "transaction_count": 12,
                "updated_at": 1716185062951482347,
            }
        )
        transactions_table.put_item(
            Item={
                "statement_id": "990210b8-58bd-4e3b-a9be-46ee69b0f81e",
                "page_number": 1,
                "created_at": 1716185067451783268,
                "item_data": "[]",
                "template_id": "trans_bbox_a6561a13-a12a-48d9-94a7-20a5055dd63c",
                "transaction_count": 23,
                "updated_at": 1716185067451783268,
            }
        )
        transactions_table.put_item(
            Item={
                "statement_id": "990210b8-58bd-4e3b-a9be-46ee69b0f81e",
                "page_number": 2,
                "created_at": 1716185068252317415,
                "item_data": "[]",
                "template_id": "trans_bbox_a6561a13-a12a-48d9-94a7-20a5055dd63c",
                "transaction_count": 24,
                "updated_at": 1716185068252317415,
            }
        )
        transactions_table.put_item(
            Item={
                "statement_id": "e7affedd-3fe7-4124-bf3c-0b520bf4c5ab",
                "page_number": 0,
                "created_at": 1716185083874044758,
                "item_data": '[{"transaction_type": "debit", "transaction_note": "TO TRANSFER- UPI/DR/400452854191/SHREE HA/YESB/q065449877/UPI-", "chq_num": "TRANSFER TO 4897693162093", "amount": 362.0, "balance": 1889.41, "date": "2024-01-04 00:00:00", "optimizations": [], "transaction_channel": "upi", "transaction_channel_regex": ".*(UPI).*", "hash": "a667608f62a3835b6e31ae65802ca1ff", "unclean_merchant": "SHREE HA", "merchant_category": "", "description": "Transfer to SHREE HA", "is_lender": false, "merchant": "", "description_regex": "", "category": "Transfer to SHREE HA"}, {"transaction_type": "debit", "transaction_note": "TO TRANSFER- UPI/DR/437408815793/Shiv Sha/PYTM/paytmqr781/UPI-", "chq_num": "TRANSFER TO 4897690162095", "amount": 200.0, "balance": 1689.41, "date": "2024-01-08 00:00:00", "optimizations": [], "transaction_channel": "upi", "transaction_channel_regex": ".*(UPI).*", "hash": "673f50b3aea824389a4d63ce381e2393", "unclean_merchant": "SHIV SHA", "merchant_category": "ewallet", "description": "Transfer to SHIV SHA", "is_lender": false, "merchant": "", "description_regex": "", "category": "Transfer to SHIV SHA"}, {"transaction_type": "debit", "transaction_note": "TO TRANSFER- UPI/DR/437453728785/Sun Medi/PYTM/paytmqr16l/UPI-", "chq_num": "TRANSFER TO 4897690162095", "amount": 425.0, "balance": 1264.41, "date": "2024-01-08 00:00:00", "optimizations": [], "transaction_channel": "upi", "transaction_channel_regex": ".*(UPI).*", "hash": "7d4cef932ce4e9ca06c6b5d34ef7fedf", "unclean_merchant": "SUN MEDI", "merchant_category": "ewallet", "description": "Transfer to SUN MEDI", "is_lender": false, "merchant": "", "description_regex": "", "category": "Transfer to SUN MEDI"}, {"transaction_type": "debit", "transaction_note": "TO TRANSFER- UPI/DR/401190976217/PRAKA SHK/PUNB/prakashsol/UPI-", "chq_num": "TRANSFER TO 4897693162093", "amount": 400.0, "balance": 864.41, "date": "2024-01-11 00:00:00", "optimizations": [], "transaction_channel": "upi", "transaction_channel_regex": ".*(UPI).*", "hash": "7791bf5ece1ab6dcda743dd739e524b2", "unclean_merchant": "PRAKA SHK", "merchant_category": "", "description": "Transfer to PRAKA SHK", "is_lender": false, "merchant": "", "description_regex": "", "category": "Transfer to PRAKA SHK"}, {"transaction_type": "debit", "transaction_note": "TO TRANSFER- UPI/DR/401667041938/EURON ETG/ICIC/euronetgpa/UPI-", "chq_num": "TRANSFER TO 4897691162095", "amount": 250.0, "balance": 614.41, "date": "2024-01-16 00:00:00", "optimizations": [], "transaction_channel": "upi", "transaction_channel_regex": ".*(UPI).*", "hash": "09bc00f188a4c8c4d8a8758a99061479", "unclean_merchant": "EURON ETG", "merchant_category": "", "description": "Transfer to EURON ETG", "is_lender": false, "merchant": "", "description_regex": "", "category": "Transfer to EURON ETG"}, {"transaction_type": "debit", "transaction_note": "TO TRANSFER- UPI/DR/401915696787/EURON ETG/ICIC/euronetgpa/UPI-", "chq_num": "TRANSFER TO 4897695162091", "amount": 240.9, "balance": 373.51, "date": "2024-01-19 00:00:00", "optimizations": [], "transaction_channel": "upi", "transaction_channel_regex": ".*(UPI).*", "hash": "01cbafe1f543c74a8eb6c742cb264ab0", "unclean_merchant": "EURON ETG", "merchant_category": "", "description": "Transfer to EURON ETG", "is_lender": false, "merchant": "", "description_regex": "", "category": "Transfer to EURON ETG"}, {"transaction_type": "credit", "transaction_note": "BY TRANSFER- UPI/CR/401982507884/GOOG LEPAY/UTIB/goog-payme/UPI-", "chq_num": "TRANSFER FROM 4897737162096", "amount": 3.0, "balance": 376.51, "date": "2024-01-19 00:00:00", "optimizations": [], "transaction_channel": "upi", "transaction_channel_regex": ".*(UPI).*", "hash": "f0ad6b56d2e34dd441b8405abee2bf03", "unclean_merchant": "GOOG LEPAY", "merchant_category": "", "description": "cash_back", "is_lender": false, "merchant": "", "description_regex": "(?i)^BY\\\\s*TRANSFER[\\\\s\\\\-]+UPI\\\\/CR\\\\/[0-9]+\\\\/.*(GOOG[\\\\s\\\\/\\\\-]+PAYME).*", "category": "Cash Back"}, {"transaction_type": "debit", "transaction_note": "TO TRANSFER- UPI/DR/401915732479/billdesk/ ICIC/billdesk.r/UPI-", "chq_num": "TRANSFER TO 4897695162091", "amount": 240.9, "balance": 135.61, "date": "2024-01-19 00:00:00", "optimizations": [], "transaction_channel": "upi", "transaction_channel_regex": ".*(UPI).*", "hash": "0cf7ffdf526e83f780920c6484f74fe7", "unclean_merchant": "BILLDESK", "merchant_category": "bills", "description": "Transfer to BILLDESK", "is_lender": false, "merchant": "", "description_regex": "", "category": "Utilities"}, {"transaction_type": "credit", "transaction_note": "BY TRANSFER- UPI/CR/402339019455/NARES HKU/KARB/pnb1646@ok/UPI-", "chq_num": "TRANSFER FROM 4897733162090", "amount": 2000.0, "balance": 2135.61, "date": "2024-01-23 00:00:00", "optimizations": [], "transaction_channel": "upi", "transaction_channel_regex": ".*(UPI).*", "hash": "57f2fee7fe9da95880f7c3cf1bdff10e", "unclean_merchant": "NARES HKU", "merchant_category": "", "description": "Transfer from NARES HKU", "is_lender": false, "merchant": "", "description_regex": "", "category": "Transfer from NARES HKU"}, {"transaction_type": "debit", "transaction_note": "TO TRANSFER- UPI/DR/402343459361/KreditB ee/ICIC/kreditbee./UPI-", "chq_num": "TRANSFER TO 4897691162095", "amount": 1648.0, "balance": 487.61, "date": "2024-01-23 00:00:00", "optimizations": [], "transaction_channel": "upi", "transaction_channel_regex": ".*(UPI).*", "hash": "2ea1c32854cc98d2130a2b4e1eea11ea", "unclean_merchant": "KREDITB EE", "merchant_category": "loans", "description": "lender_transaction", "is_lender": true, "merchant": "kreditbe", "description_regex": "", "category": "Loan"}, {"transaction_type": "debit", "transaction_note": "TO TRANSFER- UPI/DR/402322083864/Dhanva nt/PYTM/paytmqr1of/UPI-", "chq_num": "TRANSFER TO 4897691162095", "amount": 224.0, "balance": 263.61, "date": "2024-01-23 00:00:00", "optimizations": [], "transaction_channel": "upi", "transaction_channel_regex": ".*(UPI).*", "hash": "575650cd279ae80940ab0a0779af5157", "unclean_merchant": "DHANVA NT", "merchant_category": "ewallet", "description": "Transfer to DHANVA NT", "is_lender": false, "merchant": "", "description_regex": "", "category": "Transfer to DHANVA NT"}, {"transaction_type": "debit", "transaction_note": "TO TRANSFER- UPI/DR/402802367079/EURON ETG/ICIC/euronetgpa/UPI-", "chq_num": "TRANSFER TO 4897696162090", "amount": 220.0, "balance": 43.61, "date": "2024-01-28 00:00:00", "optimizations": [], "transaction_channel": "upi", "transaction_channel_regex": ".*(UPI).*", "hash": "5a108965f585a4f0941ec06c068634fa", "unclean_merchant": "EURON ETG", "merchant_category": "", "description": "Transfer to EURON ETG", "is_lender": false, "merchant": "", "description_regex": "", "category": "Transfer to EURON ETG"}]',
                "template_id": "trans_bbox_a6561a13-a12a-48d9-94a7-20a5055dd63c",
                "transaction_count": 12,
                "updated_at": 1716185083874044758,
            }
        )
        transactions_table.put_item(
            Item={
                "statement_id": "e7affedd-3fe7-4124-bf3c-0b520bf4c5ab",
                "page_number": 1,
                "created_at": 1716185087977240681,
                "item_data": '[{"transaction_type": "credit", "transaction_note": "BY TRANSFER- UPI/CR/403814648446/PRAJA PAT/KARB/prajapatis/UPI-", "chq_num": "TRANSFER FROM 4897734162099", "amount": 500.0, "balance": 543.61, "date": "2024-02-07 00:00:00", "optimizations": [], "transaction_channel": "upi", "transaction_channel_regex": ".*(UPI).*", "hash": "5e8f31330c45bfdd41aa7e3372c3fdd1", "unclean_merchant": "PRAJA PAT", "merchant_category": "", "description": "Transfer from PRAJA PAT", "is_lender": false, "merchant": "", "description_regex": "", "category": "Transfer from PRAJA PAT"}, {"transaction_type": "debit", "transaction_note": "TO TRANSFER- UPI/DR/403930634952/EURON ETG/ICIC/euronetgpa/UPI-", "chq_num": "TRANSFER TO 4897693162093", "amount": 250.0, "balance": 293.61, "date": "2024-02-08 00:00:00", "optimizations": [], "transaction_channel": "upi", "transaction_channel_regex": ".*(UPI).*", "hash": "3fe9d9c5cf7d4b72aaa4a6542e1706f9", "unclean_merchant": "EURON ETG", "merchant_category": "", "description": "Transfer to EURON ETG", "is_lender": false, "merchant": "", "description_regex": "", "category": "Transfer to EURON ETG"}, {"transaction_type": "credit", "transaction_note": "BY TRANSFER-PFM PMSV- NB5240113437449202312F DAY PMSV RO 00ASGG-", "chq_num": "TRANSFER FROM 4697769105210", "amount": 18.0, "balance": 311.61, "date": "2024-02-08 00:00:00", "optimizations": [], "transaction_channel": "net_banking_transfer", "transaction_channel_regex": "(BY TRANSFER).*", "hash": "503a746db0303c874422972d5b1e34d0", "unclean_merchant": "", "merchant_category": "", "description": "", "is_lender": false, "merchant": "", "description_regex": "", "category": "Others"}, {"transaction_type": "credit", "transaction_note": "BY TRANSFER- UPI/CR/404238060702/SHIV LAL/BARB/sivlalsuth/UPI-", "chq_num": "TRANSFER FROM 4897738162095", "amount": 1.0, "balance": 312.61, "date": "2024-02-11 00:00:00", "optimizations": [], "transaction_channel": "upi", "transaction_channel_regex": ".*(UPI).*", "hash": "d9cf96290fd6df24afd43a41b4ce4e03", "unclean_merchant": "SHIV LAL", "merchant_category": "", "description": "Transfer from SHIV LAL", "is_lender": false, "merchant": "", "description_regex": "", "category": "Transfer from SHIV LAL"}, {"transaction_type": "credit", "transaction_note": "BY TRANSFER- UPI/CR/440835169655/SHIV LAL/BARB/sivlalsuth/UPI-", "chq_num": "TRANSFER FROM 4897738162095", "amount": 9099.0, "balance": 9411.61, "date": "2024-02-11 00:00:00", "optimizations": [], "transaction_channel": "upi", "transaction_channel_regex": ".*(UPI).*", "hash": "dcbbd4a704c79c884be80b193637354b", "unclean_merchant": "SHIV LAL", "merchant_category": "", "description": "Transfer from SHIV LAL", "is_lender": false, "merchant": "", "description_regex": "", "category": "Transfer from SHIV LAL"}, {"transaction_type": "credit", "transaction_note": "BY TRANSFER- UPI/CR/404234760771/SHIV LAL/BARB/sivlalsuth/UPI-", "chq_num": "TRANSFER FROM 4897738162095", "amount": 900.0, "balance": 10311.61, "date": "2024-02-11 00:00:00", "optimizations": [], "transaction_channel": "upi", "transaction_channel_regex": ".*(UPI).*", "hash": "53ed167adf7298762006aad23f88aaed", "unclean_merchant": "SHIV LAL", "merchant_category": "", "description": "Transfer from SHIV LAL", "is_lender": false, "merchant": "", "description_regex": "", "category": "Transfer from SHIV LAL"}, {"transaction_type": "debit", "transaction_note": "TO TRANSFER- UPI/DR/404282186511/KANCH AN /SBIN/arjunrajpu/UPI-", "chq_num": "TRANSFER TO 4897696162090", "amount": 500.0, "balance": 9811.61, "date": "2024-02-11 00:00:00", "optimizations": [], "transaction_channel": "upi", "transaction_channel_regex": ".*(UPI).*", "hash": "904fe62fbc1a6998a321c0a9fbb75fbf", "unclean_merchant": "KANCH AN ", "merchant_category": "", "description": "Transfer to KANCH AN ", "is_lender": false, "merchant": "", "description_regex": "", "category": "Transfer to KANCH AN"}, {"transaction_type": "debit", "transaction_note": "TO TRANSFER- UPI/DR/404293338888/Amazin g /UTIB/gpay-11171/UPI-", "chq_num": "TRANSFER TO 4897696162090", "amount": 3000.0, "balance": 6811.61, "date": "2024-02-11 00:00:00", "optimizations": [], "transaction_channel": "upi", "transaction_channel_regex": ".*(UPI).*", "hash": "7b86e17d7a8afcfaefcee5db9c466319", "unclean_merchant": "AMAZIN G ", "merchant_category": "", "description": "Transfer to AMAZIN G ", "is_lender": false, "merchant": "", "description_regex": "", "category": "Transfer to AMAZIN G"}, {"transaction_type": "debit", "transaction_note": "TO TRANSFER- UPI/DR/404458262991/YOGES HKU/BARB/yogeshshin/UPI-", "chq_num": "TRANSFER TO 4897691162095", "amount": 3000.0, "balance": 3811.61, "date": "2024-02-13 00:00:00", "optimizations": [], "transaction_channel": "upi", "transaction_channel_regex": ".*(UPI).*", "hash": "2c81c020dcffc411c7a001cfa8c66622", "unclean_merchant": "YOGES HKU", "merchant_category": "", "description": "Transfer to YOGES HKU", "is_lender": false, "merchant": "", "description_regex": "", "category": "Transfer to YOGES HKU"}, {"transaction_type": "debit", "transaction_note": "TO TRANSFER- UPI/DR/404514449134/YOGES HKU/BARB/yogeshshin/UPI-", "chq_num": "TRANSFER TO 4897692162094", "amount": 2000.0, "balance": 1811.61, "date": "2024-02-14 00:00:00", "optimizations": [], "transaction_channel": "upi", "transaction_channel_regex": ".*(UPI).*", "hash": "afc0ac1973d71d017b5bab59f8365851", "unclean_merchant": "YOGES HKU", "merchant_category": "", "description": "Transfer to YOGES HKU", "is_lender": false, "merchant": "", "description_regex": "", "category": "Transfer to YOGES HKU"}, {"transaction_type": "debit", "transaction_note": "TO TRANSFER- UPI/DR/404651577529/GUJAR AT /PYTM/paytm-6990/UPI-", "chq_num": "TRANSFER TO 4897693162093", "amount": 411.0, "balance": 1400.61, "date": "2024-02-15 00:00:00", "optimizations": [], "transaction_channel": "upi", "transaction_channel_regex": ".*(UPI).*", "hash": "9841c2e733674f4b50c2a05c7c6dc974", "unclean_merchant": "GUJAR AT ", "merchant_category": "ewallet", "description": "Transfer to GUJAR AT ", "is_lender": false, "merchant": "", "description_regex": "", "category": "Transfer to GUJAR AT"}, {"transaction_type": "debit", "transaction_note": "TO TRANSFER- UPI/DR/404665697130/EURON ETG/ICIC/euronetgpa/UPI-", "chq_num": "TRANSFER TO 4897694162092", "amount": 240.9, "balance": 1159.71, "date": "2024-02-15 00:00:00", "optimizations": [], "transaction_channel": "upi", "transaction_channel_regex": ".*(UPI).*", "hash": "3a68628168239443bd449912976d5635", "unclean_merchant": "EURON ETG", "merchant_category": "", "description": "Transfer to EURON ETG", "is_lender": false, "merchant": "", "description_regex": "", "category": "Transfer to EURON ETG"}, {"transaction_type": "credit", "transaction_note": "BY TRANSFER- UPI/CR/404718625181/ADITYA P/KKBK/winaditya@/Wood-", "chq_num": "TRANSFER FROM 4897736162097", "amount": 7060.0, "balance": 8219.71, "date": "2024-02-16 00:00:00", "optimizations": [], "transaction_channel": "upi", "transaction_channel_regex": ".*(UPI).*", "hash": "eb961222c7c41cd287434b24bd9b6e37", "unclean_merchant": "ADITYA P", "merchant_category": "", "description": "Transfer from ADITYA P", "is_lender": false, "merchant": "", "description_regex": "", "category": "Transfer from ADITYA P"}, {"transaction_type": "debit", "transaction_note": "TO TRANSFER- UPI/DR/404793289393/MODA RAM/PUNB/mmodaram50/UPI-", "chq_num": "TRANSFER TO 4897694162092", "amount": 1.0, "balance": 8218.71, "date": "2024-02-16 00:00:00", "optimizations": [], "transaction_channel": "upi", "transaction_channel_regex": ".*(UPI).*", "hash": "f175d1c0d16ca2877bb9cd8ba0fea0e6", "unclean_merchant": "MODA RAM", "merchant_category": "", "description": "Transfer to MODA RAM", "is_lender": false, "merchant": "", "description_regex": "", "category": "Transfer to MODA RAM"}, {"transaction_type": "debit", "transaction_note": "TO TRANSFER- UPI/DR/404793402672/MODA RAM/PUNB/mmodaram50/UPI-", "chq_num": "TRANSFER TO 4897694162092", "amount": 4000.0, "balance": 4218.71, "date": "2024-02-16 00:00:00", "optimizations": [], "transaction_channel": "upi", "transaction_channel_regex": ".*(UPI).*", "hash": "a96c75df041bb8cd824317d4d0518127", "unclean_merchant": "MODA RAM", "merchant_category": "", "description": "Transfer to MODA RAM", "is_lender": false, "merchant": "", "description_regex": "", "category": "Transfer to MODA RAM"}, {"transaction_type": "debit", "transaction_note": "TO TRANSFER- UPI/DR/404793634495/NARES HKU/KARB/pnb1646@ok/UPI-", "chq_num": "TRANSFER TO 4897694162092", "amount": 1500.0, "balance": 2718.71, "date": "2024-02-16 00:00:00", "optimizations": [], "transaction_channel": "upi", "transaction_channel_regex": ".*(UPI).*", "hash": "ded50e6c30ae221ae9d5a2d82c01946c", "unclean_merchant": "NARES HKU", "merchant_category": "", "description": "Transfer to NARES HKU", "is_lender": false, "merchant": "", "description_regex": "", "category": "Transfer to NARES HKU"}, {"transaction_type": "debit", "transaction_note": "TO TRANSFER- UPI/DR/441315723086/EURON ETG/ICIC/euronetgpa/UPI-", "chq_num": "TRANSFER TO 4897694162092", "amount": 240.9, "balance": 2477.81, "date": "2024-02-16 00:00:00", "optimizations": [], "transaction_channel": "upi", "transaction_channel_regex": ".*(UPI).*", "hash": "873841db04ca6654a1311f20417ae754", "unclean_merchant": "EURON ETG", "merchant_category": "", "description": "Transfer to EURON ETG", "is_lender": false, "merchant": "", "description_regex": "", "category": "Transfer to EURON ETG"}, {"transaction_type": "debit", "transaction_note": "TO TRANSFER- UPI/DR/404832172372/ASHIS H P/HDFC/ashrimali1/UPI-", "chq_num": "TRANSFER TO 4897695162091", "amount": 1200.0, "balance": 1277.81, "date": "2024-02-17 00:00:00", "optimizations": [], "transaction_channel": "upi", "transaction_channel_regex": ".*(UPI).*", "hash": "066f4f43b9e60e41513a675c431c968a", "unclean_merchant": "ASHIS H P", "merchant_category": "", "description": "Transfer to ASHIS H P", "is_lender": false, "merchant": "", "description_regex": "", "category": "Transfer to ASHIS H P"}, {"transaction_type": "debit", "transaction_note": "TO TRANSFER- UPI/DR/405265310504/EURON ETG/ICIC/euronetgpa/UPI-", "chq_num": "TRANSFER TO 4897692162094", "amount": 250.0, "balance": 1027.81, "date": "2024-02-21 00:00:00", "optimizations": [], "transaction_channel": "upi", "transaction_channel_regex": ".*(UPI).*", "hash": "8ac302635ea29456ee2da23aaeeb7471", "unclean_merchant": "EURON ETG", "merchant_category": "", "description": "Transfer to EURON ETG", "is_lender": false, "merchant": "", "description_regex": "", "category": "Transfer to EURON ETG"}, {"transaction_type": "credit", "transaction_note": "BY TRANSFER- UPI/CR/405295277170/GOOG LEPAY/UTIB/goog- payme/Rewa-", "chq_num": "TRANSFER FROM 4897734162099", "amount": 6.0, "balance": 1033.81, "date": "2024-02-21 00:00:00", "optimizations": [], "transaction_channel": "upi", "transaction_channel_regex": ".*(UPI).*", "hash": "ec0c8b415b08db92a9ac6b00b0d25dc0", "unclean_merchant": "GOOG LEPAY", "merchant_category": "", "description": "cash_back", "is_lender": false, "merchant": "", "description_regex": "(?i)^BY\\\\s*TRANSFER[\\\\s\\\\-]+UPI\\\\/CR\\\\/[0-9]+\\\\/.*(GOOG[\\\\s\\\\/\\\\-]+PAYME).*", "category": "Cash Back"}, {"transaction_type": "credit", "transaction_note": "BY TRANSFER- UPI/CR/405299940324/NEW MELD/IDFB/rameshpraj/UPI-", "chq_num": "TRANSFER FROM 4897734162099", "amount": 5000.0, "balance": 6033.81, "date": "2024-02-21 00:00:00", "optimizations": [], "transaction_channel": "upi", "transaction_channel_regex": ".*(UPI).*", "hash": "c7565c59cebe234a45613afd65a85c4d", "unclean_merchant": "NEW MELD", "merchant_category": "", "description": "Transfer from NEW MELD", "is_lender": false, "merchant": "", "description_regex": "", "category": "Transfer from NEW MELD"}, {"transaction_type": "debit", "transaction_note": "TO TRANSFER- UPI/DR/405256846665/JKTHA KOR/KCCB/q348392261/UPI-", "chq_num": "TRANSFER TO 4897692162094", "amount": 140.0, "balance": 5893.81, "date": "2024-02-21 00:00:00", "optimizations": [], "transaction_channel": "upi", "transaction_channel_regex": ".*(UPI).*", "hash": "c27a07b39155a7b55ef4617dde18b181", "unclean_merchant": "JKTHA KOR", "merchant_category": "", "description": "Transfer to JKTHA KOR", "is_lender": false, "merchant": "", "description_regex": "", "category": "Transfer to JKTHA KOR"}, {"transaction_type": "debit", "transaction_note": "TO TRANSFER- UPI/DR/405278911097/SHREE MA/YESB/q568181933/UPI-", "chq_num": "TRANSFER TO 4897692162094", "amount": 150.0, "balance": 5743.81, "date": "2024-02-21 00:00:00", "optimizations": [], "transaction_channel": "upi", "transaction_channel_regex": ".*(UPI).*", "hash": "01867247f9e018c957100e89079d6fe0", "unclean_merchant": "SHREE MA", "merchant_category": "", "description": "Transfer to SHREE MA", "is_lender": false, "merchant": "", "description_regex": "", "category": "Transfer to SHREE MA"}]',
                "template_id": "trans_bbox_a6561a13-a12a-48d9-94a7-20a5055dd63c",
                "transaction_count": 23,
                "updated_at": 1716185087977240681,
            }
        )
        transactions_table.put_item(
            Item={
                "statement_id": "e7affedd-3fe7-4124-bf3c-0b520bf4c5ab",
                "page_number": 2,
                "created_at": 1716185088534783936,
                "item_data": '[{"transaction_type": "debit", "transaction_note": "TO TRANSFER- UPI/DR/405382186926/AEVLO N M/PYTM/paytmqr1mp/UPI-", "chq_num": "TRANSFER TO 4897693162093", "amount": 2400.0, "balance": 3343.81, "date": "2024-02-22 00:00:00", "optimizations": [], "transaction_channel": "upi", "transaction_channel_regex": ".*(UPI).*", "hash": "6920220d131b61afbb4defe606b8a86d", "unclean_merchant": "AEVLO N M", "merchant_category": "ewallet", "description": "Transfer to AEVLO N M", "is_lender": false, "merchant": "", "description_regex": "", "category": "Transfer to AEVLO N M"}, {"transaction_type": "debit", "transaction_note": "TO TRANSFER- UPI/DR/405340032742/AEVLO N M/PYTM/paytmqr281/UPI-", "chq_num": "TRANSFER TO 4897693162093", "amount": 125.0, "balance": 3218.81, "date": "2024-02-22 00:00:00", "optimizations": [], "transaction_channel": "upi", "transaction_channel_regex": ".*(UPI).*", "hash": "b2b45e4dc0164bb1994817a06f453465", "unclean_merchant": "AEVLO N M", "merchant_category": "ewallet", "description": "Transfer to AEVLO N M", "is_lender": false, "merchant": "", "description_regex": "", "category": "Transfer to AEVLO N M"}, {"transaction_type": "debit", "transaction_note": "TO TRANSFER- UPI/DR/405450938410/Krazyb ee/KKBK/cf.krazybe/kredi-", "chq_num": "TRANSFER TO 4897694162092", "amount": 1648.0, "balance": 1570.81, "date": "2024-02-23 00:00:00", "optimizations": [], "transaction_channel": "upi", "transaction_channel_regex": ".*(UPI).*", "hash": "160c1f64f59985555b898be270e2997a", "unclean_merchant": "KRAZYB EE", "merchant_category": "loans", "description": "lender_transaction", "is_lender": true, "merchant": "krazybee", "description_regex": "", "category": "Loan"}, {"transaction_type": "debit", "transaction_note": "TO TRANSFER- UPI/DR/405598866301/KRISH ANA/UTIB/gpay-11223/UPI-", "chq_num": "TRANSFER TO 4897695162091", "amount": 5.0, "balance": 1565.81, "date": "2024-02-24 00:00:00", "optimizations": [], "transaction_channel": "upi", "transaction_channel_regex": ".*(UPI).*", "hash": "b9112bd322ec3d7980dd845eacf94575", "unclean_merchant": "KRISH ANA", "merchant_category": "", "description": "Transfer to KRISH ANA", "is_lender": false, "merchant": "", "description_regex": "", "category": "Transfer to KRISH ANA"}, {"transaction_type": "credit", "transaction_note": "BY TRANSFER- UPI/CR/442191187130/NEW MELD/IDFB/rameshpraj/UPI-", "chq_num": "TRANSFER FROM 4897737162096", "amount": 2500.0, "balance": 4065.81, "date": "2024-02-24 00:00:00", "optimizations": [], "transaction_channel": "upi", "transaction_channel_regex": ".*(UPI).*", "hash": "f44f93dec41017b01b956d32bf9af2da", "unclean_merchant": "NEW MELD", "merchant_category": "", "description": "Transfer from NEW MELD", "is_lender": false, "merchant": "", "description_regex": "", "category": "Transfer from NEW MELD"}, {"transaction_type": "credit", "transaction_note": "BY TRANSFER- UPI/CR/405846955447/VIJAY VI/BARB/jaybhole71/UPI-", "chq_num": "TRANSFER FROM 4897733162090", "amount": 100.0, "balance": 4165.81, "date": "2024-02-27 00:00:00", "optimizations": [], "transaction_channel": "upi", "transaction_channel_regex": ".*(UPI).*", "hash": "5ed827ffbd842ee1138201acccdebf8c", "unclean_merchant": "VIJAY VI", "merchant_category": "", "description": "Transfer from VIJAY VI", "is_lender": false, "merchant": "", "description_regex": "", "category": "Transfer from VIJAY VI"}, {"transaction_type": "debit", "transaction_note": "TO TRANSFER- UPI/DR/442692908243/bajajpa y/INDB/bajajpay.6/UPI-", "chq_num": "TRANSFER TO 4897693162093", "amount": 52.0, "balance": 4113.81, "date": "2024-02-29 00:00:00", "optimizations": [], "transaction_channel": "upi", "transaction_channel_regex": ".*(UPI).*", "hash": "d3b36ee9a620c0152ade47d3dd5a18e8", "unclean_merchant": "BAJAJPA Y", "merchant_category": "", "description": "Transfer to BAJAJPA Y", "is_lender": false, "merchant": "", "description_regex": "", "category": "Transfer to BAJAJPA Y"}, {"transaction_type": "debit", "transaction_note": "TO TRANSFER- UPI/DR/406413919674/EURON ETG/ICIC/euronetgpa/UPI-", "chq_num": "TRANSFER TO 4897690162095", "amount": 250.0, "balance": 3863.81, "date": "2024-03-04 00:00:00", "optimizations": [], "transaction_channel": "upi", "transaction_channel_regex": ".*(UPI).*", "hash": "067d34e789e547a04e53c55b0cd280e3", "unclean_merchant": "EURON ETG", "merchant_category": "", "description": "Transfer to EURON ETG", "is_lender": false, "merchant": "", "description_regex": "", "category": "Transfer to EURON ETG"}, {"transaction_type": "credit", "transaction_note": "CASH DEPOSIT-CASH DEPOSIT SELF-", "chq_num": "", "amount": 49000.0, "balance": 52863.81, "date": "2024-03-05 00:00:00", "optimizations": [], "transaction_channel": "cash_deposit", "transaction_channel_regex": "^([\\\\-\\\\s]*(?:DEPOSIT|\\\\s*)[\\\\s\\\\-]*CASH\\\\s*DEPOSIT.*(?:AT|SELF)\\\\s*).*", "hash": "d7c8e2247a4edf78317343d1089e1202", "unclean_merchant": "", "merchant_category": "", "description": "", "is_lender": false, "merchant": "", "description_regex": "", "category": "Cash Deposit"}, {"transaction_type": "credit", "transaction_note": "CASH DEPOSIT-CASH DEPOSIT SELF-", "chq_num": "", "amount": 49000.0, "balance": 101863.81, "date": "2024-03-06 00:00:00", "optimizations": [], "transaction_channel": "cash_deposit", "transaction_channel_regex": "^([\\\\-\\\\s]*(?:DEPOSIT|\\\\s*)[\\\\s\\\\-]*CASH\\\\s*DEPOSIT.*(?:AT|SELF)\\\\s*).*", "hash": "e29c56a2c826d809b36723f51f1fda68", "unclean_merchant": "", "merchant_category": "", "description": "", "is_lender": false, "merchant": "", "description_regex": "", "category": "Cash Deposit"}, {"transaction_type": "debit", "transaction_note": "TO TRANSFER- UPI/DR/406761222480/PRAJA PAT/KARB/prajapatis/UPI-", "chq_num": "TRANSFER TO 4897693162093", "amount": 10000.0, "balance": 91863.81, "date": "2024-03-07 00:00:00", "optimizations": [], "transaction_channel": "upi", "transaction_channel_regex": ".*(UPI).*", "hash": "ccb6597474b374d8dba6ebf63898078e", "unclean_merchant": "PRAJA PAT", "merchant_category": "", "description": "Transfer to PRAJA PAT", "is_lender": false, "merchant": "", "description_regex": "", "category": "Transfer to PRAJA PAT"}, {"transaction_type": "debit", "transaction_note": "TO TRANSFER- UPI/DR/406761331215/PRAJA PAT/KARB/prajapatis/UPI-", "chq_num": "TRANSFER TO 4897693162093", "amount": 10000.0, "balance": 81863.81, "date": "2024-03-07 00:00:00", "optimizations": [], "transaction_channel": "upi", "transaction_channel_regex": ".*(UPI).*", "hash": "16aefe89d2c7068879770b70ba80e1d8", "unclean_merchant": "PRAJA PAT", "merchant_category": "", "description": "Transfer to PRAJA PAT", "is_lender": false, "merchant": "", "description_regex": "", "category": "Transfer to PRAJA PAT"}, {"transaction_type": "debit", "transaction_note": "TO TRANSFER- UPI/DR/406762489303/PRAJA PAT/KARB/prajapatis/UPI-", "chq_num": "TRANSFER TO 4897693162093", "amount": 10000.0, "balance": 71863.81, "date": "2024-03-07 00:00:00", "optimizations": [], "transaction_channel": "upi", "transaction_channel_regex": ".*(UPI).*", "hash": "3930b8a4359281d92484c98b59cfff4d", "unclean_merchant": "PRAJA PAT", "merchant_category": "", "description": "Transfer to PRAJA PAT", "is_lender": false, "merchant": "", "description_regex": "", "category": "Transfer to PRAJA PAT"}, {"transaction_type": "debit", "transaction_note": "TO TRANSFER- UPI/DR/406762529464/PRAJA PAT/KARB/prajapatis/UPI-", "chq_num": "TRANSFER TO 4897693162093", "amount": 10000.0, "balance": 61863.81, "date": "2024-03-07 00:00:00", "optimizations": [], "transaction_channel": "upi", "transaction_channel_regex": ".*(UPI).*", "hash": "c76d896426347c65695ee9846635aebb", "unclean_merchant": "PRAJA PAT", "merchant_category": "", "description": "Transfer to PRAJA PAT", "is_lender": false, "merchant": "", "description_regex": "", "category": "Transfer to PRAJA PAT"}, {"transaction_type": "debit", "transaction_note": "TO TRANSFER- UPI/DR/406762706890/PRAJA PAT/KARB/prajapatis/UPI-", "chq_num": "TRANSFER TO 4897693162093", "amount": 5000.0, "balance": 56863.81, "date": "2024-03-07 00:00:00", "optimizations": [], "transaction_channel": "upi", "transaction_channel_regex": ".*(UPI).*", "hash": "463def695a3cdd93dce0d998b88513a5", "unclean_merchant": "PRAJA PAT", "merchant_category": "", "description": "Transfer to PRAJA PAT", "is_lender": false, "merchant": "", "description_regex": "", "category": "Transfer to PRAJA PAT"}, {"transaction_type": "debit", "transaction_note": "TO TRANSFER- UPI/DR/406762737737/PRAJA PAT/KARB/prajapatis/UPI-", "chq_num": "TRANSFER TO 4897693162093", "amount": 5000.0, "balance": 51863.81, "date": "2024-03-07 00:00:00", "optimizations": [], "transaction_channel": "upi", "transaction_channel_regex": ".*(UPI).*", "hash": "dd674869116f8725c5e531747bc3f80c", "unclean_merchant": "PRAJA PAT", "merchant_category": "", "description": "Transfer to PRAJA PAT", "is_lender": false, "merchant": "", "description_regex": "", "category": "Transfer to PRAJA PAT"}, {"transaction_type": "debit", "transaction_note": "TO TRANSFER- UPI/DR/406762783639/PRAJA PAT/KARB/prajapatis/UPI-", "chq_num": "TRANSFER TO 4897693162093", "amount": 10000.0, "balance": 41863.81, "date": "2024-03-07 00:00:00", "optimizations": [], "transaction_channel": "upi", "transaction_channel_regex": ".*(UPI).*", "hash": "45fe790de4dff3e306d2fc557e8e9ec3", "unclean_merchant": "PRAJA PAT", "merchant_category": "", "description": "Transfer to PRAJA PAT", "is_lender": false, "merchant": "", "description_regex": "", "category": "Transfer to PRAJA PAT"}, {"transaction_type": "debit", "transaction_note": "TO TRANSFER- UPI/DR/406762789119/PRAJA PAT/KARB/prajapatis/UPI-", "chq_num": "TRANSFER TO 4897693162093", "amount": 20000.0, "balance": 21863.81, "date": "2024-03-07 00:00:00", "optimizations": [], "transaction_channel": "upi", "transaction_channel_regex": ".*(UPI).*", "hash": "cb23f9cbace52f77785c48797a226b09", "unclean_merchant": "PRAJA PAT", "merchant_category": "", "description": "Transfer to PRAJA PAT", "is_lender": false, "merchant": "", "description_regex": "", "category": "Transfer to PRAJA PAT"}, {"transaction_type": "debit", "transaction_note": "TO TRANSFER- UPI/DR/406762809666/PRAJA PAT/KARB/prajapatis/UPI-", "chq_num": "TRANSFER TO 4897693162093", "amount": 18000.0, "balance": 3863.81, "date": "2024-03-07 00:00:00", "optimizations": [], "transaction_channel": "upi", "transaction_channel_regex": ".*(UPI).*", "hash": "e2014ae3aadc5fb7a54a11652796a281", "unclean_merchant": "PRAJA PAT", "merchant_category": "", "description": "Transfer to PRAJA PAT", "is_lender": false, "merchant": "", "description_regex": "", "category": "Transfer to PRAJA PAT"}, {"transaction_type": "debit", "transaction_note": "TO TRANSFER- UPI/DR/406766606471/NARES HKU/KARB/pnb1646@ok/UPI-", "chq_num": "TRANSFER TO 4897693162093", "amount": 1000.0, "balance": 2863.81, "date": "2024-03-07 00:00:00", "optimizations": [], "transaction_channel": "upi", "transaction_channel_regex": ".*(UPI).*", "hash": "624b85b29046f25806447e2843457df2", "unclean_merchant": "NARES HKU", "merchant_category": "", "description": "Transfer to NARES HKU", "is_lender": false, "merchant": "", "description_regex": "", "category": "Transfer to NARES HKU"}, {"transaction_type": "debit", "transaction_note": "TO TRANSFER- UPI/DR/406899865372/KANCH AN /SBIN/arjunrajpu/UPI-", "chq_num": "TRANSFER TO 4897694162092", "amount": 400.0, "balance": 2463.81, "date": "2024-03-08 00:00:00", "optimizations": [], "transaction_channel": "upi", "transaction_channel_regex": ".*(UPI).*", "hash": "a0dec657e7e93dc08f116a68e833f159", "unclean_merchant": "KANCH AN ", "merchant_category": "", "description": "Transfer to KANCH AN ", "is_lender": false, "merchant": "", "description_regex": "", "category": "Transfer to KANCH AN"}, {"transaction_type": "credit", "transaction_note": "BY TRANSFER-PFM PMSV- NBC241285195190202401F DAY PMSV RO 00ASGG-", "chq_num": "TRANSFER FROM 4697778105219", "amount": 12.0, "balance": 2475.81, "date": "2024-03-14 00:00:00", "optimizations": [], "transaction_channel": "net_banking_transfer", "transaction_channel_regex": "(BY TRANSFER).*", "hash": "57212be4238ba1651e322a97c5563ab0", "unclean_merchant": "", "merchant_category": "", "description": "", "is_lender": false, "merchant": "", "description_regex": "", "category": "Others"}, {"transaction_type": "debit", "transaction_note": "TO TRANSFER- UPI/DR/407494996265/SONAT AN /SBIN/sona31901@/UPI-", "chq_num": "TRANSFER TO 4897693162093", "amount": 200.0, "balance": 2275.81, "date": "2024-03-14 00:00:00", "optimizations": [], "transaction_channel": "upi", "transaction_channel_regex": ".*(UPI).*", "hash": "6e802c6a7115679010888e0302a70e31", "unclean_merchant": "SONAT AN ", "merchant_category": "", "description": "Transfer to SONAT AN ", "is_lender": false, "merchant": "", "description_regex": "", "category": "Transfer to SONAT AN"}, {"transaction_type": "debit", "transaction_note": "TO TRANSFER- UPI/DR/407495082250/SONAT AN /SBIN/sona31901@/UPI-", "chq_num": "TRANSFER TO 4897693162093", "amount": 100.0, "balance": 2175.81, "date": "2024-03-14 00:00:00", "optimizations": [], "transaction_channel": "upi", "transaction_channel_regex": ".*(UPI).*", "hash": "56eaf995534848241554bbd8fa396639", "unclean_merchant": "SONAT AN ", "merchant_category": "", "description": "Transfer to SONAT AN ", "is_lender": false, "merchant": "", "description_regex": "", "category": "Transfer to SONAT AN"}]',
                "template_id": "trans_bbox_a6561a13-a12a-48d9-94a7-20a5055dd63c",
                "transaction_count": 24,
                "updated_at": 1716185088534783936,
            }
        )
        transactions_table.put_item(
            Item={
                "statement_id": "e7affedd-3fe7-4124-bf3c-0b520bf4c5ab",
                "page_number": 3,
                "created_at": 1716185074602049028,
                "item_data": '[{"transaction_type": "debit", "transaction_note": "TO TRANSFER- UPI/DR/407715277144/Google I/UTIB/gpaybillpa/UPI-", "chq_num": "TRANSFER TO 4897696162090", "amount": 250.0, "balance": 1925.81, "date": "2024-03-17 00:00:00", "optimizations": [], "transaction_channel": "upi", "transaction_channel_regex": ".*(UPI).*", "hash": "171eaec4b56f4b600f07f599d91a55ab", "unclean_merchant": "GOOGLE I", "merchant_category": "bills", "description": "Transfer to GOOGLE I", "is_lender": false, "merchant": "", "description_regex": "", "category": "Utilities"}, {"transaction_type": "debit", "transaction_note": "TO TRANSFER- UPI/DR/407715371864/EURON ETG/ICIC/euronetgpa/UPI-", "chq_num": "TRANSFER TO 4897696162090", "amount": 250.0, "balance": 1675.81, "date": "2024-03-17 00:00:00", "optimizations": [], "transaction_channel": "upi", "transaction_channel_regex": ".*(UPI).*", "hash": "fbdadcbaae23764bf514b7269b3dddfc", "unclean_merchant": "EURON ETG", "merchant_category": "", "description": "Transfer to EURON ETG", "is_lender": false, "merchant": "", "description_regex": "", "category": "Transfer to EURON ETG"}, {"transaction_type": "debit", "transaction_note": "TO TRANSFER- UPI/DR/407729771414/Janta fa/YESB/paytmqrop3/UPI-", "chq_num": "TRANSFER TO 4897696162090", "amount": 65.0, "balance": 1610.81, "date": "2024-03-17 00:00:00", "optimizations": [], "transaction_channel": "upi", "transaction_channel_regex": ".*(UPI).*", "hash": "b88130c6f911901e670a5a011f2b91ab", "unclean_merchant": "JANTA FA", "merchant_category": "ewallet", "description": "Transfer to JANTA FA", "is_lender": false, "merchant": "", "description_regex": "", "category": "Transfer to JANTA FA"}, {"transaction_type": "debit", "transaction_note": "TO TRANSFER- UPI/DR/407888119296/Shivsha n/YESB/paytmqrjgr/UPI-", "chq_num": "TRANSFER TO 4897690162095", "amount": 300.0, "balance": 1310.81, "date": "2024-03-18 00:00:00", "optimizations": [], "transaction_channel": "upi", "transaction_channel_regex": ".*(UPI).*", "hash": "f973d50a9f2fd6f1162f252ea5416c14", "unclean_merchant": "SHIVSHA N", "merchant_category": "ewallet", "description": "Transfer to SHIVSHA N", "is_lender": false, "merchant": "", "description_regex": "", "category": "Transfer to SHIVSHA N"}, {"transaction_type": "credit", "transaction_note": "BY TRANSFER-INB IMPS407914005705/99999999 99/XX0000/Transferfu-", "chq_num": "MAL00006612226 4 MAL00006612226 4", "amount": 1.0, "balance": 1311.81, "date": "2024-03-19 00:00:00", "optimizations": [], "transaction_channel": "net_banking_transfer", "transaction_channel_regex": ".*(IMPS).*", "hash": "053ebd2f8a01e6ca86f92bd831befbfd", "unclean_merchant": "", "merchant_category": "", "description": "", "is_lender": false, "merchant": "", "description_regex": "", "category": "Others"}]',
                "template_id": "trans_bbox_a6561a13-a12a-48d9-94a7-20a5055dd63c",
                "transaction_count": 5,
                "updated_at": 1716185074602049028,
            }
        )

        # 2: get_account_transactions_test data
        account_table.put_item(
            Item={
                "entity_id": "b1456a4c-6398-4805-a015-63574376fa92",
                "account_id": "b1456a4c-6398-4805-a015-63574376fa92",
                "created_at": 1707088320070624740,
                "item_data": {
                    "account_id": "10573e9d-2990-4567-b51d-83517d0571bf",
                    "account_category": "corporate",
                    "account_number": "10170004550047",
                    "account_opening_date": None,
                    "bank": "bandhan",
                    "credit_limit": None,
                    "ifsc": "BDBL0001048",
                    "input_account_category": None,
                    "input_is_od_account": None,
                    "is_od_account": None,
                    "micr": "732750504",
                    "missing_data": [],
                    "od_limit": None,
                    "salary_confidence": None,
                    "statements": ["d45276e0-7133-48a9-89e2-db195e82a685"],
                                            "dob": "",
                        "email": "",
                        "pan_number": "",
                        "phone_number": "",
                        "account_status": "",
                    "holder_type": ""
                },
                "updated_at": 1707088384271743942,
            }
        )
        statement_table.put_item(
            Item={
                "statement_id": "d45276e0-7133-48a9-89e2-db195e82a685",
                "created_at": "1707088320",
                "identity_status": "completed",
                "message": None,
                "metadata_fraud_status": "completed",
                "pages_done": 155,
                "page_count": 155,
                "page_identity_fraud_status": "completed",
                "processing_status": "completed",
                "transactions_status": "completed",
            }
        )
        transactions_table.put_item(
            Item={
                "statement_id": "d45276e0-7133-48a9-89e2-db195e82a685",
                "page_number": 0,
                "created_at": 1707088324312692383,
                "item_data": '[{"transaction_type": "credit", "transaction_note": "UPI-MR ANNAMALAI-9585782195@IBL-IDIB000B 059-451792742454-PAYMENT FROM PHONE", "chq_num": null, "amount": 100.0, "balance": 298.5, "date": "2024-05-30 00:00:00", "optimizations": [], "transaction_channel": "upi", "transaction_channel_regex": ".*(UPI).*", "hash": "25bc1992f9793f5e5d1fbd630a369d2a", "unclean_merchant": "MR ANNAMALAI", "merchant_category": "", "description": "Transfer from MR ANNAMALAI", "is_lender": false, "merchant": "", "description_regex": "", "category": "Transfer from MR ANNAMALAI"}, {"transaction_type": "debit", "transaction_note": "UPI-ARAVINDHAN S-9600879603@YBL-SBIN000 0929-451781370931-PAYMENT FROM PHONE", "chq_num": null, "amount": 100.0, "balance": 198.5, "date": "2024-05-30 00:00:00", "optimizations": [], "transaction_channel": "upi", "transaction_channel_regex": ".*(UPI).*", "hash": "86c000b6c87e0767390a239feba51fa4", "unclean_merchant": "ARAVINDHAN S", "merchant_category": "", "description": "self_transfer", "is_lender": false, "merchant": "", "description_regex": "", "category": "Self Transfer"}, {"transaction_type": "debit", "transaction_note": "UPI-KANNAN S-Q281413143@YBL-YESB0YBLUPI 451751357176-PAYMENT FROM PHONE", "chq_num": null, "amount": 115.0, "balance": 83.5, "date": "2024-05-30 00:00:00", "optimizations": [], "transaction_channel": "upi", "transaction_channel_regex": ".*(UPI).*", "hash": "564b095ceb640d0901b16ed04199b8c8", "unclean_merchant": "KANNAN S", "merchant_category": "", "description": "Transfer to KANNAN S", "is_lender": false, "merchant": "", "description_regex": "", "category": "Transfer to KANNAN S"}, {"transaction_type": "debit", "transaction_note": "UPI-MURUGAN N-PAYTMQR2810050501011DEAAZ8 ZCXD3@PAYTM-YESB0PTMUPI-415242478103-PAY MENT FROM PHONE STATEMENT SUMMARY :-", "chq_num": null, "amount": 40.0, "balance": 43.5, "date": "2024-05-31 00:00:00", "optimizations": [], "transaction_channel": "upi", "transaction_channel_regex": ".*(UPI).*", "hash": "cff2b1132bb9e5c39f68ccfb09100c97", "unclean_merchant": "MURUGAN N", "merchant_category": "", "description": "Transfer to MURUGAN N", "is_lender": false, "merchant": "", "description_regex": "", "category": "Transfer to MURUGAN N"}]',
                "transaction_count": 4,
                "updated_at": 1707088324312692383,
            }
        )

        account_table.put_item(
            Item={
                "entity_id": "4e1bf704-8dad-42f1-8be1-af99cd19afe5",
                "account_id": "1145da38-a41b-4d8e-8bac-249ea471c4ed",
                "created_at": 1707483950483616537,
                "item_data": {
                    "account_id": "1145da38-a41b-4d8e-8bac-249ea471c4ed",
                    "account_category": None,
                    "account_number": "061001528895",
                    "bank": "icici",
                    "credit_limit": None,
                    "ifsc": None,
                    "is_od_account": False,
                    "micr": None,
                    "missing_data": [],
                    "neg_txn_od": False,
                    "od_limit": 0,
                    "salary_confidence": 100,
                    "statements": [
                        "4c92ccd7-587d-4f64-9ef7-223685d0d833",
                        "c63f7d52-ea1d-4974-920a-ffb1c04d0d2d",
                        "a721e845-17e0-457d-b287-3a6c258da12b",
                        "dd9f3e6d-dcc4-49fd-a12d-fa0ca45fc5ff",
                        "b510afa6-37de-4765-a7f5-e86e92853bf7",
                        "fa05d2ee-0f34-441a-b612-197ba61821b1",
                    ],
                },
                "updated_at": 1713259408264416583,
            }
        )
        salary_transactions_table.put_item(
            Item={
                "account_id": "1145da38-a41b-4d8e-8bac-249ea471c4ed",
                "created_at": 1713259409305323043,
                "item_data": '[{"transaction_type": "credit", "transaction_note": "ACH/SAL-AMAZONDEVELCENTI/Sal  Adv Dec 104861382", "amount": 76900.0, "balance": 76900.0, "date": "2022-12-01 00:00:00", "transaction_channel": "salary", "unclean_merchant": "", "merchant_category": "shopping", "description": "", "is_lender": false, "merchant": "", "hash": "0a6d4699d6bd8bb8f1b5a78206f1193d", "page_number": 0, "sequence_number": 0, "account_id": "1145da38-a41b-4d8e-8bac-249ea471c4ed", "chq_num": "", "transaction_channel_regex": "", "description_regex": "", "category": "", "employer_name": "", "calculation_method": "keyword", "month_year": "12-2022", "salary_month": "Nov-2022"}, {"transaction_type": "credit", "transaction_note": "ACH/SAL-AMAZONDEVELCENTI/Sal  Adv Feb 104861382 0", "amount": 77200.0, "balance": 77200.0, "date": "2023-02-02 00:00:00", "transaction_channel": "salary", "unclean_merchant": "", "merchant_category": "shopping", "description": "", "is_lender": false, "merchant": "", "hash": "380169fd4fd393109dec4722a72c05bc", "page_number": 2, "sequence_number": 29, "account_id": "1145da38-a41b-4d8e-8bac-249ea471c4ed", "chq_num": "", "transaction_channel_regex": "", "description_regex": "", "category": "", "employer_name": "", "calculation_method": "keyword", "month_year": "2-2023", "salary_month": "Jan-2023"}, {"transaction_type": "credit", "transaction_note": "ACH/SAL-AMAZONDEVELCENTI/Sal  Adv Mar 104861382 0", "amount": 64487.0, "balance": 64487.0, "date": "2023-03-02 00:00:00", "transaction_channel": "salary", "unclean_merchant": "", "merchant_category": "shopping", "description": "", "is_lender": false, "merchant": "", "hash": "fd3a8bae42a83885fb7b90e8182a6554", "page_number": 3, "sequence_number": 26, "account_id": "1145da38-a41b-4d8e-8bac-249ea471c4ed", "chq_num": "", "transaction_channel_regex": "", "description_regex": "", "category": "", "employer_name": "", "calculation_method": "keyword", "month_year": "3-2023", "salary_month": "Feb-2023"}, {"transaction_type": "credit", "transaction_note": "ACH/SAL-AMAZONDEVELCENTI/Sal  Adv May 104861382 0", "amount": 85000.0, "balance": 85000.0, "date": "2023-05-03 00:00:00", "transaction_channel": "salary", "unclean_merchant": "", "merchant_category": "shopping", "description": "", "is_lender": false, "merchant": "", "hash": "c4d8f77434e7e87d8f2675573aae4919", "page_number": 12, "sequence_number": 17, "account_id": "1145da38-a41b-4d8e-8bac-249ea471c4ed", "chq_num": "", "transaction_channel_regex": "", "description_regex": "", "category": "", "employer_name": "", "calculation_method": "keyword", "month_year": "5-2023", "salary_month": "Apr-2023"}, {"transaction_type": "credit", "transaction_note": "3 ACH/SAL-AMAZONDEVELCENTI/SAL", "amount": 81900.0, "balance": 81900.0, "date": "2024-02-02 00:00:00", "transaction_channel": "salary", "unclean_merchant": "", "merchant_category": "", "description": "", "is_lender": false, "merchant": "", "hash": "41f3a69bb28d330f9ad2beb91ac36c4e", "page_number": 8, "sequence_number": 3, "account_id": "1145da38-a41b-4d8e-8bac-249ea471c4ed", "chq_num": " -", "transaction_channel_regex": "(?i)(.*[^A-Za-z]+SAL[^A-Za-z]+.*)", "description_regex": "", "category": "Salary", "employer_name": "", "calculation_method": "keyword", "month_year": "2-2024", "salary_month": "Jan-2024"}, {"transaction_type": "credit", "transaction_note": "2 ACH/SAL-AMAZONDEVELCENTI/MAR SAL 2024 104861382 0", "amount": 57888.0, "balance": 57888.0, "date": "2024-03-28 00:00:00", "transaction_channel": "salary", "unclean_merchant": "", "merchant_category": "", "description": "", "is_lender": false, "merchant": "", "hash": "155903f40110e28b8fa1545a28a94467", "page_number": 8, "sequence_number": 13, "account_id": "1145da38-a41b-4d8e-8bac-249ea471c4ed", "chq_num": " -  ", "transaction_channel_regex": "(?i)(.*[^A-Za-z]+SAL[^A-Za-z]+.*)", "description_regex": "", "category": "Salary", "employer_name": "", "calculation_method": "keyword", "month_year": "4-2024", "salary_month": "Feb-2024"}, {"transaction_type": "credit", "transaction_note": "76C3A941 ACH/SAL-AMAZONDEVELCENTI/SAL", "amount": 79017.0, "balance": 79017.0, "date": "2024-04-02 00:00:00", "transaction_channel": "salary", "unclean_merchant": "", "merchant_category": "", "description": "", "is_lender": false, "merchant": "", "hash": "c5c516ba386b2c1b87570586190edaa9", "page_number": 8, "sequence_number": 21, "account_id": "1145da38-a41b-4d8e-8bac-249ea471c4ed", "chq_num": " -", "transaction_channel_regex": "(?i)(.*[^A-Za-z]+SAL[^A-Za-z]+.*)", "description_regex": "", "category": "Salary", "employer_name": "", "calculation_method": "keyword", "month_year": "4-2024", "salary_month": "Mar-2024"}]',
                "updated_at": 1713259409305323043,
            }
        )
        yield dynamodb
