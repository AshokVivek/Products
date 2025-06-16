import os

DEFAULT_AWS_REGION = 'ap-south-1'
AWS_REGION = os.environ.get("AWS_REGION", DEFAULT_AWS_REGION)
COUNTRY = "IN"
if AWS_REGION == "ap-southeast-3":
    COUNTRY = "ID"
STAGE = os.environ.get("STAGE", "dev")

# KAFKA SETTINGS
KAFKA_BROKER_URLS = os.environ.get("KAFKA_BROKER_URL")
