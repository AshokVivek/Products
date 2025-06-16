import os
import sentry_sdk
from sentry_sdk.integrations.aws_lambda import AwsLambdaIntegration

ENABLE_SENTRY = os.environ.get('ENABLE_SENTRY', False) in ["true", "1", "t"]
CURRENT_STAGE = os.environ.get('CURRENT_STAGE', 'dev')
IS_SERVER = os.environ.get('IS_SERVER', False) in ["true", "1", "t"]
SUBSCRIPTION_TYPE = os.environ.get("SUBSCRIPTION_TYPE", False)


# if ENABLE_SENTRY and (not IS_SERVER or SUBSCRIPTION_TYPE):
if CURRENT_STAGE.lower() == "prod":
    sentry_sdk.init(
        dsn=os.environ['SENTRY_DSN'],
        integrations=[AwsLambdaIntegration()],
        environment=CURRENT_STAGE
    )