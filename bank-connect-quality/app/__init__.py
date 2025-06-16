import os
import sentry_sdk
from sentry_sdk.integrations.aws_lambda import AwsLambdaIntegration

STAGE = os.environ.get('STAGE', 'dev')

if STAGE.lower() in []:
    sentry_sdk.init(
        dsn = "https://840178fcb958b8007a776ae628cdf8dc@o82232.ingest.sentry.io/4506120980791296",
        integrations = [AwsLambdaIntegration()],
        environment = STAGE
    )