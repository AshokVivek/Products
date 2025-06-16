import os

import django

django.setup()

from django.db.models import F
from django.conf import settings

import pandas as pd
from datetime import datetime, timedelta, timezone
from sentry_sdk import capture_exception, set_context, capture_message

from finbox_dashboard.models import Organization
from fiu_module.models import RecurringConsentBulkPull, AccountAggregatorSession
from bank_connect.models import OrgEmailConfiguration
from bank_connect.constants import EMAIL_DELIVERY_KAFKA_TOPIC, CONSENT_STATUS_REPORT_EMAIL_TYPE
from bank_connect.clickhouse.firehose import send_data_to_firehose
from bank_connect.clickhouse.push_to_clickhouse import prepare_tcap_daily_aa_consent_data
from utils.aws import put_file_to_s3, get_s3_presigned_url_from_s3_url

from bank_connect_kafka_consumers.producer import KafkaProducerSingleton

BANK_CONNECT_REPORTS_BUCKET = settings.BANK_CONNECT_REPORTS_BUCKET


def remove_file(file_path):
    if os.path.exists(file_path):
        os.remove(file_path)


def push_consents_to_datalake(aa_session_details):
    try:
        for aa_session_detail in aa_session_details:
            tcap_daily_aa_consents_data = prepare_tcap_daily_aa_consent_data(aa_session_detail)
            send_data_to_firehose([tcap_daily_aa_consents_data], settings.TCAP_DAILY_AA_CONSENTS_STREAM_NAME)
    except Exception as e:
        print("Issue while pushing the data to firehose: {}".format(e))
    return


def reports_daily_consents(task_details=None):
    file_path = ""
    set_context("reports_daily_consents", task_details)
    try:
        organization_id = settings.TCAP_ORGANIZATION_ID

        if not organization_id:
            raise ValueError("organization_id is missing")

        organization = Organization.objects.filter(id=organization_id)
        if not organization:
            raise ValueError(f"Organization does not exist. Organization_id : {organization_id}")

        organization = organization.first()
        current_date = datetime.now(tz=timezone.utc).date()

        aa_session_details = AccountAggregatorSession.objects.filter(
            organization_id=organization_id,
            created_at__date__lte=current_date,
            created_at__date__gte=current_date - timedelta(hours=24),
            is_recurring_pull=False
        ).values(
            webtop_id=F('bank_connect_session__metadata__webtopNo'),
            created_date=F('created_at__date'),
            aa_vendor=F('aa_entity'),
            phone_number=F('customer_phone_number'),
            customer_id_ = F('customer_id'),
            from_date=F('date_time_range_from__date'),
            to_date=F('date_time_range_to__date'),
            consent_id_=F('consent_id'),
            consent_type_ = F('consent_type'),
            consent_expiry_ = F('consent_expiry'),
            consent_status_ = F('consent_status'),
            failure_code_ = F('failure_code'),
            failure_reason_ = F('failure_reason'),
        ).order_by('created_at')
        if not aa_session_details:
            aa_session_details = [{"Status": f"No consents processed between {current_date} and {current_date - timedelta(hours=24)}"}]
        else:
            push_consents_to_datalake(aa_session_details)

        aa_session_df = pd.DataFrame(aa_session_details)

        final_column_names= []
        for column in aa_session_df.columns:
            final_column_names.append(" ".join(column.split("_")).title())
        aa_session_df.columns = final_column_names

        file_path = f"{organization_id}_daily_consents_report_{current_date}.csv"
        aa_session_df.to_csv(file_path)
        s3_url = put_file_to_s3(file_path, BANK_CONNECT_REPORTS_BUCKET, file_path)
        s3_presigned_url = get_s3_presigned_url_from_s3_url(s3_url)

        # send email

        email_config = OrgEmailConfiguration.objects.filter(organization=organization, email_type=CONSENT_STATUS_REPORT_EMAIL_TYPE)
        if not email_config:
            raise ValueError(f"No email configuration found for {organization_id} for email type : {CONSENT_STATUS_REPORT_EMAIL_TYPE}")
        to_email = email_config[0].to_email
        cc_email = email_config[0].cc_email
        bcc_email = email_config[0].bcc_email
        subject = email_config[0].subject # Has a blank place to format current date into it
        body = email_config[0].body # Has a blank place to format current date into it
        attachments = [
            {
                "attachment_path": s3_presigned_url,
                "attachment_filename": f"AA_daily_consent_status_report_{datetime.now().strftime('%Y%m%d%H%M')}",
                "file_extension": "csv"
            }
        ]

        email_details = {
            "to_emails_ids"  : to_email,
            "subject" : subject.format(datetime.now().date()),
            "plain_body" : body.format(datetime.now().date()),
            "html_body": body.format(datetime.now().date()),
            "attachments" : attachments
        }

        kafka_producer_ = KafkaProducerSingleton()
        is_successful = kafka_producer_.send(topic=EMAIL_DELIVERY_KAFKA_TOPIC, value=email_details)
        if not is_successful:
            raise Exception(f"Failed to send message to EMAIL_DELIVERY_KAFKA_TOPIC, hence email sending failed")
        remove_file(file_path)
        return True
    except Exception as e:
        remove_file(file_path)
        capture_exception(e)
        return False