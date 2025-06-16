import json
import io
from datetime import datetime, date

import pandas as pd
import django

django.setup()

from django.core.files.base import ContentFile

from fiu_module.models import RecurringConsentBulkPull, AccountAggregatorSession
from fiu_module.views import check_consent_id_and_trigger_recurring_pull, update_and_get_job_completion_count, job_completion_check

from bank_connect.bc_config_utils import get_org_config_from_redis
from bank_connect.constants import CONSENT_TYPE_PERIODIC, EMAIL_DELIVERY_KAFKA_TOPIC

from sentry_sdk import capture_exception, set_context, capture_message

from bank_connect_kafka_consumers.producer import KafkaProducerSingleton

def send_job_status_report(recurring_bulk_pull_obj, df, file_type='csv'):
    """
    {
        "customer_id": consent_obj.customer_id,
        "message": ''
    }
    """
    successful_customers = recurring_bulk_pull_obj.successful_customers
    failed_customers = recurring_bulk_pull_obj.failed_customers
    df['Data Pull Status'] = ''
    df['Failure Reason'] = ''

    # Iterate through the list of dictionaries
    for customer in successful_customers:
        # Find the customer_id and update the status
        customer = json.loads(customer)
        print("Successful customer data = {}".format(customer))
        df.loc[df['aa_handle'] == customer['customer_id'], 'Data Pull Status'] = 'Success'

    # Iterate through the list of dictionaries
    for customer in failed_customers:
        # Find the customer_id and update the status
        customer = json.loads(customer)
        df.loc[df['aa_handle'] == customer['customer_id'], 'Data Pull Status'] = 'Failed'
        df.loc[df['aa_handle'] == customer['customer_id'], 'Failure Reason'] = customer.get('failure_reason', "Failed to get Reason")

    # Convert DataFrame to CSV format in-memory
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False)

    recurring_bulk_pull_obj.final_status_file.save(f"{recurring_bulk_pull_obj.job_id}_status_report.{file_type}", ContentFile(csv_buffer.getvalue()))

    # send Email

    email_customer = recurring_bulk_pull_obj.uploaded_by.email if recurring_bulk_pull_obj.uploaded_by else None

    if not email_customer:
        raise Exception("To Email address not found to send status report email")

    to_email = email_customer
    subject = f"Bulk AA Status Report for {recurring_bulk_pull_obj.job_id} - {date.today().strftime('%B %d, %Y')}"
    body = f"""
            <!DOCTYPE html>
            <html lang="en">
            <head>
                <meta charset="UTF-8">
                <meta name="viewport" content="width=device-width, initial-scale=1.0">
                <title>Bulk AA Status Report</title>
            </head>
            <body>
                <table width="100%" border="0" cellspacing="0" cellpadding="0">
                    <tr>
                        <td align="center">
                            <table width="600" border="0" cellspacing="0" cellpadding="20" style="border:1px solid #e0e0e0;">
                                <tr>
                                    <td align="center" bgcolor="#007bff" style="color: white;">
                                        <h2>Bulk AA Status Report</h2>
                                    </td>
                                </tr>
                                <tr>
                                    <td>
                                        <p>Hi Team,</p>
                                        <p>Please find attached the Bulk AA Status report for <strong>{date.today().strftime('%B %d, %Y')}</strong>.</p>
                                        <p>If you have any questions or need further details, feel free to reach out. We're here to assist with any clarifications or additional insights you may require.</p>
                                        <p>Looking forward to your feedback.</p>
                                        <p>Thank you,</p>
                                        <p>Team FinBox</p>
                                        <p><a href="https://finbox.in/" target="_blank">https://finbox.in/</a></p>
                                    </td>
                                </tr>
                                <tr>
                                    <td align="center" style="font-size: 12px; color: #888;">
                                        <p>&copy; 2024 FinBox. All rights reserved.</p>
                                    </td>
                                </tr>
                            </table>
                        </td>
                    </tr>
                </table>
            </body>
            </html>
        """
    attachments = [
        {
            "attachment_path": recurring_bulk_pull_obj.final_status_file.url if recurring_bulk_pull_obj.final_status_file else None,
            "attachment_filename": f"Recurring_bulk_pull_status_report_{recurring_bulk_pull_obj.job_id}",
            "file_extension": file_type
        }
    ]

    email_details = {
        "to_emails_ids": to_email,
        "subject": subject,
        "plain_body": body,
        "html_body": body,
        "attachments": attachments
    }

    kafka_producer_ = KafkaProducerSingleton()
    is_successful = kafka_producer_.send(topic=EMAIL_DELIVERY_KAFKA_TOPIC, value=email_details)
    if not is_successful:
        raise Exception(f"Failed to send message to EMAIL_DELIVERY_KAFKA_TOPIC, hence email sending failed")

    print(f"Status Report for JOB_ID : {recurring_bulk_pull_obj.job_id} Successfully sent")
    return True


def recurring_bulk_pull_consumer(recurring_pull_details):
    set_context("recurring_pull_details", recurring_pull_details)
    try:
        print("Recurring Bulk pull", recurring_pull_details)
        job_id = recurring_pull_details.get("job_id",None)
        if not job_id:
            raise ValueError("JOB ID is missing")
        recurring_bulk_pull_obj = RecurringConsentBulkPull.objects.filter(job_id=job_id).first()
        if not recurring_bulk_pull_obj:
            raise ValueError("JOB ID is matching job does not exist")

        file_type = None
        file_url = recurring_bulk_pull_obj.file.url
        organization = recurring_bulk_pull_obj.organization
        redis_org_config = get_org_config_from_redis(organization)

        if recurring_bulk_pull_obj.file.name.endswith(".csv"):
            file_type = "csv"
            df = pd.read_csv(file_url)
        elif recurring_bulk_pull_obj.file.name.endswith("xlsx"):
            file_type = "xlsx"
            df = pd.read_excel(file_url)

        if df.empty:
            raise ValueError("File failed while loading into DataFrame")

        initiate_report_generation = recurring_pull_details.get("initiate_report_generation", False)
        if initiate_report_generation:
            send_job_status_report(recurring_bulk_pull_obj, df, file_type)
            return True

        failed_customers= []
        current_completion_count = total_user_count = None
        for index, row in df.iterrows():
            try:
                customer_id = row.get("aa_handle")
                consent_id = row.get("consent_id")
                start_date = row.get("start_date")
                end_date = row.get("end_date")

                start_date = datetime.strptime(start_date, '%Y-%m-%d')
                end_date = datetime.strptime(end_date, '%Y-%m-%d')
                report_format = row.get("report_format")

                aa_session = AccountAggregatorSession.objects.filter(organization=organization,
                                                                     customer_id=customer_id,
                                                                     consent_id=consent_id,
                                                                     consent_type=CONSENT_TYPE_PERIODIC
                                                                     )
                if len(aa_session):
                    status = check_consent_id_and_trigger_recurring_pull(
                        consent_id=consent_id,
                        from_date=start_date,
                        to_date=end_date,
                        organization=organization,
                        session_flow=redis_org_config.get("session_flow", False),
                        job_id=job_id,
                        session_meta_data={
                            "report_format": report_format
                        }
                    )

                    if status.get("data", {}).get("error", None):
                        failed_customer = {
                            "customer_id": row["aa_handle"],
                            "failure_reason": status.get("data", {}).get("error", ""),
                        }

                        failed_customers.append(json.dumps(failed_customer))
                        current_completion_count, total_user_count = update_and_get_job_completion_count(recurring_bulk_pull_obj)

                else:
                    failed_customer = {
                        "customer_id": row["aa_handle"],
                        "failure_reason": "Matching customer_id does not exist"
                    }
                    failed_customers.append(json.dumps(failed_customer))
                    current_completion_count, total_user_count = update_and_get_job_completion_count(recurring_bulk_pull_obj)
            except Exception as e:
                print(f"Inside the inner exception block: {e} and row: {row}")
                set_context("erroneous_row", row)
                capture_exception(e)
                failed_customer = {
                    "customer_id": row["aa_handle"],
                    "failure_reason": "Internal Server Error"
                }
                failed_customers.append(json.dumps(failed_customer))
                current_completion_count, total_user_count = update_and_get_job_completion_count(recurring_bulk_pull_obj)


        recurring_bulk_pull_obj.failed_customers.extend(failed_customers)
        recurring_bulk_pull_obj.completed_batch_count += 1
        recurring_bulk_pull_obj.save()

        if current_completion_count and total_user_count:
            job_completion_check(current_completion_count, total_user_count, recurring_bulk_pull_obj)

        return True
    except Exception as e:
        print(f"Inside the outer exception block : {e}")
        capture_exception(e)
        return False
