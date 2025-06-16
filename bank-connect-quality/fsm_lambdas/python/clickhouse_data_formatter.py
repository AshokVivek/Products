import traceback
from datetime import datetime

from python.configs import s3, LAMBDA_LOGGER, BANK_CONNECT_REPORTS_BUCKET


def prepare_tcap_recurring_pulls_data(entity_id, fan_out_info_dashboard_resp, local_logging_context):
    aa_session_details = fan_out_info_dashboard_resp.get("aa_session_details", dict())
    xml_report_format = fan_out_info_dashboard_resp.get("report_format",
                                                        fan_out_info_dashboard_resp.get("session_metadata").get(
                                                            "report_format", "analysis"))

    if not aa_session_details:
        LAMBDA_LOGGER.warning("No aa_session_details found for creating recurring pull data",
                              extra=local_logging_context.store)

    key_mappings = {
        "webtop_id": "webtop_id",
        "aa_vendor": "aa_vendor",
        "customer_id": "customer_id_",
        "from_date": "from_date",
        "to_date": "to_date",
        "consent_id": "consent_id_",
        "consent_type": "consent_type_",
        "consent_expiry": "consent_expiry_",
        "consent_status": "consent_status_",
        "failure_code": "failure_code_",
        "failure_reason": "failure_reason_",
        "analysis_XML": "analysis",
        "raw_XML": "raw",
        "balance_XML": "balance",
        "created_at": "created_date"
    }

    final_pull_data = {}
    final_pull_data['updated_at'] = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')

    try:
        xml_file_key = f"xml_report/entity_report_{entity_id}_original.xml"
        xml_file_object = s3.get_object(Bucket=BANK_CONNECT_REPORTS_BUCKET, Key=xml_file_key)
        xml_object = xml_file_object["Body"].read().decode()
        aa_session_details[xml_report_format] = xml_object
    except Exception as e:
        local_logging_context.upsert(exception=str(e), trace=traceback.format_exc())
        LAMBDA_LOGGER.warning("Failed to fetch xml for tcap recurring pull", extra=local_logging_context.store)
        local_logging_context.remove_keys(["exception", "trace"])

    for key, value in key_mappings.items():
        if value in aa_session_details.keys() and aa_session_details[value] not in [None, ""]:
            final_pull_data[key] = aa_session_details[value]

    return final_pull_data
