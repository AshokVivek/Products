import traceback

from botocore.exceptions import ClientError

from python.aggregates import get_accounts_for_entity
from python.configs import LAMBDA_LOGGER, s3, BANK_CONNECT_REPORTS_BUCKET
from python.utils import sync_invoke_aggregate_xlsx_report_handler


def s3_fetch_xlsx(entity_id, fan_out_info_dashboard_resp, xlsx_folder, log_data):
    to_reject_account = fan_out_info_dashboard_resp.get("to_reject_account", False)
    all_accounts = get_accounts_for_entity(entity_id, to_reject_account)
    for account in all_accounts:
        account_id = account.get('account_id')
        # Download xlsx
        try:
            s3.download_file(BANK_CONNECT_REPORTS_BUCKET, f"account_report_{account_id}.xlsx", f"{xlsx_folder}/xlsx_report_{entity_id}.xlsx")
            return
            #TODO: For now assuming TCAP sessions have only one account.
        except ClientError as e:
            log_data.upsert(exception=str(e), trace=traceback.format_exc())
            LAMBDA_LOGGER.warning("xlsx download failed", extra=log_data.store)
            log_data.remove_keys(["exception", "trace"])


def s3_fetch_aggregate_xlsx(entity_id, fan_out_info_dashboard_resp, xlsx_folder, log_data):
    """
        This method is currently being unused.
    """
    api_subscriptions = fan_out_info_dashboard_resp.get("api_subscriptions", [])
    if "aggregate_xlsx_report_url" in api_subscriptions:
        return

    attempt_type_data = fan_out_info_dashboard_resp.get("attempt_type_data", {})
    aggregate_excel_report_version = fan_out_info_dashboard_resp.get("aggregate_excel_report_version", "v1")
    is_sme = fan_out_info_dashboard_resp.get("is_sme", False)
    adjusted_eod = fan_out_info_dashboard_resp.get("adjusted_eod", False)
    to_remap_predictors = fan_out_info_dashboard_resp.get("to_remap_predictors", False)
    ignore_self_transfer = fan_out_info_dashboard_resp.get("ignore_self_transfer", False)
    to_reject_account = fan_out_info_dashboard_resp.get("to_reject_account", False)
    session_flow = fan_out_info_dashboard_resp.get("session_flow", False)
    session_date_range = fan_out_info_dashboard_resp.get("session_date_range", {})

    payload = {
        "entity_id": entity_id,
        "attempt_type_data": attempt_type_data,
        "aggregate_excel_report_version": aggregate_excel_report_version,
        "is_sme": is_sme,
        "adjusted_eod": adjusted_eod,
        "to_remap_predictors": to_remap_predictors,
        "ignore_self_transfer": ignore_self_transfer,
        "to_reject_account": to_reject_account,
        "caching_enabled": True,
        "session_dict": {
            "is_session_flow": session_flow,
            "from_date": session_date_range.get("from_date"),
            "to_date": session_date_range.get("to_date"),
        },
    }

    # Generate Aggregate xlsx
    sync_invoke_aggregate_xlsx_report_handler(payload)

    # Download Aggregate xlsx
    try:
        s3.download_file(BANK_CONNECT_REPORTS_BUCKET, f"entity_report_{entity_id}.xlsx", f"{xlsx_folder}/xlsx_report_{entity_id}.xlsx")
    except ClientError as e:
        log_data.upsert(exception=str(e), trace=traceback.format_exc())
        LAMBDA_LOGGER.warning("xlsx download failed", extra=log_data.store)
        log_data.remove_keys(["exception", "trace"])
