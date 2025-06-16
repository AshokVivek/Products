import datetime
import threading

from python.bc_apis import (
    get_bank_connect_predictors, get_bank_connect_score_from_lambda,
    get_bank_connect_monthly_analysis, get_bank_connect_eod_balances
)

from python.configs import LAMBDA_LOGGER
from python.context.logging import LoggingContext
from python.handlers import access_handler
from python.output_handler_mapping import (
    REQUIRED_DATA_TYPES_TO_FUNCTION_MAP, FUNCTIONS_TO_ARGUMENT_MAP,
    FUNCTION_TO_RETURN_VALUE_MAP, JSON_OUTPUT_FORMAT_FUNCTION_MAP,
    LOCAL_FUNCTION_ARGUMENT_MAP,
    SUMMARY_INFO_TOTAL_FIELD_MAP, SUMMARY_INFO_AVERAGE_FIELD_MAP,
    ACCOUNT_ANALYSIS_MONTHLY_DETAIL,
    ADDITIONAL_ABB_SUMMARY_DETAILS, ADDITIONAL_OVERALL_DETAILS,
    ADDITIONAL_MONTHLY_DETAILS
)
from typing import Union
from python.aggregates import get_disparities_from_ddb, get_fraud_for_account
from python.constants import FRAUD_TO_CATEGORY_MAPPING


def get_customer_info(entity_id, account_list, bank_mapping):
    """
    Generating the customer related information corresponding to each bank account
    :param entity_id:
    :param account_list:
    :param bank_mapping:
    :returns: Currently returning only the information dictionary corresponding to first bank account
    """
    customer_info = []
    for account_data in account_list:
        account_id = account_data.get("account_id")
        customer_info.append(
            {
                "session_id": entity_id,
                "address": account_data.get("address", ""),
                "bank": bank_mapping.get(account_id, {}).get("full_bank_name", ""),
                "name": account_data.get("name", ""),
                "email": account_data.get("email", ""),
                "pan": account_data.get("pan", ""),
                "instId": bank_mapping.get(account_id, {}).get("perfios_institution_id"),
                "landline": bank_mapping.get(account_id, {}).get("landline"),
                "mobile": bank_mapping.get(account_id, {}).get("mobile"),
                # "perfiosTransactionId": bank_mapping.get(account_id, {}).get("perfios_transaction_id"),
                # "customerTransactionId": bank_mapping.get(account_id, {}).get("customerTransaction_id")
            }
        )

    return customer_info[0]


def get_statement_details(account_list, entity_id, entity_fraud_dict, bank_mapping, to_reject_account):
    """
    Generating the statement details of all the accounts associated with entity_id
    :param account_list:
    :param entity_id:
    :param entity_fraud_dict:
    :param bank_mapping:
    :param to_reject_account:
    :returns: list of dictionaries with the statement details
    """
    dict_ = {
        "StatementRemarks": {"remarks": ""},
        "Statement": list()
    }

    statement_stats = access_handler(
        {"entity_id": entity_id, "to_reject_account": to_reject_account, "access_type": "STATEMENT_STATS"}, None)
    statement_stat_dict = get_statement_details_data(statement_stats)

    for account_data in account_list:
        account_id = account_data.get("account_id", "")

        for frauds in entity_fraud_dict.get(account_id, {}).get("fraud_type", {}):
            if statement_stat_dict.get(frauds["account_id"], {}).get(frauds["statement_id"], {}):
                status = "FAILED" if frauds["fraud_type"] == "author_fraud" else "VERIFIED"
                statement_stat_dict[frauds["account_id"]][frauds["statement_id"]]["status"] = status

        statements = statement_stat_dict.get(account_id, {})

        for statement_id, statement_data in statements.items():
            statement_dict = {
                "fileName": statement_data.get("file_name", ""),
                "statement_id": statement_id,
                "statementStatus": statement_data.get("status", ""),
                "CustomerInfo": {
                    "session_id": entity_id,
                    "address": account_data.get("address", ""),
                    "bank": bank_mapping.get(account_id, {}).get("full_bank_name", ""),
                    "email": account_data.get("email", ""),
                    "landline": bank_mapping.get(account_id, {}).get("landline", ""),
                    "mobile": bank_mapping.get(account_id, {}).get("mobile", ""),
                    "name": account_data.get("name", ""),
                    "pan": account_data.get("pan", "")
                },
                "StatementAccounts": {
                    "StatementAccount": {
                        "accountID": account_id,
                        "accountNo": account_data.get("account_number", ""),
                        "accountType": account_data.get("account_category", ""),
                        "xnsEndDate": statement_data.get("last_transaction_timestamp", ""),
                        "xnsStartDate": statement_data.get("first_transaction_timestamp", "")
                    }
                }
            }
            dict_["Statement"].append(statement_dict)

    return dict_


def get_account_analysis_loan_track_details(monthly_analysis_data):
    """
    Generate Loan tracking detail for a particular account
    :param monthly_analysis_data:
    :returns: List of dictionaries containing loan tracking details
    """
    all_loan_transactions = {}
    loan_transaction_data = []

    if monthly_analysis_data.get("loan_emi", ""):
        for loan_emi_month, loans in monthly_analysis_data["loan_emi"].items():
            for category in loans:
                category_loans = loans[category]
                for payment in category_loans:
                    payment_date = payment.get("date", "")
                    if not payment_date:
                        continue
                    formatted_date = datetime.datetime.strptime(payment_date, "%Y-%m-%d %H:%M:%S")
                    formatted_date = formatted_date.strftime("%d-%b-%y")

                    key = "{}_{}".format(category, payment["amount"])
                    if key in all_loan_transactions:
                        all_loan_transactions[key]["dates"] += " {}".format(formatted_date)
                    else:
                        all_loan_transactions[key] = {
                            "dates": formatted_date
                        }
        if all_loan_transactions:
            for key in all_loan_transactions:
                split_key = key.split("_")
                category = split_key[0]
                amount = split_key[1]
                loan_transaction_data.append({
                    "amount": amount,
                    "category": category,
                    "dates": all_loan_transactions[key]["dates"]
                })

    return loan_transaction_data


def get_regular_credit_debit_dict(group, transaction_data):
    """
    Generate the transaction data in specified format
    :param group:
    :param transaction_data:
    :returns: dictionary with formatted transaction information
    """
    dict_ = {
        "amount": transaction_data.get("amount", ""),
        "balance": transaction_data.get("balance", ""),
        "date": transaction_data.get("date", ""),
        "narration": transaction_data.get("transaction_note", ""),
        "chqNo": transaction_data.get("chq_num", ""),
        "category": transaction_data.get("description", ""),
        "group": group
    }
    fetched_account_id = transaction_data.get("account_id", "")

    return dict_, fetched_account_id


def get_top_payments_received(recurring_transactions):
    """
    Get top 10 payments received
    :param recurring_transactions:
    :returns: Top 10 received payments
    """
    counter = 10
    dict_ = {}
    response_data = {
        "Item": list()
    }
    for transaction_object in recurring_transactions["credit_transactions"]:
        for single_transaction in transaction_object.get("transactions", {}):
            if single_transaction["clean_transaction_note"] in dict_:
                dict_[single_transaction["clean_transaction_note"]]["amount"] += single_transaction["amount"]
                dict_[single_transaction["clean_transaction_note"]]["count"] += 1
            else:
                dict_[single_transaction["clean_transaction_note"]] = {
                    "amount": single_transaction["amount"],
                    "count": 1
                }

    ordered_dict = {k: v for k, v in sorted(dict_.items(), key=lambda item: item[1]["amount"], reverse=True)}

    for party_name in ordered_dict:
        if counter == 0:
            break
        response_data["Item"].append(
            {
                "amount": str(ordered_dict[party_name]["amount"]),
                "count": str(ordered_dict[party_name]["count"]),
                "party": party_name
            }
        )
        counter -= 1
    return response_data


def get_account_analysis_regular_credits(account_id, recurring_transactions):
    """
    Get the list of regular credits
    :param account_id:
    :param recurring_transactions:
    :returns: all the regular credits
    """
    regular_credits = dict()
    regular_credits["RXn"] = list()
    for group, transaction_object in enumerate(recurring_transactions["credit_transactions"], start=1):
        for single_transaction in transaction_object.get("transactions", {}):
            dict_, fetched_account_id = get_regular_credit_debit_dict(group, single_transaction)
            if account_id == fetched_account_id:
                regular_credits["RXn"].append(dict_)

    return regular_credits


def get_account_analysis_regular_debits(account_id, recurring_transactions):
    """
    Get the list of regular debits
    :param account_id:
    :param recurring_transactions:
    :returns: all the regular debits
    """
    regular_debits = dict()
    regular_debits["RXn"] = list()

    for group, transaction_object in enumerate(recurring_transactions["debit_transactions"], start=1):
        for single_transaction in transaction_object.get("transactions", ""):
            dict_, fetched_account_id = get_regular_credit_debit_dict(group, single_transaction)
            if account_id == fetched_account_id:
                regular_debits["RXn"].append(dict_)

    return regular_debits


def get_account_analysis_monthly_details_data(monthly_analysis, predictors, month_list):
    """
    Get the monthly details data
    :param monthly_analysis:
    :param predictors:
    :param month_list:
    :returns: month wise detailed dictionary
    """
    monthly_details_list = []
    if monthly_analysis != {}:
        for month in month_list:
            item = {
                "monthName": month
            }

            for field in ACCOUNT_ANALYSIS_MONTHLY_DETAIL:
                argument_list = ACCOUNT_ANALYSIS_MONTHLY_DETAIL[field]
                item[field] = locals()[argument_list[0]].get(argument_list[1], {}).get(month, "")

            monthly_details_list.append(item)

    return monthly_details_list


def get_account_analysis_eod_balances(eod_balances, account_id, credit_limit):
    """
    Get the End of Day balances for a given account
    :param eod_balances:
    :param account_id:
    :param credit_limit:
    :returns: List of the balances at the end of every day fetched from the statement
    """
    eod_balances = eod_balances.get(account_id, {})
    eod_balance_list = []
    monthly_detail = []
    month_list = []
    if eod_balances != {}:
        for month, start_date in zip(eod_balances["Months_order"], eod_balances["start_date"]):
            start_datetime = datetime.datetime.strptime(start_date, "%d-%b-%y")
            month_datetime = datetime.datetime.strptime(month, "%b-%y").strftime("%b-%Y")
            credit_limit_overdrwan_days = 0
            start_date_index = start_datetime.day - 1

            cur_month_eod_balances = eod_balances[month][start_date_index:]
            for day_to_add, eod_balance in enumerate(cur_month_eod_balances):
                if eod_balance is None:
                    continue
                if credit_limit is not None and eod_balance < -1 * abs(credit_limit):
                    credit_limit_overdrwan_days += 1
                formatted_eod_balance = {
                    "date": str((start_datetime + datetime.timedelta(days=day_to_add)).date()),
                    "balance": eod_balance
                }
                eod_balance_list.append(formatted_eod_balance)
            monthly_detail.append(
                {
                    "monthName": month_datetime,
                    "startDate": str(start_datetime.date())
                }
            )
            month_list.append(month_datetime)
    return eod_balance_list, monthly_detail, month_list


def update_account_analysis_summary_info(predictors):
    """
    Summarised information about an account
    :param predictors:
    :returns: A dictionary having the total and the average summary data of the account
    """
    summary_total = dict()
    summary_average = dict()
    updated_dict = dict()

    for field in SUMMARY_INFO_TOTAL_FIELD_MAP:
        argument_list = SUMMARY_INFO_TOTAL_FIELD_MAP[field]
        summary_total[field] = locals()[argument_list[0]].get(argument_list[1], 0)

    for field in SUMMARY_INFO_AVERAGE_FIELD_MAP:
        argument_list = SUMMARY_INFO_AVERAGE_FIELD_MAP[field]
        summary_average[field] = locals()[argument_list[0]].get(argument_list[1], 0)

    updated_dict["Total"] = summary_total
    updated_dict["Average"] = summary_average

    return updated_dict


def get_top_funds_transferred_data(account_transactions, top_credits_debits):
    """
    Get the top 10 funds transferred
    :param account_transactions:
    :param top_credits_debits:
    :returns: A dictionary having the list of top 10 funds transferred
    """
    data = {
        "Item": list()
    }

    for month in list(top_credits_debits["top_10_debit"].keys()):
        for transaction_note, amount in top_credits_debits["top_10_debit"][month].items():
            debit_transactions = list(filter(
                lambda account_transactions: account_transactions["transaction_note"] == transaction_note and
                                             account_transactions["amount"] == amount, account_transactions))
            if len(debit_transactions) > 0:
                debit_transaction = {
                    "amount": debit_transactions[0]["amount"],
                    "month": month,
                    "category": debit_transactions[0]["description"]
                }
                data["Item"].append(debit_transaction)

    return data


def get_top_funds_received_data(account_transactions, top_credits_debits):
    """
    Get the top 10 funds received
    :param account_transactions:
    :param top_credits_debits:
    :returns: A dictionary having the list of top 10 funds received
    """
    data = {
        "Item": list()
    }

    for month in list(top_credits_debits["top_10_credit"].keys()):
        for transaction_note, amount in top_credits_debits["top_10_credit"][month].items():
            credit_transactions = list(filter(
                lambda account_transactions: account_transactions["transaction_note"] == transaction_note and
                                             account_transactions["amount"] == amount, account_transactions))

            if len(credit_transactions) > 0:
                credit_transaction = {
                    "amount": credit_transactions[0]["amount"],
                    "month": month,
                    "category": credit_transactions[0]["description"]
                }
                data["Item"].append(credit_transaction)

    return data


def get_bounce_transactions(account_transactions):
    """
    Get the list of bounce transactions
    :param account_transactions:
    :returns: A list of bounce transactions
    """
    bounce_flags = ["auto_debit_payment_bounce", "outward_cheque_bounce", "inward_cheque_bounce", "chq_bounce_charge",
                    "auto_debit_payment_bounce", "ach_bounce_charge", "chq_bounce_insuff_funds"]

    bounce_transactions = []

    for transaction_data in account_transactions:
        if transaction_data["transaction_channel"] in bounce_flags or transaction_data["description"] in bounce_flags:
            bounce_transactions.append(
                {
                    "amount": transaction_data.get("amount", ""),
                    "balance": transaction_data.get("balance", ""),
                    "category": transaction_data.get("transaction_type", ""),
                    "chqNo": transaction_data.get("chq_num", ""),
                    "date": transaction_data.get("date", ""),
                    "narration": transaction_data.get("transaction_note", "")
                }
            )
    return bounce_transactions


def get_tax_payment_transactions(fraud_dict, account_id, account_transactions):
    # TODO: Implementation pending. Code will be written post clarification on FCU Analysis field requirements
    list_ = []
    return list_


def get_negative_eod_balance_transactions():
    # TODO: Implementation pending. Code will be written post clarification on FCU Analysis field requirements
    return []


def get_fcu_analysis_data(entity_fraud, account_id, account_transactions):
    """
    Get the FCU analysis data
    :param entity_fraud:
    :param account_id:
    :param account_transactions:
    :returns: FCU analysis data
    """
    fraud_dict = {}
    for fraud in entity_fraud.get("fraud_type", []):
        account_id = fraud.get("account_id", "")
        transaction_hash = fraud.get("transaction_hash", "")
        if account_id and transaction_hash:
            if account_id in fraud_dict:
                fraud_dict[account_id][transaction_hash] = fraud["fraud_category"]
            else:
                fraud_dict[account_id] = {
                    transaction_hash: fraud["fraud_category"]
                }

    fcu_analysis_data = {
        "PossibleFraudIndicators": {
            "SuspiciousBankEStatements": {
                "status": "true"
            }
        },
        "BehaviouralTransactionalIndicators": {
            "TaxPaymentXns": {
                "TaxPaymentXn": get_tax_payment_transactions(fraud_dict, account_id, account_transactions)
            },
            "NegativeEODBalanceXns": {
                "NegativeEODBalanceXn": get_negative_eod_balance_transactions()
            },
            "EqualCreditDebitXns": {
                "status": "false"
            }
        }
    }

    return fcu_analysis_data


def get_account_analysis(account_list, bank_mapping, eod_balances_dict, entity_id, account_transactions_dict,
                         monthly_analysis, month_dict, entity_fraud_dict, predictors, to_reject_account):
    """
    Generate the account analysis data for each account associated with the entity id
    :param account_list:
    :param bank_mapping:
    :param eod_balances_dict:
    :param entity_id:
    :param account_transactions_dict:
    :param monthly_analysis:
    :param month_dict:
    :param entity_fraud_dict:
    :param predictors:
    :param to_reject_account:
    :returns: There are two kinds of possible response:
        - In case of single bank account, it returns a dictionary with all the relevant information around monthly
        analysis
        - In case of multiple bank account, it returns a list of dictionaries with all the relevant information around
        monthly analysis
    """
    account_analysis_response = []
    for account_data in account_list:
        account_id = account_data.get("account_id", "")

        monthly_detail_list = get_account_analysis_monthly_details_data(monthly_analysis[account_id],
                                                                        predictors[account_id],
                                                                        month_dict[account_id])
        updated_summary_info_dict = update_account_analysis_summary_info(predictors[account_id])

        top_credits_debits = access_handler(
            {"entity_id": entity_id, "account_id": account_id, "to_reject_account": to_reject_account,
             "access_type": "ACCOUNT_TOP_CREDITS_DEBITS",
             "req_transactions_count": 10}, None)

        recurring_transactions = access_handler(
            {"entity_id": entity_id, "account_id": account_id, "to_reject_account": to_reject_account,
             "access_type": "ACCOUNT_RECURRING_TRANSACTIONS"}, None)

        account_analysis_dict = {
            "accountNo": account_data.get("account_number", ""),
            "accountID": account_id,
            "accountType": account_data.get("account_category", ""),
            "LoanTrackDetails": {
                "LoanTrackDetail": get_account_analysis_loan_track_details(monthly_analysis[account_id])},
            "SummaryInfo": {
                "accNo": account_data.get("account_number", ""),
                "accountID": account_id,
                "accType": account_data.get("account_category", ""),
                "fullMonthCount": updated_summary_info_dict.get("fullMonthCount", ""),
                "instName": bank_mapping.get(account_id, {}).get("bank_name", ""),
                "Total": updated_summary_info_dict.get("Total", ""),
                "Average": updated_summary_info_dict.get("Average", "")
            },
            "MonthlyDetails": {"MonthlyDetail": monthly_detail_list},
            "EODBalances": {"EODBalance": eod_balances_dict.get(account_id)},
            "Top10FundsReceived": get_top_funds_received_data(account_transactions_dict[account_id],
                                                              top_credits_debits),
            "Top10PaymentsReceived": get_top_payments_received(recurring_transactions),
            "EMILOANXns": {},
            "Top10FundsTransferred": get_top_funds_transferred_data(account_transactions_dict[account_id],
                                                                    top_credits_debits),
            "BouncedOrPenalXns": {"BouncedOrPenalXn": get_bounce_transactions(account_transactions_dict[account_id])},
            "RegularCredits": get_account_analysis_regular_credits(account_id, recurring_transactions),
            "RegularDebits": get_account_analysis_regular_debits(account_id, recurring_transactions),
            "TransferFromToInterGroupXns": {},
            "FCUAnalysis": get_fcu_analysis_data(entity_fraud_dict[account_id], account_id,
                                                 account_transactions_dict[account_id])
        }

        account_analysis_response.append(account_analysis_dict)

    if not len(account_analysis_response) > 1:
        account_analysis_response = account_analysis_response[0]
    return account_analysis_response


def get_combined_monthly_details(monthly_details_dict):
    """
    Get the combined monthly details
    :param monthly_details_dict:
    :returns: List of dictionaries with monthly details having month name and start date corresponding to each month
    """
    combined_detail_dict = dict()
    combined_detail_list = list()
    check_dict = {}
    for account_id in monthly_details_dict:
        for month_data in monthly_details_dict[account_id]:
            if not month_data["startDate"] == check_dict.get(month_data["monthName"], ""):
                check_dict[month_data["monthName"]] = month_data["startDate"]
                combined_detail_list.append(
                    {
                        "monthName": month_data["monthName"],
                        "startDate": month_data["startDate"]
                    }
                )

    combined_detail_dict["CombinedMonthlyDetail"] = combined_detail_list
    return combined_detail_dict


def get_additional_ABB_summary_details(account_list, predictors):
    """
    Get the additional ABB summary details
    :param account_list:
    :param predictors:
    :returns: List of dictionaries with additional ABB summary
    """
    additional_ABB_summary_details_dict = dict()

    for count, account_data in enumerate(account_list, start=1):
        dict_ = {
            "AdditionalSummaryInfo": dict()
        }
        for field in ADDITIONAL_ABB_SUMMARY_DETAILS:
            argument_list = ADDITIONAL_ABB_SUMMARY_DETAILS[field]
            dict_["AdditionalSummaryInfo"][field] = locals()[argument_list[0]].get(argument_list[1], 0)
        additional_ABB_summary_details_dict[f"SummaryABBDetail{count}"] = dict_

    return additional_ABB_summary_details_dict


def get_additional_monthly_finone_details(account_list, month_dict, monthly_analysis):
    """
    Get the additional monthly finone details
    :param account_list:
    :param month_dict:
    :param monthly_analysis:
    :returns: List of dictionaries having fields corresponding to additional monthly finone details
    """
    additional_monthly_finone_details_dict = dict()

    for count, account_data in enumerate(account_list, start=1):
        account_id = account_data.get("account_id", "")
        dict_ = {
            "Detail": []

        }
        if monthly_analysis[account_id] != {}:
            for month in month_dict[account_id]:
                dict_["Detail"].append(
                    {
                        "avgOf5Dates": monthly_analysis[account_id].get("avgOf5Dates", {}).get(month, "0"),
                        "inwChqToDebit": monthly_analysis[account_id].get("cnt_outward_cheque_bounce_debit", {}).get(
                            month, "0"),
                        "monthName": month,
                        "outwChqToCredit": monthly_analysis[account_id].get("cnt_inward_cheque_bounce_credit", {}).get(
                            month, "0"),
                        "totalBounces": monthly_analysis[account_id].get("totalBounces", {}).get(month, "0")
                    }
                )
        additional_monthly_finone_details_dict[f"MonthlyFinOneDetail{count}"] = dict_

    return additional_monthly_finone_details_dict


def get_additional_overall_details(account_list, predictors):
    """
    Get additional overall details for each accounts
    :param account_list:
    :param predictors:
    :returns: Additional details for each account
    """
    additional_overall_details_dict = dict()
    for count, account_data in enumerate(account_list, start=1):
        account_id = account_data.get("account_id", "")

        dict_ = {
            "AdditionalOverallDetail": dict()
        }

        for field in ADDITIONAL_OVERALL_DETAILS:
            argument_list = ADDITIONAL_OVERALL_DETAILS[field]
            dict_["AdditionalOverallDetail"][field] = locals()[argument_list[0]].get(account_id, {}).get(
                argument_list[1], {})

        additional_overall_details_dict[f"OverallDetail{count}"] = dict_

    return additional_overall_details_dict


def get_additional_partial_month_details(account_list):
    # TODO: Get clarity on the fields and logic for the data corresponding to this field
    additional_partial_month_details_dict = dict()
    for count, account_data in enumerate(account_list, start=1):
        additional_partial_month_details_dict[f"PartialMonthDetail{count}"] = None
    return additional_partial_month_details_dict


def get_additional_monthly_details(account_list, monthly_analysis, predictors, month_dict):
    """
    Get additional monthly details
    :param account_list:
    :param monthly_analysis:
    :param predictors:
    :param month_dict:
    :returns: additional monthly details
    """
    additional_monthly_details_dict = dict()

    for count, account_data in enumerate(account_list, start=1):
        account_id = account_data.get("account_id", "")
        detail_dict = {
            "Detail": list()
        }

        for month in month_dict[account_id]:
            dict_ = dict()
            for field in ADDITIONAL_MONTHLY_DETAILS:
                argument_list = ADDITIONAL_MONTHLY_DETAILS[field]
                dict_[field] = locals()[argument_list[0]].get(account_id, {}).get(argument_list[1], {}).get(month, "")

            dict_["monthName"] = month
            detail_dict["Detail"].append(dict_)
        additional_monthly_details_dict[f"MonthlyDetail{count}"] = detail_dict

    return additional_monthly_details_dict


def get_transactions(account_list, account_transactions_dict):
    """
    Get list of transactions for all associated accounts
    :param account_list:
    :param account_transactions_dict:
    :returns: list of dictionaries with transaction details
    """
    transactions_response = []
    for account_data in account_list:
        account_id = account_data.get("account_id", "")
        dict_ = {
            "accountNo": account_data.get("account_number", ""),
            "accountID": account_id,
            "accountType": account_data.get("account_category", ""),
            "ifscCode": account_data.get("ifsc", ""),
            "location": account_data.get("location", ""),
            "micrCode": account_data.get("micr", ""),
            "Xn": []
        }
        for transaction in account_transactions_dict[account_id]:
            if transaction.get("transaction_type") == "debit":
                amount = "-{}".format(transaction.get("amount", "0"))
            else:
                amount = str(transaction.get("amount", "0"))

            dict_["Xn"].append({
                "amount": amount,
                "balance": transaction.get("balance", ""),
                "category": transaction.get("category", ""),
                "date": transaction.get("date", ""),
                "chqNo": transaction.get("chq_num", ""),
                "narration": transaction.get("transaction_note", "")
            }
            )
        transactions_response.append(dict_)
    if not len(account_list) > 1:
        transactions_response = transactions_response[0]
    return transactions_response


def get_statement_details_data(statement_stats):
    """
    Get the details of each statement
    :param statement_stats:
    :returns: Statement if calculated details
    """
    statement_stat_dict = {}
    for stat in statement_stats:
        stat.pop("entity_id")
        account_id = stat.pop("account_id")
        statement_id = stat.pop("statement_id")
        stat["status"] = "VERIFIED"
        if account_id in statement_stat_dict:
            statement_stat_dict[account_id][statement_id] = stat
        else:
            statement_stat_dict[account_id] = {
                statement_id: stat
            }

    return statement_stat_dict


def get_mapping_related_data(required_data, arguments):
    """
    Fetch all the extracted parameters required for the construction of the mapping
    :param required_data:
    :param arguments:
    :returns: All the required parameters like predictors, monthly analysis
    """
    result_output = {}
    predictors_global = {}
    monthly_analysis_global = {}
    eod_balances_global = {}
    thread_list = []
    for i, key in enumerate(required_data):
        function_name = REQUIRED_DATA_TYPES_TO_FUNCTION_MAP.get(key)
        function_related_arguments = FUNCTIONS_TO_ARGUMENT_MAP.get(function_name)
        new_arg_dict = {key: arguments[key] for key in function_related_arguments}
        if FUNCTION_TO_RETURN_VALUE_MAP.get(key):
            return_value = FUNCTION_TO_RETURN_VALUE_MAP[key]
            new_arg_dict[return_value] = locals()[return_value]
        if function_name:
            thread_list.append(threading.Thread(target=globals()[function_name], kwargs=new_arg_dict))
            thread_list[i].start()
    for thread in thread_list:
        thread.join()

    for key in required_data:
        return_field_name = FUNCTION_TO_RETURN_VALUE_MAP.get(key)
        result_output[return_field_name] = locals()[return_field_name]

    return result_output


def get_commonly_required_data(account_list, entity_id, eod_balances, to_reject_account):
    """
    Get the values required commonly by multiple fields of the mapping
    :param account_list:
    :param entity_id:
    :param eod_balances:
    :param to_reject_account:
    :returns: All the common parameters
    """
    eod_balances_dict = dict()
    entity_fraud_dict = dict()
    account_transactions_dict = dict()
    month_dict = dict()
    monthly_details_dict = dict()
    for count, account_data in enumerate(account_list, start=1):
        try:
            credit_limit = int(account_data.get("credit_limit", None))
        except:
            credit_limit = None

        account_id = account_data["account_id"]
        month_dict[account_id] = list()
        eod_balances_dict[account_id], monthly_details_dict[account_id], month_dict[
            account_id] = get_account_analysis_eod_balances(eod_balances, account_id, credit_limit)
        entity_fraud_dict[account_id] = access_handler(
            {"entity_id": entity_id, "account_id": account_id, "to_reject_account": to_reject_account,
             "access_type": "ENTITY_FRAUD"}, None)
        account_transactions_dict[account_id] = access_handler(
            {"entity_id": entity_id, "account_id": account_id, "to_reject_account": to_reject_account,
             "access_type": "ACCOUNT_TRANSACTIONS"}, None)

    return eod_balances_dict, entity_fraud_dict, account_transactions_dict, month_dict, monthly_details_dict


def check_format_and_return_formatted_string(value: str, input_format: str, output_format: str) -> str:
    try:
        datetime_obj = datetime.datetime.strptime(value, input_format)
        return datetime_obj.strftime(output_format)
    except Exception as e:
        print(e)
        return value


def format_dates_and_handle_null(data_dict: dict, parent_key: Union[str, None], dateformat='%Y-%m-%d'):
    for key in list(data_dict.keys()):
        value = data_dict.get(key)
        if isinstance(value, dict):
            format_dates_and_handle_null(value, key, dateformat=dateformat)
        elif isinstance(value, list):
            for i in range(len(value)):
                list_val = value[i]
                if isinstance(list_val, dict):
                    format_dates_and_handle_null(list_val, key, dateformat=dateformat)
        elif value is None or value == 'None':
            data_dict[key] = ""
        elif isinstance(value, str):
            if key in ['date'] and parent_key == 'EODBalances':  # 2023-04-01
                data_dict[key] = check_format_and_return_formatted_string(value, '%Y-%m-%d', dateformat)
            elif key in ['xnsEndDate', 'xnsStartDate', 'date']:  # 2023-04-01 00:00:00
                data_dict[key] = check_format_and_return_formatted_string(value, '%Y-%m-%d %H:%M:%S', dateformat)
            elif key in ['dates']:  # 18-Dec-23
                data_dict[key] = check_format_and_return_formatted_string(value, '%d-%b-%y', dateformat)
            elif key in ['startDate'] and parent_key == 'MonthlyDetail':  # 01-Apr-2023
                data_dict[key] = check_format_and_return_formatted_string(value, '%d-%b-%Y', dateformat)
            elif key in ['startDate']:  # 2023-04-01
                data_dict[key] = check_format_and_return_formatted_string(value, '%Y-%m-%d', dateformat)


def get_frauds_separated(account_id: str, entity_id: str, to_reject_account: bool = False) -> dict:
    frauds_dict = {
        'Metadata': False,
        'Transactional': False,
        'Behavioural': False,
        'Accounting': False
    }

    _, frauds = get_fraud_for_account(entity_id, account_id, to_reject_account=to_reject_account)
    for fraud in frauds:
        fraud_type = fraud.get('fraud_type', None)
        if fraud_type:
            frauds_dict[FRAUD_TO_CATEGORY_MAPPING.get(fraud_type, '')] = True

    return frauds_dict


def generate_xml_mappings(event, local_logging_context: LoggingContext):
    """
    Description:
    This function serves as a handler for fetching the entire data related to provided account and return the formatted
    json.
    Parameters:
        - event: dict
            - 'entity_id': str
            - 'bank_mapping': dict
            - 'account_id': str
            - 'is_sme': bool
            - 'adjusted_eod': bool
    Returns:
        Formatted JSON
    """

    entity_id = event.get("entity_id", "")
    link_id = event.get("link_id", "")
    bank_mapping = event.get("bank_mapping", {})
    is_sme = event.get("is_sme", False)
    adjusted_eod = event.get("adjusted_eod", False)
    to_reject_account = event.get("to_reject_account", False)
    caching_enabled = event.get("caching_enabled", False)
    session_flow = event.get("session_flow", False)
    session_date_range = event.get("session_date_range", {'from_date': None, 'to_date': None})

    local_logging_context.upsert(
        link_id=link_id,
    )

    LAMBDA_LOGGER.info(
        "Inside generate_xml_mappings function",
        extra=local_logging_context.store
    )

    new_dict = dict()

    account_list = access_handler(
        {"entity_id": entity_id, "to_reject_account": to_reject_account, "access_type": "ACCOUNT_IDENTITY"}, None)

    LAMBDA_LOGGER.info(
        f"Account list with reject account : {account_list}",
        extra=local_logging_context.store
    )

    if not account_list:
        account_list = access_handler(
            {"entity_id": entity_id, "to_reject_account": False, "access_type": "ACCOUNT_IDENTITY"}, None)

    LAMBDA_LOGGER.info(
        f"Account list without reject account : {account_list}",
        extra=local_logging_context.store
    )

    LAMBDA_LOGGER.info(
        f"Number of accounts accessed: {len(account_list)}",
        extra=local_logging_context.store
    )

    required_data = ["PREDICTORS", "MONTHLY_ANALYSIS", "EOD_BALANCES"]
    arguments = {
        "entity_id": entity_id,
        "adjusted_eod": adjusted_eod,
        "account_id": None,
        "to_remap_predictors": None,
        "ignore_self_transfer": None,
        "is_sme": is_sme,
        "to_reject_account": to_reject_account,
        "session_dict": {
            "is_session_flow": session_flow,
            "from_date": session_date_range.get('from_date'),
            "to_date": session_date_range.get('to_date')
        },
        "caching_enabled": caching_enabled
    }

    returned_dict = get_mapping_related_data(required_data, arguments)

    predictors = returned_dict["predictors_global"]
    monthly_analysis = returned_dict["monthly_analysis_global"]
    eod_balances = returned_dict["eod_balances_global"]

    eod_balances_dict, entity_fraud_dict, account_transactions_dict, month_dict, monthly_details_dict = \
        get_commonly_required_data(account_list, entity_id, eod_balances, to_reject_account)

    account_cache = {}
    for json_fields in JSON_OUTPUT_FORMAT_FUNCTION_MAP:
        function_name = JSON_OUTPUT_FORMAT_FUNCTION_MAP[json_fields]
        arguments = dict()
        for key in LOCAL_FUNCTION_ARGUMENT_MAP[function_name]:
            arguments[str(key)] = locals()[key]

        # arguments = {key: locals()[key] for key in LOCAL_FUNCTION_ARGUMENT_MAP[function_name]}
        new_dict[json_fields] = globals()[function_name](**arguments)

        if json_fields == 'Statementdetails':
            statements = new_dict[json_fields].get('Statement', list())
            for statement in statements:
                account_id = statement.get('StatementAccounts', dict()).get('StatementAccount', dict()).get('accountID',
                                                                                                            None)
                if account_id:
                    if account_id not in account_cache:
                        account_cache[account_id] = get_frauds_separated(account_id, entity_id, to_reject_account)

                account_fraud_dict = account_cache.get(account_id, dict())
                meta_fraud = account_fraud_dict.get('Metadata', False)
                accounting_fraud = account_fraud_dict.get('Accounting', False)
                behavioural_fraud = account_fraud_dict.get('Behavioural', False)
                transactional_fraud = account_fraud_dict.get('Transactional', False)

                statementStatus = "VERIFIED"
                if (meta_fraud or accounting_fraud):
                    statementStatus = "FRAUD"
                elif (behavioural_fraud or transactional_fraud):
                    statementStatus = "REFER"
                statement['statementStatus'] = statementStatus

            new_dict[json_fields]['Statement'] = statements

    format_dates_and_handle_null(new_dict, None)
    return new_dict
