from typing import Any

from library.date_utils import get_months_from_periods
from python.aggregates import (
    get_account_for_entity,
    get_complete_identity_for_statement,
    get_country_for_statement,
    get_currency_for_statement,
    get_date_discontinuity,
    get_final_account_category,
)
from python.dtos.account.base import AccountItemDataDTO
from pydantic import TypeAdapter

from python.dtos.subscriptions.account_details_dto import SubscriptionAPIAccountDetailsDTO
from python.utils import get_date_of_format


def get_account_details(event):
    entity_id = event.get("entity_id")
    account_id = event.get("account_id")
    to_reject_account = event.get("to_reject_account", False)

    account = get_account_for_entity(entity_id, account_id, to_reject_account)
    if not account:
        raise Exception("Account data not found")

    account_item_data: Any = account.get("item_data", {})
    if not account_item_data:
        raise Exception("Account item data not found")

    try:
        account_item_data['account_date_range']['from_date'] = get_date_of_format(account_item_data['account_date_range']['from_date'], "%Y-%m-%d")
        account_item_data['account_date_range']['to_date'] = get_date_of_format(account_item_data['account_date_range']['to_date'], "%Y-%m-%d")
    except Exception:
        pass

    account_item_data_dto = AccountItemDataDTO(**account_item_data)

    account_id = account_item_data_dto.account_id
    account_category = account_item_data_dto.account_category
    is_od_account = account_item_data_dto.is_od_account
    input_account_category = account_item_data_dto.input_account_category
    input_is_od_account = account_item_data_dto.input_is_od_account
    statements = account_item_data_dto.statements

    statement_identities = [get_complete_identity_for_statement(statement_id) for statement_id in statements]
    if len(statement_identities) > 0:
        account_item_data_dto.name = statement_identities[0].get("identity", {}).get("name")
        account_item_data_dto.address = statement_identities[0].get("identity", {}).get("address")
        account_item_data_dto.metadata_analysis = statement_identities[0].get("metadata_analysis", {"name_matches": []})

    date_ranges = list()
    extracted_date_range_list = list()
    for identity in statement_identities:
        date_range = identity.get("date_range")
        if date_range:
            date_ranges.append(date_range)
        extracted_date_range = identity.get('extracted_date_range')
        if extracted_date_range and extracted_date_range.get('from_date') and extracted_date_range.get('to_date'):
            extracted_date_range_list.append(extracted_date_range)
    account_item_data_dto.months = get_months_from_periods(date_ranges)
    account_item_data_dto.uploaded_months = get_months_from_periods(extracted_date_range_list)

    final_account_category, _ = get_final_account_category(account_category, is_od_account, input_account_category, input_is_od_account)
    account_item_data_dto.account_category = final_account_category

    if account_item_data_dto.credit_limit is None:
        account_item_data_dto.credit_limit = account_item_data_dto.od_limit
    if account_item_data_dto.od_limit is None:
        account_item_data_dto.od_limit = account_item_data_dto.credit_limit

    # also get the country and currency from ddb and write in accounts_list
    country_code = get_country_for_statement(statements[0])
    currency_code = get_currency_for_statement(statements[0])

    # default country and currency to IN and INR respectively incase of None values
    country_code = country_code if country_code is not None else "IN"
    currency_code = currency_code if currency_code is not None else "INR"
    account_item_data_dto.country_code = country_code
    account_item_data_dto.currency_code = currency_code

    # get the date discontinuity remarks if any
    account_item_data_dto.missing_data = get_date_discontinuity(entity_id, account_id)

    # validate against response model
    account_details = TypeAdapter(SubscriptionAPIAccountDetailsDTO).validate_python(account_item_data_dto.model_dump())

    return account_details.model_dump()
