from pydantic import BaseModel
from typing import Any, Union

class AccountItemDataDTO(BaseModel, validate_assignment=True):
    account_id: str
    account_number: str
    bank: str
    missing_data: Union[list[dict], None] = []
    metadata_analysis: Union[dict, None] = {"name_matches": []}
    months: list[str] = []
    uploaded_months: list[str] = []
    statements: list[str] = []
    country_code: Union[str, None] = None
    currency_code: Union[str, None] = None
    account_category: Union[str, None] = None
    account_opening_date: Union[str, None] = None
    credit_limit: Union[float, None] = None
    od_limit: Union[float, None] = None
    ifsc: Union[str, None] = None
    micr: Union[str, None] = None
    linked_account_ref_number: Union[Any, None] = None
    neg_txn_od: Union[bool, None] = None
    salary_confidence: Union[float, None] = None
    od_limit_input_by_customer: Union[float, None] = None
    name: Union[str, None] = None
    address: Union[str, None] = None
    is_od_account: Union[bool, None] = None
    input_account_category: Union[str, None] = None
    input_is_od_account: Union[bool, None] = None
    dob : Union[str, None] = None
    email :Union[str, None] = None
    pan_number :Union[str, None] = None
    phone_number :Union[str, None] = None
    account_status :Union[str, None] = None
    holder_type: Union[str, None] = None
    account_date_range: Union[dict, None] = {"from_date": None, "to_date": None}
    transaction_date_range: Union[dict, None] = {"from_date": None, "to_date": None}
