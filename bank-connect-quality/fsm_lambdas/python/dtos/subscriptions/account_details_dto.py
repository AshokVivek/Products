from pydantic import BaseModel, field_validator
from typing import Any, Dict, List, Union


class SubscriptionAPIAccountDetailsDTO(BaseModel, validate_assignment=True):
    name: Union[str, None]
    address: Union[str, None]
    metadata_analysis: Union[Dict, None] = {"name_matches": []}
    account_category: Union[str, None]
    account_number: str
    account_opening_date: Union[str, None]
    bank: str
    credit_limit: Union[float, None]
    ifsc: Union[str, None]
    micr: Union[str, None]
    missing_data: Union[List[Dict], None] = []
    od_limit: Union[float, None]
    salary_confidence: Union[float, None]
    statements: List[str] = []
    months: List[str] = []
    uploaded_months: List[str] = []
    country_code: str
    currency_code: str
    dob : Union[str, None] = None
    email :Union[str, None] = None
    pan_number :Union[str, None] = None
    phone_number :Union[str, None] = None
    account_status :Union[str, None] = None
    holder_type: Union[str, None] = None
    account_date_range: Union[dict, None] = {"from_date": None, "to_date": None}
    transaction_date_range: Union[dict, None] = {"from_date": None, "to_date": None}

    @field_validator("metadata_analysis", mode="before")
    @classmethod
    def metadata_analysis_default(cls, value: Any, info) -> Any:
        if value is None:
            return {"name_matches": []}
        else:
            return value

    @field_validator("missing_data", mode="before")
    @classmethod
    def missing_data_default(cls, value: Any, info) -> Any:
        if value is None:
            return []
        else:
            return value
