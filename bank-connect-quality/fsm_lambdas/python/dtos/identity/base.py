from pydantic import BaseModel
from typing import Union


class IdentityObjectDTO(BaseModel, validate_assignment=True):
    account_category: str
    account_id: str
    account_number: str
    address: str
    bank_name: str
    credit_limit: float
    currency: Union[str, None]
    ifsc: str
    input_account_category: Union[str, None]
    input_is_od_account: Union[bool, None]
    is_od_account: Union[bool, None]
    micr: Union[str, None]
    name: Union[str, None]
    od_limit: Union[float, None]
    raw_account_category: Union[str, None]