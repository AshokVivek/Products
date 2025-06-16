from pydantic import BaseModel
from typing import Any, Dict, Union

from python.dtos.identity.base import IdentityObjectDTO


class IdentityItemDataDTO(BaseModel, validate_assignment=True):
    closing_bal: Any
    country_code: Union[str, None]
    currency_code: Union[str, None]
    date_range: Dict = {}
    doc_metadata: Dict = {}
    extracted_date_range: Dict = {}
    fraud_type: Union[str, None] = None
    identity: IdentityObjectDTO
    is_fraud: bool = False
    keywords: Dict = {}
    keywords_in_line: Union[bool, None] = None
    metadata_analysis: Dict = {}
    opening_bal: Union[str, None] = None 
    opening_date: Union[str, None] = None
    page_count: int 
