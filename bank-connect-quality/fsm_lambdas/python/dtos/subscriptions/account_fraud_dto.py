from pydantic import BaseModel, Field
from typing import Union


class SubscriptionAPIAccountFraudDTO(BaseModel, validate_assignment=True):
    statement_id: Union[str, None]
    fraud_type: str
    transaction_hash: Union[str, None]
    fraud_category: str = Field(default="uncategorized")
