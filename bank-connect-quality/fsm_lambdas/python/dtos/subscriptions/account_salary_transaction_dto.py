from pydantic import BaseModel, field_validator
from typing import Any, Union

from python.dtos.subscriptions.account_transaction_dto import SubscriptionAPITransactionsMetadataDTO

class SubscriptionAPIAccountSalaryTransactionDTO(BaseModel, validate_assignment=True):
    transaction_type: str
    transaction_note: str
    chq_num: Union[str, None] = None
    amount: float
    balance: float
    date: str
    hash: str
    category: str
    employer_name: Union[str, None] = None
    salary_month: str
    metadata: Union[SubscriptionAPITransactionsMetadataDTO, None] = None

    @field_validator("employer_name", mode="before")
    @classmethod
    def employer_name_default(cls, value: Any, info) -> Any:
        if value == "":
            return None
        else:
            return value
