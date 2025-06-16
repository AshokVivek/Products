from pydantic import BaseModel
from typing import Union


class SubscriptionAPIAccountRecurringTransactionDTO(BaseModel, validate_assignment=True):
    transaction_type: str
    transaction_note: str
    chq_num: Union[str, None] = None
    amount: float
    balance: float
    date: str
    transaction_channel: str
    hash: str
    merchant_category: str
    description: str
    category: str