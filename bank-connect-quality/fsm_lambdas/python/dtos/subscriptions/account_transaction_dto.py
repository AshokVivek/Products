from pydantic import BaseModel
from typing import Union


class SubscriptionAPITransactionsMetadataDTO(BaseModel):
    unclean_merchant: str
    transaction_channel: str
    description: str


class SubscriptionAPIAccountTransactionDTO(BaseModel, validate_assignment=True):
    transaction_type: str
    transaction_note: str
    chq_num: Union[str, None] = None
    amount: float
    balance: float
    date: str
    hash: str
    category: str
    metadata: Union[SubscriptionAPITransactionsMetadataDTO, None] = None
