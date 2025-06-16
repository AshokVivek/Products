from pydantic import BaseModel
from typing import List

from python.dtos.subscriptions.account_transaction_dto import SubscriptionAPIAccountTransactionDTO


class AccountTopDebitCreditMonthDTO(BaseModel, validate_assignment=True):
    month: str
    data: List[SubscriptionAPIAccountTransactionDTO]


class SubscriptionAPIAccountTopDebitCreditDTO(BaseModel, validate_assignment=True):
    type: str
    data: List[AccountTopDebitCreditMonthDTO]
