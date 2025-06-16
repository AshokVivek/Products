from pydantic import BaseModel

class AccountTransactionDTO(BaseModel, validate_assignment=True):
    transaction_type: str
    transaction_note: str
    chq_num: str
    amount: float
    balance: float
    date: str
    hash: str
    category: str
    transaction_channel: str
    merchant_category: str
    account_id: str
    description: str
    month_year: str
    # TODO: add more fields