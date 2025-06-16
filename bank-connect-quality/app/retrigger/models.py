from os import listxattr
from pydantic import BaseModel, conlist
from typing import List, Optional
from datetime import datetime


class InvokeLambda(BaseModel):
    key: str
    preshared_names: Optional[list] = []
    template_uuid: Optional[str] = None

class InvokeUpdateState(BaseModel):
    bank_name: str
    statement_id: str
    entity_id: str
    attempt_type: str

class BsaStatus(BaseModel):
    statement_id: str
    transactions_status: str
    processing_status: str
    identity_status: Optional[str]
    metadata_fraud_status: Optional[str]
    page_identity_fraud_status: Optional[str]
    to_reject_statement: Optional[bool]
    message: Optional[str]
    update_message: Optional[bool]

class InvokeAnalyzeAA(BaseModel):
    statement_id: str
    bank_name: str