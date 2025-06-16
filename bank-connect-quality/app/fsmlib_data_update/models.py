from pydantic import BaseModel
from enum import Enum

class RequestDataUpdate(BaseModel):
    request_type: str 
    requested_data: dict
    statement_id: str
    operation_type: str

class RequestApproval(BaseModel):
    request_id: str
    approval_status: bool

class ApprovalStatus(str,Enum):
    APPROVED = 'APPROVED'
    REJECTED = 'REJECTED'
    PENDING = 'PENDING'