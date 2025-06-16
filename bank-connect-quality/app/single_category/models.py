from pydantic import BaseModel
from typing import Optional

class RequestRegexAddition(BaseModel):
    cluster_id: str
    regex: str
    capturing_group_details: dict
    inferred_category: Optional[str] = None
    transaction_channel_tag: Optional[str] = None
    merchant_category_tag: Optional[str] = None
    description_tag: Optional[str] = None

class RemoveFromCluster(BaseModel):
    cluster_id: str
    hash_list: list[str]

class AddMetadata(BaseModel):
    category: str
    category_description: str

class ApproveRegex(BaseModel):
    cluster_id: str

class AssignCluster(BaseModel):
    cluster_id: str
    username: str

class SuperUserApproval(BaseModel):
    cluster_id: str
    approval: bool

class TestRegex(BaseModel):
    cluster_id: str
    regex: str
    capturing_group_details: dict

class StartCategorisation(BaseModel):
    limit: Optional[int] = 20

class CreateSpecificCluster(BaseModel):
    bank_name: str
    transaction_type: str
    sample_transaction_note: str
    regex: str