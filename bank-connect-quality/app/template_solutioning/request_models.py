from os import listxattr
from pydantic import BaseModel, conlist
from typing import List, Optional, Union
from datetime import datetime
from fastapi import Query

class TemplateAddition(BaseModel):
    template_type : str
    template_json : Union[dict, list]
    statement_id : str
    bank_name : str

class AvailableTemplates(BaseModel):
    bank_name: str
    template_type: Optional[str] = None

class TemplateUpdation(BaseModel):
    bank_name : str
    template_uuid : str
    template_json : Optional[dict]
    template_type : Optional[str] = None
    priority : Optional[str] = None 
    statement_id : Optional[str] = None

class TemplateValidation(BaseModel):
    key: str
    bank_name: str
    template: Union[dict, list]
    template_type: str

class ApproveTemplate(BaseModel):
    template_uuid : str
    bank_name : str
    approval : Optional[bool] = False

class MoveTemplate(BaseModel):
    template_uuid : str
    bank_name : str

class TemplateShifting(BaseModel):
    bank_name : str
    template_type : str
    template_uuid : str
    priority_from : Optional[int] = None
    priority_to : Optional[int] = None

class PerformSimulation(BaseModel):
    bank_name     : str
    template_type : str
    template_id   : str
    parent_module : Optional[str]=""

class UpdateKeywords(BaseModel):
    keyword_type:str
    country:str
    keyword_list:list
    operation:str

class SuperUserApproval(BaseModel):
    template_id   : str
    approval      : bool

class IgnoreType(BaseModel):
    null_type     : str
    statement_ids  : list

class GetExtractedData(BaseModel):
    statement_id: str
    template_type: str
    template_json: Union[dict, list]
    bank_name: str
    page_number: Optional[int]=0

class IgnoreAccount(BaseModel):
    ignore_bool: bool
    ignore_field: str
    account_id: str
    entity_id: str
    ignore_reason : str

class TemplateActivation(BaseModel):
    bank_name : str
    template_type: Optional[str] = None
    template_uuid : str
    operation: str

class TestMetadata(BaseModel):
    bank_name: str
    metadata_type: str
    metadata: Union[str, list, dict]
    statement_list : list
    operation: str

class AdditionFraudMetadata(BaseModel):
    bank_name: str
    operation: str
    metadata_type: str
    metadata: Union[str, list, dict]

class CommonIngest(BaseModel):
    statements: list
    # access_code: str

class UpdatePatterns(BaseModel):
    pattern_type: str
    identity_type: str
    operation: str 
    country: Optional[str] = 'IN'
    pattern: Union[str,dict]

class MismatchTemplateAddition(BaseModel):
    template_json: dict
    template_type: str
    bank_name: str
    statement_id: str

class ParkedData(BaseModel):
    template_uuid: str    

class MismatchSuperuserApproval(BaseModel):
    template_id:str
    approval: bool

class MismatchIgnore(BaseModel):
    statement_ids: list
    identity_type: str

class InconsistencyCheck(BaseModel):
    statement_id: str
    bank_name: str
    attempt_type: str

class CopyTransactions(BaseModel):
    from_statement_id: str
    to_statement_ids: list
