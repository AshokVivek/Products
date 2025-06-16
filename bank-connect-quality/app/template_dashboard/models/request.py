from os import listxattr
from pydantic import BaseModel, conlist
from typing import List, Optional
from datetime import datetime


class ValidateRequest(BaseModel):
    statement_id: str
    bank_name: str
    page_no: int
    template: dict
    password: Optional[str]
    transaction_flag: Optional[bool]
    table_data: Optional[list] = []
    template_type: Optional[str] = None
    country: Optional[str] = 'IN'
    opening_date: Optional[str] = None


class IdentityTemplate(BaseModel):
    regex: Optional[str]
    bbox: Optional[conlist(int, min_items=4, max_items=4)]
    horizontal_lines: Optional[str]
    vertical_lines: Optional[list]
    column: Optional[list]


class AddTemplate(BaseModel):
    template_type: str
    template: IdentityTemplate
    bank_name: str


class StatementEntityData(BaseModel):
    statement_id: Optional[str]
    name: Optional[str]
    account_number: Optional[str]
    address: Optional[str]
    bank_name: Optional[str]
    pdf_password: Optional[str]
    from_date: Optional[datetime]
    to_date: Optional[datetime]
    ifsc: Optional[str]
    micr: Optional[str]
    is_extracted: Optional[bool]
    
class LogoMismatchData(BaseModel):
    concat: Optional[str]
    statement_id: Optional[str]
    
class LogoNullMismatchData(BaseModel):
    bank_name: Optional[str]
    statement_id: Optional[str]

class Imagehashdata(BaseModel):
    path: str
    password: Optional[str]

class Base64(BaseModel):
    statement_id: str
    password: Optional[str]
class StatementFromToData(BaseModel):
    statement_id: str
    bank_name: str
    pdf_password: Optional[str]
    name: Optional[str]

class RegexData(BaseModel):
    b64_file:str
    password: Optional[str]
    page_num: int
    bbox: list
    regex: str

class MultipleData(BaseModel):
    statement_list:Optional[list]
    passwords: Optional[list]
    page_num: int
    bbox: list
    regex: str
class Oneapicall(BaseModel):
    b64_file:str
    password: Optional[str]
    page_num: int
    bbox: list
    regex: list
    
class FromDateToDateRegexData(BaseModel):
    b64_file:str
    password: Optional[str]
    page_num: int
    from_bbox: list
    from_regex: str
    to_bbox: list
    to_regex: str

class Transaction(BaseModel):
    from_date:datetime
    to_date:datetime
    bank_name:str
    transaction_id:str