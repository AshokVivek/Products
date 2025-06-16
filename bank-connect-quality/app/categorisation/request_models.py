from os import listxattr
from pydantic import BaseModel, conlist
from typing import List, Optional, Union, Literal
from datetime import datetime
from fastapi import Query

class AddRegexToTempDB(BaseModel):
    enrichment_type: Optional[Literal["transaction_channel", "merchant_category", "description"]] = None
    regex: str
    operation: Optional[Literal["create", "delete"]] = None