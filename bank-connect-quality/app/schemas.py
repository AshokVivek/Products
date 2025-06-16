from typing import Optional
from datetime import datetime
from pydantic import BaseModel


class Token(BaseModel):
    access_token: str
    token_type: str
    user_type: str

class TokenData(BaseModel):
    username: Optional[str] = None


class UserUsername(BaseModel):
    username: str


class UserInDB(UserUsername):
    hashed_password: str
    user_type: Optional[str] = "requester"

class NewUser(UserUsername):
    password: str
    user_type: str
    
class DateRange(BaseModel):
    from_date: Optional[str] = None
    to_date: Optional[str] = None
