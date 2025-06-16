import jwt
from fastapi import Depends, HTTPException, status
from fastapi.security import OAuth2PasswordBearer
from jwt import PyJWTError

from .conf import ALGORITHM, QUALITY_SECRET
from .schemas import TokenData, UserInDB
from .crud import get_user_by_username, get_user_type
from .database_utils import quality_database

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/token")


async def get_user(username: str):
    hashed_password = await get_user_by_username(quality_database, username)
    user_type = await get_user_type(quality_database, username)
    if hashed_password:
        user_dict = {
                "username": username,
                "hashed_password": hashed_password,
                "user_type": user_type
            }
        return UserInDB(**user_dict)


async def get_current_user(token: str = Depends(oauth2_scheme)):
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )
    try:
        payload = jwt.decode(token, QUALITY_SECRET, algorithms=[ALGORITHM])
        username: str = payload.get("sub")
        if username is None:
            raise credentials_exception
        token_data = TokenData(username=username)
    except PyJWTError:
        raise credentials_exception
    user = await get_user(username=token_data.username)
    if user is None:
        raise credentials_exception
    return user
