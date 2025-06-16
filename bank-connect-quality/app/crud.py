from app.conf import redis_cli
import json

async def get_user_from_user_id(db,username):
    redis_key = f'{username}_credentials'
    redis_response = redis_cli.get(redis_key)
    if redis_response is not None:
        return json.loads(redis_response)

    quality_query = 'select * from users where username=:username'
    quality_response = await db.fetch_one(query = quality_query, values={
        'username':username
    })

    if quality_response is None:
        return None
    
    quality_response = dict(quality_response)
    quality_response.pop('created_at', None)
    redis_cli.set(redis_key, json.dumps(quality_response), ex=900)
    return quality_response

async def get_user_by_username(db, username: str):
    user_data = await get_user_from_user_id(db,username)
    if user_data:
        password = user_data.get('password')
        return password


async def get_user_type(db, username):
    user_data = await get_user_from_user_id(db,username)
    if user_data:
        return user_data.get('type')


async def create_user(db, username, password):
    query = """ INSERT INTO users (username, password) VALUES (:username, :password)"""
    await db.execute(query=query, values={"username": username, "password": password})


async def get_user_list(db):
    query = 'SELECT username, type FROM users'
    return await db.fetch_all(query=query)
