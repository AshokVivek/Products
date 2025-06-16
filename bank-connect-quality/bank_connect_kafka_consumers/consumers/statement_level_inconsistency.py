import asyncio
from app.inconsistency.statement.base import ingest_statement

def statement_level_inconsistency(statements_data):
    try:
        result = asyncio.run(ingest_statement(statements_data))
        print(result)
    except Exception as e:
        print(e)
    return True