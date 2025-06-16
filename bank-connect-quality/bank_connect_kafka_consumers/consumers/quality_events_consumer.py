import asyncio
from app.template_solutioning.ingest import ingest_helper

def quality_events_consumer(statements_data):
    try:
        result = asyncio.run(ingest_helper(statements_data))
        print(result)
    except Exception as e:
        print(e)
    return True
