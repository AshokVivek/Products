from pydantic import BaseModel

class MarkCompleted(BaseModel):
    statement_id: str
    inconsistent_remarks: str
    status: str
    reason: str
    type: str