from pydantic import BaseModel

class XlsxReportRequestPayload(BaseModel):
    entity_id: str