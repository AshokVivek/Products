from pydantic import BaseModel
from typing import Optional

class FaultyTransactionUpdateData(BaseModel):
    is_extraction_problem_confirmed: bool
    is_issue_solved: bool
    technique_used_to_solve: Optional[str] = ''

