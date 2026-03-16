from fastapi import APIRouter
from pydantic import BaseModel

router = APIRouter()

class ApprovalRequest(BaseModel):
    session_id: str
    approved: bool
    sql: str

@router.post("/")
def approve(req: ApprovalRequest):
    if not req.approved:
        return {"status": "rejected"}

    return {
        "status": "approved",
        "sql": req.sql
    }
