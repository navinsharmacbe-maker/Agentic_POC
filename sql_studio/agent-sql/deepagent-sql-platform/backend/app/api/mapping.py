from fastapi import APIRouter
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
import json
import logging

from app.agents.mapping_agent import run_mapping_agent_stream

router = APIRouter()
logger = logging.getLogger(__name__)

class MappingChatRequest(BaseModel):
    session_id: str
    message: str

@router.post("/chat/stream")
async def chat_stream(req: MappingChatRequest):
    async def event_gen():
        yield f"event: status\ndata: {json.dumps({'status': 'started'})}\n\n"
        full_reply = ""
        try:
            async for event_type, payload in run_mapping_agent_stream(req.session_id, req.message):
                if event_type == "chunk":
                    full_reply += payload
                    yield f"event: chunk\ndata: {json.dumps({'content': payload})}\n\n"
                elif event_type == "progress":
                    yield f"event: progress\ndata: {json.dumps({'node': payload})}\n\n"
        except Exception as exc:
            logger.exception("mapping chat_stream failed")
            yield f"event: error\ndata: {json.dumps({'error': str(exc)})}\n\n"
            
        yield f"event: done\ndata: {json.dumps({'status': 'completed', 'chat_reply': full_reply})}\n\n"

    return StreamingResponse(
        event_gen(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "Connection": "keep-alive"}
    )
