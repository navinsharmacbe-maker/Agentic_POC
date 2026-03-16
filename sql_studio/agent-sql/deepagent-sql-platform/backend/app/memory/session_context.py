import json
from pathlib import Path
from app.config import SESSION_UPLOADS_DIR

def _context_file(session_id: str) -> Path:
    return Path(SESSION_UPLOADS_DIR) / session_id / "context.json"

def save_context(session_id: str, context: dict):
    path = _context_file(session_id)
    path.write_text(json.dumps(context, indent=2))

def load_context(session_id: str) -> dict:
    path = _context_file(session_id)
    if not path.exists():
        return {}
    return json.loads(path.read_text())
