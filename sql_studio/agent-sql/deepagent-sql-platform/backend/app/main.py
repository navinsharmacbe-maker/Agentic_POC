from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pathlib import Path

from app.api import chat, csv_upload, approval, sql_management, lineage, mapping
from app.ingest import schema, layer, transformations
from app.memory.sqlite_store import init_db
from app.config import SESSION_UPLOADS_DIR, AGENT_MEMORIES_DIR, AGENT_WORKSPACE_DIR

app = FastAPI(title="DeepAgent SQL Platform")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Register routers
app.include_router(chat.router, prefix="/chat", tags=["Chat"])
app.include_router(csv_upload.router, prefix="/csv", tags=["CSV"])
app.include_router(approval.router, prefix="/approval", tags=["Approval"])
app.include_router(sql_management.router, prefix="/sql", tags=["SQL"])
app.include_router(lineage.router, prefix="/lineage", tags=["Lineage"])
app.include_router(mapping.router, prefix="/mapping", tags=["Mapping"])
app.include_router(schema.router, prefix="/metadata", tags=["Metadata"])
app.include_router(layer.router, prefix="/metadata", tags=["Metadata"])
app.include_router(transformations.router, prefix="/metadata", tags=["Metadata"])


@app.on_event("startup")
def startup() -> None:
    init_db()
    Path(SESSION_UPLOADS_DIR).mkdir(parents=True, exist_ok=True)
    Path(AGENT_MEMORIES_DIR).mkdir(parents=True, exist_ok=True)
    Path(AGENT_WORKSPACE_DIR).mkdir(parents=True, exist_ok=True)

@app.get("/")
def root():
    return {"status": "DeepAgent Platform Running"}
