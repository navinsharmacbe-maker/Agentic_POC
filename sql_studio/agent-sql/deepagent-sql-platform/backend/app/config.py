import os

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

NEO4J_URI = os.getenv("NEO4J_URI")
NEO4J_USER = os.getenv("NEO4J_USER")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD")

SQLITE_DB = "chat_memory.db"
SESSION_MD_PATH = "app/memory/session_md/"

AGENT_WORKSPACE_DIR = os.getenv("AGENT_WORKSPACE_DIR", "./app/workspace")
AGENT_MEMORIES_DIR = os.getenv("AGENT_MEMORIES_DIR", "./app/memory/session_md")
AGENT_DENY_WRITE_PREFIXES = [
    "/workspace/system/",
]
SESSION_UPLOADS_DIR = os.getenv("SESSION_UPLOADS_DIR", "./app/workspace/sessions")
