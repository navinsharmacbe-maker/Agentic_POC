import sqlite3
from app.config import SQLITE_DB

def init_db():
    conn = sqlite3.connect(SQLITE_DB)
    c = conn.cursor()
    c.execute("""
        CREATE TABLE IF NOT EXISTS chat_history (
            session_id TEXT,
            role TEXT,
            content TEXT
        )
    """)
    conn.commit()
    conn.close()

def save_message(session_id, role, content):
    conn = sqlite3.connect(SQLITE_DB)
    c = conn.cursor()
    c.execute(
        "INSERT INTO chat_history VALUES (?, ?, ?)",
        (session_id, role, content)
    )
    conn.commit()
    conn.close()

def load_messages(session_id):
    conn = sqlite3.connect(SQLITE_DB)
    c = conn.cursor()
    c.execute(
        "SELECT role, content FROM chat_history WHERE session_id=? ORDER BY rowid ASC",
        (session_id,)
    )
    rows = c.fetchall()
    conn.close()
    return rows
