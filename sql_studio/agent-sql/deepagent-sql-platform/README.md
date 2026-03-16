mkdir deepagent-sql-platform
cd deepagent-sql-platform

# ---------- FRONTEND ----------
mkdir frontend
mkdir frontend\src
mkdir frontend\src\components
mkdir frontend\src\components\CsvUpload
mkdir frontend\src\components\ChatEditor
mkdir frontend\src\components\SqlEditor
mkdir frontend\src\components\ApprovalPanel
mkdir frontend\src\pages
mkdir frontend\src\services

New-Item frontend\src\pages\MainPage.jsx
New-Item frontend\src\services\api.js
New-Item frontend\package.json

# ---------- BACKEND ----------
mkdir backend
mkdir backend\app
mkdir backend\app\api
mkdir backend\app\agents
mkdir backend\app\agents\tools
mkdir backend\app\agents\middleware
mkdir backend\app\memory
mkdir backend\app\memory\session_md
mkdir backend\app\skills
mkdir backend\app\skills\sql_templates
mkdir backend\app\skills\prompts
mkdir backend\app\skills\validation_rules

New-Item backend\app\main.py
New-Item backend\app\config.py

New-Item backend\app\api\csv_upload.py
New-Item backend\app\api\chat.py
New-Item backend\app\api\approval.py
New-Item backend\app\api\sql_management.py
New-Item backend\app\api\lineage.py

New-Item backend\app\agents\deepagent.py

New-Item backend\app\agents\tools\neo4j_tool.py
New-Item backend\app\agents\tools\sql_generator_tool.py
New-Item backend\app\agents\tools\sql_rewrite_tool.py
New-Item backend\app\agents\tools\filesystem_tool.py
New-Item backend\app\agents\tools\chat_memory_tool.py

New-Item backend\app\agents\middleware\human_approval.py
New-Item backend\app\agents\middleware\subagent.py
New-Item backend\app\agents\middleware\memory_summary.py

New-Item backend\app\memory\sqlite_store.py

New-Item backend\requirements.txt
New-Item README.md

## DeepAgent backend routing (implemented)

`backend/app/agents/deepagent.py` now calls:

`create_deep_agent(..., backend=build_backend)`

Backend factory is in:

`backend/app/agents/backend_factory.py`

It uses:

- `CompositeBackend(default=StateBackend(runtime), routes=...)`
- `/workspace/*` -> `FilesystemBackend(root_dir=AGENT_WORKSPACE_DIR, virtual_mode=True)`
- `/memories/*` -> `FilesystemBackend(root_dir=AGENT_MEMORIES_DIR, virtual_mode=True)`
- `PolicyWrapper` to block writes/edits under configured denied prefixes

### Configure paths

Set env vars (optional):

- `AGENT_WORKSPACE_DIR` (default: `./app/workspace`)
- `AGENT_MEMORIES_DIR` (default: `./app/memory/session_md`)
- `OPENAI_API_KEY`

### Install and run backend

```bash
cd backend
pip install -r requirements.txt
uvicorn app.main:app --reload --port 8000
```
