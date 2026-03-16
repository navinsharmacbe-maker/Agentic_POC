from fastapi import APIRouter, HTTPException
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
from app.agents.deepagent import run_agent, run_agent_stream
from app.config import SESSION_UPLOADS_DIR
from pathlib import Path
import logging
import pandas as pd
from pandas.errors import ParserError
from typing import Set
import os
import asyncio
from mcp.client.streamable_http import streamablehttp_client
from typing import Set, List, Dict ,Any # Add List and Dict here
from app.memory.session_context import save_context,load_context
from app.memory.sqlite_store import save_message
# ---------------------------------------------------
# Router + Logging
# ---------------------------------------------------

router = APIRouter()
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

from mcp import ClientSession
from mcp.client.sse import sse_client
# ---------------------------------------------------
# MCP CONFIG
# ---------------------------------------------------
MCP_URL = os.getenv("MCP_URL", "http://localhost:8002/sse")

import json # Ensure this is imported at the top
import re
from langchain_core.messages import AIMessage

async def fetch_table_schemas_batch(table_names: List[str]) -> Dict[str, Any]:
    """
    Opens a single MCP connection and fetches multiple table schemas 
    simultaneously for better performance.
    """
    if not table_names:
        return {}

    schemas = {}
    try:
        async with sse_client(MCP_URL) as (read, write):
            async with ClientSession(read, write) as session:
                await session.initialize()
                
                # Create concurrent tasks for all tables
                tasks = [
                    session.call_tool("get_table_schema", arguments={"table_name": t}) 
                    for t in table_names
                ]
                
                # Execute in parallel
                results = await asyncio.gather(*tasks, return_exceptions=True)
                
                for table_name, result in zip(table_names, results):
                    if isinstance(result, Exception):
                        logger.error(f"Error fetching {table_name}: {result}")
                        continue
                    
                    if result.content:
                        raw_text = result.content[0].text
                        try:
                            schemas[table_name] = json.loads(raw_text)
                        except json.JSONDecodeError:
                            schemas[table_name] = {"raw_output": raw_text}
                return schemas
    except Exception as e:
        logger.error(f"MCP Batch Handshake failed: {e}")
        return {}

# ---------------------------------------------------
# Chat Endpoint
# ---------------------------------------------------

class ChatRequest(BaseModel):
    session_id: str
    message: str

def _read_csv_with_fallback(file_path: Path):
    try:
        return pd.read_csv(file_path)
    except ParserError:
        return pd.read_csv(file_path, engine="python", on_bad_lines="skip")

def _normalize_metadata_df(df: pd.DataFrame) -> pd.DataFrame:
    normalized = df.copy()
    normalized.columns = [str(col).strip().lower() for col in normalized.columns]

    alias_to_canonical = {
        "target_layer": "layer",
        "target_schema": "schema",
        "target_table": "table_name",
    }
    rename_map = {}
    for alias, canonical in alias_to_canonical.items():
        if canonical not in normalized.columns and alias in normalized.columns:
            rename_map[alias] = canonical
    if rename_map:
        normalized = normalized.rename(columns=rename_map)

    if (
        "source_tables" not in normalized.columns
        and {"source_layer", "source_schema", "source_table"}.issubset(normalized.columns)
    ):
        def _compose_source_table(row):
            raw_layer = row.get("source_layer", "")
            raw_schema = row.get("source_schema", "")
            raw_table = row.get("source_table", "")

            if pd.isna(raw_layer) or pd.isna(raw_schema) or pd.isna(raw_table):
                return None

            layer = str(raw_layer).strip()
            schema = str(raw_schema).strip()
            table = str(raw_table).strip()

            if not layer or not schema or not table:
                return None
            if table.upper() == "N/A":
                return None
            if table.lower() in {"nan", "none", "null"}:
                return None
            return f"{layer}.{schema}.{table}"

        normalized["source_tables"] = normalized.apply(_compose_source_table, axis=1)

    return normalized


def _tables_from_session_csv(session_id: str) -> Set[str]:
    session_path = Path(SESSION_UPLOADS_DIR) / session_id
    if not session_path.exists():
        return set()

    source_tables: Set[str] = set()
    for file_path in session_path.glob("*.csv"):
        try:
            df = _read_csv_with_fallback(file_path)
        except Exception:
            continue

        df = _normalize_metadata_df(df)

        # Source lineage tables in metadata CSV
        if "source_tables" in df.columns:
            for val in df["source_tables"].dropna():
                source_tables.update({t.strip() for t in str(val).split(",") if t.strip()})

        # Also include target table names when available
        if {"layer", "schema", "table_name"}.issubset(df.columns):
            for _, row in df[["layer", "schema", "table_name"]].dropna().drop_duplicates().iterrows():
                source_tables.add(f"{row['layer']}.{row['schema']}.{row['table_name']}")

    return source_tables

@router.post("/")
async def chat(req: ChatRequest):
    return run_agent(req.session_id, req.message)


def _chunk_text(text: str, chunk_size: int = 48) -> list[str]:
    tokens = re.findall(r"\S+\s*", text or "")
    if not tokens:
        return []
    chunks: list[str] = []
    buf = ""
    for tok in tokens:
        if len(buf) + len(tok) > chunk_size and buf:
            chunks.append(buf)
            buf = tok
        else:
            buf += tok
    if buf:
        chunks.append(buf)
    return chunks


@router.post("/stream")
async def chat_stream(req: ChatRequest):
    def _token_text(token_obj: Any) -> str:
        content = getattr(token_obj, "content", token_obj)
        if isinstance(content, str):
            return content
        if isinstance(content, list):
            parts: List[str] = []
            for item in content:
                if isinstance(item, dict):
                    if item.get("type") == "text" and item.get("text"):
                        parts.append(str(item.get("text")))
                elif hasattr(item, "text"):
                    txt = getattr(item, "text", "")
                    if txt:
                        parts.append(str(txt))
            return "".join(parts)
        return ""

    async def event_gen():
        yield f"event: status\ndata: {json.dumps({'status': 'started'})}\n\n"
        full_reply = ""
        final_reply = ""
        try:
            async for mode, payload in run_agent_stream(
                req.session_id,
                req.message,
                stream_mode=["messages", "updates"],
            ):
                if mode == "messages":
                    token_obj, metadata = payload
                    text = _token_text(token_obj)
                    if text:
                        full_reply += text
                        node = metadata.get("langgraph_node") if isinstance(metadata, dict) else None
                        yield f"event: chunk\ndata: {json.dumps({'content': text, 'node': node})}\n\n"
                elif mode == "updates" and isinstance(payload, dict):
                    for node_name, node_update in payload.items():
                        if not isinstance(node_update, dict):
                            continue
                        yield f"event: progress\ndata: {json.dumps({'node': node_name})}\n\n"
                        msgs = node_update.get("messages") or []
                        if msgs:
                            last = msgs[-1]
                            if isinstance(last, AIMessage):
                                content = str(getattr(last, "content", "") or "")
                                if content:
                                    final_reply = content
        except Exception as exc:
            logger.exception("chat_stream failed | session=%s error=%s", req.session_id, exc)
            err = str(exc)
            yield f"event: error\ndata: {json.dumps({'error': err})}\n\n"
            yield f"event: done\ndata: {json.dumps({'status': 'error'})}\n\n"
            return

        reply_text = final_reply or full_reply
        save_message(req.session_id, "user", req.message)
        save_message(req.session_id, "assistant", reply_text)
        yield f"event: done\ndata: {json.dumps({'status': 'completed', 'chat_reply': reply_text})}\n\n"

    return StreamingResponse(
        event_gen(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


# ---------------------------------------------------
# Plan Builder (Graph-Enforced)
# ---------------------------------------------------

# --- HELPER: THE SMART PRUNER ---
def prune_graph_context(target_tables: List[str], full_graph: Dict, business_spec: Dict) -> Dict:
    """
    Filters the massive graph_context to only include schemas for 
    the target tables and their direct dependencies.
    """
    required_table_ids = set(target_tables)
    
    for target in target_tables:
        spec = business_spec.get(target, {})
        # Add all sources identified for this specific target table
        sources = spec.get("sources", [])
        required_table_ids.update(sources)
    
    # Return only the schemas present in our required set
    return {k: v for k, v in full_graph.items() if k in required_table_ids}

# --- THE UPDATED ENDPOINT ---
@router.post("/plan/{session_id}")
async def build_plan_from_session(session_id: str):
    session_path = Path(SESSION_UPLOADS_DIR) / session_id
    if not session_path.exists():
        raise HTTPException(status_code=404, detail="Session folder not found.")

    # ---------------------------------------------------
    # 1. ATTEMPT TO LOAD FROM CACHE (Context Check)
    # ---------------------------------------------------
    context_data = load_context(session_id) or {}
    graph_context = context_data.get("graph_context")
    business_spec = context_data.get("business_spec")

    # If data is missing from cache, we perform the full rebuild
    if not graph_context or not business_spec:
        logger.info(f"Context missing for {session_id}, rebuilding from CSVs...")
        
        csv_files = list(session_path.glob("*.csv"))
        if not csv_files:
            raise HTTPException(status_code=404, detail="No CSV files found.")

        source_tables_set: Set[str] = set()
        temp_business_spec = {}

        # Process CSV files to find sources and target definitions
        for file_path in csv_files:
            try:
                df = pd.read_csv(file_path)
                df = _normalize_metadata_df(df)
            except Exception as e:
                logger.error(f"Failed to read {file_path}: {e}")
                continue

            required_cols = {"layer", "schema", "table_name", "column_name", "transformation_logic"}
            if not required_cols.issubset(df.columns):
                logger.warning(
                    "Skipping %s due to missing columns: %s",
                    file_path.name,
                    ", ".join(sorted(required_cols - set(df.columns))),
                )
                continue

            grouped = df.groupby(["layer", "schema", "table_name"])
            for (layer, schema, table), group_df in grouped:
                full_target_name = f"{layer}.{schema}.{table}"
                
                current_table_sources = set()
                for val in group_df["source_tables"].dropna():
                    parts = [s.strip() for s in str(val).split(",") if s.strip()]
                    current_table_sources.update(parts)
                
                source_tables_set.update(current_table_sources)
                
                # Store structural info for the spec
                temp_business_spec[full_target_name] = {
                    "sources": list(current_table_sources),
                    "columns": {row['column_name']: row['transformation_logic'] for _, row in group_df.iterrows()}
                }

        if not source_tables_set:
            raise HTTPException(status_code=400, detail="No source tables found.")

        # Batch fetch schemas in parallel
        raw_schemas = await fetch_table_schemas_batch(list(source_tables_set))
        
        # Normalize and build the graph context
        new_graph_context = {}
        for table_name, schema_data in raw_schemas.items():
            if schema_data:
                normalized_cols = []
                for col in schema_data.get("columns", []):
                    normalized_cols.append({
                        "name": col.get("name") or col.get("column_name"),
                        "datatype": col.get("type") or col.get("datatype")
                    })
                new_graph_context[table_name] = {
                    "columns": normalized_cols,
                    "relationships": schema_data.get("relationships", [])
                }

        # Save to cache for next time
        graph_context = new_graph_context
        business_spec = temp_business_spec
        save_context(session_id, {
            "source_tables": list(source_tables_set),
            "graph_context": graph_context,
            "business_spec": business_spec
        })

    # ---------------------------------------------------
    # 2. GENERATE METADATA BLOCKS (Always rebuild for prompt)
    # ---------------------------------------------------
    all_metadata_blocks = []
    for target_name, spec in business_spec.items():
        columns_info = []
        for col_name, logic in spec.get("columns", {}).items():
            columns_info.append(f"- Column: {col_name}\n  Logic: {logic}")
            
        all_metadata_blocks.append(
            f"TARGET TABLE: {target_name}\n"
            f"COLUMNS TO GENERATE:\n" + "\n".join(columns_info)
        )

    # ---------------------------------------------------
    # 3. SMART PRUNING & PROMPT EXECUTION
    # ---------------------------------------------------
    target_tables = list(business_spec.keys())
    pruned_graph = prune_graph_context(target_tables, graph_context, business_spec)
    
    final_metadata = "\n\n---\n\n".join(all_metadata_blocks)
    final_graph_json = json.dumps(pruned_graph, indent=2)

    instruction_prompt = f"""You are a senior Data Warehouse Architect.

### OBJECTIVE
1. Interpret transformation logic and generate valid SQL CREATE TABLE statements.
2. Apply GROUP BY logic where aggregates (SUM, COUNT, etc.) are present.
3. Use the GRAPH SCHEMA CONTEXT to validate that source columns and joins actually exist.
4. IMPORTANT: If source columns contain '|' (e.g. first_name|last_name), it means multiple source columns are used in that logic.

### METADATA SPECIFICATION
{final_metadata}

### GRAPH SCHEMA CONTEXT (PRUNED)
{final_graph_json}

### OUTPUT FORMAT
Return ONLY a valid JSON object:
{{
  "table_objective": "Description of what this table achieves",
  "business_purpose": "The business value of this data",
  "final_sql": "The full SQL statement"
}}"""

    return run_agent(
        session_id=session_id,
        user_input=instruction_prompt,
        use_history=False,
        persist_history=False,
    )
