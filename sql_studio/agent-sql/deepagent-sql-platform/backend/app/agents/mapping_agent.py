import json
import logging
from langchain_openai import ChatOpenAI
from langchain_core.messages import SystemMessage, HumanMessage
from app.agents.tools.neo4j_schema_tool import get_tables_by_layer, get_table_schema
from app.config import OPENAI_API_KEY

logger = logging.getLogger(__name__)

# Use the same key as the main app
llm = ChatOpenAI(
    model="gpt-4o-mini",
    temperature=0,
    api_key=OPENAI_API_KEY
)

async def run_mapping_agent_stream(session_id: str, message: str):
    """
    Streams events back: ('progress', node_name) or ('chunk', text)
    """
    yield "progress", "Detect Intent"
    
    intent_prompt = f"""
    You are a Data Engineering Assistant. The user wants to create a new mapping sheet.
    Identify the target layer (e.g., CDP, FDP, ODP) and the concept of the new table.
    User message: {message}
    Return ONLY a JSON object with keys "target_layer" and "table_concept".
    If no layer is specified, default to "CDP".
    """
    response = llm.invoke([SystemMessage(content=intent_prompt)])
    try:
        raw = str(getattr(response, "content", "")).strip()
        raw = raw.strip("```json").strip("```").strip()
        intent = json.loads(raw)
        target_layer = intent.get("target_layer", "CDP").upper()
        table_concept = intent.get("table_concept", message)
    except Exception as e:
        logger.error(f"Failed to parse intent: {e}")
        target_layer = "CDP"
        table_concept = message

    yield "progress", "Fetch Preceding Layer"
    layer_mapping = {"CDP": "FDP", "FDP": "ODP", "ODP": "STG"}
    source_layer = layer_mapping.get(target_layer, "FDP")
    
    tables = get_tables_by_layer(source_layer)
    if not tables:
        msg = f"No tables found in the source layer ({source_layer}). I cannot build a mapping."
        yield "chunk", msg
        return

    yield "progress", "Determine Source Tables"
    
    table_list_str = "\n".join([f"- {t['table_id']} (Name: {t['table_name']})" for t in tables])
    table_select_prompt = f"""
    The user wants to create a new {target_layer} table for: {table_concept}
    Here are the available tables in the {source_layer} layer:
    {table_list_str}
    
    Based ONLY on the names, select the most relevant source tables (max 3) needed to build mapping for {table_concept}.
    Return ONLY a JSON list of table_ids. Example: ["FDP.finance.transactions"]
    """
    resp_tables = llm.invoke([SystemMessage(content=table_select_prompt)])
    try:
        raw_tbl = str(getattr(resp_tables, "content", "")).strip()
        raw_tbl = raw_tbl.strip("```json").strip("```").strip()
        selected_tables = json.loads(raw_tbl)
        if not isinstance(selected_tables, list):
            selected_tables = [tables[0]['table_id']] if tables else []
    except:
        selected_tables = [tables[0]['table_id']] if tables else []

    yield "progress", "Fetch Column Schemas"
    schema_context = []
    for sid in selected_tables:
        cols = get_table_schema(sid)
        schema_context.append({
            "table_id": sid,
            "columns": cols
        })

    yield "progress", "Generate Mapping"

    mapping_prompt = f"""
    You are an expert Data Architect. Create a detailed mapping sheet.
    Target Concept: {table_concept} (Layer: {target_layer})
    Source Schemas from {source_layer}:
    {json.dumps(schema_context, indent=2)}

    Create a mapping sheet that lists the target columns and exactly how they are derived from the source columns.
    Return your response as a JSON array of objects, inside a markdown code block ` ```json ... ``` ` so the frontend can parse it and render it as a table.
    Each object should have EXACTLY these keys:
    - "target_column": name of new column
    - "target_datatype": datatype
    - "source_tables": comma separated list of source tables used
    - "source_columns": comma separated list of source columns
    - "transformation_logic": simple SQL-like logic (e.g. "SUM(source_col)", "Direct map", "CASE WHEN...")
    
    Also, add some explanatory text outside the JSON block so I can stream it as chat text.
    First talk about what you built, then provide the JSON block.
    """

    chunks = llm.stream([SystemMessage(content="You are a data architect."), HumanMessage(content=mapping_prompt)])
    for chunk in chunks:
        text = getattr(chunk, "content", "")
        if text:
            yield "chunk", text

    yield "progress", "Done"
