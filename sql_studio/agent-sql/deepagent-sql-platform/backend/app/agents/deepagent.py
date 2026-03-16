import os
import json
import re
import logging
import sqlglot
from pathlib import Path
from datetime import datetime
from typing import TypedDict, Annotated, List, Dict, Any, Callable, Optional
from sqlglot import exp
from langchain_openai import ChatOpenAI
from langchain_core.messages import SystemMessage, HumanMessage, AIMessage, BaseMessage
from langgraph.graph import StateGraph, END, add_messages
from langchain_core.prompts import ChatPromptTemplate
from app.agents.tools.sql_rewrite_tool import rewrite_sql
from app.agents.tools.neo4j_tool import run_cypher
from app.agents.middleware.human_approval import requires_approval
from app.config import OPENAI_API_KEY
from app.memory.sqlite_store import save_message, load_messages
from app.memory.session_context import load_context, save_context
from pydantic import BaseModel, Field
from rapidfuzz import process, fuzz

LOG_DIR = Path(__file__).resolve().parents[2] / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)
LOG_FILE = LOG_DIR / "deepagent.log"

_root_logger = logging.getLogger()
_root_logger.setLevel(logging.INFO)
if not any(isinstance(h, logging.FileHandler) and Path(getattr(h, "baseFilename", "")) == LOG_FILE for h in _root_logger.handlers):
    _file_handler = logging.FileHandler(LOG_FILE, encoding="utf-8")
    _file_handler.setLevel(logging.INFO)
    _file_handler.setFormatter(logging.Formatter("%(asctime)s | %(levelname)s | %(name)s | %(message)s"))
    _root_logger.addHandler(_file_handler)

if not _root_logger.handlers:
    logging.basicConfig(level=logging.INFO)

logger = logging.getLogger(__name__)
logger.info("DeepAgent file logging initialized at %s", LOG_FILE)
# =========================================================
# 1️⃣ SYSTEM PROMPT
# =========================================================

SYSTEM_PROMPT = """
You are a Senior Data Warehouse Architect.

Rules:
- Never hallucinate tables or columns.
- Always validate against provided GRAPH CONTEXT.
- Prefer explicit JOIN conditions.
- If unsure about schema, ask or check schema tool.
- Maintain conversation continuity.
"""

# =========================================================
# 2️⃣ SESSION STATE
# =========================================================

class AgentState(TypedDict):
    messages: Annotated[List[BaseMessage], add_messages]
    user_input: str
    session_id: str
    intent: str
    last_generated_sql: str
    last_validation_errors: List[str]
    last_intent: str
    last_schema_lookup: Dict[str, Any]
    business_context: Dict[str, Any]
    orchestration_status: Dict[str, str]
    next_step: str   # ✅ ADD THIS
    stream_callback: Optional[Callable[[str], None]]

class OrchestratorDecision(BaseModel):
    next_step: str = Field(
        description="One of: pruning, sql_generate, sql_modify, schema, business, chat"
    )
    reasoning: str = Field(description="Why this route was chosen")


def _invoke_llm_text(messages: List[BaseMessage], stream_callback: Optional[Callable[[str], None]] = None) -> str:
    if stream_callback:
        chunks: List[str] = []
        for chunk in llm.stream(messages):
            text = getattr(chunk, "content", "")
            if text:
                chunks.append(text)
                try:
                    stream_callback(text)
                except Exception:
                    pass
        return "".join(chunks)

    response = llm.invoke(messages)
    return str(getattr(response, "content", ""))

def clean_table_name(table_full_name: str) -> str:
    """
    Ensure table name keeps only schema.table (or db.schema.table),
    and avoid repeating db/schema if already included.
    """
    # Split by dots
    logger.info(f"Cleaning table name: {table_full_name}")
    parts = table_full_name.split(".")
    if len(parts) >= 3:
        # Check if the last two parts are the same as a known prefix (like FDP.finance)
        if parts[-3] == parts[-2]:
            return ".".join(parts[-2:])  # keep only schema.table
        return ".".join(parts[-3:])  # db.schema.table
    return table_full_name

def orchestrator(state: AgentState):
    session_id = state["session_id"]
    context = load_context(session_id) or {}
    compiler_state = context.get("compiler_state", {})

    orchestration_prompt = f"""
You are a Workflow Orchestrator for a SQL AI system.

Available Nodes:
- pruning (prepare schema context for SQL tasks)
- sql_generate
- sql_modify
- schema
- business
- chat

Current Intent: {state.get("intent")}
User Input: {state.get("user_input")}

Compiler State:
{json.dumps(compiler_state, indent=2)}

Conversation Context:
Last Generated SQL: {compiler_state.get("last_generated_sql")}
Validation Errors: {compiler_state.get("last_validation_errors")}
Approved: {compiler_state.get("approved")}

Rules:
1. If SQL has validation errors → retry sql_generate.
2. If user modifies SQL → choose sql_modify.
3. If schema requested → schema.
4. If business spec requested → business.
5. If clarification needed → chat.
6. If new SQL request → pruning.

Return structured output.
"""

    decision_chain = llm.with_structured_output(OrchestratorDecision)

    result = decision_chain.invoke([
        SystemMessage(content="You are a system-level workflow controller."),
        HumanMessage(content=orchestration_prompt)
    ])
    logger.info(
        "Orchestrator decision | session=%s intent=%s next_step=%s reason=%s",
        session_id,
        state.get("intent"),
        result.next_step,
        result.reasoning,
    )

    if state.get("intent") in ["sql_generate", "sql_modify"] \
        and not state.get("last_schema_lookup"):
            result.next_step = "pruning"


    return {
        "next_step": result.next_step,
        "orchestration_status": {
            "decision_reason": result.reasoning
        }
    }


# =========================================================
# 3️⃣ LLM
# =========================================================

llm = ChatOpenAI(
    model="gpt-4o-mini",
    temperature=0,
    api_key=OPENAI_API_KEY)
# =========================================================)
# 4️⃣ INTENT ROUTER
# =========================================================
# Define a schema for the output so the LLM is forced to pick one
class IntentSchema(BaseModel):
    intent: str = Field(description="The classified intent: 'sql_generate', 'sql_modify', 'schema', 'business', or 'chat'")
    confidence: float = Field(description="Confidence score from 0 to 1")


class SchemaLookupPlan(BaseModel):
    operation: str = Field(description="One of: column, table, count, unknown")
    entity: str = Field(default="", description="Target table/column when applicable")
    count_kind: str = Field(default="", description="For count: tables or columns")
    layer: str = Field(default="", description="Optional: ODP/FDP/CDP")
    source: str = Field(default="auto", description="One of: auto, context, neo4j")

def detect_intent(state: AgentState):
    user_text = (state.get("user_input", "") or "").lower()

    # Deterministic override for schema-count questions that the LLM
    # may otherwise classify as "business".
    if re.search(r"\b(how many|count|number of)\b", user_text) and re.search(
        r"\b(table|tables|column|columns|schema|layer|odp|fdp|cdp)\b", user_text
    ):
        logger.info(
            "Intent override | session=%s intent=schema reason=count_schema_query input=%s",
            state.get("session_id"),
            state.get("user_input", ""),
        )
        return {
            "intent": "schema",
            "last_intent": "schema",
            "orchestration_status": {"confidence": "rule_override"},
        }

    prompt = ChatPromptTemplate.from_messages([
        ("system", "You are an Intent Classifier. Categories: sql_generate, sql_modify, schema, business, chat. "
                   "If the user asks to build a table from the docs, choose 'sql_generate'."),
        ("human", "{input}")
    ])

    chain = prompt | llm.with_structured_output(IntentSchema)
    
    try:
        result = chain.invoke({"input": state.get("user_input", "")})
        # If confidence is low, force to 'chat' so the assistant asks for clarification
        final_intent = result.intent if result.confidence >= 0.7 else "chat"
        logger.info(
            "Intent detected | session=%s intent=%s confidence=%.3f input=%s",
            state.get("session_id"),
            final_intent,
            result.confidence,
            state.get("user_input", ""),
        )
        
        return {
            "intent": final_intent, 
            "last_intent": final_intent,
            "orchestration_status": {"confidence": str(result.confidence)}
        }
    except Exception as exc:
        logger.exception("Intent detection failed | session=%s error=%s", state.get("session_id"), exc)
        return {"intent": "chat", "last_intent": "chat"}


def route_intent(state: AgentState):
    # This directly maps the LLM's decision to the Graph Node names
    return state["intent"]



def prune_schema(state: AgentState):
    """
    Production-grade pruner using Fuzzy Entity Resolution and 
    Recursive Dependency Mapping.
    """
    session_id = state["session_id"]
    user_input = state["user_input"]
    
    context_data = load_context(session_id) or {}
    full_graph = context_data.get("graph_context", {})
    full_business = context_data.get("business_spec", {})
    
    if not full_graph:
        logger.warning(f"No graph context found for session {session_id}")
        return {"last_schema_lookup": {}, "business_context": {}}

    # 1. ENTITY RESOLUTION (Fuzzy Match)
    # We extract potential table mentions from user input and match against graph keys
    all_table_identifiers = list(full_graph.keys())
    target_tables = set()
    
    # Split input into words/tokens to find candidate names
    words = re.findall(r'\b\w+\b', user_input)
    
    for word in words:
        if len(word) < 3: continue  # Skip tiny noise words
        
        # Find best match in our schema (Score > 85 is usually a solid match)
        match = process.extractOne(word, all_table_identifiers, scorer=fuzz.WRatio)
        if match and match[1] > 85:
            target_tables.add(match[0])

    # 2. FALLBACK: IF NO MATCH, CHECK BUSINESS SPEC
    if not target_tables:
        target_tables.update(full_business.keys())

    # 3. RECURSIVE DEPENDENCY FETCHING
    # Ensure we include all source tables required by the target tables
    required_entities = set(target_tables)
    for target in target_tables:
        # Check dependencies in the graph (Joins/Lineage)
        deps = full_graph.get(target, {}).get("relationships", [])
        for dep in deps:
            # dep might be a dict or string depending on your MCP structure
            dep_name = dep.get("to_table") if isinstance(dep, dict) else dep
            if dep_name in full_graph:
                required_entities.add(dep_name)
        
        # Check explicit sources in the business specification
        biz_sources = full_business.get(target, {}).get("sources", [])
        required_entities.update([s for s in biz_sources if s in full_graph])

    # 4. CONTEXT CONSTRUCTION
    pruned_graph = {k: full_graph[k] for k in required_entities if k in full_graph}
    pruned_business = {k: full_business[k] for k in target_tables if k in full_business}

    # 5. SAFETY GUARD: TOKENS & LOGGING
    logger.info(f"Pruning complete. Reduced context from {len(full_graph)} to {len(pruned_graph)} tables.")
    
    if not pruned_graph:
        # If still empty, we must provide the top-level business spec tables 
        # to prevent the LLM from being completely blind.
        logger.error("Pruning resulted in empty context. Falling back to full business spec keys.")
        return {"last_schema_lookup": full_graph, "business_context": full_business}

    return {
        "last_schema_lookup": pruned_graph,
        "business_context": pruned_business,
        "next_step": state["intent"]
    }
# 5️⃣ SQL UTILITIES
# =========================================================

def extract_sql_from_text(text: str) -> str:
    blocks = re.findall(r"```(?:sql)?\s*(.*?)```", text, re.I | re.S)
    if blocks:
        return blocks[0].strip()
    match = re.search(r"(SELECT[\s\S]+?;)", text, re.I)
    return match.group(1).strip() if match else ""

def extract_sql_metadata(sql_text: str):
    try:
        parsed = sqlglot.parse_one(sql_text)
    except Exception:
        return set(), set()
    tables = {t.name for t in parsed.find_all(exp.Table)}
    columns = {c.name for c in parsed.find_all(exp.Column)}
    return tables, columns

def resolve_table_name(table_name: str, graph_context: dict) -> str | None:
    if table_name in graph_context:
        return table_name
    for full_name in graph_context.keys():
        if full_name.split(".")[-1] == table_name:
            return full_name
    return None

def validate_sql_against_graph(
    sql_text: str,
    graph_context: Dict[str, Any],
    session_id: str
):
    """
    Validates SQL against graph schema.
    Returns:
        errors: List[str]
        resolved_tables: Dict[str, str]
    """

    errors = []
    tables, columns = extract_sql_metadata(sql_text)
    resolved_tables = {}

    if not graph_context:
        return ["Graph context is empty."], {}

    # ----------------------------
    # 1️⃣ Resolve Tables
    # ----------------------------
    for table in tables:
        resolved = resolve_table_name(table, graph_context)
        if not resolved:
            errors.append(f"Table '{table}' not found in graph context.")
        else:
            resolved_tables[table] = resolved
    if tables:
        logger.info(
            "SQL table resolution | session=%s requested=%s resolved=%s",
            session_id,
            sorted(list(tables)),
            resolved_tables,
        )

    # ----------------------------
    # 2️⃣ Validate Columns
    # Only check against resolved tables
    # ----------------------------
    for column in columns:
        column_lower = column.lower()
        found = False

        for resolved_table in resolved_tables.values():
            table_schema = graph_context.get(resolved_table, {})
            for col in table_schema.get("columns", []):
                if col.get("name", "").lower() == column_lower:
                    found = True
                    break
            if found:
                break

        if not found:
            errors.append(f"Column '{column}' not found in referenced tables.")

    # ----------------------------
    # 3️⃣ Persist Validation State
    # ----------------------------
    update_session_state(session_id, {
        "last_validation_errors": errors
    })
    if errors:
        logger.warning("SQL validation errors | session=%s errors=%s", session_id, errors)

    return errors, resolved_tables

# =========================================================
# 6️⃣ SESSION STATE MANAGER
# =========================================================

def update_session_state(session_id: str, updates: Dict[str, Any]):
    context = load_context(session_id) or {}
    context.setdefault("compiler_state", {})
    context.setdefault("orchestration_status", {})
    context["compiler_state"].update(updates)
    context["orchestration_status"].update({k: "completed" for k in updates.keys()})
    save_context(session_id, context)

def build_dependency_graph(target_tables: list, business_spec: dict):
    """
    Builds adjacency list for dependency graph.
    Graph direction: dependency -> dependent
    """

    graph = {table: [] for table in target_tables}

    for table in target_tables:
        spec = business_spec.get(table, {})

        # ✅ Use "sources" consistently
        dependencies = spec.get("sources", [])

        for dep in dependencies:
            if dep in target_tables:
                graph.setdefault(dep, [])
                graph[dep].append(table)

    return graph



from collections import deque

def topo_sort(graph: dict):
    """
    Returns execution order of tables.
    Raises error if cycle detected.
    """

    # Compute in-degree
    in_degree = {node: 0 for node in graph}

    for node in graph:
        for neighbor in graph[node]:
            in_degree[neighbor] += 1

    # Start with nodes having 0 in-degree
    queue = deque([node for node in graph if in_degree[node] == 0])

    sorted_order = []

    while queue:
        node = queue.popleft()
        sorted_order.append(node)

        for neighbor in graph[node]:
            in_degree[neighbor] -= 1
            if in_degree[neighbor] == 0:
                queue.append(neighbor)

    # Detect cycle
    if len(sorted_order) != len(graph):
        raise ValueError("❌ Cycle detected in model dependencies.")

    return sorted_order

# =========================================================
# 8️⃣ SQL PIPELINE (CSV aware)
# =========================================================
def sql_pipeline(state: AgentState):
    session_id = state["session_id"]
    user_input = state.get("user_input", "")

    context_data = load_context(session_id) or {}
    graph_context = state.get("last_schema_lookup", {})
    business_spec = state.get("business_context", {})
    compiler_state = context_data.get("compiler_state", {})
    previous_sql = compiler_state.get("last_generated_sql")

    csv_data = context_data.get("uploaded_csv", {})
    compiled_cache = compiler_state.get("compiled_models", {})
    stream_callback = state.get("stream_callback")

    # -----------------------------------------
    # 1️⃣ MULTI-MODEL MODE (If business_spec present)
    # -----------------------------------------
    if business_spec:

        target_tables = list(business_spec.keys())

        # Build dependency graph
        dep_graph = build_dependency_graph(target_tables, business_spec)

        # Topological sort
        execution_order = topo_sort(dep_graph)

        compiled_statements = []
        new_compiled_cache = compiled_cache.copy()

        for table in execution_order:

            table_spec = business_spec.get(table, {})

            # Skip if already compiled and no change
            spec_hash = hash(json.dumps(table_spec, sort_keys=True))
            if table in compiled_cache and compiled_cache[table]["hash"] == spec_hash:
                compiled_statements.append(compiled_cache[table]["sql"])
                continue

            # CSV sample scoped per table if available
            table_csv = csv_data.get(table, [])
            csv_sample = table_csv[:5] if table_csv else []

            safe_table_name = clean_table_name(table)  
            prompt = f"""
            You are a Senior Data Warehouse Architect.

            Target Table:
            {safe_table_name}

            Business Spec:
            {json.dumps(table_spec, indent=2)}

            Sample CSV Rows (if available):
            {json.dumps(csv_sample, indent=2)}

            Available Schemas:
            {json.dumps(graph_context, indent=2)}

            Rules:
            1. Use the exact table names provided.
            2. Do NOT prepend database or schema if already included in the target table name.
            3. Generate SELECT transformation logic.
            4. Do NOT hallucinate columns.
            5. Return SQL inside ```sql``` block ONLY.
            6. Use `CREATE OR REPLACE TABLE <table_name> AS` for table creation.
            7. Respect dependency order.
            8. Always append `LIMIT 100` for preview/testing.

            """


            response_text = _invoke_llm_text([
                SystemMessage(content=SYSTEM_PROMPT + "\n\nGRAPH CONTEXT:\n" + json.dumps(graph_context, indent=2)),
                HumanMessage(content=prompt)
            ], stream_callback=stream_callback)

            raw_sql = extract_sql_from_text(response_text)
            if not raw_sql:
                return {"messages": [AIMessage(content=f"❌ Failed generating SQL for {table}")]}

            safe_sql = rewrite_sql(raw_sql, allow_ddl=True,allow_dml=True)
            # Validation loop
            for _ in range(3):
                errors, resolved_tables = validate_sql_against_graph(
                    safe_sql, graph_context, session_id
                )

                for short, full in resolved_tables.items():
                    if short != full:
                        safe_sql = re.sub(rf"\b{short}\b", full, safe_sql)

                if not errors:
                    break

                fix_prompt = f"""
                    SQL has validation errors:
                    {errors}

                    Fix using schema context.
                    Return corrected SQL only.
                    """
                fix_response_text = _invoke_llm_text([
                    SystemMessage(content=SYSTEM_PROMPT),
                    HumanMessage(content=fix_prompt)
                ], stream_callback=stream_callback)

                corrected = extract_sql_from_text(fix_response_text)
                if not corrected:
                    break

                safe_sql = rewrite_sql(corrected, allow_ddl=True,allow_dml=True)

            # LLM already generated CREATE OR REPLACE TABLE
            if not re.search(r"CREATE\s+OR\s+REPLACE\s+TABLE", safe_sql, re.I):
                safe_table_name = clean_table_name(table)
                full_sql = f"CREATE OR REPLACE TABLE {safe_table_name} AS\n{safe_sql}"
            else:
                full_sql = safe_sql

            compiled_statements.append(full_sql)

            new_compiled_cache[table] = {
                "sql": full_sql,
                "hash": spec_hash
            }

        models_sql = "\n\n".join(compiled_statements)

        procedure_sql = f"""
        CREATE OR REPLACE PROCEDURE build_enterprise_models()
        RETURNS STRING
        LANGUAGE SQL
        AS
        $$
        BEGIN

        {models_sql}

        RETURN 'Enterprise Build Completed Successfully';

        END;
        $$;
        """

        final_sql = procedure_sql


    # -----------------------------------------
    # 2️⃣ MODIFY MODE
    # -----------------------------------------
    elif state.get("intent") == "sql_modify" and previous_sql:

        prompt = f"""
            Previous SQL:
            {previous_sql}

            User Request:
            {user_input}

            Return ONLY SQL inside ```sql``` block.
            """

        response_text = _invoke_llm_text([
            SystemMessage(content=SYSTEM_PROMPT),
            HumanMessage(content=prompt)
        ], stream_callback=stream_callback)

        final_sql = rewrite_sql(
            extract_sql_from_text(response_text)
        )

    # -----------------------------------------
    # 3️⃣ SIMPLE GENERATION MODE
    # -----------------------------------------
    else:

        prompt = f"""
            Generate SQL using schema context and CSV samples.

            User Request:
            {user_input}

            Schemas:
            {json.dumps(graph_context, indent=2)}

            CSV Data:
            {json.dumps(csv_data, indent=2)}

            Return SQL inside ```sql``` block.
            """

        response_text = _invoke_llm_text([
            SystemMessage(content=SYSTEM_PROMPT),
            HumanMessage(content=prompt)
        ], stream_callback=stream_callback)

        final_sql = rewrite_sql(
            extract_sql_from_text(response_text)
        )

    # -----------------------------------------
    # FINAL VALIDATION (skip for procedure mode)
    # -----------------------------------------
    if not business_spec:
        final_errors, _ = validate_sql_against_graph(
            final_sql, graph_context, session_id
        )

        if final_errors:
            return {"messages": [AIMessage(content=f"⚠️ SQL validation failed:\n{final_errors}")]}


    # -----------------------------------------
    # SAVE SQL FILE
    # -----------------------------------------
    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    filename = f"{session_id}_{timestamp}_build.sql"

    os.makedirs("./docs", exist_ok=True)
    with open(f"./docs/{filename}", "w") as f:
        f.write(final_sql)

    # -----------------------------------------
    # UPDATE SESSION STATE
    # -----------------------------------------
    update_session_state(session_id, {
    "last_generated_sql": final_sql,
    "compiled_models": new_compiled_cache if business_spec else compiled_cache,
    "last_sql_file": filename,
    "approved": not requires_approval(final_sql)
})


    return {
        "messages": [
            AIMessage(content=f"✅ SQL build generated:\n```sql\n{final_sql}\n```")
        ]
    }

# =========================================================
# 9️⃣ SCHEMA PIPELINE (supports column queries)
# =========================================================
def _format_schema_response(user_query: str, resolved_data: Any, fallback_text: str | None = None) -> str:
    """
    Optional LLM formatter for schema responses.
    Keeps deterministic data as source of truth.
    """
    raw_payload = resolved_data if isinstance(resolved_data, str) else json.dumps(resolved_data, indent=2)
    try:
        prompt = f"""
User Query:
{user_query}

Resolved Data (ground truth):
{raw_payload}

Instructions:
- Do not add or change facts.
- Keep it concise and readable.
- If list has multiple matches, show numbered bullets.
- If this is an error/not-found, keep it explicit.
Return plain text only.
"""
        resp = llm.invoke([
            SystemMessage(content="Format schema lookup results without changing facts."),
            HumanMessage(content=prompt),
        ])
        formatted = str(getattr(resp, "content", "")).strip()
        if formatted:
            return formatted
    except Exception as exc:
        logger.warning("Schema formatting failed: %s", exc)

    return fallback_text or str(raw_payload)


def schema_pipeline(state: AgentState):
    session_id = state["session_id"]
    user_input_raw = state["user_input"]
    user_input = user_input_raw.lower()
    user_input = re.sub(r"\bscehma\b", "schema", user_input)
    context_data = load_context(session_id) or {}
    graph_context = context_data.get("graph_context", {})

    all_tables = list(graph_context.keys())
    all_columns = []
    column_to_tables: Dict[str, List[Dict[str, Any]]] = {}
    for table_name, table_schema in graph_context.items():
        for col in table_schema.get("columns", []):
            col_name = str(col.get("name", "")).strip()
            if not col_name:
                continue
            all_columns.append(col_name)
            column_to_tables.setdefault(col_name.lower(), []).append({
                "table": table_name,
                "column": col_name,
                "type": col.get("datatype"),
                "primary_key": col.get("primary_key", False),
            })

    logger.info(
        "Schema pipeline | session=%s graph_tables=%d graph_columns=%d input=%s",
        session_id,
        len(all_tables),
        len(all_columns),
        user_input_raw,
    )

    def fmt(resolved: Any, fallback: str | None = None) -> str:
        return _format_schema_response(user_input_raw, resolved, fallback)

    schema_plan = None
    try:
        schema_plan_chain = llm.with_structured_output(SchemaLookupPlan)
        schema_plan = schema_plan_chain.invoke([
            SystemMessage(content=(
                "Classify schema lookup requests. "
                "operation=count for count/how-many questions; set count_kind=tables or columns; "
                "extract layer ODP/FDP/CDP if present; source=neo4j only when explicitly requested."
            )),
            HumanMessage(content=user_input_raw),
        ])
        logger.info(
            "Schema LLM plan | session=%s operation=%s entity=%s count_kind=%s layer=%s source=%s",
            session_id,
            schema_plan.operation,
            schema_plan.entity,
            schema_plan.count_kind,
            schema_plan.layer,
            schema_plan.source,
        )
    except Exception as exc:
        logger.warning("Schema LLM planning failed | session=%s error=%s", session_id, exc)

    # ----------------------------------------
    # 0️⃣ COUNT / HOW-MANY QUERIES
    # ----------------------------------------
    force_neo4j = bool(re.search(r"\bneo4j\b", user_input))
    is_count_request = False
    target_kind = ""
    target_layer = ""

    if schema_plan and str(schema_plan.operation).lower() == "count":
        is_count_request = True
        target_kind = (schema_plan.count_kind or "").lower()
        target_layer = (schema_plan.layer or "").upper()
        if str(schema_plan.source).lower() == "neo4j":
            force_neo4j = True
    else:
        count_requested = bool(re.search(r"\b(count|how many|number of)\b", user_input))
        kind_match = re.search(r"\b(table|tables|column|columns)\b", user_input)
        layer_match = re.search(r"\b(odp|fdp|cdp)\b", user_input)
        if count_requested and kind_match:
            is_count_request = True
            target_kind = kind_match.group(1).lower()
            target_layer = (layer_match.group(1) if layer_match else "").upper()

    if is_count_request and target_kind in {"table", "tables", "column", "columns"}:

        # Prefer session graph context when present
        if graph_context and not force_neo4j:
            table_ids = list(graph_context.keys())
            filtered_tables = [
                t for t in table_ids if (not target_layer or str(t).upper().startswith(f"{target_layer}."))
            ]

            if target_kind in {"table", "tables"}:
                count_value = len(filtered_tables)
                scope = target_layer or "ALL"
                msg = f"{scope} table count: {count_value}"
                update_session_state(session_id, {"last_schema_lookup": {"scope": scope, "kind": "tables", "count": count_value}})
                return {"messages": [AIMessage(content=fmt({"scope": scope, "kind": "tables", "count": count_value}, msg))]}

            # columns
            col_count = 0
            for t in filtered_tables:
                schema = graph_context.get(t, {})
                col_count += len(schema.get("columns", []))
            scope = target_layer or "ALL"
            msg = f"{scope} column count: {col_count}"
            update_session_state(session_id, {"last_schema_lookup": {"scope": scope, "kind": "columns", "count": col_count}})
            return {"messages": [AIMessage(content=fmt({"scope": scope, "kind": "columns", "count": col_count}, msg))]}

        # Fallback to Neo4j when no session graph context exists
        if target_kind in {"table", "tables"}:
            query = """
            MATCH (t:Table)
            WITH t, toUpper(coalesce(t.layer, split(coalesce(t.id, t.name), ".")[0])) AS layer
            WHERE $layer = '' OR layer = $layer
            RETURN count(DISTINCT t) AS count
            """
            rows = run_cypher(query=query, layer=target_layer)
            count_value = rows[0]["count"] if rows else 0
            scope = target_layer or "ALL"
            update_session_state(session_id, {"last_schema_lookup": {"scope": scope, "kind": "tables", "count": count_value}})
            return {"messages": [AIMessage(content=fmt({"scope": scope, "kind": "tables", "count": count_value}, f"{scope} table count: {count_value}"))]}

        query = """
        MATCH (t:Table)-[:HAS_VERSION]->(v:TableVersion)-[:HAS_COLUMN]->(c)
        WHERE coalesce(v.active, true) = true AND (c:ColumnVersion OR c:Column)
        WITH t, c, toUpper(coalesce(t.layer, split(coalesce(t.id, t.name), ".")[0])) AS layer
        WHERE $layer = '' OR layer = $layer
        RETURN count(DISTINCT c) AS count
        """
        rows = run_cypher(query=query, layer=target_layer)
        count_value = rows[0]["count"] if rows else 0
        scope = target_layer or "ALL"
        update_session_state(session_id, {"last_schema_lookup": {"scope": scope, "kind": "columns", "count": count_value}})
        return {"messages": [AIMessage(content=fmt({"scope": scope, "kind": "columns", "count": count_value}, f"{scope} column count: {count_value}"))]}

    # ----------------------------------------
    # 1️⃣ ENTITY EXTRACTION
    # ----------------------------------------
    keyword_entity_match = re.search(
        r"(?:schema\s+(?:for|of)|column|field|attribute|table)\s+([a-zA-Z0-9_.]+)",
        user_input,
    )
    entity_name = keyword_entity_match.group(1) if keyword_entity_match else ""
    entity_name = entity_name.strip(".,:;!?")

    # Handle queries like "schema for column loan_amount" or "schema for table xyz"
    if entity_name in {"column", "field", "attribute", "table"}:
        followup_match = re.search(
            rf"(?:schema\s+(?:for|of)\s+{entity_name}\s+)([a-zA-Z0-9_.]+)",
            user_input,
        )
        if followup_match:
            entity_name = followup_match.group(1).strip(".,:;!?")

    explicit_table_intent = bool(re.search(r"\btable\b", user_input))
    explicit_column_intent = bool(re.search(r"\b(column|field|attribute)\b", user_input))
    schema_phrase_present = bool(re.search(r"\bschema\s+(for|of)\b", user_input))

    if schema_plan:
        planned_entity = (schema_plan.entity or "").strip()
        if planned_entity:
            entity_name = planned_entity.strip(".,:;!?")
        planned_op = str(schema_plan.operation).lower()
        if planned_op == "column":
            explicit_column_intent = True
            explicit_table_intent = False
        elif planned_op == "table":
            explicit_table_intent = True
        if str(schema_plan.source).lower() == "neo4j":
            force_neo4j = True

    logger.info("Schema entity parsed | session=%s entity=%s", session_id, entity_name or "<none>")

    # ----------------------------------------
    # 2️⃣ COLUMN-FIRST RESOLUTION
    # ----------------------------------------
    if entity_name and not (explicit_table_intent and not explicit_column_intent):
        col_key = entity_name.lower()

        if col_key in column_to_tables and not force_neo4j:
            matches = column_to_tables[col_key]
            logger.info(
                "Schema column exact match | session=%s column=%s matches=%d",
                session_id,
                entity_name,
                len(matches),
            )
            update_session_state(session_id, {"last_schema_lookup": matches})
            if len(matches) == 1:
                return {"messages": [AIMessage(content=fmt(matches[0]))]}
            return {
                "messages": [
                    AIMessage(content=fmt(matches, f"Column '{entity_name}' exists in multiple tables:\n{json.dumps(matches, indent=2)}"))
                ]
            }

        # Fuzzy match to a column
        col_match = process.extractOne(entity_name, all_columns, scorer=fuzz.WRatio)
        if col_match and col_match[1] > 80 and not force_neo4j:
            suggested_col = col_match[0]
            matches = column_to_tables.get(str(suggested_col).lower(), [])
            if matches:
                logger.info(
                    "Schema column fuzzy match | session=%s requested=%s suggested=%s score=%s",
                    session_id,
                    entity_name,
                    suggested_col,
                    col_match[1],
                )
                update_session_state(session_id, {"last_schema_lookup": matches})
                return {
                    "messages": [
                        AIMessage(content=fmt(matches, "Exact column not found. Closest column match."))
                    ]
                }

        # Column fallback to Neo4j (supports both Column and ColumnVersion graphs)
        query = """
        MATCH (t:Table)-[:HAS_VERSION]->(v:TableVersion)-[:HAS_COLUMN]->(c)
        WHERE coalesce(v.active, true) = true
          AND (c:ColumnVersion OR c:Column)
          AND toLower(coalesce(c.name, c.id)) CONTAINS toLower($col)
        RETURN
          coalesce(t.id, t.name) AS table,
          coalesce(c.name, c.id) AS column,
          coalesce(c.datatype, c.type, "UNKNOWN") AS type
        LIMIT 10
        """
        rows = run_cypher(query=query, col=entity_name)

        if rows:
            logger.info(
                "Schema column neo4j fallback hit | session=%s requested=%s rows=%d",
                session_id,
                entity_name,
                len(rows),
            )
            update_session_state(session_id, {"last_schema_lookup": rows})
            return {
                "messages": [
                    AIMessage(content=fmt(rows, f"Column not found in cache. Possible matches from Neo4j:\n{json.dumps(rows, indent=2)}"))
                ]
            }

        # If user asked explicitly for column/schema, don't incorrectly fall through to table-not-found.
        if explicit_column_intent or (schema_phrase_present and not explicit_table_intent):
            msg = f"❌ Column '{entity_name}' not found."
            return {"messages": [AIMessage(content=fmt(msg, msg))]}

    # ----------------------------------------
    # 3️⃣ TABLE QUERY
    # ----------------------------------------
    table_match = re.search(r"(?:schema\s+(?:for|of)|table)\s+([a-zA-Z0-9_.]+)", user_input)
    if table_match:
        table_name = table_match.group(1).strip(".,:;!?")
        if table_name in {"table", "column", "field", "attribute"} and entity_name:
            table_name = entity_name
        logger.info("Schema table query | session=%s table=%s", session_id, table_name)

        # Exact match (case-insensitive)
        exact_table = None
        for candidate in all_tables:
            if str(candidate).lower() == table_name.lower():
                exact_table = candidate
                break

        if exact_table and not force_neo4j:
            logger.info("Schema table exact match | session=%s table=%s", session_id, exact_table)
            update_session_state(session_id, {
                "last_schema_lookup": graph_context[exact_table]
            })
            return {
                "messages": [
                    AIMessage(content=fmt(graph_context[exact_table]))
                ]
            }

        # Fuzzy match to full dotted table names
        match = process.extractOne(table_name, all_tables, scorer=fuzz.WRatio)
        if match and match[1] > 80 and not force_neo4j:
            suggested_table = match[0]
            logger.info(
                "Schema table fuzzy match | session=%s requested=%s suggested=%s score=%s",
                session_id,
                table_name,
                suggested_table,
                match[1],
            )
            update_session_state(session_id, {
                "last_schema_lookup": graph_context[suggested_table]
            })
            return {
                "messages": [
                    AIMessage(content=fmt(graph_context[suggested_table], "Exact table not found. Closest match."))
                ]
            }

        # Fallback to Neo4j (supports TableVersion + ColumnVersion model)
        query = """
        MATCH (t:Table)
        WHERE toLower(coalesce(t.id, t.name)) CONTAINS toLower($table)
           OR toLower(coalesce(t.name, t.id)) CONTAINS toLower($table)
        OPTIONAL MATCH (t)-[:HAS_VERSION]->(v:TableVersion)-[:HAS_COLUMN]->(c)
        WHERE coalesce(v.active, true) = true
          AND (c:ColumnVersion OR c:Column)
        RETURN
          coalesce(t.id, t.name) as table,
          [col IN collect(DISTINCT {name: coalesce(c.name, c.id), datatype: coalesce(c.datatype, c.type, "UNKNOWN")})
            WHERE col.name IS NOT NULL] as columns
        LIMIT 5
        """
        rows = run_cypher(query=query, table=table_name)

        if rows:
            logger.info(
                "Schema table neo4j fallback hit | session=%s table=%s rows=%d",
                session_id,
                table_name,
                len(rows),
            )
            update_session_state(session_id, {"last_schema_lookup": rows})
            return {
                "messages": [
                    AIMessage(content=fmt(rows))
                ]
            }

        logger.warning("Schema table not found | session=%s table=%s", session_id, table_name)
        msg = f"❌ Table '{table_name}' not found."
        return {"messages": [AIMessage(content=fmt(msg, msg))]}

    # ----------------------------------------
    # 4️⃣ No match
    # ----------------------------------------
    return {
        "messages": [
            AIMessage(content=fmt("⚠️ Please specify a table or column name.", "⚠️ Please specify a table or column name."))
        ]
    }

# =========================================================
# 10️⃣ BUSINESS PIPELINE
# =========================================================

def business_pipeline(state: AgentState):
    session_id = state["session_id"]
    user_input = state.get("user_input", "")
    context_data = load_context(session_id) or {}
    business_context = context_data.get("business_spec", {}) or {}
    graph_context = context_data.get("graph_context", {}) or {}

    # 1) Build a focused business context slice from uploaded transformation docs
    query_lower = user_input.lower()
    tokens = re.findall(r"[a-zA-Z_][a-zA-Z0-9_\.]*", query_lower)
    stop_words = {
        "the", "and", "for", "from", "with", "what", "which", "show", "tell", "about",
        "table", "tables", "column", "columns", "schema", "count", "how", "many", "is",
        "are", "in", "of", "to", "on", "a", "an", "all", "please", "me",
    }
    keywords = [t for t in tokens if len(t) >= 3 and t not in stop_words]

    relevant_business = {}
    if business_context:
        for table_name, spec in business_context.items():
            table_blob = json.dumps(spec).lower()
            score = 0
            for kw in keywords:
                if kw in table_name.lower():
                    score += 3
                if kw in table_blob:
                    score += 1
            if score > 0:
                relevant_business[table_name] = (score, spec)

    if relevant_business:
        relevant_business = {
            k: v for k, (_, v) in sorted(relevant_business.items(), key=lambda item: item[1][0], reverse=True)[:10]
        }
    else:
        # Keep response bounded when no explicit match is found
        relevant_business = {k: business_context[k] for k in list(business_context.keys())[:8]}

    # 2) Optional Neo4j hints for broader questions
    neo4j_tables = []
    neo4j_columns = []
    try:
        if keywords:
            table_rows = run_cypher(
                query="""
                MATCH (t:Table)
                WHERE any(k IN $keywords WHERE toLower(coalesce(t.id, t.name)) CONTAINS k)
                   OR any(k IN $keywords WHERE toLower(coalesce(t.name, t.id)) CONTAINS k)
                OPTIONAL MATCH (t)-[:HAS_VERSION]->(v:TableVersion)-[:HAS_COLUMN]->(c)
                WHERE coalesce(v.active, true) = true AND (c:ColumnVersion OR c:Column)
                RETURN
                  coalesce(t.id, t.name) AS table,
                  count(DISTINCT c) AS column_count
                ORDER BY column_count DESC
                LIMIT 20
                """,
                keywords=keywords,
            )
            neo4j_tables = table_rows

            column_rows = run_cypher(
                query="""
                MATCH (t:Table)-[:HAS_VERSION]->(v:TableVersion)-[:HAS_COLUMN]->(c)
                WHERE coalesce(v.active, true) = true
                  AND (c:ColumnVersion OR c:Column)
                  AND any(k IN $keywords WHERE toLower(coalesce(c.name, c.id)) CONTAINS k)
                RETURN
                  coalesce(t.id, t.name) AS table,
                  coalesce(c.name, c.id) AS column,
                  coalesce(c.datatype, c.type, "UNKNOWN") AS type
                LIMIT 30
                """,
                keywords=keywords,
            )
            neo4j_columns = column_rows
    except Exception as exc:
        logger.warning("Business Neo4j hint lookup failed | session=%s error=%s", session_id, exc)

    # 3) LLM answer generation using both sources
    prompt = f"""
User question:
{user_input}

Transformation business spec context (session):
{json.dumps(relevant_business, indent=2)}

Session graph context summary:
tables={len(graph_context)}

Neo4j table hints:
{json.dumps(neo4j_tables, indent=2)}

Neo4j column hints:
{json.dumps(neo4j_columns, indent=2)}

Rules:
1. Answer directly and concisely.
2. Prefer transformation business spec when it contains the answer.
3. Use Neo4j hints when user asks outside transformation scope.
4. Do not invent facts. If data is missing, say what is missing.
5. For counts, return explicit numeric values.
"""

    try:
        response = llm.invoke([
            SystemMessage(content="You are a data modeling assistant answering business/lineage questions from provided context only."),
            HumanMessage(content=prompt),
        ])
        answer = str(getattr(response, "content", "")).strip()
        if not answer:
            answer = "I could not derive an answer from available transformation or Neo4j context."
    except Exception as exc:
        logger.warning("Business LLM response failed | session=%s error=%s", session_id, exc)
        if relevant_business:
            answer = json.dumps(relevant_business, indent=2)
        elif neo4j_tables or neo4j_columns:
            answer = json.dumps({"tables": neo4j_tables, "columns": neo4j_columns}, indent=2)
        else:
            answer = "No business context found in session and no matching Neo4j hints were found."

    update_session_state(session_id, {
        "last_business_response": answer
    })
    return {"messages": [AIMessage(content=answer)]}

# =========================================================
# 11️⃣ CHAT PIPELINE
# =========================================================

def chat_pipeline(state: AgentState):
    session_id = state["session_id"]
    context_data = load_context(session_id) or {}
    graph_context = context_data.get("graph_context", {})
    compiler_state = context_data.get("compiler_state", {})

    system_message = SystemMessage(
        content=SYSTEM_PROMPT +
                "\n\nGRAPH CONTEXT:\n" + json.dumps(graph_context, indent=2) +
                "\n\nCOMPILER MEMORY:\n" + json.dumps(compiler_state, indent=2)
    )

    filtered_messages = [msg for msg in state["messages"] if not isinstance(msg, SystemMessage)]
    messages = [system_message] + filtered_messages
    response_text = _invoke_llm_text(messages, stream_callback=state.get("stream_callback"))
    return {"messages": [AIMessage(content=response_text)]}

# =========================================================
# 12️⃣ WORKFLOW
# =========================================================

workflow = StateGraph(AgentState)

# Nodes
workflow.add_node("detect_intent", detect_intent)
workflow.add_node("pruning", prune_schema)
workflow.add_node("sql_generate", sql_pipeline)
workflow.add_node("sql_modify", sql_pipeline) # Shared pipeline logic
workflow.add_node("schema", schema_pipeline)
workflow.add_node("business", business_pipeline)
workflow.add_node("chat", chat_pipeline)
workflow.add_node("orchestrator", orchestrator)

workflow.add_conditional_edges("orchestrator", lambda state: state["next_step"])

# Entry Point
workflow.set_entry_point("detect_intent")

# Logic: After pruning, route to the specific SQL worker
workflow.add_conditional_edges("pruning", lambda state: state["next_step"])
workflow.add_edge("detect_intent", "orchestrator")
# Endings
workflow.add_edge("sql_generate", END)
workflow.add_edge("sql_modify", END)
workflow.add_edge("schema", END)
workflow.add_edge("business", END)
workflow.add_edge("chat", END)

app = workflow.compile()

# =========================================================
# 13️⃣ RUNNER
# =========================================================

def _build_initial_state(
    session_id: str,
    user_input: str,
    stream_callback: Optional[Callable[[str], None]] = None,
) -> Dict[str, Any]:
    history = load_messages(session_id)
    initial_messages = [SystemMessage(content=SYSTEM_PROMPT)]
    for role, content in history:
        if role == "user":
            initial_messages.append(HumanMessage(content=str(content)))
        elif role == "assistant":
            initial_messages.append(AIMessage(content=str(content)))
    initial_messages.append(HumanMessage(content=user_input))
    return {
        "messages": initial_messages,
        "user_input": user_input,
        "session_id": session_id,
        "intent": "",
        "next_step": "",
        "last_generated_sql": "",
        "last_validation_errors": [],
        "business_context": {},
        "last_schema_lookup": {},
        "orchestration_status": {},
        "stream_callback": stream_callback,
    }


async def run_agent_stream(session_id: str, user_input: str, stream_mode: Any = "messages"):
    initial_state = _build_initial_state(session_id, user_input, stream_callback=None)
    async for chunk in app.astream(initial_state, stream_mode=stream_mode):
        yield chunk

def run_agent(session_id: str, user_input: str, stream_callback: Optional[Callable[[str], None]] = None):
    try:
        logger.info("Run agent start | session=%s input=%s", session_id, user_input)
        history = load_messages(session_id)
        context_data = load_context(session_id) or {}
        graph_context = context_data.get("graph_context", {})
        logger.info(
            "Run agent context | session=%s history_msgs=%d graph_tables=%d",
            session_id,
            len(history),
            len(graph_context),
        )

        # ✅ CORRECT: Just send the System Prompt. Let the nodes inject context.
        final_state = app.invoke(_build_initial_state(session_id, user_input, stream_callback=stream_callback))


        last_msg = final_state["messages"][-1]
        chat_reply = last_msg.content

        save_message(session_id, "user", user_input)
        save_message(session_id, "assistant", chat_reply)
        logger.info("Run agent complete | session=%s reply_preview=%s", session_id, str(chat_reply)[:200])

        return {"status": "completed", "chat_reply": chat_reply}

    except Exception as e:
        logger.exception("Run agent failed | session=%s error=%s", session_id, e)
        return {"status": "error", "chat_reply": str(e)}
