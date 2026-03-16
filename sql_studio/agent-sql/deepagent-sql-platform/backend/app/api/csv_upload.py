import os
import shutil
from pathlib import Path
from uuid import uuid4

from fastapi import APIRouter, File, HTTPException, UploadFile
from app.api.chat import _normalize_metadata_df, _tables_from_session_csv, fetch_table_schemas_batch
from app.memory.session_context import save_context, load_context
import logging
from app.config import SESSION_UPLOADS_DIR
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
import pandas as pd
from pandas.errors import ParserError
router = APIRouter()


def _session_dir(session_id: str) -> Path:
    root = Path(SESSION_UPLOADS_DIR)
    root.mkdir(parents=True, exist_ok=True)
    session_path = root / session_id
    session_path.mkdir(parents=True, exist_ok=True)
    return session_path


def _read_csv_with_fallback(file_path: Path):
    try:
        return pd.read_csv(file_path)
    except ParserError as err:
        logger.warning("CSV parse failed with C engine for %s: %s", file_path.name, err)
        return pd.read_csv(file_path, engine="python", on_bad_lines="skip")

@router.post("/upload")
async def upload_csv_files(files: list[UploadFile] = File(...)):
    session_id = str(uuid4())
    session_path = _session_dir(session_id)
    saved_files = []

    for file in files:
        if not file.filename.endswith(".csv"):
            raise HTTPException(status_code=400, detail="Only CSV files allowed.")
        destination = session_path / file.filename
        with destination.open("wb") as buffer:
            shutil.copyfileobj(file.file, buffer)
        saved_files.append(file.filename)

    # 1. Detect source and target tables from uploaded CSV(s)
    tables = _tables_from_session_csv(session_id)

    # 2. Batch-fetch schemas for all detected tables
    raw_schemas = await fetch_table_schemas_batch(list(tables))
    graph_context = {}
    for table_name, schema_data in raw_schemas.items():
        if schema_data:
            # Normalize to match chat.py structure
            cols = []
            for c in schema_data.get("columns", []):
                cols.append({
                    "name": c.get("name") or c.get("column_name"),
                    "datatype": c.get("type") or c.get("datatype")
                })
            graph_context[table_name] = {
                "columns": cols,
                "relationships": schema_data.get("relationships", [])
            }
    
    # 3. Build multi-source business spec from uploaded CSV(s)
    business_spec = {}
    warnings = []
    required_business_cols = {
        "layer",
        "schema",
        "table_name",
        "column_name",
        "transformation_logic",
    }

    for file_path in session_path.glob("*.csv"):
        try:
            df = _read_csv_with_fallback(file_path)
        except Exception as exc:
            warnings.append(f"{file_path.name}: unable to parse CSV ({exc})")
            continue

        df = _normalize_metadata_df(df)

        missing = sorted(required_business_cols - set(df.columns))
        if missing:
            warnings.append(
                f"{file_path.name}: missing columns {', '.join(missing)}; skipped for business_spec"
            )
            continue

        # Group by target table identity
        grouped = df.groupby(["layer", "schema", "table_name"], dropna=True)
        for (layer, schema, table), group_df in grouped:
            full_name = f"{str(layer).strip()}.{str(schema).strip()}.{str(table).strip()}"

            # Extract all unique sources from all rows for this target table
            all_sources = set()
            if "source_tables" in group_df.columns:
                for val in group_df["source_tables"].dropna():
                    all_sources.update([s.strip() for s in str(val).split(",") if s.strip()])

            business_spec[full_name] = {
                "sources": list(all_sources),
                "type": group_df["table_type"].iloc[0] if "table_type" in group_df.columns else "TABLE",
                "columns": {
                    str(row["column_name"]).strip(): str(row["transformation_logic"]).strip()
                    for _, row in group_df.iterrows()
                    if pd.notna(row["column_name"])
                },
            }

    # Merge with existing context so we don't wipe compiler/orchestration state
    existing_context = load_context(session_id) or {}
    existing_context["source_tables"] = sorted(list(tables))
    existing_context["graph_context"] = graph_context
    existing_context["business_spec"] = business_spec
    save_context(session_id, existing_context)

    logger.info(
        "Upload context saved for %s | tables=%d graph=%d spec=%d warnings=%d",
        session_id,
        len(tables),
        len(graph_context),
        len(business_spec),
        len(warnings),
    )

    return {
        "status": "uploaded",
        "session_id": session_id,
        "tables_detected": list(tables),
        "context_file": os.path.join(str(session_path), "context.json"),
        "graph_context_count": len(graph_context),
        "business_spec_count": len(business_spec),
        "warnings": warnings,
    }

@router.get("/session/{session_id}")
def get_session_files(session_id: str):
    session_path = _session_dir(session_id)
    files = [str(p) for p in session_path.glob("*.csv") if p.is_file()]
    return {
        "session_id": session_id,
        "workspace": str(session_path),
        "files": files,
    }
