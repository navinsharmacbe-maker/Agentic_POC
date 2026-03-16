from typing import List, Optional

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from app.neo4j_driver import get_session

router = APIRouter()


class ColumnSchema(BaseModel):
    name: str
    datatype: str
    is_pk: bool = False
    is_fk: bool = False


class TableSchema(BaseModel):
    layer: str
    schema: str
    name: str
    type: str
    version: int
    description: Optional[str] = None
    columns: List[ColumnSchema]


class VersionLineage(BaseModel):
    source_version: str
    target_version: str


class FKLineage(BaseModel):
    src: Optional[str] = None
    tgt: Optional[str] = None
    source_version: Optional[str] = None
    target_version: Optional[str] = None


def _normalize_fk_lineage(fk: FKLineage):
    src = (fk.src or fk.source_version or "").strip()
    tgt = (fk.tgt or fk.target_version or "").strip()
    if not src or not tgt:
        raise HTTPException(
            status_code=422,
            detail="FK payload requires src/tgt or source_version/target_version",
        )
    return src, tgt


@router.delete("/admin/drop-all")
def drop_all_graph_data():
    with get_session() as session:
        result = session.run(
            """
            MATCH (n)
            WITH count(n) AS node_count
            CALL {
                MATCH (n)
                DETACH DELETE n
                RETURN count(*) AS deleted_count
            }
            RETURN node_count, deleted_count
            """
        )
        row = result.single()

    return {
        "status": "ok",
        "message": "Neo4j graph cleared",
        "nodes_before": row["node_count"] if row else 0,
    }


@router.post("/table")
def upsert_table(table: TableSchema):
    table_id = f"{table.layer}.{table.schema}.{table.name}"
    version_id = f"{table_id}.v{table.version}"

    with get_session() as session:
        session.run(
            """
            MATCH (t:Table {id:$tid})-[:HAS_VERSION]->(ov:TableVersion {active:true})
            SET ov.active = false
            """,
            {"tid": table_id},
        )

        for col in table.columns:
            col_id = f"{version_id}.{col.name}"

            session.run(
                """
                MERGE (l:Layer {id:$layer})
                MERGE (t:Table {id:$tid})
                MERGE (v:TableVersion {
                    id:$vid,
                    version:$ver,
                    type:$type,
                    description:$desc,
                    active:true
                })
                MERGE (c:ColumnVersion {id:$cid})
                SET c.name=$cname,
                    c.datatype=$dtype,
                    c.is_pk=$is_pk,
                    c.is_fk=$is_fk,
                    c.active=true
                MERGE (l)-[:HAS_TABLE]->(t)
                MERGE (t)-[:HAS_VERSION]->(v)
                MERGE (v)-[:HAS_COLUMN]->(c)
                """,
                {
                    "layer": table.layer,
                    "tid": table_id,
                    "vid": version_id,
                    "ver": table.version,
                    "type": table.type,
                    "desc": table.description or "",
                    "cid": col_id,
                    "cname": col.name,
                    "dtype": col.datatype,
                    "is_pk": col.is_pk,
                    "is_fk": col.is_fk,
                },
            )

    return {"status": "OK", "version": version_id}


@router.post("/table/bulk")
def upsert_tables_bulk(tables: List[TableSchema]):
    results = []
    for table in tables:
        created = upsert_table(table)
        results.append({"table": table.name, "status": "created", "version": created["version"]})
    return {"count": len(results), "results": results}


@router.get("/tables")
def list_tables():
    with get_session() as session:
        result = session.run(
            """
            MATCH (l:Layer)-[:HAS_TABLE]->(t:Table)
            RETURN l.id AS layer, t.id AS table
            """
        )
        return [r.data() for r in result]


@router.get("/table/{table_id}")
def get_table(table_id: str):
    with get_session() as session:
        result = session.run(
            """
            MATCH (t:Table {id:$id})-[:HAS_VERSION]->(v:TableVersion)
            RETURN t.id AS table, v.id AS version, v.active AS active
            """,
            {"id": table_id},
        )
        return [r.data() for r in result]


@router.get("/table/version/{version_id}")
def get_table_version(version_id: str):
    with get_session() as session:
        result = session.run(
            """
            MATCH (v:TableVersion {id:$vid})-[:HAS_COLUMN]->(c)
            RETURN v.id AS version, c.id AS column, c.datatype AS datatype
            """,
            {"vid": version_id},
        )
        return [r.data() for r in result]


@router.put("/column/{column_version_id}")
def update_column(column_version_id: str, datatype: str):
    with get_session() as session:
        res = session.run(
            """
            MATCH (c:ColumnVersion {id:$id, active:true})
            SET c.datatype=$datatype
            RETURN c
            """,
            {"id": column_version_id, "datatype": datatype},
        )

        if not res.single():
            raise HTTPException(404, "Column version not found")

    return {"status": "Column updated"}


@router.delete("/column/{column_version_id}")
def deactivate_column(column_version_id: str):
    with get_session() as session:
        session.run(
            """
            MATCH (c:ColumnVersion {id:$id})
            SET c.active=false
            """,
            {"id": column_version_id},
        )
    return {"status": "Column deactivated"}


@router.delete("/table/version/{version_id}")
def deactivate_table_version(version_id: str):
    with get_session() as session:
        session.run(
            """
            MATCH (v:TableVersion {id:$id})
            SET v.active=false
            """,
            {"id": version_id},
        )
    return {"status": "Table version deactivated"}


@router.delete("/table/{table_id}")
def delete_table(table_id: str):
    with get_session() as session:
        session.run(
            """
            MATCH (t:Table {id:$id})
            DETACH DELETE t
            """,
            {"id": table_id},
        )
    return {"status": "Table deleted"}


@router.post("/lineage/table")
def add_table_lineage(lineage: VersionLineage):
    with get_session() as session:
        session.run(
            """
            MATCH (s:TableVersion {id:$src})
            MATCH (t:TableVersion {id:$tgt})
            MERGE (s)-[:DERIVED_FROM]->(t)
            """,
            {"src": lineage.source_version, "tgt": lineage.target_version},
        )
    return {"status": "Table lineage added"}


@router.post("/lineage/table/bulk")
def add_table_lineage_bulk(lineages: List[VersionLineage]):
    for lineage in lineages:
        add_table_lineage(lineage)
    return {"count": len(lineages), "status": "Table lineage bulk added"}


@router.post("/lineage/column")
def add_column_lineage(lineage: VersionLineage):
    with get_session() as session:
        session.run(
            """
            MATCH (s:ColumnVersion {id:$src})
            MATCH (t:ColumnVersion {id:$tgt})
            MERGE (s)-[:MAPS_TO]->(t)
            """,
            {"src": lineage.source_version, "tgt": lineage.target_version},
        )
    return {"status": "Column lineage added"}


@router.post("/lineage/column/bulk")
def add_column_lineage_bulk(lineages: List[VersionLineage]):
    for lineage in lineages:
        add_column_lineage(lineage)
    return {"count": len(lineages), "status": "Column lineage bulk added"}


@router.delete("/lineage/table")
def delete_table_lineage(lineage: VersionLineage):
    with get_session() as session:
        session.run(
            """
            MATCH (s:TableVersion {id:$src})-[r:DERIVED_FROM]->(t:TableVersion {id:$tgt})
            DELETE r
            """,
            {"src": lineage.source_version, "tgt": lineage.target_version},
        )
    return {"status": "Table lineage removed"}


@router.delete("/lineage/column")
def delete_column_lineage(lineage: VersionLineage):
    with get_session() as session:
        session.run(
            """
            MATCH (s:ColumnVersion {id:$src})-[r:MAPS_TO]->(t:ColumnVersion {id:$tgt})
            DELETE r
            """,
            {"src": lineage.source_version, "tgt": lineage.target_version},
        )
    return {"status": "Column lineage removed"}


@router.post("/lineage/fk")
def add_fk(fk: FKLineage):
    src, tgt = _normalize_fk_lineage(fk)

    with get_session() as session:
        result = session.run(
            """
            MATCH (s:ColumnVersion {id:$src})
            MATCH (t:ColumnVersion {id:$tgt})
            MERGE (s)-[:FK_TO]->(t)
            RETURN count(s) AS s_count, count(t) AS t_count
            """,
            {"src": src, "tgt": tgt},
        )
        row = result.single()
        if not row or row["s_count"] == 0 or row["t_count"] == 0:
            raise HTTPException(
                status_code=404,
                detail=f"ColumnVersion not found for src='{src}' or tgt='{tgt}'",
            )
    return {"status": "FK linked"}


@router.post("/lineage/fk/bulk")
def add_fk_bulk(lineages: List[FKLineage]):
    results = []
    success_count = 0

    for lineage in lineages:
        try:
            src, tgt = _normalize_fk_lineage(lineage)
            with get_session() as session:
                result = session.run(
                    """
                    MATCH (s:ColumnVersion {id:$src})
                    MATCH (t:ColumnVersion {id:$tgt})
                    MERGE (s)-[:FK_TO]->(t)
                    RETURN count(s) AS s_count, count(t) AS t_count
                    """,
                    {"src": src, "tgt": tgt},
                )
                row = result.single()

            if not row or row["s_count"] == 0 or row["t_count"] == 0:
                results.append(
                    {
                        "src": src,
                        "tgt": tgt,
                        "status": "failed",
                        "reason": "source/target ColumnVersion not found",
                    }
                )
                continue

            success_count += 1
            results.append({"src": src, "tgt": tgt, "status": "linked"})
        except HTTPException as exc:
            results.append(
                {
                    "src": lineage.src or lineage.source_version,
                    "tgt": lineage.tgt or lineage.target_version,
                    "status": "failed",
                    "reason": str(exc.detail),
                }
            )

    return {
        "count": len(lineages),
        "success_count": success_count,
        "failed_count": len(lineages) - success_count,
        "results": results,
    }


@router.get("/graph/metrics")
def graph_metrics():
    with get_session() as session:
        result = session.run(
            """
            MATCH (l:Layer)
            OPTIONAL MATCH (l)-[:HAS_TABLE]->(t:Table)
            OPTIONAL MATCH (t)-[:HAS_VERSION]->(v:TableVersion)
            WHERE coalesce(v.active, true) = true
            OPTIONAL MATCH (v)-[:HAS_COLUMN]->(c:ColumnVersion)
            WHERE coalesce(c.active, true) = true
            RETURN
                l.id AS layer,
                count(DISTINCT t) AS tables,
                count(DISTINCT v) AS versions,
                count(DISTINCT c) AS columns
            """
        )

        layers = [r.data() for r in result]
        summary = {
            "layers": len(layers),
            "tables": sum(layer["tables"] for layer in layers),
            "versions": sum(layer["versions"] for layer in layers),
            "columns": sum(layer["columns"] for layer in layers),
        }

    return {"layers": layers, "summary": summary}


@router.get("/graph/visual")
def graph_visual():
    nodes = {}
    edges = []
    with get_session() as session:
        result = session.run(
            """
            MATCH (n)
            OPTIONAL MATCH (n)-[r]->(m)
            RETURN
                elementId(n) AS sid,
                labels(n)[0] AS slabel,
                coalesce(n.name, n.id) AS sname,
                elementId(m) AS tid,
                labels(m)[0] AS tlabel,
                coalesce(m.name, m.id) AS tname,
                type(r) AS rel
            """
        )

        for row in result:
            nodes[row["sid"]] = {
                "id": row["sid"],
                "label": row["sname"],
                "group": row["slabel"],
            }

            if row["tid"] is not None:
                nodes[row["tid"]] = {
                    "id": row["tid"],
                    "label": row["tname"],
                    "group": row["tlabel"],
                }
                edges.append(
                    {
                        "from": row["sid"],
                        "to": row["tid"],
                        "label": row["rel"],
                        "arrows": "to",
                    }
                )

    return {"nodes": list(nodes.values()), "edges": edges}


@router.get("/graph/canvas")
def canvas_graph():
    with get_session() as session:
        node_result = session.run(
            """
            MATCH (t:Table)-[:HAS_VERSION]->(v:TableVersion)
            WHERE coalesce(v.active, true) = true
            OPTIONAL MATCH (v)-[:HAS_COLUMN]->(c:ColumnVersion)
            WHERE coalesce(c.active, true) = true
            WITH t, v, count(DISTINCT c) AS column_count, split(t.id, ".") AS parts
            RETURN
                t.id AS id,
                coalesce(t.name, CASE WHEN size(parts) > 2 THEN parts[2] ELSE t.id END) AS label,
                toUpper(coalesce(t.layer, CASE WHEN size(parts) > 0 THEN parts[0] ELSE "UNKNOWN" END)) AS layer
            ORDER BY layer, label
            """
        )
        nodes = [row.data() for row in node_result]

        edge_result = session.run(
            """
            MATCH (src_v:TableVersion)-[:DERIVED_FROM]->(tgt_v:TableVersion)
            WHERE coalesce(src_v.active, true) = true AND coalesce(tgt_v.active, true) = true
            MATCH (src_t:Table)-[:HAS_VERSION]->(src_v)
            MATCH (tgt_t:Table)-[:HAS_VERSION]->(tgt_v)
            RETURN DISTINCT src_t.id AS from_id, tgt_t.id AS to_id
            """
        )
        edges = [{"from": row["from_id"], "to": row["to_id"]} for row in edge_result]

    return {
        "layers": sorted({node["layer"] for node in nodes}),
        "nodes": nodes,
        "edges": edges,
    }


@router.get("/canvas")
def get_lineage_canvas():
    with get_session() as session:
        table_result = session.run(
            """
            MATCH (t:Table)-[:HAS_VERSION]->(v:TableVersion)
            WHERE coalesce(v.active, true) = true
            OPTIONAL MATCH (v)-[:HAS_COLUMN]->(c:ColumnVersion)
            WHERE coalesce(c.active, true) = true
            WITH t, v, count(DISTINCT c) AS column_count, split(t.id, ".") AS parts
            RETURN
                t.id AS id,
                coalesce(t.name, CASE WHEN size(parts) > 2 THEN parts[2] ELSE t.id END) AS name,
                toUpper(coalesce(t.layer, CASE WHEN size(parts) > 0 THEN parts[0] ELSE "UNKNOWN" END)) AS layer,
                coalesce(t.schema, CASE WHEN size(parts) > 1 THEN parts[1] ELSE "-" END) AS schema,
                column_count AS columns,
                toInteger(coalesce(v.rows, v.row_count, t.rows, t.row_count, 0)) AS rows
            ORDER BY layer, name
            """
        )
        tables = [row.data() for row in table_result]

        table_ids = {table["id"] for table in tables}

        edge_result = session.run(
            """
            MATCH (src_v:TableVersion)-[:DERIVED_FROM]->(tgt_v:TableVersion)
            WHERE coalesce(src_v.active, true) = true AND coalesce(tgt_v.active, true) = true
            MATCH (src_t:Table)-[:HAS_VERSION]->(src_v)
            MATCH (tgt_t:Table)-[:HAS_VERSION]->(tgt_v)
            RETURN DISTINCT src_t.id AS from_id, tgt_t.id AS to_id
            """
        )
        edges = [
            [row["from_id"], row["to_id"]]
            for row in edge_result
            if row["from_id"] in table_ids and row["to_id"] in table_ids
        ]

    return {"tables": tables, "edges": edges}


@router.get("/columns")
def get_column_lineage():
    with get_session() as session:
        column_result = session.run(
            """
            MATCH (t:Table)-[:HAS_VERSION]->(v:TableVersion)-[:HAS_COLUMN]->(c:ColumnVersion)
            WHERE coalesce(v.active, true) = true AND coalesce(c.active, true) = true
            WITH t, c, split(t.id, ".") AS table_parts, split(c.id, ".") AS col_parts
            RETURN
                c.id AS id,
                coalesce(c.name, CASE WHEN size(col_parts) > 4 THEN col_parts[4] ELSE c.id END) AS column,
                coalesce(t.name, CASE WHEN size(table_parts) > 2 THEN table_parts[2] ELSE t.id END) AS table,
                toUpper(coalesce(t.layer, CASE WHEN size(table_parts) > 0 THEN table_parts[0] ELSE "UNKNOWN" END)) AS layer,
                coalesce(c.datatype, c.type, "UNKNOWN") AS type
            ORDER BY layer, table, column
            """
        )
        columns = [row.data() for row in column_result]
        column_ids = {column["id"] for column in columns}

        edge_result = session.run(
            """
            MATCH (src:ColumnVersion)-[:MAPS_TO]->(tgt:ColumnVersion)
            WHERE coalesce(src.active, true) = true AND coalesce(tgt.active, true) = true
            RETURN DISTINCT src.id AS from_id, tgt.id AS to_id
            """
        )
        edges = [
            [row["from_id"], row["to_id"]]
            for row in edge_result
            if row["from_id"] in column_ids and row["to_id"] in column_ids
        ]

    return {"columns": columns, "edges": edges}
