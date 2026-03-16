import logging
from fastapi import APIRouter, HTTPException, Query
from app.neo4j_driver import get_session

router = APIRouter()
logger = logging.getLogger(__name__)

DEFAULT_LAYERS = ["ODP", "FDP", "CDP"]


def _normalize_layer(value: str | None) -> str:
    return (value or "UNKNOWN").upper()


@router.get("/graph/full")
def lineage_full_graph(
    row_limit: int = Query(default=50000, ge=100, le=300000),
):
    """
    Full lineage graph (heavy), following the legacy approach:
    MATCH all nodes and all relationships.
    """
    nodes: dict[str, dict] = {}
    edges: list[dict] = []

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
            LIMIT $row_limit
            """,
            row_limit=row_limit,
        )
        for row in result:
            sid = row["sid"]
            if sid and sid not in nodes:
                slabel = row["slabel"] or "Unknown"
                nodes[sid] = {
                    "id": sid,
                    "label": row["sname"] or sid,
                    "group": slabel,
                    "type": str(slabel).lower(),
                }

            tid = row["tid"]
            if tid:
                if tid not in nodes:
                    tlabel = row["tlabel"] or "Unknown"
                    nodes[tid] = {
                        "id": tid,
                        "label": row["tname"] or tid,
                        "group": tlabel,
                        "type": str(tlabel).lower(),
                    }
                edges.append(
                    {
                        "from": sid,
                        "to": tid,
                        "label": row["rel"] or "",
                        "type": row["rel"] or "rel",
                        "arrows": "to",
                    }
                )

    return {
        "nodes": list(nodes.values()),
        "edges": edges,
        "meta": {
            "mode": "full",
            "node_count": len(nodes.values()),
            "edge_count": len(edges),
            "legacy_heavy_query": True,
        },
    }


@router.get("/graph/roots")
def lineage_roots():
    """
    Lightweight root payload for initial graph render.
    Returns only ODP/FDP/CDP nodes with counts and layer-level edges.
    """
    with get_session() as session:
        layer_counts_rows = session.run(
            """
            MATCH (t:Table)
            WITH split(t.id, ".") AS parts, t
            WITH toUpper(coalesce(t.layer, CASE WHEN size(parts) > 0 THEN parts[0] ELSE "UNKNOWN" END)) AS layer
            RETURN layer, count(*) AS table_count
            """
        )
        layer_counts = {row["layer"]: row["table_count"] for row in layer_counts_rows}

        layer_edge_rows = session.run(
            """
            MATCH (src_v:TableVersion)-[:DERIVED_FROM]->(tgt_v:TableVersion)
            WHERE coalesce(src_v.active, true) = true AND coalesce(tgt_v.active, true) = true
            MATCH (src_t:Table)-[:HAS_VERSION]->(src_v)
            MATCH (tgt_t:Table)-[:HAS_VERSION]->(tgt_v)
            WITH
                toUpper(coalesce(src_t.layer, split(src_t.id, ".")[0])) AS src_layer,
                toUpper(coalesce(tgt_t.layer, split(tgt_t.id, ".")[0])) AS tgt_layer
            RETURN src_layer, tgt_layer, count(*) AS lineage_count
            """
        )
        layer_edges = []
        for row in layer_edge_rows:
            src = _normalize_layer(row["src_layer"])
            tgt = _normalize_layer(row["tgt_layer"])
            if src in DEFAULT_LAYERS and tgt in DEFAULT_LAYERS and src != tgt:
                layer_edges.append(
                    {"from": src, "to": tgt, "weight": row["lineage_count"], "type": "layer_lineage"}
                )

    nodes = [
        {
            "id": layer,
            "label": layer,
            "type": "layer",
            "table_count": int(layer_counts.get(layer, 0)),
        }
        for layer in DEFAULT_LAYERS
    ]

    # Fallback if layer lineage edges don't exist in graph yet.
    if not layer_edges:
        if layer_counts.get("ODP", 0) > 0 and layer_counts.get("FDP", 0) > 0:
            layer_edges.append({"from": "ODP", "to": "FDP", "weight": 0, "type": "layer_hint"})
        if layer_counts.get("FDP", 0) > 0 and layer_counts.get("CDP", 0) > 0:
            layer_edges.append({"from": "FDP", "to": "CDP", "weight": 0, "type": "layer_hint"})

    return {
        "nodes": nodes,
        "edges": layer_edges,
        "meta": {"mode": "roots", "layers": DEFAULT_LAYERS},
    }


@router.get("/graph/layer/{layer_id}")
def lineage_expand_layer(
    layer_id: str,
    include_neighbors: bool = Query(default=True),
    table_limit: int = Query(default=300, ge=10, le=2000),
    edge_limit: int = Query(default=1000, ge=10, le=10000),
):
    """
    Expand a layer node (ODP/FDP/CDP) into table nodes.
    Optionally includes immediate neighbor table stubs for progressive expansion.
    """
    layer = _normalize_layer(layer_id)
    if layer not in DEFAULT_LAYERS:
        raise HTTPException(status_code=400, detail=f"Unsupported layer '{layer_id}'. Use ODP, FDP, or CDP.")

    nodes: dict[str, dict] = {}
    edges: list[dict] = []

    with get_session() as session:
        table_rows = session.run(
            """
            MATCH (t:Table)-[:HAS_VERSION]->(v:TableVersion)
            WHERE coalesce(v.active, true) = true
            WITH DISTINCT t, split(t.id, ".") AS parts
            WITH
                t,
                toUpper(coalesce(t.layer, CASE WHEN size(parts) > 0 THEN parts[0] ELSE "UNKNOWN" END)) AS layer_norm,
                coalesce(t.name, CASE WHEN size(parts) > 2 THEN parts[2] ELSE t.id END) AS label
            WHERE layer_norm = $layer
            RETURN t.id AS id, label, layer_norm AS layer
            ORDER BY label
            LIMIT $table_limit
            """,
            layer=layer,
            table_limit=table_limit,
        )

        for row in table_rows:
            nodes[row["id"]] = {
                "id": row["id"],
                "label": row["label"],
                "layer": row["layer"],
                "type": "table",
                "stub": False,
            }
            edges.append({"from": layer, "to": row["id"], "type": "contains"})

        if include_neighbors:
            edge_rows = session.run(
                """
                MATCH (src_v:TableVersion)-[:DERIVED_FROM]->(tgt_v:TableVersion)
                WHERE coalesce(src_v.active, true) = true AND coalesce(tgt_v.active, true) = true
                MATCH (src_t:Table)-[:HAS_VERSION]->(src_v)
                MATCH (tgt_t:Table)-[:HAS_VERSION]->(tgt_v)
                WITH DISTINCT src_t, tgt_t,
                    toUpper(coalesce(src_t.layer, split(src_t.id, ".")[0])) AS src_layer,
                    toUpper(coalesce(tgt_t.layer, split(tgt_t.id, ".")[0])) AS tgt_layer
                WHERE src_layer = $layer OR tgt_layer = $layer
                RETURN
                    src_t.id AS from_id,
                    coalesce(src_t.name, src_t.id) AS from_label,
                    src_layer AS from_layer,
                    tgt_t.id AS to_id,
                    coalesce(tgt_t.name, tgt_t.id) AS to_label,
                    tgt_layer AS to_layer
                LIMIT $edge_limit
                """,
                layer=layer,
                edge_limit=edge_limit,
            )

            for row in edge_rows:
                for node_id, node_label, node_layer in [
                    (row["from_id"], row["from_label"], row["from_layer"]),
                    (row["to_id"], row["to_label"], row["to_layer"]),
                ]:
                    if node_id not in nodes:
                        nodes[node_id] = {
                            "id": node_id,
                            "label": node_label,
                            "layer": node_layer,
                            "type": "table",
                            "stub": node_layer != layer,
                        }
                edges.append({"from": row["from_id"], "to": row["to_id"], "type": "lineage"})

    logger.info(
        "Layer expansion | layer=%s nodes=%d edges=%d include_neighbors=%s",
        layer,
        len(nodes),
        len(edges),
        include_neighbors,
    )
    return {
        "layer": layer,
        "nodes": list(nodes.values()),
        "edges": edges,
        "meta": {"mode": "layer_expand", "include_neighbors": include_neighbors},
    }


@router.get("/graph/table/{table_id}")
def lineage_expand_table(
    table_id: str,
    edge_limit: int = Query(default=200, ge=10, le=5000),
    include_columns: bool = Query(default=True),
    column_limit: int = Query(default=400, ge=10, le=5000),
):
    """
    Expand one table node to its immediate lineage neighbors.
    """
    nodes: dict[str, dict] = {}
    edges: list[dict] = []

    with get_session() as session:
        table_row = session.run(
            """
            MATCH (t:Table {id: $table_id})-[:HAS_VERSION]->(v:TableVersion)
            WHERE coalesce(v.active, true) = true
            WITH DISTINCT t, split(t.id, ".") AS parts
            RETURN
                t.id AS id,
                coalesce(t.name, CASE WHEN size(parts) > 2 THEN parts[2] ELSE t.id END) AS label,
                toUpper(coalesce(t.layer, CASE WHEN size(parts) > 0 THEN parts[0] ELSE "UNKNOWN" END)) AS layer
            LIMIT 1
            """,
            table_id=table_id,
        ).single()

        if not table_row:
            raise HTTPException(status_code=404, detail=f"Table '{table_id}' not found.")

        nodes[table_row["id"]] = {
            "id": table_row["id"],
            "label": table_row["label"],
            "layer": table_row["layer"],
            "type": "table",
            "stub": False,
        }

        edge_rows = session.run(
            """
            MATCH (src_v:TableVersion)-[:DERIVED_FROM]->(tgt_v:TableVersion)
            WHERE coalesce(src_v.active, true) = true AND coalesce(tgt_v.active, true) = true
            MATCH (src_t:Table)-[:HAS_VERSION]->(src_v)
            MATCH (tgt_t:Table)-[:HAS_VERSION]->(tgt_v)
            WITH DISTINCT src_t, tgt_t,
                toUpper(coalesce(src_t.layer, split(src_t.id, ".")[0])) AS src_layer,
                toUpper(coalesce(tgt_t.layer, split(tgt_t.id, ".")[0])) AS tgt_layer
            WHERE src_t.id = $table_id OR tgt_t.id = $table_id
            RETURN
                src_t.id AS from_id,
                coalesce(src_t.name, src_t.id) AS from_label,
                src_layer AS from_layer,
                tgt_t.id AS to_id,
                coalesce(tgt_t.name, tgt_t.id) AS to_label,
                tgt_layer AS to_layer
            LIMIT $edge_limit
            """,
            table_id=table_id,
            edge_limit=edge_limit,
        )

        for row in edge_rows:
            for node_id, node_label, node_layer in [
                (row["from_id"], row["from_label"], row["from_layer"]),
                (row["to_id"], row["to_label"], row["to_layer"]),
            ]:
                if node_id not in nodes:
                    nodes[node_id] = {
                        "id": node_id,
                        "label": node_label,
                        "layer": node_layer,
                        "type": "table",
                        "stub": node_id != table_id,
                    }
            edges.append({"from": row["from_id"], "to": row["to_id"], "type": "lineage"})

        if include_columns:
            column_rows = session.run(
                """
                MATCH (t:Table {id: $table_id})-[:HAS_VERSION]->(v:TableVersion)-[:HAS_COLUMN]->(c:ColumnVersion)
                WHERE coalesce(v.active, true) = true AND coalesce(c.active, true) = true
                WITH DISTINCT t, c, split(c.id, ".") AS col_parts
                RETURN
                    c.id AS id,
                    coalesce(c.name, CASE WHEN size(col_parts) > 4 THEN col_parts[4] ELSE c.id END) AS label,
                    coalesce(c.datatype, c.type, "UNKNOWN") AS datatype,
                    t.id AS table_id
                ORDER BY label
                LIMIT $column_limit
                """,
                table_id=table_id,
                column_limit=column_limit,
            )
            for row in column_rows:
                col_id = row["id"]
                if col_id not in nodes:
                    nodes[col_id] = {
                        "id": col_id,
                        "label": row["label"],
                        "datatype": row["datatype"],
                        "type": "column",
                        "layer": None,
                        "stub": False,
                    }
                edges.append({"from": row["table_id"], "to": col_id, "type": "has_column"})

    logger.info("Table expansion | table=%s nodes=%d edges=%d", table_id, len(nodes), len(edges))
    return {
        "table": table_id,
        "nodes": list(nodes.values()),
        "edges": edges,
        "meta": {"mode": "table_expand"},
    }

@router.get("/health")
def lineage_health():
    return {"status": "lineage ok", "graph_api": "ready"}
