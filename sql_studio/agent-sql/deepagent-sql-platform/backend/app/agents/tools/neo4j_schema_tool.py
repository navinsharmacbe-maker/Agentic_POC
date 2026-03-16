from app.neo4j_driver import get_session
import logging

logger = logging.getLogger(__name__)

def get_tables_by_layer(target_layer: str) -> list:
    """Gets all tables in a given layer (ODP, FDP, CDP)."""
    target_layer = target_layer.upper()
    try:
        with get_session() as session:
            result = session.run(
                """
                MATCH (t:Table)-[:HAS_VERSION]->(v:TableVersion {active:true})
                WHERE toUpper(t.layer) = $target_layer
                RETURN t.id AS table_id, t.name AS table_name, t.schema AS schema, t.layer AS layer
                """,
                {"target_layer": target_layer}
            )
            return [r.data() for r in result]
    except Exception as e:
        logger.error(f"Error getting tables for layer {target_layer}: {e}")
        return []

def get_table_schema(table_id: str) -> list:
    """Gets all columns and datatypes for a specific table."""
    try:
        with get_session() as session:
            result = session.run(
                """
                MATCH (t:Table {id: $table_id})-[:HAS_VERSION]->(v:TableVersion {active:true})-[:HAS_COLUMN]->(c:ColumnVersion {active:true})
                RETURN c.name AS column_name, c.datatype AS datatype
                """,
                {"table_id": table_id}
            )
            return [r.data() for r in result]
    except Exception as e:
        logger.error(f"Error getting schema for table {table_id}: {e}")
        return []
