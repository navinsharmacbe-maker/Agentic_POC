import os
from dotenv import load_dotenv
from neo4j import GraphDatabase
from mcp.server.fastmcp import FastMCP

# ------------------ LOAD ENV ------------------
load_dotenv()

NEO4J_URI = os.getenv("NEO4J_URI")
NEO4J_USER = os.getenv("NEO4J_USER")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD")

# ------------------ INIT MCP ------------------
# Note: Host and port are passed to mcp.run(), not the constructor
mcp = FastMCP("Neo4j-MCP-Server", host="0.0.0.0", port=8002)
# ------------------ INIT NEO4J DRIVER ------------------
driver = GraphDatabase.driver(
    NEO4J_URI, 
    auth=(NEO4J_USER, NEO4J_PASSWORD)
)

# ------------------ SECURITY & UTILS ------------------
FORBIDDEN = ["CREATE", "MERGE", "DELETE", "SET", "DROP", "REMOVE"]

def test_connection():
    try:
        driver.verify_connectivity()
        print("✅ Neo4j Connection Verified")
    except Exception as e:
        print(f"❌ Neo4j Connection Failed: {e}")
        exit(1) # Stop the server immediately if auth fails

def validate_read_only(query: str):
    upper = query.upper()
    for keyword in FORBIDDEN:
        if keyword in upper:
            raise ValueError(f"Forbidden operation detected: {keyword}")
    
    # Simple check to ensure it's a retrieval query
    if "MATCH" not in upper and "RETURN" not in upper:
        raise ValueError("Only MATCH or RETURN queries are allowed.")

# ------------------ MCP TOOLS ------------------

@mcp.tool()
def execute_cypher(query: str) -> list:
    """Executes a read-only Cypher query against Neo4j."""
    validate_read_only(query)
    with driver.session() as session:
        result = session.run(query)
        return [record.data() for record in result]

@mcp.tool()
def get_table_schema(table_name: str) -> dict:
    """Retrieves schema, active columns, and upstream dependencies for a table."""
    query = """
    // 1. Find the Table and its currently active version
    MATCH (t:Table {id: $table_name})-[:HAS_VERSION]->(tv:TableVersion {active: true})
    
    // 2. Get all columns associated with this active version
    OPTIONAL MATCH (tv)-[:HAS_COLUMN]->(cv:ColumnVersion {active: true})
    
    // 3. Get upstream dependencies (what this version is DERIVED_FROM)
    OPTIONAL MATCH (tv)-[:DERIVED_FROM]->(upstream:TableVersion)
    OPTIONAL MATCH (upstream_table:Table)-[:HAS_VERSION]->(upstream)
    
    RETURN t.id as table_id,
           tv.type as table_type,
           tv.version as version,
           collect(distinct {
               name: cv.name, 
               type: cv.datatype, 
               is_pk: cv.is_pk
           }) as columns,
           collect(distinct upstream_table.id) as dependencies
    """
    with driver.session() as session:
        result = session.run(query, table_name=table_name)
        record = result.single()
        if not record:
            return {"error": f"Active table version for '{table_name}' not found."}
        return record.data()
# ------------------ RUN SERVER ------------------

@mcp.tool()
def generate_sql_from_spec(
    table_name: str,
    business_spec: dict
) -> str:
    """
    Generates SQL for a table using transformation specification.
    Only supports AGGREGATE tables.
    """

    if table_name not in business_spec:
        return f"Table {table_name} not found in business spec."

    spec = business_spec[table_name]

    if spec["type"] != "AGGREGATE":
        return "Only AGGREGATE tables supported."

    source = spec["source"]
    columns = spec["columns"]

    select_parts = []
    group_by = []

    for col, logic in columns.items():
        if "COUNT" in logic.upper() or "SUM" in logic.upper():
            select_parts.append(f"{logic} AS {col}")
        else:
            select_parts.append(col)
            group_by.append(col)

    sql = f"""
SELECT
    {", ".join(select_parts)}
FROM {source}
"""

    if group_by:
        sql += f"\nGROUP BY {', '.join(group_by)}"

    sql += "\nLIMIT 100;"

    return sql

@mcp.tool()
def explain_column(table_name: str, column_name: str) -> dict:
    """
    Explains a column from the Neo4j schema.
    """
    query = """
    MATCH (t:Table {id: $table_name})-[:HAS_VERSION]->(tv:TableVersion {active:true})
    MATCH (tv)-[:HAS_COLUMN]->(cv:ColumnVersion {name:$column_name, active:true})
    RETURN cv.name as name,
           cv.datatype as datatype,
           cv.business_description as description
    """

    with driver.session() as session:
        result = session.run(query, table_name=table_name, column_name=column_name)
        record = result.single()
        if not record:
            return {"error": "Column not found"}
        return record.data()



if __name__ == "__main__":
    # Use "sse" for HTTP-based transport. 
    # This will start a Starlette/FastAPI-style server.
    try:
        test_connection()
        mcp.run(
            transport="sse"
        )
    finally:
        driver.close()