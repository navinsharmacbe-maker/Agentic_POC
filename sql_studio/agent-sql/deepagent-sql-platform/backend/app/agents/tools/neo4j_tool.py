from neo4j import GraphDatabase
from app.config import NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD

driver = GraphDatabase.driver(
    NEO4J_URI,
    auth=(NEO4J_USER, NEO4J_PASSWORD)
)

def run_cypher(query: str, **params):
    with driver.session() as session:
        result = session.run(query, **params)
        return [r.data() for r in result]
