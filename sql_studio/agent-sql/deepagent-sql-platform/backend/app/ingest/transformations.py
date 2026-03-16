from fastapi import APIRouter, UploadFile

from app.neo4j_driver import get_session
from app.utils.csv_reader import read_csv

router = APIRouter()


@router.post("/ingest/transformations")
def ingest_transformations(file: UploadFile):
    df = read_csv(file)

    with get_session() as session:
        for _, row in df.iterrows():
            session.run(
                """
                MERGE (t:Table {name: $target_table})
                MERGE (tr:Transformation {
                    layer: $layer,
                    target_column: $target_column,
                    logic: $logic
                })
                MERGE (t)-[:TRANSFORMED_BY]->(tr)
                """,
                **row.to_dict(),
            )

    return {"status": "Transformations ingested"}
