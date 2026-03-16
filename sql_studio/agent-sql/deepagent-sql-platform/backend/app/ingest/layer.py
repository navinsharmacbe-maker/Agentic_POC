from fastapi import APIRouter, File, UploadFile
import pandas as pd

from app.neo4j_driver import get_session

router = APIRouter()


@router.post("/ingest/layers")
def ingest_layers(file: UploadFile = File(...)):
    df = pd.read_csv(file.file)

    df.columns = (
        df.columns
        .str.strip()
        .str.upper()
        .str.replace(" ", "_")
    )

    query = """
    MERGE (l:Layer {name: $layer_name})
    SET l.description = $description
    """

    with get_session() as session:
        for _, row in df.iterrows():
            session.run(
                query,
                layer_name=row["LAYER_NAME"],
                description=row.get("DESCRIPTION", ""),
            )

    return {"status": "Layers ingested successfully"}
