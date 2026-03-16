import json
import os

files = [
    "D:/Python/sql_studio/agent-sql/deepagent-sql-platform/backend/app/ingest_bulk_files/ODP_BULK.json",
    "D:/Python/sql_studio/agent-sql/deepagent-sql-platform/backend/app/ingest_bulk_files/FDP_BULK.json",
    "D:/Python/sql_studio/agent-sql/deepagent-sql-platform/backend/app/ingest_bulk_files/CDP_BULK.json",
]

for file_path in files:
    if os.path.exists(file_path):
        with open(file_path, "r") as f:
            data = json.load(f)

        for table in data:
            if "description" not in table:
                table["description"] = f"Description for {table.get('name', 'Unknown Table')} in layer {table.get('layer', 'Unknown Layer')}"

        with open(file_path, "w") as f:
            json.dump(data, f, indent=2)

        print(f"Updated {file_path}")
    else:
        print(f"File not found: {file_path}")
