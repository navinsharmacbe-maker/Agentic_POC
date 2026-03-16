import re

def rewrite_sql(query: str, allow_ddl: bool = False, allow_dml: bool = False):
    query = (query or "").strip()
    upper = query.upper()

    FORBIDDEN_DDL = ["DROP", "ALTER"]
    FORBIDDEN_DML = ["DELETE", "UPDATE", "TRUNCATE"]

    for keyword in FORBIDDEN_DDL:
        if not allow_ddl and keyword in upper:
            raise Exception(f"Forbidden DDL operation detected: {keyword}")

    for keyword in FORBIDDEN_DML:
        if not allow_dml and keyword in upper:
            raise Exception(f"Forbidden DML operation detected: {keyword}")

    if "LIMIT" not in upper:
        if query.endswith(";"):
            query = query[:-1].rstrip() + "\nLIMIT 100;"
        else:
            query += "\nLIMIT 100;"

    return query
