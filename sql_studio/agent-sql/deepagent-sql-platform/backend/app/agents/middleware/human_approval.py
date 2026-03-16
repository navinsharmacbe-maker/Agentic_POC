RISKY_KEYWORDS = ["FULL JOIN", "CROSS JOIN"]

def requires_approval(sql: str) -> bool:
    for keyword in RISKY_KEYWORDS:
        if keyword in sql.upper():
            return True
    return False
