import os
import json
import pandas 
from fastapi import HTTPException

def normalize_item(item: dict, record_id: int, scanner_used: list[int]):
    def get(scanner_key):
        data = item.get(scanner_key)
        if not data:
            return None, None
        return data.get("value"), data.get("valid")
    
    scanners = {
        1: get("scanner_1"),
        2: get("scanner_2"),
        3: get("scanner_3")
    }

    result = "Pass"
    fallback = False

    for scanner in scanner_used:
        value, valid = scanners.get(scanner, (None, None))

        if value is None:
            fallback = True
        
        if valid is False:
            result = "Fail"
            break

    return {
        "item_id": item["item_id"],
        "record_id": record_id,
        "scanner_1": scanners[1][0],
        "scanner_1_valid": scanners[1][1],
        "scanner_2": scanners[2][0],
        "scanner_2_valid": scanners[2][1],
        "scanner_3": scanners[3][0],
        "scanner_3_valid": scanners[3][1],
        "result": result,
        "fallback": fallback
    }


BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
JSON_DIR = os.path.join(BASE_DIR, "uploads", "json")
os.makedirs(JSON_DIR, exist_ok=True)

REQUIRED_COLUMNS = {
    "scaner 1": "scanner_1",
    "scaner 2": "scanner_2",
    "scaner 3": "scanner_3",
}

def excel_to_json(file, filename: str):
    try:
        df = pandas.read_excel(file, dtype=str)

        normalized_cols = {c.lower().strip(): c for c in df.columns}

        for col in REQUIRED_COLUMNS:
            if col not in normalized_cols:
                raise HTTPException(
                    400,
                    f"Missing required column: {col.upper()}"
                )

        result = []

        for idx, row in df.iterrows():
            result.append({
                "item_id": idx + 1,
                "scanner_1": row[normalized_cols["scaner 1"]],
                "scanner_2": row[normalized_cols["scaner 2"]],
                "scanner_3": row[normalized_cols["scaner 3"]],
            })

        json_filename = filename.replace(".xlsx", ".json")
        json_path = os.path.join(JSON_DIR, json_filename)

        with open(json_path, "w", encoding="utf-8") as f:
            json.dump(result, f, indent=2, ensure_ascii=False)

        return {
            "json_file": json_filename,
            "items": len(result),
            "data": result
        }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(500, str(e))