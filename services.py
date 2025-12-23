import os
import json
import pandas 
from openpyxl import Workbook
from datetime import datetime
from fastapi import HTTPException
from openpyxl.styles import PatternFill, Font, Alignment, Border, Side

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

def excel_to_json(file, filename: str):
    try:
        file.seek(0)
        df = pandas.read_excel(file, dtype=str)

        normalized_cols = {
            c.lower().strip(): c for c in df.columns
        }

        required = ["scanner 1", "scanner 2", "scanner 3"]

        for col in required:
            if col not in normalized_cols:
                raise HTTPException(
                    400,
                    f"Missing required column: {col.upper()}"
                )

        result = []

        for _, row in df.iterrows():
            def clean(value):
                if pandas.isna(value):
                    return None
                return str(value).strip()

            result.append({
                "Scanner 1": clean(row[normalized_cols["scanner 1"]]),
                "Scanner 2": clean(row[normalized_cols["scanner 2"]]),
                "Scanner 3": clean(row[normalized_cols["scanner 3"]]),
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
        raise HTTPException(500, f"Excel parsing error: {str(e)}")


HEADER_FILL = PatternFill(
    start_color="D9E1F2",
    end_color="D9E1F2",
    fill_type="solid"
)

THIN_BORDER = Border(
    left=Side(style="thin"),
    right=Side(style="thin"),
    top=Side(style="thin"),
    bottom=Side(style="thin")
)

GREEN = PatternFill(start_color="C6EFCE", end_color="C6EFCE", fill_type="solid")
RED   = PatternFill(start_color="FFC7CE", end_color="FFC7CE", fill_type="solid")
HEADER_FONT = Font(bold=True)
CENTER_ALIGN = Alignment(horizontal="center", vertical="center")

def export_record_to_excel(record_id, items, scanner_used):
    wb = Workbook()
    ws = wb.active
    ws.title = "Scan Result"

    headers = [
        "Item ID",
        "Scanner 1",
        "Scanner 2",
        "Scanner 3",
        "Result",
        "Scan Time"
    ]
    ws.append(headers)

    ws.column_dimensions["A"].width = 12
    ws.column_dimensions["B"].width = 25
    ws.column_dimensions["C"].width = 35
    ws.column_dimensions["D"].width = 20
    ws.column_dimensions["E"].width = 10
    ws.column_dimensions["F"].width = 20

    max_row = ws.max_row
    max_col = ws.max_column

    for row in range(1, max_row + 1):
        for col in range(1, max_col + 1):
            ws.cell(row=row, column=col).border = THIN_BORDER

    for col in range(1, len(headers) + 1):
        cell = ws.cell(row=1, column=col)
        cell.fill = HEADER_FILL
        cell.font = HEADER_FONT
        cell.alignment = CENTER_ALIGN

    for row_idx, item in enumerate(items, start=2):
        ws.append([
            item["item_id"],
            item["scanner_1"],
            item["scanner_2"],
            item["scanner_3"],
            item["result"],
            item["created_at"]
        ])

        for col in range(1, 7):
            ws.cell(row=row_idx, column=col).alignment = CENTER_ALIGN

        scanner_map = {
            1: ("scanner_1_valid", 2),
            2: ("scanner_2_valid", 3),
            3: ("scanner_3_valid", 4),
        }

        for scanner_id in scanner_used:
            key, col = scanner_map[scanner_id]
            valid_key = f"{key}_valid"

            valid = item.get(valid_key)
            cell = ws.cell(row=row_idx, column=col)

            if valid is True:
                cell.fill = GREEN
            elif valid is False:
                cell.fill = RED
            # else → no color

    os.makedirs("exports", exist_ok=True)
    filename = f"batch_{record_id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
    path = os.path.join("exports", filename)

    wb.save(path)
    return path