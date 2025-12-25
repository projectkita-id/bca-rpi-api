from __future__ import annotations
import pandas
from io import BytesIO
from datetime import datetime
from typing import Any
from fastapi import HTTPException
import openpyxl
from openpyxl import Workbook
from openpyxl.styles import PatternFill, Font, Alignment, Border, Side
import os
import json

def normalize_item(item: dict, record_id: int, scanner_used: list[int] | list) -> dict:
    """
    Normalize input item from client -> flat fields for DB insert (record_item).

    Accepts either:
    - scanner_1/scanner_2/scanner_3 as dict: {"value": "...", "valid": true/false}
    - or scanner_1/scanner_2/scanner_3 as plain string

    Returns:
      {
        record_id, item_id,
        scanner_1, scanner_2, scanner_3,
        scanner_1_valid, scanner_2_valid, scanner_3_valid,
        result, fallback
      }
    """

    def _val(x):
        return x.get("value") if isinstance(x, dict) else x

    def _valid(x):
        if isinstance(x, dict) and "valid" in x:
            v = x.get("valid")
            if v is True:
                return 1
            if v is False:
                return 0
        return None

    s1 = item.get("scanner_1")
    s2 = item.get("scanner_2")
    s3 = item.get("scanner_3")

    return {
        "record_id": record_id,
        "item_id": item.get("item_id"),
        "scanner_1": _val(s1),
        "scanner_2": _val(s2),
        "scanner_3": _val(s3),
        "scanner_1_valid": _valid(s1),
        "scanner_2_valid": _valid(s2),
        "scanner_3_valid": _valid(s3),
        "result": item.get("result") or "Unknown",
        "fallback": 1 if item.get("fallback") else 0,
    }


def excel_to_json(file):
    try:
        file.seek(0)
        df = pandas.read_excel(file, dtype=str)

        # Normalisasi header
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

        def clean(value):
            if pandas.isna(value):
                return None
            return str(value).strip()

        result = []
        for _, row in df.iterrows():
            result.append({
                "Scanner 1": clean(row[normalized_cols["scanner 1"]]),
                "Scanner 2": clean(row[normalized_cols["scanner 2"]]),
                "Scanner 3": clean(row[normalized_cols["scanner 3"]]),
            })

        # === SINGLE DATABASE FILE ===
        db_path = os.path.expanduser("~/scanner-db.json")

        with open(db_path, "w", encoding="utf-8") as f:
            json.dump(result, f, indent=2, ensure_ascii=False)

        return {
            "status": "ok",
            "items": len(result),
            "path": db_path
        }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(500, f"Excel parsing error: {str(e)}")


def export_record_to_excel(record_id: int, items: list[dict], scanner_used: list[int] | list):
    """
    Generate Excel report from DB result items.

    Expected items format (from models.get_record_items):
      {
        "item_id": ...,
        "scanner_1": ...,
        "scanner_2": ...,
        "scanner_3": ...,
        "result": "Pass"/"Fail",
        "timestamp": "ISO datetime"  (mapped from created_at)
      }

    Excel columns:
      No | Item ID | Scanner 1 | Scanner 2 | Scanner 3 | Result | Scan Time
    """
    wb = Workbook()
    ws = wb.active
    ws.title = f"Batch {record_id}"

    # Styles
    TITLE_FILL = PatternFill(start_color="1454FB", end_color="1454FB", fill_type="solid")
    HEADER_FILL = PatternFill(start_color="203864", end_color="203864", fill_type="solid")

    PASS_FILL = PatternFill(start_color="C6EFCE", end_color="C6EFCE", fill_type="solid")
    FAIL_FILL = PatternFill(start_color="FFC7CE", end_color="FFC7CE", fill_type="solid")

    TITLE_FONT = Font(bold=True, size=14, color="FFFFFF")
    HEADER_FONT = Font(bold=True, size=11, color="FFFFFF")

    CENTER = Alignment(horizontal="center", vertical="center", wrap_text=False)
    THIN = Side(style="thin", color="9E9E9E")
    BORDER = Border(left=THIN, right=THIN, top=THIN, bottom=THIN)

    def apply(cell, *, fill=None, font=None, align=None):
        cell.border = BORDER
        if fill is not None:
            cell.fill = fill
        if font is not None:
            cell.font = font
        if align is not None:
            cell.alignment = align

    # Title row
    ws.merge_cells("A1:G1")
    title_cell = ws["A1"]
    title_cell.value = f"BATCH REPORT #{record_id}"
    apply(title_cell, fill=TITLE_FILL, font=TITLE_FONT, align=CENTER)
    ws.row_dimensions[1].height = 26

    # Header row
    headers = ["No", "Item ID", "Scanner 1", "Scanner 2", "Scanner 3", "Result", "Scan Time"]
    for col_idx, header in enumerate(headers, start=1):
        c = ws.cell(row=2, column=col_idx, value=header)
        apply(c, fill=HEADER_FILL, font=HEADER_FONT, align=CENTER)
    ws.row_dimensions[2].height = 20

    # Data rows
    for i, item in enumerate(items, start=1):
        r = 2 + i

        c1 = ws.cell(r, 1, i)
        c2 = ws.cell(r, 2, item.get("item_id"))
        c3 = ws.cell(r, 3, item.get("scanner_1") or "")
        c4 = ws.cell(r, 4, item.get("scanner_2") or "")
        c5 = ws.cell(r, 5, item.get("scanner_3") or "")

        result_val = item.get("result") or "Unknown"
        c6 = ws.cell(r, 6, result_val)

        ts = item.get("timestamp") or ""
        if ts:
            try:
                dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
                ts = dt.strftime("%Y-%m-%d %H:%M:%S")
            except Exception:
                pass
        c7 = ws.cell(r, 7, ts)

        for c in (c1, c2, c3, c4, c5, c6, c7):
            apply(c)

        c6.alignment = CENTER
        if result_val == "Pass":
            c6.fill = PASS_FILL
        elif result_val == "Fail":
            c6.fill = FAIL_FILL

    # Column widths
    ws.column_dimensions["A"].width = 6
    ws.column_dimensions["B"].width = 12
    ws.column_dimensions["C"].width = 22
    ws.column_dimensions["D"].width = 28
    ws.column_dimensions["E"].width = 12
    ws.column_dimensions["F"].width = 10
    ws.column_dimensions["G"].width = 20

    # Freeze panes + filter
    ws.freeze_panes = "A3"
    if items:
        ws.auto_filter.ref = f"A2:G{2+len(items)}"

    # Save to memory
    out = BytesIO()
    wb.save(out)
    out.seek(0)

    filename = f"batch_{record_id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
    return out, filename