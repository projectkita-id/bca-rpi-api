from io import BytesIO
from datetime import datetime
from openpyxl import Workbook
from openpyxl.styles import PatternFill, Font, Alignment, Border, Side
from openpyxl.utils import get_column_letter

def export_record_to_excel(record_id: int, items: list, scanner_used: list):
    """
    Export items to Excel with format:
    No | Item ID | Scanner 1 | Scanner 2 | Scanner 3 | Result | Scan Time
    
    items format expected (flat from get_record_items):
    {
      item_id,
      scanner_1, scanner_2, scanner_3,
      result,
      timestamp (ISO format)
    }
    """

    wb = Workbook()
    ws = wb.active
    ws.title = f"Batch {record_id}"

    # ========================
    # STYLES
    # ========================
    TITLE_FILL = PatternFill(start_color="1454FB", end_color="1454FB", fill_type="solid")
    HEADER_FILL = PatternFill(start_color="203864", end_color="203864", fill_type="solid")
    
    PASS_FILL = PatternFill(start_color="C6EFCE", end_color="C6EFCE", fill_type="solid")
    FAIL_FILL = PatternFill(start_color="FFC7CE", end_color="FFC7CE", fill_type="solid")
    
    TITLE_FONT = Font(bold=True, size=14, color="FFFFFF")
    HEADER_FONT = Font(bold=True, size=11, color="FFFFFF")
    
    CENTER = Alignment(horizontal="center", vertical="center", wrap_text=False)
    LEFT = Alignment(horizontal="left", vertical="center", wrap_text=False)
    
    THIN = Side(style="thin", color="9E9E9E")
    BORDER = Border(left=THIN, right=THIN, top=THIN, bottom=THIN)

    # ========================
    # TITLE ROW
    # ========================
    ws.merge_cells("A1:G1")
    title_cell = ws["A1"]
    title_cell.value = f"BATCH REPORT #{record_id}"
    title_cell.fill = TITLE_FILL
    title_cell.font = TITLE_FONT
    title_cell.alignment = CENTER
    ws.row_dimensions[1].height = 26

    # ========================
    # HEADER ROW
    # ========================
    headers = ["No", "Item ID", "Scanner 1", "Scanner 2", "Scanner 3", "Result", "Scan Time"]
    for col_idx, header in enumerate(headers, start=1):
        cell = ws.cell(row=2, column=col_idx, value=header)
        cell.fill = HEADER_FILL
        cell.font = HEADER_FONT
        cell.alignment = CENTER
        cell.border = BORDER
    ws.row_dimensions[2].height = 20

    # ========================
    # DATA ROWS
    # ========================
    for i, item in enumerate(items, start=1):
        row = 2 + i

        # Column 1: No
        ws.cell(row=row, column=1, value=i).border = BORDER

        # Column 2: Item ID
        ws.cell(row=row, column=2, value=item.get("item_id")).border = BORDER

        # Column 3: Scanner 1
        ws.cell(row=row, column=3, value=item.get("scanner_1") or "").border = BORDER

        # Column 4: Scanner 2
        ws.cell(row=row, column=4, value=item.get("scanner_2") or "").border = BORDER

        # Column 5: Scanner 3
        ws.cell(row=row, column=5, value=item.get("scanner_3") or "").border = BORDER

        # Column 6: Result
        result_val = item.get("result") or "Unknown"
        result_cell = ws.cell(row=row, column=6, value=result_val)
        result_cell.border = BORDER
        result_cell.alignment = CENTER
        
        # Color pass/fail
        if result_val == "Pass":
            result_cell.fill = PASS_FILL
        elif result_val == "Fail":
            result_cell.fill = FAIL_FILL

        # Column 7: Scan Time (timestamp)
        timestamp = item.get("timestamp") or ""
        if timestamp:
            try:
                # Parse ISO format dan format readable
                dt = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
                timestamp = dt.strftime("%Y-%m-%d %H:%M:%S")
            except:
                pass
        
        ws.cell(row=row, column=7, value=timestamp).border = BORDER

    # ========================
    # COLUMN WIDTHS
    # ========================
    ws.column_dimensions["A"].width = 6      # No
    ws.column_dimensions["B"].width = 12     # Item ID
    ws.column_dimensions["C"].width = 22     # Scanner 1
    ws.column_dimensions["D"].width = 28     # Scanner 2
    ws.column_dimensions["E"].width = 12     # Scanner 3
    ws.column_dimensions["F"].width = 10     # Result
    ws.column_dimensions["G"].width = 20     # Scan Time

    # ========================
    # FREEZE PANES + FILTER
    # ========================
    ws.freeze_panes = "A3"
    
    if len(items) > 0:
        last_row = 2 + len(items)
        ws.auto_filter.ref = f"A2:G{last_row}"

    # ========================
    # SAVE TO MEMORY
    # ========================
    out = BytesIO()
    wb.save(out)
    out.seek(0)

    filename = f"batch_{record_id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
    return out, filename
