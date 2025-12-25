import os
import json
from schema import StartBatchRequest
from fastapi.responses import StreamingResponse
from fastapi import FastAPI, HTTPException, UploadFile, File
from services import normalize_item, excel_to_json, export_record_to_excel
from models import create_record, finish_record, get_record, get_record_items

app = FastAPI(title="BCA Envelope Sorting API")

@app.get("/")
def health():
    return {"status": "ok"}
# Tambahkan di main.py atau file backend Anda

@app.get("/batch/list")
def list_batches(status: str = None):
    """Get list of all batches, optionally filtered by status"""
    from models import get_all_records  # Anda perlu buat fungsi ini
    
    records = get_all_records(status)
    return {
        "total": len(records),
        "records": records
    }

@app.get("/batch/{record_id}")
def get_batch_detail(record_id: int):
    """Get detailed info about a specific batch"""
    record = get_record(record_id)
    
    if not record:
        raise HTTPException(404, "Record not found")
    
    items = get_record_items(record_id)
    scanner_used = json.loads(record["scanner_used"])
    
    # Calculate statistics
    total_items = len(items)
    pass_count = sum(1 for item in items if item.get("validation_result") == "PASS")
    fail_count = total_items - pass_count
    
    return {
        "record_id": record_id,
        "batch_code": record.get("batch_code"),
        "start_time": record.get("start_time"),
        "end_time": record.get("end_time"),
        "status": record.get("status"),
        "scanner_used": scanner_used,
        "total_items": total_items,
        "pass_count": pass_count,
        "fail_count": fail_count,
        "items": items
    }

@app.post("/batch/start")
def start_batch(payload: StartBatchRequest):
    record_id = create_record(payload.scanner_used, payload.batch_code)
    return {
        "record_id": record_id,
        "scanner_used": payload.scanner_used    
    }

@app.post("/batch/{record_id}/finish")
def finish_batch(record_id: int, items: list[dict]):
    records = get_record(record_id)

    if not records:
        raise HTTPException(404, "Record not found")

    if not items:
        raise HTTPException(400, detail="Item cannot be empty")

    if records["status"] != "Running":
        raise HTTPException(400, "Batch already finished")
    
    scanner_used = json.loads(records["scanner_used"])

    normalized = [
        normalize_item(item, record_id, scanner_used)
        for item in items
    ]

    finish_record(record_id, normalized)

    return {
        "status": "completed",
        "total_items": len(items),
        "scanner_used": scanner_used
    }

@app.post("/upload-file")
async def upload_excel(file: UploadFile = File(...)):
    if not file.filename.endswith(".xlsx"):
        raise HTTPException(400, "Only .xlsx files are allowed")

    result = excel_to_json(file.file)
    return result

@app.get("/download/{record_id}")
def download_record(record_id: int):
    record = get_record(record_id)

    if not record:
        raise HTTPException(404, "Record not found")

    if record["status"] != "Completed":
        raise HTTPException(400, "Record not completed yet")

    items = get_record_items(record_id)
    if not items:
        raise HTTPException(404, "No items found for this record")

    scanner_used = json.loads(record["scanner_used"])

    file_obj, filename = export_record_to_excel(
        record_id, items, scanner_used
    )

    return StreamingResponse(
        file_obj,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={
            "Content-Disposition": f'attachment; filename="{filename}"'
        }
    )