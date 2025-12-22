import json
from services import normalize_item, excel_to_json
from models import create_record, finish_record, get_record
from fastapi import FastAPI, HTTPException, UploadFile, File

app = FastAPI(title="BCA Envelope Sorting API")

@app.get("/")
def health():
    return {"status": "ok"}

@app.post("/batch/start")
def start_batch(scanner_used: list[int], batch_code: str | None = None):
    record_id = create_record(scanner_used, batch_code)
    return {
        "record_id": record_id,
        "scanner_used": scanner_used    
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

    result = excel_to_json(file.file, file.filename)

    return {
        "status": "ok",
        "items": result["items"],
        "json_file": result["json_file"]
    }