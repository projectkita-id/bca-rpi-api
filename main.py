import os
import json
from datetime import datetime
from schema import StartBatchRequest
from fastapi.responses import StreamingResponse, HTMLResponse
from fastapi import FastAPI, HTTPException, UploadFile, File
from fastapi.middleware.cors import CORSMiddleware
from services import normalize_item, excel_to_json, export_record_to_excel
from models import (
    create_record,
    finish_record,
    get_record,
    get_record_items,
    get_all_records
)

app = FastAPI(title="BCA Envelope Sorting API")

# ========== CORS MIDDLEWARE ==========
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/")
def health():
    return {"status": "ok", "message": "BCA Scanner API is running", "database": "bca_envelope"}

@app.post("/batch/start")
def start_batch(payload: StartBatchRequest):
    """Start a new batch scanning session"""
    record_id = create_record(payload.scanner_used, payload.batch_code)
    return {
        "record_id": record_id,
        "scanner_used": payload.scanner_used,
        "batch_code": payload.batch_code
    }

@app.post("/batch/{record_id}/finish")
def finish_batch(record_id: int, items: list[dict]):
    """Finish batch and save all items"""
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

@app.get("/batch/list")
def list_batches(status: str = None):
    """Get list of all batches with statistics"""
    try:
        print(f"\n🔍 API /batch/list called (status filter: {status})")
        records = get_all_records(status)

        print(f"✅ Returning {len(records)} records")

        return {
            "total": len(records),
            "records": records
        }
    except Exception as e:
        print(f"❌ Error in list_batches: {e}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/batch/{record_id}")
def get_batch_detail(record_id: int):
    """Get detailed info about a specific batch"""
    try:
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
    except HTTPException:
        raise
    except Exception as e:
        print(f"❌ Error in get_batch_detail: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/upload-file")
async def upload_excel(file: UploadFile = File(...)):
    """Upload Excel file and convert to JSON"""
    if not file.filename.endswith(".xlsx"):
        raise HTTPException(400, "Only .xlsx files are allowed")

    result = excel_to_json(file.file)
    return result

@app.get("/download/{record_id}")
def download_record(record_id: int):
    """Download Excel file for a specific record"""
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

@app.get("/export", response_class=HTMLResponse)
async def export_page():
    """Serve the batch export HTML page"""
    html_content = """
<!DOCTYPE html>
<html lang="id">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>BCA Scanner - Export Report</title>
    <style>
        * {
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }

        body {
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            min-height: 100vh;
            padding: 20px;
        }

        .container {
            max-width: 1200px;
            margin: 0 auto;
        }

        .header-card {
            background: white;
            border-radius: 20px;
            padding: 40px;
            margin-bottom: 30px;
            box-shadow: 0 20px 60px rgba(0, 0, 0, 0.3);
            text-align: center;
        }

        .header-card h1 {
            color: #1454fb;
            font-size: 36px;
            margin-bottom: 10px;
        }

        .header-card p {
            color: #0d3ea8;
            font-size: 16px;
        }

        .content-card {
            background: white;
            border-radius: 20px;
            padding: 40px;
            box-shadow: 0 20px 60px rgba(0, 0, 0, 0.3);
        }

        .filter-section {
            display: flex;
            gap: 15px;
            margin-bottom: 30px;
            flex-wrap: wrap;
            align-items: center;
        }

        .filter-btn {
            padding: 12px 24px;
            border: 2px solid #1454fb;
            background: white;
            color: #1454fb;
            border-radius: 8px;
            font-size: 14px;
            font-weight: 600;
            cursor: pointer;
            transition: all 0.3s;
        }

        .filter-btn:hover {
            background: #1454fb;
            color: white;
        }

        .filter-btn.active {
            background: #1454fb;
            color: white;
        }

        .search-box {
            flex: 1;
            min-width: 250px;
        }

        .search-box input {
            width: 100%;
            padding: 12px 20px;
            border: 2px solid #e0e0e0;
            border-radius: 8px;
            font-size: 14px;
            transition: border 0.3s;
        }

        .search-box input:focus {
            outline: none;
            border-color: #1454fb;
        }

        .batch-grid {
            display: grid;
            grid-template-columns: repeat(auto-fill, minmax(350px, 1fr));
            gap: 20px;
            margin-bottom: 20px;
        }

        .batch-card {
            border: 2px solid #e0e0e0;
            border-radius: 12px;
            padding: 20px;
            transition: all 0.3s;
            cursor: pointer;
            background: #f8f9ff;
        }

        .batch-card:hover {
            transform: translateY(-5px);
            box-shadow: 0 10px 30px rgba(20, 84, 251, 0.2);
            border-color: #1454fb;
        }

        .batch-card.selected {
            border-color: #1454fb;
            background: #e8ecff;
            border-width: 3px;
        }

        .batch-header {
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 15px;
            padding-bottom: 15px;
            border-bottom: 2px solid #e0e0e0;
        }

        .batch-id {
            font-size: 24px;
            font-weight: bold;
            color: #1454fb;
        }

        .status-badge {
            padding: 6px 12px;
            border-radius: 20px;
            font-size: 12px;
            font-weight: 600;
        }

        .status-completed {
            background: #c6efce;
            color: #006100;
        }

        .status-running {
            background: #fff4ce;
            color: #9c5700;
        }

        .batch-info {
            margin-bottom: 10px;
        }

        .info-row {
            display: flex;
            justify-content: space-between;
            padding: 8px 0;
            font-size: 14px;
        }

        .info-label {
            color: #0d3ea8;
            font-weight: 600;
        }

        .info-value {
            color: #1a1a2e;
        }

        .batch-stats {
            display: flex;
            gap: 10px;
            margin-top: 15px;
            padding-top: 15px;
            border-top: 2px solid #e0e0e0;
        }

        .stat-item {
            flex: 1;
            text-align: center;
            padding: 10px;
            background: white;
            border-radius: 8px;
        }

        .stat-number {
            font-size: 20px;
            font-weight: bold;
            color: #1454fb;
        }

        .stat-label {
            font-size: 11px;
            color: #0d3ea8;
            margin-top: 3px;
        }

        .scanner-badges {
            display: flex;
            gap: 5px;
            flex-wrap: wrap;
            margin-top: 5px;
        }

        .scanner-badge {
            background: #1454fb;
            color: white;
            padding: 4px 10px;
            border-radius: 12px;
            font-size: 11px;
            font-weight: 600;
        }

        .action-section {
            position: sticky;
            bottom: 20px;
            background: white;
            border-radius: 12px;
            padding: 20px;
            box-shadow: 0 -5px 30px rgba(0, 0, 0, 0.1);
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-top: 30px;
        }

        .selected-info {
            font-size: 14px;
            color: #0d3ea8;
        }

        .selected-info strong {
            color: #1454fb;
            font-size: 18px;
        }

        .btn-download {
            display: inline-flex;
            align-items: center;
            gap: 10px;
            background: #0f9d58;
            color: white;
            padding: 15px 30px;
            border: none;
            border-radius: 10px;
            font-size: 16px;
            font-weight: bold;
            cursor: pointer;
            transition: all 0.3s;
        }

        .btn-download:hover:not(:disabled) {
            background: #0b7c45;
            transform: translateY(-2px);
        }

        .btn-download:disabled {
            background: #ccc;
            cursor: not-allowed;
        }

        .loading {
            text-align: center;
            padding: 40px;
            color: #1454fb;
        }
.import-section {
  margin-bottom: 20px;
  padding: 16px;
  border: 2px dashed #1454fb;
  border-radius: 12px;
  background: #f8f9ff;
  display: flex;
  justify-content: space-between;
  align-items: center;
  gap: 15px;
  flex-wrap: wrap;
}

.import-title {
  font-weight: 800;
  color: #1454fb;
  font-size: 16px;
}

.import-desc {
  color: #0d3ea8;
  font-size: 13px;
  margin-top: 4px;
}

.import-right {
  display: flex;
  gap: 10px;
  align-items: center;
}

.btn-import {
  background: #1454fb;
  color: white;
  padding: 12px 18px;
  border: none;
  border-radius: 10px;
  font-weight: 700;
  cursor: pointer;
  transition: 0.2s;
}

.btn-import:hover:not(:disabled) {
  background: #0d3ea8;
}

.btn-import:disabled {
  background: #ccc;
  cursor: not-allowed;
}

.import-result {
  margin-bottom: 20px;
  padding: 12px 14px;
  border-radius: 10px;
  font-size: 13px;
  color: #1a1a2e;
  background: #e8ecff;
  border: 1px solid #c7d2ff;
  white-space: pre-wrap;
}

        .spinner {
            border: 4px solid #f3f3f3;
            border-top: 4px solid #1454fb;
            border-radius: 50%;
            width: 50px;
            height: 50px;
            animation: spin 1s linear infinite;
            margin: 0 auto 15px;
        }

        @keyframes spin {
            0% { transform: rotate(0deg); }
            100% { transform: rotate(360deg); }
        }

        .empty-state {
            text-align: center;
            padding: 60px 20px;
            color: #0d3ea8;
        }

        .empty-state-icon {
            font-size: 64px;
            margin-bottom: 20px;
        }

        .empty-state h3 {
            font-size: 20px;
            margin-bottom: 10px;
        }

        @media (max-width: 768px) {
            .batch-grid {
                grid-template-columns: 1fr;
            }

            .action-section {
                flex-direction: column;
                gap: 15px;
            }

            .btn-download {
                width: 100%;
                justify-content: center;
            }
        }
    </style>
</head>
<body>
    <div class="container">
        <div class="header-card">
            <h1>📊 Batch Export Manager</h1>
            <p>Pilih batch yang ingin di-export ke Excel</p>
        </div>

        <div class="content-card">
            <div class="filter-section">
                <button class="filter-btn active" data-filter="all">Semua</button>
                <button class="filter-btn" data-filter="Completed">Selesai</button>
                <button class="filter-btn" data-filter="Running">Berjalan</button>
                <div class="search-box">
                    <input type="text" id="searchInput" placeholder="🔍 Cari batch code atau ID...">
                </div>
            </div>
<div class="import-section">
  <div class="import-left">
    <div class="import-title">Import Excel</div>
    <div class="import-desc">Upload file .xlsx untuk diproses oleh API</div>
  </div>

  <div class="import-right">
    <input type="file" id="importFile" accept=".xlsx" />
    <button class="btn-import" id="importBtn">Upload</button>
  </div>
</div>

<div id="importResult" class="import-result" style="display:none;"></div>

            <div id="loadingBox" class="loading">
                <div class="spinner"></div>
                <p>Loading batch data...</p>
            </div>

            <div id="batchGrid" class="batch-grid" style="display: none;"></div>

            <div id="emptyState" class="empty-state" style="display: none;">
                <div class="empty-state-icon">📭</div>
                <h3>Tidak ada batch ditemukan</h3>
                <p>Belum ada data batch atau filter tidak cocok</p>
            </div>
        </div>

        <div class="action-section" id="actionSection" style="display: none;">
            <div class="selected-info">
                <span>Batch terpilih: <strong id="selectedBatchId">-</strong></span>
            </div>
            <button class="btn-download" id="downloadBtn" disabled>
                <span>📥</span>
                <span>Download Excel</span>
            </button>
        </div>
    </div>

    <script>
        const API_BASE = window.location.origin;
const importFile = document.getElementById('importFile');
const importBtn = document.getElementById('importBtn');
const importResult = document.getElementById('importResult');

        let allBatches = [];
        let selectedBatchId = null;
        let displayMap = {}; // realId -> displayNo
        let currentFilter = 'all';

        const loadingBox = document.getElementById('loadingBox');
        const batchGrid = document.getElementById('batchGrid');
        const emptyState = document.getElementById('emptyState');
        const actionSection = document.getElementById('actionSection');
        const downloadBtn = document.getElementById('downloadBtn');
        const selectedBatchIdEl = document.getElementById('selectedBatchId');
        const searchInput = document.getElementById('searchInput');

        function formatDateTime(isoString) {
            if (!isoString) return '-';
            const date = new Date(isoString);
            return date.toLocaleString('id-ID', {
                day: '2-digit',
                month: 'short',
                year: 'numeric',
                hour: '2-digit',
                minute: '2-digit'
            });
        }

        function createBatchCard(batch,displayNo) {
            const card = document.createElement('div');
            card.className = 'batch-card';
            card.dataset.batchId = batch.id;
            card.dataset.status = batch.status;
            card.dataset.batchCode = (batch.batch_code || '').toLowerCase();

            let scannerUsed = [];
            try {
                scannerUsed = JSON.parse(batch.scanner_used || '[]');
            } catch(e) {
                console.error('Error parsing scanner_used:', e);
            }

            const statusClass = batch.status === 'Completed' ? 'status-completed' : 'status-running';

            card.innerHTML = `
                <div class="batch-header">
                    <div class="batch-id">#${displayNo}</div>
                    <div class="status-badge ${statusClass}">${batch.status}</div>
                </div>
                <div class="batch-info">
                    <div class="info-row">
                        <span class="info-label">Batch Code:</span>
                        <span class="info-value">${batch.batch_code || '-'}</span>
                    </div>
                    <div class="info-row">
                        <span class="info-label">Start:</span>
                        <span class="info-value">${formatDateTime(batch.start_time)}</span>
                    </div>
                    <div class="info-row">
                        <span class="info-label">End:</span>
                        <span class="info-value">${formatDateTime(batch.end_time)}</span>
                    </div>
                    <div class="info-row">
                        <span class="info-label">Scanners:</span>
                        <div class="scanner-badges">
                            ${scannerUsed.map(s => `<span class="scanner-badge">S${s}</span>`).join('') || '-'}
                        </div>
                    </div>
                </div>
                <div class="batch-stats">
                    <div class="stat-item">
                        <div class="stat-number">${batch.total_items || 0}</div>
                        <div class="stat-label">Items</div>
                    </div>
                    <div class="stat-item">
                        <div class="stat-number" style="color: #0f9d58;">${batch.pass_count || 0}</div>
                        <div class="stat-label">Pass</div>
                    </div>
                    <div class="stat-item">
                        <div class="stat-number" style="color: #d32f2f;">${batch.fail_count || 0}</div>
                        <div class="stat-label">Fail</div>
                    </div>
                </div>
            `;

            if (batch.status === 'Completed') {
                card.addEventListener('click', () => selectBatch(batch.id));
            } else {
                card.style.opacity = '0.6';
                card.style.cursor = 'not-allowed';
            }

            return card;
        }

        function selectBatch(batchId) {
            selectedBatchId = batchId;

            document.querySelectorAll('.batch-card').forEach(card => {
                card.classList.remove('selected');
            });

            const selectedCard = document.querySelector(`[data-batch-id="${batchId}"]`);
            if (selectedCard) {
                selectedCard.classList.add('selected');
            }
            selectedBatchIdEl.textContent = `#${displayMap[batchId] || batchId}`;
            actionSection.style.display = 'flex';
            downloadBtn.disabled = false;
        }

        function filterBatches() {
            const searchTerm = searchInput.value.toLowerCase();
            const cards = document.querySelectorAll('.batch-card');
            let visibleCount = 0;

            cards.forEach(card => {
                const status = card.dataset.status;
                const batchCode = card.dataset.batchCode;
                const batchId = card.dataset.batchId;

                const matchesFilter = currentFilter === 'all' || status === currentFilter;
                const matchesSearch = !searchTerm ||
                    batchCode.includes(searchTerm) ||
                    batchId.includes(searchTerm);

                if (matchesFilter && matchesSearch) {
                    card.style.display = 'block';
                    visibleCount++;
                } else {
                    card.style.display = 'none';
                }
            });

            emptyState.style.display = visibleCount === 0 ? 'block' : 'none';
        }
importBtn.addEventListener('click', async () => {
  try {
    if (!importFile.files || importFile.files.length === 0) {
      alert('Pilih file .xlsx dulu');
      return;
    }

    const f = importFile.files[0];
    if (!f.name.toLowerCase().endsWith('.xlsx')) {
      alert('File harus .xlsx');
      return;
    }

    importBtn.disabled = true;
    importBtn.textContent = 'Uploading...';

    const form = new FormData();
    form.append('file', f);

    // lebih aman pakai API_BASE, tapi kalau mau hardcode silakan ganti:
    // const url = "http://192.168.1.29:8000/upload-file";
    const url = `${API_BASE}/upload-file`;

    const res = await fetch(url, {
      method: 'POST',
      body: form
    });

    const text = await res.text();
    importResult.style.display = 'block';

    if (!res.ok) {
      importResult.textContent = `Upload gagal (${res.status}):\n${text}`;
      return;
    }

    // coba parse json
    try {
      const json = JSON.parse(text);
      importResult.textContent = `Upload sukses:\n${JSON.stringify(json, null, 2)}`;
    } catch (e) {
      importResult.textContent = `Upload sukses:\n${text}`;
    }

  } catch (err) {
    alert('Error upload: ' + err.message);
  } finally {
    importBtn.disabled = false;
    importBtn.textContent = 'Upload';
  }
});

        async function loadBatches() {
            try {
                console.log('🔍 Loading batches from:', `${API_BASE}/batch/list`);

                loadingBox.style.display = 'block';
                batchGrid.style.display = 'none';
                emptyState.style.display = 'none';

                const response = await fetch(`${API_BASE}/batch/list`);
                console.log('📡 Response status:', response.status);

                if (!response.ok) throw new Error('Failed to load batches');

                const data = await response.json();
                console.log('✅ Data received:', data);

allBatches = data.records || [];

// urutkan ASC biar 37->1, 38->2, 39->3
allBatches.sort(function(a, b){ return a.id - b.id; });

console.log(`?? Total batches: ${allBatches.length}`);

batchGrid.innerHTML = '';
displayMap = {};  // reset mapping

if (allBatches.length === 0) {
    emptyState.style.display = 'block';
    batchGrid.style.display = 'none';
} else {
    allBatches.forEach(function(batch, idx){
        displayMap[batch.id] = idx + 1;
        const card = createBatchCard(batch, idx + 1);
        batchGrid.appendChild(card);
    });
    batchGrid.style.display = 'grid';
    emptyState.style.display = 'none';
}

loadingBox.style.display = 'none';

            } catch (error) {
                console.error('💥 Error loading batches:', error);
                loadingBox.innerHTML = `<p style="color: #d32f2f;">❌ Error: ${error.message}</p>`;
            }
        }

        downloadBtn.addEventListener('click', async () => {
            if (!selectedBatchId) return;

            try {
                downloadBtn.disabled = true;
                downloadBtn.innerHTML = '<span>⏳</span><span>Generating...</span>';

                const response = await fetch(`${API_BASE}/download/${selectedBatchId}`);

                if (!response.ok) {
                    throw new Error('Failed to generate Excel file');
                }

                const blob = await response.blob();
                const url = window.URL.createObjectURL(blob);
                const a = document.createElement('a');
                a.href = url;
                a.download = `batch_${selectedBatchId}_${Date.now()}.xlsx`;
                document.body.appendChild(a);
                a.click();
                window.URL.revokeObjectURL(url);
                document.body.removeChild(a);

                downloadBtn.innerHTML = '<span>✓</span><span>Downloaded!</span>';
                setTimeout(() => {
                    downloadBtn.innerHTML = '<span>📥</span><span>Download Excel</span>';
                    downloadBtn.disabled = false;
                }, 2000);

            } catch (error) {
                console.error('Error downloading:', error);
                alert('Failed to download: ' + error.message);
                downloadBtn.innerHTML = '<span>📥</span><span>Download Excel</span>';
                downloadBtn.disabled = false;
            }
        });

        document.querySelectorAll('.filter-btn').forEach(btn => {
            btn.addEventListener('click', () => {
                document.querySelectorAll('.filter-btn').forEach(b => b.classList.remove('active'));
                btn.classList.add('active');
                currentFilter = btn.dataset.filter;
                filterBatches();
            });
        });

        searchInput.addEventListener('input', filterBatches);

        loadBatches();
    </script>
</body>
</html>
    """
    return HTMLResponse(content=html_content)

@app.get("/debug/test-db")
def test_database():
    """Debug endpoint to test database connection"""
    import mysql.connector

    try:
        conn = mysql.connector.connect(
            host='localhost',
            user='bca_user',
            password='bca123456',
            database='bca_envelope'
        )

        cursor = conn.cursor(dictionary=True)
        cursor.execute("SELECT COUNT(*) as count FROM records")
        result = cursor.fetchone()

        cursor.execute("SELECT * FROM records LIMIT 3")
        samples = cursor.fetchall()

        cursor.close()
        conn.close()

        return {
            "status": "connected",
            "total_records": result['count'],
            "sample_records": samples
        }
    except Exception as e:
        return {"error": str(e)}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)