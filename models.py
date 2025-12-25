import sqlite3
import json
from datetime import datetime


def get_db_connection():
    """Create database connection with row factory"""
    conn = sqlite3.connect("scanner.db")
    conn.row_factory = sqlite3.Row
    return conn


def init_database():
    """Initialize database tables if not exists"""
    conn = sqlite3.connect("scanner.db")
    cursor = conn.cursor()
    
    # Create records table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS records (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            batch_code TEXT,
            scanner_used TEXT,
            start_time TEXT,
            end_time TEXT,
            status TEXT DEFAULT 'Running'
        )
    """)
    
    # Create items table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS items (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            record_id INTEGER,
            item_id INTEGER,
            timestamp TEXT,
            scanner_1_value TEXT,
            scanner_1_valid INTEGER,
            scanner_2_value TEXT,
            scanner_2_valid INTEGER,
            scanner_3_value TEXT,
            scanner_3_valid INTEGER,
            validation_result TEXT,
            FOREIGN KEY (record_id) REFERENCES records(id)
        )
    """)
    
    conn.commit()
    conn.close()
    print("✅ Database initialized")


def create_record(scanner_used: list, batch_code: str) -> int:
    """Create new batch record"""
    conn = sqlite3.connect("scanner.db")
    cursor = conn.cursor()
    
    start_time = datetime.now().isoformat()
    scanner_used_json = json.dumps(scanner_used)
    
    cursor.execute("""
        INSERT INTO records (batch_code, scanner_used, start_time, status)
        VALUES (?, ?, ?, 'Running')
    """, (batch_code, scanner_used_json, start_time))
    
    record_id = cursor.lastrowid
    conn.commit()
    conn.close()
    
    print(f"✅ Record created: ID={record_id}, Batch={batch_code}")
    return record_id


def finish_record(record_id: int, items: list):
    """Finish record and insert all items"""
    conn = sqlite3.connect("scanner.db")
    cursor = conn.cursor()
    
    # Update record status
    end_time = datetime.now().isoformat()
    cursor.execute("""
        UPDATE records 
        SET end_time = ?, status = 'Completed'
        WHERE id = ?
    """, (end_time, record_id))
    
    # Insert all items
    for item in items:
        cursor.execute("""
            INSERT INTO items (
                record_id, item_id, timestamp,
                scanner_1_value, scanner_1_valid,
                scanner_2_value, scanner_2_valid,
                scanner_3_value, scanner_3_valid,
                validation_result
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            record_id,
            item.get("item_id"),
            item.get("timestamp"),
            item.get("scanner_1_value"),
            item.get("scanner_1_valid"),
            item.get("scanner_2_value"),
            item.get("scanner_2_valid"),
            item.get("scanner_3_value"),
            item.get("scanner_3_valid"),
            item.get("validation_result")
        ))
    
    conn.commit()
    conn.close()
    
    print(f"✅ Record finished: ID={record_id}, Items={len(items)}")


def get_record(record_id: int) -> dict:
    """Get single record by ID"""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    cursor.execute("SELECT * FROM records WHERE id = ?", (record_id,))
    row = cursor.fetchone()
    conn.close()
    
    if row:
        return dict(row)
    return None


def get_record_items(record_id: int) -> list:
    """Get all items for a specific record"""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT 
            item_id,
            timestamp,
            scanner_1_value,
            scanner_1_valid,
            scanner_2_value,
            scanner_2_valid,
            scanner_3_value,
            scanner_3_valid,
            validation_result
        FROM items 
        WHERE record_id = ?
        ORDER BY id ASC
    """, (record_id,))
    
    rows = cursor.fetchall()
    conn.close()
    
    items = []
    for row in rows:
        item = {
            "item_id": row["item_id"],
            "timestamp": row["timestamp"],
            "validation_result": row["validation_result"]
        }
        
        # Scanner 1
        if row["scanner_1_value"]:
            item["scanner_1"] = {
                "value": row["scanner_1_value"],
                "valid": bool(row["scanner_1_valid"])
            }
        
        # Scanner 2
        if row["scanner_2_value"]:
            item["scanner_2"] = {
                "value": row["scanner_2_value"],
                "valid": bool(row["scanner_2_valid"])
            }
        
        # Scanner 3
        if row["scanner_3_value"]:
            item["scanner_3"] = {
                "value": row["scanner_3_value"],
                "valid": bool(row["scanner_3_valid"])
            }
        
        items.append(item)
    
    return items


def get_all_records(status_filter=None) -> list:
    """Get all records from database with statistics"""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        if status_filter:
            query = """
                SELECT 
                    r.id,
                    r.batch_code,
                    r.start_time,
                    r.end_time,
                    r.status,
                    r.scanner_used,
                    COUNT(i.id) as total_items,
                    SUM(CASE WHEN i.validation_result = 'PASS' THEN 1 ELSE 0 END) as pass_count,
                    SUM(CASE WHEN i.validation_result = 'FAIL' THEN 1 ELSE 0 END) as fail_count
                FROM records r
                LEFT JOIN items i ON r.id = i.record_id
                WHERE r.status = ?
                GROUP BY r.id
                ORDER BY r.id DESC
            """
            cursor.execute(query, (status_filter,))
        else:
            query = """
                SELECT 
                    r.id,
                    r.batch_code,
                    r.start_time,
                    r.end_time,
                    r.status,
                    r.scanner_used,
                    COUNT(i.id) as total_items,
                    SUM(CASE WHEN i.validation_result = 'PASS' THEN 1 ELSE 0 END) as pass_count,
                    SUM(CASE WHEN i.validation_result = 'FAIL' THEN 1 ELSE 0 END) as fail_count
                FROM records r
                LEFT JOIN items i ON r.id = i.record_id
                GROUP BY r.id
                ORDER BY r.id DESC
            """
            cursor.execute(query)
        
        rows = cursor.fetchall()
        conn.close()
        
        return [dict(row) for row in rows]
    
    except Exception as e:
        print(f"❌ Error get_all_records: {e}")
        conn.close()
        return []


# Initialize database on import
init_database()
