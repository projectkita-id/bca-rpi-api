import json
from db2 import get_db
from datetime import datetime
def get_all_records(status_filter=None):
    """Get all records from database"""
    import sqlite3
    
    conn = sqlite3.connect("scanner.db")
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    
    if status_filter:
        query = "SELECT * FROM records WHERE status = ? ORDER BY id DESC"
        cursor.execute(query, (status_filter,))
    else:
        query = "SELECT * FROM records ORDER BY id DESC"
        cursor.execute(query)
    
    rows = cursor.fetchall()
    conn.close()
    
    return [dict(row) for row in rows]

def create_record(scanner_used: list[int], batch_code: str | None = None):
    check_tables_exist()

    db = get_db()
    cursor = db.cursor()

    cursor.execute("""
        INSERT INTO records (batch_code, scanner_used, start_time, status)
        VALUES (%s, %s, %s, 'Running')
    """, (
        batch_code,
        json.dumps(scanner_used),
        datetime.now()
    ))

    db.commit()
    record_id = cursor.lastrowid 

    cursor.close()
    db.close()
    return record_id

def finish_record(record_id, items): 
    check_tables_exist()
    
    db = get_db()
    cursor = db.cursor()

    for item in items:
        cursor.execute("""
            INSERT INTO record_item (
                item_id,
                record_id,
                scanner_1,
                scanner_1_valid,
                scanner_2,
                scanner_2_valid,
                scanner_3,
                scanner_3_valid,
                result,
                fallback
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            item["item_id"],
            record_id,
            item["scanner_1"],
            item["scanner_1_valid"],
            item["scanner_2"],
            item["scanner_2_valid"],
            item["scanner_3"],
            item["scanner_3_valid"],
            item["result"],
            item["fallback"]
        ))

    cursor.execute("""
        UPDATE records
        SET end_time=%s,
            total_items=%s,
            status='Completed'
        WHERE id=%s;
    """, (datetime.now(), len(items), record_id))

    db.commit()
    cursor.close()
    db.close()

def get_record(record_id: int):
    db = get_db()
    cursor = db.cursor(dictionary=True)

    cursor.execute("""
        SELECT id, scanner_used, status
        FROM records
        WHERE id = %s
    """, (record_id,))

    record = cursor.fetchone()

    cursor.close()
    db.close()

    return record

def get_record_items(record_id: int):
    db = get_db()
    cursor = db.cursor(dictionary=True)

    cursor.execute("""
        SELECT
            item_id,
            scanner_1,
            scanner_1_valid,
            scanner_2,
            scanner_2_valid,
            scanner_3,
            scanner_3_valid,
            result,
            fallback,
            created_at
        FROM record_item
        WHERE record_id = %s
        ORDER BY id ASC
    """, (record_id,))

    items = cursor.fetchall()

    cursor.close()
    db.close()
    return items


def check_tables_exist():
    db = get_db()
    cursor = db.cursor()

    # records table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS records (
            id BIGINT AUTO_INCREMENT PRIMARY KEY,
            batch_code VARCHAR(50) UNIQUE,
            scanner_used JSON NOT NULL,
            start_time DATETIME,
            end_time DATETIME,
            total_items INT DEFAULT 0,
            status ENUM('Running','Completed','Failed'),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # record_item table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS record_item (
            id BIGINT AUTO_INCREMENT PRIMARY KEY,
            item_id BIGINT,
            record_id BIGINT,
                   
            scanner_1 VARCHAR(20),
            scanner_1_valid BOOLEAN,
                   
            scanner_2 VARCHAR(50),
            scanner_2_valid BOOLEAN,
                   
            scanner_3 VARCHAR(20),
            scanner_3_valid BOOLEAN,
                   
            result ENUM('Pass', 'Fail', 'Unknown'),\
            fallback BOOLEAN DEFAULT FALSE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                   
            FOREIGN KEY (record_id) REFERENCES records(id) ON DELETE CASCADE
        )
    """)

    db.commit()
    cursor.close()
    db.close()