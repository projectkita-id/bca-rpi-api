import mysql.connector
from mysql.connector import Error
import json
from datetime import datetime

# MySQL Configuration - SESUAIKAN DENGAN SETTING ANDA
DB_CONFIG = {
    'host': '127.0.0.1',
    'user': 'root',
    'password': '',  # Ganti jika ada password
    'database': 'bca_envelope',  # NAMA DATABASE ANDA
    'port': 3306
}


def get_db_connection():
    """Create MySQL database connection"""
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        if conn.is_connected():
            return conn
        return None
    except Error as e:
        print(f"❌ Error connecting to MySQL: {e}")
        return None


def create_record(scanner_used: list, batch_code: str) -> int:
    """Create new batch record"""
    conn = get_db_connection()
    if not conn:
        raise Exception("Database connection failed")
    
    cursor = conn.cursor()
    
    try:
        start_time = datetime.now()
        scanner_used_json = json.dumps(scanner_used)
        
        cursor.execute("""
            INSERT INTO records (batch_code, scanner_used, start_time, status, total_items)
            VALUES (%s, %s, %s, 'Running', 0)
        """, (batch_code, scanner_used_json, start_time))
        
        record_id = cursor.lastrowid
        conn.commit()
        
        print(f"✅ Record created: ID={record_id}, Batch={batch_code}")
        return record_id
        
    except Error as e:
        print(f"❌ Error creating record: {e}")
        conn.rollback()
        raise
    finally:
        cursor.close()
        conn.close()


def finish_record(record_id: int, items: list):
    """Finish record and insert all items"""
    conn = get_db_connection()
    if not conn:
        raise Exception("Database connection failed")
    
    cursor = conn.cursor()
    
    try:
        end_time = datetime.now()
        total_items = len(items)
        
        # Update record
        cursor.execute("""
            UPDATE records 
            SET end_time = %s, status = 'Completed', total_items = %s
            WHERE id = %s
        """, (end_time, total_items, record_id))
        
        # Insert items
        for item in items:
            timestamp = datetime.fromisoformat(item.get("timestamp")) if item.get("timestamp") else datetime.now()
            
            cursor.execute("""
                INSERT INTO record_item (
                    record_id, item_id, timestamp,
                    scanner_1_value, scanner_1_valid,
                    scanner_2_value, scanner_2_valid,
                    scanner_3_value, scanner_3_valid,
                    validation_result
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                record_id,
                item.get("item_id"),
                timestamp,
                item.get("scanner_1_value"),
                1 if item.get("scanner_1_valid") else 0,
                item.get("scanner_2_value"),
                1 if item.get("scanner_2_valid") else 0,
                item.get("scanner_3_value"),
                1 if item.get("scanner_3_valid") else 0,
                item.get("validation_result")
            ))
        
        conn.commit()
        print(f"✅ Record finished: ID={record_id}, Items={len(items)}")
        
    except Error as e:
        print(f"❌ Error finishing record: {e}")
        conn.rollback()
        raise
    finally:
        cursor.close()
        conn.close()


def get_record(record_id: int) -> dict:
    """Get single record by ID"""
    conn = get_db_connection()
    if not conn:
        return None
    
    cursor = conn.cursor(dictionary=True)
    
    try:
        cursor.execute("SELECT * FROM records WHERE id = %s", (record_id,))
        row = cursor.fetchone()
        
        if row:
            # Convert datetime to ISO string
            if row.get('start_time'):
                row['start_time'] = row['start_time'].isoformat()
            if row.get('end_time') and row['end_time']:
                row['end_time'] = row['end_time'].isoformat()
            else:
                row['end_time'] = None
            
            # Ensure scanner_used is string (JSON format)
            if isinstance(row.get('scanner_used'), str):
                pass  # Already string
            else:
                row['scanner_used'] = json.dumps(row.get('scanner_used', []))
        
        return row
        
    except Error as e:
        print(f"❌ Error getting record: {e}")
        return None
    finally:
        cursor.close()
        conn.close()


def get_record_items(record_id: int) -> list:
    """Get all items for a specific record"""
    conn = get_db_connection()
    if not conn:
        return []
    
    cursor = conn.cursor(dictionary=True)
    
    try:
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
            FROM record_item
            WHERE record_id = %s
            ORDER BY id ASC
        """, (record_id,))
        
        rows = cursor.fetchall()
        
        items = []
        for row in rows:
            item = {
                "item_id": row["item_id"],
                "timestamp": row["timestamp"].isoformat() if row.get("timestamp") else None,
                "validation_result": row.get("validation_result")
            }
            
            # Scanner 1
            if row.get("scanner_1_value"):
                item["scanner_1"] = {
                    "value": row["scanner_1_value"],
                    "valid": bool(row.get("scanner_1_valid"))
                }
            
            # Scanner 2
            if row.get("scanner_2_value"):
                item["scanner_2"] = {
                    "value": row["scanner_2_value"],
                    "valid": bool(row.get("scanner_2_valid"))
                }
            
            # Scanner 3
            if row.get("scanner_3_value"):
                item["scanner_3"] = {
                    "value": row["scanner_3_value"],
                    "valid": bool(row.get("scanner_3_valid"))
                }
            
            items.append(item)
        
        return items
        
    except Error as e:
        print(f"❌ Error getting items: {e}")
        return []
    finally:
        cursor.close()
        conn.close()


def get_all_records(status_filter=None) -> list:
    """Get all records from database with statistics"""
    conn = get_db_connection()
    if not conn:
        print("❌ No database connection")
        return []
    
    cursor = conn.cursor(dictionary=True)
    
    try:
        print(f"🔍 Fetching records from bca_envelope database (filter: {status_filter})")
        
        if status_filter:
            query = """
                SELECT 
                    r.id,
                    r.batch_code,
                    r.start_time,
                    r.end_time,
                    r.status,
                    r.scanner_used,
                    r.total_items,
                    COUNT(i.id) as item_count,
                    SUM(CASE WHEN i.validation_result = 'PASS' THEN 1 ELSE 0 END) as pass_count,
                    SUM(CASE WHEN i.validation_result = 'FAIL' THEN 1 ELSE 0 END) as fail_count
                FROM records r
                LEFT JOIN record_item i ON r.id = i.record_id
                WHERE r.status = %s
                GROUP BY r.id, r.batch_code, r.start_time, r.end_time, r.status, r.scanner_used, r.total_items
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
                    r.total_items,
                    COUNT(i.id) as item_count,
                    SUM(CASE WHEN i.validation_result = 'PASS' THEN 1 ELSE 0 END) as pass_count,
                    SUM(CASE WHEN i.validation_result = 'FAIL' THEN 1 ELSE 0 END) as fail_count
                FROM records r
                LEFT JOIN record_item i ON r.id = i.record_id
                GROUP BY r.id, r.batch_code, r.start_time, r.end_time, r.status, r.scanner_used, r.total_items
                ORDER BY r.id DESC
            """
            cursor.execute(query)
        
        rows = cursor.fetchall()
        print(f"✅ Found {len(rows)} records")
        
        records = []
        for row in rows:
            record = {}
            
            # Basic fields
            record['id'] = row['id']
            record['batch_code'] = row['batch_code']
            record['status'] = row['status']
            
            # DateTime conversion
            if row.get('start_time'):
                record['start_time'] = row['start_time'].isoformat()
            else:
                record['start_time'] = None
                
            if row.get('end_time'):
                record['end_time'] = row['end_time'].isoformat()
            else:
                record['end_time'] = None
            
            # Scanner used - ensure it's a string
            scanner_used = row.get('scanner_used')
            if isinstance(scanner_used, str):
                record['scanner_used'] = scanner_used
            elif isinstance(scanner_used, (list, dict)):
                record['scanner_used'] = json.dumps(scanner_used)
            else:
                record['scanner_used'] = '[]'
            
            # Statistics
            record['total_items'] = int(row.get('total_items') or row.get('item_count') or 0)
            record['pass_count'] = int(row.get('pass_count') or 0)
            record['fail_count'] = int(row.get('fail_count') or 0)
            
            records.append(record)
            
            print(f"  - Record #{record['id']}: {record['batch_code']} ({record['status']}) - {record['total_items']} items")
        
        return records
        
    except Error as e:
        print(f"❌ Error get_all_records: {e}")
        import traceback
        traceback.print_exc()
        return []
    finally:
        cursor.close()
        conn.close()


# Test connection on import
try:
    test_conn = get_db_connection()
    if test_conn:
        print("✅ MySQL models loaded - Connected to bca_envelope database")
        test_conn.close()
    else:
        print("❌ MySQL models loaded - Connection failed")
except Exception as e:
    print(f"❌ MySQL connection error: {e}")
