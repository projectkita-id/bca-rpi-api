import mysql.connector
from mysql.connector import Error
import json
from datetime import datetime

DB_CONFIG = {
    'host': 'localhost',
    'user': 'bca_user',
    'password': 'bca123456',
    'database': 'bca_envelope',
    'port': 3306
}

def get_db_connection():
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        if conn.is_connected():
            return conn
        return None
    except Error as e:
        print(f"Error connecting to MySQL: {e}")
        return None

def create_record(scanner_used: list, batch_code: str) -> int:
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
        return record_id
    except Error as e:
        conn.rollback()
        raise
    finally:
        cursor.close()
        conn.close()

def finish_record(record_id: int, items: list):
    conn = get_db_connection()
    if not conn:
        raise Exception("Database connection failed")
    cursor = conn.cursor()
    try:
        end_time = datetime.now()
        total_items = len(items)
        cursor.execute("""
            UPDATE records 
            SET end_time = %s, status = 'Completed', total_items = %s
            WHERE id = %s
        """, (end_time, total_items, record_id))
        for item in items:
            timestamp = datetime.fromisoformat(item.get("timestamp")) if item.get("timestamp") else datetime.now()
            cursor.execute("""
                INSERT INTO record_item (
                    record_id, item_id, timestamp,
                    scanner_1_value, scanner_1_valid,
                    scanner_2_value, scanner_2_valid,
                    scanner_3_value, scanner_3_valid
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                record_id, item.get("item_id"), timestamp,
                item.get("scanner_1_value"), 1 if item.get("scanner_1_valid") else 0,
                item.get("scanner_2_value"), 1 if item.get("scanner_2_valid") else 0,
                item.get("scanner_3_value"), 1 if item.get("scanner_3_valid") else 0
            ))
        conn.commit()
    except Error as e:
        conn.rollback()
        raise
    finally:
        cursor.close()
        conn.close()

def get_record(record_id: int) -> dict:
    conn = get_db_connection()
    if not conn:
        return None
    cursor = conn.cursor(dictionary=True)
    try:
        cursor.execute("SELECT * FROM records WHERE id = %s", (record_id,))
        row = cursor.fetchone()
        if row:
            if row.get('start_time'):
                row['start_time'] = row['start_time'].isoformat()
            if row.get('end_time') and row['end_time']:
                row['end_time'] = row['end_time'].isoformat()
            else:
                row['end_time'] = None
            if not isinstance(row.get('scanner_used'), str):
                row['scanner_used'] = json.dumps(row.get('scanner_used', []))
        return row
    except Error as e:
        return None
    finally:
        cursor.close()
        conn.close()

def get_record_items(record_id: int) -> list:
    conn = get_db_connection()
    if not conn:
        return []
    cursor = conn.cursor(dictionary=True)
    try:
        cursor.execute("SELECT * FROM record_item WHERE record_id = %s ORDER BY id ASC", (record_id,))
        rows = cursor.fetchall()
        items = []
        for row in rows:
            item = {"item_id": row.get("item_id"), "timestamp": row["timestamp"].isoformat() if row.get("timestamp") else None}
            if row.get("scanner_1_value"):
                item["scanner_1"] = {"value": row["scanner_1_value"], "valid": bool(row.get("scanner_1_valid"))}
            if row.get("scanner_2_value"):
                item["scanner_2"] = {"value": row["scanner_2_value"], "valid": bool(row.get("scanner_2_valid"))}
            if row.get("scanner_3_value"):
                item["scanner_3"] = {"value": row["scanner_3_value"], "valid": bool(row.get("scanner_3_valid"))}
            items.append(item)
        return items
    except Error as e:
        return []
    finally:
        cursor.close()
        conn.close()

def get_all_records(status_filter=None) -> list:
    conn = get_db_connection()
    if not conn:
        return []
    cursor = conn.cursor(dictionary=True)
    try:
        print(f"Fetching records with filter: {status_filter}")
        if status_filter:
            cursor.execute("SELECT * FROM records WHERE status = %s ORDER BY id DESC", (status_filter,))
        else:
            cursor.execute("SELECT * FROM records ORDER BY id DESC")
        rows = cursor.fetchall()
        print(f"Found {len(rows)} rows")
        if not rows:
            return []
        records = []
        for row in rows:
            try:
                cursor2 = conn.cursor(dictionary=True)
                cursor2.execute("SELECT COUNT(*) as item_count FROM record_item WHERE record_id = %s", (row['id'],))
                stats = cursor2.fetchone()
                cursor2.close()
                record = {
                    'id': row['id'],
                    'batch_code': row.get('batch_code', ''),
                    'status': row.get('status', ''),
                    'total_items': int(row.get('total_items', 0) or stats.get('item_count', 0) or 0),
                    'pass_count': 0,
                    'fail_count': 0,
                    'start_time': row['start_time'].isoformat() if row.get('start_time') else None,
                    'end_time': row['end_time'].isoformat() if row.get('end_time') else None,
                    'scanner_used': row.get('scanner_used') if isinstance(row.get('scanner_used'), str) else json.dumps(row.get('scanner_used', []))
                }
                records.append(record)
            except Exception as e:
                continue
        print(f"Returning {len(records)} records")
        return records
    except Error as e:
        print(f"MySQL Error: {e}")
        return []
    finally:
        cursor.close()
        conn.close()

try:
    test_conn = get_db_connection()
    if test_conn:
        print("MySQL models loaded - Connected to bca_envelope")
        test_conn.close()
except Exception as e:
    print(f"MySQL connection error: {e}")