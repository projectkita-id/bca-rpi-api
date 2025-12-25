import mysql.connector
from mysql.connector import Error
import json
from datetime import datetime

# MySQL Configuration
DB_CONFIG = {
    'host': 'localhost',
    'user': 'bca_user',
    'password': 'bca123456',
    'database': 'bca_envelope',
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
        print(f"Error connecting to MySQL: {e}")
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
        
        print(f"Record created: ID={record_id}")
        return record_id
        
    except Error as e:
        print(f"Error creating record: {e}")
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
        
        # Update record status
        cursor.execute("""
            UPDATE records 
            SET end_time = %s, status = 'Completed', total_items = %s
            WHERE id = %s
        """, (end_time, total_items, record_id))
        
        # Insert items ke record_item
# Insert items ke record_item (SESUIKAN DENGAN DB ANDA)
        for item in items:
            cursor.execute("""
                INSERT INTO record_item (
                    record_id, item_id,
                    scanner_1, scanner_1_valid,
                    scanner_2, scanner_2_valid,
                    scanner_3, scanner_3_valid,
                    result, fallback, created_at
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
            """, (
                record_id,
                item.get("item_id"),

                # scanner_1
                (item.get("scanner_1") or {}).get("value") if isinstance(item.get("scanner_1"), dict) else item.get("scanner_1"),
                1 if ((item.get("scanner_1") or {}).get("valid") if isinstance(item.get("scanner_1"), dict) else item.get("scanner_1_valid")) else 0,

                # scanner_2
                (item.get("scanner_2") or {}).get("value") if isinstance(item.get("scanner_2"), dict) else item.get("scanner_2"),
                1 if ((item.get("scanner_2") or {}).get("valid") if isinstance(item.get("scanner_2"), dict) else item.get("scanner_2_valid")) else 0,

                # scanner_3
                (item.get("scanner_3") or {}).get("value") if isinstance(item.get("scanner_3"), dict) else item.get("scanner_3"),
                1 if ((item.get("scanner_3") or {}).get("valid") if isinstance(item.get("scanner_3"), dict) else item.get("scanner_3_valid")) else 0,

                # result & fallback (kalau belum ada, auto dihitung di bawah)
                item.get("result", "Unknown"),
                1 if item.get("fallback") else 0
            ))

        
        conn.commit()
        print(f"Record finished: ID={record_id}, Items={len(items)}")
        
    except Error as e:
        print(f"Error finishing record: {e}")
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
            
            # Ensure scanner_used is string
            if not isinstance(row.get('scanner_used'), str):
                row['scanner_used'] = json.dumps(row.get('scanner_used', []))
        
        return row
        
    except Error as e:
        print(f"Error getting record: {e}")
        return None
    finally:
        cursor.close()
        conn.close()


def get_record_items(record_id: int) -> list:
    """Get all items for a specific record (complete fields)"""
    conn = get_db_connection()
    if not conn:
        return []

    cursor = conn.cursor(dictionary=True)

    try:
        cursor.execute("""
            SELECT
                id, record_id, item_id,
                scanner_1, scanner_1_valid,
                scanner_2, scanner_2_valid,
                scanner_3, scanner_3_valid,
                result, fallback, created_at
            FROM record_item
            WHERE record_id = %s
            ORDER BY id ASC
        """, (record_id,))

        rows = cursor.fetchall() or []
        items = []

        for row in rows:
            items.append({
                "item_id": row.get("item_id"),

                # Format yang enak untuk FE + export
                "scanner_1": {"value": row.get("scanner_1"), "valid": (bool(row["scanner_1_valid"]) if row.get("scanner_1_valid") is not None else None)},
                "scanner_2": {"value": row.get("scanner_2"), "valid": (bool(row["scanner_2_valid"]) if row.get("scanner_2_valid") is not None else None)},
                "scanner_3": {"value": row.get("scanner_3"), "valid": (bool(row["scanner_3_valid"]) if row.get("scanner_3_valid") is not None else None)},

                "result": row.get("result"),
                "fallback": bool(row["fallback"]) if row.get("fallback") is not None else False,

                # timestamp jangan null: fallback ke created_at
                "timestamp": row["created_at"].isoformat() if row.get("created_at") else datetime.now().isoformat()
            })

        return items

    except Error as e:
        print(f"Error getting items: {e}")
        return []
    finally:
        cursor.close()
        conn.close()


def get_all_records(status_filter=None) -> list:
    """Get all records from database"""
    conn = get_db_connection()
    if not conn:
        print("No database connection")
        return []
    
    cursor = conn.cursor(dictionary=True)
    
    try:
        print(f"Fetching records with filter: {status_filter}")
        
        # Query records
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
                # Get item count for this record
                cursor2 = conn.cursor(dictionary=True)
                cursor2.execute("SELECT COUNT(*) as item_count FROM record_item WHERE record_id = %s", (row['id'],))
                stats = cursor2.fetchone()
                cursor2.close()
                
                # Build record dict
                record = {
                    'id': row['id'],
                    'batch_code': row.get('batch_code', ''),
                    'status': row.get('status', ''),
                    'total_items': int(row.get('total_items', 0) or stats.get('item_count', 0) or 0),
                    'pass_count': 0,
                    'fail_count': 0
                }
                
                # Handle datetime
                if row.get('start_time'):
                    record['start_time'] = row['start_time'].isoformat()
                else:
                    record['start_time'] = None
                
                if row.get('end_time'):
                    record['end_time'] = row['end_time'].isoformat()
                else:
                    record['end_time'] = None
                
                # Handle scanner_used
                scanner_used = row.get('scanner_used', '[]')
                if isinstance(scanner_used, str):
                    record['scanner_used'] = scanner_used
                else:
                    record['scanner_used'] = json.dumps(scanner_used)
                
                records.append(record)
                
            except Exception as row_error:
                print(f"Error processing row {row.get('id')}: {row_error}")
                continue
        
        print(f"Returning {len(records)} records")
        return records
        
    except Error as e:
        print(f"MySQL Error: {e}")
        import traceback
        traceback.print_exc()
        return []
    except Exception as e:
        print(f"Error: {e}")
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
        print("MySQL models loaded - Connected to bca_envelope")
        test_conn.close()
    else:
        print("MySQL models loaded - Connection failed")
except Exception as e:
    print(f"MySQL connection error: {e}")
