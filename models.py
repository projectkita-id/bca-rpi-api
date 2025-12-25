import mysql.connector
from mysql.connector import Error
import json
from datetime import datetime


DB_CONFIG = {
    "host": "localhost",
    "user": "bca_user",
    "password": "bca123456",
    "database": "bca_envelope",
    "port": 3306,
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

        cursor.execute(
            """
            INSERT INTO records (batch_code, scanner_used, start_time, status, total_items)
            VALUES (%s, %s, %s, 'Running', 0)
            """,
            (batch_code, scanner_used_json, start_time),
        )

        record_id = cursor.lastrowid
        conn.commit()
        return record_id

    except Error as e:
        print(f"Error creating record: {e}")
        conn.rollback()
        raise
    finally:
        cursor.close()
        conn.close()


def finish_record(record_id: int, items: list):
    """
    Finish record:
    - update records.end_time, status=Completed, total_items
    - insert items into record_item using correct column names
    """
    conn = get_db_connection()
    if not conn:
        raise Exception("Database connection failed")

    cursor = conn.cursor()
    try:
        end_time = datetime.now()
        total_items = len(items)

        cursor.execute(
            """
            UPDATE records
            SET end_time = %s, status = 'Completed', total_items = %s
            WHERE id = %s
            """,
            (end_time, total_items, record_id),
        )

        for item in items:
            # normalized item biasanya format:
            # {
            #   item_id,
            #   scanner_1: {value, valid}, scanner_2: {value, valid}, scanner_3: {value, valid},
            #   result, fallback
            # }
            s1 = item.get("scanner_1") if isinstance(item.get("scanner_1"), dict) else {}
            s2 = item.get("scanner_2") if isinstance(item.get("scanner_2"), dict) else {}
            s3 = item.get("scanner_3") if isinstance(item.get("scanner_3"), dict) else {}

            scanner_1_val = s1.get("value") if isinstance(s1, dict) else item.get("scanner_1")
            scanner_2_val = s2.get("value") if isinstance(s2, dict) else item.get("scanner_2")
            scanner_3_val = s3.get("value") if isinstance(s3, dict) else item.get("scanner_3")

            scanner_1_ok = s1.get("valid") if isinstance(s1, dict) else item.get("scanner_1_valid")
            scanner_2_ok = s2.get("valid") if isinstance(s2, dict) else item.get("scanner_2_valid")
            scanner_3_ok = s3.get("valid") if isinstance(s3, dict) else item.get("scanner_3_valid")

            result = item.get("result", "Unknown")
            fallback = 1 if item.get("fallback") else 0

            cursor.execute(
                """
                INSERT INTO record_item (
                    record_id, item_id,
                    scanner_1, scanner_1_valid,
                    scanner_2, scanner_2_valid,
                    scanner_3, scanner_3_valid,
                    result, fallback, created_at
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
                """,
                (
                    record_id,
                    item.get("item_id"),
                    scanner_1_val,
                    1 if scanner_1_ok is True else 0 if scanner_1_ok is False else None,
                    scanner_2_val,
                    1 if scanner_2_ok is True else 0 if scanner_2_ok is False else None,
                    scanner_3_val,
                    1 if scanner_3_ok is True else 0 if scanner_3_ok is False else None,
                    result,
                    fallback,
                ),
            )

        conn.commit()

    except Error as e:
        print(f"Error finishing record: {e}")
        conn.rollback()
        raise
    finally:
        cursor.close()
        conn.close()


def get_record(record_id: int) -> dict | None:
    """Get single record by ID"""
    conn = get_db_connection()
    if not conn:
        return None

    cursor = conn.cursor(dictionary=True)
    try:
        cursor.execute("SELECT * FROM records WHERE id = %s", (record_id,))
        row = cursor.fetchone()

        if row:
            if row.get("start_time"):
                row["start_time"] = row["start_time"].isoformat()
            if row.get("end_time"):
                row["end_time"] = row["end_time"].isoformat()
            else:
                row["end_time"] = None

            if not isinstance(row.get("scanner_used"), str):
                row["scanner_used"] = json.dumps(row.get("scanner_used", []))

        return row

    except Error as e:
        print(f"Error getting record: {e}")
        return None
    finally:
        cursor.close()
        conn.close()


def get_record_items(record_id: int) -> list:
    """
    Get items for a record_id.
    Output flat fields for export:
      item_id, scanner_1, scanner_2, scanner_3, result, timestamp
    """
    conn = get_db_connection()
    if not conn:
        return []

    cursor = conn.cursor(dictionary=True)
    try:
        cursor.execute(
            """
            SELECT
                id, record_id, item_id,
                scanner_1, scanner_1_valid,
                scanner_2, scanner_2_valid,
                scanner_3, scanner_3_valid,
                result, fallback, created_at
            FROM record_item
            WHERE record_id = %s
            ORDER BY id ASC
            """,
            (record_id,),
        )

        rows = cursor.fetchall() or []
        items = []
        for r in rows:
            items.append(
                {
                    "item_id": r.get("item_id"),
                    "scanner_1": r.get("scanner_1"),
                    "scanner_2": r.get("scanner_2"),
                    "scanner_3": r.get("scanner_3"),
                    "result": r.get("result") or "Unknown",
                    "timestamp": r["created_at"].isoformat() if r.get("created_at") else None,
                }
            )

        return items

    except Error as e:
        print(f"Error getting items: {e}")
        return []
    finally:
        cursor.close()
        conn.close()


def get_all_records(status_filter=None) -> list:
    """Get all records for /batch/list"""
    conn = get_db_connection()
    if not conn:
        return []

    cursor = conn.cursor(dictionary=True)
    try:
        if status_filter:
            cursor.execute(
                "SELECT * FROM records WHERE status = %s ORDER BY id DESC",
                (status_filter,),
            )
        else:
            cursor.execute("SELECT * FROM records ORDER BY id DESC")

        rows = cursor.fetchall() or []
        records = []

        for row in rows:
            # item count + pass/fail stats
            cursor2 = conn.cursor(dictionary=True)
            cursor2.execute(
                """
                SELECT
                  COUNT(*) AS total,
                  SUM(CASE WHEN result='Pass' THEN 1 ELSE 0 END) AS pass_count,
                  SUM(CASE WHEN result='Fail' THEN 1 ELSE 0 END) AS fail_count
                FROM record_item
                WHERE record_id = %s
                """,
                (row["id"],),
            )
            stats = cursor2.fetchone() or {"total": 0, "pass_count": 0, "fail_count": 0}
            cursor2.close()

            rec = {
                "id": row["id"],
                "batch_code": row.get("batch_code", ""),
                "status": row.get("status", ""),
                "total_items": int(stats["total"] or 0),
                "pass_count": int(stats["pass_count"] or 0),
                "fail_count": int(stats["fail_count"] or 0),
                "start_time": row["start_time"].isoformat() if row.get("start_time") else None,
                "end_time": row["end_time"].isoformat() if row.get("end_time") else None,
                "scanner_used": row.get("scanner_used") if isinstance(row.get("scanner_used"), str) else json.dumps(row.get("scanner_used", [])),
            }
            records.append(rec)

        return records

    except Error as e:
        print(f"MySQL Error: {e}")
        return []
    finally:
        cursor.close()
        conn.close()
