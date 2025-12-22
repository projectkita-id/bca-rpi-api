import os
import mysql.connector as db
from dotenv import load_dotenv

load_dotenv()

def get_db():
    return db.connect(
        host=os.getenv("DB_HOST", "127.0.0.1"),
        user=os.getenv("DB_USER", "root"),
        password=os.getenv("DB_PASS", ""),
        database=os.getenv("DB_NAME", "bca-envelope"),
        autocommit=False
    )