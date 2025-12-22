import os
from dotenv import load_dotenv

load_dotenv()

DB_CONFIG = {
    "host": os.getenv("DATABASE_HOST"),
    "user": os.getenv("DATABASE_USER"),
    "pass": os.getenv("DATABASE_PASS"),
    "db_name": os.getenv("DATABASE_NAME")
}

SERVER_HOST = "0.0.0.0"
SERVER_POST = 5000