# import psycopg2
# from fastapi import HTTPException
#
# DATABASES = {
#     "brno": {
#         "host": "timescaledb_brno",  # Docker container name
#         "port": "5432",  # The port on which the database is exposed
#         "user": "analyticity_brno",
#         "password": "waze_admin",
#         "dbname": "traffic_brno"
#     },
#     "jmk": {
#         "host": "timescaledb_jmk",  # Docker container name
#         "port": "5432",
#         "user": "analyticity_jmk",
#         "password": "waze_admin_jmk",
#         "dbname": "traffic_jmk"
#     },
#     "orp_most": {
#         "host": "timescaledb_orp_most",  # Docker container name
#         "port": "5432",
#         "user": "analyticity_orp_most",
#         "password": "waze_admin_orp_most",
#         "dbname": "traffic_orp_most"
#     }
# }
#
#
# def get_db_connection(db_name: str):
#     db = DATABASES.get(db_name)
#     if not db:
#         raise HTTPException(status_code=404, detail="Database not found")
#
#     try:
#         # Using psycopg2 to connect to the database using the container's name as the hostname
#         connection = psycopg2.connect(
#             host=db["host"],
#             port=db["port"],
#             user=db["user"],
#             password=db["password"],
#             dbname=db["dbname"]
#         )
#         return connection
#     except psycopg2.Error as e:
#         raise HTTPException(status_code=500, detail=f"Database connection error: {e}")

import os
import psycopg2
from psycopg2 import OperationalError
from fastapi import HTTPException
import logging

# Use the same root logger as main.py so logs go to stdout and follow same format
logger = logging.getLogger("app.db")

###########################################################################################
# In production, load these from .env or environment variables instead of hardcoding
###########################################################################################

DATABASES = {
    "brno": {
        "host": os.getenv("DB_BRNO_HOST", "timescaledb_brno"),
        "port": os.getenv("DB_BRNO_PORT", "5432"),
        "user": os.getenv("DB_BRNO_USER", "analyticity_brno"),
        "password": os.getenv("DB_BRNO_PASSWORD", "waze_admin"),
        "dbname": os.getenv("DB_BRNO_NAME", "traffic_brno"),
    },
    "jmk": {
        "host": os.getenv("DB_JMK_HOST", "timescaledb_jmk"),
        "port": os.getenv("DB_JMK_PORT", "5432"),
        "user": os.getenv("DB_JMK_USER", "analyticity_jmk"),
        "password": os.getenv("DB_JMK_PASSWORD", "waze_admin_jmk"),
        "dbname": os.getenv("DB_JMK_NAME", "traffic_jmk"),
    },
    "orp_most": {
        "host": os.getenv("DB_ORP_MOST_HOST", "timescaledb_orp_most"),
        "port": os.getenv("DB_ORP_MOST_PORT", "5432"),
        "user": os.getenv("DB_ORP_MOST_USER", "analyticity_orp_most"),
        "password": os.getenv("DB_ORP_MOST_PASSWORD", "waze_admin_orp_most"),
        "dbname": os.getenv("DB_ORP_MOST_NAME", "traffic_orp_most"),
    },
}

###########################################################################################
# Connection helper with logging
###########################################################################################

def get_db_connection(db_name: str):
    """
    Retrieve a psycopg2 connection for the given database key.
    - Logs connection attempts and failures (without leaking credentials).
    - Raises HTTPException with meaningful status codes for FastAPI.
    """
    db = DATABASES.get(db_name)
    if not db:
        logger.warning(f"Database '{db_name}' not found in config")
        raise HTTPException(status_code=404, detail="Database not found")

    # Build a redacted DSN string for logs (never include password)
    safe_dsn = f"postgresql://{db['user']}@{db['host']}:{db['port']}/{db['dbname']}"

    try:
        logger.debug(f"Attempting DB connection: {safe_dsn}")
        connection = psycopg2.connect(
            host=db["host"],
            port=db["port"],
            user=db["user"],
            password=db["password"],
            dbname=db["dbname"],
            connect_timeout=5  # seconds
        )
        logger.info(f"✅ Connected to DB '{db_name}' at {safe_dsn}")
        return connection

    except OperationalError as e:
        # Common in Docker when DB container isn't ready or network issue
        logger.exception(f"❌ OperationalError connecting to {safe_dsn}: {e}")
        raise HTTPException(status_code=503, detail=f"Database '{db_name}' unavailable")

    except psycopg2.Error as e:
        # Other SQL-related connection errors
        logger.exception(f"❌ psycopg2 error connecting to {safe_dsn}: {e}")
        raise HTTPException(status_code=500, detail=f"Database connection error: {e}")

    except Exception as e:
        # Catch any unexpected issues
        logger.exception(f"❌ Unexpected error connecting to {safe_dsn}: {e}")
        raise HTTPException(status_code=500, detail=f"Unexpected DB connection error: {e}")
