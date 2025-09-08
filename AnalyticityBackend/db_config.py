import psycopg2
from fastapi import HTTPException

DATABASES = {
    "brno": {
        "host": "timescaledb_brno",  # Docker container name
        "port": "5432",  # The port on which the database is exposed
        "user": "analyticity_brno",
        "password": "waze_admin",
        "dbname": "traffic_brno"
    },
    "jmk": {
        "host": "timescaledb_jmk",  # Docker container name
        "port": "5432",
        "user": "analyticity_jmk",
        "password": "waze_admin_jmk",
        "dbname": "traffic_jmk"
    },
    "orp_most": {
        "host": "timescaledb_orp_most",  # Docker container name
        "port": "5432",
        "user": "analyticity_orp_most",
        "password": "waze_admin_orp_most",
        "dbname": "traffic_orp_most"
    }
}


def get_db_connection(db_name: str):
    db = DATABASES.get(db_name)
    if not db:
        raise HTTPException(status_code=404, detail="Database not found")

    try:
        # Using psycopg2 to connect to the database using the container's name as the hostname
        connection = psycopg2.connect(
            host=db["host"],  # Use the Docker container name here
            port=db["port"],
            user=db["user"],
            password=db["password"],
            dbname=db["dbname"]
        )
        return connection
    except psycopg2.Error as e:
        raise HTTPException(status_code=500, detail=f"Database connection error: {e}")
