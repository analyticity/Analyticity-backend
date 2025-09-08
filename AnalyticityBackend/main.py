from fastapi import FastAPI, HTTPException
import psycopg2
from psycopg2.extras import RealDictCursor
from fastapi.middleware.cors import CORSMiddleware

from db_config import get_db_connection
from routers import homepage_endpoints, alerts_endpoints, jams_endpoints, plot_endpoints


############################################################################################

app = FastAPI()

app.include_router(homepage_endpoints.router)
app.include_router(alerts_endpoints.router)
app.include_router(jams_endpoints.router)
app.include_router(plot_endpoints.router)


app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"]
)


@app.get("/{name}/alerts/")
async def get_alerts(name: str):
    # Connect to the specific database
    connection = get_db_connection(name)

    try:
        cursor = connection.cursor(cursor_factory=RealDictCursor)

        # Define your SQL query to fetch alerts
        query = "SELECT * FROM alerts;"  # Assuming your alerts table is named 'alerts'

        # Execute the query
        cursor.execute(query)
        alerts = cursor.fetchall()

        if not alerts:
            raise HTTPException(status_code=404, detail="No alerts found")

        return {"alerts": alerts}

    except psycopg2.Error as e:
        raise HTTPException(status_code=500, detail=f"Query execution error: {e}")

    finally:
        # Always close the connection and cursor
        if connection:
            cursor.close()
            connection.close()
