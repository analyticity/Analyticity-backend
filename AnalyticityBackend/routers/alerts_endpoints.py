import psycopg2
from datetime import datetime, timedelta
from fastapi import APIRouter, HTTPException
from psycopg2.extras import RealDictCursor

from constants.queries import QUERY_ALERTS, QUERY_ALERTS_WITH_STREETS, QUERY_ALERTS_WITH_ROUTE
from db_config import get_db_connection
from models.request_models import PlotDataRequestBody

router = APIRouter(
    tags=["alerts"]
)


@router.post("/{name}/draw_alerts/")
async def get_all_alerts_for_drawing(name: str, body: PlotDataRequestBody):
    """
    Function returns alerts from waze
    :param name: which area to connect to
    :param body: One object, containing data about time interval (from date, to date) and list of streets or concrete route
    :return: Found points
   """
    connection = get_db_connection(name)
    try:
        cursor = connection.cursor(cursor_factory=RealDictCursor)

        from_date = datetime.strptime(body.from_date, "%Y-%m-%d")
        to_date = datetime.strptime(body.to_date, "%Y-%m-%d") + timedelta(days=1)  # to include entire day

        streets = body.streets or []
        route = body.route or []

        if not streets and not route:
            cursor.execute(QUERY_ALERTS, (from_date, to_date))
            response = cursor.fetchall()
        if streets and not route:
            cursor.execute(QUERY_ALERTS_WITH_STREETS, (from_date, to_date, streets))
            response = cursor.fetchall()
        if route:
            linestring = "LINESTRING(" + ", ".join([f"{lon} {lat}" for lon, lat in route]) + ")"
            cursor.execute(QUERY_ALERTS_WITH_ROUTE, (from_date, to_date, linestring))
            response = cursor.fetchall()

        if not response:
            raise HTTPException(status_code=404, detail="No data found for the selected parameters.")

        return response

    except psycopg2.Error as e:
        raise HTTPException(status_code=500, detail=f"Database query error: {e}")
    finally:
        if connection:
            cursor.close()
            connection.close()
