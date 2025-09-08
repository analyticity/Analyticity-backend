from datetime import datetime, timedelta
from typing import List

import psycopg2
from fastapi import APIRouter, HTTPException
from psycopg2.extras import RealDictCursor

from db_config import get_db_connection
from helpers.homepage_helpers import fetch_sum_statistics, fetch_hourly_by_streets, transform_to_response_statistics, \
    fetch_hourly_by_route, transform_to_response_statistics_v2
from models.request_models import PlotDataRequestBody
from models.response_models import StatsResponse

router = APIRouter(
    tags=["home"]
)


@router.post("/{name}/data_for_plot_drawer/")
async def get_data_for_plot_drawer(name: str, body: PlotDataRequestBody):
    """
    Function returns basic statistics about traffic situation

    :param name: Which area the data should be loaded
    :param body: One object, containing data about time interval (from date, to date) and list of streets or concrete route
    :return: Calculated statistics
    """
    connection = get_db_connection(name)

    try:
        cursor = connection.cursor(cursor_factory=RealDictCursor)

        from_date = datetime.strptime(body.from_date, "%Y-%m-%d")
        to_date = datetime.strptime(body.to_date, "%Y-%m-%d") + timedelta(days=1)  # to include entire day

        streets = body.streets or []
        route = body.route or []

        if not streets and not route:
            rows = fetch_sum_statistics(cursor, from_date, to_date)
        if streets and not route:
            rows = fetch_hourly_by_streets(cursor, from_date, to_date, streets)
        if route:
            rows = fetch_hourly_by_route(cursor, from_date, to_date, route)

        if not rows:
            raise HTTPException(status_code=404, detail="No data found for the selected parameters.")

        response = transform_to_response_statistics(rows)
        return response

    except psycopg2.Error as e:
        raise HTTPException(status_code=500, detail=f"Database query error: {e}")
    finally:
        if connection:
            cursor.close()
            connection.close()


@router.post("/v2/{name}/data_for_plot_drawer/", response_model=List[StatsResponse])
async def get_data_for_plot_drawer_v2(name: str, body: PlotDataRequestBody):
    """
    Function returns basic statistics about traffic situation

    :param name: Which area the data should be loaded
    :param body: One object, containing data about time interval (from date, to date) and list of streets or concrete route
    :return: Calculated statistics
    """
    connection = get_db_connection(name)

    try:
        cursor = connection.cursor(cursor_factory=RealDictCursor)

        from_date = datetime.strptime(body.from_date, "%Y-%m-%d")
        to_date = datetime.strptime(body.to_date, "%Y-%m-%d") + timedelta(days=1)  # to include entire day

        streets = body.streets or []
        route = body.route or []

        if not streets and not route:
            rows = fetch_sum_statistics(cursor, from_date, to_date)
        if streets and not route:
            rows = fetch_hourly_by_streets(cursor, from_date, to_date, streets)
        if route:
            rows = fetch_hourly_by_route(cursor, from_date, to_date, route)

        if not rows:
            raise HTTPException(status_code=404, detail="No data found for the selected parameters.")

        response = transform_to_response_statistics_v2(rows)
        return response

    except psycopg2.Error as e:
        raise HTTPException(status_code=500, detail=f"Database query error: {e}")
    finally:
        if connection:
            cursor.close()
            connection.close()