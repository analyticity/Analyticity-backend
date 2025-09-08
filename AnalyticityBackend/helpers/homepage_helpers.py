from typing import List

from constants.queries import QUERY_SUM_STATISTICS, QUERY_SUM_STATISTICS_WITH_STREETS, QUERY_SUM_STATISTICS_WITH_ROUTE
from helpers.universal_helpers import convert_utc_to_local


def fetch_sum_statistics(cursor, from_date, to_date):
    """
    Function returns global statistics for the whole area

    :param cursor:
    :param from_date:
    :param to_date:
    :return:
    """
    cursor.execute(QUERY_SUM_STATISTICS, (from_date, to_date))
    return cursor.fetchall()


def fetch_hourly_by_streets(cursor, from_date, to_date, streets: List[str]):
    """
    Function returns statistics for given list of streets

    :param cursor:
    :param from_date:
    :param to_date:
    :param streets:
    :return:
    """
    cursor.execute(
        QUERY_SUM_STATISTICS_WITH_STREETS,
        (from_date, to_date,  # for alerts subquery
         from_date, to_date, streets)  # for jams main query
    )
    return cursor.fetchall()


def fetch_hourly_by_route(cursor, from_date, to_date, route_coords):
    """
    Fetch traffic stats by spatial proximity to a given route.
    """
    linestring = "LINESTRING(" + ", ".join([f"{lon} {lat}" for lon, lat in route_coords]) + ")"

    params = (
        linestring, from_date, to_date,   # For alerts
        linestring, from_date, to_date,  # For jams
    )

    cursor.execute(QUERY_SUM_STATISTICS_WITH_ROUTE, params)
    return cursor.fetchall()


def transform_to_response_statistics(rows):
    """
    Function transforms the result to the format accepted by (original) FE application

    :param rows:
    :return:
    """
    data_jams = []
    data_alerts = []
    speedKMH = []
    delay = []
    level = []
    length = []
    xaxis = []

    print("First row data:", rows[0])

    if not rows:
        raise ValueError("Query returned no rows")

    else:
        for row in rows:
            data_jams.append(row["data_jams"])
            data_alerts.append(row["data_alerts"])
            speedKMH.append(row["speedkmh"])
            delay.append(row["delay"])
            level.append(row["level"])
            length.append(row["length"])
            xaxis.append(convert_utc_to_local(row["utc_time"]))

    return {
        "jams": data_jams,
        "alerts": data_alerts,
        "speedKMH": speedKMH,
        "delay": delay,
        "level": level,
        "length": length,
        "xaxis": xaxis
    }


def transform_to_response_statistics_v2(rows):
    """
    Transforms the query result into a structure that is more suitable for the frontend application.
    Each timestamp will have its own object containing all statistics.

    :param rows: List of rows returned from the database query
    :return: A dictionary of timestamped statistics
    """
    statistics = []

    for row in rows:
        timestamp = convert_utc_to_local(row["utc_time"])
        statistics.append({
            "timestamp": timestamp,
            "stats": {
                "jams": row["data_jams"],
                "alerts": row["data_alerts"],
                "speedKMH": row["speedkmh"],
                "delay": row["delay"],
                "level": row["level"],
                "length": row["length"]
            }
        })

    return statistics
