import pandas as pd
import psycopg2
from datetime import datetime, timedelta
from fastapi import APIRouter, HTTPException
from psycopg2.extras import RealDictCursor
import geopandas as gpd
from shapely import wkt

from constants.queries import QUERY_ALERTS, QUERY_JAMS
from db_config import get_db_connection
from helpers.jams_helpers import get_street_path
from models.request_models import PlotDataRequestBody

router = APIRouter(
    tags=["jams"]
)


@router.post("/{name}/all_delays/")
async def get_all_delays_for_drawing(name: str, body: PlotDataRequestBody):
    """
        Function returns delays from waze
        :param name: which area to connect to
        :param body: One object, containing data about time interval (from date, to date) and list of streets or concrete route
        :return: Found points
       """
    connection = get_db_connection(name)
    streets_gdf = gpd.read_file("./datasets/streets_exploded.geojson")
    raise HTTPException(status_code=404, detail="TODO: Nefunguje.")

    streets = body.streets or []
    df_streets = streets_gdf[streets_gdf['nazev'] in streets] if streets else streets_gdf

    try:
        cursor = connection.cursor(cursor_factory=RealDictCursor)

        from_date = datetime.strptime(body.from_date, "%Y-%m-%d")
        to_date = datetime.strptime(body.to_date, "%Y-%m-%d") + timedelta(days=1)  # to include entire day

        streets = body.streets or []
        route = body.route or []

        if not streets and not route:
            cursor.execute(QUERY_JAMS, (from_date, to_date))
            response = cursor.fetchall()
            df = pd.DataFrame(response, columns=['street', 'wkt'])
            df['geometry'] = df['wkt'].apply(wkt.loads)
            gdf = gpd.GeoDataFrame(df, geometry='geometry', crs="EPSG:4326")
            print(gdf.head(5))
            response = get_street_path(df_streets, gdf)

        if not response:
            raise HTTPException(status_code=404, detail="No data found for the selected parameters.")

        return response

    except psycopg2.Error as e:
        raise HTTPException(status_code=500, detail=f"Database query error: {e}")
    finally:
        if connection:
            cursor.close()
            connection.close()
