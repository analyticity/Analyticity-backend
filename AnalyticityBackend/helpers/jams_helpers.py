import numpy as np


GREEN_COLOR = 2
ORANGE_COLOR = 5


def assign_color(df, num_days=7):
    df['color'] = np.select(
        [
            df['count'] < GREEN_COLOR * num_days,
            (df['count'] >= GREEN_COLOR * num_days) & (df['count'] <= ORANGE_COLOR * num_days),
            df['count'] > ORANGE_COLOR * num_days
        ],
        ['green', 'orange', 'red'],
        default='green'
    )
    return df


def count_delays_by_parts(street_gdf, jam_data):
    for index, row in street_gdf.iterrows():
        street_name = row['nazev']
        geom = row['geometry']
        relevant_jams = jam_data[jam_data['street'] == street_name]

        count = sum(geom.intersects(jam_geom) for jam_geom in relevant_jams['geometry'])
        street_gdf.at[index, 'count'] = count

    if street_gdf.empty:
        jam_data['count'] = 0
        return assign_color(jam_data)
    else:
        return assign_color(street_gdf)


def get_street_path(street_gdf, jams_gdf, street=None, from_time=None, to_time=None):
    df_streets = street_gdf[street_gdf['nazev'] == street] if street else street_gdf

    df_count = count_delays_by_parts(df_streets, jams_gdf)

    street_geometry_dict = []
    for _, row in df_count.iterrows():
        coords = list(row['geometry'].coords)
        final_dict = {
            'street_name': row['nazev'],
            'path': [[lon, lat] for lon, lat in coords],
            'color': row['color']
        }
        street_geometry_dict += [final_dict]

    return street_geometry_dict