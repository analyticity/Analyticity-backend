QUERY_SUM_STATISTICS = """
    SELECT 
        SUM(total_active_jams) AS data_jams,
        SUM(total_active_alerts) AS data_alerts,
        AVG(avg_speed_kmh)::FLOAT AS speedKMH,
        AVG(avg_delay)::FLOAT AS delay,
        AVG(avg_jam_level)::FLOAT AS level,
        AVG(avg_jam_length)::FLOAT AS length,
        stat_time AT TIME ZONE 'UTC' AS utc_time
    FROM sum_statistics
    WHERE stat_time >= %s AND stat_time < %s
    GROUP BY stat_time AT TIME ZONE 'UTC'
    ORDER BY utc_time
"""

QUERY_SUM_STATISTICS_WITH_STREETS = """
       SELECT 
            COUNT(*) AS data_jams,
            AVG(speed_kmh)::FLOAT AS speedKMH,
            AVG(delay)::FLOAT AS delay,
            AVG(jam_level)::FLOAT AS level,
            AVG(jam_length)::FLOAT AS length,
            published_at AT TIME ZONE 'UTC' AS utc_time,
            (
                SELECT COUNT(*) 
                FROM alerts 
                WHERE published_at >= %s AND published_at < %s
            ) AS data_alerts
        FROM jams
        WHERE published_at >= %s AND published_at < %s
          AND street = ANY(%s)
        GROUP BY utc_time
        ORDER BY utc_time
    """

QUERY_SUM_STATISTICS_WITH_ROUTE = """
        SELECT 
            COUNT(*) AS data_jams,
            AVG(speed_kmh)::FLOAT AS speedKMH,
            AVG(delay)::FLOAT AS delay,
            AVG(jam_level)::FLOAT AS level,
            AVG(jam_length)::FLOAT AS length,
            published_at AT TIME ZONE 'UTC' AS utc_time,
            (
                SELECT COUNT(*) 
                FROM alerts 
                WHERE ST_DWithin(
                          location::geography,
                          ST_GeomFromText(%s, 4326)::geography,
                          20
                      )
                  AND published_at >= %s AND published_at < %s
            ) AS data_alerts
        FROM jams
        WHERE ST_DWithin(
                  jam_line::geography,
                  ST_GeomFromText(%s, 4326)::geography,
                  20
              )
          AND published_at >= %s AND published_at < %s
        GROUP BY utc_time
        ORDER BY utc_time
    """

QUERY_ALERTS = """
    SELECT
        uuid,
        street,
        type,
        subtype,
        EXTRACT(EPOCH FROM published_at) * 1000 AS pubMillis,
        ST_X(location::geometry) AS longitude,
        ST_Y(location::geometry) AS latitude
        FROM alerts
        WHERE published_at BETWEEN %s AND %s;
"""

QUERY_ALERTS_WITH_STREETS = """
    SELECT
        uuid,
        street,
        type,
        subtype,
        EXTRACT(EPOCH FROM published_at) * 1000 AS pubMillis,
        ST_X(location::geometry) AS longitude,
        ST_Y(location::geometry) AS latitude
        FROM alerts
        WHERE published_at BETWEEN %s AND %s 
            AND street = ANY(%s);
"""

QUERY_ALERTS_WITH_ROUTE = """
    SELECT
        uuid,
        street,
        type,
        subtype,
        EXTRACT(EPOCH FROM published_at) * 1000 AS pubMillis,
        ST_X(location::geometry) AS longitude,
        ST_Y(location::geometry) AS latitude
    FROM alerts
    WHERE published_at BETWEEN %s AND %s 
        AND ST_DWithin(
                location::geography,
                ST_GeomFromText(%s, 4326)::geography,
                20
            );
"""

QUERY_JAMS = """
    SELECT
        uuid,
        street,
        ST_AsText(jam_line::geometry) AS wkt,
        jam_level,
        delay,
        published_at
    FROM
        jams
    WHERE published_at BETWEEN %s AND %s; 
"""

QUERY_TOP_N_STREETS = """
        SELECT street, COUNT(*)
        FROM %s
        WHERE published_at BETWEEN %s AND %s 
            AND street = ANY(%s); 
        GROUP BY street
        ORDER BY count DESC
        LIMIT %s;
"""

QUERY_TOP_N_ROUTE = """
        SELECT street, COUNT(*)
        FROM %s
        WHERE published_at BETWEEN %s AND %s 
            AND ST_DWithin(
                %s::geography,
                ST_GeomFromText(%s, 4326)::geography,
                20
            );
        GROUP BY street
        ORDER BY count DESC
        LIMIT %s;
"""