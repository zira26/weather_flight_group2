WITH airports_reorder AS (
    SELECT country, region, faa, 
    name, lat, lon, alt, tz, dst, city
    FROM {{ref('selected_airports')}}
)
SELECT * FROM airports_reorder