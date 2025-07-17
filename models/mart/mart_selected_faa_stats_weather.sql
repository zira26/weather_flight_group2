WITH departure AS (
SELECT p.origin AS faa,
p.flight_date, 
COUNT(distinct DEST) AS numer_of_unique_dep_connections,
count (*) AS total_planed_dep,
sum(p.CANCELLED) AS total_canceled_dep,
sum(p.DIVERTED) AS total_diverted_dep,
count(p.ARR_TIME) AS total_actual_dep
FROM {{ref('prep_flights')}} p
GROUP BY p.origin, p.FLIGHT_DATE 
),
arrival AS (
SELECT p.dest AS faa,
p.flight_date,
COUNT(distinct p.origin) AS numer_of_unique_arr_connections,
count (*) AS total_planed_arr,
sum(p.CANCELLED) AS total_canceled_arr,
sum(p.DIVERTED) AS total_diverted_arr,
count(p.ARR_TIME) AS total_actual_arr
FROM {{ref('prep_flights')}} p
GROUP BY p.dest, p.FLIGHT_DATE
), 
total_stats AS (
SELECT *
FROM departure d
JOIN arrival a USING (faa,flight_date)
)
SELECT t.*,
p.MIN_TEMP_C,
p.MAX_TEMP_C,
p.PRECIPITATION_MM,
p.MAX_SNOW_MM,
p.AVG_WIND_DIRECTION,
p.AVG_WIND_SPEED_KMH,
p.WIND_PEAKGUST_KMH
FROM {{ref('prep_weather_hourly')}} P
JOIN total_stats t on t.faa = p.AIRPORT_CODE and t.FLIGHT_DATE = p.date