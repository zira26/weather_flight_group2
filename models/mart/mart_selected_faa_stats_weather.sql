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
p.date,p.time,
p.hour,p.month_name,p.weekday,p.date_day,p.date_month,p.date_year,p.cw,p.day_part,
p.TEMP_C,
p.dewpoint_c,
p.humidity_perc,
p.PRECIPITATION_MM,
p.SNOW_MM,
p.WIND_DIRECTION,
p.WIND_SPEED_KMH,
p.WIND_PEAKGUST_KMH,
p.pressure_hpa,
p.condition_code,
CASE WHEN p.condition_code = 1 THEN 'Clear'
WHEN p.condition_code = 2 THEN 'Fair'
WHEN p.condition_code = 3 THEN 'Cloudy'
WHEN p.condition_code = 4 THEN 'Overcast'
WHEN p.condition_code = 5 THEN 'Fog'
WHEN p.condition_code = 6 THEN 'Freezing Fog'
WHEN p.condition_code = 7 THEN 'Light Rain'
WHEN p.condition_code = 8 THEN 'Rain'
WHEN p.condition_code = 9 THEN 'Heavy Rain'
WHEN p.condition_code = 10 THEN 'Freezing Rain'
WHEN p.condition_code = 11 THEN 'Heavy Freezing Rain'
WHEN p.condition_code = 12 THEN 'Sleet'
WHEN p.condition_code = 13 THEN 'Heavy Sleet'
WHEN p.condition_code = 14 THEN 'Light Snowfall'
WHEN p.condition_code = 15 THEN 'Snowfall'
WHEN p.condition_code = 16 THEN 'Heavy Snowfall'
WHEN p.condition_code = 17 THEN 'Rain Shower'
WHEN p.condition_code = 18 THEN 'Heavy Rain Shower'
WHEN p.condition_code = 19 THEN 'Sleet Shower'
WHEN p.condition_code = 20 THEN 'Heavy Sleet Shower'
WHEN p.condition_code = 21 THEN 'Snow Shower'
WHEN p.condition_code = 22 THEN 'Heavy Snow Shower'
WHEN p.condition_code = 23 THEN 'Lightning'
WHEN p.condition_code = 24 THEN 'Hail'
WHEN p.condition_code = 25 THEN 'Thunderstorm'
WHEN p.condition_code = 26 THEN 'Heavy Thunderstorm'
WHEN p.condition_code = 27 THEN 'Storm'
END AS weather_condition 
FROM {{ref('prep_weather_hourly')}} P
JOIN total_stats t on t.faa = p.AIRPORT_CODE and t.FLIGHT_DATE = p.date