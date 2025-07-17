WITH u_dep_c AS (
select origin, COUNT(distinct DEST) AS numer_of_unique_dep_connections,
 count (*) AS total_planed_dep
FROM {{ref('prep_flights')}}
GROUP BY origin
),
u_arr_c AS (
SELECT dest, COUNT(distinct origin) AS numer_of_unique_arr_connections,
count (*) AS total_planed_arr
FROM {{ref('prep_flights')}}
GROUP BY dest
),
tot_cflights_dep AS (
SELECT origin, count (*) AS total_canceled_dep
FROM  {{ref('prep_flights')}}
WHERE CANCELLED = 1
GROUP BY origin 
),
tot_cflights_arr AS (
SELECT dest, count (*) AS total_canceled_arr
FROM  {{ref('prep_flights')}}
WHERE CANCELLED = 1
GROUP BY dest
),
tot_dflights_dep AS (
SELECT origin,count (*) AS total_diverted_dep
FROM  {{ref('prep_flights')}} 
WHERE diverted = 1
GROUP BY origin 
),
tot_dflights_arr AS (
SELECT dest,count (*) AS total_diverted_arr
FROM  {{ref('prep_flights')}} 
WHERE diverted = 1
GROUP BY dest
),
tot_flights_dep AS (
SELECT origin, count (*) AS total_actual_dep
FROM  {{ref('prep_flights')}} 
WHERE CANCELLED = 0
GROUP BY origin 
),
tot_flights_arr AS (
SELECT dest, count (*) AS total_actual_arr
FROM {{ref('prep_flights')}} 
WHERE CANCELLED = 0
GROUP BY dest
)
SELECT i.faa, i.CITY, i.country,
a.NUMER_OF_UNIQUE_DEP_CONNECTIONS,
b.NUMER_OF_UNIQUE_ARR_CONNECTIONS,
a.TOTAL_PLANED_DEP,
b.TOTAL_PLANED_ARR, 
c.TOTAL_CANCELED_DEP,
d.TOTAL_CANCELED_arr,
e.total_diverted_dep,
f.total_diverted_arr,
g.total_actual_dep,
h.total_actual_arr
FROM u_dep_c a
INNER JOIN u_arr_c b ON a.origin = b.DEST
INNER JOIN tot_cflights_dep c USING (origin)
INNER JOIN tot_cflights_arr d ON a.origin = d.dest
INNER JOIN tot_dflights_dep e USING (origin)
INNER JOIN tot_dflights_arr f ON a.origin = f.DEST
INNER JOIN tot_flights_dep g USING (origin)
INNER JOIN tot_flights_arr h ON a.origin = h.dest
INNER JOIN {{ref('prep_airports')}} i ON a.origin = i.faa