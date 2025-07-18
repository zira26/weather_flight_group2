SELECT origin, p2.city AS origin_city, p2.country AS origin_country,
p.FLIGHT_DATE, date_trunc('hour',p.SCHED_DEP_TIME) AS hour_of_sched_dep,
dest, p3.city AS dest_city, p3.country AS dest_country, 
count(*) AS total_planed_flights_on_rout,
count(DISTINCT p.AIRLINE) AS unique_airlines_on_rout,
count(DISTINCT p.TAIL_NUMBER) AS unique_planes_on_rout,
avg(p.actual_elapsed_time_interval) AS avg_actual_elapsed_time_on_rout,
--round(avg(p.actual_elapsed_time),1)*'1 minute'::interval AS avg_actual_elapsed_time_on_rout,
avg(p.arr_delay_interval) AS avg_arr_delay,
--round(avg(p.arr_delay),1)*'1 minute'::interval AS avg_arr_delay,
max(p.ARR_DELAY_INTERVAL) AS max_arr_delay,
min(p.ARR_DELAY_INTERVAL) AS min_arr_delay,
sum(p.cancelled) AS total_canceled_on_rout,
sum(p.DIVERTED) AS total_diverted_on_rout 
FROM {{ref('prep_flights')}} p
INNER JOIN {{source('flights','selected_airports')}} P2 ON p2.faa = p.origin 
INNER JOIN {{source('flights','selected_airports')}} P3 ON p3.faa = p.dest
GROUP BY origin, dest,p2.city,p2.country,p3.city,p3.country