WITH source AS (
    SELECT *
    FROM {{ source('flights_data') }}
),
filtered AS (
    SELECT
        flight_date,
        dep_time,
        sched_dep_time,
        dep_delay,
        arr_time,
        sched_arr_time,
        arr_delay,
        airline,
        tail_number,
        flight_number,
        origin,
        dest,
        air_time,
        actual_elapsed_time,
        distance,
        cancelled,
        diverted
    FROM source
    WHERE origin IN ('ORD', 'DTW', 'DEN', 'JFK', 'ATL', 'LAX')
)
SELECT * 
FROM filtered
