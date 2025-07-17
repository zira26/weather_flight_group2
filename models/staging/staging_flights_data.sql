WITH source AS (
    SELECT *
    FROM {{ source('flights_data') }}
),
filtered AS (

    SELECT
        fl_date,
        op_unique_carrier,
        tail_num,
        origin,
        dest,
        dep_time,
        dep_delay,
        arr_time,
        arr_delay,
        cancelled,
        cancellation_code,
        air_time,
        distance,
        carrier_delay,
        weather_delay,
        nas_delay,
        security_delay,
        late_aircraft_delay
    FROM source
    WHERE origin IN ('ORD', 'DTW', 'DEN', 'JFK', 'ATL', 'LAX')
)
SELECT * 
FROM filtered