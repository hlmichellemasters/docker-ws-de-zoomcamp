WITH green_tripdata AS (
    SELECT
        'green'                               AS service_type,
        vendor_id,
        rate_code_id,
        pu_location_id       AS pickup_location_id,
        do_location_id       AS dropoff_location_id,
        pickup_datetime,
        dropoff_datetime,
        store_and_fwd_flag,
        passenger_count,
        trip_distance,
        fare_amount,
        total_amount
    FROM {{ ref('stg_green_tripdata') }}
),

yellow_tripdata AS (
    SELECT
        'yellow'                              AS service_type,
        vendor_id,
        rate_code_id,
        pu_location_id       AS pickup_location_id,
        do_location_id       AS dropoff_location_id,
        pickup_datetime,
        dropoff_datetime,
        store_and_fwd_flag,
        passenger_count,
        trip_distance,
        fare_amount,
        total_amount
    FROM {{ ref('stg_yellow_tripdata') }}
)

SELECT * FROM green_tripdata
UNION ALL
SELECT * FROM yellow_tripdata
