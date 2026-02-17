SELECT
    -- identifiers
    CAST(vendorid AS INT64)                 AS vendor_id,
    CAST(ratecodeid AS INT64)               AS rate_code_id,
    CAST(pulocationid AS INT64)              AS pu_location_id,
    CAST(dolocationid AS INT64)              AS do_location_id,

    -- timestamps
    CAST(tpep_pickup_datetime AS TIMESTAMP)  AS pickup_datetime,
    CAST(tpep_dropoff_datetime AS TIMESTAMP) AS dropoff_datetime,

    -- trip info
    store_and_fwd_flag,
    CAST(passenger_count AS INT64)           AS passenger_count,
    CAST(trip_distance AS FLOAT64)           AS trip_distance,
    1                                        AS trip_type, -- yellow taxis can only be street-hailed (trip_type = 1)

    -- payment + fare info
    CAST(payment_type AS INT64)              AS payment_type,
    CAST(fare_amount AS FLOAT64)             AS fare_amount,
    CAST(extra AS FLOAT64)                   AS extra,
    CAST(mta_tax AS FLOAT64)                 AS mta_tax,
    CAST(tip_amount AS FLOAT64)              AS tip_amount,
    CAST(tolls_amount AS FLOAT64)            AS tolls_amount,
    0                                        AS ehail_fee, -- yellow taxis do not have an ehail fee (ehail_fee = 0)
    CAST(improvement_surcharge AS FLOAT64)   AS improvement_surcharge,
    CAST(total_amount AS FLOAT64)            AS total_amount,
    CAST(congestion_surcharge AS FLOAT64)    AS congestion_surcharge

FROM {{ source('raw_data', 'yellow_tripdata') }}
