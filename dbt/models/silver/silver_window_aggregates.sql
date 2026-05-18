{{
  config(
    materialized = 'view',
    description  = 'Silver: raw Stream Analytics output, typed and renamed'
  )
}}

/*
  Stream Analytics writes tumbling-window output to ADLS Gen2 as Parquet.
  Synapse external tables (or COPY INTO) land that data in [raw].[revenue_windows].
  This view just casts and renames — no business logic yet.
*/

SELECT
    pickup_borough                              AS borough,
    CAST(window_end AS DATETIME2)               AS window_end_utc,
    CAST(ride_count AS INT)                     AS ride_count,
    CAST(total_revenue AS DECIMAL(18, 2))       AS total_revenue_usd,
    CAST(avg_fare AS DECIMAL(10, 2))            AS avg_fare_usd,
    CAST(avg_surge AS DECIMAL(5, 2))            AS avg_surge_multiplier,
    CAST(avg_distance AS DECIMAL(10, 2))        AS avg_distance_miles
FROM {{ source('raw', 'revenue_windows') }}
WHERE pickup_borough IS NOT NULL
