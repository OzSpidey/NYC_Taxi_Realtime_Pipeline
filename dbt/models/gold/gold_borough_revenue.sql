{{
  config(
    materialized = 'table',
    description  = 'Gold: borough revenue rolled up by hour with surge premium'
  )
}}

/*
  Business-ready table consumed directly by Power BI.
  surge_premium_usd = extra revenue earned *because* of surge pricing vs. flat 1x.
*/

WITH hourly AS (
    SELECT
        borough,
        DATEPART(hour, window_end_utc)          AS hour_of_day,
        CAST(window_end_utc AS DATE)            AS ride_date,
        SUM(ride_count)                         AS total_rides,
        SUM(total_revenue_usd)                  AS total_revenue_usd,
        AVG(avg_fare_usd)                       AS avg_fare_usd,
        AVG(avg_surge_multiplier)               AS avg_surge_multiplier,
        AVG(avg_distance_miles)                 AS avg_distance_miles
    FROM {{ ref('silver_window_aggregates') }}
    GROUP BY
        borough,
        DATEPART(hour, window_end_utc),
        CAST(window_end_utc AS DATE)
)

SELECT
    borough,
    ride_date,
    hour_of_day,
    total_rides,
    total_revenue_usd,
    avg_fare_usd,
    avg_surge_multiplier,
    avg_distance_miles,
    -- Extra revenue attributable to surge pricing
    ROUND(
        total_revenue_usd - (total_revenue_usd / NULLIF(avg_surge_multiplier, 0)),
        2
    )                                           AS surge_premium_usd
FROM hourly
