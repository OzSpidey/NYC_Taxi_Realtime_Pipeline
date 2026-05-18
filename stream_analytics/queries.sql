-- ============================================================
-- Azure Stream Analytics — four concurrent queries
-- Input  : IoTHub/EventHub alias  → [TaxiInput]
-- Outputs: ADLS Gen2 aliases      → [BronzeOutput], [RevenueOutput],
--                                   [SurgeOutput],  [LeaderboardOutput]
-- ============================================================


-- ──────────────────────────────────────────────────────────────
-- 1. BRONZE PASS-THROUGH
--    Every raw event lands in ADLS Gen2 as the Bronze layer.
--    Partitioned by date so downstream tools can prune efficiently.
-- ──────────────────────────────────────────────────────────────
SELECT
    ride_id,
    pickup_borough,
    dropoff_borough,
    driver_id,
    distance_miles,
    fare_amount,
    surge_multiplier,
    payment_type,
    passenger_count,
    event_time,
    DATEPART(year,  CAST(event_time AS datetime))  AS year,
    DATEPART(month, CAST(event_time AS datetime))  AS month,
    DATEPART(day,   CAST(event_time AS datetime))  AS day
INTO [BronzeOutput]
FROM [TaxiInput]
PARTITION BY PartitionId;


-- ──────────────────────────────────────────────────────────────
-- 2. REVENUE DASHBOARD  (Tumbling Window — 5 minutes)
--    Non-overlapping buckets: each ride counted exactly once.
--    Good for "revenue per borough per 5-min slot" dashboards.
-- ──────────────────────────────────────────────────────────────
SELECT
    pickup_borough,
    COUNT(*)                        AS ride_count,
    SUM(fare_amount)                AS total_revenue,
    AVG(fare_amount)                AS avg_fare,
    AVG(surge_multiplier)           AS avg_surge,
    AVG(distance_miles)             AS avg_distance,
    System.Timestamp()              AS window_end
INTO [RevenueOutput]
FROM [TaxiInput]
GROUP BY
    pickup_borough,
    TumblingWindow(minute, 5);


-- ──────────────────────────────────────────────────────────────
-- 3. SURGE ALERT  (Sliding Window — 2 minutes)
--    Fires on every new event and looks back 2 minutes.
--    If avg surge > 1.8 in any borough, emit an alert record
--    so the Azure Function can trigger a notification.
-- ──────────────────────────────────────────────────────────────
SELECT
    pickup_borough,
    AVG(surge_multiplier)           AS avg_surge,
    COUNT(*)                        AS ride_count,
    MAX(fare_amount)                AS max_fare,
    System.Timestamp()              AS alert_time
INTO [SurgeOutput]
FROM [TaxiInput]
GROUP BY
    pickup_borough,
    SlidingWindow(minute, 2)
HAVING AVG(surge_multiplier) > 1.8;


-- ──────────────────────────────────────────────────────────────
-- 4. DRIVER LEADERBOARD  (Hopping Window — 10 min window, 1 min hop)
--    Overlapping buckets updated every minute.
--    Shows rolling top-drivers without waiting for a full window.
-- ──────────────────────────────────────────────────────────────
SELECT
    driver_id,
    COUNT(*)                        AS rides_completed,
    SUM(fare_amount)                AS total_earned,
    AVG(surge_multiplier)           AS avg_surge,
    System.Timestamp()              AS window_end
INTO [LeaderboardOutput]
FROM [TaxiInput]
GROUP BY
    driver_id,
    HoppingWindow(minute, 10, 1);
