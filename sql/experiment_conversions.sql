CREATE OR REPLACE TABLE experiment_conversions AS
WITH checkout_events AS (
  SELECT
    user_id,
    event_ts AS conversion_ts,
    COALESCE(revenue, 0) AS revenue
  FROM stg_events
  WHERE event_name = 'checkout_completed'
)
SELECT
  exposure.experiment_key,
  exposure.user_id,
  exposure.variant,
  exposure.country,
  exposure.device_type,
  exposure.exposure_ts,
  MIN(checkout.conversion_ts) AS conversion_ts,
  ROUND(COALESCE(SUM(checkout.revenue), 0), 2) AS revenue
FROM experiment_exposures AS exposure
LEFT JOIN checkout_events AS checkout
  ON exposure.user_id = checkout.user_id
 AND checkout.conversion_ts >= exposure.exposure_ts
 AND checkout.conversion_ts < exposure.exposure_ts + INTERVAL '{lookback_days} days'
GROUP BY 1, 2, 3, 4, 5, 6;
