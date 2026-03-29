CREATE OR REPLACE TABLE experiment_metrics_daily AS
SELECT
  experiment_key,
  CAST(exposure_ts AS DATE) AS exposure_date,
  variant,
  COUNT(*) AS exposed_users,
  SUM(CASE WHEN conversion_ts IS NOT NULL THEN 1 ELSE 0 END) AS converters,
  ROUND(
    SUM(CASE WHEN conversion_ts IS NOT NULL THEN 1 ELSE 0 END)::DOUBLE / NULLIF(COUNT(*), 0),
    4
  ) AS conversion_rate,
  ROUND(SUM(revenue), 2) AS total_revenue,
  ROUND(SUM(revenue) / NULLIF(COUNT(*), 0), 2) AS revenue_per_user
FROM experiment_conversions
GROUP BY 1, 2, 3
ORDER BY 2, 3;
