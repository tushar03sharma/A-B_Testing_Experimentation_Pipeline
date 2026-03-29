CREATE OR REPLACE TABLE experiment_metrics_summary AS
WITH variant_metrics AS (
  SELECT
    experiment_key,
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
  GROUP BY 1, 2
),
control_metrics AS (
  SELECT
    experiment_key,
    conversion_rate AS control_conversion_rate,
    revenue_per_user AS control_revenue_per_user
  FROM variant_metrics
  WHERE variant = 'control'
)
SELECT
  metrics.experiment_key,
  metrics.variant,
  metrics.exposed_users,
  metrics.converters,
  metrics.conversion_rate,
  metrics.total_revenue,
  metrics.revenue_per_user,
  ROUND(metrics.conversion_rate - control.control_conversion_rate, 4) AS absolute_lift,
  ROUND(
    CASE
      WHEN control.control_conversion_rate = 0 THEN NULL
      ELSE ((metrics.conversion_rate / control.control_conversion_rate) - 1) * 100
    END,
    2
  ) AS relative_lift_pct,
  ROUND(metrics.revenue_per_user - control.control_revenue_per_user, 2) AS rpu_lift
FROM variant_metrics AS metrics
LEFT JOIN control_metrics AS control
  ON metrics.experiment_key = control.experiment_key
ORDER BY metrics.variant;
