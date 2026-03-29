CREATE OR REPLACE TABLE experiment_exposures AS
WITH ranked_exposures AS (
  SELECT
    experiment_key,
    user_id,
    variant,
    country,
    device_type,
    event_ts AS exposure_ts,
    ROW_NUMBER() OVER (
      PARTITION BY experiment_key, user_id
      ORDER BY event_ts
    ) AS exposure_rank
  FROM stg_events
  WHERE event_name = 'experiment_exposure'
    AND experiment_key IS NOT NULL
    AND variant IS NOT NULL
)
SELECT
  experiment_key,
  user_id,
  variant,
  country,
  device_type,
  exposure_ts
FROM ranked_exposures
WHERE exposure_rank = 1;
