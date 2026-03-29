CREATE OR REPLACE TABLE stg_events AS
SELECT
  CAST(event_id AS VARCHAR) AS event_id,
  CAST(user_id AS VARCHAR) AS user_id,
  CAST(session_id AS VARCHAR) AS session_id,
  CAST(event_name AS VARCHAR) AS event_name,
  CAST(event_ts AS TIMESTAMP) AS event_ts,
  CAST(experiment_key AS VARCHAR) AS experiment_key,
  CAST(variant AS VARCHAR) AS variant,
  CAST(country AS VARCHAR) AS country,
  CAST(device_type AS VARCHAR) AS device_type,
  CAST(revenue AS DOUBLE) AS revenue
FROM read_json_auto('{raw_events}', records = true);
