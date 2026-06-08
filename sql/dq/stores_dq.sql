DECLARE v_run_id STRING DEFAULT @pipeline_run_id;

CREATE OR REPLACE TABLE
`still-resource-497715-g5.retail_audit_records.stores_dq_results`
AS

WITH base AS (
  SELECT
    *,
    SAFE_CAST(created_date AS DATE) AS created_date_dt
  FROM `still-resource-497715-g5.retail_staging.stores_raw`
  WHERE pipeline_run_id = v_run_id
),

ranked AS (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY store_id
      ORDER BY load_timestamp DESC
    ) AS duplicate_rank
  FROM base
)

SELECT
  *,

  CONCAT(
    IF(
      store_id IS NULL OR TRIM(store_id) = '',
      'NULL_OR_BLANK_STORE_ID|',
      ''
    ),

    IF(
      duplicate_rank > 1,
      'DUPLICATE_STORE_ID|',
      ''
    ),

    IF(
      store_name IS NULL OR TRIM(store_name) = '',
      'NULL_OR_BLANK_STORE_NAME|',
      ''
    ),

    IF(
      region IS NULL
      OR UPPER(TRIM(region)) NOT IN ('NORTH', 'SOUTH', 'EAST', 'WEST'),
      'INVALID_REGION|',
      ''
    ),

    IF(
      created_date_dt IS NULL
      OR created_date_dt > CURRENT_DATE(),
      'FUTURE_CREATED_DATE|',
      ''
    )
  ) AS dq_reason

FROM ranked;


SELECT
  reason,
  COUNT(*) AS failed_records
FROM `still-resource-497715-g5.retail_audit_records.stores_dq_results`,
UNNEST(SPLIT(RTRIM(dq_reason, '|'), '|')) AS reason
WHERE dq_reason != ''
GROUP BY reason
ORDER BY reason;

TRUNCATE TABLE
`still-resource-497715-g5.retail_quarantine_records.stores_quarantine`;

INSERT INTO
`still-resource-497715-g5.retail_quarantine_records.stores_quarantine`
(
  store_id,
  store_name,
  city,
  state,
  quarantine_reason,
  source_table,
  load_date,
  quarantined_at
)
SELECT
  store_id,
  store_name,
  city,
  state,
  RTRIM(dq_reason, '|'),
  'retail_staging.stores_raw',
  created_date_dt,
  CURRENT_TIMESTAMP()
FROM `still-resource-497715-g5.retail_audit_records.stores_dq_results`
WHERE dq_reason != '';


DELETE FROM `still-resource-497715-g5.retail_audit_records.dq_rule_results`
WHERE pipeline_run_id = v_run_id
  AND source_name = 'stores';

INSERT INTO `still-resource-497715-g5.retail_audit_records.dq_rule_results`
(
  pipeline_run_id,
  source_name,
  source_file_name,
  rule_name,
  severity,
  total_records,
  failed_records,
  failed_percentage,
  warning_percentage,
  failure_percentage,
  max_failed_records,
  dq_status,
  evaluated_at
)
WITH totals AS (
  SELECT
    COUNT(*) AS total_records,
    MAX(source_file_name) AS source_file_name
  FROM `still-resource-497715-g5.retail_audit_records.stores_dq_results`
),

failures AS (
  SELECT
    reason AS rule_name,
    COUNT(*) AS failed_records
  FROM `still-resource-497715-g5.retail_audit_records.stores_dq_results`,
  UNNEST(SPLIT(RTRIM(dq_reason, '|'), '|')) AS reason
  WHERE dq_reason != ''
  GROUP BY reason
)

SELECT
  v_run_id,
  'stores',
  totals.source_file_name,
  threshold.rule_name,
  threshold.severity,
  totals.total_records,
  COALESCE(failures.failed_records, 0),

  CAST(
    ROUND(
      SAFE_DIVIDE(
        COALESCE(failures.failed_records, 0),
        totals.total_records
      ) * 100,
      4
    ) AS NUMERIC
  ),

  threshold.warning_percentage,
  threshold.failure_percentage,
  threshold.max_failed_records,

  CASE
    WHEN COALESCE(failures.failed_records, 0) > threshold.max_failed_records
      OR SAFE_DIVIDE(
           COALESCE(failures.failed_records, 0),
           totals.total_records
         ) * 100 > threshold.failure_percentage
      THEN 'FAIL'

    WHEN SAFE_DIVIDE(
           COALESCE(failures.failed_records, 0),
           totals.total_records
         ) * 100 > threshold.warning_percentage
      THEN 'PASS_WITH_QUARANTINE'

    ELSE 'PASS'
  END,

  CURRENT_TIMESTAMP()

FROM `still-resource-497715-g5.retail_audit_records.dq_rule_thresholds`
  AS threshold
CROSS JOIN totals
LEFT JOIN failures
  ON threshold.rule_name = failures.rule_name
WHERE threshold.source_name = 'stores'
  AND threshold.active_flag = TRUE;

-- Store batch summary
DELETE FROM `still-resource-497715-g5.retail_audit_records.dq_batch_summary`
WHERE pipeline_run_id = v_run_id
  AND source_name = 'stores';

INSERT INTO `still-resource-497715-g5.retail_audit_records.dq_batch_summary`
(
  pipeline_run_id,
  source_name,
  source_file_name,
  total_records,
  valid_records,
  invalid_records,
  invalid_percentage,
  passed_rule_count,
  warning_rule_count,
  failed_rule_count,
  batch_status,
  evaluated_at
)
WITH batch_counts AS (
  SELECT
    MAX(source_file_name) AS source_file_name,
    COUNT(*) AS total_records,
    COUNTIF(dq_reason = '') AS valid_records,
    COUNTIF(dq_reason != '') AS invalid_records
  FROM `still-resource-497715-g5.retail_audit_records.stores_dq_results`
),
rule_counts AS (
  SELECT
    COUNTIF(dq_status = 'PASS') AS passed_rule_count,
    COUNTIF(dq_status = 'PASS_WITH_QUARANTINE') AS warning_rule_count,
    COUNTIF(dq_status = 'FAIL') AS failed_rule_count
  FROM `still-resource-497715-g5.retail_audit_records.dq_rule_results`
  WHERE pipeline_run_id = v_run_id
    AND source_name = 'stores'
)
SELECT
  v_run_id,
  'stores',
  batch.source_file_name,
  batch.total_records,
  batch.valid_records,
  batch.invalid_records,
  CAST(
    ROUND(
      SAFE_DIVIDE(batch.invalid_records, batch.total_records) * 100,
      4
    ) AS NUMERIC
  ),
  rules.passed_rule_count,
  rules.warning_rule_count,
  rules.failed_rule_count,
  CASE
    WHEN rules.failed_rule_count > 0 THEN 'FAIL'
    WHEN rules.warning_rule_count > 0 THEN 'PASS_WITH_QUARANTINE'
    ELSE 'PASS'
  END,
  CURRENT_TIMESTAMP()
FROM batch_counts AS batch
CROSS JOIN rule_counts AS rules;

-- Final verification
SELECT
  total_records,
  valid_records,
  invalid_records,
  invalid_percentage,
  passed_rule_count,
  warning_rule_count,
  failed_rule_count,
  batch_status
FROM `still-resource-497715-g5.retail_audit_records.dq_batch_summary`
WHERE pipeline_run_id = v_run_id
  AND source_name = 'stores';
