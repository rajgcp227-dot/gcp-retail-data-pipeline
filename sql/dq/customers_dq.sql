DECLARE v_run_id STRING DEFAULT @pipeline_run_id;

CREATE TEMP TABLE customer_dq AS
WITH base AS (
  SELECT
    *,
    SAFE_CAST(created_date AS DATE) AS created_date_dt
  FROM `still-resource-497715-g5.retail_staging.customers_raw` AS customers
WHERE customers.pipeline_run_id = v_run_id
),
ranked AS (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY customer_id
      ORDER BY load_timestamp DESC
    ) AS rn
  FROM base
)
SELECT
  *,
  CONCAT(
    IF(customer_id IS NULL OR TRIM(customer_id) = '', 'NULL_OR_BLANK_CUSTOMER_ID|', ''),
    IF(rn > 1, 'DUPLICATE_CUSTOMER_ID|', ''),
    IF(customer_name IS NULL OR TRIM(customer_name) = '', 'NULL_OR_BLANK_CUSTOMER_NAME|', ''),
    IF(email IS NULL OR TRIM(email) = '', 'NULL_OR_BLANK_EMAIL|', ''),
    IF(
      email IS NOT NULL
      AND TRIM(email) != ''
      AND NOT REGEXP_CONTAINS(LOWER(TRIM(email)), r'^[a-z0-9._%+-]+@[a-z0-9.-]+\.[a-z]{2,}$'),
      'INVALID_EMAIL_FORMAT|',
      ''
    ),
    IF(city IS NULL OR TRIM(city) = '', 'NULL_OR_BLANK_CITY|', ''),
    IF(state IS NULL OR TRIM(state) = '', 'NULL_OR_BLANK_STATE|', ''),
    IF(created_date_dt IS NULL, 'INVALID_CREATED_DATE|', ''),
    IF(created_date_dt > CURRENT_DATE(), 'FUTURE_CREATED_DATE|', ''),
    IF(load_type IS NULL OR TRIM(load_type) = '', 'NULL_OR_BLANK_LOAD_TYPE|', ''),
    IF(
      load_type IS NOT NULL
      AND TRIM(load_type) != ''
      AND UPPER(TRIM(load_type)) NOT IN ('FULL', 'DELTA'),
      'INVALID_LOAD_TYPE|',
      ''
    ),
    IF(source_file_name IS NULL OR TRIM(source_file_name) = '', 'NULL_SOURCE_FILE_NAME|', ''),
    IF(batch_id IS NULL OR TRIM(batch_id) = '', 'NULL_BATCH_ID|', ''),
    IF(load_timestamp IS NULL, 'NULL_LOAD_TIMESTAMP|', '')
  ) AS dq_reason
FROM ranked;

CREATE OR REPLACE TABLE
`still-resource-497715-g5.retail_audit_records.customers_dq_results`
AS
SELECT *
FROM customer_dq;

TRUNCATE TABLE `still-resource-497715-g5.retail_quarantine_records.customers_quarantine`;

INSERT INTO `still-resource-497715-g5.retail_quarantine_records.customers_quarantine`
(
  customer_id,
  customer_name,
  email,
  phone,
  city,
  quarantine_reason,
  source_table,
  load_date,
  quarantined_at
)
SELECT
  customer_id,
  customer_name,
  email,
  NULL AS phone,
  city,
  RTRIM(dq_reason, '|') AS quarantine_reason,
  'retail_staging.customers_raw' AS source_table,
  created_date_dt AS load_date,
  CURRENT_TIMESTAMP() AS quarantined_at
FROM customer_dq
WHERE dq_reason != '';

CREATE TEMP TABLE customer_audit_counts AS
SELECT
  COUNT(*) AS total_record_count,
  COUNTIF(customer_id IS NULL OR TRIM(customer_id) = '') AS null_customer_id_count,
  COUNTIF(rn > 1) AS duplicate_customer_id_count,
  COUNTIF(customer_name IS NULL OR TRIM(customer_name) = '') AS null_customer_name_count,
  COUNTIF(email IS NULL OR TRIM(email) = '') AS null_email_count,
  COUNTIF(
    email IS NOT NULL
    AND TRIM(email) != ''
    AND NOT REGEXP_CONTAINS(LOWER(TRIM(email)), r'^[a-z0-9._%+-]+@[a-z0-9.-]+\.[a-z]{2,}$')
  ) AS invalid_email_count,
  COUNTIF(city IS NULL OR TRIM(city) = '') AS null_city_count,
  COUNTIF(state IS NULL OR TRIM(state) = '') AS null_state_count,
  COUNTIF(created_date_dt IS NULL) AS invalid_created_date_count,
  COUNTIF(created_date_dt > CURRENT_DATE()) AS future_created_date_count,
  COUNTIF(load_type IS NULL OR TRIM(load_type) = '') AS null_load_type_count,
  COUNTIF(
    load_type IS NOT NULL
    AND TRIM(load_type) != ''
    AND UPPER(TRIM(load_type)) NOT IN ('FULL', 'DELTA')
  ) AS invalid_load_type_count,
  COUNTIF(source_file_name IS NULL OR TRIM(source_file_name) = '') AS null_source_file_count,
  COUNTIF(batch_id IS NULL OR TRIM(batch_id) = '') AS null_batch_id_count,
  COUNTIF(load_timestamp IS NULL) AS null_load_timestamp_count
FROM customer_dq;

DELETE FROM `still-resource-497715-g5.retail_audit_records.dq_validation_results`
WHERE run_id = v_run_id
  AND table_name = 'customers_raw';


INSERT INTO `still-resource-497715-g5.retail_audit_records.dq_validation_results`
(
  validation_id,
  run_id,
  table_name,
  check_name,
  check_type,
  column_name,
  status,
  failed_record_count,
  total_record_count,
  error_message,
  created_at
)
SELECT GENERATE_UUID(), v_run_id, 'customers_raw', 'NULL_OR_BLANK_CUSTOMER_ID', 'NULL_CHECK', 'customer_id',
       IF(null_customer_id_count > 0, 'FAILED', 'PASSED'), null_customer_id_count, total_record_count,
       'customer_id is null or blank', CURRENT_TIMESTAMP()
FROM customer_audit_counts

UNION ALL
SELECT GENERATE_UUID(), v_run_id, 'customers_raw', 'DUPLICATE_CUSTOMER_ID', 'DUPLICATE_CHECK', 'customer_id',
       IF(duplicate_customer_id_count > 0, 'FAILED', 'PASSED'), duplicate_customer_id_count, total_record_count,
       'duplicate customer_id found', CURRENT_TIMESTAMP()
FROM customer_audit_counts

UNION ALL
SELECT GENERATE_UUID(), v_run_id, 'customers_raw', 'NULL_OR_BLANK_CUSTOMER_NAME', 'MANDATORY_CHECK', 'customer_name',
       IF(null_customer_name_count > 0, 'FAILED', 'PASSED'), null_customer_name_count, total_record_count,
       'customer_name is null or blank', CURRENT_TIMESTAMP()
FROM customer_audit_counts

UNION ALL
SELECT GENERATE_UUID(), v_run_id, 'customers_raw', 'NULL_OR_BLANK_EMAIL', 'MANDATORY_CHECK', 'email',
       IF(null_email_count > 0, 'FAILED', 'PASSED'), null_email_count, total_record_count,
       'email is null or blank', CURRENT_TIMESTAMP()
FROM customer_audit_counts

UNION ALL
SELECT GENERATE_UUID(), v_run_id, 'customers_raw', 'INVALID_EMAIL_FORMAT', 'FORMAT_CHECK', 'email',
       IF(invalid_email_count > 0, 'FAILED', 'PASSED'), invalid_email_count, total_record_count,
       'email format is invalid', CURRENT_TIMESTAMP()
FROM customer_audit_counts

UNION ALL
SELECT GENERATE_UUID(), v_run_id, 'customers_raw', 'NULL_OR_BLANK_CITY', 'MANDATORY_CHECK', 'city',
       IF(null_city_count > 0, 'FAILED', 'PASSED'), null_city_count, total_record_count,
       'city is null or blank', CURRENT_TIMESTAMP()
FROM customer_audit_counts

UNION ALL
SELECT GENERATE_UUID(), v_run_id, 'customers_raw', 'NULL_OR_BLANK_STATE', 'MANDATORY_CHECK', 'state',
       IF(null_state_count > 0, 'FAILED', 'PASSED'), null_state_count, total_record_count,
       'state is null or blank', CURRENT_TIMESTAMP()
FROM customer_audit_counts

UNION ALL
SELECT GENERATE_UUID(), v_run_id, 'customers_raw', 'INVALID_CREATED_DATE', 'DATE_CHECK', 'created_date',
       IF(invalid_created_date_count > 0, 'FAILED', 'PASSED'), invalid_created_date_count, total_record_count,
       'created_date is invalid', CURRENT_TIMESTAMP()
FROM customer_audit_counts

UNION ALL
SELECT GENERATE_UUID(), v_run_id, 'customers_raw', 'FUTURE_CREATED_DATE', 'DATE_CHECK', 'created_date',
       IF(future_created_date_count > 0, 'FAILED', 'PASSED'), future_created_date_count, total_record_count,
       'created_date is future date', CURRENT_TIMESTAMP()
FROM customer_audit_counts

UNION ALL
SELECT GENERATE_UUID(), v_run_id, 'customers_raw', 'NULL_OR_BLANK_LOAD_TYPE', 'NULL_CHECK', 'load_type',
       IF(null_load_type_count > 0, 'FAILED', 'PASSED'), null_load_type_count, total_record_count,
       'load_type is null or blank', CURRENT_TIMESTAMP()
FROM customer_audit_counts

UNION ALL
SELECT GENERATE_UUID(), v_run_id, 'customers_raw', 'INVALID_LOAD_TYPE', 'DOMAIN_CHECK', 'load_type',
       IF(invalid_load_type_count > 0, 'FAILED', 'PASSED'), invalid_load_type_count, total_record_count,
       'load_type should be FULL or DELTA', CURRENT_TIMESTAMP()
FROM customer_audit_counts

UNION ALL
SELECT GENERATE_UUID(), v_run_id, 'customers_raw', 'NULL_SOURCE_FILE_NAME', 'METADATA_CHECK', 'source_file_name',
       IF(null_source_file_count > 0, 'FAILED', 'PASSED'), null_source_file_count, total_record_count,
       'source_file_name is null or blank', CURRENT_TIMESTAMP()
FROM customer_audit_counts

UNION ALL
SELECT GENERATE_UUID(), v_run_id, 'customers_raw', 'NULL_BATCH_ID', 'METADATA_CHECK', 'batch_id',
       IF(null_batch_id_count > 0, 'FAILED', 'PASSED'), null_batch_id_count, total_record_count,
       'batch_id is null or blank', CURRENT_TIMESTAMP()
FROM customer_audit_counts

UNION ALL
SELECT GENERATE_UUID(), v_run_id, 'customers_raw', 'NULL_LOAD_TIMESTAMP', 'METADATA_CHECK', 'load_timestamp',
       IF(null_load_timestamp_count > 0, 'FAILED', 'PASSED'), null_load_timestamp_count, total_record_count,
       'load_timestamp is null', CURRENT_TIMESTAMP()
FROM customer_audit_counts;


DELETE FROM `still-resource-497715-g5.retail_audit_records.dq_summary`
WHERE run_id = v_run_id
  AND table_name = 'customers_raw';

  
INSERT INTO `still-resource-497715-g5.retail_audit_records.dq_summary`
(
  run_id,
  table_name,
  total_checks,
  passed_checks,
  failed_checks,
  warning_checks,
  overall_status,
  created_at
)
SELECT
  v_run_id,
  'customers_raw',
  COUNT(*) AS total_checks,
  COUNTIF(status = 'PASSED') AS passed_checks,
  COUNTIF(status = 'FAILED') AS failed_checks,
  0 AS warning_checks,
  IF(COUNTIF(status = 'FAILED') > 0, 'FAILED', 'PASSED') AS overall_status,
  CURRENT_TIMESTAMP()
FROM `still-resource-497715-g5.retail_audit_records.dq_validation_results`
WHERE run_id = v_run_id;


-- Customer threshold-based rule results
DELETE FROM `still-resource-497715-g5.retail_audit_records.dq_rule_results`
WHERE pipeline_run_id = v_run_id
  AND source_name = 'customers';

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
WITH batch_file AS (
  SELECT
    MAX(source_file_name) AS source_file_name
  FROM `still-resource-497715-g5.retail_audit_records.customers_dq_results`
),

validation_results AS (
  SELECT
    check_name,
    MAX(total_record_count) AS total_record_count,
    MAX(failed_record_count) AS failed_record_count
  FROM `still-resource-497715-g5.retail_audit_records.dq_validation_results`
  WHERE run_id = v_run_id
    AND table_name = 'customers_raw'
  GROUP BY check_name
)

SELECT
  v_run_id AS pipeline_run_id,
  'customers' AS source_name,
  batch_file.source_file_name,
  threshold.rule_name,
  threshold.severity,
  COALESCE(validation.total_record_count, 0) AS total_records,
  COALESCE(validation.failed_record_count, 0) AS failed_records,

  CAST(
    ROUND(
      SAFE_DIVIDE(
        COALESCE(validation.failed_record_count, 0),
        NULLIF(COALESCE(validation.total_record_count, 0), 0)
      ) * 100,
      4
    ) AS NUMERIC
  ) AS failed_percentage,

  threshold.warning_percentage,
  threshold.failure_percentage,
  threshold.max_failed_records,

  CASE
    WHEN COALESCE(validation.failed_record_count, 0) > threshold.max_failed_records
      OR SAFE_DIVIDE(
           COALESCE(validation.failed_record_count, 0),
           NULLIF(COALESCE(validation.total_record_count, 0), 0)
         ) * 100 > threshold.failure_percentage
      THEN 'FAIL'

    WHEN SAFE_DIVIDE(
           COALESCE(validation.failed_record_count, 0),
           NULLIF(COALESCE(validation.total_record_count, 0), 0)
         ) * 100 > threshold.warning_percentage
      THEN 'PASS_WITH_QUARANTINE'

    ELSE 'PASS'
  END AS dq_status,

  CURRENT_TIMESTAMP() AS evaluated_at

FROM `still-resource-497715-g5.retail_audit_records.dq_rule_thresholds` AS threshold
CROSS JOIN batch_file
LEFT JOIN validation_results AS validation
  ON threshold.rule_name = validation.check_name
WHERE threshold.source_name = 'customers'
  AND threshold.active_flag = TRUE;


-- Customer threshold-based batch summary
DELETE FROM `still-resource-497715-g5.retail_audit_records.dq_batch_summary`
WHERE pipeline_run_id = v_run_id
  AND source_name = 'customers';

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
  FROM `still-resource-497715-g5.retail_audit_records.customers_dq_results`
),
rule_counts AS (
  SELECT
    COUNTIF(dq_status = 'PASS') AS passed_rule_count,
    COUNTIF(dq_status = 'PASS_WITH_QUARANTINE') AS warning_rule_count,
    COUNTIF(dq_status = 'FAIL') AS failed_rule_count
  FROM `still-resource-497715-g5.retail_audit_records.dq_rule_results`
  WHERE pipeline_run_id = v_run_id
    AND source_name = 'customers'
)
SELECT
  v_run_id,
  'customers',
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
  AND source_name = 'customers';
