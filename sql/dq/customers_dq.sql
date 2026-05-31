DECLARE v_run_id STRING DEFAULT GENERATE_UUID();

CREATE TEMP TABLE customer_dq AS
WITH base AS (
  SELECT
    *,
    SAFE_CAST(created_date AS DATE) AS created_date_dt
  FROM `still-resource-497715-g5.retail_staging.customers_raw`
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