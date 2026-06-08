DECLARE v_run_id STRING DEFAULT @pipeline_run_id;

CREATE TEMP TABLE product_dq AS
WITH base AS (
  SELECT
    *,
    SAFE_CAST(price AS NUMERIC) AS price_num,
    SAFE_CAST(created_date AS DATE) AS created_date_dt
  FROM `still-resource-497715-g5.retail_staging.products_raw` AS products
WHERE products.pipeline_run_id = v_run_id
),
ranked AS (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY product_id
      ORDER BY load_timestamp DESC
    ) AS rn
  FROM base
)
SELECT
  *,
  CONCAT(
    IF(product_id IS NULL OR TRIM(product_id) = '', 'NULL_OR_BLANK_PRODUCT_ID|', ''),
    IF(rn > 1, 'DUPLICATE_PRODUCT_ID|', ''),
    IF(product_name IS NULL OR TRIM(product_name) = '', 'NULL_OR_BLANK_PRODUCT_NAME|', ''),
    IF(category IS NULL OR TRIM(category) = '', 'NULL_OR_BLANK_CATEGORY|', ''),
    IF(brand IS NULL OR TRIM(brand) = '', 'NULL_OR_BLANK_BRAND|', ''),
    IF(price_num IS NULL, 'INVALID_PRICE|', ''),
    IF(price_num <= 0, 'PRICE_LESS_THAN_EQUAL_ZERO|', ''),
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
`still-resource-497715-g5.retail_audit_records.products_dq_results`
AS
SELECT *
FROM product_dq;

INSERT INTO `still-resource-497715-g5.retail_quarantine_records.products_quarantine`
(
  product_id,
  product_name,
  category,
  price,
  quarantine_reason,
  source_table,
  load_date,
  quarantined_at
)
SELECT
  product_id,
  product_name,
  category,
  price_num,
  RTRIM(dq_reason, '|') AS quarantine_reason,
  'retail_staging.products_raw' AS source_table,
  created_date_dt AS load_date,
  CURRENT_TIMESTAMP() AS quarantined_at
FROM product_dq
WHERE dq_reason != '';

CREATE TEMP TABLE product_audit_counts AS
SELECT
  COUNT(*) AS total_record_count,
  COUNTIF(product_id IS NULL OR TRIM(product_id) = '') AS null_product_id_count,
  COUNTIF(rn > 1) AS duplicate_product_id_count,
  COUNTIF(product_name IS NULL OR TRIM(product_name) = '') AS null_product_name_count,
  COUNTIF(category IS NULL OR TRIM(category) = '') AS null_category_count,
  COUNTIF(brand IS NULL OR TRIM(brand) = '') AS null_brand_count,
  COUNTIF(price_num IS NULL) AS invalid_price_count,
  COUNTIF(price_num <= 0) AS price_less_equal_zero_count,
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
FROM product_dq;

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
SELECT GENERATE_UUID(), v_run_id, 'products_raw', 'NULL_OR_BLANK_PRODUCT_ID', 'NULL_CHECK', 'product_id',
       IF(null_product_id_count > 0, 'FAILED', 'PASSED'), null_product_id_count, total_record_count,
       'product_id is null or blank', CURRENT_TIMESTAMP()
FROM product_audit_counts

UNION ALL
SELECT GENERATE_UUID(), v_run_id, 'products_raw', 'DUPLICATE_PRODUCT_ID', 'DUPLICATE_CHECK', 'product_id',
       IF(duplicate_product_id_count > 0, 'FAILED', 'PASSED'), duplicate_product_id_count, total_record_count,
       'duplicate product_id found', CURRENT_TIMESTAMP()
FROM product_audit_counts

UNION ALL
SELECT GENERATE_UUID(), v_run_id, 'products_raw', 'NULL_OR_BLANK_PRODUCT_NAME', 'MANDATORY_CHECK', 'product_name',
       IF(null_product_name_count > 0, 'FAILED', 'PASSED'), null_product_name_count, total_record_count,
       'product_name is null or blank', CURRENT_TIMESTAMP()
FROM product_audit_counts

UNION ALL
SELECT GENERATE_UUID(), v_run_id, 'products_raw', 'NULL_OR_BLANK_CATEGORY', 'MANDATORY_CHECK', 'category',
       IF(null_category_count > 0, 'FAILED', 'PASSED'), null_category_count, total_record_count,
       'category is null or blank', CURRENT_TIMESTAMP()
FROM product_audit_counts

UNION ALL
SELECT GENERATE_UUID(), v_run_id, 'products_raw', 'NULL_OR_BLANK_BRAND', 'MANDATORY_CHECK', 'brand',
       IF(null_brand_count > 0, 'FAILED', 'PASSED'), null_brand_count, total_record_count,
       'brand is null or blank', CURRENT_TIMESTAMP()
FROM product_audit_counts

UNION ALL
SELECT GENERATE_UUID(), v_run_id, 'products_raw', 'INVALID_PRICE', 'TYPE_CHECK', 'price',
       IF(invalid_price_count > 0, 'FAILED', 'PASSED'), invalid_price_count, total_record_count,
       'price is not numeric', CURRENT_TIMESTAMP()
FROM product_audit_counts

UNION ALL
SELECT GENERATE_UUID(), v_run_id, 'products_raw', 'PRICE_LESS_THAN_EQUAL_ZERO', 'BUSINESS_RULE_CHECK', 'price',
       IF(price_less_equal_zero_count > 0, 'FAILED', 'PASSED'), price_less_equal_zero_count, total_record_count,
       'price should be greater than 0', CURRENT_TIMESTAMP()
FROM product_audit_counts

UNION ALL
SELECT GENERATE_UUID(), v_run_id, 'products_raw', 'INVALID_CREATED_DATE', 'DATE_CHECK', 'created_date',
       IF(invalid_created_date_count > 0, 'FAILED', 'PASSED'), invalid_created_date_count, total_record_count,
       'created_date is invalid', CURRENT_TIMESTAMP()
FROM product_audit_counts

UNION ALL
SELECT GENERATE_UUID(), v_run_id, 'products_raw', 'FUTURE_CREATED_DATE', 'DATE_CHECK', 'created_date',
       IF(future_created_date_count > 0, 'FAILED', 'PASSED'), future_created_date_count, total_record_count,
       'created_date is future date', CURRENT_TIMESTAMP()
FROM product_audit_counts

UNION ALL
SELECT GENERATE_UUID(), v_run_id, 'products_raw', 'NULL_OR_BLANK_LOAD_TYPE', 'NULL_CHECK', 'load_type',
       IF(null_load_type_count > 0, 'FAILED', 'PASSED'), null_load_type_count, total_record_count,
       'load_type is null or blank', CURRENT_TIMESTAMP()
FROM product_audit_counts

UNION ALL
SELECT GENERATE_UUID(), v_run_id, 'products_raw', 'INVALID_LOAD_TYPE', 'DOMAIN_CHECK', 'load_type',
       IF(invalid_load_type_count > 0, 'FAILED', 'PASSED'), invalid_load_type_count, total_record_count,
       'load_type should be FULL or DELTA', CURRENT_TIMESTAMP()
FROM product_audit_counts

UNION ALL
SELECT GENERATE_UUID(), v_run_id, 'products_raw', 'NULL_SOURCE_FILE_NAME', 'METADATA_CHECK', 'source_file_name',
       IF(null_source_file_count > 0, 'FAILED', 'PASSED'), null_source_file_count, total_record_count,
       'source_file_name is null or blank', CURRENT_TIMESTAMP()
FROM product_audit_counts

UNION ALL
SELECT GENERATE_UUID(), v_run_id, 'products_raw', 'NULL_BATCH_ID', 'METADATA_CHECK', 'batch_id',
       IF(null_batch_id_count > 0, 'FAILED', 'PASSED'), null_batch_id_count, total_record_count,
       'batch_id is null or blank', CURRENT_TIMESTAMP()
FROM product_audit_counts

UNION ALL
SELECT GENERATE_UUID(), v_run_id, 'products_raw', 'NULL_LOAD_TIMESTAMP', 'METADATA_CHECK', 'load_timestamp',
       IF(null_load_timestamp_count > 0, 'FAILED', 'PASSED'), null_load_timestamp_count, total_record_count,
       'load_timestamp is null', CURRENT_TIMESTAMP()
FROM product_audit_counts;

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
  'products_raw',
  COUNT(*) AS total_checks,
  COUNTIF(status = 'PASSED') AS passed_checks,
  COUNTIF(status = 'FAILED') AS failed_checks,
  0 AS warning_checks,
  IF(COUNTIF(status = 'FAILED') > 0, 'FAILED', 'PASSED') AS overall_status,
  CURRENT_TIMESTAMP()
FROM `still-resource-497715-g5.retail_audit_records.dq_validation_results`
WHERE run_id = v_run_id;


-- Product threshold results
DELETE FROM `still-resource-497715-g5.retail_audit_records.dq_rule_results`
WHERE pipeline_run_id = v_run_id
  AND source_name = 'products';

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
SELECT
  v_run_id,
  'products',
  MAX(p.source_file_name),
  t.rule_name,
  t.severity,
  MAX(v.total_record_count),
  MAX(v.failed_record_count),

  CAST(
    ROUND(
      SAFE_DIVIDE(
        MAX(v.failed_record_count),
        MAX(v.total_record_count)
      ) * 100,
      4
    ) AS NUMERIC
  ),

  t.warning_percentage,
  t.failure_percentage,
  t.max_failed_records,

  CASE
    WHEN MAX(v.failed_record_count) > t.max_failed_records
      OR SAFE_DIVIDE(
           MAX(v.failed_record_count),
           MAX(v.total_record_count)
         ) * 100 > t.failure_percentage
      THEN 'FAIL'

    WHEN SAFE_DIVIDE(
           MAX(v.failed_record_count),
           MAX(v.total_record_count)
         ) * 100 > t.warning_percentage
      THEN 'PASS_WITH_QUARANTINE'

    ELSE 'PASS'
  END,

  CURRENT_TIMESTAMP()

FROM `still-resource-497715-g5.retail_audit_records.dq_validation_results` v

JOIN `still-resource-497715-g5.retail_audit_records.dq_rule_thresholds` t
  ON t.source_name = 'products'
 AND t.rule_name = v.check_name
 AND t.active_flag = TRUE

CROSS JOIN product_dq p

WHERE v.run_id = v_run_id
  AND v.table_name = 'products_raw'

GROUP BY
  t.rule_name,
  t.severity,
  t.warning_percentage,
  t.failure_percentage,
  t.max_failed_records;


  -- Product batch summary
DELETE FROM `still-resource-497715-g5.retail_audit_records.dq_batch_summary`
WHERE pipeline_run_id = v_run_id
  AND source_name = 'products';

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
  FROM product_dq
),
rule_counts AS (
  SELECT
    COUNTIF(dq_status = 'PASS') AS passed_rule_count,
    COUNTIF(dq_status = 'PASS_WITH_QUARANTINE') AS warning_rule_count,
    COUNTIF(dq_status = 'FAIL') AS failed_rule_count
  FROM `still-resource-497715-g5.retail_audit_records.dq_rule_results`
  WHERE pipeline_run_id = v_run_id
    AND source_name = 'products'
)
SELECT
  v_run_id,
  'products',
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
  AND source_name = 'products';