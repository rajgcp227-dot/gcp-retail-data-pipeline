
-- ============================================================
-- sales_dq.sql
-- Purpose:
--   Validate sales records and create an idempotent quarantine
--   snapshot containing only invalid sales rows.
-- ============================================================


-- ============================================================
-- STEP 1: CREATE SALES DQ RESULT TABLE
-- ============================================================

DECLARE v_run_id STRING DEFAULT @pipeline_run_id;

CREATE OR REPLACE TABLE
`still-resource-497715-g5.retail_audit_records.sales_dq_results`
AS

WITH valid_customers AS (
  SELECT DISTINCT
    customer_id
  FROM `still-resource-497715-g5.retail_staging.customers_raw`
  WHERE customer_id IS NOT NULL
    AND TRIM(customer_id) != ''
),

valid_products AS (
  SELECT DISTINCT
    product_id
  FROM `still-resource-497715-g5.retail_staging.products_raw`
  WHERE product_id IS NOT NULL
    AND TRIM(product_id) != ''
),

valid_stores AS (
  SELECT DISTINCT
    store_id
  FROM `still-resource-497715-g5.retail_staging.stores_raw`
  WHERE store_id IS NOT NULL
    AND TRIM(store_id) != ''
),

base AS (
  SELECT
    sales.*,

    SAFE_CAST(quantity AS INT64) AS quantity_int,
    SAFE_CAST(sale_amount AS NUMERIC) AS sale_amount_num,
    SAFE_CAST(sale_date AS DATE) AS sale_date_dt,

    ROW_NUMBER() OVER (
      PARTITION BY order_id
      ORDER BY load_timestamp DESC, source_file_name DESC
    ) AS duplicate_rank

  FROM `still-resource-497715-g5.retail_staging.sales_raw` AS sales
WHERE sales.pipeline_run_id = v_run_id
)
SELECT
  base.*,

  CONCAT(

    IF(
      order_id IS NULL OR TRIM(order_id) = '',
      'INVALID_ORDER_ID|',
      ''
    ),

    IF(
      duplicate_rank > 1,
      'DUPLICATE_ORDER_ID|',
      ''
    ),

    IF(
      quantity_int IS NULL OR quantity_int <= 0,
      'INVALID_QUANTITY|',
      ''
    ),

    IF(
      sale_amount_num IS NULL OR sale_amount_num <= 0,
      'INVALID_SALE_AMOUNT|',
      ''
    ),

    IF(
      sale_date_dt IS NULL OR sale_date_dt > CURRENT_DATE(),
      'INVALID_SALE_DATE|',
      ''
    ),

    IF(
      payment_method IS NULL
      OR UPPER(TRIM(payment_method))
         NOT IN ('UPI', 'CARD', 'CASH', 'NETBANKING'),
      'INVALID_PAYMENT_METHOD|',
      ''
    ),

    IF(
      valid_customers.customer_id IS NULL,
      'INVALID_CUSTOMER_FK|',
      ''
    ),

    IF(
      valid_products.product_id IS NULL,
      'INVALID_PRODUCT_FK|',
      ''
    ),

    IF(
      valid_stores.store_id IS NULL,
      'INVALID_STORE_FK|',
      ''
    )

  ) AS dq_reason,

  CURRENT_TIMESTAMP() AS dq_run_timestamp

FROM base

LEFT JOIN valid_customers
  ON base.customer_id = valid_customers.customer_id

LEFT JOIN valid_products
  ON base.product_id = valid_products.product_id

LEFT JOIN valid_stores
  ON base.store_id = valid_stores.store_id;


-- ============================================================
-- STEP 2: CREATE IDEMPOTENT SALES QUARANTINE SNAPSHOT
-- ============================================================

CREATE OR REPLACE TABLE
`still-resource-497715-g5.retail_quarantine_records.sales_quarantine`
AS

SELECT
  order_id AS sale_id,
  customer_id,
  product_id,
  store_id,

  quantity_int AS quantity,
  sale_amount_num AS sale_amount,
  sale_date_dt AS sale_date,

  RTRIM(dq_reason, '|') AS quarantine_reason,

  'retail_staging.sales_raw' AS source_table,

  sale_date_dt AS load_date,

  source_file_name,
  batch_id,
  load_timestamp,

  CURRENT_TIMESTAMP() AS quarantined_at,
  CURRENT_TIMESTAMP() AS dq_run_timestamp

FROM `still-resource-497715-g5.retail_audit_records.sales_dq_results`

WHERE dq_reason != '';


-- ============================================================
-- STEP 3: DISPLAY DQ FAILURE SUMMARY
-- ============================================================

SELECT
  reason,
  COUNT(*) AS failed_records

FROM `still-resource-497715-g5.retail_audit_records.sales_dq_results`,

UNNEST(
  SPLIT(
    RTRIM(dq_reason, '|'),
    '|'
  )
) AS reason

WHERE dq_reason != ''

GROUP BY reason

ORDER BY failed_records DESC;


-- Sales threshold-based rule results
DELETE FROM `still-resource-497715-g5.retail_audit_records.dq_rule_results`
WHERE pipeline_run_id = v_run_id
  AND source_name = 'sales';


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

WITH batch_total AS (
  SELECT
    COUNT(*) AS total_records,
    MAX(source_file_name) AS source_file_name
  FROM `still-resource-497715-g5.retail_audit_records.sales_dq_results`
  WHERE pipeline_run_id = v_run_id
),

rule_failures AS (
  SELECT
    reason AS rule_name,
    COUNT(*) AS failed_records
  FROM `still-resource-497715-g5.retail_audit_records.sales_dq_results`,
  UNNEST(SPLIT(RTRIM(dq_reason, '|'), '|')) AS reason
  WHERE pipeline_run_id = v_run_id
    AND dq_reason != ''
  GROUP BY reason
),

metrics AS (
  SELECT
    threshold.rule_name,
    threshold.severity,
    total.total_records,
    total.source_file_name,
    COALESCE(failure.failed_records, 0) AS failed_records,

    CAST(
      ROUND(
        SAFE_DIVIDE(
          COALESCE(failure.failed_records, 0),
          total.total_records
        ) * 100,
        4
      ) AS NUMERIC
    ) AS failed_percentage,

    threshold.warning_percentage,
    threshold.failure_percentage,
    threshold.max_failed_records

  FROM `still-resource-497715-g5.retail_audit_records.dq_rule_thresholds` AS threshold

  CROSS JOIN batch_total AS total

  LEFT JOIN rule_failures AS failure
    ON threshold.rule_name = failure.rule_name

  WHERE threshold.source_name = 'sales'
    AND threshold.active_flag = TRUE
)

SELECT
  v_run_id,
  'sales',
  source_file_name,
  rule_name,
  severity,
  total_records,
  failed_records,
  failed_percentage,
  warning_percentage,
  failure_percentage,
  max_failed_records,

  CASE
    WHEN failed_records > max_failed_records
      OR failed_percentage > failure_percentage
      THEN 'FAIL'

    WHEN failed_percentage > warning_percentage
      THEN 'PASS_WITH_QUARANTINE'

    ELSE 'PASS'
  END,

  CURRENT_TIMESTAMP()

FROM metrics;


-- ============================================================
-- STEP 5: SAVE SALES BATCH SUMMARY
-- ============================================================

DELETE FROM `still-resource-497715-g5.retail_audit_records.dq_batch_summary`
WHERE pipeline_run_id = v_run_id
  AND source_name = 'sales';


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
  FROM `still-resource-497715-g5.retail_audit_records.sales_dq_results`
  WHERE pipeline_run_id = v_run_id
),

rule_counts AS (
  SELECT
    COUNTIF(dq_status = 'PASS') AS passed_rule_count,
    COUNTIF(dq_status = 'PASS_WITH_QUARANTINE') AS warning_rule_count,
    COUNTIF(dq_status = 'FAIL') AS failed_rule_count
  FROM `still-resource-497715-g5.retail_audit_records.dq_rule_results`
  WHERE pipeline_run_id = v_run_id
    AND source_name = 'sales'
)

SELECT
  v_run_id,
  'sales',
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


-- ============================================================
-- STEP 6: FINAL SALES BATCH SUMMARY CHECK
-- ============================================================

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
  AND source_name = 'sales';


