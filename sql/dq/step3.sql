DECLARE v_run_id STRING DEFAULT GENERATE_UUID();

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
WITH c AS (
  SELECT
    COUNT(*) AS total_record_count,
    COUNTIF(REGEXP_CONTAINS(dq_reason, 'DUPLICATE_ORDER_ID')) AS duplicate_order_id_count,
    COUNTIF(REGEXP_CONTAINS(dq_reason, 'INVALID_CUSTOMER_FK')) AS invalid_customer_fk_count,
    COUNTIF(REGEXP_CONTAINS(dq_reason, 'INVALID_PRODUCT_FK')) AS invalid_product_fk_count,
    COUNTIF(REGEXP_CONTAINS(dq_reason, 'INVALID_STORE_FK')) AS invalid_store_fk_count,
    COUNTIF(REGEXP_CONTAINS(dq_reason, 'INVALID_QUANTITY')) AS invalid_quantity_count,
    COUNTIF(REGEXP_CONTAINS(dq_reason, 'INVALID_SALE_AMOUNT')) AS invalid_sale_amount_count
  FROM `still-resource-497715-g5.retail_audit_records.sales_dq_results`
)
SELECT GENERATE_UUID(), v_run_id, 'sales_raw', 'DUPLICATE_ORDER_ID', 'DUPLICATE_CHECK', 'order_id',
       IF(duplicate_order_id_count > 0, 'FAILED', 'PASSED'), duplicate_order_id_count, total_record_count,
       'duplicate order_id found', CURRENT_TIMESTAMP()
FROM c

UNION ALL
SELECT GENERATE_UUID(), v_run_id, 'sales_raw', 'INVALID_CUSTOMER_FK', 'FK_CHECK', 'customer_id',
       IF(invalid_customer_fk_count > 0, 'FAILED', 'PASSED'), invalid_customer_fk_count, total_record_count,
       'customer_id not found in customers_raw', CURRENT_TIMESTAMP()
FROM c

UNION ALL
SELECT GENERATE_UUID(), v_run_id, 'sales_raw', 'INVALID_PRODUCT_FK', 'FK_CHECK', 'product_id',
       IF(invalid_product_fk_count > 0, 'FAILED', 'PASSED'), invalid_product_fk_count, total_record_count,
       'product_id not found in products_raw', CURRENT_TIMESTAMP()
FROM c

UNION ALL
SELECT GENERATE_UUID(), v_run_id, 'sales_raw', 'INVALID_STORE_FK', 'FK_CHECK', 'store_id',
       IF(invalid_store_fk_count > 0, 'FAILED', 'PASSED'), invalid_store_fk_count, total_record_count,
       'store_id not found in stores_raw', CURRENT_TIMESTAMP()
FROM c

UNION ALL
SELECT GENERATE_UUID(), v_run_id, 'sales_raw', 'INVALID_QUANTITY', 'BUSINESS_RULE_CHECK', 'quantity',
       IF(invalid_quantity_count > 0, 'FAILED', 'PASSED'), invalid_quantity_count, total_record_count,
       'quantity is invalid or <= 0', CURRENT_TIMESTAMP()
FROM c

UNION ALL
SELECT GENERATE_UUID(), v_run_id, 'sales_raw', 'INVALID_SALE_AMOUNT', 'BUSINESS_RULE_CHECK', 'sale_amount',
       IF(invalid_sale_amount_count > 0, 'FAILED', 'PASSED'), invalid_sale_amount_count, total_record_count,
       'sale_amount is invalid or <= 0', CURRENT_TIMESTAMP()
FROM c;

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
  'sales_raw',
  COUNT(*) AS total_checks,
  COUNTIF(status = 'PASSED') AS passed_checks,
  COUNTIF(status = 'FAILED') AS failed_checks,
  0 AS warning_checks,
  IF(COUNTIF(status = 'FAILED') > 0, 'FAILED', 'PASSED') AS overall_status,
  CURRENT_TIMESTAMP()
FROM `still-resource-497715-g5.retail_audit_records.dq_validation_results`
WHERE run_id = v_run_id;