INSERT INTO `still-resource-497715-g5.retail_quarantine_records.sales_quarantine`
(
  sale_id,
  customer_id,
  product_id,
  store_id,
  quantity,
  sale_amount,
  sale_date,
  quarantine_reason,
  source_table,
  load_date,
  quarantined_at
)
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
  CURRENT_TIMESTAMP() AS quarantined_at
FROM `still-resource-497715-g5.retail_audit_records.sales_dq_results`
WHERE dq_reason <> '';