CREATE OR REPLACE TABLE
`still-resource-497715-g5.retail_audit_records.sales_dq_results`
AS

WITH valid_customers AS (
  SELECT DISTINCT customer_id
  FROM `still-resource-497715-g5.retail_staging.customers_raw`
  WHERE customer_id IS NOT NULL
),

valid_products AS (
  SELECT DISTINCT product_id
  FROM `still-resource-497715-g5.retail_staging.products_raw`
  WHERE product_id IS NOT NULL
),

valid_stores AS (
  SELECT DISTINCT store_id
  FROM `still-resource-497715-g5.retail_staging.stores_raw`
  WHERE store_id IS NOT NULL
),

base AS (
  SELECT
    s.*,
    SAFE_CAST(quantity AS INT64) quantity_int,
    SAFE_CAST(sale_amount AS NUMERIC) sale_amount_num,
    SAFE_CAST(sale_date AS DATE) sale_date_dt,
    ROW_NUMBER() OVER(
      PARTITION BY order_id
      ORDER BY load_timestamp DESC
    ) rn
  FROM `still-resource-497715-g5.retail_staging.sales_raw` s
)

SELECT
  b.*,

  CONCAT(
    IF(rn > 1,'DUPLICATE_ORDER_ID|',''),
    IF(quantity_int <= 0 OR quantity_int IS NULL,'INVALID_QUANTITY|',''),
    IF(sale_amount_num <= 0 OR sale_amount_num IS NULL,'INVALID_SALE_AMOUNT|',''),
    IF(vc.customer_id IS NULL,'INVALID_CUSTOMER_FK|',''),
    IF(vp.product_id IS NULL,'INVALID_PRODUCT_FK|',''),
    IF(vs.store_id IS NULL,'INVALID_STORE_FK|','')
  ) dq_reason

FROM base b
LEFT JOIN valid_customers vc
ON b.customer_id = vc.customer_id

LEFT JOIN valid_products vp
ON b.product_id = vp.product_id

LEFT JOIN valid_stores vs
ON b.store_id = vs.store_id;