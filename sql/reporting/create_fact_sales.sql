-- =====================================================
-- create_fact_sales.sql
-- Retail Data Pipeline - Reporting Layer
-- Source : retail_history_records.sales_history
-- Target : retail_reporting_records.fact_sales
-- Logic  : Fact MERGE by order_id with record_hash
-- =====================================================

CREATE SCHEMA IF NOT EXISTS `still-resource-497715-g5.retail_reporting_records`;

CREATE TABLE IF NOT EXISTS `still-resource-497715-g5.retail_reporting_records.fact_sales`
(
  sales_sk INT64 NOT NULL,
  order_id STRING NOT NULL,

  customer_sk INT64 NOT NULL,
  product_sk INT64 NOT NULL,
  store_sk INT64 NOT NULL,

  customer_id STRING NOT NULL,
  product_id STRING NOT NULL,
  store_id STRING NOT NULL,

  quantity INT64,
  unit_price NUMERIC,
  discount_amount NUMERIC,
  tax_amount NUMERIC,
  sale_amount NUMERIC,
  sale_date DATE,

  payment_method STRING,
  coupon_code STRING,
  sale_channel STRING,

  record_hash INT64,

  source_file_name STRING,
  batch_id STRING,
  load_timestamp TIMESTAMP,

  reporting_created_at TIMESTAMP,
  reporting_updated_at TIMESTAMP
)
PARTITION BY sale_date
CLUSTER BY customer_sk, product_sk, store_sk;

MERGE `still-resource-497715-g5.retail_reporting_records.fact_sales` AS tgt
USING (
  WITH sales_source AS (
    SELECT
      order_id,
      customer_id,
      product_id,
      store_id,
      quantity,
      unit_price,
      discount_amount,
      tax_amount,
      sale_amount,
      sale_date,
      payment_method,
      coupon_code,
      sale_channel,
      source_file_name,
      batch_id,
      load_timestamp
    FROM `still-resource-497715-g5.retail_history_records.sales_history`
    WHERE order_id IS NOT NULL
      AND TRIM(order_id) != ''
      AND customer_id IS NOT NULL
      AND TRIM(customer_id) != ''
      AND product_id IS NOT NULL
      AND TRIM(product_id) != ''
      AND store_id IS NOT NULL
      AND TRIM(store_id) != ''
      AND quantity IS NOT NULL
      AND quantity > 0
      AND sale_amount IS NOT NULL
      AND sale_amount > 0
      AND sale_date IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (
      PARTITION BY order_id
      ORDER BY load_timestamp DESC
    ) = 1
  ),

  dim_customer_unique AS (
    SELECT
      customer_id,
      customer_sk
    FROM `still-resource-497715-g5.retail_reporting_records.dim_customer`
    QUALIFY ROW_NUMBER() OVER (
      PARTITION BY customer_id
      ORDER BY reporting_updated_at DESC
    ) = 1
  ),

  dim_product_unique AS (
    SELECT
      product_id,
      product_sk
    FROM `still-resource-497715-g5.retail_reporting_records.dim_product`
    QUALIFY ROW_NUMBER() OVER (
      PARTITION BY product_id
      ORDER BY reporting_updated_at DESC
    ) = 1
  ),

  dim_store_unique AS (
    SELECT
      store_id,
      store_sk
    FROM `still-resource-497715-g5.retail_reporting_records.dim_store`
    QUALIFY ROW_NUMBER() OVER (
      PARTITION BY store_id
      ORDER BY reporting_updated_at DESC
    ) = 1
  )

  SELECT
    ABS(FARM_FINGERPRINT(s.order_id)) AS sales_sk,
    s.order_id,

    c.customer_sk,
    p.product_sk,
    st.store_sk,

    s.customer_id,
    s.product_id,
    s.store_id,

    s.quantity,
    s.unit_price,
    s.discount_amount,
    s.tax_amount,
    s.sale_amount,
    s.sale_date,

    s.payment_method,
    s.coupon_code,
    s.sale_channel,

    FARM_FINGERPRINT(
      TO_JSON_STRING(
        STRUCT(
          s.customer_id,
          s.product_id,
          s.store_id,
          s.quantity,
          s.unit_price,
          s.discount_amount,
          s.tax_amount,
          s.sale_amount,
          s.sale_date,
          s.payment_method,
          s.coupon_code,
          s.sale_channel
        )
      )
    ) AS record_hash,

    s.source_file_name,
    s.batch_id,
    s.load_timestamp

  FROM sales_source s
  INNER JOIN dim_customer_unique c
    ON s.customer_id = c.customer_id
  INNER JOIN dim_product_unique p
    ON s.product_id = p.product_id
  INNER JOIN dim_store_unique st
    ON s.store_id = st.store_id
) AS src

ON tgt.order_id = src.order_id

WHEN MATCHED AND tgt.record_hash != src.record_hash THEN
  UPDATE SET
    tgt.sales_sk = src.sales_sk,

    tgt.customer_sk = src.customer_sk,
    tgt.product_sk = src.product_sk,
    tgt.store_sk = src.store_sk,

    tgt.customer_id = src.customer_id,
    tgt.product_id = src.product_id,
    tgt.store_id = src.store_id,

    tgt.quantity = src.quantity,
    tgt.unit_price = src.unit_price,
    tgt.discount_amount = src.discount_amount,
    tgt.tax_amount = src.tax_amount,
    tgt.sale_amount = src.sale_amount,
    tgt.sale_date = src.sale_date,

    tgt.payment_method = src.payment_method,
    tgt.coupon_code = src.coupon_code,
    tgt.sale_channel = src.sale_channel,

    tgt.record_hash = src.record_hash,

    tgt.source_file_name = src.source_file_name,
    tgt.batch_id = src.batch_id,
    tgt.load_timestamp = src.load_timestamp,

    tgt.reporting_updated_at = CURRENT_TIMESTAMP()

WHEN NOT MATCHED THEN
  INSERT (
    sales_sk,
    order_id,
    customer_sk,
    product_sk,
    store_sk,
    customer_id,
    product_id,
    store_id,
    quantity,
    unit_price,
    discount_amount,
    tax_amount,
    sale_amount,
    sale_date,
    payment_method,
    coupon_code,
    sale_channel,
    record_hash,
    source_file_name,
    batch_id,
    load_timestamp,
    reporting_created_at,
    reporting_updated_at
  )
  VALUES (
    src.sales_sk,
    src.order_id,
    src.customer_sk,
    src.product_sk,
    src.store_sk,
    src.customer_id,
    src.product_id,
    src.store_id,
    src.quantity,
    src.unit_price,
    src.discount_amount,
    src.tax_amount,
    src.sale_amount,
    src.sale_date,
    src.payment_method,
    src.coupon_code,
    src.sale_channel,
    src.record_hash,
    src.source_file_name,
    src.batch_id,
    src.load_timestamp,
    CURRENT_TIMESTAMP(),
    CURRENT_TIMESTAMP()
  );