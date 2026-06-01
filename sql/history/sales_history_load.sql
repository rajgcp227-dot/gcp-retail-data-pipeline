-- =====================================================
-- sales_history_load.sql
-- Purpose: Load clean and deduplicated sales records
-- Source : retail_staging.sales_raw
-- Target : retail_history_records.sales_history
-- Logic  : DQ clean filter + FK validation + latest record + MERGE
-- =====================================================

MERGE `still-resource-497715-g5.retail_history_records.sales_history` AS tgt
USING (
  WITH base AS (
    SELECT
      order_id,
      customer_id,
      product_id,
      store_id,

      SAFE_CAST(quantity AS INT64) AS quantity,
      SAFE_CAST(unit_price AS NUMERIC) AS unit_price,
      SAFE_CAST(discount_amount AS NUMERIC) AS discount_amount,
      SAFE_CAST(tax_amount AS NUMERIC) AS tax_amount,
      SAFE_CAST(sale_amount AS NUMERIC) AS sale_amount,
      SAFE_CAST(sale_date AS DATE) AS sale_date,

      payment_method,
      coupon_code,
      sale_channel,

      load_type,
      source_file_name,
      batch_id,
      load_timestamp,

      ROW_NUMBER() OVER (
        PARTITION BY order_id
        ORDER BY load_timestamp DESC
      ) AS rn
    FROM `still-resource-497715-g5.retail_staging.sales_raw`
  ),

  clean_sales AS (
    SELECT
      b.*
    FROM base b

    INNER JOIN `still-resource-497715-g5.retail_history_records.customers_history` c
      ON b.customer_id = c.customer_id

    INNER JOIN `still-resource-497715-g5.retail_history_records.products_history` p
      ON b.product_id = p.product_id

    INNER JOIN `still-resource-497715-g5.retail_history_records.stores_history` st
      ON b.store_id = st.store_id

    WHERE b.rn = 1

      AND b.order_id IS NOT NULL
      AND TRIM(b.order_id) != ''

      AND b.customer_id IS NOT NULL
      AND TRIM(b.customer_id) != ''

      AND b.product_id IS NOT NULL
      AND TRIM(b.product_id) != ''

      AND b.store_id IS NOT NULL
      AND TRIM(b.store_id) != ''

      AND b.quantity IS NOT NULL
      AND b.quantity > 0

      AND b.unit_price IS NOT NULL
      AND b.unit_price > 0

      AND b.discount_amount IS NOT NULL
      AND b.discount_amount >= 0

      AND b.tax_amount IS NOT NULL
      AND b.tax_amount >= 0

      AND b.sale_amount IS NOT NULL
      AND b.sale_amount > 0

      AND b.sale_date IS NOT NULL
      AND b.sale_date <= CURRENT_DATE()

      AND b.payment_method IS NOT NULL
      AND TRIM(b.payment_method) != ''

      AND b.load_type IS NOT NULL
      AND TRIM(b.load_type) != ''
      AND UPPER(TRIM(b.load_type)) IN ('FULL', 'DELTA')

      AND b.source_file_name IS NOT NULL
      AND TRIM(b.source_file_name) != ''

      AND b.batch_id IS NOT NULL
      AND TRIM(b.batch_id) != ''

      AND b.load_timestamp IS NOT NULL
  )

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
    load_type,
    source_file_name,
    batch_id,
    load_timestamp,
    CURRENT_TIMESTAMP() AS history_created_at,
    CURRENT_TIMESTAMP() AS history_updated_at
  FROM clean_sales

) AS src

ON tgt.order_id = src.order_id

WHEN MATCHED THEN
  UPDATE SET
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
    tgt.load_type = src.load_type,
    tgt.source_file_name = src.source_file_name,
    tgt.batch_id = src.batch_id,
    tgt.load_timestamp = src.load_timestamp,
    tgt.history_updated_at = CURRENT_TIMESTAMP()

WHEN NOT MATCHED THEN
  INSERT (
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
    load_type,
    source_file_name,
    batch_id,
    load_timestamp,
    history_created_at,
    history_updated_at
  )
  VALUES (
    src.order_id,
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
    src.load_type,
    src.source_file_name,
    src.batch_id,
    src.load_timestamp,
    CURRENT_TIMESTAMP(),
    CURRENT_TIMESTAMP()
  );