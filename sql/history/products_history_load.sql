-- =====================================================
-- products_history_load.sql
-- Purpose: Load clean and deduplicated product records
-- Source : retail_staging.products_raw
-- Target : retail_history_records.products_history
-- Logic  : DQ clean filter + latest record + MERGE
-- =====================================================

MERGE `still-resource-497715-g5.retail_history_records.products_history` AS tgt
USING (
  WITH base AS (
    SELECT
      product_id,
      product_name,
      category,
      brand,
      SAFE_CAST(price AS NUMERIC) AS price,
      SAFE_CAST(created_date AS DATE) AS created_date,
      load_type,
      source_file_name,
      batch_id,
      load_timestamp
    FROM `still-resource-497715-g5.retail_staging.products_raw`
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
    product_id,
    product_name,
    category,
    brand,
    price,
    created_date,
    load_type,
    source_file_name,
    batch_id,
    load_timestamp,
    CURRENT_TIMESTAMP() AS history_created_at,
    CURRENT_TIMESTAMP() AS history_updated_at
  FROM ranked
  WHERE rn = 1
    AND product_id IS NOT NULL
    AND TRIM(product_id) != ''
    AND product_name IS NOT NULL
    AND TRIM(product_name) != ''
    AND category IS NOT NULL
    AND TRIM(category) != ''
    AND brand IS NOT NULL
    AND TRIM(brand) != ''
    AND price IS NOT NULL
    AND price > 0
    AND created_date IS NOT NULL
    AND created_date <= CURRENT_DATE()
    AND load_type IS NOT NULL
    AND TRIM(load_type) != ''
    AND UPPER(TRIM(load_type)) IN ('FULL', 'DELTA')
    AND source_file_name IS NOT NULL
    AND TRIM(source_file_name) != ''
    AND batch_id IS NOT NULL
    AND TRIM(batch_id) != ''
    AND load_timestamp IS NOT NULL
) AS src
ON tgt.product_id = src.product_id

WHEN MATCHED THEN
  UPDATE SET
    tgt.product_name = src.product_name,
    tgt.category = src.category,
    tgt.brand = src.brand,
    tgt.price = src.price,
    tgt.created_date = src.created_date,
    tgt.load_type = src.load_type,
    tgt.source_file_name = src.source_file_name,
    tgt.batch_id = src.batch_id,
    tgt.load_timestamp = src.load_timestamp,
    tgt.history_updated_at = CURRENT_TIMESTAMP()

WHEN NOT MATCHED THEN
  INSERT (
    product_id,
    product_name,
    category,
    brand,
    price,
    created_date,
    load_type,
    source_file_name,
    batch_id,
    load_timestamp,
    history_created_at,
    history_updated_at
  )
  VALUES (
    src.product_id,
    src.product_name,
    src.category,
    src.brand,
    src.price,
    src.created_date,
    src.load_type,
    src.source_file_name,
    src.batch_id,
    src.load_timestamp,
    CURRENT_TIMESTAMP(),
    CURRENT_TIMESTAMP()
  );