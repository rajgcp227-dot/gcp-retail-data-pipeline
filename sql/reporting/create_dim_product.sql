-- =====================================================
-- create_dim_product.sql
-- Retail Data Pipeline - Reporting Layer
-- Source : retail_history_records.products_history
-- Target : retail_reporting_records.dim_product
-- Logic  : SCD Type 1 MERGE with record_hash
-- =====================================================

CREATE SCHEMA IF NOT EXISTS `still-resource-497715-g5.retail_reporting_records`;

CREATE TABLE IF NOT EXISTS `still-resource-497715-g5.retail_reporting_records.dim_product`
(
  product_sk INT64 NOT NULL,
  product_id STRING NOT NULL,
  product_name STRING,
  category STRING,
  brand STRING,
  price NUMERIC,
  created_date DATE,

  record_hash INT64,

  source_file_name STRING,
  batch_id STRING,
  load_timestamp TIMESTAMP,

  reporting_created_at TIMESTAMP,
  reporting_updated_at TIMESTAMP
)
CLUSTER BY product_id;

MERGE `still-resource-497715-g5.retail_reporting_records.dim_product` AS tgt
USING (
  SELECT
    ABS(FARM_FINGERPRINT(product_id)) AS product_sk,
    product_id,
    product_name,
    category,
    brand,
    price,
    created_date,

    FARM_FINGERPRINT(
      TO_JSON_STRING(
        STRUCT(
          product_name,
          category,
          brand,
          price,
          created_date
        )
      )
    ) AS record_hash,

    source_file_name,
    batch_id,
    load_timestamp
  FROM `still-resource-497715-g5.retail_history_records.products_history`
) AS src
ON tgt.product_id = src.product_id

WHEN MATCHED AND tgt.record_hash != src.record_hash THEN
  UPDATE SET
    tgt.product_sk = src.product_sk,
    tgt.product_name = src.product_name,
    tgt.category = src.category,
    tgt.brand = src.brand,
    tgt.price = src.price,
    tgt.created_date = src.created_date,
    tgt.record_hash = src.record_hash,
    tgt.source_file_name = src.source_file_name,
    tgt.batch_id = src.batch_id,
    tgt.load_timestamp = src.load_timestamp,
    tgt.reporting_updated_at = CURRENT_TIMESTAMP()

WHEN NOT MATCHED THEN
  INSERT (
    product_sk,
    product_id,
    product_name,
    category,
    brand,
    price,
    created_date,
    record_hash,
    source_file_name,
    batch_id,
    load_timestamp,
    reporting_created_at,
    reporting_updated_at
  )
  VALUES (
    src.product_sk,
    src.product_id,
    src.product_name,
    src.category,
    src.brand,
    src.price,
    src.created_date,
    src.record_hash,
    src.source_file_name,
    src.batch_id,
    src.load_timestamp,
    CURRENT_TIMESTAMP(),
    CURRENT_TIMESTAMP()
  );