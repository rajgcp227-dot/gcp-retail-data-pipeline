-- =====================================================
-- create_dim_customer.sql
-- Retail Data Pipeline - Reporting Layer
-- Source : retail_history_records.customers_history
-- Target : retail_reporting_records.dim_customer
-- Logic  : SCD Type 1 MERGE with record_hash
-- =====================================================

CREATE SCHEMA IF NOT EXISTS `still-resource-497715-g5.retail_reporting_records`;

CREATE TABLE IF NOT EXISTS `still-resource-497715-g5.retail_reporting_records.dim_customer`
(
  customer_sk INT64 NOT NULL,
  customer_id STRING NOT NULL,
  customer_name STRING,
  email STRING,
  city STRING,
  state STRING,
  created_date DATE,

  record_hash INT64,

  source_file_name STRING,
  batch_id STRING,
  load_timestamp TIMESTAMP,

  reporting_created_at TIMESTAMP,
  reporting_updated_at TIMESTAMP
)
CLUSTER BY customer_id;

MERGE `still-resource-497715-g5.retail_reporting_records.dim_customer` AS tgt
USING (
  SELECT
    ABS(FARM_FINGERPRINT(customer_id)) AS customer_sk,
    customer_id,
    customer_name,
    email,
    city,
    state,
    created_date,

    FARM_FINGERPRINT(
      TO_JSON_STRING(
        STRUCT(
          customer_name,
          email,
          city,
          state,
          created_date
        )
      )
    ) AS record_hash,

    source_file_name,
    batch_id,
    load_timestamp
  FROM `still-resource-497715-g5.retail_history_records.customers_history`
) AS src
ON tgt.customer_id = src.customer_id

WHEN MATCHED AND tgt.record_hash != src.record_hash THEN
  UPDATE SET
    tgt.customer_sk = src.customer_sk,
    tgt.customer_name = src.customer_name,
    tgt.email = src.email,
    tgt.city = src.city,
    tgt.state = src.state,
    tgt.created_date = src.created_date,
    tgt.record_hash = src.record_hash,
    tgt.source_file_name = src.source_file_name,
    tgt.batch_id = src.batch_id,
    tgt.load_timestamp = src.load_timestamp,
    tgt.reporting_updated_at = CURRENT_TIMESTAMP()

WHEN NOT MATCHED THEN
  INSERT (
    customer_sk,
    customer_id,
    customer_name,
    email,
    city,
    state,
    created_date,
    record_hash,
    source_file_name,
    batch_id,
    load_timestamp,
    reporting_created_at,
    reporting_updated_at
  )
  VALUES (
    src.customer_sk,
    src.customer_id,
    src.customer_name,
    src.email,
    src.city,
    src.state,
    src.created_date,
    src.record_hash,
    src.source_file_name,
    src.batch_id,
    src.load_timestamp,
    CURRENT_TIMESTAMP(),
    CURRENT_TIMESTAMP()
  );