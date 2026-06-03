-- =====================================================
-- create_dim_store.sql
-- Retail Data Pipeline - Reporting Layer
-- Source : retail_history_records.stores_history
-- Target : retail_reporting_records.dim_store
-- Logic  : SCD Type 1 MERGE with record_hash
-- =====================================================

CREATE SCHEMA IF NOT EXISTS `still-resource-497715-g5.retail_reporting_records`;

CREATE TABLE IF NOT EXISTS `still-resource-497715-g5.retail_reporting_records.dim_store`
(
  store_sk INT64 NOT NULL,
  store_id STRING NOT NULL,
  store_name STRING,
  city STRING,
  state STRING,
  region STRING,
  created_date DATE,

  record_hash INT64,

  source_file_name STRING,
  batch_id STRING,
  load_timestamp TIMESTAMP,

  reporting_created_at TIMESTAMP,
  reporting_updated_at TIMESTAMP
)
CLUSTER BY store_id;

MERGE `still-resource-497715-g5.retail_reporting_records.dim_store` AS tgt
USING (
  SELECT
    ABS(FARM_FINGERPRINT(store_id)) AS store_sk,
    store_id,
    store_name,
    city,
    state,
    region,
    created_date,

    FARM_FINGERPRINT(
      TO_JSON_STRING(
        STRUCT(
          store_name,
          city,
          state,
          region,
          created_date
        )
      )
    ) AS record_hash,

    source_file_name,
    batch_id,
    load_timestamp
  FROM `still-resource-497715-g5.retail_history_records.stores_history`
) AS src
ON tgt.store_id = src.store_id

WHEN MATCHED AND tgt.record_hash != src.record_hash THEN
  UPDATE SET
    tgt.store_sk = src.store_sk,
    tgt.store_name = src.store_name,
    tgt.city = src.city,
    tgt.state = src.state,
    tgt.region = src.region,
    tgt.created_date = src.created_date,
    tgt.record_hash = src.record_hash,
    tgt.source_file_name = src.source_file_name,
    tgt.batch_id = src.batch_id,
    tgt.load_timestamp = src.load_timestamp,
    tgt.reporting_updated_at = CURRENT_TIMESTAMP()

WHEN NOT MATCHED THEN
  INSERT (
    store_sk,
    store_id,
    store_name,
    city,
    state,
    region,
    created_date,
    record_hash,
    source_file_name,
    batch_id,
    load_timestamp,
    reporting_created_at,
    reporting_updated_at
  )
  VALUES (
    src.store_sk,
    src.store_id,
    src.store_name,
    src.city,
    src.state,
    src.region,
    src.created_date,
    src.record_hash,
    src.source_file_name,
    src.batch_id,
    src.load_timestamp,
    CURRENT_TIMESTAMP(),
    CURRENT_TIMESTAMP()
  );