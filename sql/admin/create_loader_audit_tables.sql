-- =====================================================
-- create_loader_audit_tables.sql
-- Retail Data Pipeline - Loader Audit Tables
-- Purpose:
--   Create audit tables required by load_gcs_to_bq_staging.py
-- =====================================================

CREATE SCHEMA IF NOT EXISTS `still-resource-497715-g5.retail_audit_records`
OPTIONS (
  location = 'asia-south1'
);

CREATE TABLE IF NOT EXISTS `still-resource-497715-g5.retail_audit_records.processed_files`
(
  file_name STRING,
  gcs_uri STRING,
  source_name STRING,
  target_table STRING,
  batch_id STRING,
  row_count INT64,
  status STRING,
  processed_timestamp TIMESTAMP,
  error_message STRING
)
PARTITION BY DATE(processed_timestamp)
CLUSTER BY source_name, status, target_table;


CREATE TABLE IF NOT EXISTS `still-resource-497715-g5.retail_audit_records.load_audit_log`
(
  audit_id STRING,
  pipeline_name STRING,
  source_name STRING,
  source_file_name STRING,
  target_dataset STRING,
  target_table STRING,
  load_start_time TIMESTAMP,
  load_end_time TIMESTAMP,
  status STRING,
  rows_loaded INT64,
  error_message STRING,
  created_by STRING
)
PARTITION BY DATE(load_start_time)
CLUSTER BY pipeline_name, source_name, status;