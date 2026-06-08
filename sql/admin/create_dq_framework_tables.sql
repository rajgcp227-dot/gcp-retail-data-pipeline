-- ============================================================
-- create_dq_framework_tables.sql
--
-- Purpose:
--   Create centralized metadata-driven DQ framework tables.
--
-- Tables:
--   1. dq_rule_thresholds
--   2. dq_rule_results
--   3. dq_batch_summary
-- ============================================================


CREATE SCHEMA IF NOT EXISTS
`still-resource-497715-g5.retail_audit_records`
OPTIONS (
  location = 'asia-south1'
);


-- ============================================================
-- 1. DQ RULE THRESHOLD CONFIGURATION
-- ============================================================

CREATE TABLE IF NOT EXISTS
`still-resource-497715-g5.retail_audit_records.dq_rule_thresholds`
(
  source_name STRING NOT NULL,
  rule_name STRING NOT NULL,

  severity STRING NOT NULL,

  warning_percentage NUMERIC NOT NULL,
  failure_percentage NUMERIC NOT NULL,
  max_failed_records INT64,

  warning_action STRING,
  failure_action STRING,

  active_flag BOOL NOT NULL,

  created_at TIMESTAMP NOT NULL,
  updated_at TIMESTAMP NOT NULL
)
CLUSTER BY source_name, rule_name, severity;


-- ============================================================
-- 2. RULE-LEVEL DQ EXECUTION RESULTS
-- ============================================================

CREATE TABLE IF NOT EXISTS
`still-resource-497715-g5.retail_audit_records.dq_rule_results`
(
  pipeline_run_id STRING NOT NULL,

  source_name STRING NOT NULL,
  source_file_name STRING,

  rule_name STRING NOT NULL,
  severity STRING,

  total_records INT64 NOT NULL,
  failed_records INT64 NOT NULL,
  failed_percentage NUMERIC NOT NULL,

  warning_percentage NUMERIC,
  failure_percentage NUMERIC,
  max_failed_records INT64,

  dq_status STRING NOT NULL,

  evaluated_at TIMESTAMP NOT NULL
)
PARTITION BY DATE(evaluated_at)
CLUSTER BY pipeline_run_id, source_name, rule_name, dq_status;


-- ============================================================
-- 3. BATCH-LEVEL DQ SUMMARY
-- ============================================================

CREATE TABLE IF NOT EXISTS
`still-resource-497715-g5.retail_audit_records.dq_batch_summary`
(
  pipeline_run_id STRING NOT NULL,

  source_name STRING NOT NULL,
  source_file_name STRING,

  total_records INT64 NOT NULL,
  valid_records INT64 NOT NULL,
  invalid_records INT64 NOT NULL,
  invalid_percentage NUMERIC NOT NULL,

  passed_rule_count INT64,
  warning_rule_count INT64,
  failed_rule_count INT64,

  batch_status STRING NOT NULL,

  evaluated_at TIMESTAMP NOT NULL
)
PARTITION BY DATE(evaluated_at)
CLUSTER BY pipeline_run_id, source_name, batch_status;