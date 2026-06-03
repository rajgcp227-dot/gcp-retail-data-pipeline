-- =====================================================
-- create_pipeline_log_table.sql
-- Retail Data Pipeline - Pipeline Run Logging Table
-- Dataset: retail_audit_records
-- =====================================================

CREATE TABLE IF NOT EXISTS `still-resource-497715-g5.retail_audit_records.pipeline_run_log`
(
  log_id STRING,
  dag_id STRING,
  task_id STRING,
  run_id STRING,
  execution_date TIMESTAMP,
  status STRING,
  message STRING,
  error_message STRING,
  project_id STRING,
  environment STRING,
  created_at TIMESTAMP
)
PARTITION BY DATE(created_at)
CLUSTER BY dag_id, task_id, status;