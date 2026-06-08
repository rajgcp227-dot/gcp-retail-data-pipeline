DECLARE v_run_id STRING DEFAULT @pipeline_run_id;

-- =====================================================
-- check_dq_gate.sql
-- Retail Data Pipeline - DQ Gate
-- Purpose:
--   Stop the pipeline if any mandatory source has missing
--   DQ summary or failed DQ batch status.
--
-- Sources checked:
--   customers, products, stores, sales
-- =====================================================


-- =====================================================
-- STEP 1: Ensure all required source summaries exist
-- =====================================================

ASSERT (
  SELECT COUNT(DISTINCT source_name)
  FROM `still-resource-497715-g5.retail_audit_records.dq_batch_summary`
  WHERE pipeline_run_id = v_run_id
    AND source_name IN ('customers', 'products', 'stores', 'sales')
) = 4
AS 'DQ gate failed: one or more source DQ batch summaries are missing';


-- =====================================================
-- STEP 2: Stop pipeline if any source has FAIL status
-- =====================================================

ASSERT NOT EXISTS (
  SELECT 1
  FROM `still-resource-497715-g5.retail_audit_records.dq_batch_summary`
  WHERE pipeline_run_id = v_run_id
    AND source_name IN ('customers', 'products', 'stores', 'sales')
    AND batch_status = 'FAIL'
)
AS 'DQ gate failed: one or more source batches exceeded configured thresholds';


-- =====================================================
-- STEP 3: Display final DQ gate result
-- =====================================================

SELECT
  pipeline_run_id,
  source_name,
  total_records,
  valid_records,
  invalid_records,
  invalid_percentage,
  passed_rule_count,
  warning_rule_count,
  failed_rule_count,
  batch_status,
  evaluated_at
FROM `still-resource-497715-g5.retail_audit_records.dq_batch_summary`
WHERE pipeline_run_id = v_run_id
  AND source_name IN ('customers', 'products', 'stores', 'sales')
ORDER BY source_name;