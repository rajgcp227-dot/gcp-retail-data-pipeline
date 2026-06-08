ASSERT NOT EXISTS (
  SELECT 1
  FROM `still-resource-497715-g5.retail_audit_records.dq_batch_summary`
  WHERE pipeline_run_id = @pipeline_run_id
    AND source_name = 'sales'
    AND batch_status = 'FAIL'
)
AS 'DQ gate failed: sales batch exceeded configured thresholds';


SELECT
  pipeline_run_id,
  source_name,
  batch_status,
  valid_records,
  invalid_records,
  invalid_percentage
FROM `still-resource-497715-g5.retail_audit_records.dq_batch_summary`
WHERE pipeline_run_id = @pipeline_run_id
  AND source_name = 'sales';