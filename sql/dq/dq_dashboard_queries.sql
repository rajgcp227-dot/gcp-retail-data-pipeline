SELECT
  table_name,
  total_checks,
  passed_checks,
  failed_checks,
  overall_status
FROM `still-resource-497715-g5.retail_audit_records.dq_summary`
ORDER BY created_at DESC;