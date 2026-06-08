DELETE FROM `still-resource-497715-g5.retail_audit_records.dq_rule_thresholds`
WHERE source_name = 'stores'
  AND rule_name IN (
    'NULL_OR_BLANK_STORE_ID',
    'DUPLICATE_STORE_ID'
  );

INSERT INTO `still-resource-497715-g5.retail_audit_records.dq_rule_thresholds`
(
  source_name,
  rule_name,
  severity,
  warning_percentage,
  failure_percentage,
  max_failed_records,
  warning_action,
  failure_action,
  active_flag,
  created_at,
  updated_at
)
VALUES
(
  'stores',
  'NULL_OR_BLANK_STORE_ID',
  'CRITICAL',
  5.00,
  25.00,
  5,
  'QUARANTINE_AND_CONTINUE',
  'FAIL_PIPELINE',
  TRUE,
  CURRENT_TIMESTAMP(),
  CURRENT_TIMESTAMP()
),
(
  'stores',
  'DUPLICATE_STORE_ID',
  'CRITICAL',
  5.00,
  25.00,
  5,
  'QUARANTINE_AND_CONTINUE',
  'FAIL_PIPELINE',
  TRUE,
  CURRENT_TIMESTAMP(),
  CURRENT_TIMESTAMP()
);



DELETE FROM `still-resource-497715-g5.retail_audit_records.dq_rule_thresholds`
WHERE source_name = 'stores'
  AND rule_name = 'INVALID_REGION';

INSERT INTO `still-resource-497715-g5.retail_audit_records.dq_rule_thresholds`
(
  source_name,
  rule_name,
  severity,
  warning_percentage,
  failure_percentage,
  max_failed_records,
  warning_action,
  failure_action,
  active_flag,
  created_at,
  updated_at
)
VALUES (
  'stores',
  'INVALID_REGION',
  'HIGH',
  5.00,
  25.00,
  5,
  'QUARANTINE_AND_CONTINUE',
  'FAIL_PIPELINE',
  TRUE,
  CURRENT_TIMESTAMP(),
  CURRENT_TIMESTAMP()
);

DELETE FROM `still-resource-497715-g5.retail_audit_records.dq_rule_thresholds`
WHERE source_name = 'stores'
  AND rule_name = 'NULL_OR_BLANK_STORE_NAME';

INSERT INTO `still-resource-497715-g5.retail_audit_records.dq_rule_thresholds`
VALUES (
  'stores',
  'NULL_OR_BLANK_STORE_NAME',
  'MEDIUM',
  5.00,
  25.00,
  5,
  'QUARANTINE_AND_CONTINUE',
  'FAIL_PIPELINE',
  TRUE,
  CURRENT_TIMESTAMP(),
  CURRENT_TIMESTAMP()
);


DELETE FROM `still-resource-497715-g5.retail_audit_records.dq_rule_thresholds`
WHERE source_name = 'stores'
  AND rule_name = 'FUTURE_CREATED_DATE';

INSERT INTO `still-resource-497715-g5.retail_audit_records.dq_rule_thresholds`
VALUES (
  'stores',
  'FUTURE_CREATED_DATE',
  'HIGH',
  5.00,
  25.00,
  5,
  'QUARANTINE_AND_CONTINUE',
  'FAIL_PIPELINE',
  TRUE,
  CURRENT_TIMESTAMP(),
  CURRENT_TIMESTAMP()
);