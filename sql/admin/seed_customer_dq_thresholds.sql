DELETE FROM `still-resource-497715-g5.retail_audit_records.dq_rule_thresholds`
WHERE source_name = 'customers'
  AND rule_name IN (
    'NULL_OR_BLANK_CUSTOMER_ID',
    'DUPLICATE_CUSTOMER_ID'
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
  'customers',
  'NULL_OR_BLANK_CUSTOMER_ID',
  'CRITICAL',
  0.10,
  2.00,
  20,
  'QUARANTINE_AND_CONTINUE',
  'FAIL_PIPELINE',
  TRUE,
  CURRENT_TIMESTAMP(),
  CURRENT_TIMESTAMP()
),
(
  'customers',
  'DUPLICATE_CUSTOMER_ID',
  'CRITICAL',
  0.10,
  2.00,
  20,
  'QUARANTINE_AND_CONTINUE',
  'FAIL_PIPELINE',
  TRUE,
  CURRENT_TIMESTAMP(),
  CURRENT_TIMESTAMP()
);



DELETE FROM `still-resource-497715-g5.retail_audit_records.dq_rule_thresholds`
WHERE source_name = 'customers'
  AND rule_name = 'INVALID_EMAIL_FORMAT';

INSERT INTO `still-resource-497715-g5.retail_audit_records.dq_rule_thresholds`
VALUES (
  'customers',
  'INVALID_EMAIL_FORMAT',
  'MEDIUM',
  1.00,
  5.00,
  100,
  'QUARANTINE_AND_CONTINUE',
  'FAIL_PIPELINE',
  TRUE,
  CURRENT_TIMESTAMP(),
  CURRENT_TIMESTAMP()
);

DELETE FROM `still-resource-497715-g5.retail_audit_records.dq_rule_thresholds`
WHERE source_name = 'customers'
  AND rule_name = 'NULL_OR_BLANK_CUSTOMER_NAME';

INSERT INTO `still-resource-497715-g5.retail_audit_records.dq_rule_thresholds`
VALUES (
  'customers',
  'NULL_OR_BLANK_CUSTOMER_NAME',
  'MEDIUM',
  1.00,
  5.00,
  100,
  'QUARANTINE_AND_CONTINUE',
  'FAIL_PIPELINE',
  TRUE,
  CURRENT_TIMESTAMP(),
  CURRENT_TIMESTAMP()
);

DELETE FROM `still-resource-497715-g5.retail_audit_records.dq_rule_thresholds`
WHERE source_name = 'customers'
  AND rule_name = 'FUTURE_CREATED_DATE';

INSERT INTO `still-resource-497715-g5.retail_audit_records.dq_rule_thresholds`
VALUES (
  'customers',
  'FUTURE_CREATED_DATE',
  'HIGH',
  0.10,
  2.00,
  20,
  'QUARANTINE_AND_CONTINUE',
  'FAIL_PIPELINE',
  TRUE,
  CURRENT_TIMESTAMP(),
  CURRENT_TIMESTAMP()
);