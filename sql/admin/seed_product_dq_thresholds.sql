DELETE FROM `still-resource-497715-g5.retail_audit_records.dq_rule_thresholds`
WHERE source_name = 'products'
  AND rule_name IN (
    'NULL_OR_BLANK_PRODUCT_ID',
    'DUPLICATE_PRODUCT_ID'
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
  'products',
  'NULL_OR_BLANK_PRODUCT_ID',
  'CRITICAL',
  0.50,
  5.00,
  10,
  'QUARANTINE_AND_CONTINUE',
  'FAIL_PIPELINE',
  TRUE,
  CURRENT_TIMESTAMP(),
  CURRENT_TIMESTAMP()
),
(
  'products',
  'DUPLICATE_PRODUCT_ID',
  'CRITICAL',
  0.50,
  5.00,
  10,
  'QUARANTINE_AND_CONTINUE',
  'FAIL_PIPELINE',
  TRUE,
  CURRENT_TIMESTAMP(),
  CURRENT_TIMESTAMP()
);


DELETE FROM `still-resource-497715-g5.retail_audit_records.dq_rule_thresholds`
WHERE source_name = 'products'
  AND rule_name = 'PRICE_LESS_THAN_EQUAL_ZERO';

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
  'products',
  'PRICE_LESS_THAN_EQUAL_ZERO',
  'CRITICAL',
  0.50,
  5.00,
  10,
  'QUARANTINE_AND_CONTINUE',
  'FAIL_PIPELINE',
  TRUE,
  CURRENT_TIMESTAMP(),
  CURRENT_TIMESTAMP()
);


DELETE FROM `still-resource-497715-g5.retail_audit_records.dq_rule_thresholds`
WHERE source_name = 'products'
  AND rule_name = 'NULL_OR_BLANK_CATEGORY';

INSERT INTO `still-resource-497715-g5.retail_audit_records.dq_rule_thresholds`
VALUES (
  'products',
  'NULL_OR_BLANK_CATEGORY',
  'MEDIUM',
  1.00,
  10.00,
  20,
  'QUARANTINE_AND_CONTINUE',
  'FAIL_PIPELINE',
  TRUE,
  CURRENT_TIMESTAMP(),
  CURRENT_TIMESTAMP()
);


DELETE FROM `still-resource-497715-g5.retail_audit_records.dq_rule_thresholds`
WHERE source_name = 'products'
  AND rule_name = 'FUTURE_CREATED_DATE';

INSERT INTO `still-resource-497715-g5.retail_audit_records.dq_rule_thresholds`
VALUES (
  'products',
  'FUTURE_CREATED_DATE',
  'HIGH',
  0.50,
  5.00,
  10,
  'QUARANTINE_AND_CONTINUE',
  'FAIL_PIPELINE',
  TRUE,
  CURRENT_TIMESTAMP(),
  CURRENT_TIMESTAMP()
);