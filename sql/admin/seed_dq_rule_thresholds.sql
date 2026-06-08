DELETE FROM `still-resource-497715-g5.retail_audit_records.dq_rule_thresholds`
WHERE source_name = 'sales'
  AND rule_name IN (
    'DUPLICATE_ORDER_ID',
    'INVALID_QUANTITY',
    'INVALID_SALE_AMOUNT'
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
  'sales',
  'DUPLICATE_ORDER_ID',
  'CRITICAL',
  0.00,
  0.20,
  10,
  'QUARANTINE_AND_CONTINUE',
  'FAIL_PIPELINE',
  TRUE,
  CURRENT_TIMESTAMP(),
  CURRENT_TIMESTAMP()
),
(
  'sales',
  'INVALID_QUANTITY',
  'CRITICAL',
  0.01,
  0.20,
  20,
  'QUARANTINE_AND_CONTINUE',
  'FAIL_PIPELINE',
  TRUE,
  CURRENT_TIMESTAMP(),
  CURRENT_TIMESTAMP()
),
(
  'sales',
  'INVALID_SALE_AMOUNT',
  'CRITICAL',
  0.01,
  0.20,
  20,
  'QUARANTINE_AND_CONTINUE',
  'FAIL_PIPELINE',
  TRUE,
  CURRENT_TIMESTAMP(),
  CURRENT_TIMESTAMP()
);




DELETE FROM `still-resource-497715-g5.retail_audit_records.dq_rule_thresholds`
WHERE source_name = 'sales'
  AND rule_name IN (
    'INVALID_SALE_DATE',
    'INVALID_PAYMENT_METHOD',
    'INVALID_CUSTOMER_FK'
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
  'sales', 'INVALID_SALE_DATE', 'HIGH',
  0.01, 0.20, 20,
  'QUARANTINE_AND_CONTINUE', 'FAIL_PIPELINE',
  TRUE, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()
),
(
  'sales', 'INVALID_PAYMENT_METHOD', 'HIGH',
  0.01, 0.50, 50,
  'QUARANTINE_AND_CONTINUE', 'FAIL_PIPELINE',
  TRUE, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()
),
(
  'sales', 'INVALID_CUSTOMER_FK', 'HIGH',
  0.05, 1.00, 100,
  'QUARANTINE_AND_CONTINUE', 'FAIL_PIPELINE',
  TRUE, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()    
);



DELETE FROM `still-resource-497715-g5.retail_audit_records.dq_rule_thresholds`
WHERE source_name = 'sales'
  AND rule_name IN (
    'INVALID_ORDER_ID',
    'INVALID_PRODUCT_FK',
    'INVALID_STORE_FK'
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
  'sales', 'INVALID_ORDER_ID', 'CRITICAL',
  0.00, 0.10, 5,
  'QUARANTINE_AND_CONTINUE', 'FAIL_PIPELINE',
  TRUE, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()
),
(
  'sales', 'INVALID_PRODUCT_FK', 'HIGH',
  0.05, 1.00, 100,
  'QUARANTINE_AND_CONTINUE', 'FAIL_PIPELINE',
  TRUE, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()
),
(
  'sales', 'INVALID_STORE_FK', 'HIGH',
  0.05, 1.00, 100,
  'QUARANTINE_AND_CONTINUE', 'FAIL_PIPELINE',
  TRUE, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()
);