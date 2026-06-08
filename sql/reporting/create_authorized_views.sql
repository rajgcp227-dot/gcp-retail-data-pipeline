CREATE OR REPLACE VIEW
`still-resource-497715-g5.retail_secure_views.vw_customer_masked`
AS
SELECT
  customer_sk,
  customer_id,

  CONCAT(SUBSTR(customer_name, 1, 1), '***') AS masked_customer_name,

  CASE
    WHEN email IS NULL THEN NULL
    ELSE REGEXP_REPLACE(email, r'(^.).*(@.*$)', r'\1***\2')
  END AS masked_email,

  city,
  state,
  created_date
FROM `still-resource-497715-g5.retail_reporting_records.dim_customer`;

-- =====================================================
-- create_authorized_views.sql
-- Retail Data Pipeline - Secure Reporting Views
-- Purpose: Mask PII and expose secure reporting views
-- Dataset: retail_secure_views
-- =====================================================

CREATE SCHEMA IF NOT EXISTS `still-resource-497715-g5.retail_secure_views`
OPTIONS (
  location = 'asia-south1'
);

CREATE OR REPLACE VIEW
`still-resource-497715-g5.retail_secure_views.vw_customer_masked`
AS
SELECT
  customer_sk,
  customer_id,
  CONCAT(SUBSTR(customer_name, 1, 1), '***') AS masked_customer_name,
  CASE
    WHEN email IS NULL THEN NULL
    ELSE REGEXP_REPLACE(email, r'(^.).*(@.*$)', r'\1***\2')
  END AS masked_email,
  city,
  state,
  created_date
FROM `still-resource-497715-g5.retail_reporting_records.dim_customer`;


CREATE OR REPLACE VIEW
`still-resource-497715-g5.retail_secure_views.vw_sales_detail_secure`
AS
SELECT
  f.order_id,
  f.sale_date,
  f.quantity,
  f.unit_price,
  f.discount_amount,
  f.tax_amount,
  f.sale_amount,
  f.payment_method,
  

  c.customer_id,
  CONCAT(SUBSTR(c.customer_name, 1, 1), '***') AS masked_customer_name,
  c.city AS customer_city,
  c.state AS customer_state,

  p.product_id,
  p.product_name,
  p.category AS product_category,
  p.brand AS product_brand,

  st.store_id,
  st.store_name,
  st.city AS store_city,
  st.state AS store_state,
  st.region AS store_region

FROM `still-resource-497715-g5.retail_reporting_records.fact_sales` f
LEFT JOIN `still-resource-497715-g5.retail_reporting_records.dim_customer` c
  ON f.customer_sk = c.customer_sk
LEFT JOIN `still-resource-497715-g5.retail_reporting_records.dim_product` p
  ON f.product_sk = p.product_sk
LEFT JOIN `still-resource-497715-g5.retail_reporting_records.dim_store` st
  ON f.store_sk = st.store_sk;


CREATE OR REPLACE VIEW
`still-resource-497715-g5.retail_secure_views.vw_sales_summary_secure`
AS
SELECT
  sale_date,
  sale_channel,
  payment_method,
  COUNT(DISTINCT order_id) AS total_orders,
  SUM(quantity) AS total_quantity_sold,
  ROUND(SUM(sale_amount), 2) AS total_sales_amount
FROM `still-resource-497715-g5.retail_reporting_records.fact_sales`
GROUP BY
  sale_date,
  sale_channel,
  payment_method;







  -- =====================================================
-- create_authorized_views.sql
-- Retail Data Pipeline - Secure / Authorized Views
-- Purpose:
--   Create secure reporting views for business users
--   with customer PII masking.
--
-- Note:
--   This SQL creates secure views.
--   Actual dataset authorization/IAM must be granted
--   using BigQuery UI, bq command, Terraform, or IAM.
-- =====================================================

CREATE SCHEMA IF NOT EXISTS `still-resource-497715-g5.retail_secure_views`
OPTIONS (
  location = 'asia-south1'
);

-- =====================================================
-- 1. Customer masked view
-- =====================================================

CREATE OR REPLACE VIEW `still-resource-497715-g5.retail_secure_views.vw_customer_masked`
AS
SELECT
  customer_sk,
  customer_id,

  CASE
    WHEN customer_name IS NULL THEN NULL
    WHEN LENGTH(TRIM(customer_name)) = 0 THEN NULL
    ELSE CONCAT(SUBSTR(customer_name, 1, 1), '***')
  END AS masked_customer_name,

  CASE
    WHEN email IS NULL THEN NULL
    WHEN STRPOS(email, '@') = 0 THEN 'INVALID_EMAIL'
    ELSE REGEXP_REPLACE(email, r'(^.).*(@.*$)', r'\1***\2')
  END AS masked_email,

  city,
  state,
  created_date,
  reporting_created_at,
  reporting_updated_at
FROM `still-resource-497715-g5.retail_reporting_records.dim_customer`;


-- =====================================================
-- 2. Secure sales detail view
-- =====================================================

CREATE OR REPLACE VIEW `still-resource-497715-g5.retail_secure_views.vw_sales_detail_secure`
AS
SELECT
 
  f.order_id,
  f.sale_date,

  f.quantity,
  f.unit_price,
  f.discount_amount,
  f.tax_amount,
  f.sale_amount,
  f.payment_method,

  -- If sale_channel does not exist in fact_sales, remove this line.
 

  c.customer_id,

  CASE
    WHEN c.customer_name IS NULL THEN NULL
    WHEN LENGTH(TRIM(c.customer_name)) = 0 THEN NULL
    ELSE CONCAT(SUBSTR(c.customer_name, 1, 1), '***')
  END AS masked_customer_name,

  CASE
    WHEN c.email IS NULL THEN NULL
    WHEN STRPOS(c.email, '@') = 0 THEN 'INVALID_EMAIL'
    ELSE REGEXP_REPLACE(c.email, r'(^.).*(@.*$)', r'\1***\2')
  END AS masked_email,

  c.city AS customer_city,
  c.state AS customer_state,

  p.product_id,
  p.product_name,
  p.category AS product_category,
  p.brand AS product_brand,

  s.store_id,
  s.store_name,
  s.city AS store_city,
  s.state AS store_state,
  s.region AS store_region,

  f.source_file_name,
  f.batch_id,
  f.load_timestamp,
  f.reporting_created_at,
  f.reporting_updated_at

FROM `still-resource-497715-g5.retail_reporting_records.fact_sales` f
LEFT JOIN `still-resource-497715-g5.retail_reporting_records.dim_customer` c
  ON f.customer_sk = c.customer_sk
LEFT JOIN `still-resource-497715-g5.retail_reporting_records.dim_product` p
  ON f.product_sk = p.product_sk
LEFT JOIN `still-resource-497715-g5.retail_reporting_records.dim_store` s
  ON f.store_sk = s.store_sk;


-- =====================================================
-- 3. Secure sales summary view
-- =====================================================

CREATE OR REPLACE VIEW `still-resource-497715-g5.retail_secure_views.vw_sales_summary_secure`
AS
SELECT
  sale_date,

  -- If sale_channel does not exist in fact_sales, remove this line.

  payment_method,
  COUNT(DISTINCT order_id) AS total_orders,
  SUM(quantity) AS total_quantity,
  ROUND(SUM(sale_amount), 2) AS total_sales_amount,
  ROUND(AVG(sale_amount), 2) AS average_order_value
FROM `still-resource-497715-g5.retail_reporting_records.fact_sales`
GROUP BY
  sale_date,
  payment_method;