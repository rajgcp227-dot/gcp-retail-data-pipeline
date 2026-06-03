-- =====================================================
-- create_materialized_views.sql
-- Retail Data Pipeline - Reporting Layer
-- Purpose: Fast aggregated views for dashboard use cases
-- Source : retail_reporting_records.fact_sales
-- =====================================================

-- Daily Sales Summary
CREATE MATERIALIZED VIEW IF NOT EXISTS
`still-resource-497715-g5.retail_reporting_records.mv_daily_sales_summary`
PARTITION BY sale_date
CLUSTER BY sale_date
AS
SELECT
  sale_date,
  COUNT(1) AS total_orders,
  SUM(quantity) AS total_quantity_sold,
  SUM(sale_amount) AS total_sales_amount,
  SUM(discount_amount) AS total_discount_amount,
  SUM(tax_amount) AS total_tax_amount
FROM `still-resource-497715-g5.retail_reporting_records.fact_sales`
GROUP BY sale_date;


-- Daily Product Sales Summary
CREATE MATERIALIZED VIEW IF NOT EXISTS
`still-resource-497715-g5.retail_reporting_records.mv_daily_product_sales`
PARTITION BY sale_date
CLUSTER BY product_sk
AS
SELECT
  sale_date,
  product_sk,
  COUNT(1) AS total_orders,
  SUM(quantity) AS total_quantity_sold,
  SUM(sale_amount) AS total_sales_amount,
  SUM(discount_amount) AS total_discount_amount,
  SUM(tax_amount) AS total_tax_amount
FROM `still-resource-497715-g5.retail_reporting_records.fact_sales`
GROUP BY sale_date, product_sk;


-- Daily Store Sales Summary
CREATE MATERIALIZED VIEW IF NOT EXISTS
`still-resource-497715-g5.retail_reporting_records.mv_daily_store_sales`
PARTITION BY sale_date
CLUSTER BY store_sk
AS
SELECT
  sale_date,
  store_sk,
  COUNT(1) AS total_orders,
  SUM(quantity) AS total_quantity_sold,
  SUM(sale_amount) AS total_sales_amount,
  SUM(discount_amount) AS total_discount_amount,
  SUM(tax_amount) AS total_tax_amount
FROM `still-resource-497715-g5.retail_reporting_records.fact_sales`
GROUP BY sale_date, store_sk;


-- Daily Customer Sales Summary
CREATE MATERIALIZED VIEW IF NOT EXISTS
`still-resource-497715-g5.retail_reporting_records.mv_daily_customer_sales`
PARTITION BY sale_date
CLUSTER BY customer_sk
AS
SELECT
  sale_date,
  customer_sk,
  COUNT(1) AS total_orders,
  SUM(quantity) AS total_quantity_sold,
  SUM(sale_amount) AS total_sales_amount
FROM `still-resource-497715-g5.retail_reporting_records.fact_sales`
GROUP BY sale_date, customer_sk;


-- =====================================================
-- Enriched Views on top of Materialized Views
-- =====================================================

CREATE OR REPLACE VIEW
`still-resource-497715-g5.retail_reporting_records.vw_mv_daily_product_sales`
AS
SELECT
  mv.sale_date,
  p.product_id,
  p.product_name,
  p.category,
  p.brand,
  mv.total_orders,
  mv.total_quantity_sold,
  mv.total_sales_amount,
  mv.total_discount_amount,
  mv.total_tax_amount
FROM `still-resource-497715-g5.retail_reporting_records.mv_daily_product_sales` mv
LEFT JOIN `still-resource-497715-g5.retail_reporting_records.dim_product` p
  ON mv.product_sk = p.product_sk;


CREATE OR REPLACE VIEW
`still-resource-497715-g5.retail_reporting_records.vw_mv_daily_store_sales`
AS
SELECT
  mv.sale_date,
  st.store_id,
  st.store_name,
  st.city AS store_city,
  st.state AS store_state,
  st.region AS store_region,
  mv.total_orders,
  mv.total_quantity_sold,
  mv.total_sales_amount,
  mv.total_discount_amount,
  mv.total_tax_amount
FROM `still-resource-497715-g5.retail_reporting_records.mv_daily_store_sales` mv
LEFT JOIN `still-resource-497715-g5.retail_reporting_records.dim_store` st
  ON mv.store_sk = st.store_sk;