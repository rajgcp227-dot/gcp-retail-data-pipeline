-- =====================================================
-- vw_sales_detail.sql
-- Retail Data Pipeline - Reporting View
-- Purpose: Transaction-level sales detail for Power BI
-- =====================================================

CREATE OR REPLACE VIEW `still-resource-497715-g5.retail_reporting_records.vw_sales_detail`
AS
SELECT
  f.order_id,
  f.sales_sk,

  f.sale_date,
  f.quantity,
  f.unit_price,
  f.discount_amount,
  f.tax_amount,
  f.sale_amount,
  f.payment_method,
  f.coupon_code,
  f.sale_channel,

  c.customer_sk,
  c.customer_id,
  c.customer_name,
  c.email,
  c.city AS customer_city,
  c.state AS customer_state,

  p.product_sk,
  p.product_id,
  p.product_name,
  p.category AS product_category,
  p.brand AS product_brand,
  p.price AS product_price,

  st.store_sk,
  st.store_id,
  st.store_name,
  st.city AS store_city,
  st.state AS store_state,
  st.region AS store_region,

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

LEFT JOIN `still-resource-497715-g5.retail_reporting_records.dim_store` st
  ON f.store_sk = st.store_sk;