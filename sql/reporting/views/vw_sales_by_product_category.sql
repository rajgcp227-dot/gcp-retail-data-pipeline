-- =====================================================
-- vw_sales_by_product_category.sql
-- Retail Data Pipeline - Reporting View
-- Purpose: Product category-wise sales performance
-- =====================================================

CREATE OR REPLACE VIEW `still-resource-497715-g5.retail_reporting_records.vw_sales_by_product_category`
AS
SELECT
  product_category,

  COUNT(DISTINCT order_id) AS total_orders,
  COUNT(DISTINCT customer_id) AS total_customers,
  COUNT(DISTINCT product_id) AS total_products,
  COUNT(DISTINCT store_id) AS total_stores,

  SUM(quantity) AS total_quantity_sold,
  ROUND(SUM(sale_amount), 2) AS total_sales_amount,
  ROUND(SUM(discount_amount), 2) AS total_discount_amount,
  ROUND(SUM(tax_amount), 2) AS total_tax_amount,

  ROUND(AVG(sale_amount), 2) AS avg_order_value,
  ROUND(SUM(sale_amount) / NULLIF(COUNT(DISTINCT order_id), 0), 2) AS sales_per_order

FROM `still-resource-497715-g5.retail_reporting_records.vw_sales_detail`
GROUP BY product_category;