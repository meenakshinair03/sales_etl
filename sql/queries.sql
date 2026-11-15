-- ============================================================
-- Reporting queries against the star schema in data/sales.db
-- Tables: fact_sales, dim_date, dim_product, dim_region, dim_customer
-- ============================================================

-- 1. Total revenue and orders by month (revenue trend)
SELECT
    d.year,
    d.month,
    d.month_name,
    ROUND(SUM(f.revenue), 2) AS total_revenue,
    COUNT(DISTINCT f.order_id) AS total_orders
FROM fact_sales f
JOIN dim_date d ON f.date_id = d.date_id
GROUP BY d.year, d.month
ORDER BY d.year, d.month;


-- 2. Top 10 products by revenue
SELECT
    p.product_name,
    p.category,
    ROUND(SUM(f.revenue), 2) AS total_revenue,
    SUM(f.quantity) AS units_sold
FROM fact_sales f
JOIN dim_product p ON f.product_id = p.product_id
GROUP BY p.product_id
ORDER BY total_revenue DESC
LIMIT 10;


-- 3. Revenue by region
SELECT
    r.region_name,
    ROUND(SUM(f.revenue), 2) AS total_revenue,
    COUNT(DISTINCT f.order_id) AS total_orders,
    ROUND(SUM(f.revenue) * 1.0 / COUNT(DISTINCT f.order_id), 2) AS avg_order_value
FROM fact_sales f
JOIN dim_region r ON f.region_id = r.region_id
GROUP BY r.region_id
ORDER BY total_revenue DESC;


-- 4. Revenue by category and quarter (pivot-style reporting)
SELECT
    d.year,
    d.quarter,
    p.category,
    ROUND(SUM(f.revenue), 2) AS total_revenue
FROM fact_sales f
JOIN dim_date d ON f.date_id = d.date_id
JOIN dim_product p ON f.product_id = p.product_id
GROUP BY d.year, d.quarter, p.category
ORDER BY d.year, d.quarter, total_revenue DESC;


-- 5. Top 10 customers by total spend
SELECT
    c.customer_name,
    ROUND(SUM(f.revenue), 2) AS total_spend,
    COUNT(DISTINCT f.order_id) AS orders_placed
FROM fact_sales f
JOIN dim_customer c ON f.customer_id = c.customer_id
GROUP BY c.customer_id
ORDER BY total_spend DESC
LIMIT 10;


-- 6. Region x Category revenue matrix (multi-dimension join)
SELECT
    r.region_name,
    p.category,
    ROUND(SUM(f.revenue), 2) AS total_revenue
FROM fact_sales f
JOIN dim_region r ON f.region_id = r.region_id
JOIN dim_product p ON f.product_id = p.product_id
GROUP BY r.region_name, p.category
ORDER BY r.region_name, total_revenue DESC;
