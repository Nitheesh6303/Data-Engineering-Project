-- Data mart analysis queries

-- 1. Revenue by country
SELECT *
FROM mart.sales_by_country
ORDER BY total_revenue DESC;

-- 2. Revenue by product category
SELECT *
FROM mart.sales_by_category
ORDER BY total_revenue DESC;

-- 3. Monthly revenue trend
SELECT *
FROM mart.monthly_revenue
ORDER BY year, month;

-- 4. Top customers by revenue
SELECT *
FROM mart.customer_revenue
ORDER BY total_revenue DESC;

-- 5. Pipeline audit history
SELECT *
FROM audit.pipeline_runs
ORDER BY started_at DESC;

-- 6. Data quality checks
SELECT *
FROM audit.quality_checks
ORDER BY checked_at DESC;
