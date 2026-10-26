CREATE OR REPLACE VIEW gold.v_enriched_orders AS
SELECT
    o.order_id,
    o.order_date,
    EXTRACT(YEAR FROM o.order_date)              AS order_year,
    EXTRACT(MONTH FROM o.order_date)             AS order_month,
    o.customer_id,
    c.customer_name,
    c.country,
    c.city,
    c.segment,
    o.product_id,
    p.product_name,
    p.category,
    p.subcategory,
    o.quantity,
    o.unit_price,
    o.status,
    o.channel,
    (o.quantity * o.unit_price)::DECIMAL(16,2)    AS gross_amount,
    (o.quantity * p.cost_price)::DECIMAL(16,2)    AS cost_amount,
    ((o.quantity * o.unit_price) - (o.quantity * p.cost_price))::DECIMAL(16,2) AS margin_amount
FROM silver.orders   o
JOIN silver.customers c ON o.customer_id = c.customer_id
JOIN silver.products  p ON o.product_id  = p.product_id;

1) DAILY SALES BY CATEGORY


CREATE TABLE gold.agg_daily_sales_by_category AS
SELECT
    order_date,
    category,
    subcategory,
    COUNT(DISTINCT order_id)                     AS order_count,
    SUM(quantity)                                 AS units_sold,
    SUM(gross_amount)::DECIMAL(16,2)             AS revenue,
    SUM(cost_amount)::DECIMAL(16,2)              AS cost,
    SUM(margin_amount)::DECIMAL(16,2)            AS margin
FROM gold.v_enriched_orders
GROUP BY order_date, category, subcategory
ORDER BY order_date, category;

2) MONTHLY REVENUE BY COUNTRY AND SEGMENT

CREATE TABLE gold.agg_monthly_revenue_by_country AS
SELECT
    order_year,
    order_month,
    country,
    segment,
    COUNT(DISTINCT order_id)                     AS order_count,
    COUNT(DISTINCT customer_id)                  AS active_customers,
    SUM(gross_amount)::DECIMAL(16,2)             AS revenue,
    (SUM(gross_amount) / NULLIF(COUNT(DISTINCT order_id), 0))::DECIMAL(14,2)
                                                 AS avg_order_value
FROM gold.v_enriched_orders
GROUP BY order_year, order_month, country, segment
ORDER BY order_year, order_month, country;


3) PRODUCT PERFORMANCE RANKING (TOP PRODUCTS)
CREATE TABLE gold.top_products AS
SELECT
    product_id,
    product_name,
    category,
    SUM(quantity)                                 AS units_sold,
    SUM(gross_amount)::DECIMAL(16,2)             AS revenue,
    SUM(margin_amount)::DECIMAL(16,2)            AS margin,
    COUNT(DISTINCT customer_id)                  AS unique_buyers,
    (SUM(margin_amount) / NULLIF(SUM(gross_amount), 0) * 100)::DECIMAL(6,2)
                                                 AS margin_pct
FROM gold.v_enriched_orders
GROUP BY product_id, product_name, category
ORDER BY revenue DESC;
