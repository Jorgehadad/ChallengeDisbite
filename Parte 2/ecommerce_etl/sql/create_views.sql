-- Views and indexes to support analytics and the challenge queries
-- Materialized view: per-product performance (revenue, buyers, avg rating)
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_product_performance AS
SELECT
    p.product_id,
    p.title,
    p.category,
    SUM(f.total_amount) AS revenue,
    SUM(f.quantity) AS total_quantity,
    COUNT(DISTINCT f.user_id) AS unique_buyers,
    AVG(p.rating_rate) AS avg_rating
FROM dim_products p
JOIN fact_sales f ON p.product_id = f.product_id
GROUP BY p.product_id, p.title, p.category;

-- Standard non-materialized view for quick ad-hoc queries (keeps data always up-to-date)
CREATE OR REPLACE VIEW vw_product_sales AS
SELECT
    p.product_id,
    p.title,
    p.category,
    SUM(f.total_amount) AS revenue,
    SUM(f.quantity) AS total_quantity,
    COUNT(DISTINCT f.user_id) AS unique_buyers,
    AVG(p.rating_rate) AS avg_rating
FROM dim_products p
JOIN fact_sales f ON p.product_id = f.product_id
GROUP BY p.product_id, p.title, p.category;

-- Helpful indexes to accelerate aggregations and joins
CREATE INDEX IF NOT EXISTS idx_fact_sales_product_id ON fact_sales(product_id);
CREATE INDEX IF NOT EXISTS idx_fact_sales_date_key ON fact_sales(date_key);
CREATE INDEX IF NOT EXISTS idx_fact_sales_user_id ON fact_sales(user_id);
CREATE INDEX IF NOT EXISTS idx_dim_products_category ON dim_products(category);

-- Index on materialized view to allow CONCURRENT refresh (if desired) and to speed up ordering by revenue
CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_product_performance_product_id ON mv_product_performance(product_id);
CREATE INDEX IF NOT EXISTS idx_mv_product_performance_revenue ON mv_product_performance(revenue);

-- Notes:
-- - Refresh the materialized view periodically (REFRESH MATERIALIZED VIEW mv_product_performance).
-- - The materialized view is useful for BI dashboards where near-real-time is not required.
