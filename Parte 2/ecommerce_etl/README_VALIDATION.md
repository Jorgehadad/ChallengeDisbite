Validation and QA steps for dimensions, facts and analytics views

1) Initialize database (creates tables, views and indexes):

   python src/init_db.py

2) Run ETL locally (will extract from API or cache, transform, validate and load):

   python main.py --force-refresh

3) Refresh materialized views (after loading new data):

   python scripts/refresh_mv.py

Useful SQL snippets for validation (connect to your Postgres DB and run):

-- Check counts per table
SELECT 'dim_date' AS table_name, COUNT(*) FROM dim_date
UNION ALL
SELECT 'dim_products', COUNT(*) FROM dim_products
UNION ALL
SELECT 'dim_users', COUNT(*) FROM dim_users
UNION ALL
SELECT 'dim_geography', COUNT(*) FROM dim_geography
UNION ALL
SELECT 'fact_sales', COUNT(*) FROM fact_sales;

-- Sample data from each table
SELECT * FROM dim_products ORDER BY product_id LIMIT 5;
SELECT * FROM dim_users ORDER BY user_id LIMIT 5;
SELECT * FROM fact_sales ORDER BY sale_id DESC LIMIT 10;

-- Validate referential integrity: sales referencing missing products/users
SELECT f.* FROM fact_sales f
LEFT JOIN dim_products p ON f.product_id = p.product_id
LEFT JOIN dim_users u ON f.user_id = u.user_id
WHERE p.product_id IS NULL OR u.user_id IS NULL
LIMIT 50;

-- Top products by revenue (materialized view)
SELECT * FROM mv_product_performance ORDER BY revenue DESC LIMIT 20;

-- If mv empty or outdated, refresh
REFRESH MATERIALIZED VIEW CONCURRENTLY mv_product_performance;

Notes:
- The materialized view is optimized for dashboard queries. For up-to-date results, run the refresh script after ETL.
- Indexes on fact_sales(product_id, date_key, user_id) accelerate joins and aggregations.
