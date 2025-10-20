-- Query problemático
SELECT
    p.title,
    p.category,
    COUNT(DISTINCT c.user_id) as unique_buyers,
    SUM(ci.quantity * p.price) as revenue,
    AVG(p.rating_rate) as avg_rating
FROM products p
    JOIN cart_items ci ON ci.product_id = p.id
    JOIN carts c ON c.id = ci.cart_id
GROUP BY p.title, p.category
ORDER BY revenue DESC;

-- Analysis of performance issues:
-- 1) GROUP BY on non-key/text fields (p.title, p.category) prevents efficient index usage and may
--    require sorting large intermediate datasets. Prefer grouping by product_id which is small and indexed.
-- 2) COUNT(DISTINCT c.user_id) can be expensive. Consider pre-aggregating or using a materialized view
--    that stores per-product aggregates.
-- 3) Ensure appropriate indexes exist on join/filter columns: cart_items(product_id), cart_items(cart_id), carts(id,user_id), products(id, price, rating_rate, category)
-- 4) Use numeric PKs in GROUP BY and join back to products for description/title in outer query.

-- Recommended indexes:
-- CREATE INDEX idx_ci_product_id ON cart_items(product_id);
-- CREATE INDEX idx_ci_cart_id ON cart_items(cart_id);
-- CREATE INDEX idx_carts_user_id ON carts(user_id);
-- CREATE INDEX idx_products_price ON products(price);

-- Materialized view approach (recommended for dashboards):
-- CREATE MATERIALIZED VIEW mv_product_metrics AS
-- SELECT
--   p.id AS product_id,
--   SUM(ci.quantity * p.price) AS revenue,
--   SUM(ci.quantity) AS total_quantity,
--   COUNT(DISTINCT c.user_id) AS unique_buyers,
--   AVG(p.rating_rate) AS avg_rating
-- FROM products p
-- JOIN cart_items ci ON ci.product_id = p.id
-- JOIN carts c ON c.id = ci.cart_id
-- GROUP BY p.id;
-- Then index the materialized view for fast ordering:
-- CREATE INDEX idx_mv_revenue ON mv_product_metrics(revenue DESC);

-- Optimized query (prefer this over grouping by title/category):
SELECT
    p.title,
    p.category,
    pm.unique_buyers,
    pm.revenue,
    pm.avg_rating
FROM (
    SELECT
        ci.product_id,
        SUM(ci.quantity * p.price) AS revenue,
        COUNT(DISTINCT c.user_id) AS unique_buyers,
        AVG(p.rating_rate) AS avg_rating
    FROM cart_items ci
    JOIN carts c ON c.id = ci.cart_id
    JOIN products p ON p.id = ci.product_id
    GROUP BY ci.product_id
) pm
JOIN products p ON p.id = pm.product_id
ORDER BY pm.revenue DESC;

-- Expected improvements:
-- - Grouping by integer product_id reduces memory and sort costs.
-- - Proper indexes on cart_items(product_id, cart_id) and carts(user_id) will speed up joins and reduce I/O.
-- - Using a materialized view can turn an expensive aggregation into a fast indexed read for dashboards.
