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