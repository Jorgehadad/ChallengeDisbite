-- ============================
-- TABLAS MAESTRAS (CATÁLOGOS)
-- ============================

-- Categorías de productos
CREATE TABLE categories (
    category_id SERIAL PRIMARY KEY,
    category_name VARCHAR(100) UNIQUE NOT NULL,
    description TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Usuarios del sistema
CREATE TABLE users (
    user_id SERIAL PRIMARY KEY,
    email VARCHAR(255) UNIQUE NOT NULL,
    username VARCHAR(100) UNIQUE NOT NULL,
    password_hash VARCHAR(255) NOT NULL, -- En producción usar bcrypt
    first_name VARCHAR(100) NOT NULL,
    last_name VARCHAR(100) NOT NULL,
    phone VARCHAR(50),
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Direcciones de usuarios
CREATE TABLE user_addresses (
    address_id SERIAL PRIMARY KEY,
    user_id INT NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,
    address_type VARCHAR(20) DEFAULT 'shipping', -- shipping, billing
    street VARCHAR(255) NOT NULL,
    city VARCHAR(100) NOT NULL,
    state VARCHAR(100),
    zipcode VARCHAR(20),
    country VARCHAR(100) DEFAULT 'US',
    lat NUMERIC(10,6),
    lng NUMERIC(10,6),
    is_default BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Productos
CREATE TABLE products (
    product_id SERIAL PRIMARY KEY,
    category_id INT REFERENCES categories(category_id),
    title VARCHAR(255) NOT NULL,
    description TEXT,
    price NUMERIC(10,2) NOT NULL CHECK (price >= 0),
    image_url VARCHAR(500),
    stock_quantity INT NOT NULL DEFAULT 0 CHECK (stock_quantity >= 0),
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Ratings de productos
CREATE TABLE product_ratings (
    rating_id SERIAL PRIMARY KEY,
    product_id INT NOT NULL REFERENCES products(product_id) ON DELETE CASCADE,
    user_id INT NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,
    rating_value NUMERIC(3,2) NOT NULL CHECK (rating_value BETWEEN 0 AND 5),
    review_text TEXT,
    is_verified_purchase BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(product_id, user_id) -- Un usuario solo puede rating una vez por producto
);

-- ============================
-- TABLAS TRANSACCIONALES
-- ============================

-- Carritos de compra
CREATE TABLE carts (
    cart_id SERIAL PRIMARY KEY,
    user_id INT NOT NULL REFERENCES users(user_id),
    session_id VARCHAR(100), -- Para usuarios no logueados
    status VARCHAR(20) DEFAULT 'active', -- active, completed, abandoned
    shipping_address_id INT REFERENCES user_addresses(address_id),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CHECK (user_id IS NOT NULL OR session_id IS NOT NULL) -- Al menos uno debe existir
);

-- Ítems del carrito
CREATE TABLE cart_items (
    cart_item_id SERIAL PRIMARY KEY,
    cart_id INT NOT NULL REFERENCES carts(cart_id) ON DELETE CASCADE,
    product_id INT NOT NULL REFERENCES products(product_id),
    quantity INT NOT NULL CHECK (quantity > 0),
    unit_price NUMERIC(10,2) NOT NULL CHECK (unit_price >= 0), -- Precio al momento de agregar
    added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(cart_id, product_id) -- Un producto solo una vez por carrito
);

-- Órdenes de compra
CREATE TABLE orders (
    order_id SERIAL PRIMARY KEY,
    cart_id INT NOT NULL UNIQUE REFERENCES carts(cart_id),
    user_id INT NOT NULL REFERENCES users(user_id),
    order_number VARCHAR(50) UNIQUE NOT NULL,
    status VARCHAR(20) DEFAULT 'pending', -- pending, confirmed, shipped, delivered, cancelled
    subtotal_amount NUMERIC(12,2) NOT NULL CHECK (subtotal_amount >= 0),
    tax_amount NUMERIC(10,2) NOT NULL CHECK (tax_amount >= 0),
    shipping_amount NUMERIC(10,2) NOT NULL CHECK (shipping_amount >= 0),
    total_amount NUMERIC(12,2) NOT NULL CHECK (total_amount >= 0),
    shipping_address_id INT NOT NULL REFERENCES user_addresses(address_id),
    payment_method VARCHAR(50),
    payment_status VARCHAR(20) DEFAULT 'pending',
    ordered_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    delivered_at TIMESTAMP
);

-- Líneas de orden (snapshot de los items al momento de la compra)
CREATE TABLE order_items (
    order_item_id SERIAL PRIMARY KEY,
    order_id INT NOT NULL REFERENCES orders(order_id) ON DELETE CASCADE,
    product_id INT NOT NULL REFERENCES products(product_id),
    product_title VARCHAR(255) NOT NULL, -- Snapshot del título
    product_price NUMERIC(10,2) NOT NULL CHECK (product_price >= 0), -- Snapshot del precio
    quantity INT NOT NULL CHECK (quantity > 0),
    line_total NUMERIC(12,2) NOT NULL CHECK (line_total >= 0),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ============================
-- ÍNDICES TRANSACCIONALES
-- ============================

-- Índices para usuarios
CREATE INDEX idx_users_email ON users(email);
CREATE INDEX idx_users_username ON users(username);
CREATE INDEX idx_user_addresses_user ON user_addresses(user_id);

-- Índices para productos
CREATE INDEX idx_products_category ON products(category_id);
CREATE INDEX idx_products_price ON products(price);
CREATE INDEX idx_products_active ON products(is_active) WHERE is_active = TRUE;

-- Índices para ratings
CREATE INDEX idx_product_ratings_product ON product_ratings(product_id);
CREATE INDEX idx_product_ratings_user ON product_ratings(user_id);

-- Índices para carritos
CREATE INDEX idx_carts_user ON carts(user_id);
CREATE INDEX idx_carts_status ON carts(status);
CREATE INDEX idx_carts_session ON carts(session_id);
CREATE INDEX idx_carts_updated ON carts(updated_at);

-- Índices para items de carrito
CREATE INDEX idx_cart_items_cart ON cart_items(cart_id);
CREATE INDEX idx_cart_items_product ON cart_items(product_id);

-- Índices para órdenes
CREATE INDEX idx_orders_user ON orders(user_id);
CREATE INDEX idx_orders_status ON orders(status);
CREATE INDEX idx_orders_ordered ON orders(ordered_at);
CREATE INDEX idx_orders_number ON orders(order_number);

-- Índices para items de orden
CREATE INDEX idx_order_items_order ON order_items(order_id);
CREATE INDEX idx_order_items_product ON order_items(product_id);

-- ============================
-- CONSTRAINTS ADICIONALES
-- ============================

-- Trigger para actualizar updated_at
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

CREATE TRIGGER update_users_updated_at BEFORE UPDATE ON users FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
CREATE TRIGGER update_products_updated_at BEFORE UPDATE ON products FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
CREATE TRIGGER update_carts_updated_at BEFORE UPDATE ON carts FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- ============================
-- VISTAS PARA OPERACIONES COMUNES
-- ============================

CREATE VIEW vw_active_carts AS
SELECT 
    c.cart_id,
    u.user_id,
    u.first_name || ' ' || u.last_name as user_name,
    COUNT(ci.cart_item_id) as item_count,
    SUM(ci.quantity * ci.unit_price) as cart_total,
    c.created_at,
    c.updated_at
FROM carts c
JOIN users u ON c.user_id = u.user_id
LEFT JOIN cart_items ci ON c.cart_id = ci.cart_id
WHERE c.status = 'active'
GROUP BY c.cart_id, u.user_id, user_name, c.created_at, c.updated_at;

CREATE VIEW vw_product_sales AS
SELECT 
    p.product_id,
    p.title,
    p.category_id,
    c.category_name,
    COUNT(oi.order_item_id) as times_ordered,
    SUM(oi.quantity) as total_quantity_sold,
    SUM(oi.line_total) as total_revenue,
    AVG(pr.rating_value) as avg_rating,
    COUNT(pr.rating_id) as rating_count
FROM products p
JOIN categories c ON p.category_id = c.category_id
LEFT JOIN order_items oi ON p.product_id = oi.product_id
LEFT JOIN product_ratings pr ON p.product_id = pr.product_id
GROUP BY p.product_id, p.title, p.category_id, c.category_name;