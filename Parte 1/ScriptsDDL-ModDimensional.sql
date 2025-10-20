-- ============================
-- DIMENSIONES
-- ============================

-- Dimensión Producto
CREATE TABLE dim_product (
    product_key SERIAL PRIMARY KEY,
    product_id INT UNIQUE NOT NULL,
    title TEXT NOT NULL,
    category VARCHAR(100) NOT NULL,
    price NUMERIC(10,2) NOT NULL CHECK (price >= 0),
    rating_rate NUMERIC(3,2) CHECK (rating_rate BETWEEN 0 AND 5),
    rating_count INT DEFAULT 0 CHECK (rating_count >= 0),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Dimensión Usuario
CREATE TABLE dim_user (
    user_key SERIAL PRIMARY KEY,
    user_id INT UNIQUE NOT NULL,
    name_first VARCHAR(100) NOT NULL,
    name_last VARCHAR(100) NOT NULL,
    email VARCHAR(255) UNIQUE NOT NULL,
    username VARCHAR(100) UNIQUE NOT NULL,
    phone VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Dimensión Geografía
CREATE TABLE dim_geography (
    geo_key SERIAL PRIMARY KEY,
    city VARCHAR(100) NOT NULL,
    street VARCHAR(255),
    zipcode VARCHAR(20),
    lat NUMERIC(10,6),
    lng NUMERIC(10,6),
    location_point GEOGRAPHY(POINT, 4326),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Dimensión Tiempo (pre-poblada)
CREATE TABLE dim_date (
    date_key INT PRIMARY KEY,
    date DATE NOT NULL UNIQUE,
    day INT NOT NULL CHECK (day BETWEEN 1 AND 31),
    month INT NOT NULL CHECK (month BETWEEN 1 AND 12),
    year INT NOT NULL,
    quarter INT NOT NULL CHECK (quarter BETWEEN 1 AND 4),
    iso_week INT NOT NULL CHECK (iso_week BETWEEN 1 AND 53),
    day_of_week INT CHECK (day_of_week BETWEEN 1 AND 7),
    day_name VARCHAR(10),
    month_name VARCHAR(10),
    is_weekend BOOLEAN,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ============================
-- TABLA DE HECHOS
-- ============================

CREATE TABLE fact_sales (
    sales_key SERIAL PRIMARY KEY,
    product_key INT NOT NULL REFERENCES dim_product(product_key),
    user_key INT NOT NULL REFERENCES dim_user(user_key),
    geo_key INT NOT NULL REFERENCES dim_geography(geo_key),
    date_key INT NOT NULL REFERENCES dim_date(date_key),
    cart_id INT NOT NULL,
    quantity INT NOT NULL CHECK (quantity > 0),
    unit_price NUMERIC(10,2) NOT NULL CHECK (unit_price >= 0),
    total_amount NUMERIC(12,2) NOT NULL CHECK (total_amount >= 0),
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ============================
-- ÍNDICES Y CONSTRAINTS
-- ============================

-- Índices para la tabla de hechos
CREATE INDEX idx_fact_sales_product ON fact_sales(product_key);
CREATE INDEX idx_fact_sales_user ON fact_sales(user_key);
CREATE INDEX idx_fact_sales_date ON fact_sales(date_key);
CREATE INDEX idx_fact_sales_geo ON fact_sales(geo_key);
CREATE INDEX idx_fact_sales_cart ON fact_sales(cart_id);
CREATE INDEX idx_fact_sales_loaded ON fact_sales(loaded_at);

-- Índices para búsquedas comunes
CREATE INDEX idx_dim_product_category ON dim_product(category);
CREATE INDEX idx_dim_product_price ON dim_product(price);
CREATE INDEX idx_dim_geography_city ON dim_geography(city);
CREATE INDEX idx_dim_date_year_month ON dim_date(year, month);

-- Índice espacial para geografía
CREATE INDEX idx_dim_geography_location ON dim_geography USING GIST(location_point);

-- ============================
-- VISTAS PARA ANÁLISIS COMUNES
-- ============================

CREATE VIEW vw_sales_by_category AS
SELECT 
    dp.category,
    dd.year,
    dd.month,
    SUM(fs.quantity) as total_quantity,
    SUM(fs.total_amount) as total_revenue,
    COUNT(DISTINCT fs.cart_id) as total_carts
FROM fact_sales fs
JOIN dim_product dp ON fs.product_key = dp.product_key
JOIN dim_date dd ON fs.date_key = dd.date_key
GROUP BY dp.category, dd.year, dd.month;

CREATE VIEW vw_user_purchase_behavior AS
SELECT 
    du.user_key,
    du.name_first || ' ' || du.name_last as full_name,
    COUNT(DISTINCT fs.date_key) as purchase_days,
    COUNT(DISTINCT fs.cart_id) as total_carts,
    SUM(fs.quantity) as total_items,
    SUM(fs.total_amount) as total_spent,
    AVG(fs.total_amount) as avg_cart_value
FROM fact_sales fs
JOIN dim_user du ON fs.user_key = du.user_key
GROUP BY du.user_key, full_name;