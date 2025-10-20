-- ============================
-- 1. Dimensiones
-- ============================
CREATE TABLE dim_product (
    product_key SERIAL PRIMARY KEY,
    product_id INT UNIQUE,
    title TEXT,
    category TEXT,
    price NUMERIC(10,2),
    rating_rate NUMERIC(4,2),
    rating_count INT
);

CREATE TABLE dim_user (
    user_key SERIAL PRIMARY KEY,
    user_id INT UNIQUE,
    name_first TEXT,
    name_last TEXT,
    email TEXT,
    username TEXT,
    phone TEXT
);

CREATE TABLE dim_geography (
    geo_key SERIAL PRIMARY KEY,
    city TEXT,
    street TEXT,
    zipcode TEXT,
    lat NUMERIC(10,6),
    lng NUMERIC(10,6)
);

CREATE TABLE dim_date (
    date_key INT PRIMARY KEY,
    date DATE,
    day INT,
    month INT,
    year INT,
    quarter INT,
    iso_week INT
);

-- ============================
-- 2. Tabla de hechos
-- ============================
CREATE TABLE fact_sales (
    sales_key SERIAL PRIMARY KEY,
    product_key INT REFERENCES dim_product(product_key),
    user_key INT REFERENCES dim_user(user_key),
    geo_key INT REFERENCES dim_geography(geo_key),
    date_key INT REFERENCES dim_date(date_key),
    quantity INT,
    unit_price NUMERIC(10,2),
    total_amount NUMERIC(12,2)
);

-- ============================
-- 3. Índices
-- ============================
CREATE INDEX idx_fact_sales_product ON fact_sales(product_key);
CREATE INDEX idx_fact_sales_user ON fact_sales(user_key);
CREATE INDEX idx_fact_sales_date ON fact_sales(date_key);
CREATE INDEX idx_fact_sales_geo ON fact_sales(geo_key);
