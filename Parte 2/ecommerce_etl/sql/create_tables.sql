-- Drop tables if they exist
DROP TABLE IF EXISTS fact_sales;
DROP TABLE IF EXISTS dim_date;
DROP TABLE IF EXISTS dim_products;
DROP TABLE IF EXISTS dim_users;
DROP TABLE IF EXISTS dim_geography;

-- Create dimension tables
CREATE TABLE dim_date (
    date_key INTEGER PRIMARY KEY,
    date DATE NOT NULL,
    day INTEGER NOT NULL,
    month INTEGER NOT NULL,
    year INTEGER NOT NULL,
    quarter INTEGER NOT NULL,
    iso_week INTEGER NOT NULL,
    day_of_week INTEGER NOT NULL,
    day_name VARCHAR(10) NOT NULL,
    month_name VARCHAR(10) NOT NULL
);

CREATE TABLE dim_products (
    product_id INTEGER PRIMARY KEY,
    title VARCHAR(255) NOT NULL,
    price DECIMAL(10,2) NOT NULL,
    description TEXT,
    category VARCHAR(100),
    image_url TEXT,
    rating_rate DECIMAL(3,2),
    rating_count INTEGER
);

CREATE TABLE dim_users (
    user_id INTEGER PRIMARY KEY,
    email VARCHAR(255) NOT NULL,
    username VARCHAR(100) NOT NULL,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    phone VARCHAR(50)
);

CREATE TABLE dim_geography (
    geography_id SERIAL PRIMARY KEY,
    city VARCHAR(100),
    street VARCHAR(255),
    number INTEGER,
    zipcode VARCHAR(20),
    lat DECIMAL(10,6),
    long DECIMAL(10,6)
);

CREATE TABLE fact_sales (
    sale_id SERIAL PRIMARY KEY,
    date_key INTEGER REFERENCES dim_date(date_key),
    product_id INTEGER REFERENCES dim_products(product_id),
    user_id INTEGER REFERENCES dim_users(user_id),
    quantity INTEGER NOT NULL,
    total_amount DECIMAL(10,2) NOT NULL
);