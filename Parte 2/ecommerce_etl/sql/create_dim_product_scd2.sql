CREATE TABLE IF NOT EXISTS dim_product_scd2 (
  product_key SERIAL PRIMARY KEY,
  product_id INTEGER NOT NULL,
  title TEXT,
  category TEXT,
  price NUMERIC(10,2),
  description TEXT,
  image_url TEXT,
  rating_rate NUMERIC(3,2),
  rating_count INTEGER,
  effective_date DATE DEFAULT CURRENT_DATE,
  end_date DATE DEFAULT '9999-12-31',
  is_current BOOLEAN DEFAULT TRUE,
  created_at TIMESTAMP DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_dim_product_scd2_productid_current
  ON dim_product_scd2 (product_id) WHERE is_current = TRUE;