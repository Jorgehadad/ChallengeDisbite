-- Poblar dim_date desde 2015-01-01 a 2035-12-31 (ajusta rango)
INSERT INTO dim_date (date_key, date, day, month, year, quarter, iso_week, day_of_week, day_name, month_name, is_weekend, created_at)
SELECT
  (TO_CHAR(d, 'YYYYMMDD'))::INT AS date_key,
  d::date AS date,
  EXTRACT(DAY FROM d)::INT AS day,
  EXTRACT(MONTH FROM d)::INT AS month,
  EXTRACT(YEAR FROM d)::INT AS year,
  EXTRACT(QUARTER FROM d)::INT AS quarter,
  EXTRACT(WEEK FROM d)::INT AS iso_week,
  EXTRACT(DOW FROM d)::INT AS day_of_week,
  TO_CHAR(d, 'Day')::VARCHAR AS day_name,
  TO_CHAR(d, 'Month')::VARCHAR AS month_name,
  CASE WHEN EXTRACT(DOW FROM d) IN (0,6) THEN TRUE ELSE FALSE END AS is_weekend,
  now() AS created_at
FROM generate_series('2015-01-01'::date, '2035-12-31'::date, interval '1 day') d
ON CONFLICT (date_key) DO NOTHING;