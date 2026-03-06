-- Split japan_sales_buckets into structure (definitions) + stats (analysis).
-- Run in Supabase SQL Editor (sales_data schema).
--
-- Flow: 1) Structure table defines which buckets exist (updated when new models appear)
--       2) Stats table holds computed analysis (recomputed after structure is confirmed)

-- Step 1: Create structure table (organization only)
CREATE TABLE IF NOT EXISTS sales_data.japan_sales_bucket_definitions (
  id bigserial PRIMARY KEY,
  year int NOT NULL,
  make text NOT NULL,
  model text NOT NULL,
  model_type text NOT NULL,
  score_band text NOT NULL,
  mileage_band text NOT NULL,
  created_at timestamptz DEFAULT now(),
  updated_at timestamptz DEFAULT now(),
  UNIQUE(year, make, model, model_type, score_band, mileage_band)
);

-- Step 2: Create stats table (analysis only)
CREATE TABLE IF NOT EXISTS sales_data.japan_sales_bucket_stats (
  id bigserial PRIMARY KEY,
  bucket_id bigint NOT NULL REFERENCES sales_data.japan_sales_bucket_definitions(id) ON DELETE CASCADE,
  comparable_count int NOT NULL DEFAULT 0,
  median_price bigint,
  mean_price bigint,
  min_price bigint,
  max_price bigint,
  iqr bigint,
  trimmed_mean bigint,
  price_spread_pct numeric,
  std_dev bigint,
  cv_pct numeric,
  confidence_tier text,
  last_sold_price bigint,
  last_sold_date text,
  updated_at timestamptz DEFAULT now(),
  UNIQUE(bucket_id)
);

CREATE INDEX IF NOT EXISTS idx_bucket_stats_bucket_id ON sales_data.japan_sales_bucket_stats(bucket_id);

-- Step 3: Migrate from existing japan_sales_buckets if it is a base table with the old combined schema
DO $$
BEGIN
  IF EXISTS (
    SELECT 1 FROM information_schema.tables t
    JOIN information_schema.columns c ON t.table_schema = c.table_schema AND t.table_name = c.table_name
    WHERE t.table_schema = 'sales_data' AND t.table_name = 'japan_sales_buckets'
      AND t.table_type = 'BASE TABLE' AND c.column_name = 'median_price'
  ) THEN
    INSERT INTO sales_data.japan_sales_bucket_definitions (id, year, make, model, model_type, score_band, mileage_band, updated_at)
    SELECT id, year, make, model, model_type, score_band, mileage_band, updated_at
    FROM sales_data.japan_sales_buckets
    ON CONFLICT (year, make, model, model_type, score_band, mileage_band) DO UPDATE SET updated_at = EXCLUDED.updated_at;

    INSERT INTO sales_data.japan_sales_bucket_stats (bucket_id, comparable_count, median_price, mean_price, min_price, max_price, iqr, trimmed_mean, price_spread_pct, confidence_tier, last_sold_price, last_sold_date, updated_at)
    SELECT id, comparable_count, median_price, mean_price, min_price, max_price, iqr, trimmed_mean, price_spread_pct, confidence_tier, last_sold_price, last_sold_date, updated_at
    FROM sales_data.japan_sales_buckets
    ON CONFLICT (bucket_id) DO UPDATE SET
      comparable_count = EXCLUDED.comparable_count,
      median_price = EXCLUDED.median_price,
      mean_price = EXCLUDED.mean_price,
      min_price = EXCLUDED.min_price,
      max_price = EXCLUDED.max_price,
      iqr = EXCLUDED.iqr,
      trimmed_mean = EXCLUDED.trimmed_mean,
      price_spread_pct = EXCLUDED.price_spread_pct,
      confidence_tier = EXCLUDED.confidence_tier,
      last_sold_price = EXCLUDED.last_sold_price,
      last_sold_date = EXCLUDED.last_sold_date,
      updated_at = EXCLUDED.updated_at;

    DROP TABLE IF EXISTS sales_data.japan_sales_buckets;
    PERFORM setval(pg_get_serial_sequence('sales_data.japan_sales_bucket_definitions', 'id'), (SELECT COALESCE(max(id), 1) FROM sales_data.japan_sales_bucket_definitions));
  END IF;
END $$;

-- Step 4: Create view for backward compatibility (API reads from japan_sales_buckets)
CREATE OR REPLACE VIEW sales_data.japan_sales_buckets AS
SELECT
  b.id,
  b.year,
  b.make,
  b.model,
  b.model_type,
  b.score_band,
  b.mileage_band,
  s.comparable_count,
  s.median_price,
  s.mean_price,
  s.min_price,
  s.max_price,
  s.iqr,
  s.trimmed_mean,
  s.price_spread_pct,
  s.std_dev,
  s.cv_pct,
  s.confidence_tier,
  s.last_sold_price,
  s.last_sold_date,
  s.updated_at
FROM sales_data.japan_sales_bucket_definitions b
LEFT JOIN sales_data.japan_sales_bucket_stats s ON b.id = s.bucket_id;
