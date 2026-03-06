-- Consolidate to single japan_sales_buckets table.
-- Drops view, definitions, and stats. Creates one table with structure + stats.
-- Run in Supabase SQL Editor (sales_data schema).

-- Step 1: Drop view (references definitions + stats)
DROP VIEW IF EXISTS sales_data.japan_sales_buckets;

-- Step 2: Create single table
CREATE TABLE IF NOT EXISTS sales_data.japan_sales_buckets (
  id bigserial PRIMARY KEY,
  year int NOT NULL,
  make text NOT NULL,
  model text NOT NULL,
  model_type text NOT NULL,
  score_band text NOT NULL,
  mileage_band text NOT NULL,
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
  created_at timestamptz DEFAULT now(),
  updated_at timestamptz DEFAULT now(),
  UNIQUE(year, make, model, model_type, score_band, mileage_band)
);

-- Step 3: Migrate data from definitions + stats if they exist
DO $$
BEGIN
  IF EXISTS (
    SELECT 1 FROM information_schema.tables
    WHERE table_schema = 'sales_data' AND table_name = 'japan_sales_bucket_definitions'
  ) THEN
    INSERT INTO sales_data.japan_sales_buckets (
      id, year, make, model, model_type, score_band, mileage_band,
      comparable_count, median_price, mean_price, min_price, max_price,
      iqr, trimmed_mean, price_spread_pct, std_dev, cv_pct, confidence_tier,
      last_sold_price, last_sold_date, updated_at
    )
    SELECT
      b.id, b.year, b.make, b.model, b.model_type, b.score_band, b.mileage_band,
      COALESCE(s.comparable_count, 0), s.median_price, s.mean_price, s.min_price, s.max_price,
      s.iqr, s.trimmed_mean, s.price_spread_pct, s.std_dev, s.cv_pct, s.confidence_tier,
      s.last_sold_price, s.last_sold_date, COALESCE(s.updated_at, b.updated_at)
    FROM sales_data.japan_sales_bucket_definitions b
    LEFT JOIN sales_data.japan_sales_bucket_stats s ON b.id = s.bucket_id
    ON CONFLICT (year, make, model, model_type, score_band, mileage_band) DO UPDATE SET
      comparable_count = EXCLUDED.comparable_count,
      median_price = EXCLUDED.median_price,
      mean_price = EXCLUDED.mean_price,
      min_price = EXCLUDED.min_price,
      max_price = EXCLUDED.max_price,
      iqr = EXCLUDED.iqr,
      trimmed_mean = EXCLUDED.trimmed_mean,
      price_spread_pct = EXCLUDED.price_spread_pct,
      std_dev = EXCLUDED.std_dev,
      cv_pct = EXCLUDED.cv_pct,
      confidence_tier = EXCLUDED.confidence_tier,
      last_sold_price = EXCLUDED.last_sold_price,
      last_sold_date = EXCLUDED.last_sold_date,
      updated_at = EXCLUDED.updated_at;

    PERFORM setval(pg_get_serial_sequence('sales_data.japan_sales_buckets', 'id'), (SELECT COALESCE(max(id), 1) FROM sales_data.japan_sales_buckets));

    DROP TABLE sales_data.japan_sales_bucket_stats;
    DROP TABLE sales_data.japan_sales_bucket_definitions;
  END IF;
END $$;

-- Step 4: Grant permissions
GRANT ALL ON sales_data.japan_sales_buckets TO anon, service_role, authenticated;
GRANT USAGE, SELECT ON SEQUENCE sales_data.japan_sales_buckets_id_seq TO anon, service_role, authenticated;
