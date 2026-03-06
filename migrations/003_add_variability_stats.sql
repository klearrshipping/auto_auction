-- Add variability metrics: std_dev (standard deviation) and cv_pct (coefficient of variation).
-- These quantify how variable prices are within each bucket.
-- Run after 002. Run in Supabase SQL Editor (sales_data schema).

ALTER TABLE sales_data.japan_sales_bucket_stats
  ADD COLUMN IF NOT EXISTS std_dev bigint,
  ADD COLUMN IF NOT EXISTS cv_pct numeric;

-- Update view to include new columns
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
