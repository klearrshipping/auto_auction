-- Add last_sold_price and last_sold_date to japan_sales_buckets.
-- Run in Supabase SQL Editor (sales_data schema).
-- These columns are pre-computed during preanalysis to avoid N+1 lookups in the API.

ALTER TABLE sales_data.japan_sales_buckets
  ADD COLUMN IF NOT EXISTS last_sold_price bigint,
  ADD COLUMN IF NOT EXISTS last_sold_date text;
