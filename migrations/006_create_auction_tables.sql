-- Create auction_data schema and auction tables for auction extraction.
-- Run in Supabase SQL Editor.
-- auction_db.py uses ClientOptions(schema="auction_data").

CREATE SCHEMA IF NOT EXISTS auction_data;

-- 1. vehicles: main auction listing table
CREATE TABLE IF NOT EXISTS auction_data.vehicles (
  id bigserial PRIMARY KEY,
  site_name text NOT NULL,
  lot_number text NOT NULL,
  make text,
  model text,
  year int,
  mileage int,
  start_price bigint,
  end_price bigint,
  grade text,
  color text,
  result text,
  scores jsonb,
  lot_link text,
  auction text,
  search_date text,
  created_at timestamptz DEFAULT now(),
  UNIQUE (site_name, lot_number)
);

-- 2. vehicles_staging: staging for batch inserts (same structure)
CREATE TABLE IF NOT EXISTS auction_data.vehicles_staging (
  id bigserial PRIMARY KEY,
  site_name text NOT NULL,
  lot_number text NOT NULL,
  make text,
  model text,
  year int,
  mileage int,
  start_price bigint,
  end_price bigint,
  grade text,
  color text,
  result text,
  scores jsonb,
  lot_link text,
  auction text,
  search_date text,
  created_at timestamptz DEFAULT now()
);

-- 3. processed_urls: URL processing tracking
CREATE TABLE IF NOT EXISTS auction_data.processed_urls (
  id bigserial PRIMARY KEY,
  site_name text NOT NULL,
  url text NOT NULL,
  vehicle_id bigint REFERENCES auction_data.vehicles(id) ON DELETE SET NULL,
  processed boolean NOT NULL DEFAULT false,
  processing_started timestamptz,
  processing_completed timestamptz,
  error_message text,
  created_at timestamptz DEFAULT now(),
  UNIQUE (site_name, url)
);

-- 4. vehicle_details: detailed page data (from get_details.py)
CREATE TABLE IF NOT EXISTS auction_data.vehicle_details (
  id bigserial PRIMARY KEY,
  vehicle_id bigint NOT NULL REFERENCES auction_data.vehicles(id) ON DELETE CASCADE,
  url text,
  final_price bigint,
  auction_date text,
  engine_size int,
  displacement int,
  transmission text,
  additional_info text,
  type_code text,
  chassis_number text,
  interior_score text,
  exterior_score text,
  equipment text,
  auction_time text,
  image_urls jsonb,
  total_images int,
  auction_sheet_url text,
  start_price bigint,
  extraction_date timestamptz DEFAULT now()
);

-- Indexes for common lookups
CREATE INDEX IF NOT EXISTS idx_vehicles_site_lot ON auction_data.vehicles(site_name, lot_number);
CREATE INDEX IF NOT EXISTS idx_processed_urls_vehicle ON auction_data.processed_urls(vehicle_id);
CREATE INDEX IF NOT EXISTS idx_processed_urls_processed ON auction_data.processed_urls(processed);
CREATE INDEX IF NOT EXISTS idx_vehicle_details_vehicle ON auction_data.vehicle_details(vehicle_id);
