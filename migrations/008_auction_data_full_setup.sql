-- Full auction_data schema setup for Supabase.
-- Run this in Supabase Dashboard → SQL Editor.
-- Creates schema and vehicles table only (used by tools/aggregate_auction/cloud_sync.py).

CREATE SCHEMA IF NOT EXISTS auction_data;

-- vehicles: main auction listing table (used by cloud_sync)
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
  model_type text,
  color text,
  result text,
  scores jsonb,
  lot_link text,
  auction text,
  search_date text,
  auction_time text,
  image_urls jsonb,
  created_at timestamptz DEFAULT now(),
  UNIQUE (site_name, lot_number)
);

-- Add columns if table already existed from 006
ALTER TABLE auction_data.vehicles
  ADD COLUMN IF NOT EXISTS auction_time text,
  ADD COLUMN IF NOT EXISTS image_urls jsonb,
  ADD COLUMN IF NOT EXISTS model_type text;

-- Indexes
CREATE INDEX IF NOT EXISTS idx_vehicles_site_lot ON auction_data.vehicles(site_name, lot_number);

-- Schema and table grants
GRANT USAGE ON SCHEMA auction_data TO anon, authenticated, service_role;
GRANT ALL ON auction_data.vehicles TO anon, service_role, authenticated;
GRANT USAGE, SELECT ON SEQUENCE auction_data.vehicles_id_seq TO anon, service_role, authenticated;

-- Expose auction_data to PostgREST API (avoids Dashboard setting)
-- Include all schemas you use; your project has public, graphql_public, sales_data
ALTER ROLE authenticator SET pgrst.db_schemas = 'public, graphql_public, sales_data, auction_data';
