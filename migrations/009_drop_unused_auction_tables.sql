-- Drop unused auction_data tables (vehicles_staging, processed_urls, vehicle_details).
-- Run this in Supabase SQL Editor if you previously created these tables.
-- Keeps auction_data.vehicles (used by cloud_sync).

DROP TABLE IF EXISTS auction_data.vehicle_details;
DROP TABLE IF EXISTS auction_data.processed_urls;
DROP TABLE IF EXISTS auction_data.vehicles_staging;
