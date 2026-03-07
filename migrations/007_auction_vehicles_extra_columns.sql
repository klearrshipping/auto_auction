-- Add auction_time and image_urls to auction_data.vehicles for compiled sync.
-- Run in Supabase SQL Editor.

ALTER TABLE auction_data.vehicles
  ADD COLUMN IF NOT EXISTS auction_time text,
  ADD COLUMN IF NOT EXISTS image_urls jsonb;
