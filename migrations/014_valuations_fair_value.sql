-- Add fair_value column to auction_data.valuations.
-- FV hierarchy: trimmed_mean → median_price → mean_price.

ALTER TABLE auction_data.valuations
  ADD COLUMN IF NOT EXISTS fair_value bigint;
