-- Add model_type to auction_data.vehicles for bucket pairing.
ALTER TABLE auction_data.vehicles ADD COLUMN IF NOT EXISTS model_type text;
