-- Create valuations table in auction_data schema.
-- Linked to vehicles via FK. Populated by run_get_valuation.py.

CREATE TABLE IF NOT EXISTS auction_data.valuations (
  id bigserial PRIMARY KEY,
  vehicle_id bigint NOT NULL REFERENCES auction_data.vehicles(id) ON DELETE CASCADE,
  min_value bigint,
  max_value bigint,
  trimmed_mean bigint,
  price_spread_pct numeric,
  confidence_tier text,
  created_at timestamptz DEFAULT now(),
  updated_at timestamptz DEFAULT now(),
  UNIQUE (vehicle_id)
);

CREATE INDEX IF NOT EXISTS idx_valuations_vehicle_id ON auction_data.valuations(vehicle_id);

GRANT ALL ON auction_data.valuations TO anon, service_role, authenticated;
GRANT USAGE, SELECT ON SEQUENCE auction_data.valuations_id_seq TO anon, service_role, authenticated;
