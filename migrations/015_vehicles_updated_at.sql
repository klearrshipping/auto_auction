-- Add updated_at to auction_data.vehicles for live delta detection.
-- Used by GET /api/auction-updates SSE endpoint to stream only changed lots.

ALTER TABLE auction_data.vehicles
  ADD COLUMN IF NOT EXISTS updated_at timestamptz DEFAULT now();

-- Backfill: set updated_at = created_at for existing rows
UPDATE auction_data.vehicles SET updated_at = created_at WHERE updated_at IS NULL;

-- Trigger to auto-update updated_at on row change
CREATE OR REPLACE FUNCTION auction_data.trigger_set_updated_at()
RETURNS TRIGGER AS $$
BEGIN
  NEW.updated_at = now();
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS set_vehicles_updated_at ON auction_data.vehicles;
CREATE TRIGGER set_vehicles_updated_at
  BEFORE UPDATE ON auction_data.vehicles
  FOR EACH ROW EXECUTE FUNCTION auction_data.trigger_set_updated_at();
