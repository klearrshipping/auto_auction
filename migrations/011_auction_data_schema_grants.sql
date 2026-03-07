-- Grant schema usage (fixes 42501 "permission denied for schema auction_data").
-- Run in Supabase SQL Editor.

GRANT USAGE ON SCHEMA auction_data TO anon, authenticated, service_role;
GRANT ALL ON ALL TABLES IN SCHEMA auction_data TO anon, authenticated, service_role;
GRANT ALL ON ALL SEQUENCES IN SCHEMA auction_data TO anon, authenticated, service_role;
ALTER DEFAULT PRIVILEGES IN SCHEMA auction_data GRANT ALL ON TABLES TO anon, authenticated, service_role;
