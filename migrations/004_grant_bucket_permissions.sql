-- Grant API access to bucket tables.
-- Run in Supabase SQL Editor (sales_data schema).

GRANT USAGE ON SCHEMA sales_data TO anon, service_role, authenticated;

GRANT ALL ON sales_data.japan_sales_bucket_definitions TO anon, service_role, authenticated;
GRANT ALL ON sales_data.japan_sales_bucket_stats TO anon, service_role, authenticated;

GRANT USAGE, SELECT ON SEQUENCE sales_data.japan_sales_bucket_definitions_id_seq TO anon, service_role, authenticated;
GRANT USAGE, SELECT ON SEQUENCE sales_data.japan_sales_bucket_stats_id_seq TO anon, service_role, authenticated;
