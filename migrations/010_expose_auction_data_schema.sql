-- Expose auction_data schema to PostgREST API via SQL (no Dashboard needed).
-- Run in Supabase SQL Editor if you get PGRST106 "Invalid schema: auction_data".
-- Adjust the list if your project uses other schemas (e.g. storage).

ALTER ROLE authenticator SET pgrst.db_schemas = 'public, graphql_public, sales_data, auction_data';

-- Reload PostgREST so it picks up the new schema list
NOTIFY pgrst, 'reload schema';
