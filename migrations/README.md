# Database Migrations

## Running migrations

1. **Get your database connection string** from Supabase:
   - Dashboard > Project Settings > Database
   - Under "Connection string", select **URI**
   - Copy the connection string (includes password)

2. **Add to `tools/aggregate_sales/.env`**:
   ```
   DATABASE_URL=postgresql://postgres.[project-ref]:[PASSWORD]@aws-0-[region].pooler.supabase.com:6543/postgres
   ```

3. **Run migrations**:
   ```
   python migrations/run_migrations.py
   ```

## Manual alternative

You can also run each SQL file manually in **Supabase Dashboard > SQL Editor**:
- `001_add_last_sold_to_buckets.sql`
- `002_split_buckets_structure_and_stats.sql`
- `003_add_variability_stats.sql`

Run them in order.
