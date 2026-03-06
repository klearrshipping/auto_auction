#!/usr/bin/env python3
"""
Run database migrations against Supabase.
Requires DATABASE_URL in tools/aggregate_sales/.env

Get it from: Supabase Dashboard > Project Settings > Database > Connection string (URI)
Format: postgresql://postgres.[project-ref]:[PASSWORD]@aws-0-[region].pooler.supabase.com:6543/postgres
"""

import os
import sys
from pathlib import Path

# Load .env from tools/aggregate_sales
_script_dir = Path(__file__).resolve().parent
_project_root = _script_dir.parent
_env_path = _project_root / "tools" / "aggregate_sales" / ".env"

try:
    from dotenv import load_dotenv
    load_dotenv(_env_path)
    load_dotenv(_project_root / ".env")
except ImportError:
    pass

DATABASE_URL = os.environ.get("DATABASE_URL")
if not DATABASE_URL:
    print("ERROR: DATABASE_URL not set.")
    print()
    print("Add it to tools/aggregate_sales/.env:")
    print("  DATABASE_URL=postgresql://postgres.[project-ref]:[PASSWORD]@aws-0-[region].pooler.supabase.com:6543/postgres")
    print()
    print("Get the connection string from: Supabase Dashboard > Project Settings > Database > Connection string")
    sys.exit(1)

try:
    import psycopg2
except ImportError:
    print("ERROR: psycopg2 not installed. Run: pip install psycopg2-binary")
    sys.exit(1)


def run_migration(conn, name: str, path: Path) -> bool:
    """Execute a migration file. Returns True on success."""
    sql = path.read_text(encoding="utf-8")
    print(f"  Running {name}...", end=" ", flush=True)
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
        conn.commit()
        print("OK")
        return True
    except Exception as e:
        conn.rollback()
        print(f"FAILED: {e}")
        return False


def main():
    migrations_dir = _script_dir
    migrations = [
        ("001_add_last_sold_to_buckets.sql", "001"),
        ("002_split_buckets_structure_and_stats.sql", "002"),
        ("003_add_variability_stats.sql", "003"),
        ("004_grant_bucket_permissions.sql", "004"),
        ("005_consolidate_single_buckets_table.sql", "005"),
        # 006_create_auction_tables.sql - skipped (auction DB removed, saves to JSON only)
    ]

    print("Connecting to database...")
    try:
        conn = psycopg2.connect(DATABASE_URL)
    except Exception as e:
        print(f"Connection failed: {e}")
        sys.exit(1)

    print(f"Running {len(migrations)} migrations...")
    ok = 0
    for filename, label in migrations:
        path = migrations_dir / filename
        if not path.exists():
            print(f"  Skipping {label} (file not found)")
            continue
        if run_migration(conn, label, path):
            ok += 1

    conn.close()
    print(f"\nDone. {ok}/{len(migrations)} migrations completed.")
    sys.exit(0 if ok == len(migrations) else 1)


if __name__ == "__main__":
    main()
