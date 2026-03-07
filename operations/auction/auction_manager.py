#!/usr/bin/env python3
"""
Auction manager: runs the auction data pipeline.

1. Clean: Remove expired lots (pipeline/1_remove_lots.py)
2. Listings: Extract auction listings (pipeline/2_extract_listings.py)
3. Details: Fetch details only for pending entries (pipeline/3_extract_details.py)
4. Compile: Merge state into compiled JSON (pipeline/4_compile.py)
"""

import argparse
import json
import os
import subprocess
import sys
import traceback
from datetime import datetime
from pathlib import Path

_script_dir = Path(__file__).resolve().parent
_root = _script_dir.parent.parent
sys.path.insert(0, str(_root))

_log_file = None
_RUN_MARKER_PATH = Path(__file__).resolve().parent.parent.parent / "logs" / "auction_pipeline_run.json"


def log(msg: str):
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{ts}] {msg}"
    print(line, flush=True)
    if _log_file:
        try:
            _log_file.write(line + "\n")
            _log_file.flush()
        except Exception:
            pass


def _run(cmd: list) -> tuple[int, str]:
    """Run subprocess with stdout/stderr inherited. Returns (exit_code, stderr_capture)."""
    env = dict(os.environ)
    env["PYTHONUNBUFFERED"] = "1"
    result = subprocess.run(
        cmd, cwd=str(_root), stdout=sys.stdout, stderr=subprocess.PIPE, text=True, env=env
    )
    stderr = result.stderr or ""
    # Re-print stderr so it appears in logs when inherited
    if stderr:
        print(stderr, file=sys.stderr, end="", flush=True)
    return result.returncode, stderr


def run_prune(dry_run: bool = False) -> int:
    """Run remove_lots. Returns exit code."""
    cmd = [sys.executable, "-u", str(_script_dir / "pipeline" / "1_remove_lots.py")]
    if dry_run:
        cmd.append("--dry-run")
    log(f"\n>>> Running prune: {' '.join(cmd)}")
    rc, err = _run(cmd)
    if rc != 0 and err:
        log(f"Prune stderr:\n{err}")
    return rc


def run_listings(site: str | None = None, maker: str | None = None, limit: int = 0) -> int:
    """Run listing extraction (run_all.py). UID-based skip built in."""
    cmd = [sys.executable, "-u", str(_script_dir / "pipeline" / "2_extract_listings.py")]
    if site:
        cmd.extend(["--site", site])
    if maker:
        cmd.extend(["--maker", maker])
    if limit > 0:
        cmd.extend(["--limit", str(limit)])
    log(f"\n>>> Running listings: {' '.join(cmd)}")
    rc, err = _run(cmd)
    if rc != 0 and err:
        log(f"Listings stderr:\n{err}")
    return rc


def run_details(limit: int = 0) -> int:
    """Run details extraction (run_details.py). Fetches only pending entries."""
    cmd = [sys.executable, "-u", str(_script_dir / "pipeline" / "3_extract_details.py")]
    if limit > 0:
        cmd.extend(["--limit", str(limit)])
    log(f"\n>>> Running details: {' '.join(cmd)}")
    rc, err = _run(cmd)
    if rc != 0 and err:
        log(f"Details stderr:\n{err}")
    return rc


def run_compile(limit: int = 0) -> int:
    """Run compilation (process_auction_data.py)."""
    cmd = [sys.executable, "-u", str(_script_dir / "pipeline" / "4_compile.py")]
    if limit > 0:
        cmd.extend(["--limit", str(limit)])
    log(f"\n>>> Running compile: {' '.join(cmd)}")
    rc, err = _run(cmd)
    if rc != 0 and err:
        log(f"Compile stderr:\n{err}")
    return rc


def trigger_auction_sync(replace: bool = False) -> bool:
    """Run auction cloud sync to push compiled data to Supabase.
    If replace=True, truncates vehicles table before sync (clean slate).
    """
    sync_script = _root / "tools" / "aggregate_auction" / "cloud_sync.py"
    if not sync_script.exists():
        log(f"Auction sync script not found: {sync_script}")
        return False
    cmd = [sys.executable, "-u", str(sync_script)]
    if replace:
        cmd.extend(["--full", "--truncate"])
    log(f"\n>>> Triggering auction Supabase sync: {' '.join(cmd)}")
    try:
        subprocess.run(cmd, cwd=str(_root), check=True)
        return True
    except subprocess.CalledProcessError as e:
        log(f"Auction sync failed: {e}")
        return False


def main():
    global _log_file
    parser = argparse.ArgumentParser(
        description="Auction pipeline: clean → listings → details → compile."
    )
    parser.add_argument("--no-prune", action="store_true", help="Skip cleaning expired lots")
    parser.add_argument("--no-listings", action="store_true", help="Skip listing extraction")
    parser.add_argument("--no-details", action="store_true", help="Skip details extraction")
    parser.add_argument("--no-compile", action="store_true", help="Skip compilation")
    parser.add_argument("--resume", action="store_true", help="Resume from details (skip prune + listings)")
    parser.add_argument("--site", help="Run listings for one site only (e.g. Zervtek)")
    parser.add_argument("--maker", help="Run listings for one maker only")
    parser.add_argument("--limit", type=int, default=0, help="Limit jobs per step (0 = all)")
    parser.add_argument("--dry-run", action="store_true", help="Clean only: dry-run (no writes)")
    parser.add_argument("--log-file", help="Write progress logs to file (for background runs)")
    parser.add_argument("--replace", action="store_true", help="Truncate vehicles table before sync (full replace)")
    args = parser.parse_args()

    if args.log_file:
        Path(args.log_file).parent.mkdir(parents=True, exist_ok=True)
        _log_file = open(args.log_file, "w", encoding="utf-8")
        log(f"Logging to {args.log_file}")

    # Write run marker so status checks can reliably detect completion
    _RUN_MARKER_PATH.parent.mkdir(parents=True, exist_ok=True)
    run_marker = {
        "status": "running",
        "started_at": datetime.now().isoformat(),
        "log_file": args.log_file,
    }
    _RUN_MARKER_PATH.write_text(json.dumps(run_marker, indent=2), encoding="utf-8")

    if args.resume:
        args.no_prune = True
        args.no_listings = True
        log("Resume mode: skipping prune and listings")

    log("=" * 60)
    log("AUCTION MANAGER")
    log("=" * 60)

    if not args.no_prune:
        log("\n--- Step 1: Clean expired lots ---")
        run_prune(dry_run=args.dry_run)
        if args.dry_run:
            log("Dry-run: stopping after prune.")
            return

    if not args.no_listings:
        log("\n--- Step 2: Extract listings ---")
        rc = run_listings(site=args.site, maker=args.maker, limit=args.limit)
        if rc != 0:
            log(f"Listings failed (exit {rc})")
            sys.exit(rc)

    if not args.no_details:
        log("\n--- Step 3: Fetch details ---")
        rc = run_details(limit=args.limit)
        if rc != 0:
            log(f"Details failed (exit {rc})")
            sys.exit(rc)

    if not args.no_compile:
        log("\n--- Step 4: Compile ---")
        rc = run_compile(limit=args.limit)
        if rc != 0:
            log(f"Compile failed (exit {rc})")
            sys.exit(rc)

        # Step 5: Push to Supabase (like sales pipeline)
        log("\n--- Step 5: Supabase sync ---")
        trigger_auction_sync(replace=args.replace)

    log("\n" + "=" * 60)
    log("PIPELINE COMPLETE")
    log("=" * 60)

    # Update run marker to complete
    try:
        run_marker = json.loads(_RUN_MARKER_PATH.read_text(encoding="utf-8"))
        run_marker["status"] = "complete"
        run_marker["completed_at"] = datetime.now().isoformat()
        _RUN_MARKER_PATH.write_text(json.dumps(run_marker, indent=2), encoding="utf-8")
    except Exception:
        pass

    if _log_file:
        _log_file.close()
        _log_file = None


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        tb = traceback.format_exception(type(e), e, e.__traceback__)
        msg = f"Pipeline crashed: {e}\n{''.join(tb)}"
        print(msg, file=sys.stderr)
        if _log_file:
            _log_file.write(msg + "\n")
            _log_file.flush()
        try:
            run_marker = json.loads(_RUN_MARKER_PATH.read_text(encoding="utf-8"))
            run_marker["status"] = "failed"
            run_marker["error"] = str(e)
            run_marker["failed_at"] = datetime.now().isoformat()
            _RUN_MARKER_PATH.write_text(json.dumps(run_marker, indent=2), encoding="utf-8")
        except Exception:
            pass
        sys.exit(1)
    finally:
        if _log_file:
            _log_file.close()
