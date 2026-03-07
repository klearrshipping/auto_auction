#!/usr/bin/env python3
"""
Diagnose auction pipeline issues. Runs pipeline steps with verbose logging
to help identify why the script keeps stopping.

Usage:
  python tests/diagnose_auction_pipeline.py              # run full pipeline with diag log
  python tests/diagnose_auction_pipeline.py --step 2    # run only listings step
  python tests/diagnose_auction_pipeline.py --limit 3   # limit to 3 make/model jobs (quick test)
"""

import argparse
import os
import subprocess
import sys
from datetime import datetime
from pathlib import Path

_script_dir = Path(__file__).resolve().parent
_root = _script_dir.parent
sys.path.insert(0, str(_root))


def log(msg: str):
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts}] {msg}", flush=True)


def main():
    parser = argparse.ArgumentParser(description="Diagnose auction pipeline.")
    parser.add_argument("--step", type=int, choices=[1, 2, 3, 4], help="Run only this step (1=prune, 2=listings, 3=details, 4=compile)")
    parser.add_argument("--limit", type=int, default=0, help="Limit jobs (step 2 only)")
    args = parser.parse_args()

    log_dir = _root / "logs"
    log_dir.mkdir(exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    diag_log = log_dir / f"auction_diag_{ts}.log"

    env = dict(os.environ)
    env["PYTHONUNBUFFERED"] = "1"

    def run(cmd: list) -> int:
        log(f"Running: {' '.join(cmd)}")
        with open(diag_log, "a", encoding="utf-8") as f:
            f.write(f"\n[{datetime.now().isoformat()}] {' '.join(cmd)}\n")
            f.flush()
        result = subprocess.run(
            cmd, cwd=str(_root), env=env,
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True
        )
        with open(diag_log, "a", encoding="utf-8") as f:
            f.write(result.stdout or "")
            if result.returncode != 0:
                f.write(f"\n[EXIT CODE: {result.returncode}]\n")
            f.flush()
        print(result.stdout, end="", flush=True)
        if result.returncode != 0:
            log(f"Exit code: {result.returncode}")
        return result.returncode

    py = sys.executable
    script_dir = _root / "operations" / "auction"
    pipeline = script_dir / "pipeline"

    log(f"Diagnostic log: {diag_log}")
    log("")

    if args.step is None or args.step == 1:
        log("--- Step 1: Prune ---")
        rc = run([py, "-u", str(pipeline / "1_remove_lots.py")])
        if rc != 0 and args.step == 1:
            sys.exit(rc)

    if args.step is None or args.step == 2:
        log("--- Step 2: Listings ---")
        cmd = [py, "-u", str(pipeline / "2_extract_listings.py")]
        if args.limit > 0:
            cmd.extend(["--limit", str(args.limit)])
        rc = run(cmd)
        if rc != 0:
            log(f"Listings failed. Check {diag_log} for details.")
            sys.exit(rc)

    if args.step is None or args.step == 3:
        log("--- Step 3: Details ---")
        cmd = [py, "-u", str(pipeline / "3_extract_details.py")]
        if args.limit > 0:
            cmd.extend(["--limit", str(args.limit)])
        rc = run(cmd)
        if rc != 0 and args.step == 3:
            sys.exit(rc)

    if args.step is None or args.step == 4:
        log("--- Step 4: Compile ---")
        rc = run([py, "-u", str(pipeline / "4_compile.py")])
        if rc != 0:
            sys.exit(rc)

    log("")
    log("Diagnosis complete. Check logs/auction_data_listing.log for listing-level errors.")


if __name__ == "__main__":
    main()
