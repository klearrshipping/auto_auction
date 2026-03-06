# UID-Based Auction Pipeline — Implementation Plan

## Executive Summary

Refactor the auction data pipeline from **file-level skip** (date-based) to **record-level skip** (UID-based). This enables:
- Per-lot deduplication without re-fetching details
- Same-day incremental updates
- O(1) lookup for "already have this lot"
- Crash recovery (partial saves)

---

## 1. Architecture Overview

### Current Flow
```
get_data.py     → Audi_A3_Zervtek_2026-03-04.json (array, date in filename)
run_details.py  → Audi_A3_Zervtek_2026-03-04_details.json (array, by record_index)
process_auction_data.py → Audi_A3_Zervtek_2026-03-04_compiled.json (array)
remove_lots.py  → Prunes listing + details arrays by auction_time
```

### New Flow
```
get_data.py     → Audi_A3_Zervtek.json (keyed by UID, no date in filename)
run_details.py  → Same file, fetches only pending, merges in place
process_auction_data.py → Audi_A3_Zervtek_compiled.json (array, for consumers)
remove_lots.py  → Deletes expired keys from keyed state
```

---

## 2. Data Schemas

### 2.1 New State File Schema (`Audi_A3_Zervtek.json`)

```json
{
  "schema_version": 1,
  "make": "Audi",
  "model": "A3",
  "site_name": "Zervtek",
  "last_updated": "2026-03-04T19:30:00",
  "listings": {
    "a1b2c3d4e5f6g7h8": {
      "status": "completed",
      "last_seen": "2026-03-04",
      "listing": {
        "lot_number": "64",
        "make": "AUDI",
        "model": "A3",
        "year": 2025,
        "grade": "A3 SB30TFSI ADVANCE DO",
        "color": "RED",
        "mileage": 6000,
        "auction": "ZIP Osaka",
        "lot_link": "https://auctions.zervtek.com/auctions/?p=project/lot&id=972055623&s",
        "site_name": "Zervtek"
      },
      "details": {
        "auction_time": "2026-03-05 12:27:00",
        "image_urls": ["https://p3.aleado.com/pic/..."],
        "details": {
          "mileage": "6 000",
          "color": "RED",
          "scores": "5"
        }
      }
    }
  }
}
```

**Status values:** `"pending"` (listing only, needs details) | `"completed"` (merged)

### 2.2 Compiled Output Schema (unchanged for consumers)

```json
[
  {
    "lot_number": "64",
    "make": "AUDI",
    "model": "A3",
    "grade": "A3 SB30TFSI ADVANCE DO",
    "color": "RED",
    "mileage": 6000,
    "score": "5",
    "auction": "ZIP Osaka",
    "auction_time": "2026-03-05 12:27:00",
    "image_urls": ["..."]
  }
]
```

---

## 3. UID Logic

### 3.1 Extract `lot_id` from URL

**Location:** New module `operations/auction/uid_utils.py` (or `get_market_data/Japan/auction_data/uid_utils.py`)

```python
import re
import hashlib

def extract_lot_id_from_url(lot_link: str) -> str | None:
    """
    Extract id= parameter from lot_link.
    Example: ...?p=project/lot&id=972055623&s -> 972055623
    Fallback: return full URL if no id found (for other site formats).
    """
    if not lot_link:
        return None
    m = re.search(r'[?&]id=(\d+)', lot_link, re.I)
    return m.group(1) if m else lot_link

def listing_uid(site_name: str, lot_link: str) -> str:
    """16-char hash for O(1) lookup. Resistant to URL param changes."""
    lot_id = extract_lot_id_from_url(lot_link) or lot_link or ""
    raw = f"{site_name}|{lot_id}"
    return hashlib.sha256(raw.encode()).hexdigest()[:16]
```

### 3.2 URL Patterns to Support

| Site      | URL pattern                          | Extraction |
|-----------|--------------------------------------|------------|
| Zervtek   | `...?p=project/lot&id=972055623&s`   | `id=972055623` |
| Others    | TBD (add as discovered)              | Fallback: full URL |

---

## 4. Component-by-Component Changes

### 4.1 `get_data.py` (get_market_data/Japan/auction_data/get_data.py)

| Task | Description |
|------|-------------|
| **4.1.1** | Add import for `listing_uid`, `extract_lot_id_from_url` |
| **4.1.2** | Change output path: `{Make}_{Model}_{Site}.json` (no date) |
| **4.1.3** | Before save: load existing state file if present |
| **4.1.4** | In `_process_single_site` loop: for each scraped row, compute UID |
| **4.1.5** | If UID in state and `status == "completed"` → skip (do not add to output) |
| **4.1.6** | If UID not in state or `status == "pending"` → add with `status: "pending"` |
| **4.1.7** | Merge new pendings into state; update `last_seen` for existing completed |
| **4.1.8** | Remove `skip_existing_today` / `_listing_path_for_today` (replaced by UID logic) |
| **4.1.9** | Write keyed structure; use atomic write (temp file + rename) for crash safety |

**New save function signature:**
```python
def save_auction_listings_uid(
    results: List[Dict],
    make: str, model: str, site_name: str,
    root_path: Path,
) -> Optional[Path]:
    """Save/merge into keyed state. Path: {Make}/{Model}/Japan/{Make}_{Model}_{Site}.json"""
```

### 4.2 `get_details.py` (get_market_data/Japan/auction_data/get_details.py)

| Task | Description |
|------|-------------|
| **4.2.1** | Add `fetch_pending_details(state: dict, base_url: str, ...)` — accepts keyed state |
| **4.2.2** | Iterate only over entries where `status == "pending"` |
| **4.2.3** | For each pending: fetch URL, merge into `details`, set `status = "completed"` |
| **4.2.4** | Return updated state (or modify in place) |
| **4.2.5** | Keep existing `extract_auction_page_data` unchanged |

### 4.3 `run_details.py` (operations/auction/run_details.py)

| Task | Description |
|------|-------------|
| **4.3.1** | Change file discovery: find `*.json` that are state files (exclude `*_compiled.json`) |
| **4.3.2** | State files: `{Make}_{Model}_{Site}.json` (no `_details`, no date) |
| **4.3.3** | Load state; if no `listings` or all completed → skip file |
| **4.3.4** | Call `fetch_pending_details` with state |
| **4.3.5** | Save updated state to same file (no separate _details.json) |
| **4.3.6** | Remove `skip_done` logic (check for pending count instead) |

### 4.4 `process_auction_data.py` (operations/auction/process_auction_data.py)

| Task | Description |
|------|-------------|
| **4.4.1** | Change file discovery: find state files `{Make}_{Model}_{Site}.json` (exclude `*_compiled.json`) |
| **4.4.2** | Load keyed state |
| **4.4.3** | Iterate `listings` values where `status == "completed"` |
| **4.4.4** | Build compiled record from `listing` + `details` (same field mapping as now) |
| **4.4.5** | Output: `{Make}_{Model}_{Site}_compiled.json` (array) |
| **4.4.6** | Skip entries with `status == "pending"` (no details yet) |

### 4.5 `remove_lots.py` (operations/auction/remove_lots.py)

| Task | Description |
|------|-------------|
| **4.5.1** | Change file discovery: find state files (not _details, not _compiled) |
| **4.5.2** | Load keyed state |
| **4.5.3** | For each UID: get `auction_time` from `listings[uid].details.details.auction_time` |
| **4.5.4** | If expired: `del state["listings"][uid]` |
| **4.5.5** | Save state; delete corresponding `_compiled.json` |
| **4.5.6** | Remove `prune_file_pair` (listing+details) — single file now |

### 4.6 `auction_manager.py` (operations/auction/auction_manager.py)

| Task | Description |
|------|-------------|
| **4.6.1** | Remove `--full` flag (no longer needed; UID handles incremental) |
| **4.6.2** | Pipeline order unchanged: remove_lots → run_all → run_details → process_auction_data |
| **4.6.3** | Update docstrings to reflect UID-based flow |

### 4.7 `run_all.py` (operations/auction/run_all.py)

| Task | Description |
|------|-------------|
| **4.7.1** | Remove `--skip-existing-today` (replaced by UID logic) |
| **4.7.2** | Pass `output_file` without date; `truly_optimized_main` uses new save logic |

### 4.8 `get_images.py` (operations/auction/get_images.py)

| Task | Description |
|------|-------------|
| **4.8.1** | Update `find_compiled_files`: pattern `*_compiled.json` (no date in name) |
| **4.8.2** | Update `extract_date_from_filename`: get date from `auction_time` in record, or from compiled file metadata if needed |
| **4.8.3** | Minimal changes; compiled format unchanged |

---

## 5. File Naming Convention

| File Type | Current | New |
|-----------|---------|-----|
| State (listing + details) | `Audi_A3_Zervtek_2026-03-04.json` + `_details.json` | `Audi_A3_Zervtek.json` |
| Compiled | `Audi_A3_Zervtek_2026-03-04_compiled.json` | `Audi_A3_Zervtek_compiled.json` |

**Path structure:** `data/auction_data/{Make}/{Model}/Japan/{Make}_{Model}_{Site}.json`

---

## 6. Migration Script

**Location:** `operations/auction/migrate_to_uid_state.py`

### 6.1 Migration Logic

```
FOR each path in data/auction_data/**/*.json:
  SKIP if name ends with _details.json or _compiled.json
  
  stem = filename without .json (e.g. Audi_A3_Zervtek_2026-03-03)
  Parse stem to get: Make, Model, Site (and optionally date)
  new_stem = Make_Model_Site (no date)
  new_path = same_dir / f"{new_stem}.json"
  
  Load listing file (array)
  details_path = same_dir / f"{stem}_details.json"
  Load details if exists (array, indexed by record_index)
  
  state = { "schema_version": 1, "make": ..., "model": ..., "site_name": ..., "listings": {} }
  
  FOR i, listing in enumerate(listings):
    lot_link = listing.get("lot_link") or listing.get("url")
    site_name = listing.get("site_name") or infer from path
    uid = listing_uid(site_name, lot_link)
    
    details_entry = details_by_idx.get(i) if details loaded else None
    details_data = details_entry.get("lot_details") if details_entry else None
    
    state["listings"][uid] = {
      "status": "completed" if details_data else "pending",
      "last_seen": extract_date_from_stem(stem) or today,
      "listing": { ...listing fields... },
      "details": details_data if details_data else {}
    }
  
  Write state to new_path (atomic)
  Optionally: rename/archive old files to .bak
```

### 6.2 Migration Options

- `--dry-run`: Report what would be migrated, no writes
- `--backup`: Keep old files as `*.json.bak`
- `--delete-old`: Remove old listing + details after successful migration

### 6.3 Handling Multiple Date Files

If `Audi_A3_Zervtek_2026-03-03.json` and `Audi_A3_Zervtek_2026-03-04.json` both exist:
- Merge into single `Audi_A3_Zervtek.json`
- UID deduplication: later date wins for `last_seen`; completed status preserved

---

## 7. Implementation Order

| Phase | Steps | Est. Effort |
|-------|-------|-------------|
| **Phase 0** | Create `uid_utils.py` with `extract_lot_id_from_url`, `listing_uid` | 0.5 hr |
| **Phase 1** | Migration script `migrate_to_uid_state.py` | 1–2 hr |
| **Phase 2** | Refactor `get_data.py` (save logic, UID filter, state load/merge) | 2–3 hr |
| **Phase 3** | Refactor `get_details.py` (fetch_pending_details) | 1–2 hr |
| **Phase 4** | Refactor `run_details.py` (state file handling) | 1 hr |
| **Phase 5** | Refactor `process_auction_data.py` (read keyed state) | 1 hr |
| **Phase 6** | Refactor `remove_lots.py` (keyed state pruning) | 1 hr |
| **Phase 7** | Update `run_all.py`, `auction_manager.py`, `get_images.py` | 0.5 hr |
| **Phase 8** | Run migration, test full pipeline | 1–2 hr |

**Total:** ~8–12 hours

---

## 8. Testing Strategy

### 8.1 Unit Tests

- `uid_utils.py`: `extract_lot_id_from_url` for various URL formats
- `listing_uid`: Deterministic output, collision resistance

### 8.2 Integration Tests

1. **Migration:** Run on copy of `data/auction_data`, verify state structure
2. **Full pipeline:** remove_lots → run_all → run_details → process_auction_data
3. **Incremental:** Run pipeline twice; second run should skip completed UIDs
4. **New lot:** Manually add pending to state; run details; verify completion

### 8.3 Rollback Plan

- Keep migration backup (`*.json.bak`)
- Git branch for refactor; merge only after validation
- Feature flag: `USE_UID_STATE=true` to toggle old vs new (optional)

---

## 9. Edge Cases

| Case | Handling |
|------|----------|
| `lot_link` missing | Fallback UID: `hash(site_name + lot_number + auction)`; log warning |
| URL has no `id=` | Use full URL in hash |
| Duplicate UID in scrape | Last occurrence wins; overwrite in state |
| Crash during details | State has mix of pending/completed; restart resumes pendings |
| Empty state file | Treat as new; all scraped rows are pending |
| Old date-based files left behind | Migration script; optional cleanup |

---

## 10. Appendix: Current vs New File Layout

### Before
```
data/auction_data/Audi/A3/Japan/
  Audi_A3_Zervtek_2026-03-03.json
  Audi_A3_Zervtek_2026-03-03_details.json
  Audi_A3_Zervtek_2026-03-03_compiled.json
  Audi_A3_Zervtek_2026-03-04.json       (if run again same day)
  ...
```

### After
```
data/auction_data/Audi/A3/Japan/
  Audi_A3_Zervtek.json           ← single accumulating state
  Audi_A3_Zervtek_compiled.json  ← output for consumers
```
