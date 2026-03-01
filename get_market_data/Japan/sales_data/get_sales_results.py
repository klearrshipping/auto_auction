"""
get_sales_results.py – Search form page (first page after login)

This is the page the user sees immediately after logging in. It is the search form
where you enter manufacturer config: Maker, Model, Year range, etc. Submit this form
to get to the results page (listing table or sales/stats table, depending on URL).

When this page's HTML or behavior changes, update only this script.
"""

import asyncio
from datetime import datetime
from typing import Optional
from playwright.async_api import Page

# -----------------------------------------------------------------------------
# Search page structure (from site HTML). All selectors in one place.
# -----------------------------------------------------------------------------
# Basic Search:
#   Steering wheel:  input name="rudder" value="left" type="checkbox"
#   Result:          select name="result"  (1=Sold, 2=Not sold, 0=All lots)
#   Maker:           select name="mrk" id="mrk"; option text format "MAKER (count)" e.g. "SUZUKI (19020610)"
#   Model:           div#res_models > select (disabled until maker chosen), same option text format
#   Year:            input name="year1", input name="year2"
#   Transmission:    select name="transmission" ("", AT, MT)
#   Mileage:         input name="mileage1", name="mileage2" (thousand km)
#   Shift:           select name="awd" ("", 2WD, 4WD)
#   V engine:        input name="v1", name="v2" (cc)
#   Model type:      input name="type"
#   End price:       input name="end1", name="end2" (thousand JPY)
#   Chassis number:  input name="chassis_no"
#   Lot number:      input name="lot"
#   Modification:    input name="word"
# In addition:
#   Date of Auction: since = sday, smonth, syear; till = fday, fmonth, fyear
#   Color:           input name="color[]" (checkboxes)
#   Scores:          input name="score[]" (checkboxes)
#   Equipment:       input name="eq[]"
#   Truck:           input name="truck[]"
#   Vehicle:         input name="special[]"
#   Auctions:        button ShowHideId('tblAuc'), table#tblAuc (display:none), day1[]..day6[]
# Submit:
#   Search:          input type="button" value="Search" id="btnSearsh" onclick="startSearch('btnSearsh',searchform)"
#   Clear form:      input type="reset" value="Clear form"
# -----------------------------------------------------------------------------

SELECTOR_MAKER = "select[name='mrk']"  # or #mrk
SELECTOR_MODEL = "#res_models select"  # enabled after maker selected
SELECTOR_YEAR_FROM = "input[name='year1']"
SELECTOR_YEAR_TO = "input[name='year2']"
SELECTOR_SEARCH_BUTTON = "#btnSearsh"  # site typo: btnSearsh
SELECTOR_RESULT = "select[name='result']"  # 0=All, 1=Sold, 2=Not sold
SELECTOR_TRANSMISSION = "select[name='transmission']"
SELECTOR_AWD = "select[name='awd']"
# Date of Auction
SELECTOR_SDAY = "select[name='sday']"
SELECTOR_SMONTH = "select[name='smonth']"
SELECTOR_SYEAR = "select[name='syear']"
SELECTOR_FDAY = "select[name='fday']"
SELECTOR_FMONTH = "select[name='fmonth']"
SELECTOR_FYEAR = "select[name='fyear']"


def _option_text_matches_name(option_text: str, name: str) -> bool:
    """
    Match site dropdown option text to a display name.
    Options use format "NAME (count)" e.g. "SUZUKI (19020610)"; match exact or name plus space/paren.
    """
    t = (option_text or "").strip().upper()
    n = (name or "").strip().upper()
    if not n:
        return False
    return t == n or t.startswith(n + " ") or t.startswith(n + "(")


async def resolve_maker_value_by_name(page: Page, maker_name: str) -> Optional[str]:
    """Resolve maker display name to option value from select[name='mrk']. Returns None if not found."""
    opts = await page.evaluate("""() => {
        const sel = document.querySelector('select[name="mrk"]');
        if (!sel) return [];
        return Array.from(sel.options).filter(o => o.value && o.value !== '-1').map(o => ({ text: o.text.trim(), value: o.value }));
    }""")
    for o in opts:
        if _option_text_matches_name(o.get("text") or "", maker_name):
            return o.get("value")
    return None


async def resolve_model_value_by_name(page: Page, model_name: str) -> Optional[str]:
    """Resolve model display name to option value from #res_models select. Call after select_maker. Returns None if not found."""
    opts = await page.evaluate("""() => {
        const sel = document.querySelector('#res_models select');
        if (!sel) return [];
        return Array.from(sel.options).filter(o => o.value && o.value !== '-1').map(o => ({ text: o.text.trim(), value: o.value }));
    }""")
    for o in opts:
        if _option_text_matches_name(o.get("text") or "", model_name):
            return o.get("value")
    return None


async def wait_for_search_page(page: Page, timeout_ms: int = 15000) -> bool:
    """
    Wait until the search form page is loaded (first page after login).
    Returns True if the search button is visible.
    """
    try:
        await page.wait_for_selector(SELECTOR_SEARCH_BUTTON, timeout=timeout_ms)
        return True
    except Exception:
        return False


async def select_maker(page: Page, maker_value: str) -> bool:
    """
    Select Maker in the dropdown. maker_value = option value (e.g. "9" for TOYOTA).
    Triggers SelectCompany(); model dropdown populates after a short delay.
    """
    try:
        await page.select_option(SELECTOR_MAKER, value=maker_value)
        await asyncio.sleep(0.5)  # allow model dropdown to populate
        return True
    except Exception:
        return False


async def select_model(page: Page, model_value: str) -> bool:
    """Select Model in #res_models select. model_value = option value."""
    try:
        await page.select_option(SELECTOR_MODEL, value=model_value)
        return True
    except Exception:
        return False


async def set_year_range(page: Page, year_from: str, year_to: str) -> bool:
    """Fill Year range (e.g. year_from='2019', year_to='2025')."""
    try:
        await page.fill(SELECTOR_YEAR_FROM, str(year_from))
        await page.fill(SELECTOR_YEAR_TO, str(year_to))
        return True
    except Exception:
        return False


def _parse_iso_date(date_str: str) -> Optional[tuple]:
    """Parse YYYY-MM-DD into (day, month, year) as zero-padded strings."""
    try:
        d = datetime.strptime((date_str or "").strip(), "%Y-%m-%d")
        return (d.strftime("%d"), d.strftime("%m"), d.strftime("%Y"))
    except Exception:
        return None


async def set_auction_date_range(
    page: Page,
    since_date: str,
    till_date: str,
) -> bool:
    """
    Set Date of Auction range using YYYY-MM-DD strings.
    since -> sday/smonth/syear, till -> fday/fmonth/fyear
    """
    since = _parse_iso_date(since_date)
    till = _parse_iso_date(till_date)
    if not since or not till:
        return False
    try:
        sday, smonth, syear = since
        fday, fmonth, fyear = till
        await page.select_option(SELECTOR_SDAY, value=sday)
        await page.select_option(SELECTOR_SMONTH, value=smonth)
        await page.select_option(SELECTOR_SYEAR, value=syear)
        await page.select_option(SELECTOR_FDAY, value=fday)
        await page.select_option(SELECTOR_FMONTH, value=fmonth)
        await page.select_option(SELECTOR_FYEAR, value=fyear)
        return True
    except Exception:
        return False


async def set_auction_date_range_parts(
    page: Page,
    sday: str,
    smonth: str,
    syear: str,
    fday: str,
    fmonth: str,
    fyear: str,
) -> bool:
    """Set Date of Auction range using explicit day/month/year dropdown values."""
    try:
        await page.select_option(SELECTOR_SDAY, value=str(sday).zfill(2))
        await page.select_option(SELECTOR_SMONTH, value=str(smonth).zfill(2))
        await page.select_option(SELECTOR_SYEAR, value=str(syear))
        await page.select_option(SELECTOR_FDAY, value=str(fday).zfill(2))
        await page.select_option(SELECTOR_FMONTH, value=str(fmonth).zfill(2))
        await page.select_option(SELECTOR_FYEAR, value=str(fyear))
        return True
    except Exception:
        return False


async def set_result_filter(page: Page, value: str) -> bool:
    """Set Result: "0"=All lots, "1"=Sold, "2"=Not sold."""
    try:
        await page.select_option(SELECTOR_RESULT, value=value)
        return True
    except Exception:
        return False


async def submit_search(page: Page) -> bool:
    """Click the Search button (startSearch). Call after filling the form."""
    try:
        await page.click(SELECTOR_SEARCH_BUTTON)
        return True
    except Exception:
        return False


async def fill_and_submit_search(
    page: Page,
    maker_value: str,
    model_value: Optional[str] = None,
    year_from: Optional[str] = None,
    year_to: Optional[str] = None,
    auction_since_date: Optional[str] = None,
    auction_till_date: Optional[str] = None,
    sday: Optional[str] = None,
    smonth: Optional[str] = None,
    syear: Optional[str] = None,
    fday: Optional[str] = None,
    fmonth: Optional[str] = None,
    fyear: Optional[str] = None,
    result_value: Optional[str] = None,
) -> bool:
    """
    Fill the search form and submit.
    maker_value / model_value must match the site's option values (e.g. "9" for TOYOTA).
    result_value: "0" All, "1" Sold, "2" Not sold.
    """
    if not await select_maker(page, maker_value):
        return False
    if model_value:
        await select_model(page, model_value)
    if year_from is not None and year_to is not None:
        await set_year_range(page, year_from, year_to)
    if auction_since_date and auction_till_date:
        await set_auction_date_range(page, auction_since_date, auction_till_date)
    elif all(v is not None for v in (sday, smonth, syear, fday, fmonth, fyear)):
        await set_auction_date_range_parts(page, sday, smonth, syear, fday, fmonth, fyear)
    if result_value is not None:
        await set_result_filter(page, result_value)
    return await submit_search(page)


RESULT_VALUE_SOLD = "1"  # 0=All, 1=Sold, 2=Not sold


async def run_search_from_config(
    page: Page,
    site: dict,
    search_config: dict,
) -> tuple:
    """
    Run the search form from a config dict (maker/model names or maker_value/model_value).
    Returns (success: bool, current_make: str|None, current_model: str|None).
    """
    sales_search = search_config
    current_make = sales_search.get("maker")
    current_model = sales_search.get("model")
    maker_value = sales_search.get("maker_value")
    model_value = sales_search.get("model_value")
    if not maker_value and sales_search.get("maker"):
        if not await wait_for_search_page(page):
            return (False, current_make, current_model)
        maker_value = await resolve_maker_value_by_name(page, sales_search["maker"])
        if not maker_value:
            return (False, current_make, current_model)
        current_make = sales_search.get("maker")
    if maker_value and sales_search.get("model") and not model_value:
        await select_maker(page, maker_value)
        await asyncio.sleep(0.8)
        model_value = await resolve_model_value_by_name(page, sales_search["model"])
        current_model = sales_search.get("model")
    if not maker_value:
        return (False, current_make, current_model)
    if not await wait_for_search_page(page):
        return (False, current_make, current_model)
    ok = await fill_and_submit_search(
        page,
        maker_value=maker_value,
        model_value=model_value,
        year_from=sales_search.get("year_from"),
        year_to=sales_search.get("year_to"),
        auction_since_date=sales_search.get("auction_since_date"),
        auction_till_date=sales_search.get("auction_till_date"),
        sday=sales_search.get("sday"),
        smonth=sales_search.get("smonth"),
        syear=sales_search.get("syear"),
        fday=sales_search.get("fday"),
        fmonth=sales_search.get("fmonth"),
        fyear=sales_search.get("fyear"),
        result_value=sales_search.get("result_value") or RESULT_VALUE_SOLD,
    )
    return (ok, current_make, current_model)


if __name__ == "__main__":
    import sys
    import os
    _root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
    sys.path.insert(0, _root)
    from playwright.async_api import async_playwright
    from get_market_data.Japan.auction_site_config_JP import auction_sites

    async def _main():
        site_name = list(auction_sites.keys())[0] if auction_sites else None
        if not site_name:
            print("No auction sites configured.")
            return
        site = auction_sites[site_name]
        sales_url = site["scraping"].get("sales_data_url")
        if not sales_url:
            print("No sales_data_url.")
            return
        search_config = site.get("scraping", {}).get("sales_search") or {
            "maker": "SUZUKI", "model": "SWIFT",
            "year_from": str(__import__("datetime").date.today().year - 6), "year_to": str(__import__("datetime").date.today().year),
            "result_value": RESULT_VALUE_SOLD,
        }
        from config.config import browser_settings
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=browser_settings.get("headless", False))
            context = await browser.new_context(viewport={"width": 1920, "height": 1080})
            page = await context.new_page()
            await page.goto(site["scraping"]["auction_url"])
            await page.fill("#usr_name", site.get("username", ""))
            await page.fill("#usr_pwd", site.get("password", ""))
            await page.click('input[name="Submit"][value="Sign in"]')
            await page.wait_for_load_state("networkidle")
            await page.goto(sales_url, wait_until="domcontentloaded")
            await asyncio.sleep(2)
            ok, make, model = await run_search_from_config(page, site, search_config)
            if ok:
                await page.wait_for_selector("#mainTable", timeout=20000)
                print(f"Search done. Results page ready (make={make}, model={model}).")
            else:
                print("Search failed.")
            await browser.close()
    asyncio.run(_main())
