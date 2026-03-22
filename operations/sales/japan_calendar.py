from datetime import date


def is_japanese_holiday(target_date: date):
    """
    Return holiday name for Japanese national holidays, else (None, error_message).
    Requires optional dependency: holidays (pip install holidays).
    """
    try:
        import holidays
    except Exception as e:
        return None, f"holidays package not available ({e})"

    jp_holidays = holidays.country_holidays("JP", years=[target_date.year])
    return jp_holidays.get(target_date), None


def is_japan_working_day(target_date: date):
    """Return (is_working_day, reason)."""
    if target_date.weekday() >= 5:
        return False, "weekend"

    holiday_name, holiday_check_error = is_japanese_holiday(target_date)
    if holiday_check_error:
        return False, f"cannot verify Japanese holidays: {holiday_check_error}"
    if holiday_name:
        return False, f"Japanese holiday ({holiday_name})"

    return True, "working day"
