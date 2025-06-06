from calendar import monthcalendar, THURSDAY
from datetime import datetime, timezone, timedelta


def third_thursday(year: int, month: int) -> datetime:
    cal = monthcalendar(year, month)
    thurs = [week[THURSDAY] for week in cal if week[THURSDAY]]
    return datetime(year, month, thurs[2])


def now():
    return datetime.now(tz=timezone(timedelta(hours=7)))
