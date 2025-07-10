from calendar import monthcalendar, THURSDAY
from datetime import datetime, timezone, timedelta


def third_thursday(year: int, month: int) -> datetime:
    cal = monthcalendar(year, month)
    thurs = [week[THURSDAY] for week in cal if week[THURSDAY]]
    return datetime(year, month, thurs[2])


def now():
    return datetime.now(tz=timezone(timedelta(hours=7)))


def krx_vn30f_code(year: int, month: int) -> str:
    """
    Sinh mã VN30F chuẩn KRX cho mọi năm ≥2010.
    Cơ chế: năm_code = (year - 2010) % 30
    """
    YEAR_CODES = "0123456789ABCDEFGHJKLMNPQRSTVW"  # 30 ký tự
    MONTH_CODES = "123456789ABC"  # 12 ký tự (1‒9, A‒C)

    assert month in range(1, 13), "Month must be between 1 and 12"
    assert year in range(2000, 2999), "Year must be between 2000 and 2999"

    if (year == 2025 and month in [9, 12]) or datetime(year, month, 1) <= datetime(2025, 6, 1):
        return "VN30F" + datetime(year, month, 1).strftime("%y%m")

    year_code  = YEAR_CODES[(year - 2010) % 30]
    month_code = MONTH_CODES[month - 1]
    return f"41I1{year_code}{month_code}000"


if __name__ == "__main__":
    print(krx_vn30f_code(2040, 1))   # 41I100000
    print(krx_vn30f_code(2025, 5))
